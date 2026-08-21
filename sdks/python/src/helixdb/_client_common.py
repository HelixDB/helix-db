"""Shared request and error contracts for HelixDB Python clients."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urljoin, urlparse

from .dsl import QueryRequest

DEFAULT_URL = "http://localhost:6969"
QUERY_PATH = "/v2/query"


class HelixError(Exception):
    """Error raised by the HelixDB clients."""

    def __init__(
        self,
        kind: str,
        message: str,
        *,
        details: str | None = None,
        code: str | None = None,
        status_code: int | None = None,
        retryable: bool | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.details = details
        self.code = code
        self.status_code = status_code
        self.retryable = retryable
        self.__cause__ = cause

    @classmethod
    def network(cls, message: str, *, cause: BaseException | None = None) -> "HelixError":
        return cls(
            "Network",
            f"error communicating with server: {message}",
            details=message,
            cause=cause,
        )

    @classmethod
    def remote(
        cls,
        details: str,
        *,
        code: str | None = None,
        status_code: int | None = None,
        retryable: bool | None = None,
    ) -> "HelixError":
        return cls(
            "Remote",
            f"got error from server: {details}",
            details=details,
            code=code,
            status_code=status_code,
            retryable=retryable,
        )

    def is_retryable(self) -> bool:
        """Return true only for an explicit server retry classification."""

        return self.kind == "Remote" and self.retryable is True

    @classmethod
    def serialization(cls, message: str, *, cause: BaseException | None = None) -> "HelixError":
        return cls(
            "Serialization",
            f"error serializing data: {message}",
            details=message,
            cause=cause,
        )

    @classmethod
    def invalid_url(cls, message: str, *, cause: BaseException | None = None) -> "HelixError":
        return cls("InvalidUrl", f"invalid url: {message}", details=message, cause=cause)

    @classmethod
    def invalid_request(cls, message: str) -> "HelixError":
        return cls(
            "InvalidRequest",
            f"invalid request: {message}",
            details=message,
            code="invalid_request",
        )

    @classmethod
    def embedded_unavailable(
        cls, message: str, *, cause: BaseException | None = None
    ) -> "HelixError":
        return cls(
            "EmbeddedUnavailable",
            f"embedded bindings unavailable: {message}",
            details=message,
            cause=cause,
        )

    @classmethod
    def embedded(
        cls,
        message: str,
        *,
        code: str | None = None,
        cause: BaseException | None = None,
    ) -> "HelixError":
        return cls(
            "Embedded",
            f"embedded HelixDB error: {message}",
            details=message,
            code=code,
            cause=cause,
        )

    @classmethod
    def from_embedded(cls, cause: BaseException) -> "HelixError":
        """Preserve the explicit UniFFI ``error``/``msg`` pair when available."""

        code = getattr(cause, "error", None)
        message = getattr(cause, "msg", None)
        return cls.embedded(
            message if isinstance(message, str) else str(cause),
            code=code if isinstance(code, str) else None,
            cause=cause,
        )


@dataclass(frozen=True)
class PreparedRequest:
    """Immutable wire request shared by synchronous and asynchronous transports."""

    url: str
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def header_map(self) -> dict[str, str]:
        """Return a transport-owned mutable header map."""

        return dict(self.headers)


@dataclass(frozen=True)
class ExecuteOptions:
    """Validated server routing options for one query."""

    writer_only: bool = False
    warm_only: bool = False
    await_durability: bool | None = None

    def apply(self, headers: Mapping[str, str]) -> dict[str, str]:
        """Add configured routing headers to a copy of ``headers``."""

        prepared = dict(headers)
        if self.writer_only:
            prepared["x-helix-require-writer"] = "true"
        if self.warm_only:
            prepared["x-helix-warm"] = "true"
        if self.await_durability is not None:
            prepared["x-helix-await-durable"] = "true" if self.await_durability else "false"
        return prepared


def validate_base_url(url: str | None) -> str:
    """Validate and return a server base URL using the synchronous contract."""

    base_url = url or DEFAULT_URL
    try:
        parsed = urlparse(base_url)
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise HelixError.invalid_url(str(exc), cause=exc) from exc
    if (
        parsed.scheme not in {"http", "https"}
        or hostname is None
        or any(character.isspace() for character in base_url)
    ):
        raise HelixError.invalid_url("missing scheme or host")
    return base_url


def prepare_request(
    base_url: str,
    api_key: str | None,
    headers: Mapping[str, str],
    query: QueryRequest,
) -> PreparedRequest:
    """Serialize a query and resolve the common HelixDB HTTP request."""

    body = serialize_query(query)
    try:
        url = urljoin(base_url.rstrip("/") + "/", QUERY_PATH)
    except Exception as exc:
        raise HelixError.invalid_url(str(exc), cause=exc) from exc

    prepared_headers = dict(headers)
    if api_key is not None:
        prepared_headers["Authorization"] = f"Bearer {api_key}"
    return PreparedRequest(url, tuple(prepared_headers.items()), body)


def serialize_query(query: QueryRequest) -> bytes:
    """Serialize one query with the shared sync and async error contract."""

    try:
        return query.to_json_bytes()
    except Exception as exc:
        raise HelixError.serialization(str(exc), cause=exc) from exc


def decode_response(response_body: bytes) -> Any:
    """Decode the shared empty-or-JSON response contract."""

    if not response_body:
        return None
    try:
        return json.loads(response_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HelixError.serialization(str(exc), cause=exc) from exc


def remote_error(
    response_body: bytes,
    fallback: str,
    *,
    status_code: int | None = None,
) -> HelixError:
    """Decode new and legacy error envelopes without rejecting future codes."""

    text = response_body.decode("utf-8", errors="replace")
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        payload = None
    if isinstance(payload, dict):
        error = payload.get("error")
        msg = payload.get("msg")
        legacy_code = payload.get("code")
        retryable = payload.get("retryable")
        explicit_retryable = retryable if isinstance(retryable, bool) else None
        if isinstance(error, str) and isinstance(msg, str):
            return HelixError.remote(
                msg,
                code=error,
                status_code=status_code,
                retryable=explicit_retryable,
            )
        if isinstance(error, str):
            return HelixError.remote(
                error,
                code=legacy_code if isinstance(legacy_code, str) else None,
                status_code=status_code,
                retryable=explicit_retryable,
            )
    return HelixError.remote(text or fallback, status_code=status_code)


def parse_execute_options(options: Mapping[str, Any], *, embedded: bool) -> ExecuteOptions:
    """Validate ``execute`` keyword options without changing caller-owned state."""

    if embedded and options:
        unknown = ", ".join(sorted(options))
        raise HelixError.invalid_request(
            f"embedded mode does not support execute option(s): {unknown}"
        )

    remaining = dict(options)
    writer_only = bool(remaining.pop("writer_only", False))
    warm_only = bool(remaining.pop("warm_only", False))
    await_durability = (
        bool(remaining.pop("await_durability")) if "await_durability" in remaining else None
    )
    if remaining:
        unknown = ", ".join(sorted(remaining))
        raise TypeError(f"unknown execute option(s): {unknown}")
    return ExecuteOptions(writer_only, warm_only, await_durability)
