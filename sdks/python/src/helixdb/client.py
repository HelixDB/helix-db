"""HTTP client for HelixDB query routes."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from .dsl import DynamicQueryRequest, stringify_json

DEFAULT_URL = "http://localhost:6969"
QUERY_PATH = "/v1/query"


class HelixError(Exception):
    """Error raised by the HelixDB client."""

    def __init__(
        self,
        kind: str,
        message: str,
        *,
        details: str | None = None,
        status_code: int | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.details = details
        self.status_code = status_code
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
    def remote(cls, details: str, *, status_code: int | None = None) -> "HelixError":
        return cls(
            "Remote",
            f"got error from server: {details}",
            details=details,
            status_code=status_code,
        )

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


class Client:
    """Synchronous HTTP client for running queries against a Helix instance."""

    def __init__(self, url: str | None = None, *, api_key: str | None = None) -> None:
        self._base_url = url or DEFAULT_URL
        parsed = urlparse(self._base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise HelixError.invalid_url("missing scheme or host")
        self._api_key = api_key

    def with_api_key(self, api_key: str | None = None) -> "Client":
        """Set or clear the bearer API key sent on every request."""

        self._api_key = api_key
        return self

    def query(self) -> "QueryBuilder":
        return QueryBuilder(self._base_url, self._api_key)

    @property
    def base_url(self) -> str:
        return self._base_url

    def execute(self, request: DynamicQueryRequest, **options: Any) -> Any:
        """Convenience wrapper for ``client.query().dynamic(request).send()``."""

        builder = self.query()
        if options.pop("writer_only", False):
            builder.writer_only()
        if options.pop("warm_only", False):
            builder.warm_only()
        if "await_durability" in options:
            builder.should_await_durability(bool(options.pop("await_durability")))
        if options:
            unknown = ", ".join(sorted(options))
            raise TypeError(f"unknown execute option(s): {unknown}")
        return builder.dynamic(request).send()


HelixDBClient = Client


@dataclass
class QueryBuilder:
    _base_url: str
    _api_key: str | None = None
    _headers: dict[str, str] | None = None
    _body: str | None = None

    def __post_init__(self) -> None:
        if self._headers is None:
            self._headers = {"Content-Type": "application/json"}

    def writer_only(self) -> "QueryBuilder":
        self._headers["x-helix-require-writer"] = "true"  # type: ignore[index]
        return self

    def warm_only(self) -> "QueryBuilder":
        self._headers["x-helix-warm"] = "true"  # type: ignore[index]
        return self

    def should_await_durability(self, should: bool) -> "QueryBuilder":
        self._headers["x-helix-await-durable"] = "true" if should else "false"  # type: ignore[index]
        return self

    def body(self, data: Any) -> "QueryBuilder":
        try:
            self._body = stringify_json(data)
        except (TypeError, ValueError) as exc:
            raise HelixError.serialization(str(exc), cause=exc) from exc
        return self

    def stored(self, query_name: str) -> "QueryRequest":
        return QueryRequest(
            base_url=self._base_url,
            api_key=self._api_key,
            headers=dict(self._headers or {}),
            query_type="stored",
            query_name=query_name,
            body=self._body,
        )

    def dynamic(self, query: DynamicQueryRequest) -> "QueryRequest":
        return QueryRequest(
            base_url=self._base_url,
            api_key=self._api_key,
            headers=dict(self._headers or {}),
            query_type="dynamic",
            dynamic_query=query,
        )


@dataclass(frozen=True)
class QueryRequest:
    base_url: str
    api_key: str | None
    headers: dict[str, str]
    query_type: str
    query_name: str | None = None
    dynamic_query: DynamicQueryRequest | None = None
    body: str | None = None

    def send(self) -> Any:
        if self.query_type == "dynamic":
            if self.dynamic_query is None:
                raise HelixError.serialization("dynamic query request is missing")
            path = QUERY_PATH
            payload = self.dynamic_query.to_json_bytes()
        elif self.query_type == "stored":
            if self.query_name is None:
                raise HelixError.serialization("stored query name is missing")
            path = f"{QUERY_PATH}/{self.query_name}"
            payload = self.body.encode("utf-8") if self.body is not None else None
        else:
            raise HelixError.serialization(f"unknown query type: {self.query_type}")

        try:
            url = urljoin(self.base_url.rstrip("/") + "/", path)
        except Exception as exc:
            raise HelixError.invalid_url(str(exc), cause=exc) from exc

        headers = dict(self.headers)
        if self.api_key is not None:
            headers["Authorization"] = f"Bearer {self.api_key}"

        request = Request(url, data=payload, headers=headers, method="POST")
        try:
            with urlopen(request) as response:  # nosec B310: user controls Helix endpoint.
                status = response.getcode()
                response_body = response.read()
                reason = getattr(response, "reason", "") or f"unknown error with code: {status}"
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace") or exc.reason or str(exc)
            raise HelixError.remote(details, status_code=exc.code) from exc
        except URLError as exc:
            raise HelixError.network(str(exc.reason), cause=exc) from exc
        except OSError as exc:
            raise HelixError.network(str(exc), cause=exc) from exc

        if status != 200:
            details = response_body.decode("utf-8", errors="replace") or reason
            raise HelixError.remote(details, status_code=status)
        if not response_body:
            return None
        try:
            return json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise HelixError.serialization(str(exc), cause=exc) from exc


__all__ = ["Client", "HelixDBClient", "HelixError", "QueryBuilder", "QueryRequest"]
