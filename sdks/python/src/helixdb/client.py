"""HTTP client for HelixDB query routes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from .dsl import QueryRequest

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

    @classmethod
    def invalid_request(cls, message: str) -> "HelixError":
        return cls("InvalidRequest", f"invalid request: {message}", details=message)

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
    def embedded(cls, message: str, *, cause: BaseException | None = None) -> "HelixError":
        return cls(
            "Embedded",
            f"embedded HelixDB error: {message}",
            details=message,
            cause=cause,
        )


class Client:
    """Synchronous client for running queries against HelixDB."""

    def __init__(self, url: str | None = None, *, api_key: str | None = None) -> None:
        self._mode: Literal["server", "embedded"] = "server"
        self._base_url = url or DEFAULT_URL
        parsed = urlparse(self._base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise HelixError.invalid_url("missing scheme or host")
        self._api_key = api_key
        self._native: Any | None = None

    @classmethod
    def server(cls, url: str | None = None, *, api_key: str | None = None) -> "Client":
        return cls(url, api_key=api_key)

    @classmethod
    def embedded(cls, source: "HelixDbSource") -> "Client":
        native_db, native_source = _native_helixdb()
        client = cls.server()
        client._mode = "embedded"
        try:
            client._native = _run_native(
                native_db.open(_to_native_source(native_source, source))
            )
        except HelixError:
            raise
        except Exception as exc:
            raise HelixError.embedded(str(exc), cause=exc) from exc
        return client

    @classmethod
    def embedded_reader(cls, source: "HelixDbSource") -> "Client":
        native_db, native_source = _native_helixdb()
        client = cls.server()
        client._mode = "embedded"
        try:
            client._native = _run_native(
                native_db.open_reader(_to_native_source(native_source, source))
            )
        except HelixError:
            raise
        except Exception as exc:
            raise HelixError.embedded(str(exc), cause=exc) from exc
        return client

    def with_api_key(self, api_key: str | None = None) -> "Client":
        """Set or clear the bearer API key sent on every request."""

        if self._mode == "server":
            self._api_key = api_key
        return self

    def request_builder(self) -> "QueryBuilder":
        return QueryBuilder(self._base_url, self._api_key)

    def query(self, request: QueryRequest | None = None) -> Any:
        if request is None:
            return self.request_builder()
        if self._mode == "embedded":
            if self._native is None:
                raise HelixError("EmbeddedUnavailable", "embedded HelixDB native handle is missing")
            try:
                response = _run_native(self._native.query_json(request.to_json_bytes()))
            except HelixError:
                raise
            except Exception as exc:
                raise HelixError.embedded(str(exc), cause=exc) from exc
            if not response:
                return None
            try:
                return json.loads(bytes(response).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise HelixError.serialization(str(exc), cause=exc) from exc
        return self.request_builder().query(request).send()

    @property
    def base_url(self) -> str:
        return self._base_url

    def execute(self, request: QueryRequest, **options: Any) -> Any:
        """Convenience wrapper for ``client.query(request)`` with server headers."""

        if self._mode == "embedded":
            if options:
                unknown = ", ".join(sorted(options))
                raise HelixError.invalid_request(
                    f"embedded mode does not support execute option(s): {unknown}"
                )
            return self.query(request)
        builder = self.request_builder()
        if options.pop("writer_only", False):
            builder.writer_only()
        if options.pop("warm_only", False):
            builder.warm_only()
        if "await_durability" in options:
            builder.should_await_durability(bool(options.pop("await_durability")))
        if options:
            unknown = ", ".join(sorted(options))
            raise TypeError(f"unknown execute option(s): {unknown}")
        return builder.query(request).send()

    def graph(self, selection: Any) -> Any:
        """Load one immutable native graph with one ordinary read request."""

        from .graph import load_graph

        return load_graph(self, selection)

    def _graph_response(self, request: QueryRequest, native_spec: Any) -> Any:
        if self._mode == "embedded":
            if self._native is None:
                raise HelixError("EmbeddedUnavailable", "embedded HelixDB native handle is missing")
            try:
                return _run_native(self._native.graph(request.to_json_bytes(), native_spec))
            except HelixError:
                raise
            except Exception as exc:
                raise HelixError.embedded(str(exc), cause=exc) from exc
        return self.request_builder().query(request).send_bytes()

    def close(self) -> None:
        if self._native is not None:
            try:
                _run_native(self._native.close())
            except HelixError:
                raise
            except Exception as exc:
                raise HelixError.embedded(str(exc), cause=exc) from exc


HelixDBClient = Client


@dataclass(frozen=True)
class InMemory:
    database: str


@dataclass(frozen=True)
class Disk:
    root: str
    database: str


@dataclass(frozen=True)
class ObjectStorage:
    database: str
    bucket: str
    region: str
    endpoint: str | None = None
    allow_http: bool = False


HelixDbSource = InMemory | Disk | ObjectStorage


@dataclass
class QueryBuilder:
    _base_url: str
    _api_key: str | None = None
    _headers: dict[str, str] | None = None

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

    def query(self, query: QueryRequest) -> "QueryExecutionRequest":
        return QueryExecutionRequest(
            base_url=self._base_url,
            api_key=self._api_key,
            headers=dict(self._headers or {}),
            query=query,
        )


@dataclass(frozen=True)
class QueryExecutionRequest:
    base_url: str
    api_key: str | None
    headers: dict[str, str]
    query: QueryRequest

    def send_bytes(self) -> bytes:
        try:
            url = urljoin(self.base_url.rstrip("/") + "/", QUERY_PATH)
        except Exception as exc:
            raise HelixError.invalid_url(str(exc), cause=exc) from exc

        headers = dict(self.headers)
        if self.api_key is not None:
            headers["Authorization"] = f"Bearer {self.api_key}"

        request = Request(url, data=self.query.to_json_bytes(), headers=headers, method="POST")
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
        return response_body

    def send(self) -> Any:
        response_body = self.send_bytes()
        if not response_body:
            return None
        try:
            return json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise HelixError.serialization(str(exc), cause=exc) from exc


def _native_helixdb() -> tuple[Any, Any]:
    try:
        from helixdb_uniffi import HelixDb as native_db
        from helixdb_uniffi import HelixDbSource as native_source
    except ImportError as exc:  # pragma: no cover - depends on native packaging.
        raise HelixError.embedded_unavailable(
            "embedded HelixDB native bindings are not installed", cause=exc
        ) from exc
    return native_db, native_source


def _to_native_source(native_source: Any, source: HelixDbSource) -> Any:
    if isinstance(source, InMemory):
        return native_source.IN_MEMORY(database=source.database)
    if isinstance(source, Disk):
        return native_source.DISK(root=source.root, database=source.database)
    if isinstance(source, ObjectStorage):
        return native_source.OBJECT_STORAGE(
            database=source.database,
            bucket=source.bucket,
            region=source.region,
            endpoint=source.endpoint,
            allow_http=source.allow_http,
        )
    raise HelixError.invalid_url(f"unsupported HelixDbSource: {source!r}")


def _run_native(awaitable: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    raise HelixError(
        "EmbeddedRuntime",
        "synchronous embedded client methods cannot run inside an active event loop",
    )


__all__ = [
    "Client",
    "Disk",
    "HelixDBClient",
    "HelixDbSource",
    "HelixError",
    "InMemory",
    "ObjectStorage",
    "QueryBuilder",
    "QueryExecutionRequest",
]
