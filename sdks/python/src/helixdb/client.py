"""HTTP client for HelixDB query routes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from . import _client_common
from ._client_common import (
    HelixError,
    decode_response,
    parse_execute_options,
    prepare_request,
    remote_error,
    serialize_query,
    validate_base_url,
    validate_database_id,
)
from .dsl import QueryRequest

DEFAULT_URL = _client_common.DEFAULT_URL
QUERY_PATH = _client_common.QUERY_PATH


class Client:
    """Synchronous client for running queries against HelixDB."""

    def __init__(
        self,
        url: str | None = None,
        *,
        api_key: str | None = None,
        database_id: str | None = None,
    ) -> None:
        self._mode: Literal["server", "embedded"] = "server"
        self._base_url = validate_base_url(url)
        self._api_key = api_key
        self._database_id = database_id
        self._require_database_id = False
        self._native: Any | None = None

    @classmethod
    def server(
        cls,
        url: str | None = None,
        *,
        api_key: str | None = None,
        database_id: str | None = None,
    ) -> "Client":
        return cls(url, api_key=api_key, database_id=database_id)

    @classmethod
    def managed(cls, url: str | None, *, api_key: str | None = None, database_id: str) -> "Client":
        validate_database_id(database_id, required=True)
        client = cls(url, api_key=api_key, database_id=database_id)
        client._require_database_id = True
        return client

    @classmethod
    def embedded(
        cls, source: "HelixDbSource", *, cache: "EmbeddedCacheConfig | None" = None
    ) -> "Client":
        native_db, native_source, native_cache, native_cache_mode = _native_helixdb()
        client = cls.server()
        client._mode = "embedded"
        try:
            client._native = _run_native(
                native_db.open(_to_native_source(native_source, source))
                if cache is None
                else native_db.open_with_config(
                    _to_native_source(native_source, source),
                    _to_native_cache(native_cache, native_cache_mode, cache),
                )
            )
        except HelixError:
            raise
        except Exception as exc:
            raise HelixError.from_embedded(exc) from exc
        return client

    @classmethod
    def embedded_reader(
        cls, source: "HelixDbSource", *, cache: "EmbeddedCacheConfig | None" = None
    ) -> "Client":
        native_db, native_source, native_cache, native_cache_mode = _native_helixdb()
        client = cls.server()
        client._mode = "embedded"
        try:
            client._native = _run_native(
                native_db.open_reader(_to_native_source(native_source, source))
                if cache is None
                else native_db.open_reader_with_config(
                    _to_native_source(native_source, source),
                    _to_native_cache(native_cache, native_cache_mode, cache),
                )
            )
        except HelixError:
            raise
        except Exception as exc:
            raise HelixError.from_embedded(exc) from exc
        return client

    def with_api_key(self, api_key: str | None = None) -> "Client":
        """Set or clear the bearer API key sent on every request."""

        if self._mode == "server":
            self._api_key = api_key
        return self

    def with_database_id(self, database_id: str | None = None) -> "Client":
        """Set or clear the managed database ID sent as ``x-helix-database-id``."""

        if self._mode == "server":
            self._database_id = database_id
        return self

    def request_builder(self) -> "QueryBuilder":
        return QueryBuilder(
            self._base_url,
            self._api_key,
            self._database_id,
            self._require_database_id,
        )

    def query(self, request: QueryRequest | None = None) -> Any:
        if request is None:
            return self.request_builder()
        if self._mode == "embedded":
            if self._native is None:
                raise HelixError("EmbeddedUnavailable", "embedded HelixDB native handle is missing")
            try:
                response = _run_native(self._native.query_json(serialize_query(request)))
            except HelixError:
                raise
            except Exception as exc:
                raise HelixError.from_embedded(exc) from exc
            return decode_response(bytes(response))
        return self.request_builder().query(request).send()

    @property
    def base_url(self) -> str:
        return self._base_url

    def execute(self, request: QueryRequest, **options: Any) -> Any:
        """Convenience wrapper for ``client.query(request)`` with server headers."""

        parsed = parse_execute_options(options, embedded=self._mode == "embedded")
        if self._mode == "embedded":
            return self.query(request)
        builder = self.request_builder()
        if parsed.writer_only:
            builder.writer_only()
        if parsed.warm_only:
            builder.warm_only()
        if parsed.await_durability is not None:
            builder.should_await_durability(parsed.await_durability)
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
                raise HelixError.from_embedded(exc) from exc
        return self.request_builder().query(request).send_bytes()

    def close(self) -> None:
        if self._native is not None:
            try:
                _run_native(self._native.close())
            except HelixError:
                raise
            except Exception as exc:
                raise HelixError.from_embedded(exc) from exc


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


@dataclass(frozen=True)
class VectorMemoryOnly:
    """Disable non-vector caches while retaining canonical storage."""


@dataclass(frozen=True)
class MemoryCache:
    """Use SlateDB's default in-memory cache."""


@dataclass(frozen=True)
class HybridCache:
    slate_memory_bytes: int
    slate_disk_path: str
    slate_disk_bytes: int
    object_store_disk_path: str
    object_store_disk_bytes: int


EmbeddedCacheMode = VectorMemoryOnly | MemoryCache | HybridCache


@dataclass(frozen=True)
class EmbeddedCacheConfig:
    vector_memory_bytes: int
    mode: EmbeddedCacheMode


@dataclass
class QueryBuilder:
    _base_url: str
    _api_key: str | None = None
    _database_id: str | None = None
    _require_database_id: bool = False
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
            database_id=self._database_id,
            require_database_id=self._require_database_id,
            headers=dict(self._headers or {}),
            query=query,
        )


@dataclass(frozen=True)
class QueryExecutionRequest:
    base_url: str
    api_key: str | None
    database_id: str | None
    require_database_id: bool
    headers: dict[str, str]
    query: QueryRequest

    def send_bytes(self) -> bytes:
        prepared = prepare_request(
            self.base_url,
            self.api_key,
            self.database_id,
            self.require_database_id,
            self.headers,
            self.query,
        )
        request = Request(
            prepared.url,
            data=prepared.body,
            headers=prepared.header_map(),
            method="POST",
        )
        try:
            with urlopen(request) as response:  # nosec B310: user controls Helix endpoint.
                status = response.getcode()
                response_body = response.read()
                reason = getattr(response, "reason", "") or f"unknown error with code: {status}"
        except HTTPError as exc:
            raise remote_error(
                exc.read(),
                exc.reason or str(exc),
                status_code=exc.code,
            ) from exc
        except URLError as exc:
            raise HelixError.network(str(exc.reason), cause=exc) from exc
        except OSError as exc:
            raise HelixError.network(str(exc), cause=exc) from exc

        if status not in {200, 204}:
            raise remote_error(response_body, reason, status_code=status)
        return response_body

    def send(self) -> Any:
        return decode_response(self.send_bytes())


def _native_helixdb() -> tuple[Any, Any, Any | None, Any | None]:
    try:
        import helixdb_uniffi as native
    except ImportError as exc:  # pragma: no cover - depends on native packaging.
        raise HelixError.embedded_unavailable(
            "embedded HelixDB native bindings are not installed", cause=exc
        ) from exc
    return (
        native.HelixDb,
        native.HelixDbSource,
        getattr(native, "EmbeddedCacheConfig", None),
        getattr(native, "EmbeddedCacheMode", None),
    )


def _to_native_cache(native_cache: Any, native_mode: Any, cache: EmbeddedCacheConfig) -> Any:
    if native_cache is None or native_mode is None:
        raise HelixError.embedded_unavailable(
            "native bindings do not expose embedded cache configuration"
        )
    if isinstance(cache.mode, VectorMemoryOnly):
        mode = native_mode.VECTOR_MEMORY_ONLY()
    elif isinstance(cache.mode, MemoryCache):
        mode = native_mode.MEMORY()
    elif isinstance(cache.mode, HybridCache):
        mode = native_mode.HYBRID(
            slate_memory_bytes=cache.mode.slate_memory_bytes,
            slate_disk_path=cache.mode.slate_disk_path,
            slate_disk_bytes=cache.mode.slate_disk_bytes,
            object_store_disk_path=cache.mode.object_store_disk_path,
            object_store_disk_bytes=cache.mode.object_store_disk_bytes,
        )
    else:
        raise HelixError.invalid_url(f"unsupported embedded cache mode: {cache.mode!r}")
    return native_cache(vector_memory_bytes=cache.vector_memory_bytes, mode=mode)


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
    "EmbeddedCacheConfig",
    "HybridCache",
    "MemoryCache",
    "VectorMemoryOnly",
    "InMemory",
    "ObjectStorage",
    "QueryBuilder",
    "QueryExecutionRequest",
]
