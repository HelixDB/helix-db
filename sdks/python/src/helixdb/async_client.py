"""Asynchronous HTTP and embedded client for HelixDB query routes."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any

import httpx

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
from .client import (
    EmbeddedCacheConfig,
    HelixDbSource,
    _native_helixdb,
    _to_native_cache,
    _to_native_source,
)
from .dsl import QueryRequest

Timeout = float | httpx.Timeout | None


@dataclass(frozen=True)
class _ServerBackend:
    base_url: str
    api_key: str | None
    database_id: str | None
    require_database_id: bool
    http_client: httpx.AsyncClient
    timeout: Timeout


@dataclass(frozen=True)
class _EmbeddedBackend:
    native: Any


class _ClosedBackend(Enum):
    CLOSED = "closed"


_OpenBackend = _ServerBackend | _EmbeddedBackend
_Backend = _OpenBackend | _ClosedBackend


@dataclass
class _ClientState:
    backend: _Backend


@dataclass(frozen=True)
class _ServerRequestBackend:
    state: _ClientState
    base_url: str
    api_key: str | None
    database_id: str | None
    require_database_id: bool
    http_client: httpx.AsyncClient
    timeout: Timeout


@dataclass(frozen=True)
class _EmbeddedRequestBackend:
    state: _ClientState
    native: Any


_RequestBackend = _ServerRequestBackend | _EmbeddedRequestBackend


class AsyncClient:
    """Asynchronous client for running queries against HelixDB.

    The client owns its reusable HTTP connection pool and any injected
    transport, so close it with ``async with`` or :meth:`close`.

    >>> import asyncio
    >>> import httpx
    >>> from helixdb import AsyncClient, QueryRequest, read_batch
    >>> async def example():
    ...     async def handler(request):
    ...         return httpx.Response(200, json={"count": 0})
    ...     transport = httpx.MockTransport(handler)
    ...     async with AsyncClient(transport=transport) as client:
    ...         return await client.query(QueryRequest.read(read_batch()))
    >>> asyncio.run(example())
    {'count': 0}
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        api_key: str | None = None,
        database_id: str | None = None,
        timeout: Timeout = None,
        limits: httpx.Limits | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        client_options: dict[str, Any] = {
            "follow_redirects": True,
            "timeout": timeout,
        }
        if limits is not None:
            client_options["limits"] = limits
        if transport is not None:
            client_options["transport"] = transport
        self._state = _ClientState(
            _ServerBackend(
                validate_base_url(url),
                api_key,
                database_id,
                False,
                httpx.AsyncClient(**client_options),
                timeout,
            )
        )

    @classmethod
    def _from_backend(cls, backend: _OpenBackend) -> "AsyncClient":
        client = object.__new__(cls)
        client._state = _ClientState(backend)
        return client

    @classmethod
    def server(
        cls,
        url: str | None = None,
        *,
        api_key: str | None = None,
        database_id: str | None = None,
        timeout: Timeout = None,
        limits: httpx.Limits | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> "AsyncClient":
        """Create a server client with one reusable HTTP connection pool."""

        return cls(
            url,
            api_key=api_key,
            database_id=database_id,
            timeout=timeout,
            limits=limits,
            transport=transport,
        )

    @classmethod
    def managed(
        cls,
        url: str | None,
        *,
        api_key: str | None = None,
        database_id: str,
        timeout: Timeout = None,
        limits: httpx.Limits | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> "AsyncClient":
        validate_database_id(database_id, required=True)
        client = cls(
            url,
            api_key=api_key,
            database_id=database_id,
            timeout=timeout,
            limits=limits,
            transport=transport,
        )
        backend = client._state.backend
        if isinstance(backend, _ServerBackend):
            client._state.backend = replace(backend, require_database_id=True)
        return client

    @classmethod
    async def embedded(
        cls,
        source: HelixDbSource,
        *,
        cache: EmbeddedCacheConfig | None = None,
    ) -> "AsyncClient":
        """Open an asynchronous embedded writer client."""

        native_db, native_source, native_cache, native_cache_mode = _native_helixdb()
        try:
            native = await (
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
        return cls._from_backend(_EmbeddedBackend(native))

    @classmethod
    async def embedded_reader(
        cls,
        source: HelixDbSource,
        *,
        cache: EmbeddedCacheConfig | None = None,
    ) -> "AsyncClient":
        """Open an asynchronous embedded reader client."""

        native_db, native_source, native_cache, native_cache_mode = _native_helixdb()
        try:
            native = await (
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
        return cls._from_backend(_EmbeddedBackend(native))

    def with_api_key(self, api_key: str | None = None) -> "AsyncClient":
        """Set or clear the bearer API key sent on future server requests."""

        backend = self._state.backend
        if isinstance(backend, _ServerBackend):
            self._state.backend = replace(backend, api_key=api_key)
        elif backend is _ClosedBackend.CLOSED:
            raise HelixError.invalid_request("client is closed")
        return self

    def with_database_id(self, database_id: str | None = None) -> "AsyncClient":
        """Set or clear the managed database ID sent as ``x-helix-database-id``."""

        backend = self._state.backend
        if isinstance(backend, _ServerBackend):
            self._state.backend = replace(backend, database_id=database_id)
        elif backend is _ClosedBackend.CLOSED:
            raise HelixError.invalid_request("client is closed")
        return self

    def _request_backend(self) -> _RequestBackend:
        backend = self._state.backend
        if isinstance(backend, _ServerBackend):
            return _ServerRequestBackend(
                self._state,
                backend.base_url,
                backend.api_key,
                backend.database_id,
                backend.require_database_id,
                backend.http_client,
                backend.timeout,
            )
        if isinstance(backend, _EmbeddedBackend):
            return _EmbeddedRequestBackend(self._state, backend.native)
        raise HelixError.invalid_request("client is closed")

    def request_builder(self) -> "AsyncQueryBuilder":
        """Start an immutable asynchronous query request."""

        return AsyncQueryBuilder(self._request_backend())

    async def query(self, request: QueryRequest, *, timeout: Timeout = None) -> Any:
        """Execute and decode one query."""

        return await self.request_builder().query(request).send(timeout=timeout)

    async def execute(self, request: QueryRequest, **options: Any) -> Any:
        """Execute one query with server routing and timeout options."""

        remaining = dict(options)
        timeout = remaining.pop("timeout", None)
        request_backend = self._request_backend()
        embedded = isinstance(request_backend, _EmbeddedRequestBackend)
        parsed = parse_execute_options(remaining, embedded=embedded)
        builder = AsyncQueryBuilder(request_backend)
        builder._headers = parsed.apply(builder._headers)
        return await builder.query(request).send(timeout=timeout)

    @property
    def base_url(self) -> str:
        backend = self._state.backend
        if isinstance(backend, _ServerBackend):
            return backend.base_url
        if isinstance(backend, _EmbeddedBackend):
            return "embedded://helixdb"
        raise HelixError.invalid_request("client is closed")

    async def close(self) -> None:
        """Close the owned HTTP transport or embedded database handle once."""

        backend = self._state.backend
        if backend is _ClosedBackend.CLOSED:
            return
        self._state.backend = _ClosedBackend.CLOSED
        if isinstance(backend, _ServerBackend):
            try:
                await backend.http_client.aclose()
            except Exception as exc:
                raise HelixError.network(str(exc), cause=exc) from exc
            return
        try:
            await backend.native.close()
        except Exception as exc:
            raise HelixError.from_embedded(exc) from exc

    async def __aenter__(self) -> "AsyncClient":
        self._request_backend()
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.close()


@dataclass
class AsyncQueryBuilder:
    """Builder for one asynchronous server or embedded request."""

    _backend: _RequestBackend
    _headers: dict[str, str] = field(default_factory=lambda: {"Content-Type": "application/json"})

    def writer_only(self) -> "AsyncQueryBuilder":
        """Require the authoritative writer for this server request."""

        self._headers["x-helix-require-writer"] = "true"
        return self

    def warm_only(self) -> "AsyncQueryBuilder":
        """Warm eligible backends without returning a query result."""

        self._headers["x-helix-warm"] = "true"
        return self

    def should_await_durability(self, should: bool) -> "AsyncQueryBuilder":
        """Control whether the server waits for durable write acknowledgement."""

        self._headers["x-helix-await-durable"] = "true" if should else "false"
        return self

    def query(self, query: QueryRequest) -> "AsyncQueryExecutionRequest":
        """Freeze the builder headers and attach a query body."""

        return AsyncQueryExecutionRequest(
            self._backend,
            tuple(self._headers.items()),
            query,
        )


@dataclass(frozen=True)
class AsyncQueryExecutionRequest:
    """Complete asynchronous query request ready to send."""

    _backend: _RequestBackend
    _headers: tuple[tuple[str, str], ...]
    _query: QueryRequest

    def _ensure_open(self) -> None:
        if self._backend.state.backend is _ClosedBackend.CLOSED:
            raise HelixError.invalid_request("client is closed")

    async def send_bytes(self, *, timeout: Timeout = None) -> bytes:
        """Execute the request and retain the raw response body."""

        self._ensure_open()
        if isinstance(self._backend, _EmbeddedRequestBackend):
            if timeout is not None:
                raise HelixError.invalid_request(
                    "embedded queries do not support client timeouts; use asyncio.timeout"
                )
            server_options = [name for name, _ in self._headers if name.lower() != "content-type"]
            if server_options:
                raise HelixError.invalid_request(
                    "embedded queries do not support server request options: "
                    + ", ".join(server_options)
                )
            try:
                return bytes(await self._backend.native.query_json(serialize_query(self._query)))
            except HelixError:
                raise
            except Exception as exc:
                raise HelixError.from_embedded(exc) from exc

        prepared = prepare_request(
            self._backend.base_url,
            self._backend.api_key,
            self._backend.database_id,
            self._backend.require_database_id,
            dict(self._headers),
            self._query,
        )
        request_timeout = self._backend.timeout if timeout is None else timeout
        try:
            async with self._backend.http_client.stream(
                "POST",
                prepared.url,
                headers=prepared.header_map(),
                content=prepared.body,
                timeout=request_timeout,
            ) as response:
                response_body = await response.aread()
                status = response.status_code
                reason = response.reason_phrase or f"unknown error with code: {status}"
        except httpx.RequestError as exc:
            raise HelixError.network(str(exc), cause=exc) from exc

        if status not in {200, 204}:
            raise remote_error(response_body, reason, status_code=status)
        return response_body

    async def send(self, *, timeout: Timeout = None) -> Any:
        """Execute and decode the response body."""

        return decode_response(await self.send_bytes(timeout=timeout))


__all__ = [
    "AsyncClient",
    "AsyncQueryBuilder",
    "AsyncQueryExecutionRequest",
]
