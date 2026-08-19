from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep

PYTHON_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PYTHON_ROOT.parents[1]
sys.path.insert(0, str(PYTHON_ROOT / "src"))

from parity_runtime_fixtures import (  # noqa: E402
    base_runtime_fixtures,
    node_permutation_fixtures,
)

from helixdb import (  # noqa: E402
    AsyncClient,
    Client,
    HelixError,
    QueryRequest,
    g,
    read_batch,
    write_batch,
)

EXPECTED_RUNTIME = 233
TRANSACTION_CONFLICT_ATTEMPTS = 8


@dataclass(frozen=True)
class BinaryRuntime:
    path: Path


@dataclass(frozen=True)
class ImageRuntime:
    reference: str


Runtime = BinaryRuntime | ImageRuntime


class RuntimeController:
    def __init__(self, runtime: Runtime, label: str, temporary_root: Path) -> None:
        self.runtime = runtime
        self.label = label
        self.temporary_root = temporary_root
        self.port = _unused_port()
        self.grpc_port = _unused_port()
        self.process: subprocess.Popen[bytes] | None = None
        self.log_file = None
        self.container = f"helixdb-python-parity-{label}-{uuid.uuid4().hex[:12]}"
        self.volume = f"helixdb-python-parity-{label}-{uuid.uuid4().hex[:12]}"

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def prepare(self) -> None:
        if isinstance(self.runtime, ImageRuntime):
            _run(["docker", "image", "inspect", self.runtime.reference])
            _run(["docker", "volume", "create", self.volume])

    def start(self) -> None:
        if isinstance(self.runtime, BinaryRuntime):
            log_path = self.temporary_root / f"{self.label}.log"
            self.log_file = log_path.open("ab")
            data_root = self.temporary_root / "data"
            data_root.mkdir(parents=True, exist_ok=True)
            environment = os.environ.copy()
            environment.pop("S3_BUCKET", None)
            environment.update(
                {
                    "HELIX_HTTP_ADDR": f"127.0.0.1:{self.port}",
                    "HELIX_GRPC_ADDR": f"127.0.0.1:{self.grpc_port}",
                    "HELIX_DATA_DIR": str(data_root),
                    "DB_PATH": f"python-parity-{self.label}/",
                }
            )
            self.process = subprocess.Popen(
                [str(self.runtime.path)],
                cwd=WORKSPACE_ROOT,
                env=environment,
                stdout=self.log_file,
                stderr=subprocess.STDOUT,
            )
        else:
            _run(
                [
                    "docker",
                    "run",
                    "--detach",
                    "--rm",
                    "--name",
                    self.container,
                    # A new Docker volume is root-owned. The published image's
                    # non-root UID cannot initialize it without a helper image.
                    "--user",
                    "0",
                    "--publish",
                    f"127.0.0.1:{self.port}:8080",
                    "--env",
                    "HELIX_DATA_DIR=/var/lib/helix",
                    "--mount",
                    f"type=volume,source={self.volume},target=/var/lib/helix",
                    self.runtime.reference,
                ]
            )
        self._wait_ready()

    def restart(self) -> None:
        self.stop()
        self.start()

    def stop(self) -> None:
        if isinstance(self.runtime, BinaryRuntime):
            if self.process is not None and self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=10)
            self.process = None
            if self.log_file is not None:
                self.log_file.close()
                self.log_file = None
        else:
            subprocess.run(
                ["docker", "rm", "--force", self.container],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    def cleanup(self) -> None:
        self.stop()
        if isinstance(self.runtime, ImageRuntime):
            subprocess.run(
                ["docker", "volume", "rm", "--force", self.volume],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    def _wait_ready(self) -> None:
        deadline = monotonic() + 120.0
        while monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{self.url}/readyz", timeout=1.0) as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
            if self.process is not None and self.process.poll() is not None:
                raise RuntimeError(f"server exited with {self.process.returncode}:\n{self._logs()}")
            sleep(0.1)
        raise TimeoutError(f"server did not become ready at {self.url}:\n{self._logs()}")

    def _logs(self) -> str:
        if isinstance(self.runtime, ImageRuntime):
            result = subprocess.run(
                ["docker", "logs", self.container],
                check=False,
                capture_output=True,
                text=True,
            )
            return result.stdout + result.stderr
        log_path = self.temporary_root / f"{self.label}.log"
        return log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the full Python sync/async corpus against a HelixDB server"
    )
    runtime = parser.add_mutually_exclusive_group(required=True)
    runtime.add_argument("--server-binary", type=Path)
    runtime.add_argument("--image")
    parser.add_argument(
        "--baseline-results",
        type=Path,
        help="Rust server parity result directory to compare against",
    )
    arguments = parser.parse_args()

    selected: Runtime
    if arguments.server_binary is not None:
        binary = arguments.server_binary.resolve()
        if not binary.is_file():
            raise FileNotFoundError(f"server binary does not exist: {binary}")
        selected = BinaryRuntime(binary)
        runtime_label = f"current source binary {binary}"
    else:
        selected = ImageRuntime(arguments.image)
        inspected = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                "--format",
                '{{.Id}} {{join .RepoDigests ","}}',
                arguments.image,
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        runtime_label = f"image {arguments.image} ({inspected})"

    fixtures = sorted(
        [*base_runtime_fixtures(), *node_permutation_fixtures()],
        key=lambda fixture: fixture[0],
    )
    if len(fixtures) != EXPECTED_RUNTIME:
        raise RuntimeError(
            f"Python runtime fixture count was {len(fixtures)}, expected {EXPECTED_RUNTIME}"
        )

    with tempfile.TemporaryDirectory(prefix="helixdb-python-server-parity-") as root:
        temporary_root = Path(root)
        sync_runtime = RuntimeController(selected, "sync", temporary_root / "sync")
        async_runtime = RuntimeController(selected, "async", temporary_root / "async")
        sync_runtime.temporary_root.mkdir(parents=True)
        async_runtime.temporary_root.mkdir(parents=True)
        try:
            sync_runtime.prepare()
            sync_runtime.start()
            sync_results = _run_sync_corpus(sync_runtime, fixtures)
        finally:
            sync_runtime.cleanup()

        try:
            async_runtime.prepare()
            async_runtime.start()
            async_results = asyncio.run(_run_async_corpus(async_runtime, fixtures))
        finally:
            async_runtime.cleanup()

    if sync_results != async_results:
        mismatches = [
            name
            for name in sorted(sync_results)
            if sync_results.get(name) != async_results.get(name)
        ]
        raise RuntimeError(
            f"sync/async Python server parity failed for {len(mismatches)} fixture(s): "
            + ", ".join(
                f"{name} (sync={sync_results.get(name)!r}, async={async_results.get(name)!r})"
                for name in mismatches
            )
        )
    if arguments.baseline_results is not None:
        _assert_rust_baseline(arguments.baseline_results, sync_results)
    print(
        "Python sync/async server parity passed for "
        f"{len(fixtures)} fixtures against {runtime_label}"
    )


def _assert_rust_baseline(baseline_root: Path, python_results: dict[str, object]) -> None:
    baseline_files = sorted(baseline_root.rglob("*.json"))
    if len(baseline_files) != EXPECTED_RUNTIME:
        raise RuntimeError(
            f"Rust baseline contained {len(baseline_files)} fixtures, expected {EXPECTED_RUNTIME}"
        )

    baseline = {
        path.stem: _normalize_response(json.loads(path.read_text(encoding="utf-8")))
        for path in baseline_files
    }
    if baseline.keys() != python_results.keys():
        missing = sorted(baseline.keys() - python_results.keys())
        extra = sorted(python_results.keys() - baseline.keys())
        raise RuntimeError(
            "Python server parity fixture names differ from the Rust baseline: "
            f"missing={missing}, extra={extra}"
        )
    mismatches = [name for name in sorted(baseline) if baseline[name] != python_results[name]]
    if mismatches:
        raise RuntimeError(
            f"Python/Rust server parity failed for {len(mismatches)} fixture(s): "
            + ", ".join(mismatches)
        )


def _run_sync_corpus(
    runtime: RuntimeController,
    fixtures: list[tuple[str, QueryRequest]],
) -> dict[str, object]:
    client = Client(runtime.url)
    results: dict[str, object] = {}
    try:
        for name, request in fixtures:
            response = _query_with_conflict_retry_sync(client, request)
            _await_index_operations_sync(client, response)
            results[name] = _normalize_response(response)
            if name == "905-read-text-drop-candidates":
                runtime.restart()
                reopened = _query_with_conflict_retry_sync(client, request)
                if _normalize_response(reopened) != results[name]:
                    raise RuntimeError(f"sync fixture {name} changed after server restart")
        _assert_post_drop_sync(client, fixtures)
    finally:
        client.close()
    return results


async def _run_async_corpus(
    runtime: RuntimeController,
    fixtures: list[tuple[str, QueryRequest]],
) -> dict[str, object]:
    client = AsyncClient(runtime.url)
    results: dict[str, object] = {}
    try:
        for name, request in fixtures:
            response = await _query_with_conflict_retry_async(client, request)
            await _await_index_operations_async(client, response)
            results[name] = _normalize_response(response)
            if name == "905-read-text-drop-candidates":
                runtime.restart()
                reopened = await _query_with_conflict_retry_async(client, request)
                if _normalize_response(reopened) != results[name]:
                    raise RuntimeError(f"async fixture {name} changed after server restart")
        await _assert_post_drop_async(client, fixtures)
        overlap_request = _required_fixture(fixtures, "002-read-count-all-users")
        overlap_results = await asyncio.gather(*(client.query(overlap_request) for _ in range(8)))
        if any(result != overlap_results[0] for result in overlap_results[1:]):
            raise RuntimeError("overlapping async server reads returned different results")

        writes = [
            QueryRequest.write(
                write_batch()
                .var_as(
                    "created",
                    g().add_n("AsyncServerParityConcurrent", {"sequence": sequence}),
                )
                .returning(["created"])
            )
            for sequence in range(8)
        ]
        await asyncio.gather(
            *(_query_with_conflict_retry_async(client, request) for request in writes)
        )
        concurrent_read = QueryRequest.read(
            read_batch()
            .var_as("count", g().n_with_label("AsyncServerParityConcurrent").count())
            .returning(["count"])
        )
        reads = await asyncio.gather(*(client.query(concurrent_read) for _ in range(8)))
        if any(result != reads[0] for result in reads[1:]):
            raise RuntimeError("overlapping async server reads were inconsistent")
    finally:
        await client.close()
    return results


def _query_with_conflict_retry_sync(client: Client, request: QueryRequest) -> object:
    for attempt in range(TRANSACTION_CONFLICT_ATTEMPTS):
        try:
            return client.query(request)
        except HelixError as error:
            if (
                error.kind != "Remote"
                or error.status_code != 409
                or attempt + 1 == TRANSACTION_CONFLICT_ATTEMPTS
            ):
                raise
            sleep(0.01 * 2**attempt)
    raise AssertionError("sync transaction conflict retry loop exhausted")


async def _query_with_conflict_retry_async(client: AsyncClient, request: QueryRequest) -> object:
    for attempt in range(TRANSACTION_CONFLICT_ATTEMPTS):
        try:
            return await client.query(request)
        except HelixError as error:
            if (
                error.kind != "Remote"
                or error.status_code != 409
                or attempt + 1 == TRANSACTION_CONFLICT_ATTEMPTS
            ):
                raise
            await asyncio.sleep(0.01 * 2**attempt)
    raise AssertionError("async transaction conflict retry loop exhausted")


def _await_index_operations_sync(client: Client, response: object) -> None:
    for operation_id in sorted(_collect_operation_ids(response)):
        deadline = monotonic() + 60.0
        while True:
            status_response = client.query(_index_status_request(operation_id))
            status = status_response.get("status", {}).get("status")
            if status == "succeeded":
                break
            if status not in {"queued", "running"}:
                raise RuntimeError(
                    f"operation {operation_id} reached unexpected status {status}: "
                    f"{status_response}"
                )
            if monotonic() >= deadline:
                raise TimeoutError(f"operation {operation_id} did not finish within 60s")
            sleep(0.01)


async def _await_index_operations_async(client: AsyncClient, response: object) -> None:
    for operation_id in sorted(_collect_operation_ids(response)):
        deadline = monotonic() + 60.0
        while True:
            status_response = await client.query(_index_status_request(operation_id))
            status = status_response.get("status", {}).get("status")
            if status == "succeeded":
                break
            if status not in {"queued", "running"}:
                raise RuntimeError(
                    f"operation {operation_id} reached unexpected status {status}: "
                    f"{status_response}"
                )
            if monotonic() >= deadline:
                raise TimeoutError(f"operation {operation_id} did not finish within 60s")
            await asyncio.sleep(0.01)


def _index_status_request(operation_id: str) -> QueryRequest:
    return QueryRequest.read(
        read_batch().var_as("status", g().get_index_operation(operation_id)).returning(["status"])
    )


def _assert_post_drop_sync(client: Client, fixtures: list[tuple[str, QueryRequest]]) -> None:
    for name in ("025-read-text-search-nodes", "027-read-text-search-edges"):
        try:
            client.query(_required_fixture(fixtures, name))
        except HelixError as error:
            if error.code == "index_not_found" or (
                error.code is None and "index_not_found" in str(error)
            ):
                continue
            raise
        raise RuntimeError(f"sync {name} unexpectedly succeeded after index DROP")


async def _assert_post_drop_async(
    client: AsyncClient, fixtures: list[tuple[str, QueryRequest]]
) -> None:
    for name in ("025-read-text-search-nodes", "027-read-text-search-edges"):
        try:
            await client.query(_required_fixture(fixtures, name))
        except HelixError as error:
            if error.code == "index_not_found" or (
                error.code is None and "index_not_found" in str(error)
            ):
                continue
            raise
        raise RuntimeError(f"async {name} unexpectedly succeeded after index DROP")


def _required_fixture(fixtures: list[tuple[str, QueryRequest]], name: str) -> QueryRequest:
    for fixture_name, request in fixtures:
        if fixture_name == name:
            return request
    raise RuntimeError(f"missing fixture {name}")


def _collect_operation_ids(value: object, ids: set[str] | None = None) -> set[str]:
    ids = set() if ids is None else ids
    if isinstance(value, list):
        for entry in value:
            _collect_operation_ids(entry, ids)
    elif isinstance(value, dict):
        if value.get("kind") in {"accepted", "existing_operation"} and isinstance(
            value.get("operation_id"), str
        ):
            ids.add(value["operation_id"])
        for entry in value.values():
            _collect_operation_ids(entry, ids)
    return ids


def _normalize_operation_ids(value: object) -> object:
    if isinstance(value, list):
        return [_normalize_operation_ids(entry) for entry in value]
    if not isinstance(value, dict):
        return value
    normalized = {key: _normalize_operation_ids(entry) for key, entry in value.items()}
    if normalized.get("kind") in {"accepted", "existing_operation"} and (
        "operation_id" in normalized
    ):
        normalized["operation_id"] = "<operation-id>"
    return normalized


def _normalize_response(value: object) -> object:
    """Normalize nondeterministic operation UUIDs and opaque entity ID offsets."""

    operation_normalized = _normalize_operation_ids(value)
    entity_ids = sorted(_collect_entity_ids(operation_normalized))
    replacements = {entity_id: f"<entity-id:{index}>" for index, entity_id in enumerate(entity_ids)}
    return _replace_entity_ids(operation_normalized, replacements)


def _collect_entity_ids(value: object, ids: set[int] | None = None) -> set[int]:
    ids = set() if ids is None else ids
    if isinstance(value, list):
        for entry in value:
            _collect_entity_ids(entry, ids)
    elif isinstance(value, dict):
        for key, entry in value.items():
            if key in {"$id", "$from", "$to"} and isinstance(entry, int):
                ids.add(entry)
            else:
                _collect_entity_ids(entry, ids)
    return ids


def _replace_entity_ids(value: object, replacements: dict[int, str]) -> object:
    if isinstance(value, list):
        return [_replace_entity_ids(entry, replacements) for entry in value]
    if not isinstance(value, dict):
        return value
    return {
        key: (
            replacements[entry]
            if key in {"$id", "$from", "$to"} and isinstance(entry, int) and entry in replacements
            else _replace_entity_ids(entry, replacements)
        )
        for key, entry in value.items()
    }


def _unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _run(arguments: list[str]) -> None:
    subprocess.run(arguments, check=True, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
