"""Run unchanged shared Cypher fixtures through public SDK methods, without retries."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

from helixdb import AsyncClient, Client, Disk, EmbeddedCacheConfig, HelixError, InMemory


def cases() -> list[dict]:
    path = Path(
        os.environ.get(
            "HELIX_CYPHER_PARITY_FIXTURES",
            Path(__file__).resolve().parents[2] / "tests" / "cypher" / "runtime.json",
        )
    )
    corpus = json.loads(path.read_text(encoding="utf-8"))
    if corpus.get("schema_version") != 1 or len(corpus["cases"]) != 10:
        raise ValueError("unsupported or incomplete Cypher parity corpus")
    seen: set[str] = set()
    for case in corpus["cases"]:
        name = case["name"]
        if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9-]+", name) or name in seen:
            raise ValueError("Cypher parity names must be unique safe basenames")
        seen.add(name)
    return corpus["cases"]


def write_case(client: Client, case: dict, root: Path) -> None:
    try:
        output = {
            "result": client.cypher(
                case["query"],
                case.get("parameters", {}),
                query_name=case["name"],
            )
        }
    except HelixError as error:
        output = {"error": str(error), "code": error.code, "details": error.server_details}
    (root / f"{case['name']}.json").write_text(
        json.dumps(output, ensure_ascii=False, allow_nan=False),
        encoding="utf-8",
    )


async def write_case_async(client: AsyncClient, case: dict, root: Path) -> None:
    try:
        output = {
            "result": await client.cypher(
                case["query"],
                case.get("parameters", {}),
                query_name=case["name"],
            )
        }
    except HelixError as error:
        output = {"error": str(error), "code": error.code, "details": error.server_details}
    (root / f"{case['name']}.json").write_text(
        json.dumps(output, ensure_ascii=False, allow_nan=False),
        encoding="utf-8",
    )


def run_embedded(source: Disk | InMemory, cache: EmbeddedCacheConfig, results: Path) -> None:
    fixtures = cases()
    root = results / "cypher"
    root.mkdir(parents=True, exist_ok=True)
    client = Client.embedded(source, cache=cache)
    try:
        for case in fixtures:
            if isinstance(source, Disk) and case.get("after_disk_reopen", False):
                client.close()
                reader = Client.embedded_reader(source, cache=cache)
                try:
                    write_case(reader, case, root)
                finally:
                    reader.close()
                client = Client.embedded(source, cache=cache)
            else:
                write_case(client, case, root)
    finally:
        client.close()


async def run_embedded_async(
    source: Disk | InMemory, cache: EmbeddedCacheConfig, results: Path
) -> None:
    fixtures = cases()
    root = results / "cypher"
    root.mkdir(parents=True, exist_ok=True)
    client = await AsyncClient.embedded(source, cache=cache)
    try:
        for case in fixtures:
            if isinstance(source, Disk) and case.get("after_disk_reopen", False):
                await client.close()
                reader = await AsyncClient.embedded_reader(source, cache=cache)
                try:
                    await write_case_async(reader, case, root)
                finally:
                    await reader.close()
                client = await AsyncClient.embedded(source, cache=cache)
            else:
                await write_case_async(client, case, root)
    finally:
        await client.close()
