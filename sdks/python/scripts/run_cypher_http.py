"""Public HTTP client fixture driver; the parent controls the server restart."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import cypher_parity

from helixdb import AsyncClient, Client


def selected_cases() -> list[dict]:
    phase = os.environ["HELIX_CYPHER_PARITY_PHASE"]
    if phase not in ("before", "after"):
        raise ValueError("invalid Cypher HTTP phase")
    selected = []
    after = False
    for case in cypher_parity.cases():
        after = after or case.get("after_disk_reopen", False)
        if after == (phase == "after"):
            selected.append(case)
    return selected


async def async_main(root: Path) -> None:
    client = AsyncClient(os.environ["HELIX_CYPHER_PARITY_URL"])
    try:
        for case in selected_cases():
            await cypher_parity.write_case_async(client, case, root)
    finally:
        await client.close()


def main() -> None:
    root = Path(os.environ["HELIX_CYPHER_PARITY_RESULTS"]) / "cypher"
    root.mkdir(parents=True, exist_ok=True)
    if os.environ.get("HELIX_PYTHON_PARITY_MODE") == "async":
        asyncio.run(async_main(root))
        return
    client = Client(os.environ["HELIX_CYPHER_PARITY_URL"])
    try:
        for case in selected_cases():
            cypher_parity.write_case(client, case, root)
    finally:
        client.close()


if __name__ == "__main__":
    main()
