#!/usr/bin/env python3
"""Check early-termination value contracts against a seeded smoke-test server."""
import json
import sys
import urllib.error
import urllib.request


def query(port, root, saved=None):
    payload = {"request_type": "read", "query": {"read": {
        "entries": [{"query": {"name": "result", "root": root}}],
        "returns": ["result"],
    }}}
    if saved is not None:
        payload["query"]["read"]["entries"].insert(0, {
            "query": {"name": "saved", "root": saved},
        })
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/v2/query",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)["result"]


def check(port):
    source = {"has_label": {"input": {"nodes": {"reference": "all"}},
                            "label": "DockerImageSmokeUser"}}
    count = query(port, {"count": {"input": source}})
    ids = query(port, {"id": {"input": source}})
    assert ids, "smoke fixture must contain nodes"
    for matches in (True, False):
        condition = {"eq": {"left": {"property": "scenario"},
                            "right": {"constant": {"string":
                                "docker-image-smoke" if matches else "absent"}}}}
        for projection, expected in (("count", [count]), ("id", ids[:1])):
            child = {"root": {projection: {"input": "context"}}}
            for has_else in (False, True):
                branch = {"input": source, "condition": condition, "then_traversal": child}
                if has_else:
                    branch["else_traversal"] = child
                root = {"limit": {"input": {"choose": branch}, "count": {"literal": 1}}}
                actual = query(port, root)
                assert actual == (expected if matches or has_else else None), (root, actual)

    # Full-input operators must reject scalar input even under zero demand.
    empty = {"has_label": {"input": {"nodes": {"reference": "all"}},
                           "label": "MissingEarlyTerminationFixture"}}
    for input_rows in (empty, source):
        for operator in ("label", "fold", "unfold"):
            for take in (0, 1):
                root = {"limit": {"input": {operator: {"input": {"id": {"input": input_rows}}}},
                                  "count": {"literal": take}}}
                try:
                    query(port, root)
                except urllib.error.HTTPError as error:
                    body = json.loads(error.read())
                    assert body.get("error") == "invalid_query", body
                else:
                    raise AssertionError(f"{operator} accepted a scalar input with limit {take}")

    # Saved operands must be validated before any window can stop polling.
    for operator in ("inject", "within", "without"):
        for input_rows in (empty, source):
            for saved_rows in (empty, source):
                for take in (None, 0, 1, count + 1):
                    window = {operator: {"input": input_rows, "variable": "saved"}}
                    if take is not None:
                        window = {"limit": {"input": window, "count": {"literal": take}}}
                    for terminal in ("id", "count", "exists"):
                        root = {terminal: {"input": window}}
                        try:
                            query(port, root, saved={"id": {"input": saved_rows}})
                        except urllib.error.HTTPError as error:
                            body = json.loads(error.read())
                            assert body.get("error") == "invalid_query", body
                        else:
                            raise AssertionError(
                                f"{operator} accepted scalar saved operand with limit {take} and {terminal}"
                            )
                        # Valid row operands preserve ordering and duplicate semantics.
                        left = ids if input_rows == source else []
                        right = ids if saved_rows == source else []
                        if operator == "inject":
                            expected = left + right
                        elif operator == "within":
                            expected = [item for item in left if item in right]
                        else:
                            expected = [item for item in left if item not in right]
                        expected = expected[:take]
                        if terminal == "count":
                            expected = len(expected)
                        elif terminal == "exists":
                            expected = bool(expected)
                        actual = query(port, root, saved=saved_rows)
                        # Existing empty scalar projections can serialize as
                        # null for a known-empty plan or [] for a drained plan.
                        if terminal == "id" and actual is None:
                            actual = []
                        assert actual == expected, (root, actual, expected)
                        folded = {"fold": {"input": saved_rows}}
                        if operator == "inject":
                            try:
                                query(port, root, saved=folded)
                            except urllib.error.HTTPError as error:
                                body = json.loads(error.read())
                                assert body.get("error") == "invalid_query", body
                            else:
                                raise AssertionError(f"inject accepted folded operand with limit {take}")
                        else:
                            try:
                                actual = query(port, root, saved=folded)
                            except urllib.error.HTTPError as error:
                                raise AssertionError(
                                    f"{operator} rejected folded operand with limit {take} and {terminal}"
                                ) from error
                            if terminal == "id" and actual is None:
                                actual = []
                            assert actual == expected, (root, actual, expected)

    # Enter the repeat body with a large frontier before satisfying LIMIT 1.
    large = {"nodes": {"reference": {"ids": ids[:1]}}}
    for _ in range(12):
        large = {"union": {"input": large, "traversals": [
            {"root": "context"}, {"root": "context"},
        ]}}
    assert query(port, {"count": {"input": large}}) == 4096
    repeat = {"repeat": {"input": large, "config": {
        "traversal": {"root": "context"}, "times": 1, "emit": "after", "max_depth": 1,
    }}}
    assert query(port, {"id": {"input": {"limit": {"input": repeat,
                    "count": {"literal": 1}}}}}) == ids[:1]
    print("early-termination conditional values and saved-operand type contracts passed")


if __name__ == "__main__":
    check(int(sys.argv[1]))
