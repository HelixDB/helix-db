"""Parse and window the cumulative samples a benchmark server prints to stdout.

A sample is one JSON line marked `"helix_benchmark_sample": 1`. Its counters
are cumulative since process start, so a window is the difference of two
samples of the same process. Exceptions, never subtracted:

* gauges: `GAUGES` in `queue`, `in_flight` in `io`, and storage metrics that
  are gauges in SlateDB (the JSON cannot tell them apart; a negative delta or a
  name such as `*_count` of live objects identifies them, so storage reports
  keep `start`/`end` next to `delta`);
* lifetime maxima: `max_operands`, `max_micros`, `peak_in_flight`, reported
  from the later sample and labelled `*_lifetime`.

Windows use the writer's own clocks: `unix_ms` selects the boundary samples
(the last at or before the start, the first at or after the end) and
`elapsed_ns` (monotonic, same process) measures their distance. The boundary
gaps are reported; they are at most one sample interval when samples cover the
window. Mapping a load-generator window onto `unix_ms` assumes the hosts'
clocks agree; retain clock evidence on multi-host runs.
"""

import json
import math

MARKER = "helix_benchmark_sample"
GAUGES = (
    "retained_bytes",
    "pending_members",
    "pending_operations",
    "uncertain_operations",
    "oldest_pending_micros",
)
QUANTILES = (0.5, 0.95, 0.99)
# Lag histogram layout: exact below 16 us, then 2**3 buckets per power of two.
LINEAR_MICROS = 16
SUB_BUCKET_BITS = 3


def parse(lines):
    """Returns the validated samples among arbitrary log lines, in order."""
    samples = []
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and value.get(MARKER) == 1:
            samples.append(value)
    check(samples)
    return samples


def check(samples):
    """Rejects a missing, restarted, or reordered sample stream.

    Returns anomalies that do not invalidate the stream: counter regressions
    (a server defect) and samples whose exact-lag count disagrees with
    `acknowledged - censored` (fields are read at slightly different moments,
    so only the final, quiescent sample is expected to agree exactly).
    """
    if not samples:
        raise ValueError("no benchmark samples")
    role = samples[0]["role"]
    anomalies = []
    previous = {}
    for index, sample in enumerate(samples):
        if sample["sample"] != index or sample["role"] != role:
            raise ValueError("samples must be contiguous from zero with one role")
        if index and sample["elapsed_ns"] < samples[index - 1]["elapsed_ns"]:
            raise ValueError("elapsed time regressed: a process restart?")
        queue = sample["queue"]
        expected = queue["acknowledged_operations"] - queue["censored_acknowledgements"]
        if sample["lag"]["count"] != expected:
            anomalies.append(
                {
                    "sample": index,
                    "lag_count": sample["lag"]["count"],
                    "expected": expected,
                }
            )
        current = counters(sample)
        anomalies.extend(
            {"sample": index, "regressed": name}
            for name, value in current.items()
            if value < previous.get(name, 0)
        )
        previous = current
    return anomalies


def counters(sample):
    """Flattens every cumulative counter of a sample into `{path: value}`."""
    flat = {f"queue.{k}": v for k, v in sample["queue"].items() if k not in GAUGES}
    flat["lag.count"] = sample["lag"]["count"]
    flat |= {f"lag.{k}": v for k, v in sample["lag"]["buckets"].items()}
    for kind, cost in sample["merge"].items():
        flat |= {f"merge.{kind}.{k}": v for k, v in cost.items() if k != "max_operands"}
    for row in sample["io"]:
        flat |= {
            f"io.{row['service']}.{row['method']}.{path}": value
            for path, value in io_counters(row).items()
        }
    return flat


def io_counters(row):
    """Cumulative fields of one connector row, nested maps flattened."""
    flat = {}
    for key, value in row.items():
        if key in ("service", "method", "in_flight", "peak_in_flight"):
            continue
        if isinstance(value, dict):
            flat |= {f"{key}.{k}": v for k, v in value.items()}
        else:
            flat[key] = value
    return flat


def bracket(samples, start_unix_ms, end_unix_ms):
    """Returns the samples spanning `[start, end)` and their boundary gaps."""
    before = [s for s in samples if s["unix_ms"] <= start_unix_ms]
    after = [s for s in samples if s["unix_ms"] >= end_unix_ms]
    if not before or not after or end_unix_ms <= start_unix_ms:
        raise ValueError("samples do not cover the requested window")
    first, last = before[-1], after[0]
    return (
        first,
        last,
        {
            "start_gap_ms": start_unix_ms - first["unix_ms"],
            "end_gap_ms": last["unix_ms"] - end_unix_ms,
        },
    )


def bucket_floor(micros):
    """Lower bound of the server's lag bucket holding `micros`."""
    if micros < LINEAR_MICROS:
        return micros
    shift = micros.bit_length() - 1 - SUB_BUCKET_BITS
    return (micros >> shift) << shift


def nearest_rank(q, total):
    """1-based nearest rank of quantile `q`, clamped into `[1, total]`."""
    return min(max(math.ceil(q * total), 1), total)


def bucket_quantile(buckets, q):
    """Nearest-rank quantile over `{lower_bound: count}`, as a lower bound."""
    total = sum(buckets.values())
    if not total:
        return None
    rank = nearest_rank(q, total)
    seen = 0
    for floor in sorted(buckets):
        seen += buckets[floor]
        if seen >= rank:
            return floor
    raise AssertionError("rank exceeds the histogram total")


def lag_window(before, after):
    """Exact-operation lag observed between two samples."""
    buckets = {}
    for key, count in after["buckets"].items():
        floor = int(key)
        if bucket_floor(floor) != floor:
            raise ValueError(f"lag bucket {key} is not a bucket lower bound")
        change = count - before["buckets"].get(key, 0)
        if change < 0:
            raise ValueError("lag bucket regressed between samples")
        if change:
            buckets[floor] = change
    count = after["count"] - before["count"]
    if count != sum(buckets.values()):
        raise ValueError("lag count disagrees with its bucket differences")
    return {
        "count": count,
        **{f"p{round(q * 100)}_micros": bucket_quantile(buckets, q) for q in QUANTILES},
        "mean_micros": (after["sum_micros"] - before["sum_micros"]) / count
        if count
        else None,
        "max_micros_lifetime": after["max_micros"],
        "buckets": {str(k): v for k, v in sorted(buckets.items())},
    }


def storage_key(metric):
    labels = ",".join(f"{k}={v}" for k, v in metric["labels"])
    return f"{metric['name']}{{{labels}}}" if labels else metric["name"]


def storage_window(before, after):
    """Changed SlateDB metrics: scalars as start/end/delta, histograms as deltas."""
    earlier = {storage_key(m): m["value"] for m in before}
    changed = {}
    for metric in after:
        key = storage_key(metric)
        value, start = metric["value"], earlier.get(key)
        if isinstance(value, dict):
            start = start or {"count": 0, "sum": 0}
            if value["count"] != start["count"]:
                changed[key] = {
                    "count": value["count"] - start["count"],
                    "sum": value["sum"] - start["sum"],
                }
        elif value != (start or 0):
            changed[key] = {
                "start": start or 0,
                "end": value,
                "delta": value - (start or 0),
            }
    return dict(sorted(changed.items()))


def io_window(before, after):
    """Connector deltas per `service method`; bytes are client body bytes."""
    earlier = {(r["service"], r["method"]): io_counters(r) for r in before}
    rows = {}
    for row in after:
        start = earlier.get((row["service"], row["method"]), {})
        delta = {k: v - start.get(k, 0) for k, v in io_counters(row).items()}
        if any(delta.values()) or row["in_flight"]:
            rows[f"{row['service']} {row['method']}"] = {
                k: v for k, v in delta.items() if v
            } | {
                "in_flight_end": row["in_flight"],
                "peak_in_flight_lifetime": row["peak_in_flight"],
            }
    return rows


def window(before, after, series):
    """Every server metric for the window between two samples of one process.

    `series` is every sample from `before` through `after`, for gauge extremes.
    """
    seconds = (after["elapsed_ns"] - before["elapsed_ns"]) / 1e9
    q0, q1 = before["queue"], after["queue"]
    queue = {k: v - q0[k] for k, v in q1.items() if k not in GAUGES}
    rate = {
        k: queue[k] / seconds if seconds > 0 else None
        for k in (
            "committed_operations",
            "acknowledged_operations",
            "published_operations",
            "published_entities",
            "committed_batches",
        )
    }
    return {
        "seconds": seconds,
        "samples": [before["sample"], after["sample"]],
        "queue": queue,
        "per_second": rate,
        "gauges": {
            k: {
                "start": q0[k],
                "end": q1[k],
                "min": min(s["queue"][k] for s in series),
                "max": max(s["queue"][k] for s in series),
            }
            for k in GAUGES
        },
        "lag": lag_window(before["lag"], after["lag"])
        | {
            "censored": queue["censored_acknowledgements"],
            # Still pending at the window end: no lag yet; the oldest pending
            # age is a lower bound on its eventual lag.
            "unfinished_operations": q1["pending_operations"],
            "unfinished_members": q1["pending_members"],
            "unfinished_oldest_micros_lower_bound": q1["oldest_pending_micros"],
        },
        "merge": {
            kind: {
                k: v - before["merge"][kind][k]
                for k, v in cost.items()
                if k != "max_operands"
            }
            | {"max_operands_lifetime": cost["max_operands"]}
            for kind, cost in after["merge"].items()
        },
        "io": io_window(before["io"], after["io"]),
        "storage": storage_window(before["storage"], after["storage"]),
    }


def timeseries(samples):
    """Compact per-sample trajectory rows for plotting drain and backlog."""
    return [
        {
            "sample": s["sample"],
            "elapsed_s": s["elapsed_ns"] / 1e9,
            "unix_ms": s["unix_ms"],
            **{k: s["queue"][k] for k in GAUGES},
            "acknowledged_operations": s["queue"]["acknowledged_operations"],
            "published_entities": s["queue"]["published_entities"],
            "commit_conflicts": s["queue"]["commit_conflicts"],
        }
        for s in samples
    ]
