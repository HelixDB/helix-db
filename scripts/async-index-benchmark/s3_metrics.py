#!/usr/bin/env python3
"""Archive S3 service-side request/byte observations for a UTC minute window.

CloudWatch delivery is best-effort. Missing points stay missing, and successful
retrieval is never a claim of complete request accounting or wire-level bytes.
The whole-bucket filter includes setup/evidence traffic: keep that traffic out
of measurement windows and compare the database-prefix filter independently.
"""

import argparse
import datetime
import json
import math
import subprocess
from pathlib import Path

METRICS = (
    "AllRequests",
    "GetRequests",
    "PutRequests",
    "DeleteRequests",
    "HeadRequests",
    "PostRequests",
    "ListRequests",
    "4xxErrors",
    "5xxErrors",
    "BytesUploaded",
    "BytesDownloaded",
)


def aws(region, *args):
    return json.loads(
        subprocess.check_output(
            ["aws", "--region", region, *args, "--output", "json"], text=True
        )
    )


def collect(bucket, region, filter_id, start, end, output):
    if start < 0 or start % 60 or end % 60 or not 0 < end - start <= 86400:
        raise ValueError(
            "window must use UTC minute boundaries and span at most one day"
        )
    output.mkdir(parents=True, exist_ok=False)
    initial = {
        "schema": 1,
        "status": "incomplete",
        "bucket": bucket,
        "region": region,
        "filter_id": filter_id,
        "start_unix_seconds": start,
        "end_unix_seconds": end,
        "delivery_complete": False,
        "wire_bytes_verified": False,
        "release_or_performance_acceptance": False,
    }
    report_path = output / "s3-metrics.json"
    report_path.write_text(json.dumps(initial, indent=2) + "\n")
    config = aws(
        region,
        "s3api",
        "get-bucket-metrics-configuration",
        "--bucket",
        bucket,
        "--id",
        filter_id,
    )
    (output / "filter.json").write_text(json.dumps(config, indent=2) + "\n")
    if config["MetricsConfiguration"]["Id"] != filter_id:
        raise ValueError("metrics filter identity mismatch")
    queries = [
        {
            "Id": f"m{index}",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/S3",
                    "MetricName": name,
                    "Dimensions": [
                        {"Name": "BucketName", "Value": bucket},
                        {"Name": "FilterId", "Value": filter_id},
                    ],
                },
                "Period": 60,
                "Stat": "Sum",
            },
            "ReturnData": True,
        }
        for index, name in enumerate(METRICS)
    ]
    (output / "queries.json").write_text(json.dumps(queries, indent=2) + "\n")
    values = {query["Id"]: {} for query in queries}
    seen = set()
    tokens = set()
    token = None
    page_number = 0
    statuses = {}
    messages = []
    while True:
        args = [
            "cloudwatch",
            "get-metric-data",
            "--metric-data-queries",
            json.dumps(queries),
            "--start-time",
            str(start),
            "--end-time",
            str(end),
            "--scan-by",
            "TimestampAscending",
            "--no-paginate",
        ]
        if token:
            args.extend(["--next-token", token])
        page = aws(region, *args)
        (output / f"page-{page_number:04d}.json").write_text(
            json.dumps(page, indent=2) + "\n"
        )
        page_number += 1
        messages.extend(page.get("Messages", []))
        page_ids = set()
        for result in page["MetricDataResults"]:
            key = result["Id"]
            if (
                key not in values
                or key in page_ids
                or len(result["Timestamps"]) != len(result["Values"])
            ):
                raise ValueError("invalid metric identity or point arrays")
            page_ids.add(key)
            if result["StatusCode"] not in {"Complete", "PartialData"}:
                raise ValueError("CloudWatch metric retrieval failed")
            seen.add(key)
            statuses[key] = result["StatusCode"]
            messages.extend(result.get("Messages", []))
            for stamp, value in zip(
                result["Timestamps"], result["Values"], strict=True
            ):
                parsed = datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    raise ValueError("metric timestamp must have a timezone")
                epoch = parsed.timestamp()
                if not start <= epoch < end or epoch % 60 or epoch in values[key]:
                    raise ValueError(
                        "duplicate, unaligned or out-of-window metric point"
                    )
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    or value < 0
                    or value != int(value)
                ):
                    raise ValueError("metric sum must be a finite nonnegative integer")
                values[key][int(epoch)] = int(value)
        token = page.get("NextToken")
        if not token:
            break
        if token in tokens:
            raise ValueError("repeated CloudWatch pagination token")
        tokens.add(token)
    if (
        seen != set(values)
        or any(status != "Complete" for status in statuses.values())
        or messages
    ):
        raise ValueError(
            "CloudWatch returned missing, partial or diagnostic results; retain raw pages"
        )
    metrics = {}
    for query, name in zip(queries, METRICS, strict=True):
        points = values[query["Id"]]
        metrics[name] = {
            "observed_sum": sum(points.values()) if points else None,
            "observed_minutes": len(points),
            "missing_minute_offsets": [
                (stamp - start) // 60
                for stamp in range(start, end, 60)
                if stamp not in points
            ],
            "points": [
                {"unix_seconds": stamp, "value": points[stamp]}
                for stamp in sorted(points)
            ],
        }
    report = {
        **initial,
        "status": "retrieved",
        "filter_configuration": config["MetricsConfiguration"],
        "pages": page_number,
        "metrics": metrics,
        "retrieved_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "byte_scope": "S3-observed request/response bodies; excludes TLS/TCP/IP overhead",
        "request_scope": "HTTP requests observed by S3, including delivered retry requests",
        "missing_points": "unknown or no emitted sample; never substituted with zero",
    }
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--filter-id", required=True)
    parser.add_argument("--start-unix-seconds", type=int, required=True)
    parser.add_argument("--end-unix-seconds", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    collect(
        args.bucket,
        args.region,
        args.filter_id,
        args.start_unix_seconds,
        args.end_unix_seconds,
        args.output,
    )


if __name__ == "__main__":
    main()
