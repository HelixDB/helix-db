#!/usr/bin/env python3
"""Process-cold paired reads on existing disposable native-volume fixtures.

The kernel page cache is not dropped. Restart clears the server's LSM and
property caches. The small helper image only reads process memory accounting
and resets its high-water mark; it does not read or modify database files.
"""
import argparse
import json
import http.client
import statistics
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import range_benchmark as bench

HELPER = 'busybox@sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0'


def memory(container, reset=False):
    command = 'echo 5 > /proc/1/clear_refs; cat /proc/1/status' if reset else 'cat /proc/1/status'
    output = subprocess.check_output(['docker', 'run', '--rm', '--pid', f'container:{container}',
                                      '--user', '65532:65532', HELPER, 'sh', '-c', command], text=True)
    return {line.split(':')[0]: int(line.split()[1]) for line in output.splitlines()
            if line.startswith(('VmHWM:', 'VmRSS:'))}


def restart(container, port):
    if not container.startswith('range-benchmark-'):
        raise ValueError('Only disposable range-benchmark-* containers may be restarted')
    subprocess.run(['docker', 'restart', container], check=True, stdout=subprocess.DEVNULL)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/readyz', timeout=1):
                return
        except (urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected, ConnectionResetError):
            time.sleep(.05)
    raise TimeoutError(container)


def run(args):
    rows = bench.fixture_rows(args.size, args.raw_bytes, False)
    expected_rows = sorted((row for row in rows if row['tenant'] == bench.TENANT), key=lambda row: -row['last_seen'])[:1000]
    cases = []
    for fixture in ['ordered-range-wide-projection.json', 'ordered-range-narrow-projection.json']:
        payload = json.loads((bench.FIXTURES / fixture).read_text())
        projection = payload['query']['read']['entries'][0]['query']['root']['value_map']['properties']
        expected = [{key: row[key] for key in projection if key in row} for row in expected_rows]
        measurements = {'baseline': [], 'candidate': []}
        peaks = {'baseline': [], 'candidate': []}
        for sample in range(args.samples):
            roles = [('baseline', args.baseline_container, args.baseline), ('candidate', args.candidate_container, args.candidate)]
            if sample % 2:
                roles.reverse()
            for role, container, port in roles:
                restart(container, port)
                before = memory(container, reset=True)
                result, elapsed = bench.request(port, payload)
                assert bench.normalized(result) == expected
                after = memory(container)
                measurements[role].append(elapsed)
                peaks[role].append({'peak_rss_kib': after['VmHWM'], 'rss_increase_kib': max(0, after['VmHWM'] - before['VmRSS'])})
        timing = {role: {'p50_s': statistics.median(values), 'p95_s': bench.percentile(values, .95),
                         'max_peak_rss_kib': max(item['peak_rss_kib'] for item in peaks[role]),
                         'max_rss_increase_kib': max(item['rss_increase_kib'] for item in peaks[role])}
                  for role, values in measurements.items()}
        case = {'query': payload['query_name'], 'mode': 'process-cold, kernel cache retained',
                'samples': args.samples, 'latency_samples_s': measurements, **timing}
        cases.append(case)
        print(json.dumps(case), flush=True)
    Path(args.output).write_text(json.dumps(cases, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=int, default=18196)
    parser.add_argument('--candidate', type=int, default=18197)
    parser.add_argument('--baseline-container', default='range-benchmark-disk-baseline')
    parser.add_argument('--candidate-container', default='range-benchmark-disk-candidate')
    parser.add_argument('--size', type=int, default=2000)
    parser.add_argument('--raw-bytes', type=int, default=4096)
    parser.add_argument('--samples', type=int, default=20)
    parser.add_argument('--output', required=True)
    run(parser.parse_args())
