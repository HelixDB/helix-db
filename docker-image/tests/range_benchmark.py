#!/usr/bin/env python3
"""Paired HTTP benchmark on two disposable, empty local HelixDB images.

Uses the checked-in customer requests and a fixed fixture clock. No production
credentials or endpoint defaults are accepted. Each request is checked against
an independently sorted fixture. This measures warm reads; cold storage and
churn runs must be reported separately.
"""
import argparse
import concurrent.futures
import threading
import json
import http.client
import math
import statistics
import time
import urllib.error
import urllib.request
from pathlib import Path

FIXTURES = Path(__file__).parent / 'fixtures'
CUTOFF = 1788000000000000
TENANT = 'FZob8v6ZTnCcuVk4'


def request(port, payload):
    wire = json.dumps(payload).encode()
    started = time.perf_counter()
    req = urllib.request.Request(f'http://127.0.0.1:{port}/v2/query', wire,
                                 {'content-type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(error.read().decode()) from error
    return result, time.perf_counter() - started


def batch(kind, roots):
    return {'request_type': kind, 'query': {kind: {
        'entries': [{'query': {'name': f'q{i}', 'root': root}}
                    for i, root in enumerate(roots)],
        'returns': [f'q{i}' for i in range(len(roots))]}}}


def prepare(port, rows):
    deadline = time.monotonic() + 60
    while True:
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/readyz', timeout=1):
                break
        except (urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected, ConnectionResetError):
            if time.monotonic() >= deadline:
                raise TimeoutError(f'local server {port} did not become ready')
            time.sleep(.05)
    for prop in ['tenant', 'type', 'last_seen']:
        spec = {'label': 'Resource', 'property': prop}
        family = 'node_range' if prop == 'last_seen' else 'node_equality'
        spec.update({'direction': 'asc'} if prop == 'last_seen' else {'unique': False})
        receipt, _ = request(port, batch('write', [{'create_index': {
            'spec': {family: spec}, 'if_not_exists': True}}]))
        operation = receipt['q0'].get('operation_id')
        if operation:
            deadline = time.monotonic() + 120
            while True:
                status, _ = request(port, batch('read', [{'get_index_operation': {'operation_id': operation}}]))
                state = status['q0']['status']
                if state == 'succeeded':
                    break
                if state in ['failed', 'blocked', 'aborted'] or time.monotonic() > deadline:
                    raise RuntimeError(status)
                time.sleep(.1)
    for start in range(0, len(rows), 100):
        roots = [{'add_n': {'label': 'Resource', 'properties': [
            [key, {'value': {'i64' if isinstance(value, int) else 'string': value}}]
            for key, value in row.items()]}} for row in rows[start:start + 100]]
        request(port, batch('write', roots))
    print(json.dumps({'seeded_port': port, 'rows': len(rows)}), flush=True)


def fixture_rows(size, raw_bytes, tied, full_properties=False):
    projection = json.loads((FIXTURES / 'ordered-range-wide-projection.json').read_text())['query']['read']['entries'][0]['query']['root']['value_map']['properties']
    rows = []
    for i in range(size * 5):
        # Exactly 20% belong to the target; matching IDs are interleaved.
        row = {'id': f'resource-{i:08}', 'tenant': TENANT if i % 5 == 0 else f'other-{i % 4}',
               'type': 'pod' if i % 5 in (0, 1) else 'service',
               'last_seen': CUTOFF + (1 if tied else i + 1),
               'cluster_id': 'fixture-cluster', 'namespace': 'fixture',
               'name': f'name-{i:08}', 'raw_data': 'x' * raw_bytes}
        if full_properties:
            row = {**{key: f'fixture-{key}' for key in projection}, **row}
        rows.append(row)
    return rows


def normalized(result):
    return [row.get('properties', row) for row in result['q']]


def percentile(values, percentile):
    return sorted(values)[max(0, math.ceil(len(values) * percentile) - 1)]


def calibrate(port, size, samples):
    payload = json.loads((FIXTURES / 'ordered-range-narrow-projection.json').read_text())
    payload['parameters']['limit'] = size
    order = payload['query']['read']['entries'][0]['query']['root']['value_map']['input']['limit']['input']['order_by']
    timings = {'asc': [], 'desc': []}
    for sample in range(samples + 3):
        directions = ['asc', 'desc'] if sample % 2 == 0 else ['desc', 'asc']
        results = {}
        for direction in directions:
            order['order'] = direction
            result, elapsed = request(port, payload)
            results[direction] = normalized(result)
            assert len(results[direction]) == size
            if sample >= 3:
                timings[direction].append(elapsed)
        # Fixture IDs sort in insertion/entity-ID order. Build both orderings
        # independently so a shared tie-order bug cannot pass calibration.
        ascending = sorted(results['asc'], key=lambda row: (row['last_seen'], row['id']))
        descending = sorted(ascending, key=lambda row: -row['last_seen'])
        assert results['asc'] == ascending
        assert results['desc'] == descending
    forward = statistics.median(timings['asc'])
    reverse = statistics.median(timings['desc'])
    result = {'forward_p50_s': forward, 'reverse_p50_s': reverse,
              'extra_us_per_visited_entry': (reverse - forward) * 1e6 / (size * 5),
              'entries_per_scan': size * 5, 'samples': samples}
    print(json.dumps(result), flush=True)
    return result


def run(args):
    rows = fixture_rows(args.size, args.raw_bytes, args.tied, args.full_properties)
    if not args.no_seed:
        # Seed independently, with the same insertion order and properties.
        for port in [args.baseline, args.candidate]:
            prepare(port, rows)
    report = {'size': args.size, 'raw_bytes': args.raw_bytes, 'tied': args.tied,
              'samples': args.samples, 'full_properties': args.full_properties, 'mode': 'warm', 'workers': args.workers, 'churn': args.churn, 'cases': []}
    for path in [FIXTURES / 'ordered-range-wide-projection.json',
                 FIXTURES / 'ordered-range-narrow-projection.json']:
        for window in (['broad', 'narrow'] if args.window == 'both' else [args.window]):
            payload = json.loads(path.read_text())
            cutoff = CUTOFF if window == 'broad' else CUTOFF + len(rows) - min(100, len(rows))
            payload['parameters']['since_last_seen_us'] = cutoff
            projection = payload['query']['read']['entries'][0]['query']['root']['value_map']['properties']
            selected = [row for row in rows if row['tenant'] == TENANT and row['type'] == 'pod' and row['last_seen'] > cutoff]
            # Insertion order supplies the existing ascending internal ID tie order.
            selected.sort(key=lambda row: -row['last_seen'])
            expected = [{key: row[key] for key in projection if key in row} for row in selected[:1000]]
            measurements = {args.baseline: [], args.candidate: []}
            for sample in range(args.samples + 3):
                ports = [args.baseline, args.candidate] if sample % 2 == 0 else [args.candidate, args.baseline]
                for port in ports:
                    if args.workers == 1:
                        responses = [request(port, payload)]
                    else:
                        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
                            responses = list(pool.map(lambda _: request(port, payload), range(args.workers)))
                    for result, elapsed in responses:
                        actual = normalized(result)
                        assert actual == expected, (port, path.name, window, actual[:1], expected[:1], len(actual), len(expected))
                        if sample >= 3:
                            measurements[port].append(elapsed)
            timing = {label: {'p50_s': statistics.median(measurements[port]),
                              'p95_s': percentile(measurements[port], .95)}
                      for label, port in [('baseline', args.baseline), ('candidate', args.candidate)]}
            ratios = {metric: timing['candidate'][metric] / timing['baseline'][metric] for metric in ['p50_s', 'p95_s']}
            gate = .5 if window == 'broad' else 1.1
            case = {'query': payload['query_name'], 'window': window, **timing,
                    'candidate_baseline_ratio': ratios, 'gate_passed': all(ratio <= gate for ratio in ratios.values())}
            report['cases'].append(case)
            print(json.dumps(case), flush=True)
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2) + '\n')
    if args.require_gates and not all(case['gate_passed'] for case in report['cases']):
        raise SystemExit('Performance gate failed; inspect the saved measurements')
    return report


def churn(port, stop):
    cycles = 0
    while not stop.is_set():
        root = {'add_n': {'label': 'Resource', 'properties': [
            ['tenant', {'value': {'string': 'churn'}}],
            ['type', {'value': {'string': 'event'}}],
            ['last_seen', {'value': {'i64': CUTOFF + 999999}}],
            ['raw_data', {'value': {'string': 'x' * 4096}}]]}}
        result, _ = request(port, batch('write', [root] * 20))
        ids = [node['$id'] for value in result.values() for node in value]
        source = {'nodes': {'reference': {'ids': ids}}}
        request(port, batch('write', [{'set_property': {'input': source, 'name': 'last_seen', 'value': {'value': {'i64': CUTOFF + 999998}}}}]))
        request(port, batch('write', [{'drop': {'input': source}}]))
        cycles += 1
        stop.wait(.05)
    return {'port': port, 'insert_update_delete_cycles': cycles, 'entities_per_cycle': 20}



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=int, default=18190)
    parser.add_argument('--candidate', type=int, default=18191)
    parser.add_argument('--size', type=int, default=10000)
    parser.add_argument('--raw-bytes', type=int, default=1024)
    parser.add_argument('--samples', type=int, default=30)
    parser.add_argument('--tied', action='store_true')
    parser.add_argument('--no-seed', action='store_true')
    parser.add_argument('--output')
    parser.add_argument('--calibrate', action='store_true')
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--churn', action='store_true')
    parser.add_argument('--require-gates', action='store_true')
    parser.add_argument('--full-properties', action='store_true')
    parser.add_argument('--window', choices=['broad', 'narrow', 'both'], default='both')
    args = parser.parse_args()
    if args.calibrate:
        result = calibrate(args.candidate, args.size, args.samples)
        if args.output:
            Path(args.output).write_text(json.dumps(result, indent=2) + '\n')
    elif args.churn:
        assert args.no_seed, 'seed the paired fixtures before concurrent churn'
        stop = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            writers = [pool.submit(churn, port, stop) for port in [args.baseline, args.candidate]]
            try:
                run(args)
            finally:
                stop.set()
                for writer in writers:
                    print(json.dumps(writer.result()), flush=True)
    else:
        run(args)
