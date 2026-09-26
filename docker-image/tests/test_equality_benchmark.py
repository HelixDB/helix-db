"""The benchmark oracle must reject missing, duplicate, and foreign results."""
import unittest

import equality_benchmark as benchmark


class EqualityBenchmarkTests(unittest.TestCase):
    def test_type_union_envelope_keeps_tenant_outside_the_type_union(self):
        request = benchmark.type_union_lookup(["pod", "missing"])
        self.assertEqual(request["parameters"], {"tenant": benchmark.bench.TENANT, "type-0": "pod", "type-1": "missing"})
        source = request["query"]["read"]["entries"][0]["query"]["root"]["values"]["input"]
        terms = source["nodes_where"]["predicate"]["and"]["predicates"]
        self.assertEqual(terms[2]["eq"]["left"], {"property": "tenant"})
        self.assertEqual([term["eq"]["left"] for term in terms[1]["or"]["predicates"]], [{"property": "type"}] * 2)

    def test_oracle_selects_all_and_only_matching_fixture_rows(self):
        rows = benchmark.fixture_rows(2000, 1)
        self.assertEqual(benchmark.expected_ids(rows, "pod"), [
            "resource-00000000", "resource-00004985", "resource-00009970",
        ])
        self.assertEqual(benchmark.expected_ids(rows, "no-matching-type"), [])

    def test_oracle_ignores_output_order_but_checks_complete_rows(self):
        benchmark.check_result({"q0": [{"id": "b"}, {"id": "a"}]}, ["a", "b"])
        benchmark.check_result({"q0": []}, [])
        for rows in [[{"id": "a"}], [{"id": "a"}, {"id": "a"}],
                     [{"id": "a"}, {"id": "foreign"}],
                     [{"id": "a", "extra": 1}, {"id": "b"}]]:
            with self.subTest(rows=rows), self.assertRaises(AssertionError):
                benchmark.check_result({"q0": rows}, ["a", "b"])

    def test_envelope_keeps_deletion_boolean_parameterized(self):
        request = benchmark.lookup()
        self.assertIs(request["parameters"]["deleted"], True)
        source = request["query"]["read"]["entries"][0]["query"]["root"]["values"]["input"]
        terms = source["nodes_where"]["predicate"]["and"]["predicates"]
        self.assertEqual([term["eq"]["right"] for term in terms[1:]],
                         [{"param": name} for name in ["tenant", "type", "deleted"]])
