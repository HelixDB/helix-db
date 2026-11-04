# SQL implementation notes

The user guide for SQL, including how tables, columns and joins map onto the
graph, the supported statements, and the constructs it rejects, is
[Query HelixDB with SQL](database/helix-db/query-guides/sql.mdx), published at
<https://docs.helix-db.com/database/helix-db/query-guides/sql>. The HTTP contract
is in the [HTTP API reference](database/helix-db/query-guides/http-api.mdx#sql)
and [`openapi.json`](openapi.json).

These notes record implementation detail for contributors: the Rust and transport
APIs, the translation pipeline, its rules and resource bounds, and its tests. Keep
the guide in step when behavior changes.

## Rust and transport APIs

`helix_cypher::sql::translate(text, &mappings)` returns the same
`syntax::Statement` that `helix_cypher::parse` builds for Cypher, and
`helix_cypher::sql::compile` resolves it into the shared relational query.
`sql::Mappings` holds table → label and `table.column` → relationship type
mappings; its keys match case-insensitively.

Transports decode a body with `helix_cypher::sql::request::Request`, which accepts
`parameters` as an object of named values or as a list that binds `?` placeholders
as `$1`, `$2`, …. `Request::compile` returns the ordinary `CompiledRequest`, so
`POST /v2/sql` routes by `request_type()` and executes through
`HelixQueryService::execute_compiled_cypher_json_scoped_controlled`, exactly like
`POST /v2/cypher`: the same request options, durability, limits, cancellation,
error body and status codes apply. The compiled request's text is the translated
Cypher, which `syntax::Statement`'s `Display` prints, so telemetry records the
statement's Cypher shape with literals redacted. Translation reads no storage or
catalog, so a gateway could route SQL by effect as it routes Cypher. Only the
local server serves SQL today; gRPC, the SDKs and the CLI have no SQL methods.

## Translation pipeline

The module is a port of the SQL-to-Cypher translator in neo4j-jdbc
(`neo4j-jdbc-translator/impl`, Apache-2.0), adapted to HelixDB's Cypher profile.

1. `translate` bounds the text (16 MiB) and an estimate of sqlparser's token
   count, tokenizes with sqlparser's `GenericDialect`, bounds the binary
   operators, numbers every `?` by its offset in the text, and parses one
   statement.
2. `tables.rs` resolves FROM lists and join chains into patterns and a `Scope`
   of table variables. Lookups are hash-indexed by lowercased alias, name and
   label, so resolution stays constant time per column.
3. `expr.rs`, `select.rs` and `dml.rs` translate expressions, `SELECT` (with its
   grouping and windowing), and `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE`.
   Every node is built with `Expr::new`, so Cypher's expression depth limit
   applies, and its span comes from the SQL node, converted from sqlparser's
   1-based character locations to byte offsets.
4. The binder, planner and executor take the statement as they take parsed
   Cypher, and report their errors with the SQL spans.

`print.rs` is the inverse of the Cypher parser, up to spans and redundant
parentheses: printed text parses back to the same tree. Comparisons of
translated expressions, as in grouping, use the printed text, which is canonical.

## Translation rules

Joins follow neo4j-jdbc's syntactic rules: a two-table `ON` names the type after
its second column and points away from that column's table, a three-table chain
makes the middle table the relationship, longer chains continue in pairs, and a
`Start_TYPE_End` table is a relationship table. A generated relationship
variable is unique among the statement's table variables and resolves only by
qualifier.

Where neo4j-jdbc's output differs from SQL, the translation follows SQL, and the
golden corpus marks each case with a `divergence:` comment:

- `SUBSTRING` counts from 1; literal starts below 1 fold into the length.
- `(a, b) <> (c, d)` holds when any field differs.
- `||` and `CONCAT` join strings; `CONCAT` skips nulls as PostgreSQL does.
- `SUM` of no values is null: `CASE count(x) WHEN 0 THEN NULL ELSE sum(x) END`.
- A selected column must be grouped or aggregated. `GROUP BY` without an
  aggregate deduplicates, an aggregate without `GROUP BY` keeps constants out of
  its grouping keys so an empty input returns one row, and a grouping that
  Cypher's implicit grouping would get wrong goes through `WITH`.
- `UPDATE` computes every new value in a `WITH` before assigning when several
  assignments read the row, so `SET a = b, b = a` swaps.
- An `INSERT` into a relationship table matches its endpoints instead of
  merging them, since the Cypher profile has no `MERGE`.
- An unparenthesized `XOR` operand of `AND`, `NOT` or a comparison is rejected,
  because sqlparser binds `XOR` tighter than they do.

Without `DISTINCT` or grouping, `ORDER BY`, `OFFSET` and `LIMIT` apply in a
`WITH` of the table variables before `RETURN` reads columns. Reading a column can
fail, so this is the form the executor can stop early, or read a range index in
order, for.

## Limits and resource bounds

sqlparser materializes every token, one per whitespace character, and builds
operator chains left-deep, then walks and drops them recursively. Translation
therefore enforces, before or while it builds anything large:

| Bound | Value | Error |
| --- | --- | --- |
| Text | 16 MiB | `resource_limit` / `query_too_large` |
| Token estimate | 500,000 | `resource_limit` / `too_many_tokens` |
| Binary operators | 1,024 | `resource_limit` / `expression_depth` |
| Translation nesting | 96 levels | `resource_limit` / `expression_depth` |
| Copied expressions | 16 MiB of weight: 64 per node plus its Cypher text | `resource_limit` / `query_too_large` |
| Text printed to compare grouped expressions | 64 MiB | `resource_limit` / `query_too_large` |

The token estimate never undercounts sqlparser: it ignores quoting and comments,
counting every character except within runs of letters and digits, which
sqlparser only splits where digits start or stop. The operator count includes
every keyword other than literals and clause keywords, and every sign after a
word, so no infix chain goes uncounted. The copy budget covers the SQL forms that
repeat an operand, such as `BETWEEN`, `NULLIF` and alias references, which could
otherwise grow the tree exponentially, and repeated references to one long
literal, which would otherwise copy it each time. Grouping compares expressions
by their printed Cypher through hash sets; comparing each level of a deep
expression reprints its inner parts, so that printing has its own budget. Scope
and mapping lookups use hash maps, and span conversion takes constant time per
location, so cost stays linear in the statement's size.

## Tests

- `crates/cypher/tests/sql/translations.txt` is the golden corpus, ported from
  neo4j-jdbc's tests. Each case's translation must print as the expected Cypher,
  reparse to the same text, and compile unless the case expects a rejection.
- `crates/cypher/src/sql/tests.rs` covers the rules, errors, spans, placeholders
  and resource bounds, including property tests for the token estimate and span
  conversion, and the largest accepted inputs on a 2 MiB stack.
- `crates/cypher/src/print/tests.rs` property-tests that printing round-trips.
- `crates/db/tests/cypher/sql.rs` runs SQL through the query service against a
  database, and `crates/server/src/cypher_tests.rs` covers the HTTP route.
- `docker-image/tests/sql_contracts.py` checks `/v2/sql` in the server image,
  across a restart.
