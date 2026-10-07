//! Request shapes for measuring how native requests parse, drop and plan.
//!
//! Every shape is the JSON body a transport would receive, built through the
//! public builders so it stays a valid request as the AST evolves. Sizes are
//! chosen so every shape fits the server's 16 MiB body cap.
//!
//! ```
//! use helix_ast::{query::QueryRequest, testing};
//!
//! let shape = testing::deep_chain(16);
//! assert_eq!(shape.name, "deep_chain/16");
//! assert!(QueryRequest::from_json_slice(&shape.json).is_ok());
//! ```

use crate::batch::{read_batch, write_batch};
use crate::expr::Predicate;
use crate::graph::NodeRef;
use crate::projection::Projection;
use crate::query::{QueryParamType, QueryRequest, QueryValue};
use crate::traversal::{g, Order};
use crate::value::PropertyInput;

/// One request body to measure.
#[derive(Debug, Clone, PartialEq)]
pub struct Shape {
    /// Stable name used as the benchmark argument.
    pub name: String,
    /// The JSON body exactly as a transport receives it.
    pub json: Vec<u8>,
}

impl Shape {
    fn new(name: impl Into<String>, request: &QueryRequest) -> Self {
        Self {
            name: name.into(),
            json: request
                .to_json_bytes()
                .expect("built requests always serialize"),
        }
    }
}

/// The checked-in Docker smoke-test request bodies, which the SDKs produce.
pub fn fixtures() -> Vec<Shape> {
    [
        (
            "dynamic-read",
            include_bytes!("../../../docker-image/tests/fixtures/dynamic-read.json").as_slice(),
        ),
        (
            "dynamic-write",
            include_bytes!("../../../docker-image/tests/fixtures/dynamic-write.json").as_slice(),
        ),
        (
            "dynamic-delete",
            include_bytes!("../../../docker-image/tests/fixtures/dynamic-delete.json").as_slice(),
        ),
        (
            "ordered-range-narrow-projection",
            include_bytes!(
                "../../../docker-image/tests/fixtures/ordered-range-narrow-projection.json"
            )
            .as_slice(),
        ),
        (
            "ordered-range-wide-projection",
            include_bytes!(
                "../../../docker-image/tests/fixtures/ordered-range-wide-projection.json"
            )
            .as_slice(),
        ),
    ]
    .into_iter()
    .map(|(name, json)| Shape {
        name: name.to_owned(),
        json: json.to_vec(),
    })
    .collect()
}

/// A single traversal of `steps` chained operations. Each step nests two
/// JSON levels, so the deepest accepted chain is [`MAX_DEEP_CHAIN_STEPS`].
pub fn deep_chain(steps: usize) -> Shape {
    let chain = (0..steps).fold(g().n(NodeRef::all()), |traversal, step| match step % 4 {
        0 => traversal.out(Some("FOLLOWS")),
        1 => traversal.has_label("User"),
        2 => traversal.dedup(),
        _ => traversal.limit(1_000_usize),
    });
    Shape::new(
        format!("deep_chain/{steps}"),
        &QueryRequest::read(read_batch().var_as("chain", chain).returning(["chain"])),
    )
}

/// The most chained steps a JSON request can carry: the request envelope and
/// the source node take the remaining levels below
/// [`crate::query::MAX_REQUEST_JSON_DEPTH`].
pub const MAX_DEEP_CHAIN_STEPS: usize = 123;

/// `entries` independent point lookups in one read batch, all returned.
pub fn wide_batch(entries: usize) -> Shape {
    let batch = (0..entries).fold(read_batch(), |batch, entry| {
        batch.var_as(
            &format!("user_{entry}"),
            g().n_with_label("User")
                .where_(Predicate::eq("username", format!("user-{entry}")))
                .limit(1_usize)
                .value_map(Some(vec!["$id", "username", "email"])),
        )
    });
    let returns = (0..entries).map(|entry| format!("user_{entry}"));
    Shape::new(
        format!("wide_batch/{entries}"),
        &QueryRequest::read(batch.returning(returns)),
    )
}

/// One filter whose predicate is an OR of AND groups with `leaves` leaf
/// comparisons in total, as generated filter UIs produce.
pub fn predicate_heavy(leaves: usize) -> Shape {
    const GROUP: usize = 4;
    let groups = (0..leaves.div_ceil(GROUP))
        .map(|group| {
            Predicate::and(
                (group * GROUP..leaves.min(group * GROUP + GROUP))
                    .map(|leaf| match leaf % 3 {
                        0 => Predicate::eq(format!("tag_{leaf}"), format!("value-{leaf}")),
                        1 => Predicate::gt(format!("score_{leaf}"), leaf as i64),
                        _ => Predicate::is_in(
                            format!("status_{leaf}"),
                            vec!["active".to_owned(), "pending".to_owned()],
                        ),
                    })
                    .collect(),
            )
        })
        .collect();
    let traversal = g()
        .n_with_label("Item")
        .where_(Predicate::or(groups))
        .limit(100_usize)
        .id();
    Shape::new(
        format!("predicate_heavy/{leaves}"),
        &QueryRequest::read(read_batch().var_as("items", traversal).returning(["items"])),
    )
}

/// A projection of `columns` renamed properties whose aliases are
/// `alias_len` bytes long, so strings dominate the request.
pub fn string_heavy_projection(columns: usize, alias_len: usize) -> Shape {
    let projections = (0..columns)
        .map(|column| {
            let alias = format!("{column}_{}", "a".repeat(alias_len));
            Projection::property(format!("property_{column}"), alias)
        })
        .collect();
    let traversal = g()
        .n_with_label("Resource")
        .order_by("created_at", Order::Desc)
        .limit(1_000_usize)
        .project(projections);
    Shape::new(
        format!("string_heavy_projection/{columns}x{alias_len}"),
        &QueryRequest::read(read_batch().var_as("rows", traversal).returning(["rows"])),
    )
}

fn bulk_rows(rows: usize, dimensions: usize) -> QueryValue {
    // A fixed-seed LCG with nine significant digits matches what embedding
    // clients send without depending on a random-number crate.
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let unit = (state >> 11) as f64 / (1_u64 << 53) as f64;
        ((unit * 2.0 - 1.0) * 1e9).round() / 1e9
    };
    QueryValue::Array(
        (0..rows)
            .map(|row| {
                QueryValue::Object(
                    [
                        (
                            "title".to_owned(),
                            QueryValue::String(format!("document {row}")),
                        ),
                        (
                            "embedding".to_owned(),
                            QueryValue::Array(
                                (0..dimensions).map(|_| QueryValue::F64(next())).collect(),
                            ),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                )
            })
            .collect(),
    )
}

fn bulk_write_request() -> QueryRequest {
    QueryRequest::write(write_batch().for_each_param(
        "rows",
        write_batch().var_as(
            "doc",
            g().add_n(
                "Doc",
                vec![
                    ("title", PropertyInput::param("title")),
                    ("embedding", PropertyInput::param("embedding")),
                ],
            ),
        ),
    ))
}

/// A bulk insert of `rows` documents with `dimensions`-float embeddings,
/// sent as untyped parameters.
pub fn bulk_write_untyped(rows: usize, dimensions: usize) -> Shape {
    Shape::new(
        format!("bulk_write_untyped/{rows}x{dimensions}"),
        &bulk_write_request().with_parameter_value("rows", bulk_rows(rows, dimensions)),
    )
}

/// The same bulk insert as [`bulk_write_untyped`], with a declared parameter
/// schema so typed normalization runs over every row.
pub fn bulk_write_typed(rows: usize, dimensions: usize) -> Shape {
    let request = bulk_write_request()
        .with_typed_parameter(
            "rows",
            QueryParamType::Array(Box::new(QueryParamType::Object)),
            bulk_rows(rows, dimensions),
        )
        .expect("bulk rows match their declared schema");
    Shape::new(format!("bulk_write_typed/{rows}x{dimensions}"), &request)
}

/// A count query that also carries about `bytes` of parameters it never
/// reads, isolating what parameters cost the stages that copy them.
pub fn count_with_unused_params(bytes: usize) -> Shape {
    // Each float renders to about twelve bytes.
    let floats = bytes / 12;
    let request = QueryRequest::read(
        read_batch()
            .var_as("users", g().n_with_label("User").count())
            .returning(["users"]),
    );
    let request = match floats {
        0 => request,
        floats => request.with_parameter_value(
            "unused",
            QueryValue::Array(
                (0..floats)
                    .map(|float| QueryValue::F64(float as f64 / 7.0))
                    .collect(),
            ),
        ),
    };
    Shape::new(format!("count_with_unused_params/{bytes}"), &request)
}

/// Whether `bytes` passes the flat nesting pre-scan every JSON entry point
/// runs before parsing, exposed so benchmarks can time that stage alone.
///
/// ```
/// use helix_ast::testing;
///
/// assert!(testing::json_depth_within_limit(&testing::deep_chain(16).json));
/// assert!(!testing::json_depth_within_limit(&[b'['; 300]));
/// ```
pub fn json_depth_within_limit(bytes: &[u8]) -> bool {
    crate::query::check_json_depth::<sonic_rs::Error>(bytes).is_ok()
}

/// Every shape the benchmarks measure, smallest first within each family.
pub fn all() -> Vec<Shape> {
    fixtures()
        .into_iter()
        .chain([16, 64, MAX_DEEP_CHAIN_STEPS].map(deep_chain))
        .chain([100, 1_000].map(wide_batch))
        .chain([64, 1_024].map(predicate_heavy))
        .chain([string_heavy_projection(256, 64)])
        .chain(
            [(100, 768), (1_000, 768), (10_000, 96)]
                .map(|(rows, dimensions)| bulk_write_untyped(rows, dimensions)),
        )
        .chain(
            [(100, 768), (1_000, 768)].map(|(rows, dimensions)| bulk_write_typed(rows, dimensions)),
        )
        .chain([0, 1 << 20, 8 << 20].map(count_with_unused_params))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::MAX_REQUEST_JSON_DEPTH;

    /// The server's request body cap (`server::MAX_QUERY_BODY_BYTES`).
    const MAX_QUERY_BODY_BYTES: usize = 16 * 1024 * 1024;

    /// Unoptimized builds need several times the stack an optimized build
    /// does to build, serialize, parse and drop the deepest chain (an
    /// optimized parse needs about 512 KiB), so debug test builds handle the
    /// shapes on a larger stack.
    fn on_large_stack(test: impl FnOnce() + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(32 << 20)
            .spawn(test)
            .expect("test thread spawns")
            .join()
            .expect("test passes");
    }

    #[test]
    fn every_shape_is_a_valid_request_within_the_body_cap() {
        on_large_stack(|| {
            for shape in all() {
                assert!(
                    shape.json.len() <= MAX_QUERY_BODY_BYTES,
                    "{} is {} bytes",
                    shape.name,
                    shape.json.len()
                );
                let request = QueryRequest::from_json_slice(&shape.json)
                    .unwrap_or_else(|error| panic!("{} must parse: {error}", shape.name));
                assert!(
                    request.check_nesting().is_ok(),
                    "{} nests too deeply",
                    shape.name
                );
            }
        });
    }

    #[test]
    fn deep_chain_maximum_is_the_json_depth_limit() {
        on_large_stack(|| {
            assert!(QueryRequest::from_json_slice(&deep_chain(MAX_DEEP_CHAIN_STEPS).json).is_ok());
            let error = QueryRequest::from_json_slice(&deep_chain(MAX_DEEP_CHAIN_STEPS + 1).json)
                .expect_err("one more step exceeds the depth limit");
            assert!(
                error
                    .to_string()
                    .contains(&MAX_REQUEST_JSON_DEPTH.to_string()),
                "{error}"
            );
        });
    }

    #[cfg(feature = "simd-json")]
    #[test]
    fn simd_json_parses_every_shape_to_the_sonic_tree() {
        on_large_stack(|| {
            for shape in all() {
                let sonic = QueryRequest::from_json_slice(&shape.json)
                    .unwrap_or_else(|error| panic!("{} must parse: {error}", shape.name));
                let mut body = shape.json.clone();
                let simd = QueryRequest::from_json_slice_mut(&mut body)
                    .unwrap_or_else(|error| panic!("{} must parse: {error}", shape.name));
                assert!(sonic == simd, "{} parses differently", shape.name);
            }
        });
    }

    #[test]
    fn shape_names_are_unique() {
        on_large_stack(|| {
            let shapes = all();
            let names = shapes
                .iter()
                .map(|shape| shape.name.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(names.len(), shapes.len());
        });
    }
}
