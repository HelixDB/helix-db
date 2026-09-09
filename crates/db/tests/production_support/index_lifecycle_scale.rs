//! Fixed-shape production lifecycle scale orchestration.
//!
//! Authoritative graph rows are seeded with the deployed typed key/value
//! codecs so setup does not spend 100,000 interpreter transactions. Every
//! index operation after setup crosses the public physical-plan interpreter,
//! durable outbox, supervised worker, catalog refresh, and public indexed-read
//! boundary. The test uses only current f32 vectors; deferred f16 and binary
//! codecs are neither activated nor persisted here.

use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Bound;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use slatedb::IsolationLevel;

use crate::config::{
    DbConfig, SearchIndexBackfillLimits, SearchIndexBatchLimits, SecondaryIndexLifecycleTuning,
    TextElementType, VectorElementType, VectorIndexDefinition,
};
use crate::encoding::property::property_value::PropertyValue as StoredPropertyValue;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::{AdjacencyKey, DataKey, DataKeyKind, NodePropertyKey};
use crate::encoding::v2::values::adjacency::{encode_edges, Edges};
use crate::execution::interpreter::{ExecutionResult, ExecutionScalar, ExecutionValue};
use crate::index_lifecycle::{
    IndexDdlReceipt, IndexOperationStage, IndexOperationStatus, IndexOperationStatusCommon,
    PublicIndexFamily, ValidatedDynamicIndexDefinition,
};
use crate::search::{text_index_name, vector_index_name};
use crate::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::value::PropertyValue as PlannerPropertyValue;
use helix_planner::{catalog, context, cost, exec, ir, properties, trace};

const ENTITY_COUNT: usize = 100_000;
const CI_VECTOR_ENTITY_COUNT: usize = 8_000;
const LIMIT_ENTITY_COUNT: usize = 512;
const TENANT_COUNT: usize = 16;
const VECTOR_DIMENSION: usize = 128;
const TRAVERSAL_VECTOR_DIMENSION: usize = 1_536;
const TRAVERSAL_ENTITY_COUNT: usize = 50_000;
const TRAVERSAL_PREFILTER_COUNT: usize = 1_000;
const TRAVERSAL_1M_ENTITY_COUNT: usize = 1_000_000;
const SEED_BATCH_ROWS: usize = 512;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);
const TRAVERSAL_OPERATION_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);
const TRAVERSAL_1M_OPERATION_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const DBPEDIA_FBIN_ENV: &str = "HELIX_DBPEDIA_FBIN";
const DBPEDIA_1M_FBIN_ENV: &str = "HELIX_DBPEDIA_1M_FBIN";
const DBPEDIA_1M_DB_PARENT_ENV: &str = "HELIX_DBPEDIA_1M_DB_PARENT";
const LABEL: &str = "ScaleDocument";
const NON_UNIQUE_PROPERTY: &str = "group";
const UNIQUE_PROPERTY: &str = "external_id";
const VECTOR_PROPERTY: &str = "embedding";
const TEXT_PROPERTY: &str = "body";

/// Constructs a validated planner identifier used by scale fixtures.
fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("scale fixture identifiers are non-empty")
}

/// Constructs one executable step with neutral scheduling metadata.
fn step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("scale fixture step ids are positive"),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

/// Seals a fixture DAG behind the production executable-plan validator.
fn executable(kind: ir::PlanKind, steps: Vec<exec::ExecStep>, root: usize) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("scale fixture plans are non-empty"),
        exec::ExecStepId::new(root).expect("scale fixture root ids are positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("scale fixture dependencies form a valid executable plan")
}

/// Builds one public CREATE plan for an already validated family definition.
fn create_plan(spec: ir::IndexDdlCreateSpec) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Create {
                    spec,
                    mode: ir::IndexCreateMode::ErrorIfExists,
                },
            },
        )],
        1,
    )
}

/// Builds one public DROP plan for an active family definition.
fn drop_plan(spec: ir::IndexDdlDropSpec) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Drop { spec },
            },
        )],
        1,
    )
}

/// Builds one node equality lookup followed by an ID projection.
fn equality_search_plan(
    property: &str,
    value: PlannerPropertyValue,
    uniqueness: catalog::IndexUniqueness,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("scale access id is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::exact_equality(
                            catalog::NodeEqualityIndexMeta::new(name(&format!(
                                "node_eq:{LABEL}:{property}"
                            )))
                            .with_uniqueness(uniqueness),
                            catalog::ScopedPropertyKey::try_new(LABEL, property)
                                .expect("scale equality key is valid"),
                            ir::IndexValue::Literal(
                                ir::SecondaryIndexLiteral::new(value)
                                    .expect("scale equality literal is indexable"),
                            ),
                        ),
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        2,
    )
}

/// Builds one top-one 128D vector lookup followed by an ID projection.
fn vector_search_plan(query: Vec<f32>) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("scale access id is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::VectorSearch {
                            key: catalog::NodeSearchIndexKey::try_new(LABEL, VECTOR_PROPERTY)
                                .expect("scale vector key is valid"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&vector_index_name(
                                    VectorElementType::Node,
                                    LABEL,
                                    VECTOR_PROPERTY,
                                )),
                                tenant: ir::SearchTenantPlan::Unscoped,
                            },
                            query_vector: ir::VectorQueryInputPlan::Vector(
                                ir::SearchVector::new(query)
                                    .expect("scale query vector is non-empty and finite"),
                            ),
                            k: ir::SearchLimitPlan::Literal(NonZeroUsize::MIN),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        2,
    )
}

/// Builds an indexed node prefilter followed by one outgoing graph hop.
fn traversal_prefilter_plan(group: &str) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("scale access id is positive");
    let expand_id = exec::ExecStepId::new(2).expect("scale expand id is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::exact_equality(
                            catalog::NodeEqualityIndexMeta::new(name(&format!(
                                "node_eq:{LABEL}:{NON_UNIQUE_PROPERTY}"
                            ))),
                            catalog::ScopedPropertyKey::try_new(LABEL, NON_UNIQUE_PROPERTY)
                                .expect("traversal equality key is valid"),
                            ir::IndexValue::Literal(
                                ir::SecondaryIndexLiteral::new(PlannerPropertyValue::String(
                                    group.to_string(),
                                ))
                                .expect("traversal equality literal is indexable"),
                            ),
                        ),
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Expand {
                    plan: ir::ExpandPlan {
                        direction: ir::ExpandDirection::Out,
                        output: ir::ExpandOutput::Nodes,
                        label: ir::ExpandLabelPlan::Any,
                    },
                },
            ),
            step(
                3,
                vec![expand_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        3,
    )
}

/// Builds indexed prefilter, one-hop traversal, and downstream vector ranking.
fn traversal_prefilter_vector_plan(group: &str, query: Vec<f32>) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("scale access id is positive");
    let expand_id = exec::ExecStepId::new(2).expect("scale expand id is positive");
    let vector_id = exec::ExecStepId::new(3).expect("scale vector id is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::exact_equality(
                            catalog::NodeEqualityIndexMeta::new(name(&format!(
                                "node_eq:{LABEL}:{NON_UNIQUE_PROPERTY}"
                            ))),
                            catalog::ScopedPropertyKey::try_new(LABEL, NON_UNIQUE_PROPERTY)
                                .expect("traversal equality key is valid"),
                            ir::IndexValue::Literal(
                                ir::SecondaryIndexLiteral::new(PlannerPropertyValue::String(
                                    group.to_string(),
                                ))
                                .expect("traversal equality literal is indexable"),
                            ),
                        ),
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Expand {
                    plan: ir::ExpandPlan {
                        direction: ir::ExpandDirection::Out,
                        output: ir::ExpandOutput::Nodes,
                        label: ir::ExpandLabelPlan::Any,
                    },
                },
            ),
            step(
                3,
                vec![expand_id],
                exec::ExecOp::VectorSearch {
                    plan: Box::new(ir::RestrictedVectorSearchPlan::Nodes {
                        key: catalog::NodeSearchIndexKey::try_new(LABEL, VECTOR_PROPERTY)
                            .expect("traversal vector key is valid"),
                        index: ir::SearchIndexPlan {
                            index_id: name(&vector_index_name(
                                VectorElementType::Node,
                                LABEL,
                                VECTOR_PROPERTY,
                            )),
                            tenant: ir::SearchTenantPlan::Unscoped,
                        },
                        query_vector: ir::VectorQueryInputPlan::Vector(
                            ir::SearchVector::new(query)
                                .expect("traversal query vector is non-empty and finite"),
                        ),
                        k: ir::SearchLimitPlan::Literal(
                            NonZeroUsize::new(10).expect("traversal vector limit is positive"),
                        ),
                    }),
                },
            ),
            step(
                4,
                vec![vector_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        4,
    )
}

/// Builds one top-ten text lookup followed by an ID projection.
fn text_search_plan(query: &str) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("scale access id is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::TextSearch {
                            key: catalog::NodeSearchIndexKey::try_new(LABEL, TEXT_PROPERTY)
                                .expect("scale text key is valid"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&text_index_name(
                                    TextElementType::Node,
                                    LABEL,
                                    TEXT_PROPERTY,
                                )),
                                tenant: ir::SearchTenantPlan::Unscoped,
                            },
                            query_text: ir::TextQueryInputPlan::Text(name(query)),
                            k: ir::SearchLimitPlan::Literal(
                                NonZeroUsize::new(10).expect("scale text limit is positive"),
                            ),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        2,
    )
}

/// Converts an indexed-read projection without accepting mixed scalar kinds.
fn projected_node_ids(result: ExecutionResult) -> Vec<u64> {
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("scale indexed read should return projected scalars");
    };
    values
        .into_iter()
        .map(|value| {
            let ExecutionScalar::NodeId(id) = value else {
                panic!("scale node projection should contain only node ids");
            };
            id
        })
        .collect()
}

/// Returns a source key in exactly one typed data scope.
fn source_key(scope: DataScope, entity_id: u64) -> bytes::Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
    }
    .to_bytes()
}

/// Returns one typed adjacency key in exactly one data scope.
fn adjacency_key(scope: DataScope, entity_id: u64) -> bytes::Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::Adjacency(AdjacencyKey::new(entity_id)),
    }
    .to_bytes()
}

/// Returns the deterministic 128D source vector for one entity.
fn vector(entity_id: u64) -> Vec<f32> {
    let mut state = entity_id.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut vector = Vec::with_capacity(VECTOR_DIMENSION);
    for _ in 0..VECTOR_DIMENSION {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let centered = i32::from((state & 0xffff) as u16) - 32_768;
        vector.push(centered as f32 / 32_768.0);
    }
    vector
}

/// One validated fixed-shape DBpedia vector fixture.
struct TraversalVectors {
    values: Vec<f32>,
}

impl TraversalVectors {
    /// Loads the standard little-endian fbin header followed by native f32 rows.
    fn load() -> Self {
        const HEADER_FIELD_LEN: usize = core::mem::size_of::<u32>();
        const ELEMENT_LEN: usize = core::mem::size_of::<f32>();

        let path = std::env::var_os(DBPEDIA_FBIN_ENV)
            .unwrap_or_else(|| panic!("{DBPEDIA_FBIN_ENV} must point to the 50k DBpedia fbin"));
        let bytes = std::fs::read(&path).expect("DBpedia fbin remains readable");
        let count = u32::from_le_bytes(
            bytes[..HEADER_FIELD_LEN]
                .try_into()
                .expect("DBpedia fbin count header is four bytes"),
        );
        let dimension = u32::from_le_bytes(
            bytes[HEADER_FIELD_LEN..HEADER_FIELD_LEN + HEADER_FIELD_LEN]
                .try_into()
                .expect("DBpedia fbin dimension header is four bytes"),
        );
        assert_eq!(
            usize::try_from(count).expect("DBpedia row count fits usize"),
            TRAVERSAL_ENTITY_COUNT
        );
        assert_eq!(
            usize::try_from(dimension).expect("DBpedia dimension fits usize"),
            TRAVERSAL_VECTOR_DIMENSION
        );
        let value_count = TRAVERSAL_ENTITY_COUNT
            .checked_mul(TRAVERSAL_VECTOR_DIMENSION)
            .expect("DBpedia fixture shape fits usize");
        let expected_len = HEADER_FIELD_LEN
            .checked_add(HEADER_FIELD_LEN)
            .and_then(|header_len| {
                value_count
                    .checked_mul(ELEMENT_LEN)
                    .and_then(|values_len| header_len.checked_add(values_len))
            })
            .expect("DBpedia fixture byte length fits usize");
        assert_eq!(bytes.len(), expected_len);
        let values = bytes[HEADER_FIELD_LEN + HEADER_FIELD_LEN..]
            .as_chunks::<ELEMENT_LEN>()
            .0
            .iter()
            .map(|value| f32::from_le_bytes(*value))
            .collect::<Vec<_>>();
        assert!(values.iter().all(|value| value.is_finite()));
        Self { values }
    }

    /// Returns one vector after validated entity-to-row conversion.
    fn get(&self, entity_id: u64) -> &[f32] {
        let entity_id = usize::try_from(entity_id).expect("DBpedia entity ID fits usize");
        let start = entity_id
            .checked_mul(TRAVERSAL_VECTOR_DIMENSION)
            .expect("DBpedia vector offset fits usize");
        &self.values[start..start + TRAVERSAL_VECTOR_DIMENSION]
    }
}

/// One memory-mapped fbin fixture that does not duplicate its vector payload.
struct MappedTraversalVectors {
    values: memmap2::Mmap,
}

impl MappedTraversalVectors {
    const HEADER_FIELD_LEN: usize = core::mem::size_of::<u32>();
    const ELEMENT_LEN: usize = core::mem::size_of::<f32>();

    /// Maps and validates one fixed 1M by 1536 little-endian fbin fixture.
    fn load_1m() -> Self {
        let path = std::env::var_os(DBPEDIA_1M_FBIN_ENV)
            .unwrap_or_else(|| panic!("{DBPEDIA_1M_FBIN_ENV} must point to the 1M DBpedia fbin"));
        let file = std::fs::File::open(&path).expect("1M DBpedia fbin remains readable");
        // SAFETY: the benchmark treats its pinned input fixture as immutable
        // for the complete lifetime of this read-only mapping.
        let values = unsafe { memmap2::MmapOptions::new().map(&file) }
            .expect("1M DBpedia fbin maps read-only");
        let count = u32::from_le_bytes(
            values[..Self::HEADER_FIELD_LEN]
                .try_into()
                .expect("DBpedia fbin count header is four bytes"),
        );
        let dimension = u32::from_le_bytes(
            values[Self::HEADER_FIELD_LEN..Self::HEADER_FIELD_LEN + Self::HEADER_FIELD_LEN]
                .try_into()
                .expect("DBpedia fbin dimension header is four bytes"),
        );
        assert_eq!(
            usize::try_from(count).expect("DBpedia row count fits usize"),
            TRAVERSAL_1M_ENTITY_COUNT
        );
        assert_eq!(
            usize::try_from(dimension).expect("DBpedia dimension fits usize"),
            TRAVERSAL_VECTOR_DIMENSION
        );
        let expected_len = TRAVERSAL_1M_ENTITY_COUNT
            .checked_mul(TRAVERSAL_VECTOR_DIMENSION)
            .and_then(|value_count| value_count.checked_mul(Self::ELEMENT_LEN))
            .and_then(|value_bytes| {
                Self::HEADER_FIELD_LEN
                    .checked_add(Self::HEADER_FIELD_LEN)
                    .and_then(|header_bytes| header_bytes.checked_add(value_bytes))
            })
            .expect("1M DBpedia fixture byte length fits usize");
        assert_eq!(values.len(), expected_len);
        Self { values }
    }

    /// Decodes one owned row for database insertion or query execution.
    fn get(&self, entity_id: u64) -> Vec<f32> {
        let row = self.row(entity_id);
        let values = row
            .as_chunks::<{ Self::ELEMENT_LEN }>()
            .0
            .iter()
            .map(|value| f32::from_le_bytes(*value))
            .collect::<Vec<_>>();
        assert!(values.iter().all(|value| value.is_finite()));
        values
    }

    /// Computes the exact squared Euclidean oracle without allocating a row.
    fn squared_euclidean(&self, entity_id: u64, query: &[f32]) -> f32 {
        assert_eq!(query.len(), TRAVERSAL_VECTOR_DIMENSION);
        self.row(entity_id)
            .as_chunks::<{ Self::ELEMENT_LEN }>()
            .0
            .iter()
            .zip(query)
            .map(|(candidate, query)| {
                let candidate = f32::from_le_bytes(*candidate);
                (candidate - query).powi(2)
            })
            .sum()
    }

    /// Returns one validated byte row using explicit fbin header offsets.
    fn row(&self, entity_id: u64) -> &[u8] {
        let entity_id = usize::try_from(entity_id).expect("DBpedia entity ID fits usize");
        assert!(entity_id < TRAVERSAL_1M_ENTITY_COUNT);
        let row_len = TRAVERSAL_VECTOR_DIMENSION
            .checked_mul(Self::ELEMENT_LEN)
            .expect("DBpedia row byte length fits usize");
        let start = Self::HEADER_FIELD_LEN
            .checked_add(Self::HEADER_FIELD_LEN)
            .and_then(|header_len| {
                entity_id
                    .checked_mul(row_len)
                    .and_then(|row_offset| header_len.checked_add(row_offset))
            })
            .expect("DBpedia row offset fits usize");
        &self.values[start..start + row_len]
    }
}

/// One equality-index selectivity used by the million-row traversal benchmark.
#[derive(Debug, Clone, Copy)]
struct Traversal1MShape {
    group: &'static str,
    source_start: usize,
    candidate_count: usize,
}

impl Traversal1MShape {
    const ALL: [Self; 4] = [
        Self {
            group: "prefilter-100",
            source_start: 0,
            candidate_count: 100,
        },
        Self {
            group: "prefilter-1000",
            source_start: 100,
            candidate_count: 1_000,
        },
        Self {
            group: "prefilter-10000",
            source_start: 1_100,
            candidate_count: 10_000,
        },
        Self {
            group: "prefilter-100000",
            source_start: 11_100,
            candidate_count: 100_000,
        },
    ];

    fn source_end(self) -> usize {
        self.source_start.saturating_add(self.candidate_count)
    }

    fn candidate_start(self) -> usize {
        self.source_start
            .saturating_add(TRAVERSAL_1M_ENTITY_COUNT / 2)
    }

    fn candidate_end(self) -> usize {
        self.candidate_start().saturating_add(self.candidate_count)
    }
}

/// Returns one complete benchmark row with exact indexed-prefilter selectivity.
fn traversal_properties(entity_id: u64, vector: &[f32]) -> Vec<Property> {
    let group = if entity_id
        < u64::try_from(TRAVERSAL_PREFILTER_COUNT).expect("prefilter count fits u64")
    {
        "vector-candidate"
    } else {
        "other"
    };
    vec![
        Property::new("$label", StoredPropertyValue::String(LABEL.to_string())),
        Property::new(
            NON_UNIQUE_PROPERTY,
            StoredPropertyValue::String(group.to_string()),
        ),
        Property::new(
            VECTOR_PROPERTY,
            StoredPropertyValue::F32Array(vector.to_vec()),
        ),
    ]
}

/// Returns one million-row benchmark row with four disjoint indexed groups.
fn traversal_1m_properties(entity_id: u64, vector: Vec<f32>) -> Vec<Property> {
    let entity_id = usize::try_from(entity_id).expect("DBpedia entity ID fits usize");
    let group = Traversal1MShape::ALL
        .into_iter()
        .find(|shape| (shape.source_start..shape.source_end()).contains(&entity_id))
        .map_or("other", |shape| shape.group);
    vec![
        Property::new("$label", StoredPropertyValue::String(LABEL.to_string())),
        Property::new(
            NON_UNIQUE_PROPERTY,
            StoredPropertyValue::String(group.to_string()),
        ),
        Property::new(VECTOR_PROPERTY, StoredPropertyValue::F32Array(vector)),
    ]
}

/// Returns the complete authoritative property row for one unscoped entity.
fn unscoped_properties(entity_id: u64) -> Vec<Property> {
    let group = if entity_id < 2 {
        "shared-target".to_string()
    } else {
        format!("group-{entity_id}")
    };
    let body = if entity_id == 0 {
        "common scale text uniqueneedle".to_string()
    } else {
        format!("common scale text bucket{}", entity_id % 1_000)
    };
    vec![
        Property::new("$label", StoredPropertyValue::String(LABEL.to_string())),
        Property::new(NON_UNIQUE_PROPERTY, StoredPropertyValue::String(group)),
        Property::new(
            UNIQUE_PROPERTY,
            StoredPropertyValue::String(format!("external-{entity_id}")),
        ),
        Property::new(
            VECTOR_PROPERTY,
            StoredPropertyValue::F32Array(vector(entity_id)),
        ),
        Property::new(TEXT_PROPERTY, StoredPropertyValue::String(body)),
    ]
}

/// Returns the authoritative property row for one tenant-scale entity.
fn tenant_properties(entity_id: u64) -> Vec<Property> {
    vec![
        Property::new("$label", StoredPropertyValue::String(LABEL.to_string())),
        Property::new(
            NON_UNIQUE_PROPERTY,
            StoredPropertyValue::String(format!("tenant-group-{}", entity_id % 256)),
        ),
    ]
}

/// Seeds one scope in bounded transactions using only canonical graph codecs.
async fn seed_scope(
    db: &slatedb::Db,
    scope: DataScope,
    start_id: usize,
    entity_count: usize,
    properties: fn(u64) -> Vec<Property>,
) {
    for batch_start in (0..entity_count).step_by(SEED_BATCH_ROWS) {
        let batch_end = entity_count.min(batch_start.saturating_add(SEED_BATCH_ROWS));
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("scale seed transaction opens");
        for offset in batch_start..batch_end {
            let entity_id =
                u64::try_from(start_id.saturating_add(offset)).expect("scale entity id fits u64");
            transaction
                .put(
                    source_key(scope, entity_id),
                    encode_properties(&properties(entity_id)),
                )
                .expect("scale source row stages");
        }
        transaction
            .commit()
            .await
            .expect("scale source batch commits");
    }
}

/// Extracts the durable operation ID returned through the public interpreter.
fn accepted_operation_id(result: ExecutionResult) -> crate::index_lifecycle::IndexOperationId {
    let Some(ExecutionValue::IndexDdlReceipt(receipt)) = result.last else {
        panic!("scale DDL should return a durable receipt");
    };
    match receipt {
        IndexDdlReceipt::Accepted { operation_id, .. }
        | IndexDdlReceipt::ExistingOperation { operation_id } => operation_id,
        IndexDdlReceipt::AlreadyActive { .. } => {
            panic!("fresh scale DDL should not find an already-active index")
        }
    }
}

/// Waits for one accepted operation and refreshes its exact scoped catalog.
async fn execute_ddl_to_success(
    db: &HelixDB,
    scope: DataScope,
    plan: &exec::ExecutablePlan,
) -> IndexOperationStatusCommon {
    let started = Instant::now();
    let mut next_progress_log = Duration::from_secs(30);
    let operation_id = accepted_operation_id(
        db.execute_scoped(plan, context::ParamBindings::default(), scope)
            .await
            .expect("scale DDL is durably accepted"),
    );
    let status = tokio::time::timeout(OPERATION_TIMEOUT, async {
        loop {
            match db
                .get_index_operation(scope, operation_id)
                .await
                .expect("accepted scale operation remains readable")
            {
                status @ (IndexOperationStatus::Queued { .. }
                | IndexOperationStatus::Running { .. }) => {
                    if started.elapsed() >= next_progress_log {
                        let common = status.common();
                        eprintln!(
                            "index_lifecycle_scale_progress family={:?} stage={:?} entities={} output_operations={} claims={} elapsed_ms={}",
                            common.family,
                            common.stage,
                            common.progress.entities,
                            common.progress.output_operations,
                            common.attempt,
                            started.elapsed().as_millis(),
                        );
                        next_progress_log = next_progress_log.saturating_add(Duration::from_secs(30));
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                status @ IndexOperationStatus::Succeeded { .. } => break status,
                IndexOperationStatus::Blocked {
                    common,
                    blocker_code,
                    ..
                } => panic!(
                    "scale {:?} operation blocked at {:?}: {:?}",
                    common.family, common.stage, blocker_code
                ),
                IndexOperationStatus::Aborted { common } => panic!(
                    "scale {:?} operation was unexpectedly aborted at {:?}",
                    common.family, common.stage
                ),
            }
        }
    })
    .await
    .expect("scale operation should converge within the six-hour pre-launch budget");
    db.planner_context_scoped(context::ParamBindings::default(), scope)
        .await
        .expect("terminal scale DDL is visible through the loaded catalog");
    status.common().clone()
}

/// Emits one stable release-observation line for the audit ledger.
fn record_measurement(name: &str, elapsed: Duration, status: &IndexOperationStatusCommon) {
    eprintln!(
        "index_lifecycle_scale name={name} family={:?} entities={} input_bytes={} output_operations={} output_bytes={} claims={} elapsed_ms={}",
        status.family,
        status.progress.entities,
        status.progress.input_bytes,
        status.progress.output_operations,
        status.progress.output_bytes,
        status.attempt,
        elapsed.as_millis(),
    );
}

/// Closed terminal states accepted by the scale lifecycle waiter.
#[derive(Debug, Clone, Copy)]
enum ExpectedTerminal {
    Blocked,
    Succeeded,
    Aborted,
}

/// Waits for one exact public operation state without bypassing the worker.
async fn wait_for_terminal(
    db: &HelixDB,
    scope: DataScope,
    operation_id: crate::index_lifecycle::IndexOperationId,
    expected: ExpectedTerminal,
) -> IndexOperationStatus {
    tokio::time::timeout(OPERATION_TIMEOUT, async {
        loop {
            let status = db
                .get_index_operation(scope, operation_id)
                .await
                .expect("scale operation remains readable");
            let reached = matches!(
                (expected, &status),
                (
                    ExpectedTerminal::Blocked,
                    IndexOperationStatus::Blocked { .. }
                ) | (
                    ExpectedTerminal::Succeeded,
                    IndexOperationStatus::Succeeded { .. }
                ) | (
                    ExpectedTerminal::Aborted,
                    IndexOperationStatus::Aborted { .. }
                )
            );
            if reached {
                return status;
            }
            assert!(
                matches!(
                    &status,
                    IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. }
                ),
                "scale operation reached an unexpected terminal state: {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("scale operation should reach its expected terminal state")
}

/// Enqueues one CREATE and waits for its typed resource blocker.
async fn create_to_blocked(
    db: &HelixDB,
    scope: DataScope,
    spec: ir::IndexDdlCreateSpec,
) -> IndexOperationStatus {
    let operation_id = accepted_operation_id(
        db.execute_scoped(&create_plan(spec), context::ParamBindings::default(), scope)
            .await
            .expect("limit-triggered CREATE is durably accepted"),
    );
    wait_for_terminal(db, scope, operation_id, ExpectedTerminal::Blocked).await
}

/// Returns a runtime policy whose first source row cannot fit any family.
fn blocked_limit_config() -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let search_limits = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(LIMIT_ENTITY_COUNT).expect("limit fixture entity count is positive"),
            NonZeroU64::MIN,
            defaults.batch().max_output_operations(),
            defaults.batch().max_output_bytes(),
            defaults.batch().max_single_vector_output_bytes(),
        )
        .expect("blocked search limits remain internally consistent"),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .expect("blocked search policy preserves cross-budget invariants");
    DbConfig::new()
        .with_secondary_index_lifecycle_tuning(
            SecondaryIndexLifecycleTuning::default().with_max_input_bytes(NonZeroU64::MIN),
        )
        .with_search_index_backfill_limits(search_limits)
}

/// Proves terminal DROP removed every transient and physical V2 row in the tested scopes.
async fn assert_no_lifecycle_residue(db: &HelixDB, scopes: &[DataScope]) {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("scale residue checks require writer storage");
    };
    for kind in [index_keys::GlobalKind::OperationPointer] {
        let prefix = index_keys::GlobalKey::logical_prefix(kind);
        let mut rows = writer
            .db()
            .scan_prefix(
                &prefix,
                (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
            )
            .await
            .expect("global V2 residue lane remains readable");
        assert!(
            rows.next()
                .await
                .expect("global V2 residue scan succeeds")
                .is_none(),
            "terminal DROP retained a global {kind:?} row"
        );
    }

    for scope in scopes {
        for kind in [
            index_keys::RecordKind::BuildDelta,
            index_keys::RecordKind::AppliedState,
            index_keys::RecordKind::SecondaryEntry,
            index_keys::RecordKind::TextManifestRoot,
            index_keys::RecordKind::TextManifestPage,
            index_keys::RecordKind::TextBuildArtifact,
            index_keys::RecordKind::TextEntityState,
            index_keys::RecordKind::TextCorpusStatistics,
            index_keys::RecordKind::TextTermStatistics,
            index_keys::RecordKind::TextStatisticsEntity,
            index_keys::RecordKind::VectorPartitionMapping,
        ] {
            let prefix = DataKey::data_prefix(*scope, index_keys::ScopedKey::logical_prefix(kind));
            let mut rows = writer
                .db()
                .scan_prefix(
                    &prefix,
                    (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
                )
                .await
                .expect("scoped V2 residue lane remains readable");
            assert!(
                rows.next()
                    .await
                    .expect("scoped V2 residue scan succeeds")
                    .is_none(),
                "terminal DROP retained a scoped {kind:?} row in {scope:?}"
            );
        }
    }
}

/// Executes one CREATE and records its durable bounded-work counters.
async fn build(
    db: &HelixDB,
    scope: DataScope,
    name: &str,
    spec: ir::IndexDdlCreateSpec,
) -> IndexOperationStatusCommon {
    let started = Instant::now();
    let status = execute_ddl_to_success(db, scope, &create_plan(spec)).await;
    record_measurement(name, started.elapsed(), &status);
    status
}

/// Executes one DROP through the same public lifecycle boundary.
async fn drop_index(db: &HelixDB, scope: DataScope, spec: ir::IndexDdlDropSpec) {
    let status = execute_ddl_to_success(db, scope, &drop_plan(spec)).await;
    assert_eq!(
        status.stage,
        IndexOperationStage::Finalize,
        "successful DROP should retain its terminal cleanup checkpoint"
    );
}

/// Opens a writer and seeds one positive unscoped authoritative shape.
async fn open_seeded_unscoped(
    database: &str,
    entity_count: NonZeroUsize,
) -> (ProcessLocalDatabaseToken, HelixDB) {
    let entity_count = entity_count.get();
    let token = ProcessLocalDatabaseToken::new(database).expect("scale database token is valid");
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("scale writer opens");
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("scale database should be a writer");
    };

    let seed_started = Instant::now();
    let unscoped_ids = writer
        .node_ids()
        .allocate_batch(u64::try_from(entity_count).expect("scale entity count fits u64"))
        .await
        .expect("unscoped scale IDs are durably reserved");
    assert_eq!(
        unscoped_ids,
        0..u64::try_from(entity_count).expect("scale entity count fits u64")
    );
    seed_scope(
        writer.db(),
        DataScope::LegacyUnscoped,
        usize::try_from(unscoped_ids.start).expect("unscoped start ID fits usize"),
        entity_count,
        unscoped_properties,
    )
    .await;
    eprintln!(
        "index_lifecycle_scale name=seed_unscoped entities={entity_count} batches={} elapsed_ms={}",
        entity_count.div_ceil(SEED_BATCH_ROWS),
        seed_started.elapsed().as_millis(),
    );
    (token, db)
}

/// Cold-reopens one process-local scale database without replacing its store.
async fn reopen_process_local(db: &mut HelixDB, token: &ProcessLocalDatabaseToken) {
    db.close()
        .await
        .expect("scale writer closes before restart validation");
    *db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("scale writer reopens for restart validation");
}

/// Runs secondary, text, multi-scope, and cleanup scale oracles.
pub(super) async fn run_secondary_text_tenant() {
    assert_eq!(
        ENTITY_COUNT, 100_000,
        "release scale shape must remain fixed"
    );
    assert_eq!(TENANT_COUNT, 16, "tenant release shape must remain fixed");
    let (token, mut db) = open_seeded_unscoped(
        "index-lifecycle-production-secondary-text-scale",
        NonZeroUsize::new(ENTITY_COUNT).expect("release entity count is positive"),
    )
    .await;
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("scale database should be a writer");
    };

    let property_key = |property| {
        catalog::ScopedPropertyKey::try_new(LABEL, property)
            .expect("scale scoped property key is valid")
    };
    let non_unique_status = build(
        &db,
        DataScope::LegacyUnscoped,
        "secondary_non_unique",
        ir::IndexDdlCreateSpec::NodeEquality {
            key: property_key(NON_UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::NonUnique,
        },
    )
    .await;
    assert_eq!(non_unique_status.family, PublicIndexFamily::Secondary);
    let mut actual = projected_node_ids(
        db.execute(
            &equality_search_plan(
                NON_UNIQUE_PROPERTY,
                PlannerPropertyValue::String("shared-target".to_string()),
                catalog::IndexUniqueness::NonUnique,
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("non-unique scale search succeeds"),
    );
    actual.sort_unstable();
    assert_eq!(actual, vec![0, 1]);

    build(
        &db,
        DataScope::LegacyUnscoped,
        "secondary_unique",
        ir::IndexDdlCreateSpec::NodeEquality {
            key: property_key(UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::Unique,
        },
    )
    .await;
    assert_eq!(
        projected_node_ids(
            db.execute(
                &equality_search_plan(
                    UNIQUE_PROPERTY,
                    PlannerPropertyValue::String("external-99999".to_string()),
                    catalog::IndexUniqueness::Unique,
                ),
                context::ParamBindings::default(),
            )
            .await
            .expect("unique scale search succeeds"),
        ),
        vec![99_999]
    );

    build(
        &db,
        DataScope::LegacyUnscoped,
        "text_paged",
        ir::IndexDdlCreateSpec::NodeText {
            key: property_key(TEXT_PROPERTY),
            scope: catalog::SearchIndexScope::Unscoped,
        },
    )
    .await;
    assert_eq!(
        projected_node_ids(
            db.execute(
                &text_search_plan("uniqueneedle"),
                context::ParamBindings::default(),
            )
            .await
            .expect("paged scale text search succeeds"),
        ),
        vec![0]
    );

    let tenant_seed_started = Instant::now();
    let tenant_ids = writer
        .node_ids()
        .allocate_batch(u64::try_from(ENTITY_COUNT).expect("scale entity count fits u64"))
        .await
        .expect("tenant scale IDs are durably reserved");
    let tenant_start = usize::try_from(tenant_ids.start).expect("tenant start ID fits usize");
    let mut distributed_rows = 0_usize;
    for tenant_ordinal in 0..TENANT_COUNT {
        let tenant_rows = ENTITY_COUNT / TENANT_COUNT;
        let scope = DataScope::Tenant(TenantId::from_u128(
            u128::try_from(tenant_ordinal + 1).expect("tenant ordinal fits u128"),
        ));
        seed_scope(
            writer.db(),
            scope,
            tenant_start.saturating_add(distributed_rows),
            tenant_rows,
            tenant_properties,
        )
        .await;
        distributed_rows = distributed_rows
            .checked_add(tenant_rows)
            .expect("distributed tenant row count remains bounded");
        build(
            &db,
            scope,
            &format!("tenant_{tenant_ordinal}_secondary"),
            ir::IndexDdlCreateSpec::NodeEquality {
                key: property_key(NON_UNIQUE_PROPERTY),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
        )
        .await;
        assert!(!projected_node_ids(
            db.execute_scoped(
                &equality_search_plan(
                    NON_UNIQUE_PROPERTY,
                    PlannerPropertyValue::String("tenant-group-0".to_string()),
                    catalog::IndexUniqueness::NonUnique,
                ),
                context::ParamBindings::default(),
                scope,
            )
            .await
            .expect("tenant scale search succeeds"),
        )
        .is_empty());
    }
    assert_eq!(distributed_rows, ENTITY_COUNT);
    eprintln!(
        "index_lifecycle_scale name=tenant_workload tenants={TENANT_COUNT} entities={ENTITY_COUNT} elapsed_ms={}",
        tenant_seed_started.elapsed().as_millis(),
    );

    reopen_process_local(&mut db, &token).await;
    let mut actual = projected_node_ids(
        db.execute(
            &equality_search_plan(
                NON_UNIQUE_PROPERTY,
                PlannerPropertyValue::String("shared-target".to_string()),
                catalog::IndexUniqueness::NonUnique,
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("non-unique scale search survives restart"),
    );
    actual.sort_unstable();
    assert_eq!(actual, vec![0, 1]);
    assert_eq!(
        projected_node_ids(
            db.execute(
                &equality_search_plan(
                    UNIQUE_PROPERTY,
                    PlannerPropertyValue::String("external-99999".to_string()),
                    catalog::IndexUniqueness::Unique,
                ),
                context::ParamBindings::default(),
            )
            .await
            .expect("unique scale search survives restart"),
        ),
        vec![99_999]
    );
    assert_eq!(
        projected_node_ids(
            db.execute(
                &text_search_plan("uniqueneedle"),
                context::ParamBindings::default(),
            )
            .await
            .expect("paged scale text search survives restart"),
        ),
        vec![0]
    );
    for tenant_ordinal in 0..TENANT_COUNT {
        let scope = DataScope::Tenant(TenantId::from_u128(
            u128::try_from(tenant_ordinal + 1).expect("tenant ordinal fits u128"),
        ));
        assert!(!projected_node_ids(
            db.execute_scoped(
                &equality_search_plan(
                    NON_UNIQUE_PROPERTY,
                    PlannerPropertyValue::String("tenant-group-0".to_string()),
                    catalog::IndexUniqueness::NonUnique,
                ),
                context::ParamBindings::default(),
                scope,
            )
            .await
            .expect("tenant scale search survives restart"),
        )
        .is_empty());
    }

    for tenant_ordinal in 0..TENANT_COUNT {
        let scope = DataScope::Tenant(TenantId::from_u128(
            u128::try_from(tenant_ordinal + 1).expect("tenant ordinal fits u128"),
        ));
        drop_index(
            &db,
            scope,
            ir::IndexDdlDropSpec::NodeEquality {
                key: property_key(NON_UNIQUE_PROPERTY),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
        )
        .await;
    }
    for spec in [
        ir::IndexDdlDropSpec::NodeText {
            key: property_key(TEXT_PROPERTY),
        },
        ir::IndexDdlDropSpec::NodeEquality {
            key: property_key(UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::Unique,
        },
        ir::IndexDdlDropSpec::NodeEquality {
            key: property_key(NON_UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::NonUnique,
        },
    ] {
        drop_index(&db, DataScope::LegacyUnscoped, spec).await;
    }

    assert!(db
        .execute(
            &text_search_plan("uniqueneedle"),
            context::ParamBindings::default(),
        )
        .await
        .is_err());
    let mut scopes = vec![DataScope::LegacyUnscoped];
    scopes.extend((0..TENANT_COUNT).map(|tenant_ordinal| {
        DataScope::Tenant(TenantId::from_u128(
            u128::try_from(tenant_ordinal + 1).expect("tenant ordinal fits u128"),
        ))
    }));
    assert_no_lifecycle_residue(&db, &scopes).await;
    db.close().await.expect("scale writer closes cleanly");
}

/// Runs one text CREATE/search/DROP fixture at an exact authoritative row count.
async fn run_text_drop_fixture(database: &str, measurement: &str, entity_count: usize) {
    let token =
        ProcessLocalDatabaseToken::new(database).expect("text DROP smoke database token is valid");
    let db = HelixDB::open(HelixDbSource::InMemoryToken { token })
        .await
        .expect("text DROP smoke writer opens");
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("text DROP smoke database should be a writer");
    };
    let ids = writer
        .node_ids()
        .allocate_batch(u64::try_from(entity_count).expect("text DROP smoke row count fits u64"))
        .await
        .expect("text DROP smoke IDs are durably reserved");
    assert_eq!(
        ids,
        0..u64::try_from(entity_count).expect("text DROP smoke row count fits u64")
    );
    seed_scope(
        writer.db(),
        DataScope::LegacyUnscoped,
        0,
        entity_count,
        unscoped_properties,
    )
    .await;

    let key = catalog::ScopedPropertyKey::try_new(LABEL, TEXT_PROPERTY)
        .expect("text DROP smoke key is valid");
    build(
        &db,
        DataScope::LegacyUnscoped,
        measurement,
        ir::IndexDdlCreateSpec::NodeText {
            key: key.clone(),
            scope: catalog::SearchIndexScope::Unscoped,
        },
    )
    .await;
    assert_eq!(
        projected_node_ids(
            db.execute(
                &text_search_plan("uniqueneedle"),
                context::ParamBindings::default(),
            )
            .await
            .expect("text DROP smoke search succeeds"),
        ),
        vec![0]
    );
    drop_index(
        &db,
        DataScope::LegacyUnscoped,
        ir::IndexDdlDropSpec::NodeText { key },
    )
    .await;
    assert_no_lifecycle_residue(&db, &[DataScope::LegacyUnscoped]).await;
    db.close()
        .await
        .expect("text DROP smoke writer closes cleanly");
}

/// Reproduces text publication-to-DROP handoff with one compact split.
pub(super) async fn run_text_drop_smoke() {
    run_text_drop_fixture(
        "index-lifecycle-production-text-drop-smoke",
        "text_drop_smoke",
        100,
    )
    .await;
}

/// Reproduces text publication-to-DROP handoff after multi-split compaction.
pub(super) async fn run_text_drop_multi_split_smoke() {
    run_text_drop_fixture(
        "index-lifecycle-production-text-drop-multi-split-smoke",
        "text_drop_multi_split_smoke",
        10_000,
    )
    .await;
}

/// Runs one positive 128D f32 vector lifecycle shape and brute-force oracle.
async fn run_vector_fixture(database: &str, entity_count: NonZeroUsize) {
    assert_eq!(
        VECTOR_DIMENSION, 128,
        "vector release shape must remain fixed"
    );
    let (token, mut db) = open_seeded_unscoped(database, entity_count).await;
    let property_key = catalog::ScopedPropertyKey::try_new(LABEL, VECTOR_PROPERTY)
        .expect("scale vector property key is valid");

    build(
        &db,
        DataScope::LegacyUnscoped,
        "vector_f32_128",
        ir::IndexDdlCreateSpec::NodeVector {
            key: property_key.clone(),
            dimension: ir::VectorIndexDimension::new(VECTOR_DIMENSION)
                .expect("scale vector dimension is positive"),
            metric: ir::VectorIndexMetric::Euclidean,
            scope: catalog::SearchIndexScope::Unscoped,
        },
    )
    .await;
    reopen_process_local(&mut db, &token).await;
    let query = vector(0);
    let brute_force = (0..u64::try_from(entity_count.get()).expect("scale entity count fits u64"))
        .min_by(|left, right| {
            let left_distance = vector(*left)
                .iter()
                .zip(&query)
                .map(|(candidate, query)| (candidate - query).powi(2))
                .sum::<f32>();
            let right_distance = vector(*right)
                .iter()
                .zip(&query)
                .map(|(candidate, query)| (candidate - query).powi(2))
                .sum::<f32>();
            left_distance.total_cmp(&right_distance)
        })
        .expect("scale oracle contains entities");
    assert_eq!(brute_force, 0);
    assert_eq!(
        projected_node_ids(
            db.execute(
                &vector_search_plan(query),
                context::ParamBindings::default(),
            )
            .await
            .expect("128D scale vector search succeeds"),
        ),
        vec![brute_force]
    );

    drop_index(
        &db,
        DataScope::LegacyUnscoped,
        ir::IndexDdlDropSpec::NodeVector { key: property_key },
    )
    .await;
    assert!(db
        .execute(
            &vector_search_plan(vector(0)),
            context::ParamBindings::default(),
        )
        .await
        .is_err());
    assert_no_lifecycle_residue(&db, &[DataScope::LegacyUnscoped]).await;
    db.close()
        .await
        .expect("vector scale writer closes cleanly");
}

/// Runs the isolated fixed 100k vector launch gate.
pub(super) async fn run_vector() {
    assert_eq!(
        ENTITY_COUNT, 100_000,
        "release scale shape must remain fixed"
    );
    run_vector_fixture(
        "index-lifecycle-production-vector-scale",
        NonZeroUsize::new(ENTITY_COUNT).expect("release entity count is positive"),
    )
    .await;
}

/// Runs the fixed CI-sized vector lifecycle through the production boundary.
pub(super) async fn run_vector_ci() {
    assert_eq!(
        CI_VECTOR_ENTITY_COUNT, 8_000,
        "CI vector shape must remain fixed"
    );
    run_vector_fixture(
        "index-lifecycle-production-vector-ci",
        NonZeroUsize::new(CI_VECTOR_ENTITY_COUNT).expect("CI entity count is positive"),
    )
    .await;
}

/// Returns the production traversal benchmark's fixed HNSW construction contract.
fn traversal_vector_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(
        VectorIndexDefinition::new_node(
            LABEL,
            VECTOR_PROPERTY,
            TRAVERSAL_VECTOR_DIMENSION,
            crate::search::vector::VectorDistanceMetric::Euclidean,
        )
        .expect("traversal vector definition is valid")
        .with_m(16)
        .expect("traversal graph degree is valid")
        .with_m0(32)
        .expect("traversal base graph degree is valid")
        .with_ef_construction(200)
        .expect("traversal construction beam is valid"),
    )
    .expect("traversal vector definition validates")
}

/// Builds one tuned traversal vector generation and reports durable progress.
async fn build_traversal_vector_index(
    db: &HelixDB,
    source: &slatedb::Db,
    timeout: Duration,
    benchmark: &str,
) -> (Duration, IndexOperationStatus) {
    let started = Instant::now();
    let receipt = crate::index_lifecycle::lifecycle::create_index_operation_from_current_source(
        source,
        DataScope::LegacyUnscoped,
        traversal_vector_definition(),
        ir::IndexCreateMode::ErrorIfExists,
    )
    .await
    .expect("tuned traversal vector build is accepted");
    let operation_id = match receipt {
        IndexDdlReceipt::Accepted { operation_id, .. }
        | IndexDdlReceipt::ExistingOperation { operation_id } => operation_id,
        IndexDdlReceipt::AlreadyActive { .. } => {
            panic!("fresh traversal vector build should not already be active")
        }
    };
    let mut next_progress_log = Duration::from_secs(30);
    let status = tokio::time::timeout(timeout, async {
        loop {
            match db
                .get_index_operation(DataScope::LegacyUnscoped, operation_id)
                .await
                .expect("traversal vector operation remains readable")
            {
                status @ (IndexOperationStatus::Queued { .. }
                | IndexOperationStatus::Running { .. }) => {
                    if started.elapsed() >= next_progress_log {
                        let common = status.common();
                        eprintln!(
                            "TRAVERSAL_VECTOR_PREFILTER benchmark={benchmark} build_entities={} claims={} elapsed_ms={}",
                            common.progress.entities,
                            common.attempt,
                            started.elapsed().as_millis(),
                        );
                        next_progress_log =
                            next_progress_log.saturating_add(Duration::from_secs(30));
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                status @ IndexOperationStatus::Succeeded { .. } => break status,
                IndexOperationStatus::Blocked {
                    common,
                    blocker_code,
                    ..
                } => panic!(
                    "traversal vector operation blocked at {:?}: {:?}",
                    common.stage, blocker_code
                ),
                IndexOperationStatus::Aborted { common } => {
                    panic!("traversal vector operation aborted at {:?}", common.stage)
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("traversal vector build should converge within {timeout:?}"));
    (started.elapsed(), status)
}

/// Runs a real indexed prefilter, one-hop traversal, and restricted DBpedia search.
pub(super) async fn run_traversal_vector_prefilter() {
    const QUERY_COUNT: usize = 128;
    let vectors = TraversalVectors::load();
    let token = ProcessLocalDatabaseToken::new("index-lifecycle-traversal-vector-prefilter-scale")
        .expect("traversal benchmark token is valid");
    let db = HelixDB::open(HelixDbSource::InMemoryToken { token })
        .await
        .expect("traversal benchmark writer opens");
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("traversal benchmark database should be a writer");
    };
    let ids = writer
        .node_ids()
        .allocate_batch(
            u64::try_from(TRAVERSAL_ENTITY_COUNT).expect("traversal entity count fits u64"),
        )
        .await
        .expect("traversal benchmark IDs are durably reserved");
    assert_eq!(
        ids,
        0..u64::try_from(TRAVERSAL_ENTITY_COUNT).expect("traversal entity count fits u64")
    );

    let seed_started = Instant::now();
    for batch_start in (0..TRAVERSAL_ENTITY_COUNT).step_by(SEED_BATCH_ROWS) {
        let batch_end = TRAVERSAL_ENTITY_COUNT.min(batch_start.saturating_add(SEED_BATCH_ROWS));
        let transaction = writer
            .db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("traversal seed transaction opens");
        for entity_id in batch_start..batch_end {
            let entity_id = u64::try_from(entity_id).expect("traversal entity ID fits u64");
            transaction
                .put(
                    source_key(DataScope::LegacyUnscoped, entity_id),
                    encode_properties(&traversal_properties(entity_id, vectors.get(entity_id))),
                )
                .expect("traversal source row stages");
            let mut edges = Edges::new();
            edges.add_out(
                (entity_id
                    + u64::try_from(TRAVERSAL_ENTITY_COUNT / 2)
                        .expect("traversal half count fits u64"))
                    % u64::try_from(TRAVERSAL_ENTITY_COUNT)
                        .expect("traversal entity count fits u64"),
            );
            transaction
                .put(
                    adjacency_key(DataScope::LegacyUnscoped, entity_id),
                    encode_edges(&edges),
                )
                .expect("traversal adjacency row stages");
        }
        transaction
            .commit()
            .await
            .expect("traversal seed batch commits");
    }
    eprintln!(
        "TRAVERSAL_VECTOR_PREFILTER seed_entities={TRAVERSAL_ENTITY_COUNT} dimension={TRAVERSAL_VECTOR_DIMENSION} elapsed_ms={}",
        seed_started.elapsed().as_millis()
    );

    let property_key = |property| {
        catalog::ScopedPropertyKey::try_new(LABEL, property)
            .expect("traversal property key is valid")
    };
    build(
        &db,
        DataScope::LegacyUnscoped,
        "traversal_secondary_prefilter",
        ir::IndexDdlCreateSpec::NodeEquality {
            key: property_key(NON_UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::NonUnique,
        },
    )
    .await;
    let (vector_build_elapsed, status) =
        build_traversal_vector_index(&db, writer.db(), TRAVERSAL_OPERATION_TIMEOUT, "dbpedia-50k")
            .await;
    record_measurement(
        "traversal_dbpedia_f32_1536_m16_ef200_search100",
        vector_build_elapsed,
        status.common(),
    );
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("active traversal vector generation enters the planner catalog");

    let prefilter = traversal_prefilter_plan("vector-candidate");
    let prefilter_ids = projected_node_ids(
        db.execute(&prefilter, context::ParamBindings::default())
            .await
            .expect("indexed one-hop prefilter succeeds"),
    );
    assert_eq!(prefilter_ids.len(), TRAVERSAL_PREFILTER_COUNT);
    assert_eq!(prefilter_ids[0], 25_000);
    assert_eq!(prefilter_ids[TRAVERSAL_PREFILTER_COUNT - 1], 25_999);

    let mut prefilter_latencies = Vec::with_capacity(QUERY_COUNT);
    for _ in 0..QUERY_COUNT {
        let started = Instant::now();
        let result = db
            .execute(&prefilter, context::ParamBindings::default())
            .await
            .expect("indexed one-hop prefilter succeeds");
        prefilter_latencies.push(started.elapsed());
        assert_eq!(projected_node_ids(result).len(), TRAVERSAL_PREFILTER_COUNT);
    }

    let mut sorted_prefilter_latencies = prefilter_latencies.clone();
    sorted_prefilter_latencies.sort_unstable();
    let percentile_index = (QUERY_COUNT - 1) * 95 / 100;
    let prefilter_p95 = sorted_prefilter_latencies[percentile_index];
    let queries = (0..QUERY_COUNT)
        .map(|query_index| {
            let query_id = 40_000_u64
                + u64::try_from(query_index * 10_000 / QUERY_COUNT)
                    .expect("held-out DBpedia query offset fits u64");
            let query = vectors.get(query_id).to_vec();
            let mut exact = (25_000_u64..26_000)
                .map(|entity_id| {
                    let distance = vectors
                        .get(entity_id)
                        .iter()
                        .zip(&query)
                        .map(|(candidate, query)| (candidate - query).powi(2))
                        .sum::<f32>();
                    (distance, entity_id)
                })
                .collect::<Vec<_>>();
            exact.sort_unstable_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            (
                query,
                exact
                    .into_iter()
                    .take(10)
                    .map(|(_, entity_id)| entity_id)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();

    let mut beam_measurements = Vec::new();
    for beam_scale in [
        crate::search::vector::RestrictedBeamScale::Base,
        crate::search::vector::RestrictedBeamScale::OneAndHalf,
        crate::search::vector::RestrictedBeamScale::Double,
    ] {
        let _beam_guard = crate::search::vector::RestrictedBeamOverrideGuard::acquire(beam_scale);
        let mut end_to_end_latencies = Vec::with_capacity(QUERY_COUNT);
        let mut vector_increment_latencies = Vec::with_capacity(QUERY_COUNT);
        let mut matched = 0_usize;
        for (query_index, (query, exact)) in queries.iter().enumerate() {
            let plan = traversal_prefilter_vector_plan("vector-candidate", query.clone());
            let started = Instant::now();
            let actual = projected_node_ids(
                db.execute(&plan, context::ParamBindings::default())
                    .await
                    .expect("indexed one-hop restricted vector search succeeds"),
            );
            let end_to_end = started.elapsed();
            end_to_end_latencies.push(end_to_end);
            vector_increment_latencies
                .push(end_to_end.saturating_sub(prefilter_latencies[query_index]));
            assert_eq!(actual.len(), 10);
            assert!(actual
                .iter()
                .all(|entity_id| prefilter_ids.contains(entity_id)));
            matched = matched.saturating_add(
                actual
                    .iter()
                    .filter(|entity_id| exact.contains(entity_id))
                    .count(),
            );
        }
        end_to_end_latencies.sort_unstable();
        vector_increment_latencies.sort_unstable();
        let end_to_end_p95 = end_to_end_latencies[percentile_index];
        let vector_increment_p95 = vector_increment_latencies[percentile_index];
        let recall = matched as f64 / (QUERY_COUNT * 10) as f64;
        eprintln!(
            "TRAVERSAL_VECTOR_PREFILTER dataset=dbpedia-openai3-large entities={TRAVERSAL_ENTITY_COUNT} dimension={TRAVERSAL_VECTOR_DIMENSION} m=16 m0=32 ef_construction=200 ef_search=100 filtered_beam={} filtered_beam_percent={} hop_count=1 prefilter_rows={TRAVERSAL_PREFILTER_COUNT} held_out_queries={QUERY_COUNT} recall_at_10={recall:.6} prefilter_p50_us={} prefilter_p95_us={} end_to_end_p50_us={} end_to_end_p95_us={} vector_increment_p95_us={}",
            beam_scale.width(100),
            beam_scale.percent(),
            sorted_prefilter_latencies[(QUERY_COUNT - 1) * 50 / 100].as_micros(),
            prefilter_p95.as_micros(),
            end_to_end_latencies[(QUERY_COUNT - 1) * 50 / 100].as_micros(),
            end_to_end_p95.as_micros(),
            vector_increment_p95.as_micros(),
        );
        beam_measurements.push((beam_scale, recall, vector_increment_p95, end_to_end_p95));
    }
    let Some((selected_beam, _, _, _)) =
        beam_measurements
            .iter()
            .find(|(_, recall, vector_p95, end_to_end_p95)| {
                *recall >= 0.92
                    && *vector_p95 <= Duration::from_millis(15)
                    && *end_to_end_p95 <= Duration::from_millis(50)
            })
    else {
        panic!("no filtered beam satisfied the DBpedia accuracy and latency gates");
    };
    eprintln!(
        "TRAVERSAL_VECTOR_PREFILTER selected_filtered_beam={} recall_target=0.92 vector_p95_target_ms=15 end_to_end_p95_target_ms=50",
        selected_beam.width(100),
    );
    db.close()
        .await
        .expect("traversal benchmark writer closes cleanly");
}

/// Inserts and indexes one million DBpedia vectors, then benchmarks four prefilters.
pub(super) async fn run_traversal_vector_prefilter_1m() {
    const QUERY_COUNT: usize = 32;
    let vectors = MappedTraversalVectors::load_1m();
    let database_parent = PathBuf::from(std::env::var_os(DBPEDIA_1M_DB_PARENT_ENV).unwrap_or_else(
        || panic!("{DBPEDIA_1M_DB_PARENT_ENV} must point to a benchmark scratch directory"),
    ));
    std::fs::create_dir_all(&database_parent)
        .expect("million-row benchmark scratch parent is writable");
    let database_root = tempfile::Builder::new()
        .prefix("helix-dbpedia-1m-")
        .tempdir_in(&database_parent)
        .expect("million-row benchmark creates an isolated disk root");
    eprintln!(
        "TRAVERSAL_VECTOR_PREFILTER_1M phase=open entity_count={TRAVERSAL_1M_ENTITY_COUNT} dimension={TRAVERSAL_VECTOR_DIMENSION} seed_batch_rows={SEED_BATCH_ROWS} db_root={}",
        database_root.path().display(),
    );
    let db = HelixDB::open(HelixDbSource::Disk {
        root: database_root.path().to_path_buf(),
        database: "index-lifecycle-traversal-vector-prefilter-1m".to_string(),
    })
    .await
    .expect("million-row traversal benchmark writer opens");
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("million-row traversal benchmark database should be a writer");
    };
    let ids = writer
        .node_ids()
        .allocate_batch(
            u64::try_from(TRAVERSAL_1M_ENTITY_COUNT)
                .expect("million-row traversal entity count fits u64"),
        )
        .await
        .expect("million-row traversal IDs are durably reserved");
    assert_eq!(
        ids,
        0..u64::try_from(TRAVERSAL_1M_ENTITY_COUNT)
            .expect("million-row traversal entity count fits u64")
    );

    let seed_started = Instant::now();
    let mut next_seed_log = Duration::from_secs(30);
    for batch_start in (0..TRAVERSAL_1M_ENTITY_COUNT).step_by(SEED_BATCH_ROWS) {
        let batch_end = TRAVERSAL_1M_ENTITY_COUNT.min(batch_start.saturating_add(SEED_BATCH_ROWS));
        let transaction = writer
            .db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("million-row traversal seed transaction opens");
        for entity_id in batch_start..batch_end {
            let entity_id =
                u64::try_from(entity_id).expect("million-row traversal entity ID fits u64");
            transaction
                .put(
                    source_key(DataScope::LegacyUnscoped, entity_id),
                    encode_properties(&traversal_1m_properties(entity_id, vectors.get(entity_id))),
                )
                .expect("million-row traversal source row stages");
            let mut edges = Edges::new();
            edges.add_out(
                (entity_id
                    + u64::try_from(TRAVERSAL_1M_ENTITY_COUNT / 2)
                        .expect("million-row traversal half count fits u64"))
                    % u64::try_from(TRAVERSAL_1M_ENTITY_COUNT)
                        .expect("million-row traversal entity count fits u64"),
            );
            transaction
                .put(
                    adjacency_key(DataScope::LegacyUnscoped, entity_id),
                    encode_edges(&edges),
                )
                .expect("million-row traversal adjacency row stages");
        }
        transaction
            .commit()
            .await
            .expect("million-row traversal seed batch commits");
        if seed_started.elapsed() >= next_seed_log || batch_end == TRAVERSAL_1M_ENTITY_COUNT {
            eprintln!(
                "TRAVERSAL_VECTOR_PREFILTER_1M phase=seed inserted_entities={batch_end} elapsed_ms={}",
                seed_started.elapsed().as_millis(),
            );
            next_seed_log = next_seed_log.saturating_add(Duration::from_secs(30));
        }
    }
    let seed_elapsed = seed_started.elapsed();
    eprintln!(
        "TRAVERSAL_VECTOR_PREFILTER_1M phase=seed_complete inserted_entities={TRAVERSAL_1M_ENTITY_COUNT} elapsed_ms={} entities_per_second={:.3}",
        seed_elapsed.as_millis(),
        TRAVERSAL_1M_ENTITY_COUNT as f64 / seed_elapsed.as_secs_f64(),
    );

    let property_key = |property| {
        catalog::ScopedPropertyKey::try_new(LABEL, property)
            .expect("million-row traversal property key is valid")
    };
    let equality_started = Instant::now();
    build(
        &db,
        DataScope::LegacyUnscoped,
        "traversal_1m_secondary_prefilter",
        ir::IndexDdlCreateSpec::NodeEquality {
            key: property_key(NON_UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::NonUnique,
        },
    )
    .await;
    eprintln!(
        "TRAVERSAL_VECTOR_PREFILTER_1M phase=equality_index_complete elapsed_ms={}",
        equality_started.elapsed().as_millis(),
    );

    let (vector_build_elapsed, status) = build_traversal_vector_index(
        &db,
        writer.db(),
        TRAVERSAL_1M_OPERATION_TIMEOUT,
        "dbpedia-1m",
    )
    .await;
    record_measurement(
        "traversal_dbpedia_1m_f32_1536_m16_ef200_search100",
        vector_build_elapsed,
        status.common(),
    );
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("active million-row traversal vector generation enters the planner catalog");

    let mut accuracy_failures = Vec::new();
    for shape in Traversal1MShape::ALL {
        let prefilter = traversal_prefilter_plan(shape.group);
        let prefilter_ids = projected_node_ids(
            db.execute(&prefilter, context::ParamBindings::default())
                .await
                .expect("million-row indexed one-hop prefilter succeeds"),
        );
        assert_eq!(prefilter_ids.len(), shape.candidate_count);
        assert!(prefilter_ids.iter().all(|entity_id| {
            (u64::try_from(shape.candidate_start()).expect("candidate start fits u64")
                ..u64::try_from(shape.candidate_end()).expect("candidate end fits u64"))
                .contains(entity_id)
        }));

        let queries = (0..QUERY_COUNT)
            .map(|query_index| {
                let query_id = 800_000_u64
                    + u64::try_from(query_index * 100_000 / QUERY_COUNT)
                        .expect("million-row held-out DBpedia query offset fits u64");
                let query = vectors.get(query_id);
                let mut exact = (shape.candidate_start()..shape.candidate_end())
                    .map(|entity_id| {
                        let entity_id =
                            u64::try_from(entity_id).expect("candidate entity ID fits u64");
                        (vectors.squared_euclidean(entity_id, &query), entity_id)
                    })
                    .collect::<Vec<_>>();
                exact.sort_unstable_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| left.1.cmp(&right.1))
                });
                (
                    query,
                    exact
                        .into_iter()
                        .take(10)
                        .map(|(_, entity_id)| entity_id)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();

        let mut prefilter_latencies = Vec::with_capacity(QUERY_COUNT);
        let mut end_to_end_latencies = Vec::with_capacity(QUERY_COUNT);
        let mut vector_increment_latencies = Vec::with_capacity(QUERY_COUNT);
        let mut matched = 0_usize;
        let mut scored_candidates = 0_usize;
        let mut max_scored_candidates = 0_usize;
        let mut vector_bytes = 0_usize;
        let mut directory_rows = 0_usize;
        let mut directory_hits = 0_usize;
        let mut bridge_rows = 0_usize;
        let mut multi_get_calls = 0_usize;
        let mut termination_counts = [0_usize; 6];
        for (query, exact) in &queries {
            let prefilter_started = Instant::now();
            let observed_prefilter = projected_node_ids(
                db.execute(&prefilter, context::ParamBindings::default())
                    .await
                    .expect("million-row indexed one-hop prefilter succeeds"),
            );
            let prefilter_elapsed = prefilter_started.elapsed();
            assert_eq!(observed_prefilter.len(), shape.candidate_count);
            prefilter_latencies.push(prefilter_elapsed);

            let plan = traversal_prefilter_vector_plan(shape.group, query.clone());
            let started = Instant::now();
            let (result, stats) = crate::search::vector::observe_restricted_search(
                db.execute(&plan, context::ParamBindings::default()),
            )
            .await;
            let end_to_end = started.elapsed();
            let actual = projected_node_ids(
                result.expect("million-row indexed restricted vector search succeeds"),
            );
            let stats = stats.expect("million-row restricted search records its counters");
            assert_eq!(actual.len(), 10);
            assert!(actual.iter().all(|entity_id| {
                (u64::try_from(shape.candidate_start()).expect("candidate start fits u64")
                    ..u64::try_from(shape.candidate_end()).expect("candidate end fits u64"))
                    .contains(entity_id)
            }));
            assert!(
                stats.distance_computations <= 800,
                "restricted vector scoring must remain bounded"
            );
            assert!(stats.directory_rows <= 65_536);
            if shape.candidate_count <= 256 {
                assert_eq!(
                    stats.strategy,
                    Some(crate::search::vector::RestrictedSearchStrategy::Exact)
                );
            } else {
                assert_eq!(
                    stats.strategy,
                    Some(crate::search::vector::RestrictedSearchStrategy::FilteredGraph)
                );
            }
            matched = matched.saturating_add(
                actual
                    .iter()
                    .filter(|entity_id| exact.contains(entity_id))
                    .count(),
            );
            scored_candidates = scored_candidates.saturating_add(stats.distance_computations);
            max_scored_candidates = max_scored_candidates.max(stats.distance_computations);
            vector_bytes = vector_bytes.saturating_add(stats.vector_bytes);
            directory_rows = directory_rows.saturating_add(stats.directory_rows);
            directory_hits = directory_hits.saturating_add(stats.directory_hits);
            bridge_rows = bridge_rows.saturating_add(stats.bridge_rows);
            multi_get_calls = multi_get_calls
                .saturating_add(stats.simhash_multi_get_calls)
                .saturating_add(stats.neighbor_multi_get_calls)
                .saturating_add(stats.vector_multi_get_calls);
            let termination_index = match (stats.strategy, stats.termination) {
                (Some(crate::search::vector::RestrictedSearchStrategy::Exact), None) => 5,
                (
                    Some(crate::search::vector::RestrictedSearchStrategy::FilteredGraph),
                    Some(crate::search::vector::RestrictedSearchTermination::Exhausted),
                ) => 0,
                (
                    Some(crate::search::vector::RestrictedSearchStrategy::FilteredGraph),
                    Some(crate::search::vector::RestrictedSearchTermination::BeamComplete),
                ) => 1,
                (
                    Some(crate::search::vector::RestrictedSearchStrategy::FilteredGraph),
                    Some(crate::search::vector::RestrictedSearchTermination::RoutingBudget),
                ) => 2,
                (
                    Some(crate::search::vector::RestrictedSearchStrategy::FilteredGraph),
                    Some(crate::search::vector::RestrictedSearchTermination::BridgeBudget),
                ) => 3,
                (
                    Some(crate::search::vector::RestrictedSearchStrategy::FilteredGraph),
                    Some(crate::search::vector::RestrictedSearchTermination::VectorBudget),
                ) => 4,
                _ => panic!("million-row restricted search records a valid termination"),
            };
            termination_counts[termination_index] =
                termination_counts[termination_index].saturating_add(1);
            end_to_end_latencies.push(end_to_end);
            vector_increment_latencies.push(end_to_end.saturating_sub(prefilter_elapsed));
        }

        prefilter_latencies.sort_unstable();
        end_to_end_latencies.sort_unstable();
        vector_increment_latencies.sort_unstable();
        let p50_index = (QUERY_COUNT - 1) * 50 / 100;
        let p95_index = (QUERY_COUNT - 1) * 95 / 100;
        let recall = matched as f64 / (QUERY_COUNT * 10) as f64;
        eprintln!(
            "TRAVERSAL_VECTOR_PREFILTER_1M phase=query dataset=dbpedia-openai3-large entities={TRAVERSAL_1M_ENTITY_COUNT} dimension={TRAVERSAL_VECTOR_DIMENSION} m=16 m0=32 ef_construction=200 ef_search=100 filtered_beam=150 hop_count=1 group={} prefilter_rows={} held_out_queries={QUERY_COUNT} recall_at_10={recall:.6} prefilter_p50_us={} prefilter_p95_us={} end_to_end_p50_us={} end_to_end_p95_us={} vector_increment_p95_us={} scored_candidates_total={scored_candidates} scored_candidates_max={max_scored_candidates} vector_bytes={vector_bytes} directory_rows={directory_rows} directory_hits={directory_hits} bridge_rows={bridge_rows} multi_get_calls={multi_get_calls} terminations_exhausted_beam_routing_bridge_vector_exact={termination_counts:?}",
            shape.group,
            shape.candidate_count,
            prefilter_latencies[p50_index].as_micros(),
            prefilter_latencies[p95_index].as_micros(),
            end_to_end_latencies[p50_index].as_micros(),
            end_to_end_latencies[p95_index].as_micros(),
            vector_increment_latencies[p95_index].as_micros(),
        );
        if recall < 0.92 {
            accuracy_failures.push((shape.group, recall));
        }
    }

    db.close()
        .await
        .expect("million-row traversal benchmark writer closes cleanly");
    assert!(
        accuracy_failures.is_empty(),
        "million-row prefilter recall gates failed: {accuracy_failures:?}"
    );
}

/// Runs typed resource-block, raised-limit retry, and abort cleanup for every family.
pub(super) async fn run_blocked_limits() {
    assert_eq!(
        LIMIT_ENTITY_COUNT,
        SearchIndexBackfillLimits::default()
            .batch()
            .max_entities()
            .get(),
        "the limit fixture must fill one configured source batch"
    );
    let token = ProcessLocalDatabaseToken::new("index-lifecycle-production-blocked-limit-scale")
        .expect("blocked-limit database token is valid");
    let mut db = HelixDB::open_with_config(
        HelixDbSource::InMemoryToken {
            token: token.clone(),
        },
        blocked_limit_config(),
    )
    .await
    .expect("blocked-limit writer opens");
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("blocked-limit database should be a writer");
    };
    let ids = writer
        .node_ids()
        .allocate_batch(
            u64::try_from(LIMIT_ENTITY_COUNT).expect("blocked-limit entity count fits u64"),
        )
        .await
        .expect("blocked-limit IDs are durably reserved");
    assert_eq!(
        ids,
        0..u64::try_from(LIMIT_ENTITY_COUNT).expect("blocked-limit entity count fits u64")
    );
    seed_scope(
        writer.db(),
        DataScope::LegacyUnscoped,
        0,
        LIMIT_ENTITY_COUNT,
        unscoped_properties,
    )
    .await;

    let property_key = |property| {
        catalog::ScopedPropertyKey::try_new(LABEL, property)
            .expect("blocked-limit property key is valid")
    };
    let cases = [
        (
            PublicIndexFamily::Secondary,
            ir::IndexDdlCreateSpec::NodeEquality {
                key: property_key(NON_UNIQUE_PROPERTY),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
            ir::IndexDdlDropSpec::NodeEquality {
                key: property_key(NON_UNIQUE_PROPERTY),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
        ),
        (
            PublicIndexFamily::Vector,
            ir::IndexDdlCreateSpec::NodeVector {
                key: property_key(VECTOR_PROPERTY),
                dimension: ir::VectorIndexDimension::new(VECTOR_DIMENSION)
                    .expect("blocked-limit vector dimension is positive"),
                metric: ir::VectorIndexMetric::Euclidean,
                scope: catalog::SearchIndexScope::Unscoped,
            },
            ir::IndexDdlDropSpec::NodeVector {
                key: property_key(VECTOR_PROPERTY),
            },
        ),
        (
            PublicIndexFamily::Text,
            ir::IndexDdlCreateSpec::NodeText {
                key: property_key(TEXT_PROPERTY),
                scope: catalog::SearchIndexScope::Unscoped,
            },
            ir::IndexDdlDropSpec::NodeText {
                key: property_key(TEXT_PROPERTY),
            },
        ),
    ];

    for (family, create, drop) in cases {
        let blocked = create_to_blocked(&db, DataScope::LegacyUnscoped, create.clone()).await;
        let IndexOperationStatus::Blocked {
            common,
            blocker_code,
            ..
        } = blocked
        else {
            unreachable!("create_to_blocked returns only Blocked")
        };
        assert_eq!(common.family, family);
        assert_eq!(
            blocker_code,
            crate::index_lifecycle::IndexOperationBlockerCode::OversizedEntity
        );
        let blocked_progress = common.progress;
        let operation_id = common.operation_id;
        db.close()
            .await
            .expect("blocked-limit writer closes before raised-limit retry");

        db = HelixDB::open_with_config(
            HelixDbSource::InMemoryToken {
                token: token.clone(),
            },
            DbConfig::new(),
        )
        .await
        .expect("raised-limit writer reopens");
        let retried = db
            .retry_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("blocked operation requeues at its exact checkpoint");
        assert_eq!(retried.common().progress, blocked_progress);
        let succeeded = wait_for_terminal(
            &db,
            DataScope::LegacyUnscoped,
            operation_id,
            ExpectedTerminal::Succeeded,
        )
        .await;
        assert_eq!(succeeded.common().family, family);
        drop_index(&db, DataScope::LegacyUnscoped, drop.clone()).await;
        db.close()
            .await
            .expect("raised-limit writer closes before abort fixture");

        db = HelixDB::open_with_config(
            HelixDbSource::InMemoryToken {
                token: token.clone(),
            },
            blocked_limit_config(),
        )
        .await
        .expect("blocked-limit writer reopens for abort fixture");
        let blocked_abort = create_to_blocked(&db, DataScope::LegacyUnscoped, create).await;
        assert_eq!(blocked_abort.common().family, family);
        let abort_operation_id = blocked_abort.common().operation_id;
        db.close()
            .await
            .expect("blocked-limit writer closes before abort cleanup");

        db = HelixDB::open_with_config(
            HelixDbSource::InMemoryToken {
                token: token.clone(),
            },
            DbConfig::new(),
        )
        .await
        .expect("raised-limit writer reopens for abort cleanup");
        db.abort_index_operation(DataScope::LegacyUnscoped, abort_operation_id)
            .await
            .expect("blocked build enters abort cleanup");
        let aborted = wait_for_terminal(
            &db,
            DataScope::LegacyUnscoped,
            abort_operation_id,
            ExpectedTerminal::Aborted,
        )
        .await;
        assert_eq!(aborted.common().family, family);
        db.close()
            .await
            .expect("abort-cleanup writer closes before the next family");
        db = HelixDB::open_with_config(
            HelixDbSource::InMemoryToken {
                token: token.clone(),
            },
            blocked_limit_config(),
        )
        .await
        .expect("blocked-limit writer reopens for the next family");
    }

    assert_no_lifecycle_residue(&db, &[DataScope::LegacyUnscoped]).await;
    db.close()
        .await
        .expect("blocked-limit scale writer closes cleanly");
}
