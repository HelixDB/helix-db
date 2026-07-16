//! Shared database and plan fixtures for interpreter unit tests.

use helix_ast::value::PropertyValue;
use helix_planner::{context, cost, exec, ir, properties, trace};
use slatedb::object_store::{memory::InMemory, ObjectStore};

use super::{ElementRef, ExecutionRow, ExecutionValue};
use crate::{config, search, HelixDB, HelixDbSource};

use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct TestDbConfig {
    database: String,
    object_store: Arc<dyn ObjectStore>,
    indexes: config::IndexConfig,
}

impl TestDbConfig {
    pub(crate) fn new(database: &str, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            database: database.to_string(),
            object_store,
            indexes: config::IndexConfig::new(),
        }
    }

    pub(crate) fn with_equality_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_equality_index(label, property)
            .expect("valid equality index");
        self
    }

    pub(crate) fn with_edge_equality_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_edge_equality_index(label, property)
            .expect("valid edge equality index");
        self
    }

    pub(crate) fn with_range_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_range_index(label, property)
            .expect("valid range index");
        self
    }

    pub(crate) fn with_range_desc_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_range_desc_index(label, property)
            .expect("valid descending range index");
        self
    }

    pub(crate) fn with_edge_range_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_edge_range_index(label, property)
            .expect("valid edge range index");
        self
    }

    pub(crate) fn with_edge_range_desc_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_edge_range_desc_index(label, property)
            .expect("valid descending edge range index");
        self
    }

    pub(crate) fn with_node_vector_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: usize,
        metric: search::vector::VectorDistanceMetric,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_node_vector_index(label, property, dimension, metric)
            .expect("valid node vector index");
        self
    }

    pub(crate) fn with_edge_vector_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
        dimension: usize,
        metric: search::vector::VectorDistanceMetric,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_edge_vector_index(label, property, dimension, metric)
            .expect("valid edge vector index");
        self
    }

    pub(crate) fn with_node_text_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_node_text_index(label, property)
            .expect("valid node text index");
        self
    }

    pub(crate) fn with_edge_text_index(
        mut self,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        self.indexes = self
            .indexes
            .with_edge_text_index(label, property)
            .expect("valid edge text index");
        self
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }
}

pub(crate) async fn open_db(database: &str) -> HelixDB {
    HelixDB::open(HelixDbSource::InMemory {
        database: database.to_string(),
    })
    .await
    .expect("db opens")
}

pub(crate) async fn open_db_with_config(config: TestDbConfig) -> HelixDB {
    HelixDB::open_with_object_store_and_index_config_for_tests(
        config.database.clone(),
        config.object_store(),
        config.indexes.clone(),
    )
    .await
    .expect("db opens")
}

pub(crate) async fn open_reader_with_config(config: TestDbConfig) -> HelixDB {
    HelixDB::open_reader_with_object_store_and_index_config_for_tests(
        config.database.clone(),
        config.object_store(),
        config.indexes.clone(),
    )
    .await
    .expect("reader opens")
}

pub(crate) fn in_memory_config(database: &str) -> TestDbConfig {
    TestDbConfig::new(database, Arc::new(InMemory::new()))
}

pub(crate) fn in_memory_config_with_store(
    database: &str,
    object_store: Arc<dyn ObjectStore>,
) -> TestDbConfig {
    TestDbConfig::new(database, object_store)
}

pub(crate) fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("valid test name")
}

pub(crate) fn step(
    id: usize,
    dependencies: Vec<exec::ExecStepId>,
    op: exec::ExecOp,
) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("positive step id"),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

pub(crate) fn executable(
    kind: ir::PlanKind,
    steps: Vec<exec::ExecStep>,
    root: usize,
) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("non-empty test plan"),
        exec::ExecStepId::new(root).expect("positive root"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("valid executable test plan")
}

pub(crate) fn subplan(steps: Vec<exec::ExecStep>, root: usize) -> exec::ExecutableSubplan {
    exec::ExecutableSubplan::new(
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("non-empty test subplan"),
        exec::ExecStepId::new(root).expect("positive subplan root"),
    )
    .expect("valid executable test subplan")
}

pub(crate) fn assignments(items: Vec<(&str, PropertyValue)>) -> ir::PropertyAssignments {
    ir::PropertyAssignments::try_from_vec(
        items
            .into_iter()
            .map(|(name, value)| (self::name(name), ir::PropertyInputPlan::Value(value)))
            .collect(),
    )
    .expect("valid property assignments")
}

pub(crate) fn ids(values: Vec<u64>) -> ir::ElementIds {
    ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).expect("test ids are non-empty"))
        .expect("test ids are valid")
}

pub(crate) async fn add_user(db: &HelixDB, username: &str) -> u64 {
    add_node_with_properties(db, "User", vec![("name", PropertyValue::from(username))]).await
}

pub(crate) async fn add_node_with_properties(
    db: &HelixDB,
    label: &str,
    properties: Vec<(&str, PropertyValue)>,
) -> u64 {
    let plan = executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::AddNodeSource {
                    label: name(label),
                    properties: assignments(properties),
                },
            },
        )],
        1,
    );
    let result = db
        .execute(&plan, context::ParamBindings::default())
        .await
        .expect("node write succeeds");
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("node write should return a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Node(id)),
        ..
    }) = rows.first()
    else {
        panic!("node write should return a node row");
    };
    *id
}

pub(crate) async fn add_edge(db: &HelixDB, from: u64, to: u64, label: &str) -> u64 {
    add_edge_with_properties(db, from, to, label, Vec::new()).await
}

pub(crate) async fn add_edge_with_properties(
    db: &HelixDB,
    from: u64,
    to: u64,
    label: &str,
    properties: Vec<(&str, PropertyValue)>,
) -> u64 {
    let from_param = name("from");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let plan = executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: from_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddEdge {
                        label: name(label),
                        to: ir::NodeTargetPlan::PointIds { ids: ids(vec![to]) },
                        properties: assignments(properties),
                    },
                },
            ),
        ],
        2,
    );
    let result = db
        .execute(
            &plan,
            context::ParamBindings::default()
                .with_value(from_param, PropertyValue::I64(from as i64)),
        )
        .await
        .expect("edge write succeeds");
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("edge write should return a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Edge(id)),
        ..
    }) = rows.first()
    else {
        panic!("edge write should return an edge row");
    };
    *id
}
