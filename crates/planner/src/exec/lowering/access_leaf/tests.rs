use super::*;
use crate::exec::{ExecEdgeAccessPlan, ExecNodeAccessPlan, ExecPlanError};
use crate::{ir, properties};

fn element_ids(values: Vec<u64>) -> ir::ElementIds {
    ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
}

#[test]
fn node_simple_access_leaf_accepts_only_direct_leaf_shapes() {
    let simple = ir::NodeAccessPlan::AllScan;
    let complex = ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![1]),
    };

    assert!(matches!(
        node_exec_access(SimpleNodeAccessLeaf::try_from(&simple).unwrap()),
        ExecNodeAccessPlan::AllScan
    ));
    assert_eq!(
        SimpleNodeAccessLeaf::try_from(&complex).unwrap_err(),
        ExecPlanError::UnsupportedSimpleAccessLeaf {
            element: properties::ElementKind::Node,
        }
    );
}

#[test]
fn edge_simple_access_leaf_accepts_only_direct_leaf_shapes() {
    let simple = ir::EdgeAccessPlan::AllScan;
    let complex = ir::EdgeAccessPlan::PointIds {
        ids: element_ids(vec![1]),
    };

    assert!(matches!(
        edge_exec_access(SimpleEdgeAccessLeaf::try_from(&simple).unwrap()),
        ExecEdgeAccessPlan::AllScan
    ));
    assert_eq!(
        SimpleEdgeAccessLeaf::try_from(&complex).unwrap_err(),
        ExecPlanError::UnsupportedSimpleAccessLeaf {
            element: properties::ElementKind::Edge,
        }
    );
}
