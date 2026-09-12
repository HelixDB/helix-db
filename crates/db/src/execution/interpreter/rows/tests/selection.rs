use super::super::*;
use helix_ast::{expr, value};
use helix_planner::{context, ir};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

struct Input {
    key: usize,
    drops: Arc<AtomicUsize>,
}
impl Drop for Input {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct SelectionModel {
    visited: Vec<usize>,
    retained: Vec<Input>,
    error_at: Option<usize>,
    capacity: usize,
}
impl r::SelectionEvaluator<r::Expression> for SelectionModel {
    type Row = Input;
    type Error = &'static str;
    async fn evaluate(
        &mut self,
        row: &Input,
        _expression: &r::Expression,
    ) -> std::result::Result<r::Selection, Self::Error> {
        self.visited.push(row.key);
        if self.error_at == Some(self.visited.len()) {
            return Err("evaluate");
        }
        Ok(match row.key {
            0 => r::Selection::False,
            1 => r::Selection::Unknown,
            _ => r::Selection::True,
        })
    }
    fn retain(&mut self, row: Input) -> std::result::Result<(), Self::Error> {
        if self.retained.len() == self.capacity {
            return Err("admission");
        }
        self.retained.push(row);
        Ok(())
    }
}

#[tokio::test]
async fn selection_moves_rows_once_and_preserves_duplicates_errors_and_cleanup() {
    let program =
        r::SelectionProgram::new(r::Expression::Literal(r::Value::Boolean(true))).unwrap();
    for (error_at, capacity, expected_error, visited, kept) in [
        (None, 10, None, vec![0, 1, 2, 2, 3], vec![2, 2, 3]),
        (Some(4), 10, Some("evaluate"), vec![0, 1, 2, 2], vec![2]),
        (None, 1, Some("admission"), vec![0, 1, 2, 2], vec![2]),
    ] {
        let drops = Arc::new(AtomicUsize::new(0));
        let input = [0, 1, 2, 2, 3].map(|key| Input {
            key,
            drops: Arc::clone(&drops),
        });
        let mut model = SelectionModel {
            visited: Vec::new(),
            retained: Vec::new(),
            error_at,
            capacity,
        };
        assert_eq!(
            program.select(input, &mut model).await.err(),
            expected_error
        );
        assert_eq!(model.visited, visited);
        assert_eq!(
            model.retained.iter().map(|row| row.key).collect::<Vec<_>>(),
            kept
        );
        assert_eq!(drops.load(Ordering::SeqCst), 5 - model.retained.len());
        drop(model);
        assert_eq!(drops.load(Ordering::SeqCst), 5);
    }
}

#[test]
fn selection_construction_and_deserialization_rebuild_validated_dependencies() {
    let expression = r::Expression::Binary(
        r::Binary::Equal,
        Box::new(r::Expression::Slot(r::Slot(2))),
        Box::new(r::Expression::Literal(r::Value::Integer(7))),
    );
    let program = r::SelectionProgram::new(expression.clone()).unwrap();
    assert_eq!(program.references(), &BTreeSet::from([r::Slot(2)]));
    assert_eq!(
        program.validate_input(&BTreeSet::new()).unwrap_err().detail,
        "UnboundSlot"
    );
    program
        .validate_input(&BTreeSet::from([r::Slot(2)]))
        .unwrap();
    let wire = serde_json::to_value(&program).unwrap();
    assert_eq!(wire, serde_json::to_value(expression).unwrap());
    let decoded: r::SelectionProgram = serde_json::from_value(wire).unwrap();
    assert_eq!(decoded, program);
    let invalid = r::Expression::Function(r::Function::Id, Vec::new());
    assert!(r::SelectionProgram::new(invalid.clone()).is_err());
    assert!(
        serde_json::from_value::<r::SelectionProgram>(serde_json::to_value(invalid).unwrap())
            .is_err()
    );

    let native_ast = expr::Predicate::eq("key", 7);
    let native = ir::PredicatePlan::new(native_ast.clone()).unwrap();
    assert_eq!(
        native.program().references(),
        &BTreeSet::from([ir::native::CURRENT])
    );
    native
        .program()
        .validate_input(&BTreeSet::from([ir::native::CURRENT]))
        .unwrap();
    assert_eq!(
        serde_json::to_value(&native).unwrap(),
        serde_json::to_value(native_ast).unwrap()
    );
    let decoded: ir::PredicatePlan =
        serde_json::from_value(serde_json::to_value(&native).unwrap()).unwrap();
    assert_eq!(decoded.program(), native.program());
}

#[tokio::test]
async fn native_and_cypher_selection_share_execution_and_keep_null_semantics() {
    let db =
        crate::execution::interpreter::test_support::open_db("common-selection-semantics").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(65536));
    let native = ir::PredicatePlan::new(expr::Predicate::Eq {
        left: expr::Expr::Constant(value::PropertyValue::Null),
        right: expr::Expr::Constant(value::PropertyValue::Null),
    })
    .unwrap();
    let input = vec![crate::execution::interpreter::ExecutionRow::empty(); 3];
    assert_eq!(
        ctx.select_native_rows(input, &native).await.unwrap().len(),
        3
    );
    let cypher = r::SelectionProgram::new(r::Expression::Binary(
        r::Binary::Equal,
        Box::new(r::Expression::Literal(r::Value::Null)),
        Box::new(r::Expression::Literal(r::Value::Null)),
    ))
    .unwrap();
    let rows = Rows::new(vec![vec![r::Value::Integer(1)]; 3], ctx.row_budget()).unwrap();
    assert!(ctx
        .filter_relation(
            rows,
            &cypher,
            &BTreeMap::new(),
            Limits {
                batch_rows: 2,
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_empty());
    assert_eq!(ctx.row_budget().available(), 65536);
    drop(ctx);
    db.close().await.unwrap();
}
