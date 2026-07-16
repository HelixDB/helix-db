//! Runtime ID-source extraction for executable access.
//!
//! Planner access sources can read element IDs from static parameters, query
//! transport parameters, or previously bound streams. This module keeps those
//! shape checks away from the element-specific access dispatcher.

use helix_ast::query::QueryValue;
use helix_ast::value::PropertyValue as AstPropertyValue;
use helix_planner::ir;

use super::super::{ElementRef, ExecutionContext, ExecutionRow, ExecutionValue};
use crate::error::{HelixDbError, Result};

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn param_ids(
        &self,
        param: &ir::NonEmptyString,
    ) -> Result<Vec<u64>> {
        if let Some(value) = self.params.values.get(param) {
            return ast_ids(value, param);
        }
        let Some(value) = self.params.query_values.get(param) else {
            return Err(HelixDbError::Query(format!(
                "parameter `{param}` is not bound"
            )));
        };
        query_parameter_ids(value, param)
    }

    pub(in crate::execution::interpreter) fn variable_nodes(
        &self,
        variable: &ir::NonEmptyString,
    ) -> Result<Vec<u64>> {
        let value = self.variable_value(variable)?;
        node_ids_from_variable_value(variable, value)
    }

    pub(in crate::execution::interpreter) fn access_variable_nodes(
        &self,
        variable: &ir::NonEmptyString,
    ) -> Result<Vec<u64>> {
        self.variables
            .get(variable)
            .map_or(Ok(Vec::new()), |value| {
                node_ids_from_variable_value(variable, value)
            })
    }

    pub(in crate::execution::interpreter) fn variable_edges(
        &self,
        variable: &ir::NonEmptyString,
    ) -> Result<Vec<u64>> {
        let value = self.variable_value(variable)?;
        edge_ids_from_variable_value(variable, value)
    }

    pub(in crate::execution::interpreter) fn access_variable_edges(
        &self,
        variable: &ir::NonEmptyString,
    ) -> Result<Vec<u64>> {
        self.variables
            .get(variable)
            .map_or(Ok(Vec::new()), |value| {
                edge_ids_from_variable_value(variable, value)
            })
    }
}

fn node_ids_from_variable_value(
    variable: &ir::NonEmptyString,
    value: &ExecutionValue,
) -> Result<Vec<u64>> {
    match value {
        ExecutionValue::Stream(rows) => Ok(node_ids_from_rows(rows)),
        ExecutionValue::FoldedStream(folded) => Ok(node_ids_from_rows(folded.rows())),
        other @ (ExecutionValue::Count(_)
        | ExecutionValue::Bool(_)
        | ExecutionValue::Scalars(_)
        | ExecutionValue::IndexDdlReceipt(_)
        | ExecutionValue::IndexOperationStatus(_)) => Err(HelixDbError::Query(format!(
            "variable `{variable}` is not a node stream: {other:?}"
        ))),
    }
}

fn edge_ids_from_variable_value(
    variable: &ir::NonEmptyString,
    value: &ExecutionValue,
) -> Result<Vec<u64>> {
    match value {
        ExecutionValue::Stream(rows) => Ok(edge_ids_from_rows(rows)),
        ExecutionValue::FoldedStream(folded) => Ok(edge_ids_from_rows(folded.rows())),
        other @ (ExecutionValue::Count(_)
        | ExecutionValue::Bool(_)
        | ExecutionValue::Scalars(_)
        | ExecutionValue::IndexDdlReceipt(_)
        | ExecutionValue::IndexOperationStatus(_)) => Err(HelixDbError::Query(format!(
            "variable `{variable}` is not an edge stream: {other:?}"
        ))),
    }
}

fn node_ids_from_rows(rows: &[ExecutionRow]) -> Vec<u64> {
    rows.iter()
        .filter_map(|row| match row.current {
            Some(ElementRef::Node(id)) => Some(id),
            Some(ElementRef::Edge(_)) | None => None,
        })
        .collect()
}

fn edge_ids_from_rows(rows: &[ExecutionRow]) -> Vec<u64> {
    rows.iter()
        .filter_map(|row| match row.current {
            Some(ElementRef::Edge(id)) => Some(id),
            Some(ElementRef::Node(_)) | None => None,
        })
        .collect()
}

fn ast_ids(value: &AstPropertyValue, param: &ir::NonEmptyString) -> Result<Vec<u64>> {
    match value {
        AstPropertyValue::I64(id) if *id >= 0 => Ok(vec![*id as u64]),
        AstPropertyValue::I64Array(values) => values
            .iter()
            .map(|id| non_negative_id(*id, param))
            .collect(),
        AstPropertyValue::Array(values) => values
            .iter()
            .map(|value| {
                let AstPropertyValue::I64(id) = value else {
                    return Err(HelixDbError::Query(format!(
                        "parameter `{param}` must contain integer ids"
                    )));
                };
                non_negative_id(*id, param)
            })
            .collect(),
        AstPropertyValue::Null
        | AstPropertyValue::Bool(_)
        | AstPropertyValue::I64(_)
        | AstPropertyValue::DateTime(_)
        | AstPropertyValue::F64(_)
        | AstPropertyValue::F32(_)
        | AstPropertyValue::String(_)
        | AstPropertyValue::Bytes(_)
        | AstPropertyValue::F64Array(_)
        | AstPropertyValue::F32Array(_)
        | AstPropertyValue::StringArray(_)
        | AstPropertyValue::Object(_) => Err(HelixDbError::Query(format!(
            "parameter `{param}` must be an integer id or array of integer ids"
        ))),
    }
}

fn query_parameter_ids(value: &QueryValue, param: &ir::NonEmptyString) -> Result<Vec<u64>> {
    match value {
        QueryValue::I64(id) if *id >= 0 => Ok(vec![*id as u64]),
        QueryValue::Array(values) => values
            .iter()
            .map(|value| {
                let QueryValue::I64(id) = value else {
                    return Err(HelixDbError::Query(format!(
                        "parameter `{param}` must contain integer ids"
                    )));
                };
                non_negative_id(*id, param)
            })
            .collect(),
        QueryValue::Null
        | QueryValue::Bool(_)
        | QueryValue::I64(_)
        | QueryValue::F64(_)
        | QueryValue::F32(_)
        | QueryValue::String(_)
        | QueryValue::Object(_) => Err(HelixDbError::Query(format!(
            "parameter `{param}` must be an integer id or array of integer ids"
        ))),
    }
}

fn non_negative_id(id: i64, param: &ir::NonEmptyString) -> Result<u64> {
    if id < 0 {
        return Err(HelixDbError::Query(format!(
            "parameter `{param}` contains negative id {id}"
        )));
    }
    Ok(id as u64)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use helix_planner::context;

    use super::super::super::test_support;
    use super::super::super::{ElementRef, ExecutionContext, FoldedStream};
    use super::super::super::{
        ExecutionRow, ExecutionValue, RowPath, RowSack, RowVirtualProperties,
    };
    use super::*;

    fn name(value: &str) -> ir::NonEmptyString {
        test_support::name(value)
    }

    fn row(current: Option<ElementRef>) -> ExecutionRow {
        ExecutionRow {
            current,
            virtual_properties: RowVirtualProperties::empty(),
            bindings: BTreeMap::new(),
            binding_virtual_properties: BTreeMap::new(),
            path: RowPath::empty(),
            path_visible: false,
            sack: RowSack::empty(),
        }
    }

    fn error_message<T>(result: Result<T>) -> String {
        match result {
            Ok(_) => panic!("operation should fail"),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn ast_id_parameters_accept_supported_integer_shapes() {
        let param = name("ids");

        assert_eq!(ast_ids(&AstPropertyValue::I64(7), &param).unwrap(), vec![7]);
        assert_eq!(
            ast_ids(&AstPropertyValue::I64Array(vec![1, 2, 3]), &param).unwrap(),
            vec![1, 2, 3]
        );
        assert_eq!(
            ast_ids(
                &AstPropertyValue::Array(vec![AstPropertyValue::I64(4), AstPropertyValue::I64(5)]),
                &param
            )
            .unwrap(),
            vec![4, 5]
        );
    }

    #[test]
    fn ast_id_parameters_reject_negative_and_non_integer_shapes() {
        let param = name("ids");

        assert!(error_message(ast_ids(&AstPropertyValue::I64(-1), &param))
            .contains("must be an integer id or array of integer ids"));
        assert!(
            error_message(ast_ids(&AstPropertyValue::I64Array(vec![1, -2]), &param))
                .contains("contains negative id -2")
        );
        assert!(error_message(ast_ids(
            &AstPropertyValue::Array(vec![
                AstPropertyValue::I64(1),
                AstPropertyValue::String("not-an-id".to_string()),
            ]),
            &param,
        ))
        .contains("must contain integer ids"));
        assert!(error_message(ast_ids(
            &AstPropertyValue::String("not-an-id".to_string()),
            &param
        ))
        .contains("must be an integer id or array of integer ids"));
    }

    #[test]
    fn query_id_parameters_accept_supported_integer_shapes() {
        let param = name("ids");

        assert_eq!(
            query_parameter_ids(&QueryValue::I64(9), &param).unwrap(),
            vec![9]
        );
        assert_eq!(
            query_parameter_ids(
                &QueryValue::Array(vec![QueryValue::I64(10), QueryValue::I64(11),]),
                &param,
            )
            .unwrap(),
            vec![10, 11]
        );
    }

    #[test]
    fn query_id_parameters_reject_negative_and_non_integer_shapes() {
        let param = name("ids");

        assert!(
            error_message(query_parameter_ids(&QueryValue::I64(-1), &param))
                .contains("must be an integer id or array of integer ids")
        );
        assert!(error_message(query_parameter_ids(
            &QueryValue::Array(vec![QueryValue::I64(1), QueryValue::I64(-2),]),
            &param,
        ))
        .contains("contains negative id -2"));
        assert!(error_message(query_parameter_ids(
            &QueryValue::Array(vec![
                QueryValue::I64(1),
                QueryValue::String("not-an-id".to_string()),
            ]),
            &param,
        ))
        .contains("must contain integer ids"));
        assert!(error_message(query_parameter_ids(
            &QueryValue::String("not-an-id".to_string()),
            &param
        ))
        .contains("must be an integer id or array of integer ids"));
    }

    #[tokio::test]
    async fn param_ids_prefer_static_bindings_over_dynamic_bindings() {
        let db = test_support::open_db("access-params-static-first").await;
        let param = name("ids");
        let ctx = ExecutionContext::new(
            &db,
            context::ParamBindings::default()
                .with_value(param.clone(), AstPropertyValue::I64Array(vec![1, 2]))
                .with_query_value(param.clone(), QueryValue::I64(-1)),
        );

        assert_eq!(ctx.param_ids(&param).unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn param_ids_use_dynamic_bindings_when_static_is_absent() {
        let db = test_support::open_db("access-params-query").await;
        let param = name("ids");
        let ctx = ExecutionContext::new(
            &db,
            context::ParamBindings::default().with_query_value(
                param.clone(),
                QueryValue::Array(vec![QueryValue::I64(3), QueryValue::I64(4)]),
            ),
        );

        assert_eq!(ctx.param_ids(&param).unwrap(), vec![3, 4]);
    }

    #[tokio::test]
    async fn param_ids_reject_missing_bindings() {
        let db = test_support::open_db("access-params-missing").await;
        let param = name("missing");
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

        assert!(error_message(ctx.param_ids(&param)).contains("is not bound"));
    }

    #[tokio::test]
    async fn variable_id_sources_extract_matching_current_rows_only() {
        let db = test_support::open_db("access-params-stream-vars").await;
        let nodes = name("nodes");
        let edges = name("edges");
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.variables.insert(
            nodes.clone(),
            ExecutionValue::Stream(vec![
                row(Some(ElementRef::Node(1))),
                row(Some(ElementRef::Edge(9))),
                row(None),
                row(Some(ElementRef::Node(2))),
            ]),
        );
        ctx.variables.insert(
            edges.clone(),
            ExecutionValue::Stream(vec![
                row(Some(ElementRef::Node(7))),
                row(Some(ElementRef::Edge(3))),
                row(None),
                row(Some(ElementRef::Edge(4))),
            ]),
        );

        assert_eq!(ctx.variable_nodes(&nodes).unwrap(), vec![1, 2]);
        assert_eq!(ctx.variable_edges(&edges).unwrap(), vec![3, 4]);
    }

    #[tokio::test]
    async fn variable_id_sources_read_folded_stream_rows() {
        let db = test_support::open_db("access-params-folded-vars").await;
        let nodes = name("folded_nodes");
        let edges = name("folded_edges");
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.variables.insert(
            nodes.clone(),
            ExecutionValue::FoldedStream(FoldedStream::new(vec![
                row(Some(ElementRef::Node(5))),
                row(Some(ElementRef::Edge(6))),
            ])),
        );
        ctx.variables.insert(
            edges.clone(),
            ExecutionValue::FoldedStream(FoldedStream::new(vec![
                row(Some(ElementRef::Node(7))),
                row(Some(ElementRef::Edge(8))),
            ])),
        );

        assert_eq!(ctx.variable_nodes(&nodes).unwrap(), vec![5]);
        assert_eq!(ctx.variable_edges(&edges).unwrap(), vec![8]);
    }

    #[tokio::test]
    async fn variable_id_sources_reject_missing_or_scalar_variables() {
        let db = test_support::open_db("access-params-var-errors").await;
        let missing = name("missing");
        let scalar = name("scalar");
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.variables
            .insert(scalar.clone(), ExecutionValue::Bool(true));

        assert!(error_message(ctx.variable_nodes(&missing)).contains("is not bound"));
        assert_eq!(
            ctx.access_variable_nodes(&missing).unwrap(),
            Vec::<u64>::new()
        );
        assert_eq!(
            ctx.access_variable_edges(&missing).unwrap(),
            Vec::<u64>::new()
        );
        assert!(error_message(ctx.variable_edges(&scalar)).contains("is not an edge stream"));
        assert!(error_message(ctx.access_variable_nodes(&scalar)).contains("is not a node stream"));
    }
}
