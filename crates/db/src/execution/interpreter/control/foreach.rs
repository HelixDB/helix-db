//! Executable `ForEach` batch interpretation.

use helix_ast::query::QueryValue;
use helix_ast::value::PropertyValue as AstPropertyValue;
use helix_planner::{context, exec, ir};

use super::super::{ExecutionContext, ExecutionValue};
use crate::error::{HelixDbError, Result};

#[derive(Debug, Clone, PartialEq)]
enum ForEachParamFrame {
    Ast(Vec<(ir::NonEmptyString, AstPropertyValue)>),
    Query(Vec<(ir::NonEmptyString, QueryValue)>),
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn execute_foreach(
        &mut self,
        param: &ir::NonEmptyString,
        body: &exec::ExecutableSubplan,
    ) -> Result<ExecutionValue> {
        let original_params = self.params.clone();
        let mut base_params = original_params.clone();
        base_params.values.remove(param);
        base_params.query_values.remove(param);
        let frames = self.foreach_param_frames(param)?;
        let result = async {
            let mut last = None;
            for frame in frames {
                self.params = base_params.clone();
                frame.apply_to(&mut self.params);
                last = Some(self.execute_subplan(body).await?);
            }
            Ok(last.unwrap_or_else(|| ExecutionValue::Stream(Vec::new())))
        }
        .await;
        self.params = original_params;
        result
    }

    fn foreach_param_frames(&self, param: &ir::NonEmptyString) -> Result<Vec<ForEachParamFrame>> {
        if let Some(value) = self.params.values.get(param) {
            return ast_foreach_frames(param, value);
        }

        if let Some(value) = self.params.query_values.get(param) {
            return query_parameter_frames(param, value);
        }

        Err(HelixDbError::Query(format!(
            "foreach parameter `{param}` is not bound"
        )))
    }
}

impl ForEachParamFrame {
    fn apply_to(self, params: &mut context::ParamBindings) {
        match self {
            Self::Ast(fields) => {
                for (name, value) in fields {
                    params.query_values.remove(&name);
                    params.values.insert(name, value);
                }
            }
            Self::Query(fields) => {
                for (name, value) in fields {
                    params.values.remove(&name);
                    params.query_values.insert(name, value);
                }
            }
        }
    }
}

fn ast_foreach_frames(
    param: &ir::NonEmptyString,
    value: &AstPropertyValue,
) -> Result<Vec<ForEachParamFrame>> {
    let AstPropertyValue::Array(rows) = value else {
        return Err(HelixDbError::Query(format!(
            "foreach batch entry expected `{param}` to be an array of objects"
        )));
    };
    rows.iter()
        .map(|row| match row {
            AstPropertyValue::Object(fields) => fields
                .iter()
                .map(|(name, value)| {
                    let Some(name) = ir::NonEmptyString::new(name.clone()) else {
                        return Err(HelixDbError::Query(
                            "foreach object field names must not be empty".to_string(),
                        ));
                    };
                    Ok((name, value.clone()))
                })
                .collect::<Result<Vec<_>>>()
                .map(ForEachParamFrame::Ast),
            AstPropertyValue::Null
            | AstPropertyValue::Bool(_)
            | AstPropertyValue::I64(_)
            | AstPropertyValue::DateTime(_)
            | AstPropertyValue::F64(_)
            | AstPropertyValue::F32(_)
            | AstPropertyValue::String(_)
            | AstPropertyValue::Bytes(_)
            | AstPropertyValue::I64Array(_)
            | AstPropertyValue::F64Array(_)
            | AstPropertyValue::F32Array(_)
            | AstPropertyValue::StringArray(_)
            | AstPropertyValue::Array(_) => Err(HelixDbError::Query(format!(
                "foreach batch entry expected `{param}` items to be objects"
            ))),
        })
        .collect()
}

fn query_parameter_frames(
    param: &ir::NonEmptyString,
    value: &QueryValue,
) -> Result<Vec<ForEachParamFrame>> {
    let QueryValue::Array(rows) = value else {
        return Err(HelixDbError::Query(format!(
            "foreach batch entry expected `{param}` to be an array of objects"
        )));
    };
    rows.iter()
        .map(|row| match row {
            QueryValue::Object(fields) => fields
                .iter()
                .map(|(name, value)| {
                    let Some(name) = ir::NonEmptyString::new(name.clone()) else {
                        return Err(HelixDbError::Query(
                            "foreach object field names must not be empty".to_string(),
                        ));
                    };
                    Ok((name, value.clone()))
                })
                .collect::<Result<Vec<_>>>()
                .map(ForEachParamFrame::Query),
            QueryValue::Null
            | QueryValue::Bool(_)
            | QueryValue::I64(_)
            | QueryValue::F64(_)
            | QueryValue::F32(_)
            | QueryValue::String(_)
            | QueryValue::Array(_) => Err(HelixDbError::Query(format!(
                "foreach batch entry expected `{param}` items to be objects"
            ))),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::super::test_support;
    use super::*;

    fn name(value: &str) -> ir::NonEmptyString {
        test_support::name(value)
    }

    fn noop_subplan() -> exec::ExecutableSubplan {
        test_support::subplan(
            vec![test_support::step(1, Vec::new(), exec::ExecOp::Noop)],
            1,
        )
    }

    async fn static_frames(value: AstPropertyValue) -> Result<Vec<ForEachParamFrame>> {
        let db = test_support::open_db("foreach-static-values").await;
        let param = name("item");
        let ctx = ExecutionContext::new(
            &db,
            context::ParamBindings::default().with_value(param.clone(), value),
        );

        ctx.foreach_param_frames(&param)
    }

    async fn query_frames(value: QueryValue) -> Result<Vec<ForEachParamFrame>> {
        let db = test_support::open_db("foreach-query-values").await;
        let param = name("item");
        let ctx = ExecutionContext::new(
            &db,
            context::ParamBindings::default().with_query_value(param.clone(), value),
        );

        ctx.foreach_param_frames(&param)
    }

    fn error_message<T>(result: Result<T>) -> String {
        match result {
            Ok(_) => panic!("operation should fail"),
            Err(err) => err.to_string(),
        }
    }

    #[tokio::test]
    async fn static_array_parameters_expand_object_rows_to_ast_frames() {
        assert_eq!(
            static_frames(AstPropertyValue::array([
                AstPropertyValue::object([
                    ("externalId", AstPropertyValue::from("u-1")),
                    ("tier", AstPropertyValue::from("premium")),
                ]),
                AstPropertyValue::object([
                    ("externalId", AstPropertyValue::from("u-2")),
                    ("tier", AstPropertyValue::from("free")),
                ]),
            ]))
            .await
            .expect("static foreach frames"),
            vec![
                ForEachParamFrame::Ast(vec![
                    (name("externalId"), AstPropertyValue::from("u-1")),
                    (name("tier"), AstPropertyValue::from("premium")),
                ]),
                ForEachParamFrame::Ast(vec![
                    (name("externalId"), AstPropertyValue::from("u-2")),
                    (name("tier"), AstPropertyValue::from("free")),
                ]),
            ]
        );
    }

    #[tokio::test]
    async fn query_array_parameters_expand_object_rows_to_query_frames() {
        assert_eq!(
            query_frames(QueryValue::Array(vec![
                QueryValue::Object(BTreeMap::from([
                    (
                        "externalId".to_string(),
                        QueryValue::String("u-1".to_string()),
                    ),
                    (
                        "tier".to_string(),
                        QueryValue::String("premium".to_string()),
                    ),
                ])),
                QueryValue::Object(BTreeMap::from([
                    (
                        "externalId".to_string(),
                        QueryValue::String("u-2".to_string()),
                    ),
                    ("tier".to_string(), QueryValue::String("free".to_string()),),
                ])),
            ]))
            .await
            .expect("query foreach frames"),
            vec![
                ForEachParamFrame::Query(vec![
                    (name("externalId"), QueryValue::String("u-1".to_string()),),
                    (name("tier"), QueryValue::String("premium".to_string()),),
                ]),
                ForEachParamFrame::Query(vec![
                    (name("externalId"), QueryValue::String("u-2".to_string()),),
                    (name("tier"), QueryValue::String("free".to_string())),
                ]),
            ]
        );
    }

    #[tokio::test]
    async fn foreach_param_frames_reject_non_object_contract_violations() {
        assert!(
            error_message(static_frames(AstPropertyValue::I64Array(vec![1, 2])).await)
                .contains("array of objects")
        );
        assert!(error_message(
            static_frames(AstPropertyValue::Array(vec![AstPropertyValue::String(
                "not-an-object".to_string()
            )]))
            .await
        )
        .contains("items to be objects"));
        assert!(error_message(
            static_frames(AstPropertyValue::Array(vec![AstPropertyValue::Object(
                BTreeMap::from([(String::new(), AstPropertyValue::I64(1))])
            )]))
            .await
        )
        .contains("field names must not be empty"));
        assert!(error_message(query_frames(QueryValue::I64(1)).await).contains("array of objects"));
        assert!(error_message(
            query_frames(QueryValue::Array(vec![QueryValue::String(
                "not-an-object".to_string()
            )]))
            .await
        )
        .contains("items to be objects"));
        assert!(error_message(
            query_frames(QueryValue::Array(vec![QueryValue::Object(BTreeMap::from(
                [(String::new(), QueryValue::I64(1))]
            ))]))
            .await
        )
        .contains("field names must not be empty"));
    }

    #[tokio::test]
    async fn foreach_param_frames_prefer_static_binding_over_dynamic_binding() {
        let db = test_support::open_db("foreach-static-over-query").await;
        let param = name("item");
        let ctx = ExecutionContext::new(
            &db,
            context::ParamBindings::default()
                .with_value(
                    param.clone(),
                    AstPropertyValue::array([AstPropertyValue::object([(
                        "id",
                        AstPropertyValue::from("static"),
                    )])]),
                )
                .with_query_value(
                    param.clone(),
                    QueryValue::Array(vec![QueryValue::Object(BTreeMap::from([(
                        "id".to_string(),
                        QueryValue::String("dynamic".to_string()),
                    )]))]),
                ),
        );

        assert_eq!(
            ctx.foreach_param_frames(&param).expect("static values win"),
            vec![ForEachParamFrame::Ast(vec![(
                name("id"),
                AstPropertyValue::from("static")
            )])]
        );
    }

    #[tokio::test]
    async fn foreach_param_frames_reject_missing_binding() {
        let db = test_support::open_db("foreach-missing-param").await;
        let param = name("missing");
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let err = ctx
            .foreach_param_frames(&param)
            .expect_err("missing foreach parameter should fail");

        assert!(err
            .to_string()
            .contains("foreach parameter `missing` is not bound"));
    }

    #[tokio::test]
    async fn empty_foreach_returns_empty_stream_without_executing_body() {
        let db = test_support::open_db("foreach-empty").await;
        let param = name("item");
        let mut ctx = ExecutionContext::new(
            &db,
            context::ParamBindings::default()
                .with_value(param.clone(), AstPropertyValue::Array(Vec::new())),
        );

        let result = ctx
            .execute_foreach(&param, &noop_subplan())
            .await
            .expect("empty foreach executes");

        assert_eq!(result, ExecutionValue::Stream(Vec::new()));
    }

    #[tokio::test]
    async fn foreach_restores_static_and_query_bindings_after_body_error() {
        let db = test_support::open_db("foreach-restore-after-error").await;
        let param = name("item");
        let missing = name("missing");
        let mut ctx = ExecutionContext::new(
            &db,
            context::ParamBindings::default()
                .with_value(
                    param.clone(),
                    AstPropertyValue::array([AstPropertyValue::object([(
                        "id",
                        AstPropertyValue::from("u-1"),
                    )])]),
                )
                .with_query_value(param.clone(), QueryValue::String("original".into())),
        );
        let body = test_support::subplan(
            vec![test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param: missing },
                    )),
                },
            )],
            1,
        );

        let err = ctx
            .execute_foreach(&param, &body)
            .await
            .expect_err("body failure should propagate");

        assert!(err.to_string().contains("is not bound"));
        assert_eq!(
            ctx.params.values.get(&param),
            Some(&AstPropertyValue::array([AstPropertyValue::object([(
                "id",
                AstPropertyValue::from("u-1")
            )])]))
        );
        assert_eq!(
            ctx.params.query_values.get(&param),
            Some(&QueryValue::String("original".to_string()))
        );
    }

    #[test]
    fn foreach_frame_apply_overrides_matching_static_and_query_fields() {
        let id = name("id");
        let mut params = context::ParamBindings::default()
            .with_value(id.clone(), AstPropertyValue::from("old-static"))
            .with_query_value(name("other"), QueryValue::I64(2));

        ForEachParamFrame::Query(vec![(
            id.clone(),
            QueryValue::String("new-query".to_string()),
        )])
        .apply_to(&mut params);

        assert!(!params.values.contains_key(&id));
        assert_eq!(
            params.query_values.get(&id),
            Some(&QueryValue::String("new-query".to_string()))
        );
    }
}
