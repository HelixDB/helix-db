//! Canonical non-empty stream-pipeline operator storage.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use super::validation::validate_stream_pipeline_ops;
use super::StreamPipelineOp;
use crate::ir;

/// Validated, canonical, non-empty stream-pipeline operators.
///
/// Contiguous filter runs are stored as one flat conjunction. Every other
/// operator terminates the current run, so canonicalization cannot move a
/// predicate across a semantic boundary.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub(in crate::logical) struct CanonicalStreamPipelineOps(ir::AtLeast<StreamPipelineOp, 1>);

impl CanonicalStreamPipelineOps {
    pub(in crate::logical) fn new(ops: ir::AtLeast<StreamPipelineOp, 1>) -> Option<Self> {
        let ops = canonicalize_stream_pipeline_ops(ops.into_iter().collect());
        validate_stream_pipeline_ops(&ops)?;
        Some(Self(ir::AtLeast::try_from_vec(ops).expect(
            "canonicalizing non-empty pipeline operators preserves cardinality",
        )))
    }

    pub(in crate::logical) fn as_slice(&self) -> &[StreamPipelineOp] {
        self.0.as_ref()
    }

    pub(in crate::logical) const fn as_at_least(&self) -> &ir::AtLeast<StreamPipelineOp, 1> {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CanonicalStreamPipelineOps {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ops = ir::AtLeast::<StreamPipelineOp, 1>::deserialize(deserializer)?;
        Self::new(ops).ok_or_else(|| D::Error::custom("non-canonical stream pipeline operators"))
    }
}

pub(crate) fn canonicalize_stream_pipeline_ops(
    ops: Vec<StreamPipelineOp>,
) -> Vec<StreamPipelineOp> {
    let mut canonical = Vec::with_capacity(ops.len());
    let mut filters = Vec::new();

    for op in ops {
        match op {
            StreamPipelineOp::Filter { predicate } => filters.push(predicate),
            op => {
                flush_filters(&mut canonical, &mut filters);
                canonical.push(op);
            }
        }
    }
    flush_filters(&mut canonical, &mut filters);
    canonical
}

fn flush_filters(ops: &mut Vec<StreamPipelineOp>, filters: &mut Vec<ir::PredicatePlan>) {
    match filters.as_slice() {
        [] => {}
        [predicate]
            if !matches!(
                predicate.resolved(),
                ir::native::Expression::Function(ir::native::Function::All, _)
            ) =>
        {
            ops.push(StreamPipelineOp::Filter {
                predicate: predicate.clone(),
            })
        }
        [first, rest @ ..] => {
            ops.push(StreamPipelineOp::Filter {
                predicate: ir::PredicatePlan::flattened_conjunction(first, rest),
            });
        }
    }
    filters.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::expr::Predicate;

    #[test]
    fn canonicalization_preserves_empty_and_single_filter_inputs() {
        assert!(canonicalize_stream_pipeline_ops(Vec::new()).is_empty());

        let predicate = ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap();
        let canonical = canonicalize_stream_pipeline_ops(vec![StreamPipelineOp::Filter {
            predicate: predicate.clone(),
        }]);
        assert_eq!(canonical, vec![StreamPipelineOp::Filter { predicate }]);
    }

    #[test]
    fn canonicalization_flattens_a_nested_single_filter() {
        let canonical = canonicalize_stream_pipeline_ops(vec![StreamPipelineOp::Filter {
            predicate: ir::PredicatePlan::new(Predicate::and(vec![
                Predicate::eq("$label", "Organization"),
                Predicate::and(vec![
                    Predicate::eq_param("organization_id", "organization_id"),
                    Predicate::eq("organization_type", "company"),
                ]),
            ]))
            .unwrap(),
        }]);

        assert!(matches!(
            canonical.as_slice(),
            [StreamPipelineOp::Filter { predicate }]
                if predicate.as_ref() == &Predicate::and(vec![
                    Predicate::eq("$label", "Organization"),
                    Predicate::eq_param("organization_id", "organization_id"),
                    Predicate::eq("organization_type", "company"),
                ])
        ));
    }

    #[test]
    fn resolved_filter_runs_preserve_order_and_semantic_boundaries() {
        use crate::logical::variables::StreamVariableWriteOp;
        let first =
            ir::PredicatePlan::new(Predicate::and(vec![Predicate::eq("first", 1)])).unwrap();
        let second = ir::PredicatePlan::new(Predicate::eq("second", 2)).unwrap();
        let expected = ir::PredicatePlan::new(Predicate::and(vec![
            Predicate::eq("first", 1),
            Predicate::eq("second", 2),
        ]))
        .unwrap();
        for boundary in [
            StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            },
            StreamPipelineOp::Distinct,
            StreamPipelineOp::VariableWrite {
                op: StreamVariableWriteOp::Store(ir::NonEmptyString::new("saved").unwrap()),
            },
        ] {
            let actual = canonicalize_stream_pipeline_ops(vec![
                StreamPipelineOp::Filter {
                    predicate: first.clone(),
                },
                StreamPipelineOp::Filter {
                    predicate: second.clone(),
                },
                boundary.clone(),
                StreamPipelineOp::Filter {
                    predicate: second.clone(),
                },
            ]);
            assert_eq!(
                actual,
                vec![
                    StreamPipelineOp::Filter {
                        predicate: expected.clone()
                    },
                    boundary,
                    StreamPipelineOp::Filter {
                        predicate: second.clone()
                    }
                ]
            );
            let StreamPipelineOp::Filter { predicate: last } = &actual[2] else {
                panic!("last filter")
            };
            assert!(
                std::ptr::eq(last.program().expression(), second.program().expression()),
                "a lone filter retains its shared program"
            );
        }
    }
}
