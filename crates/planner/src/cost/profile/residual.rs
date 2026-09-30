//! Residual evaluation mirrors the interpreter's per-filter value resolver.

use std::collections::BTreeSet;

use helix_ast::expr::{Expr, Predicate};

use super::{CostVector, EstimatedRows, StorageCostProfile};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum GraphRead {
    CurrentProperties,
    EdgeEndpoints,
    FromProperties,
    ToProperties,
}

#[derive(Default)]
struct Evaluation {
    atoms: u64,
    reads: BTreeSet<GraphRead>,
}

impl Evaluation {
    fn property(&mut self, property: &str) {
        match property {
            "$id" | "$score" | "$distance" => {}
            "$from" | "$to" | "$from.$id" | "$to.$id" => {
                self.reads.insert(GraphRead::EdgeEndpoints);
            }
            property if property.starts_with("$from.") => {
                self.reads.insert(GraphRead::EdgeEndpoints);
                self.reads.insert(GraphRead::FromProperties);
            }
            property if property.starts_with("$to.") => {
                self.reads.insert(GraphRead::EdgeEndpoints);
                self.reads.insert(GraphRead::ToProperties);
            }
            _ => {
                self.reads.insert(GraphRead::CurrentProperties);
            }
        }
    }

    fn expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Property(property) => self.property(property),
            Expr::Add { left, right }
            | Expr::Sub { left, right }
            | Expr::Mul { left, right }
            | Expr::Div { left, right }
            | Expr::Mod { left, right } => {
                self.expr(left);
                self.expr(right);
            }
            Expr::Neg { expr } => self.expr(expr),
            Expr::Case {
                when_then,
                else_expr,
            } => {
                for branch in when_then {
                    self.predicate(&branch.when);
                    self.expr(&branch.then);
                }
                else_expr.iter().for_each(|expr| self.expr(expr));
            }
            Expr::Id | Expr::Timestamp | Expr::DateTimeNow | Expr::Constant(_) | Expr::Param(_) => {
            }
        }
    }

    fn predicate(&mut self, predicate: &Predicate) {
        match predicate {
            Predicate::And { predicates } | Predicate::Or { predicates } => {
                for predicate in predicates {
                    self.predicate(predicate);
                }
                return;
            }
            Predicate::Not { predicate } => {
                self.predicate(predicate);
                return;
            }
            Predicate::Eq { left, right }
            | Predicate::Neq { left, right }
            | Predicate::Gt { left, right }
            | Predicate::Gte { left, right }
            | Predicate::Lt { left, right }
            | Predicate::Lte { left, right }
            | Predicate::Compare { left, right, .. }
            | Predicate::StartsWith {
                value: left,
                prefix: right,
            }
            | Predicate::EndsWith {
                value: left,
                suffix: right,
            }
            | Predicate::Contains {
                value: left,
                substring: right,
            }
            | Predicate::IsIn {
                value: left,
                values: right,
            } => {
                self.expr(left);
                self.expr(right);
            }
            Predicate::Between { value, min, max } => {
                self.expr(value);
                self.expr(min);
                self.expr(max);
            }
            Predicate::HasKey { property }
            | Predicate::IsNull { property }
            | Predicate::IsNotNull { property } => self.property(property),
        }
        self.atoms = self.atoms.saturating_add(1);
    }
}

impl StorageCostProfile {
    /// Cost graph fetching and CPU work for one residual filter.
    ///
    /// The interpreter caches a property blob within one predicate evaluation,
    /// not across filters or access stages. Charge each required blob once per
    /// candidate and each predicate leaf once. Without conditional selectivity
    /// statistics, do not assume short-circuiting or reduce the output estimate.
    /// Lookup, row construction, and projection are charged separately.
    ///
    /// ```
    /// use helix_ast::expr::Predicate;
    /// use helix_planner::cost::{EstimatedRows, StorageCostProfile};
    /// let predicate = Predicate::and(vec![Predicate::eq("a", 1), Predicate::eq("b", 2)]);
    /// let cost = StorageCostProfile::default().residual_filter(&predicate, EstimatedRows::rows(10));
    /// assert_eq!(cost.authoritative_graph_reads, 10);
    /// assert_eq!(cost.cpu_units, 30); // 10 graph reads + 20 comparisons
    /// assert_eq!(cost.latency.as_micros(), 120);
    /// ```
    pub fn residual_filter(&self, predicate: &Predicate, rows: EstimatedRows) -> CostVector {
        let mut evaluation = Evaluation::default();
        evaluation.predicate(predicate);
        self.authoritative_verification(EstimatedRows::rows(
            rows.as_rows().saturating_mul(evaluation.reads.len() as u64),
        ))
        .serial(self.predicate_eval(EstimatedRows::rows(
            rows.as_rows().saturating_mul(evaluation.atoms),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn residual_cost_counts_boolean_leaves_and_shares_property_fetches() {
        let predicates = vec![
            Predicate::Eq {
                left: Expr::prop("a"),
                right: Expr::val(1),
            },
            Predicate::Neq {
                left: Expr::prop("a"),
                right: Expr::val(1),
            },
            Predicate::Gt {
                left: Expr::prop("a"),
                right: Expr::val(1),
            },
            Predicate::Gte {
                left: Expr::prop("a"),
                right: Expr::val(1),
            },
            Predicate::Lt {
                left: Expr::prop("a"),
                right: Expr::val(1),
            },
            Predicate::Lte {
                left: Expr::prop("a"),
                right: Expr::val(1),
            },
            Predicate::Compare {
                left: Expr::prop("a"),
                op: helix_ast::expr::CompareOp::Eq,
                right: Expr::val(1),
            },
            Predicate::StartsWith {
                value: Expr::prop("a"),
                prefix: Expr::val("x"),
            },
            Predicate::EndsWith {
                value: Expr::prop("a"),
                suffix: Expr::val("x"),
            },
            Predicate::Contains {
                value: Expr::prop("a"),
                substring: Expr::val("x"),
            },
            Predicate::IsIn {
                value: Expr::prop("a"),
                values: Expr::param("values"),
            },
            Predicate::Between {
                value: Expr::prop("a"),
                min: Expr::val(0),
                max: Expr::val(9),
            },
            Predicate::HasKey {
                property: "a".into(),
            },
            Predicate::IsNull {
                property: "b".into(),
            },
            Predicate::IsNotNull {
                property: "c".into(),
            },
        ];
        let atoms = predicates.len() as u64;
        let predicate = Predicate::and(vec![
            Predicate::or(predicates),
            Predicate::Not {
                predicate: Box::new(Predicate::eq("d", 1)),
            },
        ]);
        let profile = StorageCostProfile::default();
        let cost = profile.residual_filter(&predicate, EstimatedRows::rows(3));
        assert_eq!(cost.authoritative_graph_reads, 3);
        assert_eq!(cost.cpu_units, 3 + 3 * (atoms + 1));
        assert_eq!(
            profile.residual_filter(&predicate, EstimatedRows::ZERO),
            CostVector::ZERO
        );
        assert_eq!(
            profile
                .residual_filter(&predicate, EstimatedRows::rows(u64::MAX))
                .latency
                .as_micros(),
            u64::MAX
        );
    }

    #[test]
    fn residual_cost_distinguishes_virtual_properties_and_endpoint_blobs() {
        let profile = StorageCostProfile::default();
        let virtuals = Predicate::and(
            ["$id", "$score", "$distance"]
                .into_iter()
                .map(|name| Predicate::eq(name, 1))
                .collect(),
        );
        assert_eq!(
            profile
                .residual_filter(&virtuals, EstimatedRows::rows(2))
                .authoritative_graph_reads,
            0
        );
        let endpoints = Predicate::and(
            [
                "$from",
                "$to",
                "$from.$id",
                "$to.$id",
                "$from.name",
                "$from.age",
                "$to.name",
                "weight",
            ]
            .into_iter()
            .map(|name| Predicate::eq(name, 1))
            .collect(),
        );
        let cost = profile.residual_filter(&endpoints, EstimatedRows::rows(2));
        assert_eq!(cost.authoritative_graph_reads, 8); // endpoint record and three property blobs per row
        assert_eq!(cost.cpu_units, 24);
    }

    #[test]
    fn residual_cost_walks_computed_and_conditional_expressions() {
        let computed = Expr::prop("a")
            .add_expr(Expr::prop("b"))
            .sub_expr(Expr::val(2))
            .mul_expr(Expr::val(3))
            .div_expr(Expr::val(4))
            .modulo(Expr::val(5))
            .neg_expr();
        let value = Expr::case(
            vec![(Predicate::eq("a", 1), computed)],
            Some(Expr::prop("b")),
        );
        let other = Expr::case(vec![(Predicate::eq("c", 2), Expr::Id)], None);
        let predicate = Predicate::and(vec![
            Predicate::Eq {
                left: value,
                right: other,
            },
            Predicate::Eq {
                left: Expr::Timestamp,
                right: Expr::DateTimeNow,
            },
        ]);
        let cost =
            StorageCostProfile::default().residual_filter(&predicate, EstimatedRows::rows(1));
        assert_eq!(cost.authoritative_graph_reads, 1);
        assert_eq!(cost.cpu_units, 5); // four predicate leaves plus one graph fetch
    }
}
