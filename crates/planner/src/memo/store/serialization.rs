//! Serialized memo-record validation.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer};

use super::super::ids::{MemoExprId, MemoGroupId};
use super::super::index::{MemoExprLocation, MemoIndexes};
use super::super::records::MemoGroup;
use super::Memo;

impl Memo {
    fn from_groups(groups: Vec<MemoGroup>) -> Result<Self, MemoRecordError> {
        let expr_count = groups
            .iter()
            .map(|group| group.expressions.len())
            .sum::<usize>();
        let mut expr_locations = vec![None; expr_count];
        let mut indexes = MemoIndexes::new(expr_count);

        for (index, group) in groups.iter().enumerate() {
            let expected_group = index + 1;
            if group.id.get() != expected_group {
                return Err(MemoRecordError::NonSequentialGroup {
                    expected: expected_group,
                    actual: group.id.get(),
                });
            }
            if group.expressions.is_empty() {
                return Err(MemoRecordError::EmptyGroup { group: group.id });
            }
            if group.digest != group.expressions[0].digest {
                return Err(MemoRecordError::GroupDigestMismatch { group: group.id });
            }

            for (expr_index, expr) in group.expressions.iter().enumerate() {
                if expr.group != group.id {
                    return Err(MemoRecordError::ExpressionGroupMismatch {
                        group: group.id,
                        expr_group: expr.group,
                        expr: expr.id,
                    });
                }
                let id_index =
                    expr.id
                        .get()
                        .checked_sub(1)
                        .ok_or(MemoRecordError::NonSequentialExpr {
                            expected: 1,
                            actual: expr.id.get(),
                        })?;
                let Some(slot) = expr_locations.get_mut(id_index) else {
                    return Err(MemoRecordError::NonSequentialExpr {
                        expected: expr_count,
                        actual: expr.id.get(),
                    });
                };
                if slot.is_some() {
                    return Err(MemoRecordError::DuplicateExpr { expr: expr.id });
                }
                *slot = Some(MemoExprLocation::new(index, expr_index));
            }
        }

        for (index, location) in expr_locations.into_iter().enumerate() {
            let location = location.ok_or(MemoRecordError::NonSequentialExpr {
                expected: index + 1,
                actual: index + 2,
            })?;
            let expr = index.checked_add(1).and_then(MemoExprId::new).ok_or(
                MemoRecordError::NonSequentialExpr {
                    expected: index + 1,
                    actual: index + 1,
                },
            )?;
            indexes.push_expr(expr, location);
        }

        Ok(Self {
            next_group_id: next_group_id_after(groups.len()),
            next_expr_id: next_expr_id_after(expr_count),
            groups,
            indexes,
        })
    }
}

fn next_group_id_after(max: usize) -> Option<MemoGroupId> {
    max.checked_add(1).and_then(MemoGroupId::new)
}

fn next_expr_id_after(max: usize) -> Option<MemoExprId> {
    max.checked_add(1).and_then(MemoExprId::new)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoRecordError {
    NonSequentialGroup {
        expected: usize,
        actual: usize,
    },
    NonSequentialExpr {
        expected: usize,
        actual: usize,
    },
    EmptyGroup {
        group: MemoGroupId,
    },
    GroupDigestMismatch {
        group: MemoGroupId,
    },
    ExpressionGroupMismatch {
        group: MemoGroupId,
        expr_group: MemoGroupId,
        expr: MemoExprId,
    },
    DuplicateExpr {
        expr: MemoExprId,
    },
}

impl std::fmt::Display for MemoRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonSequentialGroup { expected, actual } => write!(
                f,
                "memo group IDs must be sequential: expected {expected}, got {actual}"
            ),
            Self::NonSequentialExpr { expected, actual } => write!(
                f,
                "memo expression IDs must be dense: expected {expected}, got {actual}"
            ),
            Self::EmptyGroup { group } => {
                write!(f, "memo group {} has no expressions", group.get())
            }
            Self::GroupDigestMismatch { group } => write!(
                f,
                "memo group {} digest does not match its first expression",
                group.get()
            ),
            Self::ExpressionGroupMismatch {
                group,
                expr_group,
                expr,
            } => write!(
                f,
                "memo expression {} belongs to group {}, not containing group {}",
                expr.get(),
                expr_group.get(),
                group.get()
            ),
            Self::DuplicateExpr { expr } => write!(f, "duplicate memo expression {}", expr.get()),
        }
    }
}

impl<'de> Deserialize<'de> for Memo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawMemo {
            groups: Vec<MemoGroup>,
        }

        let raw = RawMemo::deserialize(deserializer)?;
        Self::from_groups(raw.groups).map_err(D::Error::custom)
    }
}
