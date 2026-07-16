//! Mutable memo insertion contracts.

use super::super::expression::MemoExpression;
use super::super::identity::expression_digest;
use super::super::ids::{MemoExprId, MemoGroupId};
use super::super::index::MemoExprLocation;
use super::super::records::{InsertedMemoExpr, MemoError, MemoExpr, MemoGroup};
use super::Memo;

impl Memo {
    /// Insert a new group with one expression.
    pub fn insert_group(&mut self, expression: MemoExpression) -> Result<MemoGroupId, MemoError> {
        self.insert_group_with_expr_id(expression)
            .map(|inserted| inserted.group)
    }

    /// Insert a new group with one expression, returning both stable IDs.
    pub fn insert_group_with_expr_id(
        &mut self,
        expression: MemoExpression,
    ) -> Result<InsertedMemoExpr, MemoError> {
        let (group_id, expr_id) = self.take_group_and_expr_ids()?;
        debug_assert_eq!(
            group_id.get(),
            self.groups.len() + 1,
            "memo group IDs must remain dense and one-based"
        );
        debug_assert_eq!(
            expr_id.get(),
            self.indexes.expr_count() + 1,
            "memo expression IDs must remain dense and one-based"
        );
        let digest = expression_digest(&expression);
        let (expr, children) = expression.into_parts();
        let group_index = self.groups.len();
        self.groups.push(MemoGroup {
            id: group_id,
            digest,
            expressions: vec![MemoExpr {
                id: expr_id,
                group: group_id,
                digest,
                expr,
                children,
            }],
        });
        self.indexes
            .push_expr(expr_id, MemoExprLocation::new(group_index, 0));
        Ok(InsertedMemoExpr {
            group: group_id,
            expr: expr_id,
        })
    }

    /// Insert an equivalent expression into an existing group.
    pub fn insert_expr(
        &mut self,
        group: MemoGroupId,
        expression: MemoExpression,
    ) -> Result<InsertedMemoExpr, MemoError> {
        let target_index = self.group_index(group)?;
        let expr_id = self.take_expr_id()?;
        debug_assert_eq!(
            expr_id.get(),
            self.indexes.expr_count() + 1,
            "memo expression IDs must remain dense and one-based"
        );
        let digest = expression_digest(&expression);
        let (expr, children) = expression.into_parts();
        let target = &mut self.groups[target_index];
        let expr_index = target.expressions.len();
        target.expressions.push(MemoExpr {
            id: expr_id,
            group,
            digest,
            expr,
            children,
        });
        self.indexes
            .push_expr(expr_id, MemoExprLocation::new(target_index, expr_index));
        Ok(InsertedMemoExpr {
            group,
            expr: expr_id,
        })
    }

    /// True when the group already contains an equivalent expression.
    pub fn contains_expr(
        &self,
        group: MemoGroupId,
        expression: &MemoExpression,
    ) -> Result<bool, MemoError> {
        let digest = expression_digest(expression);
        let target = &self.groups[self.group_index(group)?];
        Ok(target.expressions.iter().any(|candidate| {
            candidate.digest == digest
                && &candidate.expr == expression.expr()
                && &candidate.children == expression.children()
        }))
    }

    fn take_group_and_expr_ids(&mut self) -> Result<(MemoGroupId, MemoExprId), MemoError> {
        let group_id = self.next_group_id.ok_or(MemoError::GroupIdSpaceExhausted)?;
        let expr_id = self.next_expr_id.ok_or(MemoError::ExprIdSpaceExhausted)?;
        self.next_group_id = group_id.next();
        self.next_expr_id = expr_id.next();
        Ok((group_id, expr_id))
    }

    fn take_expr_id(&mut self) -> Result<MemoExprId, MemoError> {
        let id = self.next_expr_id.ok_or(MemoError::ExprIdSpaceExhausted)?;
        self.next_expr_id = id.next();
        Ok(id)
    }
}
