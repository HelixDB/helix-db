//! Variable stream operation contracts.

use std::collections::BTreeSet;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn variable(
        &mut self,
        input: ExecutionValue,
        op: &exec::ExecVariableOp,
    ) -> Result<ExecutionValue> {
        match op {
            exec::ExecVariableOp::SourceInject { variable } => {
                self.variable_value(variable).cloned()
            }
            exec::ExecVariableOp::Stream(op) => match op {
                ir::StreamVariableOp::As(name) | ir::StreamVariableOp::Store(name) => {
                    self.variables.insert(name.clone(), input.clone());
                    Ok(input)
                }
                ir::StreamVariableOp::Select(name) => self.variable_value(name).cloned(),
                ir::StreamVariableOp::Bind(name) => Ok(ExecutionValue::Stream(bind_rows(
                    self.stream_rows(input, "bind")?,
                    name,
                ))),
                ir::StreamVariableOp::Inject(name) => {
                    let mut rows = self.stream_rows(input, "inject")?;
                    let injected =
                        self.stream_rows(self.variable_value(name)?.clone(), "inject")?;
                    rows.extend(injected);
                    Ok(ExecutionValue::Stream(rows))
                }
                ir::StreamVariableOp::Within(name) => {
                    let allowed = self.element_set(self.variable_value(name)?)?;
                    Ok(ExecutionValue::Stream(filter_within_rows(
                        self.stream_rows(input, "within")?,
                        &allowed,
                    )))
                }
                ir::StreamVariableOp::Without(name) => {
                    let rejected = self.element_set(self.variable_value(name)?)?;
                    Ok(ExecutionValue::Stream(filter_without_rows(
                        self.stream_rows(input, "without")?,
                        &rejected,
                    )))
                }
            },
        }
    }
}

pub(in crate::execution::interpreter::stream) fn bind_rows(
    rows: Vec<ExecutionRow>,
    name: &ir::NonEmptyString,
) -> Vec<ExecutionRow> {
    rows.into_iter()
        .map(|mut row| {
            if let Some(current) = row.current.clone() {
                row.bindings.insert(name.clone(), current);
                if row.virtual_properties.is_empty() {
                    row.binding_virtual_properties.remove(name);
                } else {
                    row.binding_virtual_properties
                        .insert(name.clone(), row.virtual_properties.clone());
                }
            }
            row
        })
        .collect()
}

pub(in crate::execution::interpreter::stream) fn filter_within_rows(
    rows: Vec<ExecutionRow>,
    allowed: &BTreeSet<ElementRef>,
) -> Vec<ExecutionRow> {
    rows.into_iter()
        .filter(|row| {
            row.current
                .as_ref()
                .is_some_and(|element| allowed.contains(element))
        })
        .collect()
}

pub(in crate::execution::interpreter::stream) fn filter_without_rows(
    rows: Vec<ExecutionRow>,
    rejected: &BTreeSet<ElementRef>,
) -> Vec<ExecutionRow> {
    rows.into_iter()
        .filter(|row| {
            row.current
                .as_ref()
                .is_none_or(|element| !rejected.contains(element))
        })
        .collect()
}
