//! Native access-stream wrapper append operations.

use helix_ast::expr::{Predicate, StreamBound};
use helix_ast::traversal;

use crate::planning::selected::native::stream;
use crate::planning::selected::native::{ordering, variables};
use crate::{context, error, ir};

/// Validated access-stream wrapper operation pending a source-rooted input.
pub(super) enum NativeAccessStreamOp<'a> {
    Filter(Predicate),
    Distinct,
    Limit(&'a StreamBound),
    Skip(&'a StreamBound),
    Range {
        start: &'a StreamBound,
        end: &'a StreamBound,
    },
    OrderBy {
        property: &'a str,
        order: traversal::Order,
    },
    OrderByMultiple(&'a [(String, traversal::Order)]),
    Within(&'a str),
    Without(&'a str),
    Select(&'a str),
    Bind(&'a str),
    Inject(&'a str),
    As(&'a str),
    Store(&'a str),
}

impl<'a> NativeAccessStreamOp<'a> {
    pub(super) fn append_to(
        self,
        ctx: &context::PlannerContext,
        stream: stream::NativeAccessStream,
    ) -> Result<stream::NativeAccessStream, error::PlannerError> {
        match self {
            Self::Filter(predicate) => stream.filter(ctx, &predicate),
            Self::Distinct => Ok(stream.distinct()),
            Self::Limit(count) => stream.limit(count),
            Self::Skip(count) => stream.skip(count),
            Self::Range { start, end } => stream.range(start, end),
            Self::OrderBy { property, order } => ordering::order_key(property, order)
                .map(ir::OrderKeys::from)
                .map(|keys| stream.order(keys)),
            Self::OrderByMultiple(orderings) => {
                ordering::order_keys(orderings).map(|keys| stream.order(keys))
            }
            Self::Within(variable) => variables::within(variable).map(|op| stream.variable(op)),
            Self::Without(variable) => variables::without(variable).map(|op| stream.variable(op)),
            Self::Select(name) => variables::select(name).map(|op| stream.variable(op)),
            Self::Bind(name) => variables::bind(name).map(|op| stream.variable(op)),
            Self::Inject(variable) => variables::inject(variable).map(|op| stream.variable(op)),
            Self::As(name) => variables::as_write(name).map(|op| stream.variable_write(op)),
            Self::Store(name) => variables::store(name).map(|op| stream.variable_write(op)),
        }
    }
}
