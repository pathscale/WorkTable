use alloc::collections::VecDeque;

use crate::runtime::Tuning;

mod query;

pub use query::{SelectQueryAsyncExecutor, SelectQueryBuilder, SelectQueryExecutor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Default, Clone)]
pub struct QueryParams<ColumnRange, RowFields> {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order: VecDeque<(Order, RowFields)>,
    pub range: VecDeque<(ColumnRange, RowFields)>,
    pub sorted_by: Option<RowFields>,
    /// The pool settings the profile named at the call site asks for, `None`
    /// when no `.runtime()` was written. Carried here rather than acted on,
    /// because `execute` is generated and this is what it reads.
    pub tuning: Option<Tuning>,
    /// Submission chosen by the explicit runtime callsite.
    pub dispatch: Option<crate::runtime::Dispatch>,
}
