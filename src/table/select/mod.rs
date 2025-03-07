mod query;
use std::collections::VecDeque;

pub use query::{SelectQueryBuilder, SelectQueryExecutor};

#[derive(Debug, Clone, Copy)]
pub enum Order {
    Asc,
    Desc,
}

type Column = String;

#[derive(Debug, Default, Clone)]
pub struct QueryParams<ColumnRange> {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub orders: VecDeque<(Order, Column)>,
    pub range: Option<(ColumnRange, Column)>,
}
