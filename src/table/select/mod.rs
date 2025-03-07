mod query;
mod result;

use std::collections::VecDeque;

pub use query::{
    SelectQueryBuilder, SelectQueryBuilder2, SelectQueryExecutor, SelectQueryExecutor2,
};
pub use result::{SelectResult, SelectResultExecutor};

#[derive(Debug, Clone, Copy)]
pub enum Order {
    Asc,
    Desc,
}

type Column = String;

#[derive(Debug, Default, Clone)]
pub struct QueryParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub orders: VecDeque<(Order, String)>,
}

#[derive(Debug, Default, Clone)]
pub struct QueryParams2<ColumnRange> {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub orders: VecDeque<(Order, Column)>,
    pub range: Option<(ColumnRange, Column)>,
}
