use std::collections::VecDeque;

use crate::select::{Order, QueryParams};
use crate::WorkTableError;

#[derive(Clone)]
pub struct SelectQueryBuilder<Row, I, ColumnRange>
where
    I: Iterator<Item = Row>,
{
    pub params: QueryParams<ColumnRange>,
    pub iter: I,
}

impl<Row, I, ColumnRange> SelectQueryBuilder<Row, I, ColumnRange>
where
    I: Iterator<Item = Row>,
{
    pub fn new(iter: I) -> Self {
        Self {
            params: QueryParams {
                limit: None,
                offset: None,
                orders: VecDeque::new(),
                range: None,
            },
            iter,
        }
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.params.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.params.offset = Some(offset);
        self
    }

    pub fn order_by<S: Into<String>>(mut self, order: Order, column: S) -> Self {
        self.params.orders.push_back((order, column.into()));
        self
    }

    pub fn where_by<R>(mut self, range: R, column: impl Into<String>) -> Self
    where
        R: Into<ColumnRange>,
    {
        self.params.range = Some((range.into(), column.into()));
        self
    }
}

pub trait SelectQueryExecutor<Row, I, T>
where
    Self: Sized,
{
    fn execute(self) -> Result<Vec<Row>, WorkTableError>;
}
