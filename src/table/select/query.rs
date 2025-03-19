use std::collections::VecDeque;

use crate::select::{Order, QueryParams};
use crate::WorkTableError;

pub struct SelectQueryBuilder<Row, I, ColumnRange>
where
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    pub params: QueryParams<ColumnRange>,
    pub iter: I,
}

impl<Row, I, ColumnRange> SelectQueryBuilder<Row, I, ColumnRange>
where
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    pub fn new(iter: I) -> Self {
        Self {
            params: QueryParams {
                limit: None,
                offset: None,
                order: VecDeque::new(),
                range: VecDeque::new(),
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

    pub fn order_on<O>(mut self, column: impl Into<&'static str>, order: O) -> Self
    where
        O: Into<Order>,
    {
        self.params
            .order
            .push_back((order.into(), column.into().to_string()));
        self
    }

    pub fn range_on<R>(mut self, column: impl Into<&'static str>, range: R) -> Self
    where
        R: Into<ColumnRange>,
    {
        self.params
            .range
            .push_back((range.into(), column.into().to_string()));
        self
    }
}

pub trait SelectQueryExecutor<Row, I, ColumnRange>
where
    Self: Sized,
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    fn execute(self) -> Result<Vec<Row>, WorkTableError>;
    fn where_by<F>(
        self,
        predicate: F,
    ) -> SelectQueryBuilder<Row, impl DoubleEndedIterator<Item = Row> + Sized, ColumnRange>
    where
        F: FnMut(&Row) -> bool;
}
