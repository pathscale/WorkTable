use std::collections::VecDeque;
use std::marker::PhantomData;

use crate::select::{Order, QueryParams, QueryParams2};
use crate::WorkTableError;

pub trait SelectQueryExecutor<'a, Row>
where
    Self: Sized,
{
    fn execute(&self, q: SelectQueryBuilder<'a, Row, Self>) -> Result<Vec<Row>, WorkTableError>;
}

pub struct SelectQueryBuilder<'a, Row, W> {
    table: &'a W,
    pub params: QueryParams,
    phantom_data: PhantomData<Row>,
}

impl<'a, Row, W> SelectQueryBuilder<'a, Row, W> {
    pub fn new(table: &'a W) -> Self {
        Self {
            table,
            params: QueryParams::default(),
            phantom_data: PhantomData,
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

    pub fn execute(self) -> Result<Vec<Row>, WorkTableError>
    where
        W: SelectQueryExecutor<'a, Row>,
    {
        self.table.execute(self)
    }
}

#[derive(Clone)]
pub struct SelectQueryBuilder2<Row, I, ColumnRange>
where
    I: Iterator<Item = Row>,
{
    pub params: QueryParams2<ColumnRange>,
    pub iter: I,
}

impl<Row, I, ColumnRange> SelectQueryBuilder2<Row, I, ColumnRange>
where
    I: Iterator<Item = Row>,
{
    pub fn new(iter: I) -> Self {
        Self {
            params: QueryParams2 {
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

pub trait SelectQueryExecutor2<Row, I, T>
where
    Self: Sized,
{
    fn execute2(self) -> Result<Vec<Row>, WorkTableError>;
}
