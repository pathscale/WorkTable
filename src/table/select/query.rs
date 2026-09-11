use crate::WorkTableError;
use crate::runtime::{Profile, RuntimeUnpinned, TableRuntime};
use crate::select::{Order, QueryParams};
use alloc::vec::Vec;

use alloc::collections::VecDeque;

pub struct SelectQueryBuilder<Row, I, ColumnRange, RowFields>
where
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    pub params: QueryParams<ColumnRange, RowFields>,
    pub iter: I,
}

impl<Row, I, ColumnRange, RowFields> SelectQueryBuilder<Row, I, ColumnRange, RowFields>
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
                sorted_by: None,
                tuning: None,
                dispatch: None,
            },
            iter,
        }
    }

    pub fn new_sorted(iter: I, sorted_by: RowFields) -> Self {
        Self {
            params: QueryParams {
                limit: None,
                offset: None,
                order: VecDeque::new(),
                range: VecDeque::new(),
                sorted_by: Some(sorted_by),
                tuning: None,
                dispatch: None,
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

    pub fn order_on(mut self, column: RowFields, order: Order) -> Self {
        if !self.params.order.is_empty() {
            self.params.sorted_by = None;
        }
        self.params.order.push_back((order, column));
        self
    }

    pub fn range_on<R>(mut self, column: RowFields, range: R) -> Self
    where
        R: Into<ColumnRange>,
    {
        self.params.sorted_by = None;
        self.params.range.push_back((range.into(), column));
        self
    }

    /// Select the executor for an owned asynchronous query.
    ///
    /// Finish with `execute_async().await`. Calling synchronous `execute()`
    /// after this link returns `RuntimeRequiresAsync` instead of ignoring it.
    /// Borrowed iteration and `where_by` predicates run on the caller while
    /// constructing the future. Range filters, ordering, offset and limit run
    /// on the selected executor over owned rows. This materializes all input
    /// rows, so use synchronous execution for short or streaming selections.
    ///
    /// The profile backend, including its Nagoya flavor, must exactly match
    /// the generated row's `TableRuntime::Backend`. A mutation section profile
    /// applies to its own methods and does not pin unrelated select builders.
    /// Hosted paged tables implement these markers; Vec tables stay synchronous.
    pub fn runtime<P>(mut self, profile: P) -> Self
    where
        Row: TableRuntime + RuntimeUnpinned,
        P: Profile<Backend = <Row as TableRuntime>::Backend>,
        <P::Backend as crate::runtime::Runtime>::JoinHandle<()>: Unpin,
    {
        let _ = profile;
        self.params.tuning = Some(P::tuning());
        self.params.dispatch = Some(P::dispatcher());
        self
    }
}

pub trait SelectQueryExecutor<Row, I, ColumnRange, RowFields>
where
    Self: Sized,
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    fn execute(self) -> Result<Vec<Row>, WorkTableError>;
    fn where_by<F>(
        self,
        predicate: F,
    ) -> SelectQueryBuilder<Row, impl DoubleEndedIterator<Item = Row> + Sized, ColumnRange, RowFields>
    where
        F: FnMut(&Row) -> bool;
}

/// Owned asynchronous select execution. Borrowed iteration and predicates are
/// materialized by the caller; the owned filtering/sorting plan can be dispatched.
pub trait SelectQueryAsyncExecutor<Row> {
    fn execute_async(self) -> impl core::future::Future<Output = Result<Vec<Row>, WorkTableError>> + Send;
}
