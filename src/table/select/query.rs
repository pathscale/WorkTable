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

    /// Run this query on the named runtime profile.
    ///
    /// # Why only here
    ///
    /// This method is on the **builder-returning** selects, `select_all` and
    /// `select_by_pk_range`, and deliberately not on `select(pk)`, which hands
    /// back a row rather than a builder. Moving a point read onto another
    /// worker costs more than the read: a spawn measures 21 ns and the wake
    /// that follows it about 2,250 ns at the median, against roughly 400 ns for
    /// the read itself. `.runtime()` is for work already measured in
    /// microseconds, where a few thousand nanoseconds of hop can be repaid.
    ///
    /// # One argument, always
    ///
    /// Exactly one profile, no worker count, no durations. Every distinct
    /// parameterisation is a distinct thread pool, so free-form numbers here
    /// would mean an unbounded pool set that nobody reading the call site can
    /// see; with names only, every pool the process will ever create can be
    /// enumerated by reading one `runtimes!` block. If parameters are wanted
    /// later they arrive either as fields on the profile or as a further
    /// builder link, `.runtime(wide).workers(12)`, never as a second argument:
    /// an arity change breaks every existing call.
    ///
    /// # The two ways this fails to compile
    ///
    /// Naming a profile whose backend is not the table's is an error that can
    /// never be waived, because the table's `runtime:` picked the sync types
    /// underneath it. The bound is written as an equality so the message names
    /// both backends.
    ///
    /// Calling this when a section annotation already pinned a runtime is also
    /// an error, on purpose rather than a silent override, so that there is one
    /// answer to "which runtime does this query use" and it is visible where
    /// you are reading. See [`RuntimeUnpinned`] for why that is a bound and not
    /// a missing method, and for why the impl that satisfies it is emitted per
    /// table rather than blanket.
    pub fn runtime<P>(mut self, profile: P) -> Self
    where
        Row: TableRuntime + RuntimeUnpinned,
        P: Profile<Backend = <Row as TableRuntime>::Backend>,
    {
        let _ = profile;
        self.params.tuning = Some(P::tuning());
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
