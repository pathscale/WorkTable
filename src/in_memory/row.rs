use std::fmt::Debug;

use rkyv::Archive;

/// Common trait for the `Row`s that can be stored on the [`Data`] page.
///
/// [`Data`]: crate::in_memory::data::Data
pub trait StorableRow {
    type WrappedRow: Archive + Debug;
}

pub trait RowWrapper<Inner> {
    fn get_inner(self) -> Inner;
    fn is_ghosted(&self) -> bool;
    fn is_vacuumed(&self) -> bool;
    fn is_deleted(&self) -> bool;
    fn from_inner(inner: Inner) -> Self;
}

pub trait ArchivedRowWrapper {
    fn unghost(&mut self);
    fn set_in_vacuum_process(&mut self);
    fn delete(&mut self);
    fn is_deleted(&self) -> bool;
}

pub trait Query<Row> {
    fn merge(self, row: Row) -> Row;
}
