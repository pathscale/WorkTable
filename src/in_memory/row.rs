use core::fmt::Debug;
use rkyv::Archive;

pub trait PublicationSafe: Send + Sync + 'static {}

impl<T: Send + Sync + 'static> PublicationSafe for T {}

/// A row whose archived form holds no relative pointers.
///
/// Every archived field is a fixed-size scalar sitting inline in the cell, so
/// reading one while a writer mutates it can tear a value but cannot produce a
/// pointer that refers anywhere else. That is what makes the zero-copy
/// [`select_with`] path sound: the closure may observe a torn number, and the
/// seqlock retry discards whatever it computed from one.
///
/// A row with a `String` or `Vec` column has archived relative pointers, and a
/// torn pointer dereferenced inside the closure is undefined behaviour rather
/// than a wrong number. Those rows deliberately do not implement this trait, so
/// they fail to compile against the zero-copy API instead of silently copying,
/// and must use the owned `select`.
///
/// # Safety
///
/// Implementors must contain no archived relative pointers. The `worktable!`
/// macro implements this only when every column is a known scalar shape; it is
/// never implemented for an opaque user type, because the macro cannot inspect
/// that type's `Archive::Archived` layout.
///
/// [`select_with`]: crate::WorkTable::select_with
pub unsafe trait InlineArchived {}

/// Common trait for the `Row`s that can be stored on the [`Data`] page.
///
/// [`Data`]: crate::in_memory::data::Data
pub trait StorableRow: PublicationSafe {
    type WrappedRow: Archive<Archived: ArchivedRowWrapper> + Debug;
}

pub trait RowWrapper<Inner> {
    fn get_inner(self) -> Inner;
    fn is_ghosted(&self) -> bool;
    fn is_vacuumed(&self) -> bool;
    fn is_deleted(&self) -> bool;
    fn from_inner(inner: Inner) -> Self;
}

pub trait ArchivedRowWrapper {
    type Inner: ?Sized;

    fn inner(&self) -> &Self::Inner;
    fn is_ghosted(&self) -> bool;
    fn unghost(&mut self);
    fn set_in_vacuum_process(&mut self);
    fn delete(&mut self);
    fn is_deleted(&self) -> bool;
}

pub trait Query<Row> {
    fn merge(self, row: Row) -> Row;
}
