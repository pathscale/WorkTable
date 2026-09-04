use std::fmt::Debug;
use std::sync::atomic::AtomicU8;

use rkyv::rancor::Fallible;
use rkyv::{Archive, Deserialize, Place, Serialize};

/// Runtime synchronization state embedded as the first byte of every archived
/// cell wrapper.
///
/// The source value is zero-sized; its archived representation is one byte.
/// Deserialization deliberately ignores that byte because active readers
/// modify it atomically. It is synchronization state, never row data.
#[derive(Clone, Copy, Debug, Default)]
pub struct CellState;

impl Archive for CellState {
    type Archived = u8;
    type Resolver = ();

    fn resolve(&self, _: Self::Resolver, out: Place<Self::Archived>) {
        out.write(0);
    }
}

impl<S: Fallible + ?Sized> Serialize<S> for CellState {
    fn serialize(&self, _: &mut S) -> Result<Self::Resolver, S::Error> {
        Ok(())
    }
}

impl<D: Fallible + ?Sized> Deserialize<CellState, D> for u8 {
    fn deserialize(&self, _: &mut D) -> Result<CellState, D::Error> {
        Ok(CellState)
    }
}

pub trait PublicationSafe: Send + Sync + 'static {}

impl<T: Send + Sync + 'static> PublicationSafe for T {}

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
    /// Returns the atomic synchronization byte for this archived cell without
    /// first creating a reference to the rest of the row. Implementations must
    /// place `cell_state` in a stable position in a `repr(C)` archived wrapper.
    ///
    /// # Safety
    ///
    /// `this` must point to a valid archived wrapper in writable page memory.
    unsafe fn cell_state_ptr(this: *mut Self) -> *mut AtomicU8;
    fn unghost(&mut self);
    fn set_in_vacuum_process(&mut self);
    fn delete(&mut self);
    fn is_deleted(&self) -> bool;
}

pub trait Query<Row> {
    fn merge(self, row: Row) -> Row;
}
