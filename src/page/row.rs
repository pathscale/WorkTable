use std::fmt::Debug;
use std::sync::atomic::AtomicBool;

use rkyv::{Archive, Deserialize, Serialize};

/// Common trait for the `Row`s that can be stored on the [`Data`] page.
///
/// [`Data`]: crate::represent::page::DataPage
pub trait StorableRow {
    type WrappedRow: Archive + Debug;
}

pub trait RowWrapper<Inner> {
    fn get_inner(self) -> Inner;

    fn from_inner(inner: Inner) -> Self;
}

pub trait ArchivedRow {
    fn is_locked(&self) -> Option<u16>;
}

/// General `Row` wrapper that is used to append general data for every `Inner`
/// `Row`.
#[derive(Archive, Deserialize, Debug, Serialize)]
pub struct GeneralRow<Inner> {
    /// Indicator for deleted rows.
    pub deleted: AtomicBool,
    /// Inner generic `Row`.
    pub inner: Inner,
}

impl<Inner> RowWrapper<Inner> for GeneralRow<Inner> {
    fn get_inner(self) -> Inner {
        self.inner
    }

    /// Creates new [`GeneralRow`] from `Inner`.
    fn from_inner(inner: Inner) -> Self {
        Self {
            inner,
            deleted: AtomicBool::new(false),
        }
    }
}

impl<Inner> ArchivedRow for ArchivedGeneralRow<Inner>
where
    Inner: Archive,
{
    fn is_locked(&self) -> Option<u16> {
        None
    }
}