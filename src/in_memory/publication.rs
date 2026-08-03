use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use parking_lot::RwLock;

pub(super) const GHOSTED: u8 = 1 << 0;
pub(super) const DELETED: u8 = 1 << 1;
pub(super) const VACUUMED: u8 = 1 << 2;

/// One immutable application-visible row version plus atomic lifecycle bits.
///
/// Readers hold an `Arc` to a complete version, so replacing or retiring a
/// version cannot invalidate an in-flight read. The short per-row lock only
/// protects the `Arc` pointer; readers never access mutable archived bytes.
pub(super) struct PublishedRow<Row> {
    row: RwLock<Arc<Row>>,
    flags: AtomicU8,
}

impl<Row> PublishedRow<Row> {
    pub(super) fn new(row: Row, flags: u8) -> Self {
        Self {
            row: RwLock::new(Arc::new(row)),
            flags: AtomicU8::new(flags),
        }
    }

    pub(super) fn replace(&self, row: Row, flags: u8) {
        *self.row.write() = Arc::new(row);
        self.flags.store(flags, Ordering::Release);
    }

    pub(super) fn load(&self) -> (Arc<Row>, u8) {
        let flags = self.flags.load(Ordering::Acquire);
        let row = self.row.read().clone();
        (row, flags)
    }

    pub(super) fn snapshot(&self) -> Arc<Row> {
        self.row.read().clone()
    }
}

impl<Row> Debug for PublishedRow<Row> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishedRow")
            .field("flags", &self.flags.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}
