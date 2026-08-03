use parking_lot::RwLock;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub(super) const GHOSTED: u8 = 1 << 0;
pub(super) const DELETED: u8 = 1 << 1;
pub(super) const VACUUMED: u8 = 1 << 2;

/// One immutable application-visible row version plus atomic lifecycle bits.
///
/// Readers hold an `Arc` to a complete version, so replacing or retiring a
/// version cannot invalidate an in-flight read. The short per-row lock keeps
/// the `Arc` and its lifecycle flags in one coherent publication; readers
/// never access mutable archived bytes.
struct PublishedVersion<Row> {
    row: Arc<Row>,
    flags: u8,
}

pub(super) struct PublishedRow<Row> {
    version: RwLock<PublishedVersion<Row>>,
}

impl<Row> PublishedRow<Row> {
    pub(super) fn new(row: Row, flags: u8) -> Self {
        Self {
            version: RwLock::new(PublishedVersion {
                row: Arc::new(row),
                flags,
            }),
        }
    }

    pub(super) fn replace(&self, row: Row, flags: u8) {
        *self.version.write() = PublishedVersion {
            row: Arc::new(row),
            flags,
        };
    }

    pub(super) fn load(&self) -> (Arc<Row>, u8) {
        let version = self.version.read();
        (version.row.clone(), version.flags)
    }

    pub(super) fn snapshot(&self) -> Arc<Row> {
        self.version.read().row.clone()
    }
}

impl<Row> Debug for PublishedRow<Row> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let version = self.version.read();
        f.debug_struct("PublishedRow")
            .field("flags", &version.flags)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    use super::PublishedRow;

    #[test]
    fn row_and_flags_are_loaded_from_one_version() {
        const ITERATIONS: usize = 100_000;

        let published = Arc::new(PublishedRow::new(0_u8, 0));
        let done = Arc::new(AtomicBool::new(false));
        let writer = {
            let published = published.clone();
            let done = done.clone();
            thread::spawn(move || {
                for value in 0..ITERATIONS {
                    let state = (value & 1) as u8;
                    published.replace(state, state);
                }
                done.store(true, Ordering::Release);
            })
        };

        while !done.load(Ordering::Acquire) {
            let (row, flags) = published.load();
            assert_eq!(*row, flags);
        }

        writer.join().unwrap();
    }
}
