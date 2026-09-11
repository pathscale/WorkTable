//! Proof that `worktable!` emits nothing a `no_std` consumer cannot resolve.
//!
//! **The crate under test cannot check this itself.** `worktable` builds with
//! `--no-default-features` whether or not the macro is sound, because the
//! expansion only happens where the macro is invoked. So the verifier has to be
//! a separate crate that invokes it, which is what this is.
//!
//! Three names have gone through here: `ArtPersistenceKey`, `WorkTableVacuum`
//! and `EmptyDataVacuum`. All three are std-only for real reasons, so the fix
//! was to stop emitting them rather than to export them, and the mechanism is
//! `worktable::__wt_if_std!`.
#![no_std]

extern crate alloc;
#[cfg(test)]
extern crate std;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: NoStdTable,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    }
);

/// Proof that the table's *operations* compile without `std`, not merely its
/// declaration.
///
/// The `worktable!` invocation above only proves the macro expands. That is a
/// weaker claim than it looks: a type can name itself fine and still be
/// unusable. This calls the three operations any consumer actually needs, so a
/// std-only path inside one of them fails the build.
///
/// Not run, because running needs an allocator and an executor that a
/// `no_std` target brings itself. Compiling is the claim being made.
pub async fn smoke(table: &NoStdTableWorkTable) -> Option<u64> {
    table.insert(NoStdTableRow { id: 1, value: 42 }).await.ok()?;
    let selected = table.select(NoStdTablePrimaryKey::from(1u64))?;
    let all = table.select_all().execute().ok()?;
    core::mem::drop(all);
    Some(selected.value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_calls_run_with_the_no_std_dependency_graph() {
        let table = NoStdTableWorkTable::default();
        assert_eq!(nagoya::block_on(smoke(&table)), Some(42));
        nagoya::block_on(table.delete(1u64)).unwrap();
        assert!(table.select(1u64).is_none());
    }

    #[test]
    fn snapshot_growth_and_reads_remain_safe_across_threads() {
        let table = NoStdTableWorkTable::default();
        std::thread::scope(|scope| {
            for worker in 0..4u64 {
                let table = &table;
                scope.spawn(move || {
                    for i in 0..16_384u64 {
                        let id = worker * 16_384 + i;
                        nagoya::block_on(table.insert(NoStdTableRow { id, value: id + 1 })).unwrap();
                        assert_eq!(table.select(id).unwrap().value, id + 1);
                    }
                });
            }
        });
        let rows = table.select_all().execute().unwrap();
        assert_eq!(rows.len(), 65_536);
    }

    #[test]
    fn operation_identifiers_use_the_os_clock_and_remain_ordered() {
        let first = OperationId::default();
        for _ in 0..1024 {
            let next = OperationId::default();
            assert!(next > first);
        }
        let OperationId::Single(id) = first else {
            panic!("expected single operation");
        };
        let (seconds, _) = id.get_timestamp().unwrap().to_unix();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(now.abs_diff(seconds) < 10);
    }
}
