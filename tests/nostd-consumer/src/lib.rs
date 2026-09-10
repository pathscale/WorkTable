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
pub fn smoke(table: &NoStdTableWorkTable) -> Option<u64> {
    let inserted = table.insert(NoStdTableRow { id: 1, value: 42 });
    core::mem::drop(inserted);
    let selected = table.select(NoStdTablePrimaryKey::from(1u64))?;
    let all = table.select_all().execute().ok()?;
    core::mem::drop(all);
    Some(selected.value)
}
