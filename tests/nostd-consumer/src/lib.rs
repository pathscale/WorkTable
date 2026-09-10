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
