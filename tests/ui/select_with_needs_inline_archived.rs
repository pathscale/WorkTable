// Rule: `select_with` reads the archived cell in place with no copy, so a
// concurrent writer can tear a value the closure observes. That is recoverable
// for an inline scalar, because the seqlock retry discards whatever the closure
// computed. It is undefined behaviour for a relative pointer, which is what an
// archived `String` or `Vec` column is. Tables carrying one therefore do not
// get a `select_with` at all; they use the owned `select`.
use worktable::prelude::*;
use worktable::worktable;

worktable! {
    name: HasString,
    persist: false,
    columns: {
        id: u64 primary_key,
        label: String,
    },
}

fn main() {
    let table = HasStringWorkTable::default();
    let _ = table.select_with(1u64, |archived| archived.id);
}
