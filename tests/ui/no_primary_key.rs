// Rule: every table needs a primary key. Without one there is nothing to
// resolve a row by, so the parser refuses the `columns` block outright rather
// than generating a table that can only be scanned.
use worktable::worktable;

worktable! {
    name: NoPrimaryKey,
    persist: false,
    columns: {
        id: u64,
        value: u64,
    },
}

fn main() {}
