// Rule: `worktable!` requires a `columns` block. Without one there is no row
// shape or primary key to generate against.
use worktable::worktable;

worktable! {
    name: MissingColumns,
    persist: false,
}

fn main() {}
