// Rule: `autoincrement` maps the key type to an atomic counter. `usize` reads
// like one of the accepted set and is not in the mapping, so it is the case
// worth pinning.
use worktable::worktable;

worktable! {
    name: AutoincrementUsize,
    persist: false,
    columns: {
        id: usize primary_key autoincrement,
        value: u64,
    },
}

fn main() {}
