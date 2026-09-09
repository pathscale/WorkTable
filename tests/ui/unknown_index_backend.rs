// Rule: `using` takes one of the four index backends. A misspelling has to
// name the four rather than fall through to the default, because silently
// defaulting picks a data structure the author did not ask for.
use worktable::worktable;

worktable! {
    name: UnknownBackend,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
    indexes: {
        value_idx: value unique using treap,
    },
}

fn main() {}
