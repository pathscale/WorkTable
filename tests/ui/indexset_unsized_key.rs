// Rule: `using indexset` cannot hold a variable-sized key. The upstream crate
// has no node type for one, so the declaration is refused with the backend
// that can, rather than being accepted and failing deep inside the emitted
// generic types.
use worktable::worktable;

worktable! {
    name: IndexsetUnsized,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        name: String,
    },
    indexes: {
        name_idx: name unique using indexset,
    },
}

fn main() {}
