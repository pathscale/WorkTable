// Rule: an `in_place` query writes the archived column bytes and maintains no
// index, so a column any index is built over cannot be mutated on that path.
// The index would keep resolving the old value.
use worktable::worktable;

worktable! {
    name: InPlaceIndexed,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
    indexes: {
        value_idx: value unique,
    },
    queries: {
        in_place: {
            ValueById(value) by id,
        }
    },
}

fn main() {}
