// Rule: a non-unique index needs a backend that can store several rows under
// one key. Congee is an ART over unique keys, so pairing the two is refused
// and the message names the backends that do work.
use worktable::worktable;

worktable! {
    name: NonUniqueCongee,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        group_id: u64,
    },
    indexes: {
        group_idx: group_id using congee,
    },
}

fn main() {}
