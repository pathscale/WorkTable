// Rule: the backends that persist differently from the default require the
// author to say which they meant. Omitting `persist` leaves the choice to the
// macro, and for these backends that choice is not one it should make.
use worktable::worktable;

worktable! {
    name: CongeeNoPersist,
    columns: {
        id: u64 primary_key autoincrement using congee,
        value: u64,
    },
}

fn main() {}
