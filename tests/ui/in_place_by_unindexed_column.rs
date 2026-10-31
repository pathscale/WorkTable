// Rule: an in-place update declared `by` a column that is neither the primary key nor indexed is refused, because the keyed name would hide a full scan.

use worktable::worktable;

worktable!(
    name: InPlaceBySlot,
    persist: false,
    columns: {
        id: u64 primary_key,
        amount: u64,
        slot: u64,
    },
    queries: {
        update_in_place: {
            BumpBySlot(amount) by slot,
        }
    }
);

fn main() {}
