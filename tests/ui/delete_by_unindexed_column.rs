// Rule: a delete query declared `by` a column that is neither the primary key nor indexed is refused, because the keyed name would hide a full scan.

use worktable::worktable;

worktable!(
    name: DeleteBySlot,
    persist: false,
    columns: {
        id: u64 primary_key,
        slot: u64,
    },
    queries: {
        delete: {
            BySlot() by slot,
        }
    }
);

fn main() {}
