// Rule: the persisted backend refuses a delete query declared `by` a column that is neither the primary key nor indexed, exactly like the in-memory one.

use worktable::worktable;

worktable!(
    name: DeleteBySlotPersist,
    persist: true,
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
