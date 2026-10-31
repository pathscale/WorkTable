// Rule: an update query cannot select by one part of a composite primary key, because that part alone does not identify a row.

use worktable::worktable;

worktable!(
    name: UpdateByTenant,
    persist: false,
    columns: {
        tenant_id: u64 primary_key using worktables_index,
        record_id: u64 primary_key using worktables_index,
        value: u64,
    },
    queries: {
        update: {
            ValueByTenant(value) by tenant_id,
        }
    }
);

fn main() {}
