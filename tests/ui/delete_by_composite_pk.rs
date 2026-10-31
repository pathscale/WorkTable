// Rule: a delete query cannot select by one part of a composite primary key, because that part alone does not identify a row.

use worktable::worktable;

worktable!(
    name: DeleteByTenant,
    persist: false,
    columns: {
        tenant_id: u64 primary_key using worktables_index,
        record_id: u64 primary_key using worktables_index,
        value: u64,
    },
    queries: {
        delete: {
            ByTenant() by tenant_id,
        }
    }
);

fn main() {}
