use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: SelectorTypes,
    columns: {
        id: u64 primary_key,
        amount: u64,
    },
    queries: {
        update: { AmountById(amount) by id }
    }
);

fn main() {
    let table = SelectorTypesWorkTable::default();
    let _ = table.update_by_id(1, SelectorTypesColumns::AMOUNT, "not a u64");
}
