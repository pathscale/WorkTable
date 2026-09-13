use worktable::worktable;

worktable!(
    name: SelectorCollision,
    columns: {
        id: u64 primary_key,
        amount: u64,
    },
    queries: {
        update: {
            First(amount) by id,
            Second(amount) by id,
        }
    }
);

fn main() {}
