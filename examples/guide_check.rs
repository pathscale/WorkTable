//! The declaration and the three calls printed in `docs/wt-user-guide.typ`.
//! If the guide drifts from the API, this stops compiling.
use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Order,
    columns: {
        id: u64 primary_key autoincrement,
        symbol: String,
        quantity: u64,
    },
    indexes: {
        symbol_idx: symbol,
    }
);

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let table = OrderWorkTable::default();
    table
        .insert(OrderRow {
            id: table.get_next_pk().into(),
            symbol: "ETH".into(),
            quantity: 3,
        })
        .await?;
    let found = table.select_by_symbol("ETH".into()).execute()?;
    assert_eq!(found.len(), 1);
    Ok(())
}
