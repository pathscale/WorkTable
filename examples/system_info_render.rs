use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Shown,
    columns: { id: u64 primary_key autoincrement, symbol: String, qty: u64 },
    indexes: { symbol_idx: symbol, qty_idx: qty }
);

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let table = ShownWorkTable::default();
    for i in 0..5_000u64 {
        table
            .insert(ShownRow {
                id: table.get_next_pk().into(),
                symbol: format!("SYM{}", i % 97),
                qty: i,
            })
            .await?;
    }
    print!("{}", table.system_info());
    Ok(())
}
