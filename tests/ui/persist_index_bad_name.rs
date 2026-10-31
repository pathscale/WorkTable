// Rule: `#[derive(PersistIndex)]` requires the struct name to end in `Index`,
// because it derives the table and persisted index names by stripping that
// suffix.
use worktable_codegen::PersistIndex;

#[derive(PersistIndex)]
struct BadName {
    by_id: IndexMap<u64, u64>,
}

fn main() {}
