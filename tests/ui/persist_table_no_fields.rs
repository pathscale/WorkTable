// Rule: `#[derive(PersistTable)]` requires a struct with at least one field
// holding the inner `WorkTable<Row, PrimaryKey, ...>`.
use worktable_codegen::PersistTable;

#[derive(PersistTable)]
struct EmptyWorkTable;

fn main() {}
