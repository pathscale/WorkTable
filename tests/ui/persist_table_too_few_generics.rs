// Rule: `#[derive(PersistTable)]` requires the first field's type to carry at
// least two generic type arguments (`<Row, PrimaryKey, ...>`).
use worktable_codegen::PersistTable;

#[derive(PersistTable)]
struct TooFewGenericsWorkTable(Inner<u64>);

fn main() {}
