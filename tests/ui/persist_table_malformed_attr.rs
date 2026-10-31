// Rule: `#[derive(PersistTable)]` propagates syntax errors from malformed
// `#[table(...)]` attributes instead of panicking.
use worktable_codegen::PersistTable;

#[derive(PersistTable)]
#[table(123)]
struct MalformedAttrWorkTable(Inner<Row, u64>);

fn main() {}
