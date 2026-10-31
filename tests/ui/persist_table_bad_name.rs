// Rule: `#[derive(PersistTable)]` requires the struct name to end in
// `WorkTable`, because it derives the space file and storage types by
// stripping that suffix.
use worktable_codegen::PersistTable;

#[derive(PersistTable)]
struct BadTableName(Inner<Row, u64>);

fn main() {}
