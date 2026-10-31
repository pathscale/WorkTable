// Rule: `#[derive(PersistIndex)]` requires a struct with named fields, where
// each field names one secondary index.
use worktable_codegen::PersistIndex;

#[derive(PersistIndex)]
struct TupleIndex(u64);

fn main() {}
