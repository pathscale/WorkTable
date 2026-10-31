// Rule: `#[derive(PersistIndex)]` propagates syntax errors from malformed
// `#[index(...)]` attributes instead of panicking.
use worktable_codegen::PersistIndex;

#[derive(PersistIndex)]
struct MalformedAttrIndex {
    #[index(123)]
    by_id: IndexMap<u64, u64>,
}

fn main() {}
