// Rule: `#[derive(PersistIndex)]` requires each field type to be a generic path
// type such as `IndexMap<K, V>`.
use worktable_codegen::PersistIndex;

#[derive(PersistIndex)]
struct NonPathFieldIndex {
    by_id: (u64, u64),
}

fn main() {}
