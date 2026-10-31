// Rule: `#[derive(PersistIndex)]` requires each field type to carry generic
// type arguments in angle brackets so the indexed key type can be extracted.
use worktable_codegen::PersistIndex;

#[derive(PersistIndex)]
struct MissingTypeArgsIndex {
    by_id: u64,
}

fn main() {}
