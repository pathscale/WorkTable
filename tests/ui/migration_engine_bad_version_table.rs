// Rule: `migration_engine!` requires every `version_tables` path to end in a
// type name ending in `WorkTable`, so the corresponding `<Name>Row` type can
// be derived alongside it.
use worktable::migration_engine;

migration_engine! {
    migration: BadVersionTableMigration,
    current: ItemWorkTable,
    ctx: MigrationContext,
    version_tables: {
        1 => v1::NotATable,
    },
}

fn main() {}
