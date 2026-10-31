// Rule: `migration_engine!` requires `current:` to name a struct ending in
// `WorkTable`, because it derives the table and row names by stripping that
// suffix.
use worktable::migration_engine;

migration_engine! {
    migration: BadCurrentMigration,
    current: NotATable,
    ctx: MigrationContext,
    version_tables: {},
}

fn main() {}
