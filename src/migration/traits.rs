/// User implements this trait for NEIGHBOR VERSION PAIRS only.
/// v1→v2, v2→v3, etc. The macro chains them internally.
/// Single context type shared across all migrations.
pub trait Migration<FromRow, ToRow> {
    type Context: Default + Send + Sync;

    fn migrate(row: FromRow, ctx: &Self::Context) -> ToRow;
}
