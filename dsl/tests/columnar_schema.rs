use worktable_dsl::schema::{Change, Cost, Diff};
use worktable_dsl::{Schema, check};

const SOURCE: &str = "
    name: Metrics,
    persist: false,
    columns: {
        id: u64 primary_key,
        host: u64 columnar(chunk_rows(8), compression(none)),
        timestamp: i64 columnar,
    },
    columnar_indexes: { host_time: { cluster_by: [host, timestamp] } },
    config: { columnar_slot_id: ColumnSlotId16, columnar_chunk_rows: 32 },
";

#[test]
fn columnar_survives_the_round_trip() {
    let schema = Schema::parse(SOURCE).unwrap();
    assert_eq!(schema.columns.iter().filter(|c| c.columnar.is_some()).count(), 2);
    assert_eq!(
        schema.column("host").unwrap().columnar.as_ref().unwrap().chunk_rows,
        Some(8)
    );
    assert_eq!(schema.columnar_indexes[0].cluster_by, ["host", "timestamp"]);
    assert_eq!(schema.config.columnar_chunk_rows, Some(32));
    assert_eq!(schema.config.columnar_slot_id.as_deref(), Some("ColumnSlotId16"));
    assert_eq!(schema, Schema::parse(&schema.to_dsl()).unwrap());
    assert!(check(&schema.to_dsl()).is_acceptable());
}

#[test]
fn every_derived_layout_change_requires_a_rebuild() {
    let stored = Schema::parse(SOURCE).unwrap();
    let mut variants = vec![stored.clone(); 4];
    variants[0].config.columnar_chunk_rows = Some(64);
    variants[1].config.columnar_slot_id = Some("ColumnSlotId32".into());
    variants[2].columnar_indexes[0].cluster_by.reverse();
    variants[3].columns[1].columnar.as_mut().unwrap().chunk_rows = Some(16);
    for declared in variants {
        let diff = Diff::between(&stored, &declared);
        assert!(diff.changes.contains(&Change::ColumnarChanged), "{}", diff.describe());
        assert_eq!(diff.cost(), Cost::RebuildIndexes);
        assert!(diff.rows_are_readable());
    }
}

#[test]
fn checker_rejects_a_cluster_key_that_is_not_columnar() {
    let checked = check(
        "name: Bad, persist: false,
        columns: { id: u64 primary_key, value: u64 columnar, other: u64 },
        columnar_indexes: { bad: { cluster_by: [other] } }",
    );
    assert!(checked.schema.is_some());
    assert!(
        checked.diagnostics.iter().any(|d| d.message.contains("requires field")),
        "{:?}",
        checked.diagnostics
    );
}

#[test]
fn changed_page_size_cannot_reuse_row_links() {
    let stored = Schema::parse("name: Rows, columns: { id: u64 primary_key }, config: { page_size: 8192 }").unwrap();
    let declared = Schema::parse("name: Rows, columns: { id: u64 primary_key }, config: { page_size: 32768 }").unwrap();
    let diff = Diff::between(&stored, &declared);
    assert_eq!(diff.cost(), Cost::RewriteRows);
    assert!(!diff.rows_are_readable());
    assert!(diff.describe().contains("page size"));
}

#[test]
fn changing_storage_requires_an_explicit_conversion() {
    let stored = Schema::parse("name: Rows, columns: { id: u64 primary_key }").unwrap();
    let declared = Schema::parse("name: Rows, vec: true, columns: { id: u64 primary_key }").unwrap();
    let diff = Diff::between(&stored, &declared);
    assert_eq!(diff.cost(), Cost::NeedsIntent);
    assert!(!diff.rows_are_readable());
    assert!(diff.describe().contains("row storage"));
}

#[test]
fn changing_runtime_is_reported_without_a_row_rewrite() {
    let stored = Schema::parse(SOURCE).unwrap();
    let declared = Schema::parse(&format!("{SOURCE} runtime: nagoya(spread),")).unwrap();
    let diff = Diff::between(&stored, &declared);
    assert_eq!(diff.changes, vec![Change::RuntimeChanged]);
    assert_eq!(diff.cost(), Cost::Nothing);
}
