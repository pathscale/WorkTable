//! What the migration planner promises, one claim per test.

use worktable_dsl::{Change, Cost, Diff, Schema, TableChange, TransformReason, plan};

fn parse(source: &str) -> Schema {
    Schema::parse(source).unwrap_or_else(|error| panic!("{error}\n{source}"))
}

fn base() -> Schema {
    parse(
        "
        name: User,
        version: 1,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            email: String,
            age: u8,
        },
        indexes: { email_idx: email unique }
        ",
    )
}

#[test]
fn a_schema_does_not_differ_from_itself() {
    let diff = Diff::between(&base(), &base());
    assert!(diff.is_empty());
    assert_eq!(diff.cost(), Cost::Nothing);
    assert!(diff.rows_are_readable());
    assert_eq!(diff.describe(), "`User` is unchanged");
}

#[test]
fn a_version_bump_on_its_own_costs_nothing() {
    // The version is what triggers a migration, not what it costs. A binary
    // that bumped the version and changed nothing else has nothing to do, and
    // saying so is what keeps the bump cheap enough to be habitual.
    let declared = parse(
        "
        name: User,
        version: 2,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            email: String,
            age: u8,
        },
        indexes: { email_idx: email unique }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert_eq!(diff.changes, vec![Change::Version { from: 1, to: 2 }]);
    assert_eq!(diff.cost(), Cost::Nothing);
    assert!(diff.rows_are_readable());
}

#[test]
fn an_added_index_leaves_the_rows_where_they_are() {
    // Every index holds links, and rebuilding one reads rows that have not
    // moved. Nothing is invalidated, so this is the cheap kind of change.
    let declared = parse(
        "
        name: User,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            email: String,
            age: u8,
        },
        indexes: { email_idx: email unique, age_idx: age }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert_eq!(diff.cost(), Cost::RebuildIndexes);
    assert!(diff.rows_are_readable());
    assert!(diff.transforms_required().is_empty());
}

#[test]
fn an_added_optional_column_rewrites_rows_but_needs_no_decision() {
    // The archived layout changes, so every link is invalidated and every row
    // is copied forward. There is only one value the new column could hold in
    // an existing row, so nobody has to be asked.
    let declared = parse(
        "
        name: User,
        version: 2,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            email: String,
            age: u8,
            nickname: String optional,
        },
        indexes: { email_idx: email unique }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert_eq!(diff.cost(), Cost::RewriteRows);
    assert!(!diff.rows_are_readable());
    assert!(diff.transforms_required().is_empty());
}

#[test]
fn an_added_required_column_has_to_be_asked_about() {
    let declared = parse(
        "
        name: User,
        version: 2,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            email: String,
            age: u8,
            nickname: String,
        },
        indexes: { email_idx: email unique }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert_eq!(diff.cost(), Cost::RewriteRows);
    let transforms = diff.transforms_required();
    assert_eq!(transforms.len(), 1);
    assert_eq!(transforms[0].column, "nickname");
    assert_eq!(
        transforms[0].reason,
        TransformReason::NoValueToFillItWith {
            ty: "String".to_string()
        }
    );
}

#[test]
fn a_type_change_has_to_be_asked_about() {
    let declared = parse(
        "
        name: User,
        version: 2,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            email: String,
            age: u32,
        },
        indexes: { email_idx: email unique }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    let transforms = diff.transforms_required();
    assert_eq!(transforms.len(), 1);
    assert_eq!(
        transforms[0].reason,
        TransformReason::NoConversionExists {
            from: "u8".to_string(),
            to: "u32".to_string(),
        }
    );
}

#[test]
fn widening_to_optional_is_decided_but_narrowing_is_not() {
    // There is exactly one thing an existing value becomes when a column gains
    // `optional`. There is no one thing a stored `None` becomes when it loses
    // it, and that is a question about the data rather than about the schema.
    let widened = parse(
        "
        name: User, version: 2, persist: true,
        columns: { id: u64 primary_key autoincrement, email: String, age: u8 optional },
        indexes: { email_idx: email unique }
        ",
    );
    assert!(Diff::between(&base(), &widened).transforms_required().is_empty());

    let narrowed = Diff::between(&widened, &base());
    assert_eq!(narrowed.transforms_required().len(), 1);
    assert_eq!(
        narrowed.transforms_required()[0].reason,
        TransformReason::NothingToPutWhereNoneWas
    );
}

#[test]
fn reordering_columns_is_a_layout_change() {
    // Declaration order is the generated row struct's field order, so moving a
    // column changes the archived layout exactly as changing its type does.
    // It is the change most likely to be made by accident and least likely to
    // look like one.
    let declared = parse(
        "
        name: User, version: 2, persist: true,
        columns: { id: u64 primary_key autoincrement, age: u8, email: String },
        indexes: { email_idx: email unique }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert_eq!(diff.cost(), Cost::RewriteRows);
    assert!(diff.changes.iter().any(|change| matches!(
        change,
        Change::ColumnMoved { name, from: 1, to: 2 } if name == "email"
    )));
}

#[test]
fn a_dropped_column_says_the_data_goes_with_it() {
    let declared = parse(
        "
        name: User, version: 2, persist: true,
        columns: { id: u64 primary_key autoincrement, email: String },
        indexes: { email_idx: email unique }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert_eq!(diff.cost(), Cost::RewriteRows);
    assert!(
        diff.warnings()
            .iter()
            .any(|warning| warning.contains("`age` is dropped"))
    );
}

#[test]
fn a_renamed_column_reads_as_a_drop_and_an_add() {
    // Nothing in a declaration distinguishes a rename from a deletion next to
    // an unrelated addition, and guessing by type would be wrong exactly when
    // it mattered. The transform is where the intent goes.
    let declared = parse(
        "
        name: User, version: 2, persist: true,
        columns: { id: u64 primary_key autoincrement, email_address: String, age: u8 },
        indexes: { email_idx: email_address unique }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert!(
        diff.changes
            .iter()
            .any(|c| matches!(c, Change::ColumnAdded(column) if column.name == "email_address"))
    );
    assert!(
        diff.changes
            .iter()
            .any(|c| matches!(c, Change::ColumnDropped(column) if column.name == "email"))
    );
    assert_eq!(diff.transforms_required().len(), 1);
}

#[test]
fn a_changed_primary_key_needs_a_person() {
    let declared = parse(
        "
        name: User, version: 2, persist: true,
        columns: { id: u64 primary_key, email: String primary_key, age: u8 },
        indexes: { email_idx: email unique }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert_eq!(diff.cost(), Cost::NeedsIntent);
    assert!(!diff.rows_are_readable());
}

#[test]
fn a_changed_partition_key_needs_a_person() {
    // The routing key is not in the row, so which partition a row belongs to
    // cannot be recomputed from the row: it is only knowable from where the
    // row already is.
    let stored = parse("name: Price, columns: { id: u64 primary_key, bid: f64 }");
    let declared =
        parse("name: Price, version: 2, partition_by: shard: u32, columns: { id: u64 primary_key, bid: f64 }");
    assert_eq!(Diff::between(&stored, &declared).cost(), Cost::NeedsIntent);
}

#[test]
fn making_an_index_unique_says_it_can_still_fail() {
    let stored = parse(
        "name: User, persist: true, columns: { id: u64 primary_key, email: String }, indexes: { email_idx: email }",
    );
    let declared = parse(
        "name: User, version: 2, persist: true, columns: { id: u64 primary_key, email: String }, indexes: { email_idx: email unique }",
    );
    let diff = Diff::between(&stored, &declared);
    assert_eq!(diff.cost(), Cost::RebuildIndexes);
    assert!(diff.warnings().iter().any(|warning| warning.contains("duplicate")));
}

#[test]
fn a_schema_change_without_a_version_bump_is_still_visible() {
    // This is the middle branch of the load state machine: the versions agree
    // and the schemas do not, which is a forgotten bump rather than a
    // migration. Catching it costs one comparison of two small structs and no
    // row access at all.
    let declared = parse(
        "
        name: User, version: 1, persist: true,
        columns: { id: u64 primary_key autoincrement, email: String, age: u32 },
        indexes: { email_idx: email unique }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert!(
        !diff
            .changes
            .iter()
            .any(|change| matches!(change, Change::Version { .. }))
    );
    assert!(!diff.is_empty());
    assert!(!diff.rows_are_readable());
}

#[test]
fn queries_and_config_never_reach_the_data() {
    let declared = parse(
        "
        name: User, version: 2, persist: true,
        columns: { id: u64 primary_key autoincrement, email: String, age: u8 },
        indexes: { email_idx: email unique },
        queries: { update: { Age(age) by id } },
        config: { row_derives: Clone }
        ",
    );
    let diff = Diff::between(&base(), &declared);
    assert_eq!(diff.cost(), Cost::Nothing);
    assert!(diff.rows_are_readable());
    assert!(diff.changes.contains(&Change::QueriesChanged));
    assert!(diff.changes.contains(&Change::ConfigChanged));
}

#[test]
fn a_plan_sorts_tables_into_created_changed_and_dropped() {
    let stored = vec![
        base(),
        parse("name: Legacy, persist: true, columns: { id: u64 primary_key }"),
    ];
    let declared = vec![
        parse(
            "
            name: User, version: 2, persist: true,
            columns: { id: u64 primary_key autoincrement, email: String, age: u8, nickname: String optional },
            indexes: { email_idx: email unique }
            ",
        ),
        parse("name: Session, persist: true, columns: { id: u64 primary_key }"),
    ];

    let plan = plan(&stored, &declared);
    assert_eq!(plan.len(), 3);
    assert!(plan.contains(&TableChange::Created("Session".to_string())));
    assert!(plan.contains(&TableChange::Dropped("Legacy".to_string())));
    assert!(plan.iter().any(|change| matches!(
        change,
        TableChange::Changed(diff) if diff.table == "User" && diff.cost() == Cost::RewriteRows
    )));
}

#[test]
fn a_new_table_costs_nothing_and_a_dropped_one_is_never_assumed() {
    // A table the binary declares and disk does not is created empty: there is
    // nothing to move. A table on disk the binary no longer declares is a
    // different matter, because deleting data is a decision rather than a
    // consequence of a declaration.
    assert_eq!(TableChange::Created("New".to_string()).cost(), Cost::Nothing);
    assert_eq!(TableChange::Dropped("Old".to_string()).cost(), Cost::NeedsIntent);
}

#[test]
fn an_unchanged_table_is_absent_from_the_plan() {
    assert!(plan(&[base()], &[base()]).is_empty());
}

#[test]
fn the_report_names_the_cost_the_changes_and_what_is_still_needed() {
    let declared = parse(
        "
        name: User, version: 2, persist: true,
        columns: { id: u64 primary_key autoincrement, email: String, nickname: String },
        indexes: { email_idx: email unique }
        ",
    );
    let report = Diff::between(&base(), &declared).describe();
    assert!(report.contains("every row is copied forward"));
    assert!(report.contains("version 1 -> 2"));
    assert!(report.contains("column added: nickname: String"));
    assert!(report.contains("column dropped: age: u8"));
    assert!(report.contains("needs a transform written for:"));
    assert!(report.contains("nickname: new non-optional column"));
    assert!(report.contains("note: column `age` is dropped"));
}
