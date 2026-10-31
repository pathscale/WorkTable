use worktable_dsl::check::check;

#[test]
fn paged_mutation_shapes_fail_before_emission() {
    for query in [
        "update: { Change(value) by value }",
        "update_in_place: { Change(value) by value }",
        "update_in_place: { Change(id) by id }",
    ] {
        let checked = check(&format!(
            "name: T, columns: {{ id: u64 primary_key, value: u64 }}, queries: {{ {query} }}"
        ));
        assert!(!checked.diagnostics.is_empty(), "{query} should be rejected");
    }
}
#[test]
fn a_vec_query_cannot_silently_ignore_a_runtime_profile() {
    let checked = check(
        "name: T, vec: true, columns: { id: u64 primary_key, value: u64 }, queries: { update runtime scheduled: { Change(value) by id } }",
    );
    assert!(checked.diagnostics.iter().any(|d| d.message.contains("synchronous")));
}
#[test]
fn supported_paged_index_updates_remain_valid() {
    let checked = check(
        "name: T, columns: { id: u64 primary_key, value: u64, amount: u64 }, indexes: { value_idx: value }, queries: { update: { Change(amount) by value } }",
    );
    assert!(checked.diagnostics.is_empty(), "{:?}", checked.diagnostics);
}

#[test]
fn paged_update_over_primary_key_is_rejected_and_vec_is_allowed() {
    for decl in [
        "name: T, persist: false, columns: { id: u64 primary_key, v: u64 }, queries: { update: { ReKey(id) by id } }",
        "name: T, persist: true, columns: { id: u64 primary_key, k: u64, v: u64 }, indexes: { k_idx: k unique }, queries: { update: { ReKey(id, v) by k } }",
        "name: T, persist: false, columns: { a: u64 primary_key, b: u64 primary_key, k: u64 }, indexes: { k_idx: k unique }, queries: { update: { ReKey(b) by k } }",
        "name: T, persist: false, columns: { a: u64 primary_key, b: u64 primary_key, v: u64 }, queries: { update: { ReKey(a, v) by a } }",
    ] {
        let checked = check(decl);
        assert!(
            checked
                .diagnostics
                .iter()
                .any(|d| d.message.contains("cannot change primary key column")
                    && d.message.contains("delete the row and insert it with the new key")),
            "expected primary-key update error for `{decl}`, got: {:?}",
            checked.diagnostics
        );
    }

    let vec_checked = check(
        "name: T, vec: true, columns: { id: u64 primary_key, v: u64 }, queries: { update: { ReKey(id, v) by id } }",
    );
    assert!(vec_checked.diagnostics.is_empty(), "{:?}", vec_checked.diagnostics);
}

#[test]
fn duplicate_index_on_same_column_is_rejected() {
    let checked =
        check("name: T, persist: false, columns: { id: u64 primary_key, a: u64 }, indexes: { a1: a, a2: a unique }");
    assert!(
        checked
            .diagnostics
            .iter()
            .any(|d| d.message == "column `a` is already indexed by `a1`; declare one index per column"),
        "got: {:?}",
        checked.diagnostics
    );
}

#[test]
fn update_with_empty_column_set_is_rejected() {
    for query in ["update: { Empty() by id }", "update_in_place: { Empty() by id }"] {
        let checked = check(&format!(
            "name: T, persist: false, columns: {{ id: u64 primary_key, a: u64 }}, queries: {{ {query} }}"
        ));
        assert!(
            checked
                .diagnostics
                .iter()
                .any(|d| d.message == "update query `Empty` declares no columns to update"),
            "got: {:?}",
            checked.diagnostics
        );
    }
}

#[test]
fn delete_with_columns_is_rejected() {
    let checked = check(
        "name: T, persist: false, columns: { id: u64 primary_key, a: u64, b: u64 }, indexes: { b_idx: b }, queries: { delete: { ByB(a) by b } }",
    );
    assert!(
        checked
            .diagnostics
            .iter()
            .any(|d| d.message == "delete query `ByB` takes no columns; write it as `ByB() by b`"),
        "got: {:?}",
        checked.diagnostics
    );
}
