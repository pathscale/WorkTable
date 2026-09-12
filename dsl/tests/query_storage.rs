use worktable_dsl::check::check;

#[test]
fn paged_mutation_shapes_fail_before_emission() {
    for query in [
        "update: { Change(value) by value }",
        "in_place: { Change(value) by value }",
        "in_place: { Change(id) by id }",
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
