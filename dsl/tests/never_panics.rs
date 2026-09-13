//! `check` is the oracle every tool trusts: an editor calls it on each
//! keystroke, and a generator validates against it before emitting. Both feed
//! it text that is wrong in ways nobody enumerated, so the contract it has to
//! keep is that a bad declaration comes back as a diagnostic and never as a
//! panic. A proc macro that panics reports a compiler ICE-shaped error with no
//! span, which is the worst possible way to tell someone they typed a comma.
//!
//! This is a mutation sweep rather than a fuzzer: a fixed seed, valid
//! declarations as seeds, and the edits that actually happen while typing.
//! Deterministic, so a failure here is reproducible from the printed input.

use std::panic::{AssertUnwindSafe, catch_unwind};

const SEEDS: &[&str] = &[
    "worktable!(name: T, columns: { id: u64 primary_key });",
    "worktable!(name: T, persist: true, columns: { id: u64 primary_key autoincrement, v: String }, indexes: { v_idx: v unique });",
    "worktable!(name: T, columns: { id: u32 primary_key using arctic, v: i64 }, indexes: { v_idx: v });",
    "worktable!(name: T, columns: { id: u64 primary_key }, queries: { update: { ById(v) by id } });",
    "worktable!(name: T, persist: false, columns: { id: u8 primary_key using congee }, config: { page_size: 4096 });",
];

/// A tiny deterministic PRNG, so this needs no dependency and a failure is
/// reproducible from the seed alone.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn mutate(rng: &mut Rng, source: &str) -> String {
    let mut bytes: Vec<char> = source.chars().collect();
    if bytes.is_empty() {
        return String::new();
    }
    match rng.below(6) {
        // Truncate: what every declaration looks like while it is typed.
        0 => bytes.truncate(rng.below(bytes.len())),
        // Drop a character.
        1 => {
            let at = rng.below(bytes.len());
            bytes.remove(at);
        }
        // Duplicate one, which is how doubled commas and braces arrive.
        2 => {
            let at = rng.below(bytes.len());
            bytes.insert(at, bytes[at]);
        }
        // Swap two neighbours.
        3 => {
            let at = rng.below(bytes.len().saturating_sub(1).max(1));
            if at + 1 < bytes.len() {
                bytes.swap(at, at + 1);
            }
        }
        // Insert a delimiter or separator somewhere it does not belong.
        4 => {
            let at = rng.below(bytes.len());
            let c = [',', ':', '{', '}', '(', ')', ';'][rng.below(7)];
            bytes.insert(at, c);
        }
        // Replace a character with another from the source alphabet.
        _ => {
            let at = rng.below(bytes.len());
            let from = rng.below(bytes.len());
            bytes[at] = bytes[from];
        }
    }
    bytes.into_iter().collect()
}

#[test]
fn check_never_panics_on_malformed_input() {
    let mut rng = Rng(0x5EED_1234_ABCD_0001);
    let mut failures = Vec::new();

    for seed in SEEDS {
        // Compounding edits, so the sweep reaches inputs several mistakes deep
        // rather than only one edit from valid.
        let mut current = seed.to_string();
        for round in 0..2_000 {
            current = mutate(&mut rng, &current);
            if catch_unwind(AssertUnwindSafe(|| {
                let _ = worktable_dsl::check(&current);
            }))
            .is_err()
            {
                failures.push(format!("round {round}: {current:?}"));
                break;
            }
            // Restart from the seed periodically so one degenerate string does
            // not swallow the rest of the budget.
            if round % 50 == 49 {
                current = seed.to_string();
            }
        }
    }

    assert!(
        failures.is_empty(),
        "`check` panicked on {} input(s) instead of returning diagnostics:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The mutation sweep above rarely produces input that *parses* and is then
/// semantically wrong, which is exactly where the known panics lived: a
/// `.expect` reached after the grammar was satisfied. These are written by
/// hand because a character-level mutator will not find them.
#[test]
fn check_reports_semantic_errors_rather_than_panicking() {
    let cases: &[(&str, &str)] = &[
        (
            "autoincrement with no primary key at all",
            "worktable!(name: T, columns: { id: u64 autoincrement, v: String });",
        ),
        (
            "autoincrement on a type that has no atomic",
            "worktable!(name: T, columns: { id: usize primary_key autoincrement });",
        ),
        (
            "autoincrement on a string",
            "worktable!(name: T, columns: { id: String primary_key autoincrement });",
        ),
        (
            "congee over a string key",
            "worktable!(name: T, persist: false, columns: { id: String primary_key using congee });",
        ),
        (
            "arctic over a signed type it does not take",
            "worktable!(name: T, persist: false, columns: { id: i8 primary_key using arctic });",
        ),
        (
            "congee on a non-unique secondary",
            "worktable!(name: T, persist: false, columns: { id: u64 primary_key, v: u64 }, indexes: { v_idx: v using congee });",
        ),
        (
            "an ART backend with no persistence decision",
            "worktable!(name: T, columns: { id: u64 primary_key using arctic });",
        ),
        ("no columns at all", "worktable!(name: T, columns: { });"),
        ("no primary key", "worktable!(name: T, columns: { v: u64 });"),
        (
            "two primary keys",
            "worktable!(name: T, columns: { a: u64 primary_key, b: u64 primary_key });",
        ),
        (
            "an index over a column that does not exist",
            "worktable!(name: T, columns: { id: u64 primary_key }, indexes: { ghost_idx: nope });",
        ),
        (
            "a query over a column that does not exist",
            "worktable!(name: T, columns: { id: u64 primary_key }, queries: { update: { ById(nope) by id } });",
        ),
        (
            "a page size that is not a number",
            "worktable!(name: T, persist: true, columns: { id: u64 primary_key }, config: { page_size: huge });",
        ),
    ];

    let mut panicked = Vec::new();
    let mut accepted = Vec::new();
    for (what, source) in cases {
        match catch_unwind(AssertUnwindSafe(|| worktable_dsl::check(source))) {
            Err(_) => panicked.push(*what),
            // Not panicking is the floor. The point of `check` is to answer
            // "would the macro accept this", so silently accepting one of
            // these is the same class of bug wearing a quieter face: a
            // generator validating against it emits a declaration that then
            // fails to compile.
            Ok(checked) if checked.diagnostics.is_empty() => accepted.push(*what),
            Ok(_) => {}
        }
    }

    assert!(
        panicked.is_empty(),
        "`check` panicked instead of reporting a diagnostic for: {panicked:?}"
    );
    assert!(
        accepted.is_empty(),
        "`check` accepted declarations the macro refuses: {accepted:?}"
    );
}
