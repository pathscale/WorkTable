//! The narrow-primary-key lint, as a compiler warning rather than as tokens.
//!
//! A codegen test can only say the tokens are emitted. Whether rustc turns them
//! into a warning, and whether `#[allow(deprecated)]` silences it, is a
//! property of the expansion in a real crate, so it is checked in one.
//!
//! Both tables here are deliberate, so both are silenced. What this file
//! asserts is that the silencing works: if `#[allow(deprecated)]` stopped
//! covering the expansion, this file would warn, and `-D warnings` in CI would
//! fail it. That is the regression worth catching, because a lint a consumer
//! cannot turn off is worse than no lint.

use worktable::worktable;

/// 256 rows is genuinely what this means: one row per exchange, no partitions.
#[allow(deprecated)]
mod deliberate {
    use worktable::worktable;

    worktable!(
        name: Exchange,
        vec: true,
        columns: {
            id: u8 primary_key,
            name_len: u32,
        }
    );

    pub fn build() -> ExchangeWorkTable {
        let mut table = ExchangeWorkTable::new();
        table.insert(ExchangeRow { id: 3, name_len: 7 }).expect("fresh");
        table
    }
}

// A wide key is not linted, so it needs no `allow`. If the lint ever started
// firing on `u64`, this file would warn and `-D warnings` in CI would catch it.
worktable!(
    name: Wide,
    vec: true,
    columns: {
        id: u64 primary_key,
        v: u64,
    }
);

#[test]
fn a_silenced_narrow_key_still_works() {
    let table = deliberate::build();
    assert_eq!(table.select(&3).expect("present").name_len, 7);
}

#[test]
fn a_wide_key_needs_no_allow() {
    let mut table = WideWorkTable::new();
    table.insert(WideRow { id: 9, v: 1 }).expect("fresh");
    assert_eq!(table.select(&9).expect("present").v, 1);
}
