pub mod config;

use worktable::prelude::*;
use worktable::worktable;

// Simple table for basic insert/select benchmarks
worktable!(
    name: Simple,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    }
);

// Table with unique index for index lookup benchmarks
worktable!(
    name: UniqueIndex,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64,
    },
    indexes: {
        test_idx: test unique,
    }
);

// Table with non-unique index for multi-value lookups
worktable!(
    name: NonUniqueIndex,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
        category: u64,
    },
    indexes: {
        category_idx: category,
    }
);

// Table with String field and queries for async benchmarks
worktable!(
    name: FullFeatured,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        val1: u64,
        another: String,
        something: u64,
    },
    indexes: {
        val1_idx: val1 unique,
        another_idx: another,
    },
    queries: {
        in_place: {
            ValById(val) by id,
        }
        update: {
            AnotherById(another) by id,
            SomethingById(something) by id,
            AnotherByVal1(another) by val1,
        },
        delete: {
            ById() by id,
            ByAnother() by another,
        }
    }
);