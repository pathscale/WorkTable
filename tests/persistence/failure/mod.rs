use std::time::Duration;
use tokio::time::timeout;
use worktable::prelude::*;
use worktable::worktable;

mod insert;
mod reinsert;
mod update;
mod update_non_unique;
mod update_unsized;

worktable!(
    name: TwoUniqueIdx,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        unique_a: u64,
        unique_b: u64,
    },
    indexes: {
        unique_a_idx: unique_a unique,
        unique_b_idx: unique_b unique,
    },
);

worktable!(
    name: ThreeUniqueIdx,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        unique_a: u64,
        unique_b: u64,
        unique_c: u64,
    },
    indexes: {
        unique_a_idx: unique_a unique,
        unique_b_idx: unique_b unique,
        unique_c_idx: unique_c unique,
    },
);

worktable!(
    name: MixedIdx,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        category: u64,
        unique_value: u64,
        data: u64,
    },
    indexes: {
        category_idx: category,
        unique_value_idx: unique_value unique,
    },
    queries: {
        update: { UniqueValueByCategory(unique_value) by category },
    },
);

worktable!(
    name: PrimaryOnly,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        data: u64,
    },
);

worktable!(
    name: NonUniqueUnsized,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        category: u64,
        unique_value: u64,
        name: String,
    },
    indexes: {
        category_idx: category,
        unique_value_idx: unique_value unique,
    },
    queries: {
        update: { NameAndValueByCategory(name, unique_value) by category },
    },
);

pub fn get_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}
