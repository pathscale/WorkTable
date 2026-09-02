use crate::remove_dir_if_exists;

use worktable::prelude::*;
use worktable_codegen::{worktable, worktable_version};

// A primary key that is not generated and not fixed-size. The read-only table it produces
// needs both the read-only shape and the unsized primary index, so this exercises the
// `#[table(read_only, pk_unsized)]` pairing that a `u64 primary_key autoincrement` does not.
worktable!(
    name: Doc,
    persist: true,
    columns: {
        id: String primary_key,
        title: String,
        author: String,
    },
    indexes: {
        author_idx: author,
    },
);

worktable_version!(
    name: DocV1,
    columns: {
        id: String primary_key,
        title: String,
        author: String,
    },
    indexes: {
        author_idx: author,
    },
);

#[test]
fn test_version_reads_persisted_data_with_string_primary_key() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/version/string_primary_key",
        DocWorkTable::name_snake_case(),
        DocWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/version/string_primary_key".to_string()).await;

        {
            let engine = DocPersistenceEngine::new(config.clone()).await.unwrap();
            let table = DocWorkTable::load(engine).await.unwrap();

            table
                .insert(DocRow {
                    id: "doc-alpha".to_string(),
                    title: "Alpha".to_string(),
                    author: "Alice".to_string(),
                })
                .await
                .unwrap();

            table
                .insert(DocRow {
                    id: "doc-beta".to_string(),
                    title: "Beta".to_string(),
                    author: "Bob".to_string(),
                })
                .await
                .unwrap();

            table.wait_for_ops().await.unwrap()
        }

        {
            let engine = ReadOnlyPersistenceEngine::create(config.clone()).await.unwrap();
            let table = DocV1WorkTable::load(engine).await.unwrap();

            assert_eq!(table.count(), 2);

            let rows = table.select_all().execute().unwrap();
            assert_eq!(rows.len(), 2);

            let titles: Vec<_> = rows.iter().map(|r| r.title.clone()).collect();
            assert!(titles.contains(&"Alpha".to_string()));
            assert!(titles.contains(&"Beta".to_string()));

            // Look the row up by its string key, not just by scanning every row.
            let alpha = table.select("doc-alpha".to_string()).unwrap();
            assert_eq!(alpha.author, "Alice".to_string());
        }
    });
}
