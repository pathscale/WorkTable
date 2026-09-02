use std::sync::atomic::{AtomicU64, Ordering};

use rkyv::{Archive, Deserialize, Serialize};
use worktable::prelude::*;
use worktable::worktable;

#[derive(
    Archive,
    Debug,
    Default,
    Deserialize,
    Clone,
    Eq,
    From,
    Hash,
    PartialOrd,
    PartialEq,
    Ord,
    Serialize,
    SizeMeasure,
    MemStat,
)]
#[rkyv(compare(PartialEq), derive(Debug, PartialOrd, PartialEq, Eq, Ord))]
struct CustomId(u64);

#[derive(Debug, Default)]
pub struct Generator(AtomicU64);

impl PrimaryKeyGenerator<TestPrimaryKey> for Generator {
    fn next(&self) -> TestPrimaryKey {
        let res = self.0.fetch_add(1, Ordering::Relaxed);

        if res >= 10 {
            self.0.store(0, Ordering::Relaxed);
        }

        CustomId::from(res).into()
    }
}

impl TablePrimaryKey for TestPrimaryKey {
    type Generator = Generator;
}

worktable! (
    name: Test,
    columns: {
        id: CustomId primary_key custom,
        test: u64
    }
);

#[test]
fn test_custom_pk() {
    let table = TestWorkTable::default();
    let pk = table.get_next_pk();
    assert_eq!(pk, CustomId::from(0).into());

    for _ in 0..10 {
        let _ = table.get_next_pk();
    }
    let pk = table.get_next_pk();
    assert_eq!(pk, CustomId::from(0).into());
}

#[tokio::test]
async fn borrowed_custom_primary_key_is_accepted() {
    let table = TestWorkTable::default();
    let id = CustomId(42);
    let row = TestRow {
        id: id.clone(),
        test: 7,
    };
    table.insert(row.clone()).await.unwrap();

    assert_eq!(table.select(&id), Some(row));
    table.delete(&id).await.unwrap();
    assert!(table.select(&id).is_none());
}
