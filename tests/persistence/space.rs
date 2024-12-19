use crate::persistence::{SizeTestRow, SizeTestWrapper, TestPersistRow, TestPersistSpace};

#[test]
fn test_file_write() {
    let row = TestPersistRow {
        another: 100,
        id: 1000,
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).unwrap();
    let space = TestPersistSpace {
        file: 
    }
}
