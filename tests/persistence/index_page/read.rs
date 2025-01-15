use data_bucket::page::IndexValue;
use data_bucket::{parse_page, GeneralPage, Link, NewIndexPage, INNER_PAGE_SIZE};
use rkyv::{Archive, Serialize};
use serde::Deserialize;
use std::fs::{File, OpenOptions};

#[test]
fn test_index_page_read_after_create_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node.wt.idx")
        .unwrap();

    let page = parse_page::<NewIndexPage<u8>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 5);
    assert_eq!(page.inner.values_count, 1);
    assert_eq!(page.inner.slots.get(0).unwrap(), &0);
    assert_eq!(page.inner.index_values.get(0).unwrap().key, 5);
}

#[test]
fn test_index_page_read_after_insert_at_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node.wt.idx")
        .unwrap();

    let page = parse_page::<NewIndexPage<u8>, { INNER_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();
    assert_eq!(page.inner.node_id, 5);
    assert_eq!(page.inner.values_count, 2);
    assert_eq!(page.inner.slots.get(0).unwrap(), &1);
    assert_eq!(page.inner.index_values.get(0).unwrap().key, 5);
    assert_eq!(page.inner.slots.get(1).unwrap(), &0);
    assert_eq!(page.inner.index_values.get(1).unwrap().key, 4);
}

#[derive(Archive, Serialize)]
struct XD {
    a: u64,
    b: u8,
    c: Vec<IndexValue<u16>>,
}

#[test]
fn test() {
    let xd = XD {
        a: 1,
        b: 2,
        c: vec![
            IndexValue {
                key: 3,
                link: Link {
                    page_id: 0.into(),
                    offset: 1,
                    length: 2,
                },
            },
            IndexValue {
                key: 4,
                link: Link {
                    page_id: 0.into(),
                    offset: 5,
                    length: 6,
                },
            },
            IndexValue {
                key: 7,
                link: Link {
                    page_id: 0.into(),
                    offset: 8,
                    length: 9,
                },
            },
        ],
    };

    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&xd).unwrap();
    println!("{:?}", bytes);
}
