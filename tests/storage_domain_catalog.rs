use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use worktable::Database;
use worktable::data_bucket::storage::{
    CatalogMutation, CatalogName, CatalogRecord, CommittedGeneration, GenerationPlan, Head, MutationKind, ObjectRef,
    PageAddress, PageKind, PageRef, PageStore, StagedGeneration, StagedPage, StorageDomainId, SystemTableRecord,
    TableId,
};
use worktable::data_bucket::{PAGE_SIZE, PageId, SpaceId};

fn data_page_image(address: PageAddress, rows: u32, live_bytes: u32, fill: u8) -> Vec<u8> {
    use worktable::data_bucket::{DataPage, GeneralHeader, INNER_PAGE_SIZE, Link, PageType};

    let mut page = DataPage::<INNER_PAGE_SIZE>::new();
    let base = live_bytes / rows;
    let mut offset = 0;
    for index in 0..rows {
        let length = if index + 1 == rows { live_bytes - offset } else { base };
        page.update_at(
            Link {
                page_id: address.page_id,
                offset,
                length,
            },
            &vec![fill; length as usize],
        )
        .unwrap();
        offset += length;
    }
    let mut header = GeneralHeader::new(address.page_id, PageType::Data, address.space_id);
    header.data_length = page.length;
    let mut image = worktable::prelude::rkyv::to_bytes::<worktable::prelude::rkyv::rancor::Error>(&header)
        .unwrap()
        .to_vec();
    image.extend_from_slice(&page.encode(INNER_PAGE_SIZE).unwrap());
    assert_eq!(image.len(), PAGE_SIZE);
    image
}

#[derive(Clone, Debug)]
struct MemoryError;

impl Display for MemoryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("in-memory page store rejected the generation")
    }
}

impl std::error::Error for MemoryError {}

#[derive(Default)]
struct State {
    next_object: u64,
    head: Option<Head>,
    catalogs: BTreeMap<[u8; 32], Vec<u8>>,
    pages: BTreeMap<[u8; 32], Vec<u8>>,
}

#[derive(Clone, Default)]
struct MemoryStore(Arc<Mutex<State>>);

impl MemoryStore {
    fn store_object(state: &mut State, bytes: Vec<u8>) -> ObjectRef {
        state.next_object += 1;
        let mut object = [0; 32];
        object[..8].copy_from_slice(&state.next_object.to_le_bytes());
        let length = bytes.len() as u32;
        state.pages.insert(object, bytes);
        ObjectRef {
            object,
            offset: 0,
            encoded_length: length,
            decoded_length: length,
            checksum: object,
        }
    }
}

impl PageStore for MemoryStore {
    type Error = MemoryError;

    fn load_head(&self, _domain: StorageDomainId) -> Result<Option<Head>, Self::Error> {
        Ok(self.0.lock().unwrap().head.clone())
    }

    fn load_catalog(&self, head: &Head) -> Result<Vec<u8>, Self::Error> {
        self.0
            .lock()
            .unwrap()
            .catalogs
            .get(&head.catalog.object)
            .cloned()
            .ok_or(MemoryError)
    }

    fn read_page(&self, page: &PageRef) -> Result<Vec<u8>, Self::Error> {
        self.0
            .lock()
            .unwrap()
            .pages
            .get(&page.object.object)
            .cloned()
            .ok_or(MemoryError)
    }

    fn stage(&self, plan: &GenerationPlan) -> Result<StagedGeneration, Self::Error> {
        let mut state = self.0.lock().unwrap();
        let mut pages = Vec::new();
        for mutation in &plan.pages {
            if let MutationKind::Put {
                image,
                live_rows,
                live_bytes,
            } = &mutation.kind
            {
                pages.push(StagedPage {
                    address: mutation.address,
                    object: Self::store_object(&mut state, image.clone()),
                    live_rows: *live_rows,
                    live_bytes: *live_bytes,
                });
            }
        }
        Ok(StagedGeneration {
            domain: plan.domain,
            generation: plan.id,
            parent: plan.parent,
            writer_epoch: plan.writer_epoch,
            pages,
            catalog: None,
        })
    }

    fn stage_catalog(&self, staged: &mut StagedGeneration, checkpoint: &[u8]) -> Result<(), Self::Error> {
        let mut state = self.0.lock().unwrap();
        let object = Self::store_object(&mut state, checkpoint.to_vec());
        state.catalogs.insert(object.object, checkpoint.to_vec());
        staged.catalog = Some(object);
        Ok(())
    }

    fn commit(&self, staged: StagedGeneration) -> Result<CommittedGeneration, Self::Error> {
        let mut state = self.0.lock().unwrap();
        if state.head.as_ref().map_or(0, |head| head.generation) != staged.parent {
            return Err(MemoryError);
        }
        let head = Head {
            domain: staged.domain,
            generation: staged.generation,
            parent: staged.parent,
            writer_epoch: staged.writer_epoch,
            catalog: staged.catalog.ok_or(MemoryError)?,
        };
        state.head = Some(head.clone());
        Ok(CommittedGeneration { head })
    }
}

#[test]
fn generated_catalog_commits_pages_and_restores_the_database() {
    let id = StorageDomainId([7; 16]);
    let store = MemoryStore::default();
    let database = Database::new(id, 11, store.clone());
    let table_id = database.register_table("orders", 3).unwrap();
    assert_eq!(table_id, TableId(1));

    let address = PageAddress {
        domain: id,
        table_id,
        space_id: SpaceId(2),
        page_id: PageId::from(4),
        page_kind: PageKind::Data,
    };
    let image = data_page_image(address, 9, 777, 0x5a);
    let mut generation = database.begin_generation().unwrap();
    generation.put_page(address, image.clone(), 9, 777);
    database.commit_generation(generation.finish()).unwrap();

    let catalog = database.catalog();
    let tables = catalog.system_tables();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name.as_str(), "orders");
    assert_eq!(tables[0].row_count, 9);
    assert_eq!(tables[0].live_row_bytes, 777);
    assert_eq!(tables[0].live_data_pages, 1);
    assert_eq!(database.read_page(address).unwrap(), Some(image.clone()));

    drop(database);
    let reopened = Database::open(id, 12, store).unwrap();
    assert_eq!(reopened.catalog().system_pages().len(), 1);
    assert_eq!(reopened.read_page(address).unwrap(), Some(image));

    let table = SystemTableRecord {
        table_id,
        name: CatalogName::new("orders").unwrap(),
        schema_version: 4,
        data_space_id: SpaceId(2),
        page_stride: PAGE_SIZE as u32,
        row_count: 9,
        live_row_bytes: 777,
        allocated_data_pages: 1,
        live_data_pages: 1,
        primary_index_entries: 0,
        secondary_index_entries: 0,
        tombstones: 0,
        applied_generation: 2,
        durable_generation: 2,
    };
    let mut generation = reopened.begin_generation().unwrap();
    generation.update_catalog(CatalogMutation::Upsert(CatalogRecord::Table(table)));
    reopened.commit_generation(generation.finish()).unwrap();
    assert_eq!(reopened.catalog().system_tables()[0].schema_version, 4);
}

#[test]
fn registering_an_incompatible_existing_table_does_not_mutate_its_metadata() {
    let id = StorageDomainId([8; 16]);
    let database = Database::new(id, 11, MemoryStore::default());
    let table_id = database
        .register_table_with_stride("orders", 3, PAGE_SIZE as u32)
        .unwrap();
    let generation = database.generation();

    assert!(
        database
            .register_table_with_stride("orders", 4, (PAGE_SIZE / 2) as u32)
            .is_err()
    );

    assert_eq!(database.generation(), generation);
    let table = database.catalog().system_tables().pop().unwrap();
    assert_eq!(table.table_id, table_id);
    assert_eq!(table.schema_version, 3);
    assert_eq!(table.page_stride, PAGE_SIZE as u32);
}
