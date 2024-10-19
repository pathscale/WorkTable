use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::prelude::PageLink;
use crate::represent::page::PageId;
use derive_more::{Display, Error};
use innodb::innodb::page::{FIL_HEADER_SIZE, FIL_PAGE_SIZE};

#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::{
    with::{Skip, Unsafe},
    AlignedBytes, Archive, Deserialize, Serialize,
};

pub const PAGE_BODY_SIZE: usize = FIL_PAGE_SIZE - FIL_HEADER_SIZE;
#[derive(Archive, Deserialize, Debug, Serialize)]
pub struct DataPage<Row, const SIZE: usize = PAGE_BODY_SIZE> {
    /// [`PageId`] of the [`General`] page of this [`DataPage`].
    ///
    /// [`Id]: page::Id
    /// [`General`]: page::General
    #[with(Skip)]
    id: PageId,

    /// Offset to the first free byte on this [`DataPage`] page.
    free_offset: AtomicU32,

    /// Inner array of bytes where deserialized `Row`s will be stored.
    #[with(Unsafe)]
    inner_data: UnsafeCell<AlignedBytes<SIZE>>,

    /// `Row` phantom data.
    _phantom: PhantomData<Row>,
}

unsafe impl<Row, const DATA_LENGTH: usize> Sync for DataPage<Row, DATA_LENGTH> {}

impl<Row, const DATA_LENGTH: usize> DataPage<Row, DATA_LENGTH> {
    /// Creates new [`DataPage`] page.
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            free_offset: AtomicU32::default(),
            inner_data: UnsafeCell::default(),
            _phantom: PhantomData,
        }
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataRow")
    )]
    pub fn save_row<const N: usize>(&self, row: &Row) -> Result<PageLink, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
    {
        let bytes = rkyv::to_bytes(row).map_err(|_| ExecutionError::SerializeError)?;
        let length = bytes.len() as u32;
        let offset = self.free_offset.fetch_add(length, Ordering::SeqCst);
        if offset > DATA_LENGTH as u32 - length {
            return Err(ExecutionError::PageIsFull {
                need: length,
                left: DATA_LENGTH as i64 - offset as i64,
            });
        }

        let inner_data = unsafe { &mut *self.inner_data.get() };
        inner_data[offset as usize..][..length as usize].copy_from_slice(bytes.as_slice());

        let link = PageLink {
            page_id: self.id,
            offset,
            length,
        };

        Ok(link)
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataRow")
    )]
    pub unsafe fn save_row_by_link<const N: usize>(
        &self,
        row: &Row,
        link: PageLink,
    ) -> Result<PageLink, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
    {
        let bytes = rkyv::to_bytes(row).map_err(|_| ExecutionError::SerializeError)?;
        let length = bytes.len() as u32;
        if length != link.length {
            return Err(ExecutionError::InvalidLink);
        }

        let inner_data = unsafe { &mut *self.inner_data.get() };
        inner_data[link.offset as usize..][..link.length as usize]
            .copy_from_slice(bytes.as_slice());

        Ok(link)
    }

    pub unsafe fn get_mut_row_ref(
        &self,
        link: PageLink,
    ) -> Result<Pin<&mut <Row as Archive>::Archived>, ExecutionError>
    where
        Row: Archive,
    {
        if link.offset > self.free_offset.load(Ordering::Relaxed) {
            return Err(ExecutionError::DeserializeError);
        }

        let inner_data = unsafe { &mut *self.inner_data.get() };
        let bytes = &mut inner_data[link.offset as usize..(link.offset + link.length) as usize];
        Ok(unsafe { rkyv::archived_root_mut::<Row>(Pin::new(&mut bytes[..])) })
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataRow")
    )]
    pub fn get_row_ref(&self, link: PageLink) -> Result<&<Row as Archive>::Archived, ExecutionError>
    where
        Row: Archive,
    {
        if link.offset > self.free_offset.load(Ordering::Relaxed) {
            return Err(ExecutionError::DeserializeError);
        }

        let inner_data = unsafe { &*self.inner_data.get() };
        let bytes = &inner_data[link.offset as usize..(link.offset + link.length) as usize];
        Ok(unsafe { rkyv::archived_root::<Row>(&bytes[..]) })
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataRow")
    )]
    pub fn get_row(&self, link: PageLink) -> Result<Row, ExecutionError>
    where
        Row: Archive,
        <Row as Archive>::Archived: Deserialize<Row, rkyv::de::deserializers::SharedDeserializeMap>,
    {
        let archived = self.get_row_ref(link)?;
        let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
        archived
            .deserialize(&mut map)
            .map_err(|_| ExecutionError::DeserializeError)
    }
}

/// Error that can appear on [`DataPage`] page operations.
#[derive(Copy, Clone, Debug, Display, Error)]
pub enum ExecutionError {
    /// Error of trying to save row in [`DataPage`] page with not enough space left.
    #[display("need {}, but {} left", need, left)]
    PageIsFull { need: u32, left: i64 },

    /// Error of saving `Row` in [`DataPage`] page.
    SerializeError,

    /// Error of loading `Row` from [`DataPage`] page.
    DeserializeError,

    /// Link provided for saving `Row` is invalid.
    InvalidLink,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::{mpsc, Arc};
    use std::thread;

    use rkyv::{Archive, Deserialize, Serialize};

    use crate::represent::page::data::{DataPage, PAGE_BODY_SIZE};

    #[derive(
        Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    #[archive(compare(PartialEq))]
    #[archive_attr(derive(Debug))]
    struct TestRow {
        a: u64,
        b: u64,
    }

    #[test]
    fn data_page_length_valid() {
        let data = DataPage::<()>::new(1.into());
        let bytes = rkyv::to_bytes::<_, 4096>(&data).unwrap();

        assert_eq!(bytes.len(), PAGE_BODY_SIZE)
    }

    #[test]
    fn data_page_save_row() {
        let page = DataPage::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<16>(&row).unwrap();
        assert_eq!(link.page_id, page.id);
        assert_eq!(link.length, 16);
        assert_eq!(link.offset, 0);

        assert_eq!(page.free_offset.load(Ordering::Relaxed), link.length);

        let inner_data = unsafe { &mut *page.inner_data.get() };
        let bytes = &inner_data[link.offset as usize..link.length as usize];
        let archived = unsafe { rkyv::archived_root::<TestRow>(bytes) };
        assert_eq!(archived, &row)
    }

    #[test]
    fn data_page_overwrite_row() {
        let page = DataPage::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<16>(&row).unwrap();

        let new_row = TestRow { a: 20, b: 20 };
        let res = unsafe { page.save_row_by_link::<16>(&new_row, link) }.unwrap();

        assert_eq!(res, link);

        let inner_data = unsafe { &mut *page.inner_data.get() };
        let bytes = &inner_data[link.offset as usize..link.length as usize];
        let archived = unsafe { rkyv::archived_root::<TestRow>(bytes) };
        assert_eq!(archived, &new_row)
    }

    #[test]
    fn data_page_full() {
        let page = DataPage::<TestRow, 16>::new(1.into());
        let row = TestRow { a: 10, b: 20 };
        let _ = page.save_row::<16>(&row).unwrap();

        let new_row = TestRow { a: 20, b: 20 };
        let res = page.save_row::<16>(&new_row);

        assert!(res.is_err());
    }

    #[test]
    fn data_page_full_multithread() {
        let page = DataPage::<TestRow, 128>::new(1.into());
        let shared = Arc::new(page);

        let (tx, rx) = mpsc::channel();
        let second_shared = shared.clone();

        thread::spawn(move || {
            let mut links = Vec::new();
            for i in 1..10 {
                let row = TestRow {
                    a: 10 + i,
                    b: 20 + i,
                };

                let link = second_shared.save_row::<16>(&row);
                links.push(link)
            }

            tx.send(links).unwrap();
        });

        let mut links = Vec::new();
        for i in 1..10 {
            let row = TestRow {
                a: 30 + i,
                b: 40 + i,
            };

            let link = shared.save_row::<16>(&row);
            links.push(link)
        }
        let other_links = rx.recv().unwrap();

        print!("{:?}", other_links);
        print!("{:?}", links);
    }

    #[test]
    fn data_page_save_many_rows() {
        let page = DataPage::<TestRow>::new(1.into());

        let mut rows = Vec::new();
        let mut links = Vec::new();
        for i in 1..10 {
            let row = TestRow {
                a: 10 + i,
                b: 20 + i,
            };
            rows.push(row);

            let link = page.save_row::<16>(&row);
            links.push(link)
        }

        let inner_data = unsafe { &mut *page.inner_data.get() };

        for (i, link) in links.into_iter().enumerate() {
            let link = link.unwrap();

            let bytes = &inner_data[link.offset as usize..(link.offset + link.length) as usize];
            let archived = unsafe { rkyv::archived_root::<TestRow>(bytes) };
            let row = rows.get(i).unwrap();

            assert_eq!(row, archived)
        }
    }

    #[test]
    fn data_page_get_row_ref() {
        let page = DataPage::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<16>(&row).unwrap();
        let archived = page.get_row_ref(link).unwrap();
        assert_eq!(archived, &row)
    }

    #[test]
    fn data_page_get_row() {
        let page = DataPage::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<16>(&row).unwrap();
        let deserialized = page.get_row(link).unwrap();
        assert_eq!(deserialized, row)
    }

    #[test]
    fn multithread() {
        let page = DataPage::<TestRow>::new(1.into());
        let shared = Arc::new(page);

        let (tx, rx) = mpsc::channel();
        let second_shared = shared.clone();

        thread::spawn(move || {
            let mut links = Vec::new();
            for i in 1..10 {
                let row = TestRow {
                    a: 10 + i,
                    b: 20 + i,
                };

                let link = second_shared.save_row::<16>(&row);
                links.push(link)
            }

            tx.send(links).unwrap();
        });

        let mut links = Vec::new();
        for i in 1..10 {
            let row = TestRow {
                a: 30 + i,
                b: 40 + i,
            };

            let link = shared.save_row::<16>(&row);
            links.push(link)
        }
        let other_links = rx.recv().unwrap();

        let links = other_links
            .into_iter()
            .chain(links.into_iter())
            .map(|v| v.unwrap())
            .collect::<Vec<_>>();

        for link in links {
            let _ = shared.get_row(link).unwrap();
        }
    }
}
