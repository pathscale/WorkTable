use std::collections::HashMap;

use data_bucket::Link;

use crate::{Difference, IndexError, TableSecondaryIndex};

pub trait TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes> {
    fn save_row_cdc(
        &self,
        row: Row,
        link: Link,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn reinsert_row_cdc(
        &self,
        row_old: Row,
        link_old: Link,
        row_new: Row,
        link_new: Link,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn delete_row_cdc(
        &self,
        row: Row,
        link: Link,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn process_difference_insert_cdc(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn process_difference_remove_cdc(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
}

impl<T, Row, AvailableTypes, AvailableIndexes>
    TableSecondaryIndexCdc<Row, AvailableTypes, (), AvailableIndexes> for T
where
    T: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
{
    fn save_row_cdc(&self, row: Row, link: Link) -> Result<(), IndexError<AvailableIndexes>> {
        self.save_row(row, link)
    }

    fn reinsert_row_cdc(
        &self,
        row_old: Row,
        link_old: Link,
        row_new: Row,
        link_new: Link,
    ) -> Result<(), IndexError<AvailableIndexes>> {
        self.reinsert_row(row_old, link_old, row_new, link_new)
    }

    fn delete_row_cdc(&self, row: Row, link: Link) -> Result<(), IndexError<AvailableIndexes>> {
        self.delete_row(row, link)
    }

    fn process_difference_insert_cdc(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), IndexError<AvailableIndexes>> {
        self.process_difference_insert(link, differences)
    }

    fn process_difference_remove_cdc(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), IndexError<AvailableIndexes>> {
        self.process_difference_remove(link, differences)
    }
}
