use std::collections::HashMap;

use data_bucket::Link;

use crate::persistence::Operation;
use crate::Difference;
use crate::WorkTableError;

pub trait TableSecondaryIndex<Row, AvailableTypes>
where
    AvailableTypes: 'static,
{
    fn save_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn process_difference(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), WorkTableError>;
}

pub trait TableSecondaryIndexCdc<Row, SecondaryEvents> {
    fn save_row_cdc(&self, row: Row, link: Link) -> Result<SecondaryEvents, WorkTableError>;
}

impl<Row, AvailableTypes> TableSecondaryIndex<Row, AvailableTypes> for ()
where
    AvailableTypes: 'static,
{
    fn save_row(&self, _: Row, _: Link) -> Result<(), WorkTableError> {
        Ok(())
    }

    fn delete_row(&self, _: Row, _: Link) -> Result<(), WorkTableError> {
        Ok(())
    }

    fn process_difference(
        &self,
        _: Link,
        _: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), WorkTableError> {
        Ok(())
    }
}
