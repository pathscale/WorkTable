use crate::{IndexType, TableIndex};
use data_bucket::Link;
use std::ops::RangeBounds;

pub trait SpaceIndex {}

pub trait SpaceData {
    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()>;
}
