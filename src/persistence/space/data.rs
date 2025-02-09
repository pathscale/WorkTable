use crate::persistence::space::open_or_create_file;
use data_bucket::{update_at, Link};
use std::fs::{File, OpenOptions};
use std::path::Path;

pub trait SpaceDataOps {
    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()>;
}

#[derive(Debug)]
pub struct SpaceData<const DATA_LENGTH: usize> {
    pub data_file: File,
}

impl<const DATA_LENGTH: usize> SpaceData<DATA_LENGTH> {
    pub fn from_path(path: String) -> eyre::Result<Self> {
        let data_file = open_or_create_file(path)?;
        Ok(Self { data_file })
    }

    pub fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()> {
        update_at::<{ DATA_LENGTH }>(&mut self.data_file, link, bytes)
    }
}
