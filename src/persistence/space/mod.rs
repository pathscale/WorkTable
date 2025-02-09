mod data;
mod index;

pub use data::SpaceData;
use data_bucket::Link;
pub use index::{IndexTableOfContents, SpaceIndex};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use std::fs::{File, OpenOptions};
use std::path::Path;

pub trait SpaceDataOps {
    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()>;
}

pub trait SpaceIndexOps<T> {
    fn process_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()>;
}

pub trait SpaceSecondaryIndexOps<SecondaryIndexEvents> {
    fn process_change_events(&mut self, events: SecondaryIndexEvents) -> eyre::Result<()>;
}

pub fn open_or_create_file<S: AsRef<str>>(path: S) -> eyre::Result<File> {
    let path = Path::new(path.as_ref());
    Ok(OpenOptions::new()
        .write(true)
        .read(true)
        .create(!path.exists())
        .open(path)?)
}
