mod data;
mod index;

pub use data::{SpaceData, SpaceDataOps};
pub use index::{IndexTableOfContents, SpaceIndex};
use std::fs::{File, OpenOptions};
use std::path::Path;

pub fn open_or_create_file<S: AsRef<str>>(path: S) -> eyre::Result<File> {
    let path = Path::new(path.as_ref());
    Ok(OpenOptions::new()
        .write(true)
        .read(true)
        .create(!path.exists())
        .open(path)?)
}
