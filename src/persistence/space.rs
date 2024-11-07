//! [`Space`] type declaration.

use crate::persistence::page;

/// [`Space`] represents whole [`WorkTable`] file.
#[derive(Debug, Default)]
pub struct Space {
    pub pages: Vec<page::General>
}