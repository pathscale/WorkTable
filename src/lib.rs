#![feature(core_intrinsics)]

pub mod in_memory;
mod row;
mod table;
mod index;
mod primary_key;

// mod ty;
// mod value;
//
// pub use column::*;
// pub use field::*;
pub use index::*;
pub use row::*;
pub use table::*;

pub use worktable_codegen::worktable;

pub mod prelude {
    pub use crate::{TableIndex, WorkTable, in_memory::page::Link, TableRow};
}