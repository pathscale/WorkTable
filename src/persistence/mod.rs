use crate::persistence::operation::Operation;

mod engine;
mod manager;
mod operation;
mod space;

pub use space::{
    IndexTableOfContents, SpaceData, SpaceDataOps, SpaceIndex, SpaceIndexOps,
    SpaceSecondaryIndexOps,
};
