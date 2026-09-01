use indexset::core::multipair::MultiPair;
use indexset::core::pair::Pair;

/// Rebuild a multimap entry from a persisted pair.
///
/// A `MultiPair` is identified by its `(key, value)` pair, both of which the snapshot
/// stores, so there is nothing to recreate beyond moving them across. The discriminator
/// these methods used to take existed only for the random representation, which could not
/// locate an entry by its value and had to invent an identity instead.
pub trait MultiPairRecreate<T, L> {
    fn recreate(self) -> MultiPair<T, L>;
}

impl<T, L> MultiPairRecreate<T, L> for Pair<T, L> {
    fn recreate(self) -> MultiPair<T, L> {
        MultiPair {
            key: self.key,
            value: self.value,
        }
    }
}
