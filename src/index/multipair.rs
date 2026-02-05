use indexset::core::multipair::MultiPair;
use indexset::core::pair::Pair;

pub trait MultiPairRecreate<T, L> {
    fn with_last_discriminator(self, discriminator: u64) -> MultiPair<T, L>;
}

impl<T, L> MultiPairRecreate<T, L> for Pair<T, L> {
    fn with_last_discriminator(self, discriminator: u64) -> MultiPair<T, L> {
        MultiPair {
            key: self.key,
            value: self.value,
            discriminator: fastrand::u64(discriminator..),
        }
    }
}
