use std::sync::atomic::{
    AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering,
};

pub trait TablePrimaryKey {
    type Generator;
}

pub trait PrimaryKeyGenerator<T> {
    fn next(&self) -> T;
}

pub trait PrimaryKeyGeneratorState {
    type State;

    fn get_state(&self) -> Self::State;

    fn from_state(state: Self::State) -> Self;
}

macro_rules! atomic_primary_key {
    ($ty:ident, $atomic_ty:ident) => {
        impl<T> PrimaryKeyGenerator<T> for $atomic_ty
        where
            T: From<$ty>,
        {
            fn next(&self) -> T {
                let previous = self.fetch_add(1, Ordering::AcqRel);
                // A wrapped counter would silently hand out duplicate primary
                // keys; exhausting the key space must be loud instead.
                assert!(
                    previous != <$ty>::MAX,
                    "autoincrement primary key space exhausted: {} overflowed",
                    stringify!($ty),
                );
                previous.into()
            }
        }

        impl PrimaryKeyGeneratorState for $atomic_ty {
            type State = $ty;

            fn get_state(&self) -> Self::State {
                self.load(Ordering::Acquire)
            }

            fn from_state(state: Self::State) -> Self {
                $atomic_ty::from(state)
            }
        }
    };
}

atomic_primary_key!(u8, AtomicU8);
atomic_primary_key!(u16, AtomicU16);
atomic_primary_key!(u32, AtomicU32);
atomic_primary_key!(u64, AtomicU64);

atomic_primary_key!(i8, AtomicI8);
atomic_primary_key!(i16, AtomicI16);
atomic_primary_key!(i32, AtomicI32);
atomic_primary_key!(i64, AtomicI64);

impl PrimaryKeyGeneratorState for () {
    type State = ();

    fn get_state(&self) -> Self::State {}

    fn from_state((): Self::State) -> Self {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_counts_up_from_its_state() {
        let generator = AtomicU8::from_state(250);
        let next: u8 = generator.next();
        assert_eq!(next, 250);
        assert_eq!(generator.get_state(), 251);
    }

    #[test]
    #[should_panic(expected = "primary key space exhausted")]
    fn an_exhausted_generator_panics_rather_than_wrapping() {
        let generator = AtomicU8::from_state(u8::MAX);
        // Wrapping here would silently restart at 0 and hand out duplicate
        // primary keys; the contract is a loud failure instead.
        let _: u8 = generator.next();
    }
}
