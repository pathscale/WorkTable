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
                self.fetch_add(1, Ordering::AcqRel).into()
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
