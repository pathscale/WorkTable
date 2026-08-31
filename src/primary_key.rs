use std::sync::atomic::{
    AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering,
};

pub trait TablePrimaryKey {
    type Generator;
}

pub trait PrimaryKeyGenerator<T> {
    fn next(&self) -> T;
}

/// Range reservation for autoincrement generators.
///
/// `Raw` is the raw column type behind the generated primary-key newtype, so
/// the returned range can be iterated directly when assigning keys to a batch
/// of rows before `insert_many`.
pub trait PrimaryKeyGeneratorRange<Raw> {
    /// Atomically reserves `count` consecutive keys and returns them as a
    /// half-open range.
    ///
    /// Concurrent `reserve` and [`PrimaryKeyGenerator::next`] calls never
    /// observe overlapping keys.
    fn reserve(&self, count: usize) -> std::ops::Range<Raw>;
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

        impl PrimaryKeyGeneratorRange<$ty> for $atomic_ty {
            fn reserve(&self, count: usize) -> std::ops::Range<$ty> {
                let count = <$ty>::try_from(count).unwrap_or_else(|_| {
                    panic!(
                        "autoincrement primary key space exhausted: cannot reserve {count} {} keys",
                        stringify!($ty),
                    )
                });
                let mut current = self.load(Ordering::Acquire);
                loop {
                    // A wrapped counter would silently hand out duplicate
                    // primary keys; exhausting the key space must be loud
                    // instead, mirroring `next`.
                    let end = current.checked_add(count).unwrap_or_else(|| {
                        panic!(
                            "autoincrement primary key space exhausted: {} overflowed",
                            stringify!($ty),
                        )
                    });
                    match self.compare_exchange_weak(current, end, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => return current..end,
                        Err(actual) => current = actual,
                    }
                }
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
    fn reserve_returns_consecutive_keys_and_advances_the_state() {
        let generator = AtomicU64::from_state(10);
        let range = generator.reserve(5);
        assert_eq!(range, 10..15);
        assert_eq!(generator.get_state(), 15);
        let next: u64 = generator.next();
        assert_eq!(next, 15, "next must continue after the reserved range");
    }

    #[test]
    fn reserve_of_zero_keys_is_an_empty_range() {
        let generator = AtomicU64::from_state(7);
        let range = generator.reserve(0);
        assert!(range.is_empty());
        assert_eq!(generator.get_state(), 7);
    }

    #[test]
    fn concurrent_reservations_never_overlap() {
        use std::sync::Arc;

        let generator = Arc::new(AtomicU64::from_state(0));
        let mut handles = vec![];
        for _ in 0..8 {
            let generator = generator.clone();
            handles.push(std::thread::spawn(move || {
                (0..100).map(|_| generator.reserve(3)).collect::<Vec<_>>()
            }));
        }
        let mut ranges = vec![];
        for handle in handles {
            ranges.extend(handle.join().unwrap());
        }
        ranges.sort_by_key(|range| range.start);
        for pair in ranges.windows(2) {
            assert!(
                pair[0].end <= pair[1].start,
                "ranges {:?} and {:?} overlap",
                pair[0],
                pair[1]
            );
        }
        assert_eq!(generator.get_state(), 8 * 100 * 3);
    }

    #[test]
    #[should_panic(expected = "primary key space exhausted")]
    fn an_overflowing_reservation_panics_rather_than_wrapping() {
        let generator = AtomicU8::from_state(250);
        let _ = generator.reserve(10);
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
