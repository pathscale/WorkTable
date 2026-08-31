/// Struct for storing data in a vector with stable indexes and slot reuse.
/// Slots are `Option<T>`: `remove` is `Option::take`, so the value moves out
/// without a `Clone` bound and the slot is freed immediately. The previous
/// representation cloned the value out and left the original in the slot
/// until reuse, doubling resident memory for every pending element.
/// If the `empty` vector is empty, then the data vector is extended.
/// If the `empty` vector is not empty, then an index from the empty vector is
/// reused to insert the data.
#[derive(Debug)]
pub struct OptimizedVec<T> {
    /// Slots of data; `None` marks a free slot awaiting reuse.
    data: Vec<Option<T>>,
    /// Vector of empty indexes.
    empty: Vec<usize>,
    /// Number of occupied slots.
    length: usize,
}

impl<T> Default for OptimizedVec<T> {
    fn default() -> Self {
        OptimizedVec {
            data: Vec::new(),
            empty: Vec::new(),
            length: 0,
        }
    }
}

impl<T> OptimizedVec<T> {
    pub fn with_capacity(cap: usize) -> Self {
        OptimizedVec {
            data: Vec::with_capacity(cap),
            empty: Vec::with_capacity(cap),
            length: 0,
        }
    }

    /// Pushes a value to the vector, reusing a free slot when one exists.
    /// # Arguments
    /// * `value` - Value to push
    /// # Returns
    /// * `usize` - Index of the pushed value
    pub fn push(&mut self, value: T) -> usize {
        let index = if let Some(index) = self.empty.pop() {
            debug_assert!(self.data[index].is_none(), "free list pointed at an occupied slot");
            self.data[index] = Some(value);
            index
        } else {
            self.data.push(Some(value));
            self.data.len() - 1
        };

        self.length += 1;

        index
    }

    /// Gets a value from the vector.
    /// # Arguments
    /// * `index` - Index of the value to get
    /// # Returns
    /// * `Option<&T>` - Value at the index,
    ///   or `None` if the index is out of bounds or the slot is empty.
    #[allow(dead_code)]
    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index).and_then(Option::as_ref)
    }

    /// Gets a mutable value from the vector.
    /// # Arguments
    /// * `index` - Index of the value to get
    /// # Returns
    /// * `Option<&mut T>` - Mutable value at the index,
    ///   or `None` if the index is out of bounds or the slot is empty.
    #[allow(dead_code)]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.data.get_mut(index).and_then(Option::as_mut)
    }

    /// Removes a value from the vector, moving it out of its slot and freeing
    /// the slot for reuse.
    /// # Arguments
    /// * `index` - Index of the value to remove.
    /// # Returns
    /// * `Option<T>` - Value at the index,
    ///   or `None` if the index is out of bounds or the slot is empty.
    pub fn remove(&mut self, index: usize) -> Option<T> {
        let value = self.data.get_mut(index)?.take()?;
        self.empty.push(index);
        self.length -= 1;

        Some(value)
    }

    /// Iterates over the occupied slots.
    /// # Returns
    /// * `impl Iterator<Item = &T>` - Occupied values in index order.
    #[allow(dead_code)]
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.data.iter().filter_map(Option::as_ref)
    }

    /// Gets the length of the vector.
    /// # Returns
    /// * `usize` - Length of the vector.
    #[allow(dead_code)]
    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns true of [`OptimizedVec`] is empty.
    /// # Returns
    /// * `bool` - State of emptiness.
    #[allow(dead_code)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::OptimizedVec;

    #[test]
    fn test_optimized_vec_new() {
        let vec = OptimizedVec::<i32>::default();

        assert_eq!(vec.data.len(), 0);
        assert_eq!(vec.empty.len(), 0);

        assert_eq!(vec.length, 0);
    }

    #[test]
    fn test_optimized_vec_push() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(index, 0);
        assert_eq!(vec.data.len(), 1);
        assert_eq!(vec.empty.len(), 0);
        assert_eq!(vec.length, 1);
    }

    #[test]
    fn test_optimized_vec_get() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(vec.get(index), Some(&1));
        assert_eq!(vec.get(index + 1), None);
    }

    #[test]
    fn test_optimized_vec_get_mut() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(vec.get_mut(index), Some(&mut 1));
        assert_eq!(vec.get_mut(index + 1), None);
    }

    #[test]
    fn test_optimized_vec_remove() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(vec.remove(index), Some(1));
        assert_eq!(vec.remove(index + 1), None);
        assert_eq!(vec.data.len(), 1);
        assert_eq!(vec.empty.len(), 1);
        assert_eq!(vec.empty[0], index);
        assert_eq!(vec.length, 0);
    }

    #[test]
    fn test_optimized_vec_push_remove() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(index, 0);
        assert_eq!(vec.data.len(), 1);
        assert_eq!(vec.empty.len(), 0);
        assert_eq!(vec.length, 1);

        assert_eq!(vec.remove(index), Some(1));

        let index = vec.push(2);

        assert_eq!(index, 0);
        assert_eq!(vec.data.len(), 1);
        assert_eq!(vec.empty.len(), 0);
        assert_eq!(vec.length, 1);
    }

    /// The old representation cloned the value out of the slot, so `remove`
    /// demanded `T: Clone` and the slot kept the original alive until reuse.
    /// A non-cloneable element type proves the bound is gone, and the drop
    /// count proves the removed value is the only remaining owner.
    #[test]
    fn test_optimized_vec_remove_moves_without_clone() {
        use std::rc::Rc;

        struct NotClone(#[allow(dead_code)] Rc<()>);

        let witness = Rc::new(());
        let mut vec = OptimizedVec::<NotClone>::default();
        let index = vec.push(NotClone(witness.clone()));
        assert_eq!(Rc::strong_count(&witness), 2);

        let Some(removed) = vec.remove(index) else {
            panic!("occupied slot must yield its value");
        };
        assert_eq!(
            Rc::strong_count(&witness),
            2,
            "removal must move the value, not duplicate it"
        );

        drop(removed);
        assert_eq!(
            Rc::strong_count(&witness),
            1,
            "the freed slot must not keep the removed value alive"
        );

        assert!(vec.get(index).is_none());
        assert!(vec.remove(index).is_none(), "a freed slot removes as None");
    }

    #[test]
    fn test_optimized_vec_slot_is_freed_immediately_and_reused() {
        let mut vec = OptimizedVec::<String>::default();
        let a = vec.push("a".to_owned());
        let b = vec.push("b".to_owned());

        assert_eq!(vec.remove(a), Some("a".to_owned()));
        assert!(vec.data[a].is_none(), "removed slot must not retain the value");
        assert_eq!(vec.get(b).map(String::as_str), Some("b"));

        let c = vec.push("c".to_owned());
        assert_eq!(c, a, "push must reuse the freed slot");
        assert_eq!(vec.data.len(), 2);
        assert_eq!(vec.len(), 2);
        assert_eq!(vec.iter().count(), 2);
    }
}
