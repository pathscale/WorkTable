use crate::IndexMap;
use crate::IndexMultiMap;
use data_bucket::Link;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

pub trait MemStat {
    fn heap_size(&self) -> usize;
    fn used_size(&self) -> usize;
}

impl<T: MemStat> MemStat for Option<T> {
    fn heap_size(&self) -> usize {
        self.as_ref().map_or(0, |v| v.heap_size())
    }
    fn used_size(&self) -> usize {
        self.as_ref().map_or(0, |v| v.used_size())
    }
}

impl<T: MemStat> MemStat for Vec<T> {
    fn heap_size(&self) -> usize {
        self.capacity() * std::mem::size_of::<T>()
            + self.iter().map(|v| v.heap_size()).sum::<usize>()
    }
    fn used_size(&self) -> usize {
        self.len() * std::mem::size_of::<T>() + self.iter().map(|v| v.used_size()).sum::<usize>()
    }
}

impl MemStat for String {
    fn heap_size(&self) -> usize {
        self.capacity()
    }
    fn used_size(&self) -> usize {
        self.len()
    }
}

impl<K, V> MemStat for IndexMap<K, V>
where
    K: Ord + Clone + 'static + MemStat + Send,
    V: Clone + 'static + MemStat + Send,
{
    fn heap_size(&self) -> usize {
        let slot_size = std::mem::size_of::<indexset::core::pair::Pair<K, V>>();
        let base_heap = self.capacity() * slot_size;

        let kv_heap: usize = self
            .iter()
            .map(|(k, v)| k.heap_size() + v.heap_size())
            .sum();

        base_heap + kv_heap
    }

    fn used_size(&self) -> usize {
        let pair_size = std::mem::size_of::<indexset::core::pair::Pair<K, V>>();
        let base = self.len() * pair_size;

        let used: usize = self
            .iter()
            .map(|(k, v)| k.used_size() + v.used_size())
            .sum();

        base + used
    }
}

impl<K, V> MemStat for IndexMultiMap<K, V>
where
    K: Ord + Clone + 'static + MemStat + Send,
    V: Ord + Clone + 'static + MemStat + Send,
{
    fn heap_size(&self) -> usize {
        let slot_size = std::mem::size_of::<indexset::core::multipair::MultiPair<K, V>>();
        let base_heap = self.capacity() * slot_size;

        let kv_heap: usize = self
            .iter()
            .map(|(k, v)| k.heap_size() + v.heap_size())
            .sum();

        base_heap + kv_heap
    }

    fn used_size(&self) -> usize {
        let pair_size = std::mem::size_of::<indexset::core::multipair::MultiPair<K, V>>();
        let base = self.len() * pair_size;

        let used: usize = self
            .iter()
            .map(|(k, v)| k.used_size() + v.used_size())
            .sum();

        base + used
    }
}

impl<T: MemStat> MemStat for Box<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_size()
    }
    fn used_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).used_size()
    }
}

impl<T: MemStat> MemStat for Arc<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_size()
    }
    fn used_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).used_size()
    }
}

impl<T: MemStat> MemStat for Rc<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_size()
    }
    fn used_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).used_size()
    }
}

impl<K: MemStat + Eq + std::hash::Hash, V: MemStat> MemStat for HashMap<K, V> {
    fn heap_size(&self) -> usize {
        let bucket_size = size_of::<(K, V)>();
        let base_heap = self.capacity() * bucket_size;

        let kv_heap: usize = self
            .iter()
            .map(|(k, v)| k.heap_size() + v.heap_size())
            .sum();

        base_heap + kv_heap
    }
    fn used_size(&self) -> usize {
        let bucket_size = size_of::<(K, V)>();
        let base_used = self.len() * bucket_size;

        let kv_used: usize = self
            .iter()
            .map(|(k, v)| k.used_size() + v.used_size())
            .sum();

        base_used + kv_used
    }
}

impl MemStat for u8 {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for u16 {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for u32 {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for i32 {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for u64 {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for i64 {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for f64 {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for f32 {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for usize {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for isize {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for bool {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
impl MemStat for char {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}

impl MemStat for Link {
    fn heap_size(&self) -> usize {
        0
    }
    fn used_size(&self) -> usize {
        0
    }
}
