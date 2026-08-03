use std::fmt::Debug;
use std::hash::Hash;

use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::multipair::MultiPair;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use vanilla_indexset::core::node::NodeLike as VanillaNodeLike;
use vanilla_indexset::core::pair::Pair as VanillaPair;

use crate::index::table_index::util::{convert_change_events, convert_upstream_change_events};
use crate::util::OffsetEqLink;
use crate::{ArcticIndex, ArcticKey, CongeeIndex, CongeeKey, IndexMap, IndexMultiMap, UniqueIndex, UpstreamIndexMap};

pub trait TableIndexCdc<T> {
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>);
    fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>>;
    #[allow(clippy::type_complexity)]
    fn remove_cdc(&self, value: T, link: Link) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>);
}

impl<T, Node, const N: usize> TableIndexCdc<T> for IndexMultiMap<T, OffsetEqLink<N>, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<MultiPair<T, OffsetEqLink<N>>> + Send + 'static,
{
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, evs) = self.insert_cdc(value, OffsetEqLink(link));
        let pair_evs = evs.into_iter().map(Into::into).collect();
        let res_link = res.map(|l| l.0);
        (res_link, convert_change_events(pair_evs))
    }

    fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>> {
        let (res, evs) = self.insert_cdc(value, OffsetEqLink(link));
        let pair_evs = evs.into_iter().map(Into::into).collect();
        if res.is_some() {
            None
        } else {
            Some(convert_change_events(pair_evs))
        }
    }

    fn remove_cdc(&self, value: T, link: Link) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, evs) = self.remove_cdc(&value, &OffsetEqLink(link));
        let pair_evs = evs.into_iter().map(Into::into).collect();
        let res_pair = res.map(|(k, v)| (k, v.into()));
        (res_pair, convert_change_events(pair_evs))
    }
}

impl<T, Node, const N: usize> TableIndexCdc<T> for IndexMap<T, OffsetEqLink<N>, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<Pair<T, OffsetEqLink<N>>> + Send + 'static,
{
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, evs) = self.insert_cdc(value, OffsetEqLink(link));
        let res_link = res.map(|l| l.0);
        (res_link, convert_change_events(evs))
    }

    fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>> {
        let res = self.checked_insert_cdc(value, OffsetEqLink(link));
        res.map(|evs| convert_change_events(evs))
    }

    fn remove_cdc(&self, value: T, _: Link) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, evs) = self.remove_cdc(&value);
        let res_pair = res.map(|(k, v)| (k, v.0));
        (res_pair, convert_change_events(evs))
    }
}

impl<T, Node, const N: usize> TableIndexCdc<T> for UpstreamIndexMap<T, OffsetEqLink<N>, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: VanillaNodeLike<VanillaPair<T, OffsetEqLink<N>>> + Send + 'static,
{
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, events) = self.insert_cdc(value, OffsetEqLink(link));
        (res.map(|value| value.0), convert_upstream_change_events(events))
    }

    fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>> {
        self.checked_insert_cdc(value, OffsetEqLink(link))
            .map(convert_upstream_change_events)
    }

    fn remove_cdc(&self, value: T, _: Link) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, events) = self.remove_cdc(&value);
        (
            res.map(|(key, value)| (key, value.0)),
            convert_upstream_change_events(events),
        )
    }
}

/// Memory-only ARTs participate in the common mutation path but emit no
/// durable events. The DSL prevents these implementations from appearing in
/// a persisted table.
macro_rules! impl_memory_only_cdc {
    ($index:ty, [$($bound:tt)*]) => {
        impl<T, const N: usize> TableIndexCdc<T> for $index
        where
            T: $($bound)*,
        {
            fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
                let old = self.insert_value(value, OffsetEqLink(link)).map(|value| value.0);
                (old, Vec::new())
            }

            fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>> {
                self.insert_value_checked(value, OffsetEqLink(link)).map(|()| Vec::new())
            }

            fn remove_cdc(&self, value: T, _: Link) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
                let removed = self.remove_value(&value).map(|(key, value)| (key, value.0));
                (removed, Vec::new())
            }
        }
    };
}

impl_memory_only_cdc!(CongeeIndex<T, OffsetEqLink<N>>, [CongeeKey + Eq + Hash]);
impl_memory_only_cdc!(ArcticIndex<T, OffsetEqLink<N>>, [ArcticKey + Eq + Hash]);
