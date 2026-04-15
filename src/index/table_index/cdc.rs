use std::fmt::Debug;
use std::hash::Hash;

use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::multipair::MultiPair;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;

use crate::index::table_index::util::convert_change_events;
use crate::util::OffsetEqLink;
use crate::{IndexMap, IndexMultiMap};

pub trait TableIndexCdc<T> {
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>);
    fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>>;
    #[allow(clippy::type_complexity)]
    fn remove_cdc(
        &self,
        value: T,
        link: Link,
    ) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>);
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

    fn remove_cdc(
        &self,
        value: T,
        link: Link,
    ) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
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

    fn remove_cdc(
        &self,
        value: T,
        _: Link,
    ) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, evs) = self.remove_cdc(&value);
        let res_pair = res.map(|(k, v)| (k, v.0));
        (res_pair, convert_change_events(evs))
    }
}
