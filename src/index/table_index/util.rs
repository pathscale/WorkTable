use indexset::cdc::change::ChangeEvent;
use indexset::core::multipair::MultiPair;
use indexset::core::pair::Pair;
use vanilla_indexset::cdc::change::ChangeEvent as VanillaChangeEvent;
use vanilla_indexset::core::pair::Pair as VanillaPair;

pub fn convert_change_event<T, L1, L2>(ev: ChangeEvent<Pair<T, L1>>) -> ChangeEvent<Pair<T, L2>>
where
    L1: Into<L2>,
{
    match ev {
        ChangeEvent::InsertAt {
            event_id,
            max_value,
            value,
            index,
        } => ChangeEvent::InsertAt {
            event_id,
            max_value: Pair {
                key: max_value.key,
                value: max_value.value.into(),
            },
            value: Pair {
                key: value.key,
                value: value.value.into(),
            },
            index,
        },
        ChangeEvent::RemoveAt {
            event_id,
            max_value,
            value,
            index,
        } => ChangeEvent::RemoveAt {
            event_id,
            max_value: Pair {
                key: max_value.key,
                value: max_value.value.into(),
            },
            value: Pair {
                key: value.key,
                value: value.value.into(),
            },
            index,
        },
        ChangeEvent::CreateNode { event_id, max_value } => ChangeEvent::CreateNode {
            event_id,
            max_value: Pair {
                key: max_value.key,
                value: max_value.value.into(),
            },
        },
        ChangeEvent::RemoveNode { event_id, max_value } => ChangeEvent::RemoveNode {
            event_id,
            max_value: Pair {
                key: max_value.key,
                value: max_value.value.into(),
            },
        },
        ChangeEvent::SplitNode {
            event_id,
            max_value,
            split_index,
        } => ChangeEvent::SplitNode {
            event_id,
            max_value: Pair {
                key: max_value.key,
                value: max_value.value.into(),
            },
            split_index,
        },
    }
}

pub fn convert_change_events<T, L1, L2>(evs: Vec<ChangeEvent<Pair<T, L1>>>) -> Vec<ChangeEvent<Pair<T, L2>>>
where
    L1: Into<L2>,
{
    evs.into_iter().map(convert_change_event).collect()
}

/// Converts the ordered pair representation used by WTI multimaps into the
/// key-only `Pair` representation retained by WorkTable's persistence format.
pub fn convert_multi_change_events<T, L1, L2>(evs: Vec<ChangeEvent<MultiPair<T, L1>>>) -> Vec<ChangeEvent<Pair<T, L2>>>
where
    L1: Into<L2>,
{
    fn pair<T, L1, L2>(value: MultiPair<T, L1>) -> Pair<T, L2>
    where
        L1: Into<L2>,
    {
        Pair {
            key: value.key,
            value: value.value.into(),
        }
    }

    evs.into_iter()
        .map(|event| match event {
            ChangeEvent::InsertAt {
                event_id,
                max_value,
                value,
                index,
            } => ChangeEvent::InsertAt {
                event_id,
                max_value: pair(max_value),
                value: pair(value),
                index,
            },
            ChangeEvent::RemoveAt {
                event_id,
                max_value,
                value,
                index,
            } => ChangeEvent::RemoveAt {
                event_id,
                max_value: pair(max_value),
                value: pair(value),
                index,
            },
            ChangeEvent::CreateNode { event_id, max_value } => ChangeEvent::CreateNode {
                event_id,
                max_value: pair(max_value),
            },
            ChangeEvent::RemoveNode { event_id, max_value } => ChangeEvent::RemoveNode {
                event_id,
                max_value: pair(max_value),
            },
            ChangeEvent::SplitNode {
                event_id,
                max_value,
                split_index,
            } => ChangeEvent::SplitNode {
                event_id,
                max_value: pair(max_value),
                split_index,
            },
        })
        .collect()
}

/// Normalizes upstream IndexSet CDC events into WorkTablesIndex's event type,
/// which remains the stable persistence boundary used by DataBucket.
pub fn convert_upstream_change_events<T, L1, L2>(
    evs: Vec<VanillaChangeEvent<VanillaPair<T, L1>>>,
) -> Vec<ChangeEvent<Pair<T, L2>>>
where
    L1: Into<L2>,
{
    evs.into_iter()
        .map(|event| match event {
            VanillaChangeEvent::InsertAt {
                event_id,
                max_value,
                value,
                index,
            } => ChangeEvent::InsertAt {
                event_id: event_id.inner().into(),
                max_value: upstream_pair(max_value),
                value: upstream_pair(value),
                index,
            },
            VanillaChangeEvent::RemoveAt {
                event_id,
                max_value,
                value,
                index,
            } => ChangeEvent::RemoveAt {
                event_id: event_id.inner().into(),
                max_value: upstream_pair(max_value),
                value: upstream_pair(value),
                index,
            },
            VanillaChangeEvent::CreateNode { event_id, max_value } => ChangeEvent::CreateNode {
                event_id: event_id.inner().into(),
                max_value: upstream_pair(max_value),
            },
            VanillaChangeEvent::RemoveNode { event_id, max_value } => ChangeEvent::RemoveNode {
                event_id: event_id.inner().into(),
                max_value: upstream_pair(max_value),
            },
            VanillaChangeEvent::SplitNode {
                event_id,
                max_value,
                split_index,
            } => ChangeEvent::SplitNode {
                event_id: event_id.inner().into(),
                max_value: upstream_pair(max_value),
                split_index,
            },
        })
        .collect()
}

fn upstream_pair<T, L1, L2>(pair: VanillaPair<T, L1>) -> Pair<T, L2>
where
    L1: Into<L2>,
{
    Pair {
        key: pair.key,
        value: pair.value.into(),
    }
}
