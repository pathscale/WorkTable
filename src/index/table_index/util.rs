use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;

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
        ChangeEvent::CreateNode {
            event_id,
            max_value,
        } => ChangeEvent::CreateNode {
            event_id,
            max_value: Pair {
                key: max_value.key,
                value: max_value.value.into(),
            },
        },
        ChangeEvent::RemoveNode {
            event_id,
            max_value,
        } => ChangeEvent::RemoveNode {
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

pub fn convert_change_events<T, L1, L2>(
    evs: Vec<ChangeEvent<Pair<T, L1>>>,
) -> Vec<ChangeEvent<Pair<T, L2>>>
where
    L1: Into<L2>,
{
    evs.into_iter().map(convert_change_event).collect()
}
