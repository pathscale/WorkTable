use data_bucket::Link;
use indexset::cdc::change::{self, ChangeEvent};
use indexset::core::pair::Pair;
use std::fmt::Debug;

pub fn validate_events<T>(evs: &mut Vec<ChangeEvent<Pair<T, Link>>>) -> Vec<ChangeEvent<Pair<T, Link>>>
where
    T: Debug,
{
    let mut removed_events = vec![];
    let mut finish_condition = false;

    while !finish_condition {
        let (iteration_events, error_pos) = validate_events_iteration(evs);
        if iteration_events.is_empty() {
            finish_condition = true;
        } else {
            let drain_pos = evs.len() - error_pos;
            removed_events.extend(evs.drain(drain_pos..));
        }
    }

    removed_events.sort_by_key(|ev2| std::cmp::Reverse(ev2.id()));

    removed_events
}

fn validate_events_iteration<T>(evs: &[ChangeEvent<Pair<T, Link>>]) -> (Vec<change::Id>, usize) {
    let Some(mut last_ev_id) = evs.last().map(|ev| ev.id()) else {
        return (vec![], 0);
    };
    let mut evs_before_error = vec![last_ev_id];
    let mut rev_evs_iter = evs.iter().rev().skip(1);
    let mut error_flag = false;
    let mut check_depth = 1;

    // The scan must cover the whole batch: change event ids are only valid to
    // apply as a gapless stream, and page-grouped batch collection routinely
    // produces interior gaps (ops on other data pages carry the missing ids).
    // A bounded scan that stops inside a long contiguous tail would miss such
    // a gap and let the batch corrupt the on-disk index node/TOC state.
    while !error_flag {
        if let Some(next_ev) = rev_evs_iter.next().map(|ev| ev.id()) {
            if last_ev_id.is_next_for(next_ev) || last_ev_id == next_ev {
                check_depth += 1;
                last_ev_id = next_ev;
                evs_before_error.push(last_ev_id);
            } else {
                error_flag = true
            }
        } else {
            break;
        }
    }

    if error_flag {
        (evs_before_error, check_depth)
    } else {
        (vec![], 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert_at(id: u64) -> ChangeEvent<Pair<u64, Link>> {
        ChangeEvent::InsertAt {
            event_id: id.into(),
            max_value: Pair {
                key: id,
                value: Link::default(),
            },
            value: Pair {
                key: id,
                value: Link::default(),
            },
            index: 0,
        }
    }

    #[test]
    fn detects_gap_behind_long_contiguous_tail() {
        // Interior gap (100..=139, then 1000..=1049) whose tail is longer than
        // any bounded scan window: everything after the gap must be deferred.
        let mut evs: Vec<_> = (100..140).map(insert_at).collect();
        evs.extend((1000..1050).map(insert_at));

        let removed = validate_events(&mut evs);

        assert_eq!(evs.len(), 40);
        assert!(evs.iter().all(|ev| ev.id() < 140.into()));
        assert_eq!(removed.len(), 50);
        assert!(removed.iter().all(|ev| ev.id() >= 1000.into()));
    }

    #[test]
    fn keeps_gapless_stream_untouched() {
        let mut evs: Vec<_> = (100..200).map(insert_at).collect();
        let removed = validate_events(&mut evs);
        assert!(removed.is_empty());
        assert_eq!(evs.len(), 100);
    }
}
