use indexset::cdc::change;

pub trait TableSecondaryIndexEventsOps<AvailableIndexes> {
    fn extend(&mut self, another: Self)
    where
        Self: Sized;
    fn remove(&mut self, another: &Self)
    where
        Self: Sized;
    fn iter_event_ids(&self) -> impl Iterator<Item = (AvailableIndexes, change::Id)>;
    fn contains_event(&self, index: AvailableIndexes, id: change::Id) -> bool {
        false
    }
    fn sort(&mut self);
    fn validate(&mut self) -> Self
    where
        Self: Sized;
    fn is_empty(&self) -> bool;
    fn is_unit() -> bool;
}
