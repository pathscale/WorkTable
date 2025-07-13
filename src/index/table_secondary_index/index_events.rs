pub trait TableSecondaryIndexEventsOps {
    fn extend(&mut self, another: Self)
    where
        Self: Sized;
    fn sort(&mut self);
    fn validate(&mut self) -> Self
    where
        Self: Sized;
    fn is_empty(&self) -> bool;
    fn is_unit() -> bool;
}
