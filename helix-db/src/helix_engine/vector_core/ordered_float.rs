/// A wrapper type around f32s implementing `Ord`
///
/// Since distance metrics satisfy d(x,x)=0 and d(x,y)>0 for x!=y we don't need to operate on the
/// full range of f32's. Comparing the u32 representation of a non-negative f32 should suffice and
/// is actually a lot quicker.
///
/// https://en.wikipedia.org/wiki/IEEE_754-1985#NaN
#[derive(Default, Debug, Clone, Copy)]
pub struct OrderedFloat(pub f32);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits().eq(&other.0.to_bits())
    }
}

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.to_bits().cmp(&other.0.to_bits())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::helix_engine::vector_core::ordered_float::OrderedFloat;

    proptest! {
        #[test]
        fn ordering_makes_sense(
            (upper, lower) in (0.0f32..=f32::MAX).prop_flat_map(|u|{
                (Just(u), 0.0f32..=u)
            })
        ){
            assert!(OrderedFloat(upper) > OrderedFloat(lower));
        }
    }
}
