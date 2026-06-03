use std::collections::HashMap;
use std::hash::Hash;

#[derive(Clone)]
struct LeftInterval<L, K> {
    row: L,
    lower: K,
    upper: K,
    weight: i64,
}

struct LeftIntervalNode<L, K> {
    center: K,
    by_lower: Vec<LeftInterval<L, K>>,
    by_upper_desc: Vec<LeftInterval<L, K>>,
    left: Option<Box<LeftIntervalNode<L, K>>>,
    right: Option<Box<LeftIntervalNode<L, K>>>,
}

pub(super) struct LeftIntervalIndex<L, K> {
    root: Option<Box<LeftIntervalNode<L, K>>>,
}

impl<L, K> LeftIntervalIndex<L, K>
where
    L: Clone + Eq + Hash,
    K: Clone + Ord,
{
    pub(super) fn from_cache(cache: &HashMap<L, (K, K, i64)>) -> Self {
        let intervals = cache
            .iter()
            .filter(|&(_row, (lower, upper, weight))| *weight != 0 && lower < upper)
            .map(|(row, (lower, upper, weight))| LeftInterval {
                row: row.clone(),
                lower: lower.clone(),
                upper: upper.clone(),
                weight: *weight,
            })
            .collect::<Vec<_>>();
        Self {
            root: Self::build_node(intervals),
        }
    }

    fn build_node(intervals: Vec<LeftInterval<L, K>>) -> Option<Box<LeftIntervalNode<L, K>>> {
        if intervals.is_empty() {
            return None;
        }

        let mut lowers = intervals
            .iter()
            .map(|interval| interval.lower.clone())
            .collect::<Vec<_>>();
        lowers.sort();
        let center = lowers[lowers.len() / 2].clone();

        let mut left = Vec::new();
        let mut right = Vec::new();
        let mut center_intervals = Vec::new();
        for interval in intervals {
            if interval.upper <= center {
                left.push(interval);
            } else if interval.lower > center {
                right.push(interval);
            } else {
                center_intervals.push(interval);
            }
        }

        let mut by_lower = center_intervals;
        by_lower.sort_by(|a, b| a.lower.cmp(&b.lower).then_with(|| a.upper.cmp(&b.upper)));
        let mut by_upper_desc = by_lower.clone();
        by_upper_desc.sort_by(|a, b| b.upper.cmp(&a.upper).then_with(|| a.lower.cmp(&b.lower)));

        Some(Box::new(LeftIntervalNode {
            center,
            by_lower,
            by_upper_desc,
            left: Self::build_node(left),
            right: Self::build_node(right),
        }))
    }

    pub(super) fn visit_point<F>(&self, point: &K, visitor: &mut F)
    where
        F: FnMut(&L, &K, &K, i64),
    {
        if let Some(root) = self.root.as_ref() {
            root.visit_point(point, visitor);
        }
    }
}

impl<L, K> LeftIntervalNode<L, K>
where
    K: Ord,
{
    fn visit_point<F>(&self, point: &K, visitor: &mut F)
    where
        F: FnMut(&L, &K, &K, i64),
    {
        if point < &self.center {
            for interval in &self.by_lower {
                if &interval.lower > point {
                    break;
                }
                visitor(
                    &interval.row,
                    &interval.lower,
                    &interval.upper,
                    interval.weight,
                );
            }
            if let Some(left) = self.left.as_ref() {
                left.visit_point(point, visitor);
            }
        } else {
            for interval in &self.by_upper_desc {
                if &interval.upper <= point {
                    break;
                }
                visitor(
                    &interval.row,
                    &interval.lower,
                    &interval.upper,
                    interval.weight,
                );
            }
            if let Some(right) = self.right.as_ref() {
                right.visit_point(point, visitor);
            }
        }
    }
}
