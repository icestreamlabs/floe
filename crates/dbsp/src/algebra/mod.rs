use std::sync::Arc;

/// Basic operations required for an Abelian group.
///
/// This mirrors the `AbelianGroupOperation` protocol from `pydbsp.core`.
pub trait AbelianGroup<T>: Send + Sync {
    fn add(&self, a: &T, b: &T) -> T;
    fn neg(&self, a: &T) -> T;
    fn identity(&self) -> T;
}

impl<T, G> AbelianGroup<T> for Arc<G>
where
    G: AbelianGroup<T> + ?Sized,
{
    fn add(&self, a: &T, b: &T) -> T {
        (**self).add(a, b)
    }

    fn neg(&self, a: &T) -> T {
        (**self).neg(a)
    }

    fn identity(&self) -> T {
        (**self).identity()
    }
}
