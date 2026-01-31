use async_trait::async_trait;

use crate::algebra::AbelianGroup;

#[derive(Clone)]
pub(crate) struct HandleGroup<T>
where
    T: Clone + Send + Sync + 'static,
{
    default: T,
}

impl<T> HandleGroup<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub(crate) fn new(default: T) -> Self {
        Self { default }
    }
}

#[async_trait]
impl<T> AbelianGroup<T> for HandleGroup<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn add(&self, _a: &T, _b: &T) -> T {
        unreachable!("handle addition is unsupported")
    }

    async fn neg(&self, _a: &T) -> T {
        unreachable!("handle negation is unsupported")
    }

    async fn identity(&self) -> T {
        self.default.clone()
    }
}
