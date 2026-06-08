use async_trait::async_trait;

use anyhow::{Result, bail};

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
    async fn add(&self, _a: &T, _b: &T) -> Result<T> {
        bail!("handle addition is unsupported")
    }

    async fn neg(&self, _a: &T) -> Result<T> {
        bail!("handle negation is unsupported")
    }

    async fn identity(&self) -> Result<T> {
        Ok(self.default.clone())
    }
}
