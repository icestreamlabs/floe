use std::ops::{Deref, DerefMut};

use crate::handles::ZSetHandle;

use super::core::stream::Stream;

/// Stream of snapshot handles (integrated state).
#[derive(Clone)]
pub struct SnapshotHandleStream {
    stream: Stream<ZSetHandle>,
}

impl SnapshotHandleStream {
    pub fn new(stream: Stream<ZSetHandle>) -> Self {
        Self { stream }
    }

    pub fn stream(&self) -> Stream<ZSetHandle> {
        self.stream.clone()
    }

    pub fn into_stream(self) -> Stream<ZSetHandle> {
        self.stream
    }
}

impl Deref for SnapshotHandleStream {
    type Target = Stream<ZSetHandle>;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for SnapshotHandleStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

/// Stream of delta handles (per-tick changes).
///
/// ```compile_fail
/// use dbsp::stream::{DeltaHandleStream, SnapshotHandleStream};
///
/// fn needs_delta(_stream: &DeltaHandleStream) {}
///
/// let snapshot: SnapshotHandleStream = todo!();
/// needs_delta(&snapshot);
/// ```
#[derive(Clone)]
pub struct DeltaHandleStream {
    stream: Stream<ZSetHandle>,
}

impl DeltaHandleStream {
    pub fn new(stream: Stream<ZSetHandle>) -> Self {
        Self { stream }
    }

    pub fn stream(&self) -> Stream<ZSetHandle> {
        self.stream.clone()
    }

    pub fn into_stream(self) -> Stream<ZSetHandle> {
        self.stream
    }
}

impl Deref for DeltaHandleStream {
    type Target = Stream<ZSetHandle>;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for DeltaHandleStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}
