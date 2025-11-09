use crate::stream_types::Timestamp;

/// Monotonically increasing identifier for barrier steps.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct StepId(u64);

impl StepId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }

    fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// Tracks the current barrier step and watermark to coordinate DBSP sealing.
#[derive(Debug)]
pub struct BarrierClock {
    step: StepId,
    watermark: Timestamp,
}

impl BarrierClock {
    pub fn new() -> Self {
        Self {
            step: StepId::default(),
            watermark: 0,
        }
    }

    /// Returns the latest committed watermark.
    pub fn watermark(&self) -> Timestamp {
        self.watermark
    }

    /// Returns the current step identifier.
    pub fn step(&self) -> StepId {
        self.step
    }

    /// Advances the clock to a higher watermark, returning the new `StepId`.
    pub fn advance(&mut self, watermark: Timestamp) -> Option<StepId> {
        if watermark <= self.watermark {
            return None;
        }
        self.watermark = watermark;
        self.step = self.step.next();
        Some(self.step)
    }

    /// Initializes the clock when replaying from a persisted manifest.
    pub fn bootstrap(&mut self, watermark: Timestamp) {
        self.watermark = watermark;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advances_monotonically() {
        let mut clock = BarrierClock::new();
        assert!(clock.advance(5).is_some());
        assert_eq!(clock.watermark(), 5);
        assert_eq!(clock.step().as_u64(), 1);
        assert!(clock.advance(5).is_none());
        assert_eq!(clock.step().as_u64(), 1);
        assert!(clock.advance(6).is_some());
        assert_eq!(clock.step().as_u64(), 2);
    }

    #[test]
    fn bootstraps_from_manifest() {
        let mut clock = BarrierClock::new();
        clock.bootstrap(42);
        assert!(clock.advance(43).is_some());
        assert_eq!(clock.step().as_u64(), 1);
    }
}
