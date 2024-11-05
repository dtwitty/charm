use crate::rng::CharmRng;
use derive_builder::Builder;
use rand::Rng;
use std::time::Duration;

#[derive(Debug, Clone, Builder)]
pub struct RetryStrategy {
    /// The initial delay before the first retry.
    #[builder(default = "Duration::from_millis(100)")]
    initial_delay: Duration,

    /// The maximum delay between retries.
    #[builder(default = "Duration::from_secs(1)")]
    max_delay: Duration,

    /// How many times to attempt the operation.
    /// Default is u64::MAX (basically infinite).
    #[builder(default = "u64::MAX")]
    max_attempts: u64,

    /// The factor to multiply the delay by after each retry.
    #[builder(default = "2.0")]
    factor: f64,

    /// Whether to apply jitter.
    #[builder(default = "true")]
    enable_jitter: bool,

    /// The random number generator to use for jitter.
    /// We need to pass one in for deterministic tests.
    rng: CharmRng
}

impl RetryStrategy {
    #[must_use] pub fn with_seed(seed: u64) -> Self {
        RetryStrategyBuilder::default().rng(CharmRng::new(seed)).build().unwrap()
    }

    fn validate(&self) {
        assert!(self.initial_delay >= Duration::from_secs(0));
        assert!(self.max_attempts > 0);
        assert!(self.factor >= 1.0);
    }

    fn apply_jitter(&mut self, delay: Duration) -> Duration {
        if !self.enable_jitter {
            return delay;
        }
        
        let jitter = self.rng.gen::<f64>();
        delay.mul_f64(jitter)
    }

    #[must_use] pub fn clone_rng(&self) -> CharmRng {
        self.rng.clone()
    }
}

#[derive(Debug, Clone)]
pub struct RetryStrategyIterator {
    strategy: RetryStrategy,
    current: Duration,
    attempts: u64,
}

impl Iterator for RetryStrategyIterator {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        if self.attempts >= self.strategy.max_attempts {
            return None;
        }

        self.attempts += 1;

        // Apply jitter.
        let delay = self.strategy.apply_jitter(self.current).min(self.strategy.max_delay);

        // Exponential backoff.
        self.current = self.current.mul_f64(self.strategy.factor).min(self.strategy.max_delay);

        Some(delay)
    }
}

impl IntoIterator for RetryStrategy {
    type Item = Duration;
    type IntoIter = RetryStrategyIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.validate();
        let current = self.initial_delay;
        RetryStrategyIterator {
            strategy: self,
            current,
            attempts: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_strategy() {
        let strategy = RetryStrategyBuilder::default()
            .rng(CharmRng::new(0))
            .max_attempts(7)
            .max_delay(Duration::from_secs(1))
            .enable_jitter(false)
            .build()
            .expect("valid");
        let mut iter = strategy.into_iter();
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(200)));
        assert_eq!(iter.next(), Some(Duration::from_millis(400)));
        assert_eq!(iter.next(), Some(Duration::from_millis(800)));
        assert_eq!(iter.next(), Some(Duration::from_secs(1)));
        assert_eq!(iter.next(), Some(Duration::from_secs(1)));
        assert_eq!(iter.next(), Some(Duration::from_secs(1)));
        assert_eq!(iter.next(), None);
    }
}




