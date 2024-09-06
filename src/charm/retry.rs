use derive_builder::Builder;
use std::time::Duration;
use madsim::rand;

#[derive(Debug, Clone, Builder)]
pub struct RetryStrategy {
    /// The initial delay before the first retry.
    #[builder(default = "Duration::from_millis(100)")]
    initial_delay: Duration,

    /// The maximum delay between retries.
    #[builder(default = "Duration::from_secs(1)")]
    max_delay: Duration,

    /// The total time spent on retries before giving up.
    #[builder(default = "Duration::from_secs(5)")]
    total_retry_time: Duration,

    /// The factor to multiply the delay by after each retry.
    #[builder(default = "2.0")]
    factor: f64,

    /// The minimum amount of jitter to add to the delay.
    /// The actual jitter will be a random value between `jitter_min` and `jitter_max`.
    #[builder(default = "0.8")]
    jitter_min: f64,

    /// The maximum amount of jitter to add to the delay.
    /// The actual jitter will be a random value between `jitter_min` and `jitter_max`.
    #[builder(default = "1.2")]
    jitter_max: f64,
}

impl Default for RetryStrategy {
    fn default() -> Self {
        RetryStrategy::new()
    }
}

impl RetryStrategyBuilder {
    pub fn no_jitter(&mut self) -> &mut Self {
        self.jitter_min(1.0).jitter_max(1.0)
    }
}

impl RetryStrategy {
    pub fn new() -> RetryStrategy {
        RetryStrategyBuilder::default().build().unwrap()
    }

    fn validate(&self) {
        assert!(self.initial_delay >= Duration::from_secs(0));
        assert!(self.total_retry_time >= Duration::from_secs(0));
        assert!(self.factor >= 1.0);
        assert!(self.jitter_min >= 0.0);
        assert!(self.jitter_max >= self.jitter_min);
    }

    fn apply_jitter(&self, delay: Duration) -> Duration {
        let range = self.jitter_max - self.jitter_min;
        let jitter = rand::random::<f64>() * range + self.jitter_min;
        delay.mul_f64(jitter)
    }
}

pub struct RetryStrategyIterator {
    strategy: RetryStrategy,
    current: Duration,
    so_far: Duration,
}

impl Iterator for RetryStrategyIterator {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        // Cap the total time spent on retries.
        if self.so_far >= self.strategy.total_retry_time {
            return None;
        }

        // Apply jitter.
        let delay = self.strategy.apply_jitter(self.current).min(self.strategy.max_delay);

        // Exponential backoff.
        self.current = self.current.mul_f64(self.strategy.factor);

        // Update the total time spent on retries.
        self.so_far += delay;

        Some(delay)
    }
}

impl IntoIterator for RetryStrategy {
    type Item = Duration;
    type IntoIter = RetryStrategyIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.validate();
        let current = self.initial_delay.clone();
        RetryStrategyIterator {
            strategy: self,
            current,
            so_far: Duration::from_secs(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_strategy() {
        let strategy = RetryStrategyBuilder::default().no_jitter().build().expect("valid");
        let mut iter = strategy.into_iter();
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(200)));
        assert_eq!(iter.next(), Some(Duration::from_millis(400)));
        assert_eq!(iter.next(), Some(Duration::from_millis(800)));
        assert_eq!(iter.next(), Some(Duration::from_secs(1)));
        assert_eq!(iter.next(), Some(Duration::from_secs(1)));
        assert_eq!(iter.next(), Some(Duration::from_secs(1)));
        assert_eq!(iter.next(), Some(Duration::from_secs(1)));
        assert_eq!(iter.next(), None);
    }
}




