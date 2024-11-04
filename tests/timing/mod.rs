use rand::distributions::Distribution;
use rand::RngCore;
use rand_distr::{Exp, Normal};
use std::time::Duration;

pub struct PoissonProcess<R> {
    rng: R,
    dist: Exp<f64>,
}

impl<R: RngCore> PoissonProcess<R> {
    pub fn new(rng: R, mean_interval: Duration) -> Self {
        let lambda = 1.0 / mean_interval.as_secs_f64();
        let dist = Exp::new(lambda).unwrap();
        Self { rng, dist }
    }
}

impl<R: RngCore> Iterator for PoissonProcess<R> {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        let sample = self.dist.sample(&mut self.rng);
        Some(Duration::from_secs_f64(sample))
    }
}

#[cfg(test)]
mod poisson_tests {
    use super::*;

    #[test]
    fn test_poisson_process() {
        let rng = wyrand::WyRand::new(0);
        let mean_interval = Duration::from_secs(1);
        let process = PoissonProcess::new(rng, mean_interval);

        for interval in process.take(100) {
            assert!(interval >= Duration::ZERO);
            assert!(interval < mean_interval * 10);
        }
    }
}

pub struct RandomDuration<R> {
    rng: R,
    dist: Normal<f64>,
}

impl<R: RngCore> RandomDuration<R> {
    pub fn new(rng: R, mean: Duration, std_dev: Duration) -> Self {
        let mean = mean.as_secs_f64();
        let std_dev = std_dev.as_secs_f64();
        let dist = Normal::new(mean, std_dev).unwrap();
        Self { rng, dist }
    }

    pub fn sample(&mut self) -> Duration {
        let sample = self.dist.sample(&mut self.rng);
        Duration::from_secs_f64(sample).max(Duration::ZERO)
    }
}

#[cfg(test)]
mod random_duration_tests {
    use super::*;

    #[test]
    fn test_random_duration() {
        let rng = wyrand::WyRand::new(0);
        let mean = Duration::from_secs(1);
        let std_dev = Duration::from_millis(100);
        let mut dist = RandomDuration::new(rng, mean, std_dev);

        for _ in 0..100 {
            let sample = dist.sample();
            assert!(sample >= Duration::ZERO);
            assert!(sample < mean * 10);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashEvent {
    Crash,
    Recover,
}

/// A schedule for the crashing and recovery of a single node.
pub struct CrashSchedule<R> {
    crash_process: PoissonProcess<R>,
    recover_time_dist: RandomDuration<R>,
    next_event: CrashEvent,
    next_event_time: Duration,
}

impl<R: RngCore + Clone> CrashSchedule<R> {
    pub fn new(rng: R, crash_interval_mean: Duration, recover_time_mean: Duration, recover_time_stddev: Duration) -> Self {
        let mut crash_process = PoissonProcess::new(rng.clone(), crash_interval_mean);
        let recover_time_dist = RandomDuration::new(rng, recover_time_mean, recover_time_stddev);
        let next_event_time = crash_process.next().unwrap();
        Self {
            crash_process,
            recover_time_dist,
            next_event: CrashEvent::Crash,
            next_event_time,
        }
    }


    /// Inform the schedule of the current time, and return any event that has happened in the meantime.
    pub fn advance(&mut self, elapsed: Duration) -> Option<CrashEvent> {
        if self.next_event_time > elapsed {
            // Not enough time has passed.
            return None;
        }


        let event = self.next_event;

        match event {
            CrashEvent::Crash => {
                self.next_event = CrashEvent::Recover;
                self.next_event_time = elapsed + self.recover_time_dist.sample();
            }

            CrashEvent::Recover => {
                self.next_event = CrashEvent::Crash;
                self.next_event_time = elapsed + self.crash_process.next()?;
            }
        }

        Some(event)
    }
}

#[cfg(test)]
mod crash_schedule_tests {
    use super::*;

    #[test]
    fn test_crash_schedule() {
        let rng = wyrand::WyRand::new(0);
        let crash_interval_mean = Duration::from_secs(30);
        let recover_time_mean = Duration::from_secs(3);
        let recover_time_stddev = Duration::from_millis(100);
        let mut schedule = CrashSchedule::new(rng, crash_interval_mean, recover_time_mean, recover_time_stddev);

        let mut events = Vec::new();

        let mut elapsed = Duration::ZERO;
        for _ in 0..1000 {
            elapsed += Duration::from_secs(1);
            let event = schedule.advance(elapsed);
            if let Some(event) = event {
                events.push(event);
            }
        }
        
        // Assert alternating crash and recover events.
        for (i, event) in events.iter().enumerate() {
            match (i % 2, event) {
                (0, CrashEvent::Crash) => {}
                (1, CrashEvent::Recover) => {}
                _ => panic!("unexpected event: {:?}", event),
            }
        }
    }
}

pub struct ClusterCrashSchedule<T, R> {
    crash_schedules: Vec<(T, CrashSchedule<R>)>,
}

impl<R: RngCore + Clone, T: Clone> ClusterCrashSchedule<T, R> {
    pub fn new(mut rng: R, nodes: Vec<T>, crash_interval_mean: Duration, recover_time_mean: Duration, recover_time_stddev: Duration) -> Self {
        let crash_schedules = nodes
            .into_iter()
            .map(|n| {
                // Consume some random numbers to avoid correlated schedules.
                let _ = rng.next_u64();
                (n, CrashSchedule::new(rng.clone(), crash_interval_mean, recover_time_mean, recover_time_stddev))
            })
            .collect();
        Self { crash_schedules }
    }

    pub fn advance(&mut self, elapsed: Duration) -> Vec<(T, CrashEvent)> {
        self.crash_schedules.iter_mut()
            .filter_map(|(n, schedule)| schedule.advance(elapsed).map(|event| (n.clone(), event)))
            .collect()
    }
}
