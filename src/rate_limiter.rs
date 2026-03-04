//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

use std::{
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// A rate limiter implementation using a simple time-based mechanism.
///
/// This limiter keeps a single "lastNano" time and a "nanos_per_unit".
/// Together these represent a number of units available based on
/// the current time.
///
/// When units are consumed, the lastNano value is incremented by
/// (units * nanos_per_unit). If the result is greater than the current
/// time, a single sleep() is called to wait for the time difference.
///
/// This method inherently "queues" the consume calls, since each consume
/// will increment the lastNano time. For example, a request for a small
/// number of units will have to wait for a previous request for a large
/// number of units. In this way, small requests can never "starve" large
/// requests.
///
/// This limiter allows for a specified number of seconds of "duration"
/// to be used: if units have not been used in the past N seconds, they can
/// be used now. The minimum duration is internally bound such that a
/// consume of 1 unit will always have a chance of succeeding without waiting.
///
/// Note that "giving back" (returning) previously consumed units will
/// only affect consume calls made after the return. Currently sleeping
/// consume calls will not have their sleep times shortened.
#[derive(Default, Debug, Clone)]
pub struct RateLimiter {
    // how many nanoseconds represent one unit
    nanos_per_unit: i64,

    // how many seconds worth of units can we use from the past
    duration_nanos: i64,

    // last used unit nanosecond: this is the main "value"
    last_nano: i64,
}

impl RateLimiter {
    /// Create a simple time-based rate limiter.
    ///
    /// This limiter will allow for one second of duration; that is, it
    /// will allow unused units from within the last second to be used.
    ///
    /// limit_per_sec: the maximum number of units allowed per second
    pub fn new(limit_per_sec: f64) -> RateLimiter {
        Self::new_with_duration(limit_per_sec, 1.0)
    }

    /// Creates a simple time-based rate limiter.
    ///
    /// limit_per_sec: the maximum number of units allowed per second
    /// duration_secs: maximum amount of time to consume unused units from the past
    pub fn new_with_duration(limit_per_sec: f64, duration_secs: f64) -> RateLimiter {
        let mut rl = RateLimiter {
            ..Default::default()
        };
        rl.set_limit_per_second(limit_per_sec);
        rl.set_duration(duration_secs);
        rl.reset();
        rl
    }

    pub fn set_limit_per_second(&mut self, limit_per_sec: f64) {
        if limit_per_sec <= 0.0 {
            self.nanos_per_unit = 0;
        } else {
            self.nanos_per_unit = (1_000_000_000.0 / limit_per_sec) as i64;
        }
        self.enforce_minimum_duration();
    }

    pub fn set_duration(&mut self, duration_secs: f64) {
        self.duration_nanos = (1_000_000_000.0 / duration_secs) as i64;
    }

    fn enforce_minimum_duration(&mut self) {
        // Force duration_nanos such that the limiter can always be capable
        // of consuming at least 1 unit without waiting
        if self.duration_nanos < self.nanos_per_unit {
            self.duration_nanos = self.nanos_per_unit;
        }
    }

    fn nano_time() -> i64 {
        let now = SystemTime::now();
        let duration_since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards"); // Handle potential error if system time is set incorrectly

        duration_since_epoch.as_nanos() as i64
    }

    fn reset(&mut self) {
        self.last_nano = RateLimiter::nano_time();
    }

    pub async fn consume_units_with_timeout(
        &mut self,
        units: i64,
        timeout_ms: i64,
        always_consume: bool,
    ) -> Result<i64, Box<dyn Error>> {
        if timeout_ms < 0 {
            return Err("timeout_ms must not be negative".into());
        }
        let ms_to_sleep =
            self.consume_internal(units, timeout_ms, always_consume, RateLimiter::nano_time());
        if ms_to_sleep == 0 {
            return Ok(0);
        }
        if timeout_ms > 0 && ms_to_sleep > timeout_ms {
            tokio::time::sleep(Duration::from_millis(ms_to_sleep as u64)).await;
            return Err("consume timed out".into());
        }
        tokio::time::sleep(Duration::from_millis(ms_to_sleep as u64)).await;
        Ok(ms_to_sleep)
    }

    fn consume_internal(
        &mut self,
        units: i64,
        timeout_ms: i64,
        always_consume: bool,
        now_nanos: i64,
    ) -> i64 {
        // If disabled, just return success
        if self.nanos_per_unit <= 0 {
            return 0;
        }

        /* determine how many nanos we need to add based on units requested */
        let nanos_needed = units * self.nanos_per_unit;

        /* ensure we never use more from the past than duration allows */
        let max_past = now_nanos - self.duration_nanos;
        if self.last_nano < max_past {
            self.last_nano = max_past;
        }

        /* compute the new "last nano used" */
        let new_last = self.last_nano + nanos_needed;

        /* if units < 0, we're "returning" them */
        if units < 0 {
            /* consume the units */
            self.last_nano = new_last;
            return 0;
        }
        /*
         * if the limiter is currently under its limit, the consume
         * succeeds immediately (no sleep required).
         */
        if self.last_nano <= now_nanos {
            /* consume the units */
            self.last_nano = new_last;
            return 0;
        }

        /*
         * determine the amount of time that the caller needs to sleep
         * for this limiter to go below its limit. Note that the limiter
         * is not guaranteed to be below the limit after this time, as
         * other consume calls may come in after this one and push the
         * "at the limit time" further out.
         */
        let mut sleep_ms = (self.last_nano - now_nanos) / 1_000_000;
        if sleep_ms == 0 {
            sleep_ms = 1;
        }
        if always_consume {
            /*
             * if we're told to always consume the units no matter what,
             * consume the units
             */
            self.last_nano = new_last;
        } else if timeout_ms == 0 {
            /* if the timeout is zero, consume the units */
            self.last_nano = new_last;
        } else if sleep_ms < timeout_ms {
            /*
             * if the given timeout is more than the amount of time to
             * sleep, consume the units
             */
            self.last_nano = new_last;
        }

        sleep_ms
    }
}
