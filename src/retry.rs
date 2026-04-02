//! Retry logic with exponential backoff for transient gRPC errors.

use std::{future::Future, time::Duration};

use arrow_flight::error::FlightError;
use tonic::Code;

use crate::error::{Error, Result};

// ── RetryConfig ───────────────────────────────────────────────────────────────

/// Configures exponential-backoff retry behaviour for non-streaming operations.
///
/// The default has `max_attempts = 1` (no retries) — opt in explicitly via
/// [`ClientBuilder::retry_config`](crate::ClientBuilder::retry_config).
///
/// # Backoff schedule
///
/// Delay between attempt *n* and attempt *n+1*:
/// ```text
/// min(initial_delay × 2^(n-1), max_delay)  [+ optional jitter up to 50%]
/// ```
/// With `initial_delay = 200ms` and `max_delay = 10s`:
/// 1→2: 200ms, 2→3: 400ms, 3→4: 800ms, …, capped at 10s.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Total attempts including the first. `1` = no retries (default).
    pub max_attempts: u32,
    /// Delay before the second attempt.
    pub initial_delay: Duration,
    /// Upper bound on computed delay (before jitter).
    pub max_delay: Duration,
    /// Add up to 50% random jitter using nanosecond wall-clock entropy.
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts:  1,
            initial_delay: Duration::from_millis(200),
            max_delay:     Duration::from_secs(10),
            jitter:        false,
        }
    }
}

// ── Transient error classification ────────────────────────────────────────────

pub(crate) fn is_retryable(err: &Error) -> bool {
    match err {
        Error::Transport(_) => true,
        Error::Status(s)    => retryable_code(s.code()),
        Error::Flight(fe)   => match fe {
            FlightError::Tonic(s) => retryable_code(s.code()),
            _                     => false,
        },
        _ => false,
    }
}

fn retryable_code(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Unknown
            | Code::Aborted
    )
}

// ── Retry helper ─────────────────────────────────────────────────────────────

/// Run `f` up to `config.max_attempts` times with exponential backoff.
/// Non-retryable errors are returned immediately.
pub(crate) async fn with_retry<F, Fut, T>(config: &RetryConfig, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let max = config.max_attempts.max(1);
    let mut attempt = 0u32;

    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                attempt += 1;
                if attempt >= max || !is_retryable(&e) {
                    return Err(e);
                }
                tokio::time::sleep(backoff_delay(config, attempt)).await;
            }
        }
    }
}

fn backoff_delay(config: &RetryConfig, attempt: u32) -> Duration {
    let factor    = 1u64.checked_shl(attempt.saturating_sub(1)).unwrap_or(u64::MAX);
    let base_ms   = config.initial_delay.as_millis() as u64;
    let max_ms    = config.max_delay.as_millis() as u64;
    let delay_ms  = base_ms.saturating_mul(factor).min(max_ms);
    let jitter_ms = if config.jitter { pseudo_jitter(delay_ms / 2) } else { 0 };
    Duration::from_millis(delay_ms + jitter_ms)
}

fn pseudo_jitter(max_ms: u64) -> u64 {
    if max_ms == 0 { return 0; }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    nanos % max_ms
}
