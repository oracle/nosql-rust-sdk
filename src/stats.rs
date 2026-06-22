//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

//! Public configuration and control types for client-side request statistics.
//!
//! The stats runtime mirrors the Java and Python SDK profile model. A
//! [`HandleBuilder`](crate::HandleBuilder) configures the initial profile,
//! interval, pretty-print preference, logging preference, and optional handler.
//! [`StatsControl`] can then inspect or update the runtime profile and start or
//! stop the collection gate for a [`Handle`](crate::Handle). Request and query
//! observations are collected internally; periodic log and snapshot emission use
//! the same public configuration surface.

use crate::error::{NoSQLError, NoSQLErrorCode};
use crate::handle_builder::HandleBuilder;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::info;

static NEXT_STATS_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

// Preserve Java SDK request ordering in emitted stats JSON so output from the
// Rust SDK is easy to compare against Java logs and tests.
const JAVA_REQUEST_OUTPUT_ORDER: &[&str] = &[
    "Put",
    "WriteMultiple",
    "Get",
    "Prepare",
    "MultiDelete",
    "GetTable",
    "Delete",
    "Table",
    "ListTables",
    "GetIndexes",
    "Query",
    "System",
    "SystemStatus",
    "TableUsage",
    "Write",
];

#[cfg(test)]
#[path = "stats_functional_tests.rs"]
mod functional_tests;

/// Statistics collection detail level.
///
/// The variants mirror the Java and Python SDK profiles:
///
/// - [`StatsProfile::None`] disables statistics collection.
/// - [`StatsProfile::Regular`] collects per-request counters and basic timing.
/// - [`StatsProfile::More`] adds percentile-oriented timing data.
/// - [`StatsProfile::All`] adds query details where available.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsProfile {
    /// Disable statistics collection.
    #[default]
    None,
    /// Collect regular request statistics.
    Regular,
    /// Collect regular request statistics plus percentile data.
    More,
    /// Collect all available request statistics, including query details.
    All,
}

impl StatsProfile {
    /// Java-compatible alias for [`StatsProfile::None`].
    pub const NONE: StatsProfile = StatsProfile::None;
    /// Java-compatible alias for [`StatsProfile::Regular`].
    pub const REGULAR: StatsProfile = StatsProfile::Regular;
    /// Java-compatible alias for [`StatsProfile::More`].
    pub const MORE: StatsProfile = StatsProfile::More;
    /// Java-compatible alias for [`StatsProfile::All`].
    pub const ALL: StatsProfile = StatsProfile::All;

    /// Return the Java/Python profile name used in configuration and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            StatsProfile::None => "NONE",
            StatsProfile::Regular => "REGULAR",
            StatsProfile::More => "MORE",
            StatsProfile::All => "ALL",
        }
    }

    fn includes_percentiles(self) -> bool {
        matches!(self, StatsProfile::More | StatsProfile::All)
    }
}

impl fmt::Display for StatsProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StatsProfile {
    type Err = NoSQLError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_uppercase().as_str() {
            "NONE" => Ok(StatsProfile::None),
            "REGULAR" => Ok(StatsProfile::Regular),
            "MORE" => Ok(StatsProfile::More),
            "ALL" => Ok(StatsProfile::All),
            _ => Err(NoSQLError::new(
                NoSQLErrorCode::IllegalArgument,
                &format!(
                    "invalid stats profile '{}'. expected one of: NONE, REGULAR, MORE, ALL",
                    s
                ),
            )),
        }
    }
}

/// Percentile calculation strategy for [`StatsProfile::More`] and
/// [`StatsProfile::All`].
///
/// [`StatsPercentileMode::Exact`] stores latency samples and sorts them at
/// interval emission time. This is closest to the Java SDK implementation.
/// [`StatsPercentileMode::Hdr`] stores latency values in a bounded
/// HDR-style histogram for lower memory use and emission cost under high
/// request volume.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsPercentileMode {
    /// Store exact latency samples and sort them when the interval is emitted.
    #[default]
    Exact,
    /// Store latency values in a bounded HDR-style histogram.
    Hdr,
}

impl StatsPercentileMode {
    /// Java-compatible exact sample mode.
    pub const EXACT: StatsPercentileMode = StatsPercentileMode::Exact;
    /// Bounded-memory HDR histogram mode.
    pub const HDR: StatsPercentileMode = StatsPercentileMode::Hdr;

    /// Return the configuration name used by environment variables and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            StatsPercentileMode::Exact => "EXACT",
            StatsPercentileMode::Hdr => "HDR",
        }
    }
}

impl fmt::Display for StatsPercentileMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StatsPercentileMode {
    type Err = NoSQLError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_uppercase().as_str() {
            "EXACT" | "SAMPLE" | "SAMPLES" => Ok(StatsPercentileMode::Exact),
            "HDR" | "HISTOGRAM" => Ok(StatsPercentileMode::Hdr),
            _ => Err(NoSQLError::new(
                NoSQLErrorCode::IllegalArgument,
                &format!(
                    "invalid stats percentile mode '{}'. expected one of: EXACT, HDR",
                    s
                ),
            )),
        }
    }
}

/// Snapshot delivered to a configured [`StatsHandler`].
///
/// [`StatsSnapshot::as_json`] returns the Java-compatible stats payload. The
/// payload uses Java SDK field names and profile behavior so it can be compared
/// directly with Java SDK stats logs for the same workload.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    json: String,
}

impl StatsSnapshot {
    fn new(json: String) -> Self {
        StatsSnapshot { json }
    }

    /// Returns the JSON representation of this statistics snapshot.
    pub fn as_json(&self) -> &str {
        &self.json
    }
}

/// Application callback invoked when a statistics snapshot is produced.
///
/// Handlers must be [`Send`] and [`Sync`] because snapshots are produced from a
/// background task while the handle may be used concurrently. Handler
/// implementations should avoid blocking for long periods; copy or enqueue the
/// snapshot if expensive processing is needed.
pub trait StatsHandler: Send + Sync + 'static {
    /// Accept a statistics snapshot.
    fn accept(&self, stats: &StatsSnapshot);
}

impl<F> StatsHandler for F
where
    F: Fn(&StatsSnapshot) + Send + Sync + 'static,
{
    fn accept(&self, stats: &StatsSnapshot) {
        self(stats);
    }
}

#[derive(Clone, Default)]
pub(crate) struct StatsHandlerRef(pub(crate) Option<Arc<dyn StatsHandler>>);

impl StatsHandlerRef {
    pub(crate) fn new<H>(handler: H) -> Self
    where
        H: StatsHandler,
    {
        StatsHandlerRef(Some(Arc::new(handler)))
    }
}

impl fmt::Debug for StatsHandlerRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatsHandlerRef")
            .field("configured", &self.0.is_some())
            .finish()
    }
}

/// Runtime control for client-side request statistics.
///
/// This control owns the shared stats runtime configuration for a
/// [`Handle`](crate::Handle). It can be cloned and used from multiple threads;
/// all mutable runtime settings and accumulated observations are protected by an
/// internal lock. Emitted snapshots follow the Java SDK stats schema and profile
/// behavior.
#[derive(Clone)]
pub struct StatsControl {
    inner: Arc<StatsControlRef>,
}

struct StatsControlRef {
    id: String,
    interval: Duration,
    enable_log: bool,
    percentile_mode: StatsPercentileMode,
    request_collection_enabled: AtomicBool,
    query_collection_enabled: AtomicBool,
    // Runtime configuration and the current interval's counters live together
    // so profile/start-stop changes and request observations are serialized.
    state: RwLock<StatsControlState>,
}

impl StatsControlRef {
    fn update_collection_flags(&self, started: bool, profile: StatsProfile) {
        self.request_collection_enabled
            .store(started && profile != StatsProfile::None, Ordering::Release);
        self.query_collection_enabled
            .store(started && profile == StatsProfile::All, Ordering::Release);
    }
}

struct StatsControlState {
    profile: StatsProfile,
    pretty_print: bool,
    handler: Option<Arc<dyn StatsHandler>>,
    started: bool,
    interval_start: DateTime<Utc>,
    request_stats: HashMap<&'static str, RequestLifecycleStats>,
    query_stats: HashMap<String, QueryEntryStats>,
    connection_stats: ConnectionStats,
}

type SnapshotEmission = (StatsSnapshot, Option<Arc<dyn StatsHandler>>, bool);

impl StatsControl {
    pub(crate) fn new(builder: &HandleBuilder) -> Self {
        let profile = builder.stats_profile;
        let started = profile != StatsProfile::None;
        StatsControl {
            inner: Arc::new(StatsControlRef {
                id: format!(
                    "rust-{}",
                    NEXT_STATS_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
                ),
                interval: builder
                    .stats_interval
                    .unwrap_or_else(|| Duration::from_secs(600)),
                enable_log: builder.stats_enable_log.unwrap_or(true),
                percentile_mode: builder.stats_percentile_mode,
                request_collection_enabled: AtomicBool::new(started),
                query_collection_enabled: AtomicBool::new(started && profile == StatsProfile::All),
                state: RwLock::new(StatsControlState {
                    profile,
                    pretty_print: builder.stats_pretty_print,
                    handler: builder.stats_handler.0.clone(),
                    started,
                    interval_start: Utc::now(),
                    request_stats: HashMap::new(),
                    query_stats: HashMap::new(),
                    connection_stats: ConnectionStats::default(),
                }),
            }),
        }
    }

    pub(crate) fn start_log_task(&self) {
        Self::log_settings(&self.inner);

        let weak = Arc::downgrade(&self.inner);
        let interval = self.inner.interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                Self::emit_interval(&inner);
            }
        });
    }

    /// Returns the current statistics collection interval.
    pub fn get_interval(&self) -> Duration {
        self.inner.interval
    }

    /// Returns the percentile calculation mode used by `MORE` and `ALL`.
    pub fn get_percentile_mode(&self) -> StatsPercentileMode {
        self.inner.percentile_mode
    }

    /// Returns the current statistics collection profile.
    pub fn get_profile(&self) -> StatsProfile {
        self.inner.state.read().unwrap().profile
    }

    /// Sets the statistics collection profile.
    ///
    /// This updates the profile only. It does not start collection.
    pub fn set_profile(&self, profile: StatsProfile) -> StatsControl {
        let mut state = self.inner.state.write().unwrap();
        state.profile = profile;
        self.inner
            .update_collection_flags(state.started, state.profile);
        self.clone()
    }

    /// Returns whether future statistics log output should be pretty-printed.
    pub fn get_pretty_print(&self) -> bool {
        self.inner.state.read().unwrap().pretty_print
    }

    /// Configures whether future statistics log output should be pretty-printed.
    pub fn set_pretty_print(&self, pretty_print: bool) -> StatsControl {
        self.inner.state.write().unwrap().pretty_print = pretty_print;
        self.clone()
    }

    /// Returns the registered statistics handler, if any.
    pub fn get_stats_handler(&self) -> Option<Arc<dyn StatsHandler>> {
        self.inner.state.read().unwrap().handler.clone()
    }

    /// Registers an application callback for future statistics snapshots.
    pub fn set_stats_handler<H>(&self, handler: H) -> StatsControl
    where
        H: StatsHandler,
    {
        self.inner.state.write().unwrap().handler = Some(Arc::new(handler));
        self.clone()
    }

    /// Enables the stats collection gate.
    ///
    /// This mirrors Java's `StatsControl.start()`: it allows future requests
    /// to be counted, but the active profile still controls what is emitted.
    pub fn start(&self) {
        let mut state = self.inner.state.write().unwrap();
        state.started = true;
        self.inner
            .update_collection_flags(state.started, state.profile);
    }

    /// Disables the stats collection gate.
    ///
    /// Like the Java SDK, this stops collecting new observations but does not
    /// disable the periodic task. Non-`None` profiles can still emit empty
    /// interval snapshots while stopped.
    pub fn stop(&self) {
        let mut state = self.inner.state.write().unwrap();
        state.started = false;
        self.inner
            .update_collection_flags(state.started, state.profile);
    }

    /// Returns whether the stats collection gate is enabled.
    pub fn is_started(&self) -> bool {
        self.inner.state.read().unwrap().started
    }

    pub(crate) fn observe(&self, observation: StatsObservation) {
        if !self
            .inner
            .request_collection_enabled
            .load(Ordering::Acquire)
        {
            return;
        }
        let mut state = self.inner.state.write().unwrap();
        if !state.started || state.profile == StatsProfile::None {
            return;
        }
        let query_metadata = observation.metadata.query.clone();
        // Every HTTP request contributes to the aggregate request entry. Query
        // requests may also update a logical query entry when profile is ALL.
        let percentile_mode = state
            .profile
            .includes_percentiles()
            .then_some(self.inner.percentile_mode);
        state
            .request_stats
            .entry(observation.metadata.request_name)
            .or_default()
            .observe(observation.clone(), percentile_mode);
        /*
         * The Java driver reports the number of simultaneously open HTTP
         * connections. reqwest/hyper does not expose the connection pool count,
         * so the Rust SDK records the closest compatible signal: a live handle
         * that observed HTTP traffic has one active client-side connection for
         * the interval. This keeps non-empty local proxy intervals aligned with
         * Java without inventing a Rust-only JSON field.
         */
        state.connection_stats.observe(1);

        if state.profile == StatsProfile::All {
            if let Some(query_metadata) = query_metadata {
                let entry = state
                    .query_stats
                    .entry(query_metadata.query.clone())
                    .or_insert_with(|| QueryEntryStats::new(&query_metadata));
                entry.observe_request(observation, percentile_mode);
            }
        }
    }

    pub(crate) fn observe_query(&self, metadata: QueryStatsMetadata) {
        if !self.inner.query_collection_enabled.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.inner.state.write().unwrap();
        if !state.started || state.profile != StatsProfile::All {
            return;
        }
        let entry = state
            .query_stats
            .entry(metadata.query.clone())
            .or_insert_with(|| QueryEntryStats::new(&metadata));
        entry.observe_logical(&metadata);
    }

    pub(crate) fn observe_query_metadata(&self, metadata: QueryStatsMetadata) {
        if !self.inner.query_collection_enabled.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.inner.state.write().unwrap();
        if !state.started || state.profile != StatsProfile::All {
            return;
        }
        let entry = state
            .query_stats
            .entry(metadata.query.clone())
            .or_insert_with(|| QueryEntryStats::new(&metadata));
        entry.update_response_metadata(&metadata);
    }

    fn log_settings(inner: &Arc<StatsControlRef>) {
        if !inner.enable_log {
            return;
        }
        let state = inner.state.read().unwrap();
        if !state.started || state.profile == StatsProfile::None {
            return;
        }
        let settings = ordered_object([
            ("sdkName", json!("Oracle NoSQL Rust SDK")),
            ("sdkVersion", json!(env!("CARGO_PKG_VERSION"))),
            ("clientId", json!(inner.id.as_str())),
            ("profile", json!(state.profile.as_str())),
            ("intervalSec", json!(inner.interval.as_secs())),
            ("prettyPrint", json!(state.pretty_print)),
            ("percentileMode", json!(inner.percentile_mode.as_str())),
            ("rateLimitingEnabled", json!(false)),
        ]);
        let text = stringify_json(&settings, state.pretty_print);
        info!("Client stats|{}", text);
    }

    fn emit_interval(inner: &Arc<StatsControlRef>) -> Option<StatsSnapshot> {
        let (snapshot, handler, enable_log) = Self::snapshot_and_reset(inner)?;
        if let Some(handler) = handler {
            handler.accept(&snapshot);
        }
        if enable_log {
            info!("Client stats|{}", snapshot.as_json());
        }
        Some(snapshot)
    }

    fn snapshot_and_reset(inner: &Arc<StatsControlRef>) -> Option<SnapshotEmission> {
        let end_time = Utc::now();
        let mut state = inner.state.write().unwrap();
        if state.profile == StatsProfile::None {
            return None;
        }

        // Snapshot emission is intentionally independent of `started`. Java's
        // stop() suppresses collection only; the scheduler continues to emit
        // empty intervals until the profile is NONE or the handle is dropped.
        let profile = state.profile;
        let pretty_print = state.pretty_print;
        let handler = state.handler.clone();
        let interval_start = state.interval_start;
        state.interval_start = end_time;
        let request_stats = std::mem::take(&mut state.request_stats);
        let query_stats = if profile == StatsProfile::All {
            std::mem::take(&mut state.query_stats)
        } else {
            state.query_stats.clear();
            HashMap::new()
        };
        let connection_stats = std::mem::take(&mut state.connection_stats);
        drop(state);

        let json_value = interval_snapshot_json(
            &inner.id,
            profile,
            interval_start,
            end_time,
            request_stats,
            query_stats,
            connection_stats,
        );
        let snapshot = StatsSnapshot::new(stringify_json(&json_value, pretty_print));
        Some((snapshot, handler, inner.enable_log))
    }

    #[cfg(test)]
    pub(crate) fn emit_interval_for_test(&self) -> Option<StatsSnapshot> {
        Self::emit_interval(&self.inner)
    }
}

impl fmt::Debug for StatsControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.inner.state.read().unwrap();
        f.debug_struct("StatsControl")
            .field("interval", &self.inner.interval)
            .field("enable_log", &self.inner.enable_log)
            .field("percentile_mode", &self.inner.percentile_mode)
            .field("profile", &state.profile)
            .field("pretty_print", &state.pretty_print)
            .field("handler_configured", &state.handler.is_some())
            .field("started", &state.started)
            .field("request_stats_count", &state.request_stats.len())
            .field("query_stats_count", &state.query_stats.len())
            .field("connection_stats_count", &state.connection_stats.count)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatsRequestMetadata {
    pub(crate) request_name: &'static str,
    pub(crate) retryable: bool,
    pub(crate) has_compartment_id: bool,
    pub(crate) has_namespace: bool,
    pub(crate) query: Option<QueryStatsMetadata>,
}

impl StatsRequestMetadata {
    pub(crate) fn new(
        request_name: &'static str,
        retryable: bool,
        has_compartment_id: bool,
        has_namespace: bool,
    ) -> Self {
        StatsRequestMetadata {
            request_name,
            retryable,
            has_compartment_id,
            has_namespace,
            query: None,
        }
    }

    pub(crate) fn with_query(mut self, query: Option<QueryStatsMetadata>) -> Self {
        self.query = query;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatsObservation {
    pub(crate) metadata: StatsRequestMetadata,
    pub(crate) request_size: usize,
    pub(crate) response_size: usize,
    pub(crate) latency: Duration,
    pub(crate) retry_count: u16,
    pub(crate) retry_delay_ms: u64,
    pub(crate) auth_retry_count: u16,
    pub(crate) throttle_retry_count: u16,
    pub(crate) rate_limit_delay_ms: u64,
    pub(crate) error_code: Option<NoSQLErrorCode>,
}

impl StatsObservation {
    pub(crate) fn success(
        metadata: StatsRequestMetadata,
        request_size: usize,
        response_size: usize,
        latency: Duration,
        retry_count: u16,
        auth_retry_count: u16,
    ) -> Self {
        StatsObservation {
            metadata,
            request_size,
            response_size,
            latency,
            retry_count,
            retry_delay_ms: 0,
            auth_retry_count,
            throttle_retry_count: 0,
            rate_limit_delay_ms: 0,
            error_code: None,
        }
    }

    pub(crate) fn error(
        metadata: StatsRequestMetadata,
        request_size: usize,
        response_size: usize,
        latency: Duration,
        retry_count: u16,
        auth_retry_count: u16,
        error_code: NoSQLErrorCode,
    ) -> Self {
        StatsObservation {
            metadata,
            request_size,
            response_size,
            latency,
            retry_count,
            retry_delay_ms: 0,
            auth_retry_count,
            throttle_retry_count: 0,
            rate_limit_delay_ms: 0,
            error_code: Some(error_code),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_retry_delay_ms(mut self, retry_delay_ms: u64) -> Self {
        self.retry_delay_ms = retry_delay_ms;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_throttle_retry_count(mut self, throttle_retry_count: u16) -> Self {
        self.throttle_retry_count = throttle_retry_count;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_rate_limit_delay_ms(mut self, rate_limit_delay_ms: u64) -> Self {
        self.rate_limit_delay_ms = rate_limit_delay_ms;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RequestLifecycleStats {
    pub(crate) success_count: u64,
    pub(crate) error_count: u64,
    pub(crate) latency: DurationSummary,
    pub(crate) request_size: SizeSummary,
    pub(crate) response_size: SizeSummary,
    pub(crate) retry_count: u64,
    pub(crate) retry_delay_ms: u64,
    pub(crate) auth_retry_count: u64,
    pub(crate) throttle_retry_count: u64,
    pub(crate) rate_limit_delay_ms: u64,
    pub(crate) last_error_code: Option<NoSQLErrorCode>,
    pub(crate) last_metadata: Option<StatsRequestMetadata>,
}

impl RequestLifecycleStats {
    fn http_request_count(&self) -> u64 {
        self.success_count + self.error_count
    }

    fn observe(
        &mut self,
        observation: StatsObservation,
        percentile_mode: Option<StatsPercentileMode>,
    ) {
        if let Some(code) = observation.error_code {
            // Java-compatible stats count final failed requests, but latency
            // and payload summaries are based only on successful responses.
            self.error_count += 1;
            self.last_error_code = Some(code);
        } else {
            self.success_count += 1;
            self.observe_success_values(&observation, percentile_mode);
        }
        self.retry_count += u64::from(observation.retry_count);
        self.retry_delay_ms += observation.retry_delay_ms;
        self.auth_retry_count += u64::from(observation.auth_retry_count);
        self.throttle_retry_count += u64::from(observation.throttle_retry_count);
        self.rate_limit_delay_ms += observation.rate_limit_delay_ms;
        self.last_metadata = Some(observation.metadata);
    }

    fn observe_query_request(
        &mut self,
        observation: StatsObservation,
        percentile_mode: Option<StatsPercentileMode>,
    ) {
        if let Some(code) = observation.error_code {
            self.error_count += 1;
            self.last_error_code = Some(code);
        } else {
            self.success_count += 1;
            self.observe_success_values(&observation, percentile_mode);
        }
        self.retry_count += u64::from(observation.retry_count);
        self.retry_delay_ms += observation.retry_delay_ms;
        self.auth_retry_count += u64::from(observation.auth_retry_count);
        self.throttle_retry_count += u64::from(observation.throttle_retry_count);
        self.rate_limit_delay_ms += observation.rate_limit_delay_ms;
        self.last_metadata = Some(observation.metadata);
    }

    fn observe_success_values(
        &mut self,
        observation: &StatsObservation,
        percentile_mode: Option<StatsPercentileMode>,
    ) {
        self.latency.observe(observation.latency, percentile_mode);
        self.request_size.observe(observation.request_size);
        self.response_size.observe(observation.response_size);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryStatsMetadata {
    pub(crate) query: String,
    pub(crate) unprepared: bool,
    pub(crate) simple: bool,
    pub(crate) does_writes: bool,
    pub(crate) plan: Option<String>,
}

impl QueryStatsMetadata {
    pub(crate) fn new(
        query: String,
        unprepared: bool,
        simple: bool,
        does_writes: bool,
        plan: Option<String>,
    ) -> Self {
        QueryStatsMetadata {
            query,
            unprepared,
            simple,
            does_writes,
            plan,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct QueryEntryStats {
    pub(crate) count: u64,
    pub(crate) unprepared: u64,
    pub(crate) simple: bool,
    pub(crate) does_writes: bool,
    pub(crate) plan: Option<String>,
    pub(crate) request_stats: RequestLifecycleStats,
}

impl QueryEntryStats {
    fn new(metadata: &QueryStatsMetadata) -> Self {
        let mut entry = QueryEntryStats::default();
        entry.update_response_metadata(metadata);
        entry
    }

    fn observe_logical(&mut self, metadata: &QueryStatsMetadata) {
        // Java query stats distinguish logical query executions (`count`) from
        // the HTTP request count accumulated below in `request_stats`.
        self.count += 1;
        if metadata.unprepared {
            self.unprepared += 1;
        } else if metadata.simple {
            self.simple = true;
        }
        self.update_response_metadata(metadata);
    }

    fn observe_request(
        &mut self,
        observation: StatsObservation,
        percentile_mode: Option<StatsPercentileMode>,
    ) {
        self.request_stats
            .observe_query_request(observation, percentile_mode);
    }

    fn update_response_metadata(&mut self, metadata: &QueryStatsMetadata) {
        if metadata.does_writes {
            self.does_writes = true;
        }
        if self.plan.is_none() && metadata.plan.is_some() {
            self.plan = metadata.plan.clone();
        }
    }
}

fn interval_snapshot_json(
    client_id: &str,
    profile: StatsProfile,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    request_stats: HashMap<&'static str, RequestLifecycleStats>,
    query_stats: HashMap<String, QueryEntryStats>,
    connection_stats: ConnectionStats,
) -> Value {
    // Keep request and top-level JSON field order aligned with the Java SDK.
    // Tests assert this because the stats output is often inspected in logs.
    let mut request_entries: Vec<(&'static str, RequestLifecycleStats)> =
        request_stats.into_iter().collect();
    request_entries.sort_by_key(|(name, _)| request_output_rank(name));
    let requests: Vec<Value> = request_entries
        .into_iter()
        .map(|(name, stats)| request_stats_json(name, &stats, profile))
        .collect();

    let mut query_entries: Vec<(String, QueryEntryStats)> = query_stats.into_iter().collect();
    let query_capacity = query_entries_capacity(query_entries.len());
    // Java stores query entries in a HashMap, so duplicate its bucket ordering
    // to keep ALL-profile query output comparable for the same workload.
    query_entries.sort_by(|(left, _), (right, _)| {
        java_hashmap_string_order(left, query_capacity)
            .cmp(&java_hashmap_string_order(right, query_capacity))
            .then_with(|| left.cmp(right))
    });
    let queries: Vec<Value> = query_entries
        .into_iter()
        .map(|(query, stats)| query_stats_json(&query, &stats, profile))
        .collect();

    let mut snapshot = Map::new();
    snapshot.insert("clientId".to_string(), json!(client_id));
    snapshot.insert(
        "startTime".to_string(),
        json!(format_stats_time(start_time)),
    );
    snapshot.insert("endTime".to_string(), json!(format_stats_time(end_time)));
    snapshot.insert("requests".to_string(), Value::Array(requests));

    if profile == StatsProfile::All && !queries.is_empty() {
        snapshot.insert("queries".to_string(), Value::Array(queries));
    }
    if !connection_stats.is_empty() {
        snapshot.insert(
            "connections".to_string(),
            connection_stats_json(&connection_stats),
        );
    }

    Value::Object(snapshot)
}

fn request_stats_json(name: &str, stats: &RequestLifecycleStats, profile: StatsProfile) -> Value {
    // Field names and insertion order intentionally match Java's periodic
    // stats payload. Do not add Rust-only metric fields here.
    let mut request = Map::new();
    request.insert(
        "httpRequestCount".to_string(),
        json!(stats.http_request_count()),
    );
    if stats.response_size.max.unwrap_or(0) > 0 {
        request.insert(
            "resultSize".to_string(),
            size_summary_json(&stats.response_size),
        );
    }
    request.insert("name".to_string(), json!(name));
    // Emit latency when samples exist, even if every local request rounded to
    // 0ms. Java-style MORE/ALL output should still include percentile fields.
    if stats.latency.count > 0 {
        request.insert(
            "httpRequestLatencyMs".to_string(),
            duration_summary_json(&stats.latency, profile),
        );
    }
    if stats.request_size.max.unwrap_or(0) > 0 {
        request.insert(
            "requestSize".to_string(),
            size_summary_json(&stats.request_size),
        );
    }
    request.insert(
        "rateLimitDelayMs".to_string(),
        json!(stats.rate_limit_delay_ms),
    );
    request.insert("errors".to_string(), json!(stats.error_count));
    request.insert(
        "retry".to_string(),
        retry_stats_json(
            stats.retry_count,
            stats.retry_delay_ms,
            stats.auth_retry_count,
            stats.throttle_retry_count,
        ),
    );

    Value::Object(request)
}

fn query_stats_json(query: &str, stats: &QueryEntryStats, profile: StatsProfile) -> Value {
    // Query entries use the same nested metric and retry shapes as request
    // entries so ALL-profile output matches Java's contract.
    let request_stats = &stats.request_stats;
    let mut entry = Map::new();
    entry.insert("doesWrites".to_string(), json!(stats.does_writes));
    entry.insert("unprepared".to_string(), json!(stats.unprepared));
    entry.insert(
        "httpRequestCount".to_string(),
        json!(request_stats.http_request_count()),
    );
    entry.insert("query".to_string(), json!(query));
    if request_stats.response_size.max.unwrap_or(0) > 0 {
        entry.insert(
            "resultSize".to_string(),
            size_summary_json(&request_stats.response_size),
        );
    }
    entry.insert("count".to_string(), json!(stats.count));
    entry.insert("simple".to_string(), json!(stats.simple));
    // Query entries follow the same zero-ms rule as aggregate request entries.
    if request_stats.latency.count > 0 {
        entry.insert(
            "httpRequestLatencyMs".to_string(),
            duration_summary_json(&request_stats.latency, profile),
        );
    }
    if request_stats.request_size.max.unwrap_or(0) > 0 {
        entry.insert(
            "requestSize".to_string(),
            size_summary_json(&request_stats.request_size),
        );
    }
    if let Some(plan) = &stats.plan {
        entry.insert("plan".to_string(), json!(plan));
    }
    entry.insert(
        "rateLimitDelayMs".to_string(),
        json!(request_stats.rate_limit_delay_ms),
    );
    entry.insert("errors".to_string(), json!(request_stats.error_count));
    entry.insert(
        "retry".to_string(),
        retry_stats_json(
            request_stats.retry_count,
            request_stats.retry_delay_ms,
            request_stats.auth_retry_count,
            request_stats.throttle_retry_count,
        ),
    );

    Value::Object(entry)
}

fn duration_summary_json(summary: &DurationSummary, profile: StatsProfile) -> Value {
    // Java emits min/avg/max for REGULAR and adds percentile fields for
    // MORE/ALL. Values are reported in whole milliseconds.
    let mut latency = Map::new();
    latency.insert("min".to_string(), json!(summary.min_ms));
    latency.insert("avg".to_string(), json!(summary.avg_ms()));
    latency.insert("max".to_string(), json!(summary.max_ms));

    if profile.includes_percentiles() {
        latency.insert("95th".to_string(), json!(summary.percentile_ms(0.95)));
        latency.insert("99th".to_string(), json!(summary.percentile_ms(0.99)));
    }

    Value::Object(latency)
}

fn size_summary_json(summary: &SizeSummary) -> Value {
    ordered_object([
        ("min", json!(summary.min.unwrap_or(0))),
        ("avg", json!(summary.avg())),
        ("max", json!(summary.max.unwrap_or(0))),
    ])
}

fn connection_stats_json(stats: &ConnectionStats) -> Value {
    ordered_object([
        ("min", json!(stats.min)),
        ("avg", json!(stats.sum as f64 / stats.count as f64)),
        ("max", json!(stats.max)),
    ])
}

fn retry_stats_json(count: u64, delay_ms: u64, auth_count: u64, throttle_count: u64) -> Value {
    ordered_object([
        ("delayMs", json!(delay_ms)),
        ("authCount", json!(auth_count)),
        ("throttleCount", json!(throttle_count)),
        ("count", json!(count)),
    ])
}

fn ordered_object<const N: usize>(fields: [(&str, Value); N]) -> Value {
    let mut object = Map::with_capacity(N);
    for (key, value) in fields {
        object.insert(key.to_string(), value);
    }
    Value::Object(object)
}

fn request_output_rank(name: &str) -> usize {
    JAVA_REQUEST_OUTPUT_ORDER
        .iter()
        .position(|request_name| *request_name == name)
        .unwrap_or(JAVA_REQUEST_OUTPUT_ORDER.len())
}

fn query_entries_capacity(len: usize) -> usize {
    if len <= 12 {
        16
    } else {
        ((len * 4 / 3) + 1).next_power_of_two()
    }
}

fn java_hashmap_string_order(value: &str, capacity: usize) -> usize {
    let hash = java_string_hash(value);
    let spread = hash ^ ((hash as u32 >> 16) as i32);
    (spread as usize) & (capacity - 1)
}

fn java_string_hash(value: &str) -> i32 {
    let mut hash = 0_i32;
    for unit in value.encode_utf16() {
        hash = hash.wrapping_mul(31).wrapping_add(i32::from(unit));
    }
    hash
}

fn stringify_json(value: &Value, pretty_print: bool) -> String {
    // Stats JSON has Java-compatible field ordering, but the SDK should not
    // force serde_json's global preserve_order feature on downstream builds.
    let mut output = String::new();
    write_stats_json_value(value, pretty_print, 0, &mut output);
    output
}

fn write_stats_json_value(value: &Value, pretty_print: bool, indent: usize, output: &mut String) {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => output.push_str(&value.to_string()),
        Value::String(value) => output.push_str(&serde_json::to_string(value).unwrap()),
        Value::Array(values) => write_stats_json_array(values, pretty_print, indent, output),
        Value::Object(object) => write_stats_json_object(object, pretty_print, indent, output),
    }
}

fn write_stats_json_array(
    values: &[Value],
    pretty_print: bool,
    indent: usize,
    output: &mut String,
) {
    output.push('[');
    if !values.is_empty() {
        if pretty_print {
            output.push('\n');
        }
        for (index, value) in values.iter().enumerate() {
            if pretty_print {
                push_json_indent(output, indent + 2);
            }
            write_stats_json_value(value, pretty_print, indent + 2, output);
            if index + 1 < values.len() {
                output.push(',');
            }
            if pretty_print {
                output.push('\n');
            }
        }
        if pretty_print {
            push_json_indent(output, indent);
        }
    }
    output.push(']');
}

fn write_stats_json_object(
    object: &Map<String, Value>,
    pretty_print: bool,
    indent: usize,
    output: &mut String,
) {
    output.push('{');
    if !object.is_empty() {
        let keys = ordered_stats_json_keys(object);
        if pretty_print {
            output.push('\n');
        }
        for (index, key) in keys.iter().enumerate() {
            if pretty_print {
                push_json_indent(output, indent + 2);
            }
            output.push_str(&serde_json::to_string(key.as_str()).unwrap());
            if pretty_print {
                output.push_str(": ");
            } else {
                output.push(':');
            }
            write_stats_json_value(
                object.get(key.as_str()).unwrap(),
                pretty_print,
                indent + 2,
                output,
            );
            if index + 1 < keys.len() {
                output.push(',');
            }
            if pretty_print {
                output.push('\n');
            }
        }
        if pretty_print {
            push_json_indent(output, indent);
        }
    }
    output.push('}');
}

fn push_json_indent(output: &mut String, indent: usize) {
    for _ in 0..indent {
        output.push(' ');
    }
}

fn ordered_stats_json_keys(object: &Map<String, Value>) -> Vec<&String> {
    let preferred_order = preferred_stats_json_key_order(object);
    let mut keys: Vec<&String> = object.keys().collect();
    keys.sort_by(|left, right| {
        stats_json_key_rank(left.as_str(), preferred_order)
            .cmp(&stats_json_key_rank(right.as_str(), preferred_order))
            .then_with(|| left.cmp(right))
    });
    keys
}

fn preferred_stats_json_key_order(object: &Map<String, Value>) -> &'static [&'static str] {
    if object.contains_key("sdkName") {
        &[
            "sdkName",
            "sdkVersion",
            "clientId",
            "profile",
            "intervalSec",
            "prettyPrint",
            "percentileMode",
            "rateLimitingEnabled",
        ]
    } else if object.contains_key("startTime") && object.contains_key("requests") {
        &[
            "clientId",
            "startTime",
            "endTime",
            "requests",
            "queries",
            "connections",
        ]
    } else if object.contains_key("doesWrites") {
        &[
            "doesWrites",
            "unprepared",
            "httpRequestCount",
            "query",
            "resultSize",
            "count",
            "simple",
            "httpRequestLatencyMs",
            "requestSize",
            "plan",
            "rateLimitDelayMs",
            "errors",
            "retry",
        ]
    } else if object.contains_key("httpRequestCount") && object.contains_key("name") {
        &[
            "httpRequestCount",
            "resultSize",
            "name",
            "httpRequestLatencyMs",
            "requestSize",
            "rateLimitDelayMs",
            "errors",
            "retry",
        ]
    } else if object.contains_key("delayMs") {
        &["delayMs", "authCount", "throttleCount", "count"]
    } else if object.contains_key("95th") || object.contains_key("min") {
        &["min", "avg", "max", "95th", "99th"]
    } else {
        &[]
    }
}

fn stats_json_key_rank(key: &str, preferred_order: &[&str]) -> usize {
    preferred_order
        .iter()
        .position(|ordered_key| *ordered_key == key)
        .unwrap_or(preferred_order.len())
}

fn format_stats_time(time: DateTime<Utc>) -> String {
    // Java stats timestamps are UTC RFC3339 strings with seconds precision.
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DurationSummary {
    pub(crate) count: u64,
    pub(crate) min_ms: u64,
    pub(crate) max_ms: u64,
    pub(crate) total_ms: u64,
    percentile_values: PercentileValues,
}

impl DurationSummary {
    fn observe(&mut self, value: Duration, percentile_mode: Option<StatsPercentileMode>) {
        let value_ms = value.as_millis() as u64;
        self.count += 1;
        if self.count == 1 {
            self.min_ms = value_ms;
            self.max_ms = value_ms;
        } else {
            self.min_ms = self.min_ms.min(value_ms);
            self.max_ms = self.max_ms.max(value_ms);
        }
        self.total_ms += value_ms;
        if let Some(mode) = percentile_mode {
            self.percentile_values.record(value_ms, mode);
        }
    }

    fn percentile_ms(&self, percentile: f64) -> u64 {
        self.percentile_values.percentile_ms(percentile)
    }

    fn avg_ms(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total_ms as f64 / self.count as f64
        }
    }
}

#[derive(Debug, Clone, Default)]
enum PercentileValues {
    #[default]
    None,
    Exact(Vec<u64>),
    Hdr(LatencyHistogram),
}

impl PercentileValues {
    fn record(&mut self, value_ms: u64, mode: StatsPercentileMode) {
        match self {
            PercentileValues::Exact(values) => values.push(value_ms),
            PercentileValues::Hdr(histogram) => histogram.record(value_ms),
            PercentileValues::None => match mode {
                StatsPercentileMode::Exact => {
                    // Exact mode is intentionally sample based for Java parity.
                    *self = PercentileValues::Exact(vec![value_ms]);
                }
                StatsPercentileMode::Hdr => {
                    // HDR mode trades exact percentile values for bounded
                    // per-interval memory under high request volume.
                    let mut histogram = LatencyHistogram::default();
                    histogram.record(value_ms);
                    *self = PercentileValues::Hdr(histogram);
                }
            },
        }
    }

    fn percentile_ms(&self, percentile: f64) -> u64 {
        match self {
            PercentileValues::None => 0,
            PercentileValues::Exact(values) => exact_percentile_ms(values, percentile),
            PercentileValues::Hdr(histogram) => histogram.percentile_ms(percentile),
        }
    }

    #[cfg(test)]
    fn sample_count(&self) -> u64 {
        match self {
            PercentileValues::None => 0,
            PercentileValues::Exact(values) => values.len() as u64,
            PercentileValues::Hdr(histogram) => histogram.count,
        }
    }

    #[cfg(test)]
    fn is_hdr(&self) -> bool {
        matches!(self, PercentileValues::Hdr(_))
    }
}

const LATENCY_HISTOGRAM_BUCKETS_MS: &[u64] = &[
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    12,
    14,
    16,
    18,
    20,
    25,
    30,
    40,
    50,
    75,
    100,
    150,
    200,
    250,
    300,
    400,
    500,
    750,
    1_000,
    1_500,
    2_000,
    3_000,
    5_000,
    7_500,
    10_000,
    15_000,
    30_000,
    60_000,
    120_000,
    300_000,
    600_000,
    900_000,
    1_800_000,
    3_600_000,
    u64::MAX,
];

#[derive(Debug, Clone)]
struct LatencyHistogram {
    buckets: Vec<u64>,
    count: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        LatencyHistogram {
            buckets: vec![0; LATENCY_HISTOGRAM_BUCKETS_MS.len()],
            count: 0,
        }
    }
}

impl LatencyHistogram {
    fn record(&mut self, value_ms: u64) {
        let index = LATENCY_HISTOGRAM_BUCKETS_MS
            .binary_search(&value_ms)
            .unwrap_or_else(|index| index)
            .min(LATENCY_HISTOGRAM_BUCKETS_MS.len() - 1);
        self.buckets[index] += 1;
        self.count += 1;
    }

    fn percentile_ms(&self, percentile: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let rank = ((percentile * self.count as f64).ceil() as u64).max(1);
        let mut cumulative = 0_u64;
        for (index, bucket_count) in self.buckets.iter().enumerate() {
            cumulative += *bucket_count;
            if cumulative >= rank {
                return LATENCY_HISTOGRAM_BUCKETS_MS[index];
            }
        }
        *LATENCY_HISTOGRAM_BUCKETS_MS.last().unwrap()
    }
}

fn exact_percentile_ms(values: &[u64], percentile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }

    let mut values = values.to_vec();
    values.sort();
    let mut index = (percentile * values.len() as f64 - 1.0).round() as isize;
    if index < 0 {
        index = 0;
    }
    let index = usize::try_from(index)
        .unwrap_or(0)
        .min(values.len().saturating_sub(1));
    values[index]
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConnectionStats {
    pub(crate) count: u64,
    min: u32,
    max: u32,
    sum: u64,
}

impl ConnectionStats {
    fn observe(&mut self, connections: u32) {
        if self.count == 0 {
            self.min = connections;
            self.max = connections;
        } else {
            self.min = self.min.min(connections);
            self.max = self.max.max(connections);
        }
        self.sum += u64::from(connections);
        self.count += 1;
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SizeSummary {
    pub(crate) count: u64,
    pub(crate) min: Option<usize>,
    pub(crate) max: Option<usize>,
    pub(crate) total: usize,
}

impl SizeSummary {
    fn observe(&mut self, value: usize) {
        self.count += 1;
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        self.total += value;
    }

    fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total as f64 / self.count as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Handle, HandleMode};
    use std::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn parsed_snapshot(control: &StatsControl) -> Value {
        serde_json::from_str(control.emit_interval_for_test().unwrap().as_json()).unwrap()
    }

    fn request_entry<'a>(snapshot: &'a Value, name: &str) -> &'a Value {
        snapshot["requests"]
            .as_array()
            .unwrap()
            .iter()
            .find(|request| request["name"] == name)
            .unwrap()
    }

    fn assert_java_metric_shape(metric: &Value) {
        assert!(metric.get("min").is_some());
        assert!(metric.get("avg").is_some());
        assert!(metric.get("max").is_some());
        assert!(metric.get("count").is_none());
        assert!(metric.get("total").is_none());
    }

    fn assert_java_timestamp(value: &Value) {
        let timestamp = value.as_str().unwrap();
        assert_eq!(timestamp.len(), "2026-06-09T06:42:45Z".len());
        assert!(timestamp.ends_with('Z'));
        assert!(!timestamp.contains('.'));
        DateTime::parse_from_rfc3339(timestamp).unwrap();
    }

    fn assert_key_sequence(json: &str, keys: &[&str]) {
        let mut offset = 0;
        for key in keys {
            let needle = format!("\"{key}\":");
            let Some(index) = json[offset..].find(&needle) else {
                panic!("key {key} not found after byte offset {offset} in {json}");
            };
            offset += index + needle.len();
        }
    }

    #[test]
    fn stats_profile_display_parse_and_aliases_match_java_names() -> Result<(), NoSQLError> {
        assert_eq!(StatsProfile::NONE, StatsProfile::None);
        assert_eq!(StatsProfile::REGULAR, StatsProfile::Regular);
        assert_eq!(StatsProfile::MORE, StatsProfile::More);
        assert_eq!(StatsProfile::ALL, StatsProfile::All);

        assert_eq!(StatsProfile::None.to_string(), "NONE");
        assert_eq!(StatsProfile::Regular.to_string(), "REGULAR");
        assert_eq!(StatsProfile::More.to_string(), "MORE");
        assert_eq!(StatsProfile::All.to_string(), "ALL");

        assert_eq!("none".parse::<StatsProfile>()?, StatsProfile::None);
        assert_eq!("regular".parse::<StatsProfile>()?, StatsProfile::Regular);
        assert_eq!("more".parse::<StatsProfile>()?, StatsProfile::More);
        assert_eq!("all".parse::<StatsProfile>()?, StatsProfile::All);
        assert!("verbose".parse::<StatsProfile>().is_err());
        Ok(())
    }

    #[test]
    fn stats_percentile_mode_display_parse_and_aliases_match_cli_names() -> Result<(), NoSQLError> {
        assert_eq!(StatsPercentileMode::EXACT, StatsPercentileMode::Exact);
        assert_eq!(StatsPercentileMode::HDR, StatsPercentileMode::Hdr);

        assert_eq!(StatsPercentileMode::Exact.to_string(), "EXACT");
        assert_eq!(StatsPercentileMode::Hdr.to_string(), "HDR");

        assert_eq!(
            "exact".parse::<StatsPercentileMode>()?,
            StatsPercentileMode::Exact
        );
        assert_eq!(
            "samples".parse::<StatsPercentileMode>()?,
            StatsPercentileMode::Exact
        );
        assert_eq!(
            "hdr".parse::<StatsPercentileMode>()?,
            StatsPercentileMode::Hdr
        );
        assert_eq!(
            "histogram".parse::<StatsPercentileMode>()?,
            StatsPercentileMode::Hdr
        );
        assert!("tdigest".parse::<StatsPercentileMode>().is_err());
        Ok(())
    }

    #[test]
    fn default_control_uses_java_python_defaults() {
        let builder = HandleBuilder::new();
        let control = StatsControl::new(&builder);

        assert_eq!(control.get_profile(), StatsProfile::None);
        assert_eq!(control.get_interval(), Duration::from_secs(600));
        assert_eq!(control.get_percentile_mode(), StatsPercentileMode::Exact);
        assert!(!control.get_pretty_print());
        assert!(!control.is_started());
        assert!(control.get_stats_handler().is_none());
        assert!(control.inner.enable_log);
    }

    #[test]
    fn builder_values_propagate_to_control() -> Result<(), NoSQLError> {
        let builder = HandleBuilder::new()
            .stats_profile(StatsProfile::Regular)?
            .stats_interval(Duration::from_secs(42))?
            .stats_pretty_print(true)?
            .stats_enable_log(false)?
            .stats_percentile_mode(StatsPercentileMode::Hdr)?
            .stats_handler(|_: &StatsSnapshot| {})?;

        let control = StatsControl::new(&builder);

        assert_eq!(control.get_profile(), StatsProfile::Regular);
        assert_eq!(control.get_interval(), Duration::from_secs(42));
        assert_eq!(control.get_percentile_mode(), StatsPercentileMode::Hdr);
        assert!(control.get_pretty_print());
        assert!(control.is_started());
        assert!(control.get_stats_handler().is_some());
        assert!(!control.inner.enable_log);
        Ok(())
    }

    #[test]
    fn runtime_setters_update_shared_state() {
        let control = StatsControl::new(&HandleBuilder::new());
        let clone = control.clone();

        control.set_profile(StatsProfile::All);
        control.set_pretty_print(true);
        control.set_stats_handler(|_: &StatsSnapshot| {});

        assert_eq!(clone.get_profile(), StatsProfile::All);
        assert!(clone.get_pretty_print());
        assert!(clone.get_stats_handler().is_some());
    }

    #[test]
    fn start_and_stop_toggle_collection_gate() {
        let control = StatsControl::new(&HandleBuilder::new());

        control.start();
        assert!(control.is_started());

        control.stop();
        assert!(!control.is_started());
    }

    #[test]
    fn collection_fast_path_flags_track_profile_and_started() {
        let control = StatsControl::new(&HandleBuilder::new());

        assert!(!control
            .inner
            .request_collection_enabled
            .load(Ordering::Acquire));
        assert!(!control
            .inner
            .query_collection_enabled
            .load(Ordering::Acquire));

        control.set_profile(StatsProfile::Regular);
        assert!(!control
            .inner
            .request_collection_enabled
            .load(Ordering::Acquire));
        assert!(!control
            .inner
            .query_collection_enabled
            .load(Ordering::Acquire));

        control.start();
        assert!(control
            .inner
            .request_collection_enabled
            .load(Ordering::Acquire));
        assert!(!control
            .inner
            .query_collection_enabled
            .load(Ordering::Acquire));

        control.set_profile(StatsProfile::All);
        assert!(control
            .inner
            .request_collection_enabled
            .load(Ordering::Acquire));
        assert!(control
            .inner
            .query_collection_enabled
            .load(Ordering::Acquire));

        control.set_profile(StatsProfile::None);
        assert!(control.is_started());
        assert!(!control
            .inner
            .request_collection_enabled
            .load(Ordering::Acquire));
        assert!(!control
            .inner
            .query_collection_enabled
            .load(Ordering::Acquire));

        control.set_profile(StatsProfile::All);
        assert!(control
            .inner
            .request_collection_enabled
            .load(Ordering::Acquire));
        assert!(control
            .inner
            .query_collection_enabled
            .load(Ordering::Acquire));

        control.stop();
        assert!(!control
            .inner
            .request_collection_enabled
            .load(Ordering::Acquire));
        assert!(!control
            .inner
            .query_collection_enabled
            .load(Ordering::Acquire));
    }

    #[test]
    fn observations_are_skipped_when_stopped_or_profile_none() {
        let control = StatsControl::new(&HandleBuilder::new());
        control.start();

        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Get", true, false, false),
            10,
            20,
            Duration::from_millis(3),
            1,
            0,
        ));

        assert!(control.inner.state.read().unwrap().request_stats.is_empty());

        control.set_profile(StatsProfile::Regular);
        control.stop();
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Get", true, false, false),
            10,
            20,
            Duration::from_millis(3),
            1,
            0,
        ));

        assert!(control.inner.state.read().unwrap().request_stats.is_empty());
    }

    #[test]
    fn observations_accumulate_request_lifecycle_stats() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::Regular)
                .unwrap(),
        );

        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Get", true, true, false),
            100,
            200,
            Duration::from_millis(5),
            2,
            1,
        ));
        control.observe(StatsObservation::error(
            StatsRequestMetadata::new("Get", true, true, false),
            110,
            12,
            Duration::from_millis(8),
            3,
            2,
            NoSQLErrorCode::ServerError,
        ));

        let state = control.inner.state.read().unwrap();
        let stats = state.request_stats.get("Get").unwrap();

        assert_eq!(stats.success_count, 1);
        assert_eq!(stats.error_count, 1);
        assert_eq!(stats.retry_count, 5);
        assert_eq!(stats.auth_retry_count, 3);
        assert_eq!(stats.request_size.min, Some(100));
        assert_eq!(stats.request_size.max, Some(100));
        assert_eq!(stats.request_size.total, 100);
        assert_eq!(stats.response_size.total, 200);
        assert_eq!(stats.latency.count, 1);
        assert_eq!(stats.latency.min_ms, 5);
        assert_eq!(stats.latency.max_ms, 5);
        assert_eq!(stats.last_error_code, Some(NoSQLErrorCode::ServerError));
        assert_eq!(
            stats.last_metadata,
            Some(StatsRequestMetadata::new("Get", true, true, false))
        );
    }

    #[test]
    fn duration_summary_summarizes_raw_latency_samples() {
        let raw_latency_ms = [1_u64, 2, 3, 4, 5, 100];
        let mut summary = DurationSummary::default();

        for latency_ms in raw_latency_ms {
            summary.observe(
                Duration::from_millis(latency_ms),
                Some(StatsPercentileMode::Exact),
            );
        }

        assert_eq!(summary.count, 6);
        assert_eq!(summary.min_ms, 1);
        assert_eq!(summary.max_ms, 100);
        assert_eq!(summary.total_ms, 115);
        assert_eq!(summary.percentile_ms(0.90), 5);
        assert_eq!(summary.percentile_ms(0.95), 100);
        assert_eq!(summary.percentile_ms(0.99), 100);

        let latency_json = duration_summary_json(&summary, StatsProfile::More);
        assert_eq!(latency_json["min"], 1);
        assert_eq!(latency_json["max"], 100);
        assert_eq!(latency_json["95th"], 100);
        assert_eq!(latency_json["99th"], 100);
        let avg_ms = latency_json["avg"].as_f64().unwrap();
        assert!((avg_ms - (115.0 / 6.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn regular_profile_does_not_store_percentile_samples() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::Regular)
                .unwrap(),
        );

        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Get", true, false, false),
            100,
            200,
            Duration::from_millis(5),
            0,
            0,
        ));

        let state = control.inner.state.read().unwrap();
        let stats = state.request_stats.get("Get").unwrap();
        assert_eq!(stats.latency.count, 1);
        assert_eq!(stats.latency.percentile_values.sample_count(), 0);
    }

    #[test]
    fn more_profile_exact_mode_stores_percentile_samples() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::More)
                .unwrap(),
        );

        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Get", true, false, false),
            100,
            200,
            Duration::from_millis(5),
            0,
            0,
        ));

        let state = control.inner.state.read().unwrap();
        let stats = state.request_stats.get("Get").unwrap();
        assert_eq!(stats.latency.percentile_values.sample_count(), 1);
        assert!(!stats.latency.percentile_values.is_hdr());
    }

    #[test]
    fn more_profile_hdr_mode_stores_histogram_samples() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::More)
                .unwrap()
                .stats_percentile_mode(StatsPercentileMode::Hdr)
                .unwrap(),
        );

        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Get", true, false, false),
            100,
            200,
            Duration::from_millis(5),
            0,
            0,
        ));

        let state = control.inner.state.read().unwrap();
        let stats = state.request_stats.get("Get").unwrap();
        assert_eq!(stats.latency.percentile_values.sample_count(), 1);
        assert!(stats.latency.percentile_values.is_hdr());
    }

    #[test]
    fn interval_snapshot_logs_json_and_resets_request_stats() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::Regular)
                .unwrap(),
        );

        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Get", true, true, false),
            100,
            200,
            Duration::from_millis(5),
            2,
            1,
        ));

        let snapshot = parsed_snapshot(&control);
        assert!(snapshot.get("profile").is_none());
        assert!(snapshot.get("sdkName").is_none());
        assert!(snapshot.get("sdkVersion").is_none());
        assert!(snapshot.get("queries").is_none());
        assert_java_timestamp(&snapshot["startTime"]);
        assert_java_timestamp(&snapshot["endTime"]);

        let request = request_entry(&snapshot, "Get");
        assert_eq!(request["httpRequestCount"], 1);
        assert_eq!(request["retry"]["authCount"], 1);
        assert_eq!(request["retry"]["delayMs"], 0);
        assert_eq!(request["retry"]["throttleCount"], 0);
        assert_eq!(request["rateLimitDelayMs"], 0);
        assert_java_metric_shape(&request["httpRequestLatencyMs"]);
        assert!(request["httpRequestLatencyMs"].get("95th").is_none());
        assert!(request["httpRequestLatencyMs"].get("99th").is_none());
        assert_java_metric_shape(&request["requestSize"]);
        assert_java_metric_shape(&request["resultSize"]);
        assert_eq!(snapshot["connections"]["min"], 1);
        assert_eq!(snapshot["connections"]["max"], 1);
        assert_eq!(snapshot["connections"]["avg"], 1.0);
        assert!(control.inner.state.read().unwrap().request_stats.is_empty());
    }

    #[test]
    fn interval_snapshot_groups_stats_by_java_request_name() {
        let request_names = [
            "Put",
            "WriteMultiple",
            "Get",
            "Prepare",
            "MultiDelete",
            "GetTable",
            "Delete",
            "Table",
            "ListTables",
            "GetIndexes",
            "Query",
            "System",
            "SystemStatus",
            "TableUsage",
            "Write",
        ];
        assert_eq!(JAVA_REQUEST_OUTPUT_ORDER, request_names.as_slice());

        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::More)
                .unwrap(),
        );

        for (index, request_name) in request_names.iter().enumerate() {
            let count = if *request_name == "Query" { 2 } else { 1 };
            for _ in 0..count {
                control.observe(StatsObservation::success(
                    StatsRequestMetadata::new(request_name, true, false, false),
                    100 + index,
                    200 + index,
                    Duration::from_millis((index + 1) as u64),
                    0,
                    0,
                ));
            }
        }

        let snapshot = parsed_snapshot(&control);
        let requests = snapshot["requests"].as_array().unwrap();
        assert_eq!(requests.len(), request_names.len());

        for (index, request_name) in request_names.iter().enumerate() {
            let request = &requests[index];
            let expected_count = if *request_name == "Query" { 2 } else { 1 };

            assert_eq!(request["name"], *request_name);
            assert_eq!(request["httpRequestCount"], expected_count);
            assert_eq!(request["errors"], 0);
            assert_eq!(request["rateLimitDelayMs"], 0);
            assert_eq!(request["retry"]["count"], 0);
            assert_java_metric_shape(&request["requestSize"]);
            assert_java_metric_shape(&request["resultSize"]);
            assert_java_metric_shape(&request["httpRequestLatencyMs"]);
            assert!(request["httpRequestLatencyMs"].get("95th").is_some());
            assert!(request["httpRequestLatencyMs"].get("99th").is_some());
        }
    }

    #[test]
    fn none_profile_emits_no_stats_payload() {
        let control = StatsControl::new(&HandleBuilder::new());
        control.start();
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Get", true, false, false),
            10,
            20,
            Duration::from_millis(3),
            0,
            0,
        ));

        assert!(control.emit_interval_for_test().is_none());
    }

    #[test]
    fn empty_interval_snapshot_matches_java_shape() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );

        let snapshot = parsed_snapshot(&control);
        let object = snapshot.as_object().unwrap();
        assert_eq!(object.len(), 4);
        assert!(object.contains_key("clientId"));
        assert!(object.contains_key("startTime"));
        assert!(object.contains_key("endTime"));
        assert_eq!(snapshot["requests"].as_array().unwrap().len(), 0);
        assert!(object.get("queries").is_none());
        assert!(object.get("connections").is_none());
        assert_java_timestamp(&snapshot["startTime"]);
        assert_java_timestamp(&snapshot["endTime"]);
    }

    #[test]
    fn more_profile_interval_snapshot_includes_latency_percentiles() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::More)
                .unwrap(),
        );

        for latency in [1_u64, 2, 3, 4, 5] {
            control.observe(StatsObservation::success(
                StatsRequestMetadata::new("Get", true, false, false),
                100,
                200,
                Duration::from_millis(latency),
                0,
                0,
            ));
        }

        let snapshot = parsed_snapshot(&control);
        assert!(snapshot.get("profile").is_none());
        assert!(snapshot.get("sdkName").is_none());
        assert!(snapshot.get("sdkVersion").is_none());
        assert!(snapshot.get("queries").is_none());
        let request = request_entry(&snapshot, "Get");
        assert_eq!(request["httpRequestLatencyMs"]["95th"], 5);
        assert_eq!(request["httpRequestLatencyMs"]["99th"], 5);
        assert_java_metric_shape(&request["httpRequestLatencyMs"]);
    }

    #[test]
    fn interval_snapshot_includes_query_entries_for_all_profile() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );
        let query_metadata =
            QueryStatsMetadata::new("select * from users".to_string(), true, false, false, None);

        control.observe_query(query_metadata.clone());
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Query", true, false, false).with_query(Some(query_metadata)),
            100,
            200,
            Duration::from_millis(7),
            0,
            0,
        ));

        let snapshot = parsed_snapshot(&control);
        let queries = snapshot["queries"].as_array().unwrap();
        assert_eq!(queries.len(), 1);
        let query = &queries[0];
        assert_eq!(query["query"], "select * from users");
        assert_eq!(query["unprepared"], 1);
        assert_eq!(query["simple"], false);
        assert_eq!(query["httpRequestLatencyMs"]["95th"], 7);
        assert_eq!(query["httpRequestLatencyMs"]["99th"], 7);
        assert_eq!(query["retry"]["throttleCount"], 0);
        assert_java_metric_shape(&query["httpRequestLatencyMs"]);
        assert_java_metric_shape(&query["requestSize"]);
        assert_java_metric_shape(&query["resultSize"]);
        assert!(control.inner.state.read().unwrap().query_stats.is_empty());
    }

    #[test]
    fn serialized_stats_key_order_matches_java_output() {
        let mut request_stats = RequestLifecycleStats::default();
        request_stats.observe(
            StatsObservation::success(
                StatsRequestMetadata::new("Get", true, false, false),
                100,
                200,
                Duration::from_millis(7),
                2,
                1,
            ),
            Some(StatsPercentileMode::Exact),
        );
        let request_json = stringify_json(
            &request_stats_json("Get", &request_stats, StatsProfile::All),
            false,
        );
        assert_key_sequence(
            &request_json,
            &[
                "httpRequestCount",
                "resultSize",
                "name",
                "httpRequestLatencyMs",
                "requestSize",
                "rateLimitDelayMs",
                "errors",
                "retry",
            ],
        );
        assert_key_sequence(
            &stringify_json(
                &request_stats_json("Get", &request_stats, StatsProfile::Regular),
                false,
            ),
            &["min", "avg", "max"],
        );
        assert_key_sequence(
            &stringify_json(
                &duration_summary_json(&request_stats.latency, StatsProfile::All),
                false,
            ),
            &["min", "avg", "max", "95th", "99th"],
        );
        assert_key_sequence(
            &stringify_json(&retry_stats_json(2, 0, 1, 0), false),
            &["delayMs", "authCount", "throttleCount", "count"],
        );

        let metadata =
            QueryStatsMetadata::new("select * from users".to_string(), true, false, false, None);
        let mut query_stats = QueryEntryStats::new(&metadata);
        query_stats.observe_logical(&metadata);
        query_stats.observe_request(
            StatsObservation::success(
                StatsRequestMetadata::new("Query", true, false, false).with_query(Some(metadata)),
                100,
                200,
                Duration::from_millis(7),
                0,
                0,
            ),
            Some(StatsPercentileMode::Exact),
        );
        let query_json = stringify_json(
            &query_stats_json("select * from users", &query_stats, StatsProfile::All),
            false,
        );
        assert_key_sequence(
            &query_json,
            &[
                "doesWrites",
                "unprepared",
                "httpRequestCount",
                "query",
                "resultSize",
                "count",
                "simple",
                "httpRequestLatencyMs",
                "requestSize",
                "rateLimitDelayMs",
                "errors",
                "retry",
            ],
        );

        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );
        let query_metadata =
            QueryStatsMetadata::new("select * from users".to_string(), true, false, false, None);
        control.observe_query(query_metadata.clone());
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Query", true, false, false).with_query(Some(query_metadata)),
            100,
            200,
            Duration::from_millis(7),
            0,
            0,
        ));
        let snapshot = control.emit_interval_for_test().unwrap();
        assert_key_sequence(
            snapshot.as_json(),
            &[
                "clientId",
                "startTime",
                "endTime",
                "requests",
                "queries",
                "connections",
            ],
        );
        assert_key_sequence(
            &stringify_json(
                &connection_stats_json(&ConnectionStats {
                    count: 1,
                    min: 1,
                    max: 1,
                    sum: 1,
                }),
                false,
            ),
            &["min", "avg", "max"],
        );
    }

    #[test]
    fn all_profile_query_entries_match_java_invariant_without_prepare_query() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );
        let prepare_query = "select * from users where profileName = \"ALL\"";
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Prepare", true, false, false),
            164,
            2340,
            Duration::from_millis(1),
            0,
            0,
        ));

        for group in ["g0", "g1", "g2", "g3", "g4", "g0"] {
            let sql = format!("{prepare_query} and grp = \"{group}\"");
            let metadata = QueryStatsMetadata::new(sql, true, false, false, None);
            control.observe_query(metadata.clone());
            control.observe(StatsObservation::success(
                StatsRequestMetadata::new("Query", true, false, false).with_query(Some(metadata)),
                200,
                300,
                Duration::from_millis(2),
                0,
                0,
            ));
        }

        let snapshot = parsed_snapshot(&control);
        let queries = snapshot["queries"].as_array().unwrap();
        assert_eq!(queries.len(), 5);
        assert!(queries.iter().all(|query| query["simple"] == false));
        assert!(queries
            .iter()
            .all(|query| query["query"].as_str().unwrap() != prepare_query));

        let query_count: u64 = queries
            .iter()
            .map(|query| query["count"].as_u64().unwrap())
            .sum();
        let aggregate_query_count = request_entry(&snapshot, "Query")["httpRequestCount"]
            .as_u64()
            .unwrap();
        assert_eq!(query_count, 6);
        assert_eq!(query_count, aggregate_query_count);
        assert_eq!(
            request_entry(&snapshot, "Prepare")["httpRequestCount"]
                .as_u64()
                .unwrap(),
            1
        );
    }

    #[test]
    fn interval_snapshot_honors_pretty_print_and_handler() {
        let handler_count = Arc::new(AtomicUsize::new(0));
        let handler_count_clone = Arc::clone(&handler_count);
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::Regular)
                .unwrap()
                .stats_pretty_print(true)
                .unwrap()
                .stats_enable_log(false)
                .unwrap()
                .stats_handler(move |stats: &StatsSnapshot| {
                    assert!(stats.as_json().contains('\n'));
                    handler_count_clone.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap(),
        );

        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Put", true, false, false),
            75,
            150,
            Duration::from_millis(9),
            0,
            0,
        ));

        let snapshot = control.emit_interval_for_test().unwrap();
        assert!(snapshot.as_json().contains('\n'));
        assert_eq!(handler_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn handle_accessor_returns_shared_control() -> Result<(), Box<dyn Error>> {
        let handle = Handle::builder()
            .endpoint("http://localhost:8080")?
            .mode(HandleMode::Cloudsim)?
            .stats_profile(StatsProfile::Regular)?
            .build()
            .await?;

        let control = handle.get_stats_control();
        let same_control = handle.get_stats_control();

        assert!(control.is_started());
        control.set_profile(StatsProfile::More);
        control.stop();

        assert_eq!(same_control.get_profile(), StatsProfile::More);
        assert!(!same_control.is_started());
        Ok(())
    }

    #[test]
    fn query_observations_only_accumulate_for_all_profile() {
        let regular = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::Regular)
                .unwrap(),
        );
        regular.observe_query(QueryStatsMetadata::new(
            "select * from users".to_string(),
            true,
            false,
            false,
            None,
        ));
        assert!(regular.inner.state.read().unwrap().query_stats.is_empty());

        let all = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );
        all.observe_query(QueryStatsMetadata::new(
            "select * from users".to_string(),
            true,
            false,
            false,
            None,
        ));

        let state = all.inner.state.read().unwrap();
        let stats = state.query_stats.get("select * from users").unwrap();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.unprepared, 1);
        assert!(!stats.simple);
        assert!(!stats.does_writes);
    }

    #[test]
    fn stopped_control_does_not_accumulate_query_observations() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );
        control.stop();

        let query_metadata =
            QueryStatsMetadata::new("select * from users".to_string(), true, false, false, None);
        control.observe_query(query_metadata.clone());
        control.observe_query_metadata(query_metadata.clone());
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Query", true, false, false).with_query(Some(query_metadata)),
            10,
            20,
            Duration::from_millis(1),
            0,
            0,
        ));

        let state = control.inner.state.read().unwrap();
        assert!(state.request_stats.is_empty());
        assert!(state.query_stats.is_empty());
    }

    #[test]
    fn query_response_metadata_does_not_mark_unprepared_query_simple() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );

        control.observe_query(QueryStatsMetadata::new(
            "select * from users".to_string(),
            true,
            false,
            false,
            None,
        ));
        control.observe_query_metadata(QueryStatsMetadata::new(
            "select * from users".to_string(),
            false,
            true,
            true,
            Some("driver plan".to_string()),
        ));

        let state = control.inner.state.read().unwrap();
        let stats = state.query_stats.get("select * from users").unwrap();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.unprepared, 1);
        assert!(!stats.simple);
        assert!(stats.does_writes);
        assert_eq!(stats.plan, Some("driver plan".to_string()));
    }

    #[test]
    fn prepared_logical_query_can_mark_entry_simple() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );

        control.observe_query(QueryStatsMetadata::new(
            "select * from users where id = ?".to_string(),
            false,
            true,
            false,
            Some("simple plan".to_string()),
        ));

        let state = control.inner.state.read().unwrap();
        let stats = state
            .query_stats
            .get("select * from users where id = ?")
            .unwrap();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.unprepared, 0);
        assert!(stats.simple);
        assert_eq!(stats.plan, Some("simple plan".to_string()));
    }

    #[test]
    fn query_metadata_keeps_first_available_plan() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );

        control.observe_query_metadata(QueryStatsMetadata::new(
            "select * from users order by name".to_string(),
            false,
            false,
            false,
            Some("first plan".to_string()),
        ));
        control.observe_query_metadata(QueryStatsMetadata::new(
            "select * from users order by name".to_string(),
            false,
            false,
            false,
            Some("second plan".to_string()),
        ));

        let state = control.inner.state.read().unwrap();
        let stats = state
            .query_stats
            .get("select * from users order by name")
            .unwrap();
        assert_eq!(stats.plan, Some("first plan".to_string()));
        assert_eq!(stats.count, 0);
        assert_eq!(stats.unprepared, 0);
    }

    #[test]
    fn query_request_observations_accumulate_under_query_entry() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );
        let query_metadata =
            QueryStatsMetadata::new("select * from users".to_string(), true, false, false, None);

        control.observe_query(query_metadata.clone());
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Query", true, true, false).with_query(Some(query_metadata)),
            100,
            200,
            Duration::from_millis(7),
            2,
            1,
        ));

        let state = control.inner.state.read().unwrap();
        let stats = state.query_stats.get("select * from users").unwrap();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.unprepared, 1);
        assert_eq!(stats.request_stats.success_count, 1);
        assert_eq!(stats.request_stats.retry_count, 2);
        assert_eq!(stats.request_stats.auth_retry_count, 1);
        assert_eq!(stats.request_stats.response_size.total, 200);
    }

    #[test]
    fn query_error_observations_accumulate_under_query_entry() {
        let control = StatsControl::new(
            &HandleBuilder::new()
                .stats_profile(StatsProfile::All)
                .unwrap(),
        );
        let query_metadata = QueryStatsMetadata::new(
            "select * from missing".to_string(),
            true,
            false,
            false,
            None,
        );

        control.observe_query(query_metadata.clone());
        control.observe(StatsObservation::error(
            StatsRequestMetadata::new("Query", true, false, false).with_query(Some(query_metadata)),
            80,
            12,
            Duration::from_millis(4),
            1,
            0,
            NoSQLErrorCode::TableNotFound,
        ));

        let state = control.inner.state.read().unwrap();
        let stats = state.query_stats.get("select * from missing").unwrap();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.unprepared, 1);
        assert_eq!(stats.request_stats.success_count, 0);
        assert_eq!(stats.request_stats.error_count, 1);
        assert_eq!(
            stats.request_stats.last_error_code,
            Some(NoSQLErrorCode::TableNotFound)
        );
        assert_eq!(stats.request_stats.latency.count, 0);
        assert_eq!(stats.request_stats.request_size.total, 0);
        assert_eq!(stats.request_stats.response_size.total, 0);
    }
}
