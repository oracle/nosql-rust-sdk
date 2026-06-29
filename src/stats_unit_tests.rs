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
fn setting_profile_none_discards_current_interval_stats() {
    let control = StatsControl::new(
        &HandleBuilder::new()
            .stats_profile(StatsProfile::All)
            .unwrap(),
    );
    let metadata =
        QueryStatsMetadata::new("select * from users".to_string(), true, false, false, None);

    control.observe_query(metadata.clone());
    control.observe(StatsObservation::success(
        StatsRequestMetadata::new("Query", true, false, false).with_query(Some(metadata)),
        10,
        20,
        Duration::from_millis(3),
        0,
        0,
    ));

    control.set_profile(StatsProfile::None);
    {
        let state = control.inner.state.read().unwrap();
        assert!(state.request_stats.is_empty());
        assert!(state.query_stats.is_empty());
        assert!(state.connection_stats.is_empty());
    }
    assert!(control.emit_interval_for_test().is_none());

    control.set_profile(StatsProfile::All);
    let snapshot: Value =
        serde_json::from_str(control.emit_interval_for_test().unwrap().as_json()).unwrap();
    assert_eq!(snapshot["requests"].as_array().unwrap().len(), 0);
    assert!(snapshot.get("queries").is_none());
    assert!(snapshot.get("connections").is_none());
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
fn zero_millisecond_successes_omit_latency_summary_like_java() {
    let mut request_stats = RequestLifecycleStats::default();
    request_stats.observe(
        StatsObservation::success(
            StatsRequestMetadata::new("Get", true, false, false),
            10,
            20,
            Duration::ZERO,
            0,
            0,
        ),
        Some(StatsPercentileMode::Exact),
    );
    let request = request_stats_json("Get", &request_stats, StatsProfile::More);
    assert!(request.get("httpRequestLatencyMs").is_none());

    let metadata =
        QueryStatsMetadata::new("select * from users".to_string(), true, false, false, None);
    let mut query_stats = QueryEntryStats::new(&metadata);
    query_stats.observe_logical(&metadata);
    query_stats.observe_request(
        StatsObservation::success(
            StatsRequestMetadata::new("Query", true, false, false)
                .with_query(Some(metadata.clone())),
            10,
            20,
            Duration::ZERO,
            0,
            0,
        ),
        Some(StatsPercentileMode::Exact),
    );
    let query = query_stats_json("select * from users", &query_stats, StatsProfile::All);
    assert!(query.get("httpRequestLatencyMs").is_none());
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
