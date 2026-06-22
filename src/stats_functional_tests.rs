use super::*;
use serde_json::Value;
use std::sync::{Arc, Mutex};

// These tests intentionally mirror the Java SDK StatsTest style: validate
// output shape, field presence, lifecycle behavior, and deterministic counters
// without asserting exact latency values.
const FUNCTIONAL_REQUEST_NAMES: &[&str] = &[
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

fn parsed_snapshot(control: &StatsControl) -> Value {
    serde_json::from_str(control.emit_interval_for_test().unwrap().as_json()).unwrap()
}

fn parsed_stats_snapshot(snapshot: &StatsSnapshot) -> Value {
    serde_json::from_str(snapshot.as_json()).unwrap()
}

fn request_entry<'a>(snapshot: &'a Value, name: &str) -> &'a Value {
    snapshot["requests"]
        .as_array()
        .unwrap()
        .iter()
        .find(|request| request["name"] == name)
        .unwrap()
}

fn assert_u64_with_report(request_name: &str, field: &str, expected: u64, actual: u64) {
    let result = if expected == actual {
        "good"
    } else {
        "mismatch"
    };
    println!("{request_name:<14} {field:<32} expected={expected:<8} actual={actual:<8} {result}");
    assert_eq!(actual, expected, "{request_name} {field}");
}

fn assert_f64_with_report(request_name: &str, field: &str, expected: f64, actual: f64) {
    let matches = (actual - expected).abs() < f64::EPSILON;
    let result = if matches { "good" } else { "mismatch" };
    println!(
        "{request_name:<14} {field:<32} expected={expected:<8.3} actual={actual:<8.3} {result}"
    );
    assert!(
        matches,
        "{request_name} {field}: expected {expected}, got {actual}"
    );
}

fn assert_avg_latency_range_with_report(
    request_name: &str,
    latency: &Value,
    min_ms: f64,
    max_ms: f64,
) {
    let actual = latency["avg"]
        .as_f64()
        .unwrap_or_else(|| panic!("{request_name} latency avg must be numeric: {latency}"));
    let matches = actual >= min_ms && actual <= max_ms;
    let result = if matches { "good" } else { "mismatch" };
    println!(
        "{request_name:<14} {:<32} expected=[{min_ms:.3}, {max_ms:.3}] actual={actual:.3} {result}",
        "httpRequestLatencyMs.avg"
    );
    assert!(
        matches,
        "{request_name} average latency expected in [{min_ms}, {max_ms}] ms, got {actual} ms"
    );
}

fn assert_metric_values_with_report(
    request_name: &str,
    metric_name: &str,
    metric: &Value,
    min: u64,
    avg: f64,
    max: u64,
) {
    assert_u64_with_report(
        request_name,
        &format!("{metric_name}.min"),
        min,
        metric["min"].as_u64().unwrap(),
    );
    assert_f64_with_report(
        request_name,
        &format!("{metric_name}.avg"),
        avg,
        metric["avg"].as_f64().unwrap(),
    );
    assert_u64_with_report(
        request_name,
        &format!("{metric_name}.max"),
        max,
        metric["max"].as_u64().unwrap(),
    );
}

fn assert_numeric_metric(metric: &Value, fields: &[&str]) {
    for field in fields {
        assert!(
            metric[*field].is_number(),
            "expected {field} to be numeric in {metric}"
        );
    }
}

fn assert_java_metric_shape(metric: &Value, require_percentiles: bool) {
    assert_numeric_metric(metric, &["min", "avg", "max"]);
    assert!(
        metric.get("count").is_none(),
        "Java stats metrics must not emit count: {metric}"
    );
    assert!(
        metric.get("total").is_none(),
        "Java stats metrics must not emit total: {metric}"
    );

    if require_percentiles {
        assert_numeric_metric(metric, &["95th", "99th"]);
    } else {
        assert!(
            metric.get("95th").is_none(),
            "REGULAR latency must not emit 95th: {metric}"
        );
        assert!(
            metric.get("99th").is_none(),
            "REGULAR latency must not emit 99th: {metric}"
        );
    }
}

fn assert_java_retry_shape(retry: &Value) {
    assert_numeric_metric(retry, &["delayMs", "authCount", "throttleCount", "count"]);
}

fn assert_java_request_shape(request: &Value, require_percentiles: bool) {
    assert!(request["httpRequestCount"].is_number(), "{request}");
    assert!(request["name"].is_string(), "{request}");
    assert!(request["rateLimitDelayMs"].is_number(), "{request}");
    assert!(request["errors"].is_number(), "{request}");
    assert_java_metric_shape(&request["requestSize"], false);
    assert_java_metric_shape(&request["resultSize"], false);
    assert_java_metric_shape(&request["httpRequestLatencyMs"], require_percentiles);
    assert_java_retry_shape(&request["retry"]);
}

fn assert_java_query_shape(query: &Value) {
    assert!(query["query"].is_string(), "{query}");
    assert!(query["doesWrites"].is_boolean(), "{query}");
    assert!(query["unprepared"].is_number(), "{query}");
    assert!(query["httpRequestCount"].is_number(), "{query}");
    assert!(query["count"].is_number(), "{query}");
    assert!(query["simple"].is_boolean(), "{query}");
    assert!(query["rateLimitDelayMs"].is_number(), "{query}");
    assert!(query["errors"].is_number(), "{query}");
    assert_java_metric_shape(&query["requestSize"], false);
    assert_java_metric_shape(&query["resultSize"], false);
    assert_java_metric_shape(&query["httpRequestLatencyMs"], true);
    assert_java_retry_shape(&query["retry"]);
}

fn assert_java_snapshot_basics(snapshot: &Value) {
    assert!(
        snapshot["clientId"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "{snapshot}"
    );
    assert!(snapshot["startTime"].is_string(), "{snapshot}");
    assert!(snapshot["endTime"].is_string(), "{snapshot}");
    assert!(snapshot["requests"].is_array(), "{snapshot}");
    assert!(
        snapshot.get("profile").is_none(),
        "periodic stats must not include profile: {snapshot}"
    );
    assert!(
        snapshot.get("sdkName").is_none(),
        "periodic stats must not include sdkName: {snapshot}"
    );
    assert!(
        snapshot.get("sdkVersion").is_none(),
        "periodic stats must not include sdkVersion: {snapshot}"
    );
}

fn assert_java_connections_shape(snapshot: &Value) {
    let connections = &snapshot["connections"];
    assert_numeric_metric(connections, &["min", "avg", "max"]);
    assert!(
        connections["max"].as_u64().unwrap() > 0,
        "non-empty Java-compatible stats interval should report a non-zero connection count"
    );
}

fn stats_control(profile: StatsProfile) -> StatsControl {
    StatsControl::new(&HandleBuilder::new().stats_profile(profile).unwrap())
}

#[test]
fn request_type_stats_accumulate_counts_sizes_latency_and_retries() {
    assert_eq!(JAVA_REQUEST_OUTPUT_ORDER, FUNCTIONAL_REQUEST_NAMES);

    let control = stats_control(StatsProfile::More);

    for (index, request_name) in FUNCTIONAL_REQUEST_NAMES.iter().enumerate() {
        let metadata = StatsRequestMetadata::new(request_name, true, false, false);
        control.observe(StatsObservation::success(
            metadata.clone(),
            100 + index,
            200 + index,
            Duration::from_millis(10 + index as u64),
            1,
            0,
        ));
        control.observe(StatsObservation::success(
            metadata.clone(),
            300 + index,
            600 + index,
            Duration::from_millis(30 + index as u64),
            2,
            1,
        ));
        control.observe(StatsObservation::error(
            metadata,
            900 + index,
            900 + index,
            Duration::from_millis(90 + index as u64),
            4,
            3,
            NoSQLErrorCode::ServerError,
        ));
    }

    let snapshot = parsed_snapshot(&control);
    let requests = snapshot["requests"].as_array().unwrap();
    assert_eq!(requests.len(), FUNCTIONAL_REQUEST_NAMES.len());

    println!();
    println!("Stats functional test criteria");
    println!("Input per request: 2 successful observations and 1 error observation.");
    println!("Count/retry fields include successes and errors.");
    println!("Size and latency summaries include successful observations.");
    println!();

    for (index, request_name) in FUNCTIONAL_REQUEST_NAMES.iter().enumerate() {
        let request = request_entry(&snapshot, request_name);
        let index = index as u64;

        assert_eq!(request["name"], *request_name);
        println!("Request: {request_name}");
        assert_u64_with_report(
            request_name,
            "httpRequestCount",
            3,
            request["httpRequestCount"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "errors",
            1,
            request["errors"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "retry.count",
            7,
            request["retry"]["count"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "retry.authCount",
            4,
            request["retry"]["authCount"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "retry.delayMs",
            0,
            request["retry"]["delayMs"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "retry.throttleCount",
            0,
            request["retry"]["throttleCount"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "rateLimitDelayMs",
            0,
            request["rateLimitDelayMs"].as_u64().unwrap(),
        );

        assert_metric_values_with_report(
            request_name,
            "requestSize",
            &request["requestSize"],
            100 + index,
            200.0 + index as f64,
            300 + index,
        );
        assert_metric_values_with_report(
            request_name,
            "resultSize",
            &request["resultSize"],
            200 + index,
            400.0 + index as f64,
            600 + index,
        );
        assert_metric_values_with_report(
            request_name,
            "httpRequestLatencyMs",
            &request["httpRequestLatencyMs"],
            10 + index,
            20.0 + index as f64,
            30 + index,
        );
        assert_u64_with_report(
            request_name,
            "httpRequestLatencyMs.95th",
            30 + index,
            request["httpRequestLatencyMs"]["95th"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "httpRequestLatencyMs.99th",
            30 + index,
            request["httpRequestLatencyMs"]["99th"].as_u64().unwrap(),
        );
        println!("Request {request_name}: all expected values matched; test checks passed.");
        println!();
    }
}

#[test]
fn request_error_and_throttling_fields_are_emitted() {
    let error_codes = [
        NoSQLErrorCode::ServerError,
        NoSQLErrorCode::IllegalArgument,
        NoSQLErrorCode::ReadLimitExceeded,
        NoSQLErrorCode::WriteLimitExceeded,
        NoSQLErrorCode::OperationLimitExceeded,
    ];
    let control = stats_control(StatsProfile::Regular);

    for (index, request_name) in FUNCTIONAL_REQUEST_NAMES.iter().enumerate() {
        let error_code = error_codes[index % error_codes.len()];
        control.observe(
            StatsObservation::error(
                StatsRequestMetadata::new(request_name, true, false, false),
                100 + index,
                20 + index,
                Duration::from_millis(5 + index as u64),
                1,
                0,
                error_code,
            )
            .with_retry_delay_ms(25 + index as u64)
            .with_throttle_retry_count(1)
            .with_rate_limit_delay_ms(40 + index as u64),
        );
    }

    let snapshot = parsed_snapshot(&control);
    let requests = snapshot["requests"].as_array().unwrap();
    assert_eq!(requests.len(), FUNCTIONAL_REQUEST_NAMES.len());

    println!();
    println!("Stats error/throttling test criteria");
    println!("Input per request: 1 error observation with throttle and delay fields.");
    println!();

    for (index, request_name) in FUNCTIONAL_REQUEST_NAMES.iter().enumerate() {
        let request = request_entry(&snapshot, request_name);
        let index = index as u64;

        println!("Request: {request_name}");
        assert_u64_with_report(
            request_name,
            "httpRequestCount",
            1,
            request["httpRequestCount"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "errors",
            1,
            request["errors"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "retry.count",
            1,
            request["retry"]["count"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "retry.authCount",
            0,
            request["retry"]["authCount"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "retry.delayMs",
            25 + index,
            request["retry"]["delayMs"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "retry.throttleCount",
            1,
            request["retry"]["throttleCount"].as_u64().unwrap(),
        );
        assert_u64_with_report(
            request_name,
            "rateLimitDelayMs",
            40 + index,
            request["rateLimitDelayMs"].as_u64().unwrap(),
        );
        println!("Request {request_name}: error and throttling fields matched.");
        println!();
    }
}

#[test]
fn latency_fields_are_present_and_numeric_for_stats_profiles() {
    for profile in [StatsProfile::Regular, StatsProfile::More, StatsProfile::All] {
        let control = stats_control(profile);
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Get", true, false, false),
            100,
            200,
            Duration::from_millis(7),
            0,
            0,
        ));

        let snapshot = parsed_snapshot(&control);
        let request = request_entry(&snapshot, "Get");
        let latency = &request["httpRequestLatencyMs"];
        assert_numeric_metric(latency, &["min", "avg", "max"]);

        if matches!(profile, StatsProfile::More | StatsProfile::All) {
            assert_numeric_metric(latency, &["95th", "99th"]);
        } else {
            assert!(latency.get("95th").is_none());
            assert!(latency.get("99th").is_none());
        }
    }
}

#[test]
fn average_latency_is_within_expected_ranges_for_crud_and_query() {
    let control = stats_control(StatsProfile::All);
    let sample_count = 101_u64;

    for request_name in ["Put", "Get", "Delete"] {
        for latency_ms in 100_u64..=200 {
            control.observe(StatsObservation::success(
                StatsRequestMetadata::new(request_name, true, false, false),
                100,
                200,
                Duration::from_millis(latency_ms),
                0,
                0,
            ));
        }
    }

    let query_text = "select * from statsTestTable where grp = \"g0\"";
    let query_metadata = QueryStatsMetadata::new(query_text.to_string(), true, false, false, None);
    control.observe_query(query_metadata.clone());
    for latency_ms in 200_u64..=300 {
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Query", true, true, false)
                .with_query(Some(query_metadata.clone())),
            180,
            360,
            Duration::from_millis(latency_ms),
            0,
            0,
        ));
    }

    let snapshot = parsed_snapshot(&control);

    println!();
    println!("Average latency range validation");
    println!(
        "CRUD inputs: {sample_count} observations per request from 100ms..200ms; expected average range [100, 200] ms."
    );
    println!(
        "Query inputs: {sample_count} observations from 200ms..300ms; expected average range [200, 300] ms."
    );
    println!();

    for request_name in ["Put", "Get", "Delete"] {
        let request = request_entry(&snapshot, request_name);
        assert_u64_with_report(
            request_name,
            "httpRequestCount",
            sample_count,
            request["httpRequestCount"].as_u64().unwrap(),
        );
        assert_avg_latency_range_with_report(
            request_name,
            &request["httpRequestLatencyMs"],
            100.0,
            200.0,
        );
    }

    let query_request = request_entry(&snapshot, "Query");
    assert_u64_with_report(
        "Query",
        "httpRequestCount",
        sample_count,
        query_request["httpRequestCount"].as_u64().unwrap(),
    );
    assert_avg_latency_range_with_report(
        "Query",
        &query_request["httpRequestLatencyMs"],
        200.0,
        300.0,
    );

    let query_entries = snapshot["queries"].as_array().unwrap();
    assert_eq!(query_entries.len(), 1);
    assert_eq!(query_entries[0]["query"], query_text);
    assert_u64_with_report(
        "QueryEntry",
        "httpRequestCount",
        sample_count,
        query_entries[0]["httpRequestCount"].as_u64().unwrap(),
    );
    assert_avg_latency_range_with_report(
        "QueryEntry",
        &query_entries[0]["httpRequestLatencyMs"],
        200.0,
        300.0,
    );
}

#[test]
fn stats_snapshot_shape_matches_java_stats_test_expectations() {
    let control = stats_control(StatsProfile::All);
    let query_text = "select * from statsTestTable";

    for row in 0..6 {
        control.observe(StatsObservation::success(
            StatsRequestMetadata::new("Put", true, true, false),
            100 + row,
            200 + row,
            Duration::from_millis(5 + row as u64),
            0,
            0,
        ));
    }

    let query_metadata = QueryStatsMetadata::new(query_text.to_string(), true, false, false, None);
    control.observe_query(query_metadata.clone());
    control.observe(
        StatsObservation::success(
            StatsRequestMetadata::new("Query", true, true, false).with_query(Some(query_metadata)),
            180,
            360,
            Duration::from_millis(12),
            1,
            0,
        )
        .with_retry_delay_ms(15),
    );

    let snapshot = parsed_snapshot(&control);
    assert_java_snapshot_basics(&snapshot);
    assert_java_connections_shape(&snapshot);

    let requests = snapshot["requests"].as_array().unwrap();
    assert!(
        requests.len() >= 2,
        "expected at least Put and Query request entries: {snapshot}"
    );
    assert_java_request_shape(request_entry(&snapshot, "Put"), true);
    assert_java_request_shape(request_entry(&snapshot, "Query"), true);

    let queries = snapshot["queries"].as_array().unwrap();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0]["query"], query_text);
    assert_java_query_shape(&queries[0]);

    println!("Java-style stats snapshot validation");
    println!("Top level fields: clientId/startTime/endTime/requests");
    println!("Request fields: Put and Query contain count, size, latency, retry, error fields");
    println!("Query fields: ALL profile contains one query entry with Java-compatible shape");
    println!("Connections: non-empty interval reports min/avg/max and max > 0");
}

#[test]
fn stop_start_collection_gate_matches_java_functional_expectations() {
    let control = stats_control(StatsProfile::All);
    let query_text = "select * from statsTestTable";

    control.stop();
    assert!(!control.is_started());

    let stopped_query = QueryStatsMetadata::new(query_text.to_string(), true, false, false, None);
    control.observe_query(stopped_query.clone());
    control.observe(StatsObservation::success(
        StatsRequestMetadata::new("Query", true, true, false).with_query(Some(stopped_query)),
        100,
        200,
        Duration::from_millis(5),
        0,
        0,
    ));
    control.observe(StatsObservation::success(
        StatsRequestMetadata::new("Put", true, true, false),
        100,
        200,
        Duration::from_millis(5),
        0,
        0,
    ));

    let stopped_snapshot = parsed_snapshot(&control);
    assert_java_snapshot_basics(&stopped_snapshot);
    assert_eq!(stopped_snapshot["requests"].as_array().unwrap().len(), 0);
    assert!(stopped_snapshot.get("queries").is_none());
    assert!(stopped_snapshot.get("connections").is_none());

    control.start();
    assert!(control.is_started());

    let empty_after_start = parsed_snapshot(&control);
    assert_java_snapshot_basics(&empty_after_start);
    assert_eq!(empty_after_start["requests"].as_array().unwrap().len(), 0);
    assert!(empty_after_start.get("queries").is_none());
    assert!(empty_after_start.get("connections").is_none());

    let started_query = QueryStatsMetadata::new(query_text.to_string(), true, false, false, None);
    control.observe_query(started_query.clone());
    control.observe(StatsObservation::success(
        StatsRequestMetadata::new("Query", true, true, false).with_query(Some(started_query)),
        100,
        200,
        Duration::from_millis(5),
        0,
        0,
    ));
    control.observe(StatsObservation::success(
        StatsRequestMetadata::new("Put", true, true, false),
        100,
        200,
        Duration::from_millis(5),
        0,
        0,
    ));

    let started_snapshot = parsed_snapshot(&control);
    assert_java_snapshot_basics(&started_snapshot);
    assert_eq!(
        request_entry(&started_snapshot, "Put")["httpRequestCount"],
        1
    );
    assert_eq!(
        request_entry(&started_snapshot, "Query")["httpRequestCount"],
        1
    );
    assert_eq!(started_snapshot["queries"].as_array().unwrap().len(), 1);
    assert_java_connections_shape(&started_snapshot);

    println!("Stop/start stats lifecycle validation");
    println!("Stopped work: emitted as an empty Java-style interval and not carried forward");
    println!("Started work: Put and Query are emitted with request count 1 each");
}

#[tokio::test(start_paused = true)]
async fn periodic_stats_task_emits_non_empty_and_empty_intervals() {
    let snapshots = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = snapshots.clone();
    let builder = HandleBuilder::new()
        .stats_profile(StatsProfile::Regular)
        .unwrap()
        .stats_interval(Duration::from_secs(5))
        .unwrap()
        .stats_enable_log(false)
        .unwrap()
        .stats_handler(move |stats: &StatsSnapshot| {
            captured.lock().unwrap().push(parsed_stats_snapshot(stats));
        })
        .unwrap();
    let control = StatsControl::new(&builder);

    control.start_log_task();
    tokio::task::yield_now().await;
    control.observe(StatsObservation::success(
        StatsRequestMetadata::new("Put", true, false, false),
        100,
        200,
        Duration::from_millis(5),
        0,
        0,
    ));

    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    {
        let values = snapshots.lock().unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(request_entry(&values[0], "Put")["httpRequestCount"], 1);
    }

    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    let values = snapshots.lock().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[1]["requests"].as_array().unwrap().len(), 0);
}
