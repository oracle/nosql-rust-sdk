//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

use oracle_nosql_rust_sdk::types::{MapValue, NoSQLColumnToFieldValue, TableLimits};
use oracle_nosql_rust_sdk::{
    DeleteRequest, GetIndexesRequest, GetRequest, GetTableRequest, Handle, HandleMode,
    ListTablesRequest, MultiDeleteRequest, NoSQLError, PutRequest, QueryRequest,
    StatsPercentileMode, StatsProfile, StatsSnapshot, TableRequest, WriteMultipleRequest,
};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// Opt-in because it needs a live CloudSim/proxy. The live tests are ignored by
// default and also require RUN_NOSQL_STATS_E2E=1 so normal cargo test output
// cannot be mistaken for live service coverage. They validate the real SDK path,
// not just collector math, by checking that SDK snapshots match independently
// measured counts, throughput, average latency, p95, and p99.
const DEFAULT_REQUEST_COUNT: u64 = 10;
const DEFAULT_STATS_INTERVAL_SECS: u64 = 10;
const DEFAULT_LATENCY_ABS_TOLERANCE_MS: f64 = 100.0;
const DEFAULT_LATENCY_REL_TOLERANCE: f64 = 2.0;
const DEFAULT_THROUGHPUT_REL_TOLERANCE: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestMode {
    Cloudsim,
    Onprem,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires RUN_NOSQL_STATS_E2E=1 and a live CloudSim/proxy endpoint"]
async fn e2e_stats_match_independent_workload_metrics() -> Result<(), Box<dyn Error>> {
    if env::var("RUN_NOSQL_STATS_E2E").ok().as_deref() != Some("1") {
        println!("skipping stats e2e test; set RUN_NOSQL_STATS_E2E=1 and run with --ignored");
        return Ok(());
    }

    let endpoint = env::var("NOSQL_STATS_E2E_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());
    let mode = test_mode();
    let table_name = env::var("NOSQL_STATS_E2E_TABLE")
        .unwrap_or_else(|_| format!("sdk_stats_e2e_{}", current_millis()));
    let index_name = format!("idx_{table_name}");
    let request_count = env_u64("NOSQL_STATS_E2E_REQUEST_COUNT", DEFAULT_REQUEST_COUNT)?;
    let interval_secs = env_u64("NOSQL_STATS_E2E_INTERVAL_SECS", DEFAULT_STATS_INTERVAL_SECS)?;
    let tolerance = ComparisonTolerance::from_env()?;
    let snapshots = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = snapshots.clone();

    let setup_handle =
        test_handle(&endpoint, mode, StatsProfile::None, None, interval_secs).await?;
    create_table(&setup_handle, mode, &table_name).await?;

    let handle = test_handle(
        &endpoint,
        mode,
        StatsProfile::More,
        Some(captured),
        interval_secs,
    )
    .await?;

    let validation =
        match run_mixed_workload(&handle, &table_name, &index_name, request_count).await {
            Ok(measured) => {
                let snapshot_result = wait_for_complete_snapshot(
                    &snapshots,
                    &measured.metrics,
                    Duration::from_secs(interval_secs + 15),
                )
                .await;

                println!();
                println!("End-to-end SDK stats workload metrics validation");
                println!("Endpoint: {endpoint}");
                println!("Mode: {:?}", mode);
                println!("Table: {table_name}");
                println!("Configured request count per repeated workload: {request_count}");
                println!("Stats interval: {interval_secs}s");
                println!("Workload elapsed: {:.3}s", measured.elapsed.as_secs_f64());
                println!();

                snapshot_result.and_then(|snapshot| {
                    assert_snapshot_matches_measured_metrics(&snapshot, &measured, tolerance)
                })
            }
            Err(error) => Err(error),
        };

    handle.get_stats_control().stop();
    let cleanup = drop_table(&handle, &table_name).await;

    validation?;
    cleanup?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires RUN_NOSQL_STATS_E2E=1 and a live CloudSim/proxy endpoint"]
async fn e2e_all_profile_query_requests_create_query_entries() -> Result<(), Box<dyn Error>> {
    if env::var("RUN_NOSQL_STATS_E2E").ok().as_deref() != Some("1") {
        println!("skipping stats e2e test; set RUN_NOSQL_STATS_E2E=1 and run with --ignored");
        return Ok(());
    }

    let endpoint = env::var("NOSQL_STATS_E2E_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());
    let mode = test_mode();
    let table_name = env::var("NOSQL_STATS_E2E_ALL_TABLE")
        .unwrap_or_else(|_| format!("sdk_stats_all_e2e_{}", current_millis()));
    let interval_secs = env_u64("NOSQL_STATS_E2E_INTERVAL_SECS", DEFAULT_STATS_INTERVAL_SECS)?;
    let snapshots = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = snapshots.clone();

    let setup_handle =
        test_handle(&endpoint, mode, StatsProfile::None, None, interval_secs).await?;
    create_table(&setup_handle, mode, &table_name).await?;

    let handle = test_handle(
        &endpoint,
        mode,
        StatsProfile::All,
        Some(captured),
        interval_secs,
    )
    .await?;

    let profile_name = "statsAllE2E";
    let group = format!("run{}", current_millis());
    let id = 1;
    let sql = format!(
        "SELECT * FROM {table_name} WHERE profileName = \"{profile_name}\" AND grp = \"{group}\" AND id = {id}"
    );

    let validation = async {
        PutRequest::new(&table_name)
            .value(row_value(profile_name, &group, id, "ALL_QUERY_ENTRY"))
            .execute(&handle)
            .await?;

        let result = QueryRequest::new(&sql).execute(&handle).await?;
        if result.rows().is_empty() {
            return Err("expected one row from ALL-profile e2e query".into());
        }

        let snapshot = wait_for_query_entry_snapshot(
            &snapshots,
            &sql,
            Duration::from_secs(interval_secs + 15),
        )
        .await?;
        let entry = query_entry(&snapshot, &sql)
            .ok_or_else(|| format!("missing query entry for SQL: {sql}"))?;

        assert_eq!(entry["query"], sql);
        assert_eq!(entry["count"], 1);
        assert_eq!(entry["unprepared"], 1);
        assert!(
            entry["httpRequestCount"].as_u64().unwrap_or(0) >= 1,
            "query entry must include at least one HTTP request"
        );
        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    handle.get_stats_control().stop();
    let cleanup = drop_table(&handle, &table_name).await;

    validation?;
    cleanup?;

    Ok(())
}

fn test_mode() -> TestMode {
    match env::var("NOSQL_STATS_E2E_MODE")
        .unwrap_or_else(|_| "cloudsim".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "onprem" | "on-prem" => TestMode::Onprem,
        _ => TestMode::Cloudsim,
    }
}

async fn test_handle(
    endpoint: &str,
    mode: TestMode,
    profile: StatsProfile,
    snapshots: Option<Arc<Mutex<Vec<Value>>>>,
    interval_secs: u64,
) -> Result<Handle, NoSQLError> {
    let mut builder = Handle::builder()
        .endpoint(endpoint)?
        .timeout(Duration::from_secs(30))?
        .stats_profile(profile)?
        .stats_percentile_mode(StatsPercentileMode::Exact)?
        .stats_interval(Duration::from_secs(interval_secs))?
        .stats_enable_log(false)?;

    if let Some(snapshots) = snapshots {
        builder = builder.stats_handler(move |stats: &StatsSnapshot| {
            let snapshot: Value = serde_json::from_str(stats.as_json()).unwrap();
            snapshots.lock().unwrap().push(snapshot);
        })?;
    }

    builder = match mode {
        TestMode::Cloudsim => builder.mode(HandleMode::Cloudsim)?,
        TestMode::Onprem => builder.onprem_auth("", "")?,
    };

    builder.build().await
}

fn env_u64(name: &str, default_value: u64) -> Result<u64, NoSQLError> {
    env::var(name)
        .ok()
        .map(|value| {
            value.parse::<u64>().map_err(|err| {
                oracle_nosql_rust_sdk::NoSQLError::new(
                    oracle_nosql_rust_sdk::NoSQLErrorCode::IllegalArgument,
                    &format!("{name} must be an integer: {err}"),
                )
            })
        })
        .transpose()
        .map(|value| value.unwrap_or(default_value))
}

fn env_f64(name: &str, default_value: f64) -> Result<f64, NoSQLError> {
    env::var(name)
        .ok()
        .map(|value| {
            value.parse::<f64>().map_err(|err| {
                oracle_nosql_rust_sdk::NoSQLError::new(
                    oracle_nosql_rust_sdk::NoSQLErrorCode::IllegalArgument,
                    &format!("{name} must be a number: {err}"),
                )
            })
        })
        .transpose()
        .map(|value| value.unwrap_or(default_value))
}

async fn create_table(handle: &Handle, mode: TestMode, table_name: &str) -> Result<(), NoSQLError> {
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (profileName STRING, grp STRING, id INTEGER, payload STRING, idx LONG, PRIMARY KEY(SHARD(profileName, grp), id))"
    );
    let mut request = TableRequest::new(table_name).statement(&ddl);
    if mode == TestMode::Cloudsim {
        request = request.limits(&TableLimits::provisioned(10_000, 10_000, 10));
    }

    request
        .execute(handle)
        .await?
        .wait_for_completion_ms(handle, 120_000, 1_000)
        .await?;
    Ok(())
}

async fn drop_table(handle: &Handle, table_name: &str) -> Result<(), NoSQLError> {
    let ddl = format!("DROP TABLE IF EXISTS {table_name}");
    TableRequest::new(table_name)
        .statement(&ddl)
        .execute(handle)
        .await?
        .wait_for_completion_ms(handle, 120_000, 1_000)
        .await?;
    Ok(())
}

async fn run_mixed_workload(
    handle: &Handle,
    table_name: &str,
    index_name: &str,
    request_count: u64,
) -> Result<MeasuredWorkload, Box<dyn Error>> {
    let mut metrics = IndependentMetrics::default();
    let workload_start = Instant::now();
    let profile_name = "statsE2E";
    let run_id = format!("run{}", current_millis());

    // Measure metadata request types before creating an index. Some CloudSim
    // builds omit index field TYPE values when indexes exist; parser strictness
    // is covered by unit tests, while this e2e focuses on stats accounting.
    for _ in 0..request_count {
        measure_request(&mut metrics, "GetTable", async {
            GetTableRequest::new(table_name).execute(handle).await
        })
        .await?;
        measure_request(&mut metrics, "ListTables", async {
            ListTablesRequest::new().limit(100).execute(handle).await
        })
        .await?;
        measure_request(&mut metrics, "GetIndexes", async {
            GetIndexesRequest::new(table_name).execute(handle).await
        })
        .await?;
    }

    // Keep schema setup outside the measured section, but include one safe DDL
    // request so the functional workload validates the aggregate "Table" entry.
    let index_ddl = format!("CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}(idx)");
    measure_request(&mut metrics, "Table", async {
        TableRequest::new(table_name)
            .statement(&index_ddl)
            .execute(handle)
            .await
    })
    .await?;

    for id in 0..request_count {
        let id = id as i32;
        let group = format!("{run_id}_base");
        measure_request(&mut metrics, "Put", async {
            PutRequest::new(table_name)
                .value(row_value(profile_name, &group, id, "PUT"))
                .execute(handle)
                .await
        })
        .await?;
    }

    for id in 0..request_count {
        let id = id as i32;
        let group = format!("{run_id}_base");
        let result = measure_request(&mut metrics, "Get", async {
            GetRequest::new(table_name)
                .key(primary_key(profile_name, &group, id))
                .execute(handle)
                .await
        })
        .await?;
        if result.row().is_none() {
            return Err(format!("expected row for id {id}").into());
        }
    }

    for id in 0..request_count {
        let id = id as i32;
        let group = format!("{run_id}_base");
        let sql = format!(
            "SELECT * FROM {table_name} WHERE profileName = \"{profile_name}\" AND grp = \"{group}\" AND id = {id}"
        );
        let result = measure_request(&mut metrics, "Query", async {
            QueryRequest::new(&sql).execute(handle).await
        })
        .await?;
        if result.rows().is_empty() {
            return Err(format!("expected query row for id {id}").into());
        }
    }

    for _ in 0..request_count {
        let sql = format!("SELECT * FROM {table_name} WHERE profileName = \"{profile_name}\"");
        measure_request(&mut metrics, "Prepare", async {
            QueryRequest::new(&sql)
                .prepare_only()
                .get_query_plan(true)
                .get_query_schema(true)
                .execute(handle)
                .await
        })
        .await?;
    }

    for id in 0..request_count {
        let id = id as i32;
        let group = format!("{run_id}_wm_{id}");
        let put1 = PutRequest::new(table_name).value(row_value(
            profile_name,
            &group,
            10_000 + id * 2,
            "WRITE_MULTIPLE_1",
        ));
        let put2 = PutRequest::new(table_name).value(row_value(
            profile_name,
            &group,
            10_001 + id * 2,
            "WRITE_MULTIPLE_2",
        ));
        measure_request(&mut metrics, "WriteMultiple", async {
            WriteMultipleRequest::new(table_name)
                .add(Box::new(put1))
                .add(Box::new(put2))
                .execute(handle)
                .await
        })
        .await?;
    }

    for id in 0..request_count {
        let id = id as i32;
        let group = format!("{run_id}_md_{id}");
        for row in 0..2 {
            measure_request(&mut metrics, "Put", async {
                PutRequest::new(table_name)
                    .value(row_value(
                        profile_name,
                        &group,
                        20_000 + id * 2 + row,
                        "MULTI_DELETE_SETUP",
                    ))
                    .execute(handle)
                    .await
            })
            .await?;
        }
        let partial_key = MapValue::new()
            .str("profileName", profile_name)
            .str("grp", &group)
            .to_field_value();
        measure_request(&mut metrics, "MultiDelete", async {
            MultiDeleteRequest::new(table_name, &partial_key)
                .execute(handle)
                .await
        })
        .await?;
    }

    for id in 0..request_count {
        let id = id as i32;
        let group = format!("{run_id}_base");
        measure_request(&mut metrics, "Delete", async {
            DeleteRequest::new(table_name, primary_key(profile_name, &group, id))
                .execute(handle)
                .await
        })
        .await?;
    }

    Ok(MeasuredWorkload {
        metrics,
        elapsed: workload_start.elapsed(),
    })
}

async fn measure_request<T, F>(
    metrics: &mut IndependentMetrics,
    request_name: &'static str,
    future: F,
) -> Result<T, NoSQLError>
where
    F: Future<Output = Result<T, NoSQLError>>,
{
    let started = Instant::now();
    let result = future.await;
    let elapsed = started.elapsed();
    if result.is_ok() {
        metrics.record(request_name, elapsed);
    }
    result
}

async fn wait_for_complete_snapshot(
    snapshots: &Arc<Mutex<Vec<Value>>>,
    metrics: &IndependentMetrics,
    timeout: Duration,
) -> Result<Value, Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let snapshots = snapshots.lock().unwrap();
            if let Some(snapshot) = snapshots
                .iter()
                .find(|snapshot| snapshot_has_expected_counts(snapshot, metrics))
            {
                return Ok(snapshot.clone());
            }
        }

        if tokio::time::Instant::now() >= deadline {
            let totals = collect_totals(snapshots);
            return Err(format!(
                "timed out waiting for one complete stats interval; aggregate totals so far: {:?}",
                totals
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_query_entry_snapshot(
    snapshots: &Arc<Mutex<Vec<Value>>>,
    sql: &str,
    timeout: Duration,
) -> Result<Value, Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let snapshots = snapshots.lock().unwrap();
            if let Some(snapshot) = snapshots
                .iter()
                .find(|snapshot| query_entry(snapshot, sql).is_some())
            {
                return Ok(snapshot.clone());
            }
        }

        if tokio::time::Instant::now() >= deadline {
            let queries = collect_query_entries(snapshots);
            return Err(format!(
                "timed out waiting for ALL-profile query entry; query entries so far: {:?}",
                queries
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn query_entry<'a>(snapshot: &'a Value, sql: &str) -> Option<&'a Value> {
    snapshot["queries"]
        .as_array()?
        .iter()
        .find(|entry| entry["query"] == sql)
}

fn collect_query_entries(snapshots: &Arc<Mutex<Vec<Value>>>) -> Vec<String> {
    let snapshots = snapshots.lock().unwrap();
    snapshots
        .iter()
        .flat_map(|snapshot| snapshot["queries"].as_array().into_iter().flatten())
        .filter_map(|entry| entry["query"].as_str())
        .map(ToString::to_string)
        .collect()
}

fn snapshot_has_expected_counts(snapshot: &Value, metrics: &IndependentMetrics) -> bool {
    metrics
        .entries
        .iter()
        .all(|(name, expected)| request_count(snapshot, name) == Some(expected.count()))
}

fn collect_totals(snapshots: &Arc<Mutex<Vec<Value>>>) -> HashMap<String, u64> {
    let snapshots = snapshots.lock().unwrap();
    let mut totals = HashMap::<String, u64>::new();
    for snapshot in snapshots.iter() {
        for request in snapshot["requests"].as_array().into_iter().flatten() {
            let Some(name) = request["name"].as_str() else {
                continue;
            };
            let Some(count) = request["httpRequestCount"].as_u64() else {
                continue;
            };
            if count == 0 {
                continue;
            }

            *totals.entry(name.to_string()).or_default() += count;
        }
    }
    totals
}

fn assert_snapshot_matches_measured_metrics(
    snapshot: &Value,
    measured: &MeasuredWorkload,
    tolerance: ComparisonTolerance,
) -> Result<(), Box<dyn Error>> {
    let sdk_metrics = sdk_metrics_from_snapshot(snapshot)?;
    let independent_total = measured.metrics.total_count();
    let sdk_total: u64 = sdk_metrics.values().map(|stats| stats.count).sum();
    let independent_throughput = independent_total as f64 / measured.elapsed.as_secs_f64();
    let sdk_throughput = sdk_total as f64 / measured.elapsed.as_secs_f64();
    let throughput_matches =
        relative_delta(independent_throughput, sdk_throughput) <= tolerance.throughput_relative;

    println!(
        "{:<14} {:<14} expected={:<10.3} actual={:<10.3} {}",
        "ALL",
        "throughput/s",
        independent_throughput,
        sdk_throughput,
        if throughput_matches {
            "good"
        } else {
            "mismatch"
        }
    );

    if !throughput_matches {
        return Err(format!(
            "throughput mismatch: independent={independent_throughput:.3}, sdk={sdk_throughput:.3}"
        )
        .into());
    }

    for (name, independent) in measured.metrics.entries.iter() {
        let sdk = sdk_metrics
            .get(*name)
            .ok_or_else(|| format!("missing SDK stats entry for {name}"))?;
        assert_request_metrics_match(name, independent, sdk, tolerance)?;
    }

    Ok(())
}

fn assert_request_metrics_match(
    name: &str,
    independent: &IndependentRequestMetrics,
    sdk: &SdkRequestMetrics,
    tolerance: ComparisonTolerance,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        sdk.count,
        independent.count(),
        "{name} SDK count must match independently measured count"
    );

    println!("{name}: count={}", sdk.count);
    let Some(latency) = &sdk.latency else {
        if independent.max_ms() == 0 {
            println!(
                "{name:<14} {:<14} expected=omitted   actual=omitted   good",
                "latency"
            );
            println!();
            return Ok(());
        }
        return Err(format!("{name} is missing httpRequestLatencyMs").into());
    };

    let comparisons = [
        ("avg", independent.avg_ms(), latency.avg_ms),
        (
            "95th",
            independent.percentile_ms(0.95) as f64,
            latency.p95_ms,
        ),
        (
            "99th",
            independent.percentile_ms(0.99) as f64,
            latency.p99_ms,
        ),
    ];

    for (field, expected, actual) in comparisons {
        let matches = latency_matches(expected, actual, tolerance);
        println!(
            "{name:<14} {field:<14} expected={expected:<10.3} actual={actual:<10.3} tolerance={} {}",
            tolerance.describe_latency(expected),
            if matches { "good" } else { "mismatch" }
        );
        if !matches {
            return Err(format!(
                "{name} {field} latency mismatch: independent={expected:.3}ms sdk={actual:.3}ms"
            )
            .into());
        }
    }
    println!();
    Ok(())
}

fn latency_matches(expected: f64, actual: f64, tolerance: ComparisonTolerance) -> bool {
    (actual - expected).abs() <= tolerance.latency_ms(expected)
}

fn relative_delta(expected: f64, actual: f64) -> f64 {
    if expected == 0.0 {
        if actual == 0.0 {
            0.0
        } else {
            f64::INFINITY
        }
    } else {
        (actual - expected).abs() / expected.abs()
    }
}

fn sdk_metrics_from_snapshot(
    snapshot: &Value,
) -> Result<HashMap<String, SdkRequestMetrics>, Box<dyn Error>> {
    let mut metrics = HashMap::new();
    for request in snapshot["requests"].as_array().into_iter().flatten() {
        let Some(name) = request["name"].as_str() else {
            continue;
        };
        let Some(count) = request["httpRequestCount"].as_u64() else {
            continue;
        };
        if count == 0 {
            continue;
        }
        let latency = match request.get("httpRequestLatencyMs") {
            Some(latency) => Some(SdkLatencyMetrics {
                avg_ms: latency["avg"]
                    .as_f64()
                    .ok_or_else(|| format!("{name} avg latency is missing"))?,
                p95_ms: latency["95th"]
                    .as_u64()
                    .ok_or_else(|| format!("{name} p95 latency is missing"))?
                    as f64,
                p99_ms: latency["99th"]
                    .as_u64()
                    .ok_or_else(|| format!("{name} p99 latency is missing"))?
                    as f64,
            }),
            None => None,
        };
        metrics.insert(name.to_string(), SdkRequestMetrics { count, latency });
    }
    Ok(metrics)
}

fn request_count(snapshot: &Value, name: &str) -> Option<u64> {
    snapshot["requests"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|request| request["name"] == name)
        .and_then(|request| request["httpRequestCount"].as_u64())
}

fn primary_key(profile_name: &str, group: &str, id: i32) -> MapValue {
    MapValue::new()
        .str("profileName", profile_name)
        .str("grp", group)
        .i32("id", id)
}

fn row_value(profile_name: &str, group: &str, id: i32, operation: &str) -> MapValue {
    MapValue::new()
        .str("profileName", profile_name)
        .str("grp", group)
        .i32("id", id)
        .string("payload", format!("payload-{operation}-{id}"))
        .i64("idx", i64::from(id))
}

fn current_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[derive(Debug)]
struct MeasuredWorkload {
    metrics: IndependentMetrics,
    elapsed: Duration,
}

#[derive(Debug, Default)]
struct IndependentMetrics {
    entries: HashMap<&'static str, IndependentRequestMetrics>,
}

impl IndependentMetrics {
    fn record(&mut self, name: &'static str, elapsed: Duration) {
        self.entries.entry(name).or_default().record(elapsed);
    }

    fn total_count(&self) -> u64 {
        self.entries
            .values()
            .map(IndependentRequestMetrics::count)
            .sum()
    }
}

#[derive(Debug, Default)]
struct IndependentRequestMetrics {
    samples_ms: Vec<u64>,
}

impl IndependentRequestMetrics {
    fn record(&mut self, elapsed: Duration) {
        self.samples_ms.push(elapsed.as_millis() as u64);
    }

    fn count(&self) -> u64 {
        self.samples_ms.len() as u64
    }

    fn avg_ms(&self) -> f64 {
        if self.samples_ms.is_empty() {
            return 0.0;
        }
        self.samples_ms.iter().sum::<u64>() as f64 / self.samples_ms.len() as f64
    }

    fn max_ms(&self) -> u64 {
        self.samples_ms.iter().copied().max().unwrap_or(0)
    }

    fn percentile_ms(&self, percentile: f64) -> u64 {
        exact_percentile_ms(&self.samples_ms, percentile)
    }
}

#[derive(Debug)]
struct SdkRequestMetrics {
    count: u64,
    latency: Option<SdkLatencyMetrics>,
}

#[derive(Debug)]
struct SdkLatencyMetrics {
    avg_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Debug, Clone, Copy)]
struct ComparisonTolerance {
    latency_abs_ms: f64,
    latency_relative: f64,
    throughput_relative: f64,
}

impl ComparisonTolerance {
    fn from_env() -> Result<Self, NoSQLError> {
        Ok(ComparisonTolerance {
            latency_abs_ms: env_f64(
                "NOSQL_STATS_E2E_LATENCY_ABS_TOLERANCE_MS",
                DEFAULT_LATENCY_ABS_TOLERANCE_MS,
            )?,
            latency_relative: env_f64(
                "NOSQL_STATS_E2E_LATENCY_REL_TOLERANCE",
                DEFAULT_LATENCY_REL_TOLERANCE,
            )?,
            throughput_relative: env_f64(
                "NOSQL_STATS_E2E_THROUGHPUT_REL_TOLERANCE",
                DEFAULT_THROUGHPUT_REL_TOLERANCE,
            )?,
        })
    }

    fn latency_ms(self, expected: f64) -> f64 {
        self.latency_abs_ms
            .max(expected.abs() * self.latency_relative)
    }

    fn describe_latency(self, expected: f64) -> String {
        format!("+/-{:.3}ms", self.latency_ms(expected))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independent_metrics_compute_average_and_exact_percentiles() {
        let mut metrics = IndependentRequestMetrics::default();
        for latency_ms in [1_u64, 2, 3, 4, 100] {
            metrics.record(Duration::from_millis(latency_ms));
        }

        assert_eq!(metrics.count(), 5);
        assert_eq!(metrics.avg_ms(), 22.0);
        assert_eq!(metrics.percentile_ms(0.95), 100);
        assert_eq!(metrics.percentile_ms(0.99), 100);
    }

    #[test]
    fn snapshot_count_match_requires_one_complete_interval() {
        let mut metrics = IndependentMetrics::default();
        metrics.record("Put", Duration::from_millis(1));
        metrics.record("Get", Duration::from_millis(1));

        let complete = serde_json::json!({
            "requests": [
                { "name": "Put", "httpRequestCount": 1 },
                { "name": "Get", "httpRequestCount": 1 }
            ]
        });
        let split = serde_json::json!({
            "requests": [
                { "name": "Put", "httpRequestCount": 1 },
                { "name": "Get", "httpRequestCount": 0 }
            ]
        });

        assert!(snapshot_has_expected_counts(&complete, &metrics));
        assert!(!snapshot_has_expected_counts(&split, &metrics));
    }
}
