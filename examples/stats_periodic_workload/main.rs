//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Periodic stats validation workload.
//
// Default run:
//   cargo run --example stats_periodic_workload -- localhost 8080
//
// Optional args:
//   cargo run --example stats_periodic_workload -- <host> <port> <duration_sec> <interval_sec> <profile> <cycles_per_sec> <percentile_mode> <workers>
//
// Default values:
//   host=localhost, port=8080, duration_sec=120, interval_sec=5, profile=MORE, cycles_per_sec=1, percentile_mode=EXACT, workers=auto
//
// Optional CloudSim table name and limits can be set through environment variables:
//   STATS_TABLE_NAME=sdk_stats_periodic_high STATS_TABLE_READ_UNITS=50000 STATS_TABLE_WRITE_UNITS=50000 cargo run --example stats_periodic_workload -- localhost 8080 300 5 MORE 5000 HDR 512
//
// Each cycle performs exactly one Put, one Get, and one Delete.
// The final summary checks deterministic request counts across all emitted
// stats snapshots and computes rough throughput from those counts.

use oracle_nosql_rust_sdk::types::MapValue;
use oracle_nosql_rust_sdk::{
    DeleteRequest, GetRequest, Handle, HandleMode, NoSQLError, PutRequest, StatsPercentileMode,
    StatsProfile, StatsSnapshot, TableRequest,
};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant as StdInstant};
use tokio::task::JoinSet;
use tokio::time::{sleep, sleep_until, Instant as TokioInstant};

const DEFAULT_TABLE: &str = "sdk_stats_periodic_demo";
const DEFAULT_TABLE_READ_UNITS: i32 = 10_000;
const DEFAULT_TABLE_WRITE_UNITS: i32 = 10_000;
const DEFAULT_TABLE_STORAGE_GB: i32 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DemoMode {
    Onprem,
    Cloudsim,
}

impl DemoMode {
    fn from_env() -> DemoMode {
        match env::var("NOSQL_DEMO_MODE")
            .unwrap_or_else(|_| "cloudsim".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "onprem" | "proxy" => DemoMode::Onprem,
            _ => DemoMode::Cloudsim,
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    endpoint: String,
    mode: DemoMode,
    duration_secs: u64,
    interval_secs: u64,
    profile: StatsProfile,
    cycles_per_sec: u64,
    percentile_mode: StatsPercentileMode,
    worker_count: usize,
    table_name: String,
    table_read_units: i32,
    table_write_units: i32,
    table_storage_gb: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_config()?;
    if config.profile == StatsProfile::None {
        return fail("stats_periodic_workload requires REGULAR, MORE, or ALL");
    }

    println!("Using endpoint: {}", config.endpoint);
    println!("Using mode: {:?}", config.mode);
    println!("Using stats profile: {}", config.profile);
    println!("Using stats percentile mode: {}", config.percentile_mode);
    println!("Using table: {}", config.table_name);
    println!(
        "CloudSim table limits: read={} write={} storage_gb={}",
        config.table_read_units, config.table_write_units, config.table_storage_gb
    );
    println!("Stats interval: {}s", config.interval_secs);
    println!("Workload duration: {}s", config.duration_secs);
    println!("Cycles per second: {}", config.cycles_per_sec);
    println!("Concurrent workers: {}", config.worker_count);
    println!(
        "Target request rate: {} requests/sec",
        config.cycles_per_sec.saturating_mul(3)
    );
    println!("Each cycle: 1 Put + 1 Get + 1 Delete");

    ensure_schema(&config).await?;

    let snapshots = Arc::new(Mutex::new(Vec::<Value>::new()));
    let handle = create_handle(&config, Some(snapshots.clone())).await?;
    let stats_control = handle.get_stats_control();
    stats_control.start();

    let expected_per_request = run_deterministic_workload(
        &handle,
        &config.table_name,
        config.duration_secs,
        config.cycles_per_sec,
        config.worker_count,
    )
    .await?;

    println!(
        "Workload finished. Waiting for two stats intervals so final and empty intervals can emit..."
    );
    sleep(Duration::from_secs(config.interval_secs * 2 + 1)).await;
    stats_control.stop();
    sleep(Duration::from_millis(200)).await;

    validate_snapshots(&snapshots.lock().unwrap(), expected_per_request, &config)?;
    println!("Periodic stats workload validation passed.");
    Ok(())
}

fn parse_config() -> Result<Config, Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let host = args.get(1).map(String::as_str).unwrap_or("localhost");
    let port = args.get(2).map(String::as_str).unwrap_or("8080");
    let duration_secs = parse_arg(&args, 3, 120)?;
    let interval_secs = parse_arg(&args, 4, 5)?;
    let profile = args
        .get(5)
        .map(String::as_str)
        .unwrap_or("MORE")
        .parse::<StatsProfile>()?;
    let cycles_per_sec = parse_arg(&args, 6, 1)?;
    let percentile_mode = args
        .get(7)
        .map(String::as_str)
        .unwrap_or("EXACT")
        .parse::<StatsPercentileMode>()?;
    let worker_count = parse_worker_count(&args, cycles_per_sec)?;
    let table_read_units = parse_env_i32("STATS_TABLE_READ_UNITS", DEFAULT_TABLE_READ_UNITS)?;
    let table_write_units = parse_env_i32("STATS_TABLE_WRITE_UNITS", DEFAULT_TABLE_WRITE_UNITS)?;
    let table_storage_gb = parse_env_i32("STATS_TABLE_STORAGE_GB", DEFAULT_TABLE_STORAGE_GB)?;
    let table_name = env::var("STATS_TABLE_NAME").unwrap_or_else(|_| DEFAULT_TABLE.to_string());

    Ok(Config {
        endpoint: format!("http://{}:{}", host, port),
        mode: DemoMode::from_env(),
        duration_secs,
        interval_secs,
        profile,
        cycles_per_sec,
        percentile_mode,
        worker_count,
        table_name,
        table_read_units,
        table_write_units,
        table_storage_gb,
    })
}

fn parse_arg(args: &[String], index: usize, default_value: u64) -> Result<u64, Box<dyn Error>> {
    Ok(args
        .get(index)
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(default_value))
}

fn parse_worker_count(args: &[String], cycles_per_sec: u64) -> Result<usize, Box<dyn Error>> {
    let value = if let Some(value) = args.get(8) {
        value.parse::<usize>()?
    } else if let Some(value) = parse_env_usize("STATS_WORKERS")? {
        value
    } else {
        default_worker_count(cycles_per_sec)
    };
    if value == 0 {
        return fail("worker count must be greater than zero");
    }
    Ok(value)
}

fn parse_env_usize(name: &str) -> Result<Option<usize>, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value.parse::<usize>()?;
            if parsed == 0 {
                return fail(&format!("{name} must be greater than zero"));
            }
            Ok(Some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(Box::new(err)),
    }
}

fn default_worker_count(cycles_per_sec: u64) -> usize {
    if cycles_per_sec <= 100 {
        1
    } else {
        cycles_per_sec.div_ceil(25).clamp(1, 512) as usize
    }
}

fn parse_env_i32(name: &str, default_value: i32) -> Result<i32, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value.parse::<i32>()?;
            if parsed <= 0 {
                return fail(&format!("{name} must be greater than zero"));
            }
            Ok(parsed)
        }
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(err) => Err(Box::new(err)),
    }
}

async fn create_handle(
    config: &Config,
    snapshots: Option<Arc<Mutex<Vec<Value>>>>,
) -> Result<Handle, NoSQLError> {
    let mut builder = Handle::builder()
        .endpoint(&config.endpoint)?
        .timeout(Duration::from_secs(10))?
        .stats_profile(config.profile)?
        .stats_interval(Duration::from_secs(config.interval_secs))?
        .stats_pretty_print(true)?
        .stats_percentile_mode(config.percentile_mode)?
        .stats_enable_log(false)?;

    if let Some(snapshots) = snapshots {
        builder = builder.stats_handler(move |stats: &StatsSnapshot| {
            let snapshot: Value = serde_json::from_str(stats.as_json()).unwrap();
            print_snapshot_summary(&snapshot);
            snapshots.lock().unwrap().push(snapshot);
        })?;
    }

    builder = match config.mode {
        DemoMode::Onprem => builder.onprem_auth("", "")?,
        DemoMode::Cloudsim => builder.mode(HandleMode::Cloudsim)?,
    };

    builder.build().await
}

async fn ensure_schema(config: &Config) -> Result<(), Box<dyn Error>> {
    println!("\nEnsuring schema exists...");
    let setup_config = Config {
        profile: StatsProfile::None,
        ..config.clone()
    };
    let handle = create_handle(&setup_config, None).await?;

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {} (id INTEGER, payload STRING, PRIMARY KEY(id))",
        config.table_name
    );
    let mut request = TableRequest::new(&config.table_name).statement(&ddl);
    if config.mode == DemoMode::Cloudsim {
        request = request.limits(&oracle_nosql_rust_sdk::types::TableLimits::provisioned(
            config.table_read_units,
            config.table_write_units,
            config.table_storage_gb,
        ));
    }

    let mut result = request.execute(&handle).await?;
    result
        .wait_for_completion_ms(&handle, 120_000, 1000)
        .await?;
    println!("CREATE TABLE state: {:?}", result.state());
    Ok(())
}

async fn run_deterministic_workload(
    handle: &Handle,
    table_name: &str,
    duration_secs: u64,
    cycles_per_sec: u64,
    worker_count: usize,
) -> Result<u64, Box<dyn Error>> {
    let total_cycles = duration_secs
        .checked_mul(cycles_per_sec)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cycle count overflow"))?;
    let expected_per_request = total_cycles;
    let started = StdInstant::now();
    let schedule_started = TokioInstant::now();
    let base_id = current_millis() as i32;
    let completed_cycles = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();

    println!("\nStarting deterministic concurrent workload...");
    println!(
        "Target cycles: {} using {} workers",
        total_cycles, worker_count
    );

    for worker_index in 0..worker_count {
        let worker_handle = handle.clone();
        let worker_table = table_name.to_string();
        let worker_completed = completed_cycles.clone();
        workers.spawn(async move {
            let mut completed_by_worker = 0_u64;
            let mut cycle_index = worker_index as u64;
            while cycle_index < total_cycles {
                let scheduled_offset =
                    Duration::from_secs_f64(cycle_index as f64 / cycles_per_sec as f64);
                sleep_until(schedule_started + scheduled_offset).await;
                let id = base_id.wrapping_add((cycle_index as i32).wrapping_add(1));
                put_get_delete_cycle(&worker_handle, &worker_table, id)
                    .await
                    .map_err(|err| format!("{err:?}"))?;
                completed_by_worker += 1;
                worker_completed.fetch_add(1, Ordering::Relaxed);
                cycle_index += worker_count as u64;
            }
            Ok::<u64, String>(completed_by_worker)
        });
    }

    let progress_completed = completed_cycles.clone();
    let progress_task = tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(10)).await;
            let completed = progress_completed.load(Ordering::Relaxed);
            let elapsed = started.elapsed().as_secs().min(duration_secs);
            println!(
                "Completed approximately {} / {} workload seconds; cycles issued {} / {}",
                elapsed, duration_secs, completed, total_cycles
            );
            if completed >= total_cycles {
                break;
            }
        }
    });

    let mut completed_by_workers = 0_u64;
    let mut worker_error = None;
    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(count)) => {
                completed_by_workers += count;
            }
            Ok(Err(err)) => {
                worker_error = Some(err);
                workers.abort_all();
                break;
            }
            Err(err) => {
                worker_error = Some(err.to_string());
                workers.abort_all();
                break;
            }
        }
    }
    while workers.join_next().await.is_some() {}
    progress_task.abort();
    let _ = progress_task.await;

    if let Some(err) = worker_error {
        return fail(&format!("concurrent workload failed: {err}"));
    }
    if completed_by_workers != total_cycles {
        return fail(&format!(
            "concurrent workload issued {} cycles, expected {}",
            completed_by_workers, total_cycles
        ));
    }

    println!(
        "Workload issued {} Put, {} Get, and {} Delete requests in {:.2}s.",
        expected_per_request,
        expected_per_request,
        expected_per_request,
        started.elapsed().as_secs_f64()
    );
    Ok(expected_per_request)
}

async fn put_get_delete_cycle(
    handle: &Handle,
    table_name: &str,
    id: i32,
) -> Result<(), NoSQLError> {
    PutRequest::new(table_name)
        .value(row_value(id))
        .execute(handle)
        .await?;

    GetRequest::new(table_name)
        .key(primary_key(id))
        .execute(handle)
        .await?;

    DeleteRequest::new(table_name, primary_key(id))
        .execute(handle)
        .await?;

    Ok(())
}

fn primary_key(id: i32) -> MapValue {
    MapValue::new().i32("id", id)
}

fn row_value(id: i32) -> MapValue {
    MapValue::new()
        .i32("id", id)
        .string("payload", format!("payload-{}", id))
}

fn print_snapshot_summary(snapshot: &Value) {
    let put = request_count(snapshot, "Put");
    let get = request_count(snapshot, "Get");
    let delete = request_count(snapshot, "Delete");
    println!(
        "Stats interval {} -> {} | Put={} {} | Get={} {} | Delete={} {} | requests={}",
        snapshot["startTime"].as_str().unwrap_or("?"),
        snapshot["endTime"].as_str().unwrap_or("?"),
        put,
        latency_summary(snapshot, "Put"),
        get,
        latency_summary(snapshot, "Get"),
        delete,
        latency_summary(snapshot, "Delete"),
        snapshot["requests"].as_array().map_or(0, Vec::len)
    );
}

fn validate_snapshots(
    snapshots: &[Value],
    expected_per_request: u64,
    config: &Config,
) -> Result<(), Box<dyn Error>> {
    let mut totals = HashMap::<String, u64>::new();
    let mut latency_entries = HashMap::<String, u64>::new();
    let mut empty_intervals = 0_u64;

    for snapshot in snapshots {
        let requests = snapshot["requests"].as_array().unwrap();
        if requests.is_empty() {
            empty_intervals += 1;
        }
        for request in requests {
            let name = request["name"].as_str().unwrap();
            let count = request["httpRequestCount"].as_u64().unwrap();
            *totals.entry(name.to_string()).or_default() += count;
            if matches!(name, "Put" | "Get" | "Delete") {
                assert_latency_is_numeric(request, config.profile)?;
                *latency_entries.entry(name.to_string()).or_default() += 1;
            }
        }
    }

    let put = totals.get("Put").copied().unwrap_or(0);
    let get = totals.get("Get").copied().unwrap_or(0);
    let delete = totals.get("Delete").copied().unwrap_or(0);
    let put_latency_entries = latency_entries.get("Put").copied().unwrap_or(0);
    let get_latency_entries = latency_entries.get("Get").copied().unwrap_or(0);
    let delete_latency_entries = latency_entries.get("Delete").copied().unwrap_or(0);
    let total = put + get + delete;
    let expected_total = expected_per_request * 3;
    let expected_min_snapshots = config.duration_secs / config.interval_secs;

    println!("\nPeriodic stats validation summary");
    println!(
        "Snapshots observed: expected_at_least={} actual={}",
        expected_min_snapshots,
        snapshots.len()
    );
    println!("Empty intervals observed: {}", empty_intervals);
    println!(
        "Put count:    expected={} actual={}",
        expected_per_request, put
    );
    println!(
        "Get count:    expected={} actual={}",
        expected_per_request, get
    );
    println!(
        "Delete count: expected={} actual={}",
        expected_per_request, delete
    );
    println!(
        "Latency entries checked: Put={} Get={} Delete={}",
        put_latency_entries, get_latency_entries, delete_latency_entries
    );
    println!(
        "Rough throughput: {:.3} requests/sec",
        total as f64 / config.duration_secs as f64
    );

    if snapshots.len() < expected_min_snapshots as usize {
        return fail(&format!(
            "expected at least {} stats snapshots, got {}",
            expected_min_snapshots,
            snapshots.len()
        ));
    }
    if empty_intervals == 0 {
        return fail("expected at least one empty interval after workload completion");
    }
    if put != expected_per_request || get != expected_per_request || delete != expected_per_request
    {
        return fail(&format!(
            "request totals mismatch: expected each={}, actual Put={}, Get={}, Delete={}",
            expected_per_request, put, get, delete
        ));
    }
    if total != expected_total {
        return fail(&format!(
            "total request mismatch: expected={}, actual={}",
            expected_total, total
        ));
    }
    if put_latency_entries == 0 || get_latency_entries == 0 || delete_latency_entries == 0 {
        return fail(&format!(
            "latency was not checked for all request types: Put={}, Get={}, Delete={}",
            put_latency_entries, get_latency_entries, delete_latency_entries
        ));
    }

    Ok(())
}

fn assert_latency_is_numeric(request: &Value, profile: StatsProfile) -> Result<(), Box<dyn Error>> {
    let name = request["name"].as_str().unwrap_or("?");
    let Some(latency) = request.get("httpRequestLatencyMs") else {
        return fail(&format!("{name} is missing httpRequestLatencyMs"));
    };

    for field in ["min", "avg", "max"] {
        if !latency[field].is_number() {
            return fail(&format!(
                "{name}.httpRequestLatencyMs.{field} is not numeric: {}",
                latency[field]
            ));
        }
    }

    if matches!(profile, StatsProfile::More | StatsProfile::All) {
        for field in ["95th", "99th"] {
            if !latency[field].is_number() {
                return fail(&format!(
                    "{name}.httpRequestLatencyMs.{field} is not numeric: {}",
                    latency[field]
                ));
            }
        }
    }

    Ok(())
}

fn request_count(snapshot: &Value, name: &str) -> u64 {
    snapshot["requests"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|request| request["name"] == name)
        .and_then(|request| request["httpRequestCount"].as_u64())
        .unwrap_or(0)
}

fn latency_summary(snapshot: &Value, name: &str) -> String {
    let Some(request) = request_entry(snapshot, name) else {
        return "latency=n/a".to_string();
    };
    let Some(latency) = request.get("httpRequestLatencyMs") else {
        return "latency=n/a".to_string();
    };

    let avg = latency["avg"]
        .as_f64()
        .map(|value| format!("{:.3}ms", value))
        .unwrap_or_else(|| "n/a".to_string());
    let p95 = latency["95th"]
        .as_u64()
        .map(|value| format!(" p95={}ms", value))
        .unwrap_or_default();
    let p99 = latency["99th"]
        .as_u64()
        .map(|value| format!(" p99={}ms", value))
        .unwrap_or_default();

    format!("latency_avg={}{}{}", avg, p95, p99)
}

fn request_entry<'a>(snapshot: &'a Value, name: &str) -> Option<&'a Value> {
    snapshot["requests"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|request| request["name"] == name)
}

fn current_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn fail<T>(message: &str) -> Result<T, Box<dyn Error>> {
    Err(Box::new(io::Error::new(
        io::ErrorKind::Other,
        message.to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_config(profile: StatsProfile) -> Config {
        Config {
            endpoint: "http://localhost:8080".to_string(),
            mode: DemoMode::Cloudsim,
            duration_secs: 10,
            interval_secs: 5,
            profile,
            cycles_per_sec: 1,
            percentile_mode: StatsPercentileMode::Exact,
            worker_count: 1,
            table_name: DEFAULT_TABLE.to_string(),
            table_read_units: DEFAULT_TABLE_READ_UNITS,
            table_write_units: DEFAULT_TABLE_WRITE_UNITS,
            table_storage_gb: DEFAULT_TABLE_STORAGE_GB,
        }
    }

    fn request(name: &str, count: u64, avg_latency: f64) -> Value {
        json!({
            "httpRequestCount": count,
            "name": name,
            "httpRequestLatencyMs": {
                "min": 1,
                "avg": avg_latency,
                "max": 3,
                "95th": 3,
                "99th": 3
            }
        })
    }

    fn snapshot(put: u64, get: u64, delete: u64) -> Value {
        json!({
            "clientId": "test-client",
            "startTime": "2026-06-11T14:25:13Z",
            "endTime": "2026-06-11T14:25:18Z",
            "requests": [
                request("Put", put, 1.25),
                request("Get", get, 2.5),
                request("Delete", delete, 3.75)
            ]
        })
    }

    fn empty_snapshot() -> Value {
        json!({
            "clientId": "test-client",
            "startTime": "2026-06-11T14:25:18Z",
            "endTime": "2026-06-11T14:25:23Z",
            "requests": []
        })
    }

    #[test]
    fn request_count_returns_named_request_count() {
        let stats = snapshot(5, 4, 3);

        assert_eq!(request_count(&stats, "Put"), 5);
        assert_eq!(request_count(&stats, "Get"), 4);
        assert_eq!(request_count(&stats, "Delete"), 3);
        assert_eq!(request_count(&stats, "Query"), 0);
    }

    #[test]
    fn latency_summary_formats_average_and_percentiles() {
        let stats = snapshot(5, 5, 5);

        assert_eq!(
            latency_summary(&stats, "Put"),
            "latency_avg=1.250ms p95=3ms p99=3ms"
        );
        assert_eq!(latency_summary(&stats, "Query"), "latency=n/a");
    }

    #[test]
    fn validate_snapshots_accepts_expected_counts_and_latency() {
        let snapshots = vec![snapshot(5, 5, 5), snapshot(5, 5, 5), empty_snapshot()];
        let config = test_config(StatsProfile::More);

        validate_snapshots(&snapshots, 10, &config).unwrap();
    }

    #[test]
    fn validate_snapshots_rejects_missing_latency() {
        let snapshots = vec![
            json!({
                "clientId": "test-client",
                "startTime": "2026-06-11T14:25:13Z",
                "endTime": "2026-06-11T14:25:18Z",
                "requests": [
                    request("Put", 1, 1.0),
                    request("Get", 1, 1.0),
                    {
                        "httpRequestCount": 1,
                        "name": "Delete"
                    }
                ]
            }),
            empty_snapshot(),
        ];
        let config = test_config(StatsProfile::More);

        let error = validate_snapshots(&snapshots, 1, &config).unwrap_err();
        assert!(error.to_string().contains("Delete is missing"));
    }

    #[test]
    fn regular_profile_latency_does_not_require_percentiles() {
        let request = json!({
            "httpRequestCount": 1,
            "name": "Put",
            "httpRequestLatencyMs": {
                "min": 1,
                "avg": 1.5,
                "max": 2
            }
        });

        assert!(assert_latency_is_numeric(&request, StatsProfile::Regular).is_ok());
        assert!(assert_latency_is_numeric(&request, StatsProfile::More).is_err());
    }
}
