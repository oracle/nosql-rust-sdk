//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Rust equivalent of StatsProfileOutputDemo.java.
//
// This example runs the same request mix under one selected stats profile so the
// terminal stats output can be compared with the Java SDK:
//   TableRequest, Put, Get, WriteMultiple, Prepare, Query, Delete,
//   MultiDelete, GetTable, ListTables, GetIndexes.
//
// Default endpoint is http://localhost:8080 and default mode is onprem to
// match the Java demo's proxy setup. For Cloud Simulator:
//   NOSQL_DEMO_MODE=cloudsim cargo run --example stats_profile_output_demo -- localhost 8080 ALL
//
// Optional host/port/profile:
//   cargo run --example stats_profile_output_demo -- localhost 8080 MORE HDR

use oracle_nosql_rust_sdk::types::{MapValue, NoSQLColumnToFieldValue};
use oracle_nosql_rust_sdk::{
    DeleteRequest, GetIndexesRequest, GetRequest, GetTableRequest, Handle, HandleMode,
    ListTablesRequest, MultiDeleteRequest, NoSQLError, PutRequest, QueryRequest,
    StatsPercentileMode, StatsProfile, StatsSnapshot, TableRequest, WriteMultipleRequest,
};
use std::env;
use std::error::Error;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tracing_subscriber::filter::EnvFilter;

const TABLE: &str = "sdk_stats_all_req_demo";
const INDEX_NAME: &str = "IdxSdkStatsAllReqDemo";
const STATS_INTERVAL_SECONDS: u64 = 5;
const ROWS_PER_PROFILE: i32 = 40;
const QUERIES_PER_PROFILE: i32 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DemoMode {
    Onprem,
    Cloudsim,
}

impl DemoMode {
    fn from_env() -> DemoMode {
        match env::var("NOSQL_DEMO_MODE")
            .unwrap_or_else(|_| "onprem".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "cloudsim" | "cloud" => DemoMode::Cloudsim,
            _ => DemoMode::Onprem,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .with_ansi(false)
        .compact()
        .init();

    let args: Vec<String> = env::args().collect();
    let proxy_host = args.get(1).map(String::as_str).unwrap_or("localhost");
    let proxy_port = args.get(2).map(String::as_str).unwrap_or("8080");
    let profile = args
        .get(3)
        .map(String::as_str)
        .unwrap_or("ALL")
        .parse::<StatsProfile>()?;
    let percentile_mode = args
        .get(4)
        .map(String::as_str)
        .unwrap_or("EXACT")
        .parse::<StatsPercentileMode>()?;
    let endpoint = format!("http://{}:{}", proxy_host, proxy_port);
    let mode = DemoMode::from_env();

    println!("Using proxy endpoint: {}", endpoint);
    println!("Using demo mode: {:?}", mode);
    println!("Using stats profile: {}", profile);
    println!("Using stats percentile mode: {}", percentile_mode);

    ensure_schema(&endpoint, mode).await?;

    run_profile(&endpoint, mode, profile, percentile_mode).await?;

    println!("\nDone.");
    Ok(())
}

async fn create_handle(
    endpoint: &str,
    mode: DemoMode,
    profile: StatsProfile,
    percentile_mode: StatsPercentileMode,
) -> Result<Handle, NoSQLError> {
    let mut builder = Handle::builder()
        .endpoint(endpoint)?
        .timeout(Duration::from_secs(10))?
        .stats_profile(profile)?
        .stats_interval(Duration::from_secs(STATS_INTERVAL_SECONDS))?
        .stats_pretty_print(true)?
        .stats_percentile_mode(percentile_mode)?
        .stats_handler(move |stats: &StatsSnapshot| {
            println!(
                "\n================ SDK STATS PROFILE: {} ================",
                profile
            );
            println!("{}", stats.as_json());
            println!("====================================================\n");
        })?;

    builder = match mode {
        DemoMode::Onprem => builder.onprem_auth("", "")?,
        DemoMode::Cloudsim => builder.mode(HandleMode::Cloudsim)?,
    };

    builder.build().await
}

fn create_table_ddl() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\
         profileName STRING, \
         grp STRING, \
         id INTEGER, \
         payload STRING, \
         operation STRING, \
         idx LONG, \
         PRIMARY KEY (SHARD(profileName, grp), id))",
        TABLE
    )
}

fn create_index_ddl() -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {}(idx)",
        INDEX_NAME, TABLE
    )
}

async fn ensure_schema(endpoint: &str, mode: DemoMode) -> Result<(), Box<dyn Error>> {
    println!("\nEnsuring schema exists...");

    let handle = create_handle(
        endpoint,
        mode,
        StatsProfile::None,
        StatsPercentileMode::Exact,
    )
    .await?;
    run_table_ddl(&handle, mode, &create_table_ddl(), "Initial CREATE TABLE").await?;
    run_table_ddl(&handle, mode, &create_index_ddl(), "Initial CREATE INDEX").await?;
    Ok(())
}

async fn run_table_ddl(
    handle: &Handle,
    mode: DemoMode,
    ddl: &str,
    label: &str,
) -> Result<(), Box<dyn Error>> {
    let mut request = TableRequest::new(TABLE).statement(ddl);

    if mode == DemoMode::Cloudsim && ddl.to_ascii_uppercase().contains("CREATE TABLE") {
        request = request.limits(&oracle_nosql_rust_sdk::types::TableLimits::provisioned(
            1000, 1000, 10,
        ));
    }

    let mut result = request.execute(handle).await?;
    result.wait_for_completion_ms(handle, 120_000, 1000).await?;
    println!("{} state: {:?}", label, result.state());
    Ok(())
}

async fn run_profile(
    endpoint: &str,
    mode: DemoMode,
    profile: StatsProfile,
    percentile_mode: StatsPercentileMode,
) -> Result<(), Box<dyn Error>> {
    println!("\n\n");
    println!("====================================================");
    println!("Running workload for profile: {}", profile);
    println!("====================================================");

    if profile == StatsProfile::None {
        println!("Expected behavior: NONE should not emit stats JSON.");
    }

    let handle = create_handle(endpoint, mode, profile, percentile_mode).await?;
    let stats_control = handle.get_stats_control();
    stats_control.start();

    let profile_name = profile.as_str().to_string();
    let run_id = format!(
        "{}_{}",
        profile.as_str().to_ascii_lowercase(),
        current_millis()
    );

    table_request_workload(&handle, mode).await?;
    put_workload(&handle, &profile_name, &run_id, ROWS_PER_PROFILE).await?;
    get_workload(&handle, &profile_name, &run_id).await?;
    write_multiple_workload(&handle, &profile_name, &run_id).await?;
    prepare_workload(&handle, &profile_name).await?;
    query_workload(&handle, &profile_name, &run_id, QUERIES_PER_PROFILE).await?;
    delete_workload(&handle, &profile_name, &run_id).await?;
    multi_delete_workload(&handle, &profile_name, &run_id).await?;
    metadata_workload(&handle).await?;

    println!(
        "Workload finished for {}. Waiting for stats interval...",
        profile
    );
    sleep(Duration::from_secs(STATS_INTERVAL_SECONDS + 3)).await;

    stats_control.stop();
    sleep(Duration::from_secs(1)).await;

    Ok(())
}

async fn table_request_workload(handle: &Handle, mode: DemoMode) -> Result<(), Box<dyn Error>> {
    run_table_ddl(
        handle,
        mode,
        &create_table_ddl(),
        "Profile CREATE TABLE IF EXISTS",
    )
    .await?;
    run_table_ddl(
        handle,
        mode,
        &create_index_ddl(),
        "Profile CREATE INDEX IF EXISTS",
    )
    .await?;

    println!("TableRequest workload completed.");
    Ok(())
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
        .string("payload", format!("payload-{}-{}", operation, id))
        .str("operation", operation)
        .i64("idx", i64::from(id))
}

async fn put_workload(
    handle: &Handle,
    profile_name: &str,
    run_id: &str,
    rows: i32,
) -> Result<(), Box<dyn Error>> {
    for i in 1..=rows {
        let group = format!("{}_g{}", run_id, i % 5);
        PutRequest::new(TABLE)
            .value(row_value(profile_name, &group, i, "PUT_WORKLOAD"))
            .if_absent()
            .return_row(true)
            .execute(handle)
            .await?;
    }

    println!("Put workload completed. Rows attempted: {}", rows);
    Ok(())
}

async fn get_workload(
    handle: &Handle,
    profile_name: &str,
    run_id: &str,
) -> Result<(), Box<dyn Error>> {
    for i in 1..=10 {
        let group = format!("{}_g{}", run_id, i % 5);
        GetRequest::new(TABLE)
            .key(primary_key(profile_name, &group, i))
            .execute(handle)
            .await?;
    }

    println!("Get workload completed.");
    Ok(())
}

async fn delete_workload(
    handle: &Handle,
    profile_name: &str,
    run_id: &str,
) -> Result<(), Box<dyn Error>> {
    for i in 1..=5 {
        let group = format!("{}_g{}", run_id, i % 5);
        DeleteRequest::new(TABLE, primary_key(profile_name, &group, i))
            .return_row(true)
            .execute(handle)
            .await?;
    }

    println!("Delete workload completed.");
    Ok(())
}

async fn write_multiple_workload(
    handle: &Handle,
    profile_name: &str,
    run_id: &str,
) -> Result<(), Box<dyn Error>> {
    let group = format!("{}_wm", run_id);

    PutRequest::new(TABLE)
        .value(row_value(
            profile_name,
            &group,
            1003,
            "WRITE_MULTIPLE_DELETE_TARGET",
        ))
        .execute(handle)
        .await?;

    let put1 = PutRequest::new(TABLE).value(row_value(
        profile_name,
        &group,
        1001,
        "WRITE_MULTIPLE_PUT_1",
    ));
    let put2 = PutRequest::new(TABLE).value(row_value(
        profile_name,
        &group,
        1002,
        "WRITE_MULTIPLE_PUT_2",
    ));
    let delete_req = DeleteRequest::new(TABLE, primary_key(profile_name, &group, 1003));

    WriteMultipleRequest::new(TABLE)
        .add(Box::new(put1))
        .add(Box::new(put2))
        .add(Box::new(delete_req))
        .execute(handle)
        .await?;

    println!("WriteMultiple workload completed.");
    Ok(())
}

async fn multi_delete_workload(
    handle: &Handle,
    profile_name: &str,
    run_id: &str,
) -> Result<(), Box<dyn Error>> {
    let group = format!("{}_md", run_id);

    for i in 1..=5 {
        PutRequest::new(TABLE)
            .value(row_value(
                profile_name,
                &group,
                2000 + i,
                "MULTI_DELETE_SETUP",
            ))
            .execute(handle)
            .await?;
    }

    let partial_key = MapValue::new()
        .str("profileName", profile_name)
        .str("grp", &group)
        .to_field_value();

    MultiDeleteRequest::new(TABLE, &partial_key)
        .execute(handle)
        .await?;

    println!("MultiDelete workload completed.");
    Ok(())
}

async fn prepare_workload(handle: &Handle, profile_name: &str) -> Result<(), Box<dyn Error>> {
    let sql = format!(
        "SELECT * FROM {} WHERE profileName = \"{}\"",
        TABLE, profile_name
    );

    QueryRequest::new(&sql)
        .prepare_only()
        .get_query_plan(true)
        .get_query_schema(true)
        .execute(handle)
        .await?;

    println!("Prepare workload completed.");
    Ok(())
}

async fn query_workload(
    handle: &Handle,
    profile_name: &str,
    run_id: &str,
    rounds: i32,
) -> Result<(), Box<dyn Error>> {
    for i in 0..rounds {
        let group = format!("{}_g{}", run_id, i % 5);
        let sql = format!(
            "SELECT * FROM {} WHERE profileName = \"{}\" AND grp = \"{}\"",
            TABLE, profile_name, group
        );

        let result = QueryRequest::new(&sql).execute(handle).await?;
        println!(
            "Query round {} for profile {} returned {} rows",
            i,
            profile_name,
            result.rows().len()
        );
    }

    println!("Query workload completed.");
    Ok(())
}

async fn metadata_workload(handle: &Handle) -> Result<(), Box<dyn Error>> {
    GetTableRequest::new(TABLE).execute(handle).await?;
    ListTablesRequest::new().limit(100).execute(handle).await?;
    GetIndexesRequest::new(TABLE).execute(handle).await?;

    println!("Metadata workload completed: GetTable, ListTables, GetIndexes.");
    Ok(())
}

fn current_millis() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
