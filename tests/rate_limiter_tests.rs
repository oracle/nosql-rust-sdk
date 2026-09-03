//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use oracle_nosql_rust_sdk::types::{Consistency, MapValue, TableLimits, TableState};
use oracle_nosql_rust_sdk::{
    GetRequest, GetTableRequest, Handle, HandleMode, PutRequest, TableRequest,
};
use std::env;
use std::error::Error;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CLOUDSIM_ENDPOINT: &str = "http://127.0.0.1:8080";
const LIMIT_UNITS_PER_SECOND: i32 = 100;
const RATE_TEST_OPERATIONS: i32 = 250;

fn skip_for_onprem_mode() -> bool {
    env::var("ORACLE_NOSQL_AUTH")
        .map(|mode| {
            matches!(
                mode.trim().to_ascii_lowercase().as_str(),
                "onprem" | "sonprem"
            )
        })
        .unwrap_or(false)
}

async fn cloudsim_handle(rate_limiting_enabled: bool) -> Result<Handle, Box<dyn Error>> {
    Ok(Handle::builder()
        .endpoint(CLOUDSIM_ENDPOINT)?
        .mode(HandleMode::Cloudsim)?
        .timeout(Duration::from_secs(60))?
        .rate_limiting_enabled(rate_limiting_enabled)?
        .build()
        .await?)
}

fn unique_table_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be after unix epoch")
        .as_nanos();
    format!("{}_{}_{}", prefix, std::process::id(), nanos)
}

async fn create_rate_limited_table(
    handle: &Handle,
    table_name: &str,
) -> Result<(), Box<dyn Error>> {
    let mut result = TableRequest::new(table_name)
        .statement(&format!(
            "create table if not exists {} (id integer, payload string, primary key(id))",
            table_name
        ))
        .limits(&TableLimits::provisioned(
            LIMIT_UNITS_PER_SECOND,
            LIMIT_UNITS_PER_SECOND,
            10,
        ))
        .execute(handle)
        .await?;

    if !result.operation_id().is_empty() {
        result.wait_for_completion_ms(handle, 30000, 500).await?;
    } else if result.state() != TableState::Active {
        wait_for_table_active(handle, table_name).await?;
    }

    Ok(())
}

async fn drop_table(handle: &Handle, table_name: &str) -> Result<(), Box<dyn Error>> {
    let mut result = TableRequest::new(table_name)
        .statement(&format!("drop table if exists {}", table_name))
        .timeout(&Duration::from_secs(30))
        .execute(handle)
        .await?;

    if !result.operation_id().is_empty() {
        result.wait_for_completion_ms(handle, 30000, 500).await?;
    }

    Ok(())
}

async fn wait_for_table_active(handle: &Handle, table_name: &str) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if GetTableRequest::new(table_name)
            .execute(handle)
            .await?
            .state()
            == TableState::Active
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(format!("table {table_name} did not become active within 30 seconds").into())
}

async fn reset_rate_limiters(handle: &Handle, table_name: &str) -> Result<(), Box<dyn Error>> {
    GetTableRequest::new(table_name).execute(handle).await?;
    Ok(())
}

fn assert_rate_close_to_limit(
    operation: &str,
    consumed_units: i64,
    elapsed: Duration,
) -> Result<(), Box<dyn Error>> {
    if consumed_units <= 0 {
        return Err(format!("{operation} did not report consumed capacity").into());
    }

    let observed_rate = consumed_units as f64 / elapsed.as_secs_f64();
    let expected = LIMIT_UNITS_PER_SECOND as f64;
    let lower = expected * 0.80;
    let upper = expected * 1.20;

    println!(
        "{operation}: consumed={} elapsed={:?} observed_rate={:.2} units/sec",
        consumed_units, elapsed, observed_rate
    );

    if observed_rate < lower || observed_rate > upper {
        return Err(format!(
            "{operation} rate {observed_rate:.2} units/sec is not close to {expected:.2} units/sec"
        )
        .into());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limited_puts_are_close_to_table_write_limit() -> Result<(), Box<dyn Error>> {
    if skip_for_onprem_mode() {
        println!("skipping rate limiter test in onprem/sonprem mode");
        return Ok(());
    }

    let setup_handle = cloudsim_handle(false).await?;
    let table_name = unique_table_name("ratelimit_put");

    let result = async {
        create_rate_limited_table(&setup_handle, &table_name).await?;

        let rate_limited_handle = cloudsim_handle(true).await?;
        reset_rate_limiters(&rate_limited_handle, &table_name).await?;

        let start = Instant::now();
        let mut consumed_write_units = 0i64;
        for id in 0..RATE_TEST_OPERATIONS {
            let res = PutRequest::new(&table_name)
                .value(MapValue::new().i32("id", id).str("payload", "value"))
                .execute(&rate_limited_handle)
                .await?;
            let consumed = res
                .consumed()
                .ok_or("put result did not include consumed capacity")?;
            consumed_write_units += consumed.write_kb as i64;
        }

        assert_rate_close_to_limit("put", consumed_write_units, start.elapsed())
    }
    .await;

    let cleanup_result = drop_table(&setup_handle, &table_name).await;
    result?;
    cleanup_result?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limited_gets_are_close_to_table_read_limit() -> Result<(), Box<dyn Error>> {
    if skip_for_onprem_mode() {
        println!("skipping rate limiter test in onprem/sonprem mode");
        return Ok(());
    }

    let setup_handle = cloudsim_handle(false).await?;
    let table_name = unique_table_name("ratelimit_get");

    let result = async {
        create_rate_limited_table(&setup_handle, &table_name).await?;
        PutRequest::new(&table_name)
            .value(MapValue::new().i32("id", 1).str("payload", "value"))
            .execute(&setup_handle)
            .await?;

        let rate_limited_handle = cloudsim_handle(true).await?;
        reset_rate_limiters(&rate_limited_handle, &table_name).await?;

        let start = Instant::now();
        let mut consumed_read_units = 0i64;
        for _ in 0..RATE_TEST_OPERATIONS {
            let res = GetRequest::new(&table_name)
                .key(MapValue::new().i32("id", 1))
                .consistency(Consistency::Eventual)
                .execute(&rate_limited_handle)
                .await?;
            if res.row().is_none() {
                return Err("get result did not include the expected row".into());
            }
            let consumed = res
                .consumed()
                .ok_or("get result did not include consumed capacity")?;
            consumed_read_units += consumed.read_units as i64;
        }

        assert_rate_close_to_limit("get", consumed_read_units, start.elapsed())
    }
    .await;

    let cleanup_result = drop_table(&setup_handle, &table_name).await;
    result?;
    cleanup_result?;
    Ok(())
}
