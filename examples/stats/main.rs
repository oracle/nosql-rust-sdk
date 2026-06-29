//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// This example shows Java-style client statistics output in the terminal.
//
// To run this example:
//    cargo run --example stats
//
// To run with a specific profile:
//    STATS_PROFILE=more cargo run --example stats
//
// To choose percentile calculation:
//    STATS_PERCENTILE_MODE=hdr cargo run --example stats

use oracle_nosql_rust_sdk::Handle;
use oracle_nosql_rust_sdk::HandleMode;
use oracle_nosql_rust_sdk::ListTablesRequest;
use oracle_nosql_rust_sdk::NoSQLError;
use oracle_nosql_rust_sdk::StatsPercentileMode;
use oracle_nosql_rust_sdk::StatsProfile;
use std::env;
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;
use tracing_subscriber::filter::EnvFilter;

async fn get_handle() -> Result<Handle, NoSQLError> {
    let profile = env::var("STATS_PROFILE")
        .unwrap_or_else(|_| "all".to_string())
        .parse::<StatsProfile>()?;
    let percentile_mode = env::var("STATS_PERCENTILE_MODE")
        .unwrap_or_else(|_| "exact".to_string())
        .parse::<StatsPercentileMode>()?;

    Handle::builder()
        // Default to cloudsim, overridden by environment below.
        .endpoint("http://localhost:8080")?
        .mode(HandleMode::Cloudsim)?
        .from_environment()?
        .stats_profile(profile)?
        .stats_interval(Duration::from_secs(1))?
        .stats_pretty_print(true)?
        .stats_percentile_mode(percentile_mode)?
        .build()
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .with_ansi(false)
        .compact()
        .init();

    let handle = get_handle().await?;
    let stats = handle.get_stats_control();

    println!("stats profile: {}", stats.get_profile());
    println!("stats interval: {:?}", stats.get_interval());
    println!("stats percentile mode: {}", stats.get_percentile_mode());
    println!("stats pretty print: {}", stats.get_pretty_print());
    println!("stats started: {}", stats.is_started());

    let _ = ListTablesRequest::new().execute(&handle).await;

    sleep(Duration::from_secs(2)).await;

    Ok(())
}
