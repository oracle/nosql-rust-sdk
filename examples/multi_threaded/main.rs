//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// This is an example program showing how a NoSQL Handle can be used across
// multiple threads. The program doesn't do anything very useful, it just creates
// a table then spawns a bunch of threads to insert/update/put/get/delete/select
// table data in loops until a certain amount of time expires. It then drops the
// table.

// To run this example:
//    cargo run --example multi_threaded
//
// for extra output:
//    RUST_LOG=debug cargo run --example multi_threaded
//
// or, for a LOT of tracing output:
//    RUST_LOG=trace cargo run --example multi_threaded

// Parameters for runtime execution
// Highest ID to use in the table
const MAX_ID: u32 = 100;
// Amount of time to run each thread for
const RUNTIME_SECONDS: u64 = 10;

use oracle_nosql_rust_sdk::types::*;
use oracle_nosql_rust_sdk::DeleteRequest;
use oracle_nosql_rust_sdk::GetRequest;
use oracle_nosql_rust_sdk::Handle;
use oracle_nosql_rust_sdk::HandleMode;
use oracle_nosql_rust_sdk::NoSQLError;
use oracle_nosql_rust_sdk::PutRequest;
use oracle_nosql_rust_sdk::QueryRequest;
use oracle_nosql_rust_sdk::TableRequest;
use std::error::Error;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tracing::{debug, info, trace};

// This method shows various ways to configure a NoSQL Handle.
async fn get_handle() -> Result<Handle, NoSQLError> {
    // Note: later methods called on this builder will override earlier methods.
    // This allows for setting desired defaults that can be overridden by, for example,
    // .from_environment().
    Handle::builder()
        // Default to cloudsim, overridden by environment below
        .endpoint("http://localhost:8080")?
        .mode(HandleMode::Cloudsim)?
        //
        // For on-premises: endpoint, Onprem mode:
        // .endpoint("https://my.company.com:8080")?
        // .mode(HandleMode::Onprem)?
        // Optional file with onprem secure login credentials
        // .onprem_auth_from_file("/path/to/user_pass_file")?
        // Optional path to x509 certificate in PEM format
        // .onprem_auth_from_file("/path/to/certificate.pem")?
        //
        // For cloud, using user-based file:
        // .cloud_auth_from_file("~/.oci/config")?
        // Optional region, if not specified in above file
        // .cloud_region("us-ashburn-1")?
        // Optional full endpoint, if region is not recognized
        //.endpoint("https://nosql.us-unknown-1.oci.oraclecloud.com")?
        //
        // For cloud, using Instance Principal:
        // .cloud_auth_from_instance()?
        //
        // For cloud, using Resource Principal:
        // .cloud_auth_from_resource()?
        //
        // To read all of the above from environment variables:
        // or, to override above from environment;
        .from_environment()?
        //
        // Optional: set a different default timeout (default is 30 seconds)
        .timeout(Duration::from_secs(15))?
        //
        // Build the handle
        .build()
        .await
}

// simple convenience function to get current seconds since the epoch
fn now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// Example way to use multiple threaded tokio runtime
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    // Set up a tracing subscriber to see output based on RUST_LOG environment setting
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .with_ansi(false)
        .compact()
        .init();

    // Create a NoSQL database handle. This should be used throughout the program, across all threads
    info!("Creating new handle...");
    let handle = get_handle().await?;

    // Create an example table
    TableRequest::new("test_multi_thread")
        .statement(
            "create table if not exists test_multi_thread (id integer, name string, primary key(id))",
        )
        // the following line is only used in Cloud mode
        .limits(&TableLimits::provisioned(10000, 10000, 10))
        .execute(&handle)
        .await?
        // wait up to 15 seconds for table to be created
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    // set the time (in seconds since epoch) when all threads should end
    let end_time = now() + RUNTIME_SECONDS;

    // create a new set of tokio tasks to allow easy creation of threads
    let mut tasks = tokio::task::JoinSet::new();

    // Note: since the compiler doesn't know for sure that the handle will outlive
    // the created threads, we have to move cloned versions of the handle to each
    // thread we create. This is fine to do, since Handle uses an internal Arc to
    // allow for clones to use the same underlying data.
    //
    // It would be nice if the rust compiler had an "async clone" keyword that could be
    // used in place of "async move", as that would save a few lines and the extra closure.

    // - puts
    tasks.spawn({
        let h = handle.clone();
        let et = end_time.clone();
        async move {
            match do_puts(h, et).await {
                Ok(()) => return 0,
                Err(e) => {
                    info!("do_puts returned error: {}", e.to_string());
                    return 1;
                }
            }
        }
    });

    // - gets
    tasks.spawn({
        let h = handle.clone();
        let et = end_time.clone();
        async move {
            match do_gets(h, et).await {
                Ok(()) => return 0,
                Err(e) => {
                    info!("do_gets returned error: {}", e.to_string());
                    return 1;
                }
            }
        }
    });

    // - INSERTs
    tasks.spawn({
        let h = handle.clone();
        let et = end_time.clone();
        async move {
            match do_inserts(h, et).await {
                Ok(()) => return 0,
                Err(e) => {
                    info!("do_inserts returned error: {}", e.to_string());
                    return 1;
                }
            }
        }
    });

    // - UPDATEs
    tasks.spawn({
        let h = handle.clone();
        let et = end_time.clone();
        async move {
            match do_updates(h, et).await {
                Ok(()) => return 0,
                Err(e) => {
                    info!("do_updates returned error: {}", e.to_string());
                    return 1;
                }
            }
        }
    });

    // - SELECTs (single records)
    tasks.spawn({
        let h = handle.clone();
        let et = end_time.clone();
        async move {
            match do_selects(h, et).await {
                Ok(()) => return 0,
                Err(e) => {
                    info!("do_selects returned error: {}", e.to_string());
                    return 1;
                }
            }
        }
    });

    // - SELECTs (full table)
    tasks.spawn({
        let h = handle.clone();
        let et = end_time.clone();
        async move {
            match do_full_selects(h, et).await {
                Ok(()) => return 0,
                Err(e) => {
                    info!("do_full_selects returned error: {}", e.to_string());
                    return 1;
                }
            }
        }
    });

    // - deletes
    tasks.spawn({
        let h = handle.clone();
        let et = end_time.clone();
        async move {
            match do_deletes(h, et).await {
                Ok(()) => return 0,
                Err(e) => {
                    info!("do_deletes returned error: {}", e.to_string());
                    return 1;
                }
            }
        }
    });

    info!("All tasks started. Waiting for them to finish.");
    while tasks.join_next().await.is_some() {
        // TODO: accumulate return codes
    }

    info!("All tasks complete. Dropping table.");

    // Drop the table
    TableRequest::new("test_multi_thread")
        .statement("drop table if exists test_multi_thread")
        .execute(&handle)
        .await?
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    Ok(())
}

// Loop putting records into the table using PutRequest
async fn do_puts(handle: Handle, end_sec: u64) -> Result<(), Box<dyn Error>> {
    while now() <= end_sec {
        // Put a record into the table
        let id = (rand::random::<u32>() % MAX_ID) as i32;
        debug!("do_puts() writing record for id {}", id);
        let putres = PutRequest::new("test_multi_thread")
            .value(MapValue::new().i32("id", id).str("name", "jane"))
            .execute(&handle)
            .await?;
        trace!("PutResult={:?}", putres);
        // PutResult should have a version
        if putres.version().is_none() {
            return Err("PutRequest should have returned a version, but did not".into());
        }
        let ms = (rand::random::<u64>() % 100) + 5;
        debug!("do_puts() sleeping for {} ms", ms);
        sleep(Duration::from_millis(ms)).await;
    }
    Ok(())
}

async fn do_gets(handle: Handle, end_sec: u64) -> Result<(), Box<dyn Error>> {
    while now() <= end_sec {
        // Get a record from the table. In most cases this will fail until there
        // are a lot of records inserted.
        let id = (rand::random::<u32>() % MAX_ID) as i32;
        debug!("do_gets() reading record for id {}", id);
        let getres = GetRequest::new("test_multi_thread")
            .key(MapValue::new().i32("id", id))
            .execute(&handle)
            .await?;
        trace!("GetResult={:?}", getres);
        // GetResult should have a version
        if getres.version().is_none() {
            trace!("No row found for id {}", id);
        }
        let ms = (rand::random::<u64>() % 100) + 5;
        debug!("do_gets() sleeping for {} ms", ms);
        sleep(Duration::from_millis(ms)).await;
    }
    Ok(())
}

async fn do_deletes(handle: Handle, end_sec: u64) -> Result<(), Box<dyn Error>> {
    while now() <= end_sec {
        // Delete a record from the table (if it exists)
        let id = (rand::random::<u32>() % MAX_ID) as i32;

        debug!("do_deletes() deleting record for id {}", id);
        let delres = DeleteRequest::new("test_multi_thread", MapValue::new().i32("id", id))
            .execute(&handle)
            .await?;
        trace!("DeleteResult={:?}", delres);
        let ms = (rand::random::<u64>() % 100) + 10;
        debug!("do_deletes() sleeping for {} ms", ms);
        sleep(Duration::from_millis(ms)).await;
    }
    Ok(())
}

async fn do_inserts(handle: Handle, end_sec: u64) -> Result<(), Box<dyn Error>> {
    // Use a prepared statement for all inserts
    let stmt = "INSERT INTO test_multi_thread(id, name) VALUES($id, \"john\")";
    let res = QueryRequest::new(stmt)
        .prepare_only()
        .execute(&handle)
        .await?;
    let ps = res.prepared_statement();
    let mut insreq = QueryRequest::new_prepared(&ps);
    while now() <= end_sec {
        // Put a record into the table using prepared statement and bind variable
        let id = (rand::random::<u32>() % MAX_ID) as i32;
        debug!("do_inserts() writing record for id {}", id);
        insreq.set_variable("$id", &id)?;
        let qres = insreq.execute(&handle).await?;
        debug!("Insert QueryResult={:?}", qres);
        let ms = (rand::random::<u64>() % 100) + 5;
        debug!("do_inserts() sleeping for {} ms", ms);
        sleep(Duration::from_millis(ms)).await;
    }
    Ok(())
}

async fn do_selects(handle: Handle, end_sec: u64) -> Result<(), Box<dyn Error>> {
    // Use a prepared statement for all queries
    let stmt = "SELECT * FROM test_multi_thread where id = $id";
    let res = QueryRequest::new(stmt)
        .prepare_only()
        .execute(&handle)
        .await?;
    let ps = res.prepared_statement();
    let mut qreq = QueryRequest::new_prepared(&ps);
    while now() <= end_sec {
        // Get record(s) using prepared satement and bind variable
        let id = (rand::random::<u32>() % MAX_ID) as i32;
        debug!("do_selects() reading record for id {}", id);
        qreq.set_variable("$id", &id)?;
        let selres = qreq.execute(&handle).await?;
        debug!("Select QueryResult={:?}", selres);
        let ms = (rand::random::<u64>() % 100) + 5;
        debug!("do_selects() sleeping for {} ms", ms);
        sleep(Duration::from_millis(ms)).await;
    }
    Ok(())
}

async fn do_full_selects(handle: Handle, end_sec: u64) -> Result<(), Box<dyn Error>> {
    while now() <= end_sec {
        // Get records using a simple select statement
        let stmt = format!("SELECT * FROM test_multi_thread order by id");
        let selres = QueryRequest::new(&stmt).execute(&handle).await?;
        debug!("Full Select QueryResult={:?}", selres);
        let ms = (rand::random::<u64>() % 100) + 10;
        debug!("do_full_selects() sleeping for {} ms", ms);
        sleep(Duration::from_millis(ms)).await;
    }
    Ok(())
}

async fn do_updates(handle: Handle, end_sec: u64) -> Result<(), Box<dyn Error>> {
    // Use a prepared statement for all updates
    let stmt = "UPDATE test_multi_thread set name = 'martha' where id = $id";
    let res = QueryRequest::new(stmt)
        .prepare_only()
        .execute(&handle)
        .await?;
    let ps = res.prepared_statement();
    let mut updreq = QueryRequest::new_prepared(&ps);
    while now() <= end_sec {
        // Update record using prepared statement and bind variable
        let id = (rand::random::<u32>() % MAX_ID) as i32;
        debug!("do_updates() updating record for id {}", id);
        updreq.set_variable("$id", &id)?;
        let updres = updreq.execute(&handle).await?;
        debug!("Update Result={:?}", updres);
        let ms = (rand::random::<u64>() % 100) + 5;
        debug!("do_updates() sleeping for {} ms", ms);
        sleep(Duration::from_millis(ms)).await;
    }
    Ok(())
}
