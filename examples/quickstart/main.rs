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
//    cargo run --example quickstart
//
// for extra output:
//    RUST_LOG=debug cargo run --example quickstart
//
// or, for a LOT of tracing output:
//    RUST_LOG=trace cargo run --example quickstart

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
use std::time::Duration;
use tracing::info;

// This method shows various ways to configure a NoSQL Handle.
async fn get_handle() -> Result<Handle, NoSQLError> {
    // Note: later methods called on this builder will override earlier methods.
    // This allows for setting desired defaults that can be overridden by, for example,
    // .from_environment().
    Handle::builder()
        // For local cloudsim: endpoint, Cloudsim mode:
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Set up a tracing subscriber to see output based on RUST_LOG environment setting
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .with_ansi(false)
        .compact()
        .init();

    // Create a handle. This should be used throughout the program
    info!("Creating new handle...");
    let handle = get_handle().await?;

    // Create an example table
    TableRequest::new("testusers")
        .statement(
            "create table if not exists testusers (id integer, name string,
            created timestamp(3), primary key(id))",
        )
        // the following line is only needed for Cloud mode
        .limits(&TableLimits::provisioned(1000, 1000, 10))
        .execute(&handle)
        .await?
        // wait up to 15 seconds for table to be created
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    // Put a record into the table
    // Note: ttl is a Duration, but internally is converted to a whole number of
    // hours or days. Minimum duration is one hour.
    let putres = PutRequest::new("testusers")
        .timeout(&Duration::from_millis(3000))
        .value(MapValue::new().i32("id", 10).str("name", "jane"))
        .ttl(&Duration::new(7200, 0))
        .execute(&handle)
        .await?;
    println!("PutResult={:?}", putres);
    // PutResult should have a version
    if putres.version().is_none() {
        return Err("PutRequest should have returned a version, but did not".into());
    }

    // Example of using if_version to overwrite existing record
    let putreq2 = PutRequest::new("testusers")
        .timeout(&Duration::from_millis(3000))
        .value(MapValue::new().i32("id", 10).str("name", "john"))
        .if_version(&putres.version().unwrap())
        .ttl(&Duration::new(7200, 0));
    println!("Putreq2={:?}", putreq2);
    let putres2 = putreq2.execute(&handle).await?;
    println!("PutResult2={:?}", putres2);

    // Get the record back
    let getres = GetRequest::new("testusers")
        .key(MapValue::new().i32("id", 10))
        .consistency(Consistency::Eventual)
        .execute(&handle)
        .await?;
    println!("GetResult={:?}", getres);
    // GetResult should have a version
    if getres.version().is_none() {
        return Err("GetRequest should have returned a version, but did not".into());
    }

    // write in some more records, so query below has more to return
    for i in 20..30 {
        //let name = format!("name{}", i);
        let name = "somename".to_string();
        let _ = PutRequest::new("testusers")
            .value(MapValue::new().i32("id", i).str("name", name.as_str()))
            .execute(&handle)
            .await?;
        // Optional delay between puts, if desired
        // tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }

    // Simple query, get all rows
    let qres = QueryRequest::new("select * from testusers")
        .execute(&handle)
        .await?;
    println!("QueryResult = {:?}", qres);

    // Query with ordering
    let qres1 = QueryRequest::new("select * from testusers order by id")
        .execute(&handle)
        .await?;
    println!("QueryResult1 = {:?}", qres1);

    // Example of how to remove a record with if_version
    let delres = DeleteRequest::new("testusers", MapValue::new().i32("id", 10))
        .if_version(&getres.version().unwrap())
        .execute(&handle)
        .await?;
    println!("delres={:?}", delres);

    // Drop the table
    TableRequest::new("testusers")
        .statement("drop table if exists testusers")
        .execute(&handle)
        .await?
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    Ok(())
}
