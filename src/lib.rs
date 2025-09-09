//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
//! Oracle NoSQL Database Rust SDK
//!
//! This is the Rust SDK for Oracle NoSQL Database. The SDK provides APIs and
//! [examples](https://github.com/oracle/nosql-rust-sdk/tree/main/examples) to help
//! developers write Rust applications that connect to the
//! [Oracle NoSQL Database Cloud Service](https://www.oracle.com/database/nosql-cloud.html),
//! the [Oracle NoSQL Database on-premises server](https://www.oracle.com/database/technologies/related/nosql.html)
//! and to the [Oracle NoSQL Cloud Simulator](https://www.oracle.com/downloads/cloud/nosql-cloud-sdk-downloads.html).
//!
//! This SDK supplies and uses Rust `async` methods throughout. There is currently no blocking support.
//!
//! The general flow for an application using the Oracle NoSQL Database is:
//! - Create a [`HandleBuilder`] with all needed parameters
//! - Create a [`Handle`] from the [`HandleBuilder`] that will be used throughout the application, across all threads
//! - Interact with the NoSQL Database using the [`Handle`] and Request structs such as [`GetRequest`], [`PutRequest`], [`QueryRequest`], etc.
//!
//! ## Simple Example
//! The following code creates a NoSQL [`Handle`] from values in the the current environment and then reads a single record from a table in the database. For a more complete example, see the [Quickstart](#quickstart) below.
//! ```no_run
//! use oracle_nosql_rust_sdk::{Handle, GetRequest};
//! use oracle_nosql_rust_sdk::types::MapValue;
//! use std::error::Error;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     let handle = Handle::builder()
//! #       .endpoint("http://localhost:8080")?
//! #       .mode(oracle_nosql_rust_sdk::HandleMode::Cloudsim)?
//!         .from_environment()?
//!         .build().await?;
//!     let getres = GetRequest::new("test_table")
//!         .key(MapValue::new().i32("id", 10))
//!         .execute(&handle)
//!         .await?;
//!     println!("GetResult={:?}", getres);
//!     Ok(())
//! }
//! ```
//!
//!
//! ## Prerequisites
//! - Rust 1.78 or later
//!   - Download and install a [Rust](https://www.rust-lang.org/tools/install) binary release suitable for your system. See the install and setup instructions on that page.
//! - Oracle NoSQL Database. Use one of the options:
//!   - Subscribe to the [Oracle NoSQL Database Cloud Service](https://www.oracle.com/database/nosql-cloud.html).
//!   - Download the [Oracle NoSQL Cloud Simulator](https://www.oracle.com/downloads/cloud/nosql-cloud-sdk-downloads.html).
//!   - Download the Oracle NoSQL Database Server (on-premise) and the Oracle NoSQL
//! Database Proxy (aka HTTP Proxy) at [here](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html).
//!     ***NOTE***: The on-premise configuration requires a running instance of the HTTP Proxy,
//!     see the [Oracle NoSQL Database Proxy and Driver](https://docs.oracle.com/en/database/other-databases/nosql-database/22.3/admin/proxy-and-driver.html) for HTTP proxy configuration information.
//!
//!
//! ## Installation
//! The Rust SDK for Oracle NoSQL Database is published as a Rust crate. It is
//! recommended to use the crates.io standard Rust mechanism for usage of this crate.
//!
//! To do so, add the following dependency to your `Cargo.toml` file:
//! ```text
//! [dependencies]
//! oracle-nosql-rust-sdk = "0.1"
//! ```
//!
//! ## Configuring the SDK
//!
//! This section describes configuring the SDK for the 3 environments supported:
//!
//! - **NoSQL DB Cloud Service**
//! - **Cloud Simulator**
//! - **NoSQL DB On-Premise**
//!
//! The areas where the environments and use differ are:
//!
//! - **Authentication and authorization.**
//!   - The **Cloud Service** is secure and requires a Cloud Service identity as well as authorization for desired operations.
//!   - The **Cloud Simulator** is not secure at all and requires no identity.
//!   - The **On-Premises** configuration can be either secure or not, and also requires an instance of the NoSQL DB Proxy service to access the on-premise database.
//! - **API differences.** Some types and methods are specific to an environment. For example, the on-premise configuration includes methods to create namespaces and users and these concepts don’t exist in the cloud service. Similarly, the cloud service includes interfaces to specify and acquire throughput information on tables that is not relevant on-premise. Such differences are noted in the API documentation.
//!
//! Before using the Cloud Service, it is recommended that users start with the Cloud Simulator to become familiar with the interfaces supported by the SDK.
//!
//! ### Configure for the Cloud Service
//!
//! The SDK requires an Oracle Cloud account and a subscription to the Oracle NoSQL Database Cloud Service. If you do not already have an Oracle Cloud account you can start [here](https://cloud.oracle.com/home).
//!
//! There are several ways of specifying the cloud service credentials to use, including:
//! - Instance Principals
//! - Resource Principals
//! - User Config File
//!
//! #### Using Instance Principal Credentials
//!
//! Instance Principal is an IAM service feature that enables instances to be authorized actors (or principals) to perform actions on service resources.
//! If the application is running on an OCI compute instance in the Oracle Cloud,
//! the SDK can make use of the instance environment to determine its credentials (no config file is required). Each compute instance has its own identity, and it authenticates using the certificates that are added to it. See [Calling Services from an Instance](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm) for prerequisite steps to set up Instance Principal.
//!
//! To configure NoSQL in this mode, use the [`HandleBuilder::cloud_auth_from_instance()`] method on the config struct.
//!
//! #### Using Resource Principal Credentials
//!
//! Resource Principal is an IAM service feature that enables the resources to be authorized actors (or principals) to perform actions on service resources. You may use Resource Principal when calling Oracle NoSQL Database Cloud Service from other Oracle Cloud service resource such as [Functions](https://docs.cloud.oracle.com/en-us/iaas/Content/Functions/Concepts/functionsoverview.htm). See [Accessing Other Oracle Cloud Infrastructure Resources from Running Functions](https://docs.cloud.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsaccessingociresources.htm) for how to set up Resource Principal.
//!
//! To configure NoSQL in this mode, use the [`HandleBuilder::cloud_auth_from_resource()`] method on the config struct.
//!
//! #### Using User Config File to Specify OCI Credentials
//!
//! Several pieces of information comprise your credentials used by the Oracle NoSQL Database Cloud Service:
//!
//! - Tenancy ID
//! - User ID
//! - Fingerprint
//! - Private Key File
//! - Passphrase (optional)
//! - Region (optional)
//!
//! Information about how to acquire this information is found in the [Required Keys and OCIDs](https://docs.cloud.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm) page. Specifically, these topics can be found on that page:
//!
//! - [Where to Get the Tenancy’s OCID and User’s OCID](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#Other)
//! - [How to Generate an API Signing Key](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How)
//! - [How to Get the Key’s Fingerprint](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How3)
//! - [How to Upload the Public Key](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How2)
//!
//! The way to supply the credentials is to use a credentials file, which by default is found in `$HOME/.oci/config` but the location can be specified in the API calls (see below).
//!
//! The format of the file is that of a properties file with the format of `key=value`, with one property per line. The contents and format are:
//!
//! ```ini
//! [DEFAULT]
//! tenancy=<your-tenancy-id>
//! user=<your-user-id>
//! fingerprint=<fingerprint-of-your-public-key>
//! key_file=<path-to-your-private-key-file>
//! pass_phrase=<optional-passphrase>
//! region=<optional-region-identifier>
//! ```
//!
//! Details of the configuration file can be found on the [SDK and Configuration File](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm) page. Note that multiple profiles can exist (using the `[PROFILENAME]` properties file convention) and can be selected using the API (see example below).
//!
//! The **Tenancy ID**, **User ID** and **fingerprint** should be acquired using the instructions above. The path to your private key file is the absolute path of the RSA private key. The order of the properties does not matter.
//!
//! The **region** is only required if [`HandleBuilder::cloud_region()`] is not used. It should specify the region of the NoSQL cloud service you are connecting to (for example: `us-phoenix-1`). For more information on regions, see [Regions and Availability Domains](https://docs.cloud.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm).
//!
//! The **pass_phrase** is only required if the RSA key file itself requires a passphrase.
//!
//! #### Using the Config to Connect an Application to the Cloud Service
//!
//! The first step in any Oracle NoSQL Database Cloud Service `rust` application is to create a [`Handle`] used to send requests to the service, based on the configuration given in a [`HandleBuilder`] struct. Instances of the Handle are safe for concurrent use by multiple goroutines and intended to be shared in a multi-threaded / async application.
//!
//! The following code example shows how to connect to the cloud service, using a user config file:
//!
//! ```no_run
//! # use oracle_nosql_rust_sdk::Handle;
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//!     let handle = Handle::builder()
//! 	    .cloud_auth_from_file("~/.oci/config")?
//!         .build().await?;
//!     // use handle for all NoSQL DB operations
//!     // ...
//! # Ok(())
//! # }
//! ```
//!
//! ### Configure for the Cloud Simulator
//!
//! The Oracle NoSQL Cloud Simulator is a useful way to use this SDK to connect to a local server that supports the same protocol.
//!
//! See [Download the Oracle NoSQL Cloud Simulator](https://www.oracle.com/downloads/cloud/nosql-cloud-sdk-downloads.html) to download and start the Cloud Simulator.
//!
//! The Cloud Simulator should not be used for deploying production applications or important data, but it does allow for rapid development and testing.
//!
//! The Cloud Simulator does not require the credentials and authentication information required by the Oracle NoSQL Database Cloud Service, but the SDK does require an
//! endpoint and the auth mode specifying Cloudsim:
//!
//! ```no_run
//! # use oracle_nosql_rust_sdk::{Handle, HandleMode};
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//!     let handle = Handle::builder()
//! 	    .mode(HandleMode::Cloudsim)?
//! 	    .endpoint("http://localhost:8080")?
//!         .build().await?;
//!     // use handle for all NoSQL DB operations
//!     // ...
//! # Ok(())
//! # }
//! ```
//!
//! ### Configure for the On-Premise Oracle NoSQL Database
//!
//! The on-premise configuration requires a running instance of the Oracle NoSQL database. In addition a running proxy service is required. See [Oracle NoSQL Database Downloads](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html) for downloads, and see [Information about the proxy](https://docs.oracle.com/en/database/other-databases/nosql-database/22.3/admin/proxy-and-driver.html) for proxy configuration information.
//!
//! In this case, the `endpoint` config parameter should point to the NoSQL proxy host and port location.
//!
//! If running a secure store, a user identity must be created in the store (separately) that has permission to perform the required operations of the application, such as manipulating tables and data. If the secure server has installed a certificate that is self-signed or is not trusted by the default system CA, specify [`HandleBuilder::danger_accept_invalid_certs()`] to instruct the client to skip verifying server's certificate, or specify the [`HandleBuilder::add_cert_from_pemfile()`] to verify server's certificate.
//!
//! ```no_run
//! # use oracle_nosql_rust_sdk::Handle;
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//!     let handle = Handle::builder()
//!        .endpoint("https://nosql.mycompany.com:8080")?
//!        .onprem_auth("testUser", "12345")?
//!        .danger_accept_invalid_certs(true)?
//!        // alternatively, specify a cert path
//!        // .add_cert_from_pemfile("/path/to/pemfile.pem")?
//!        .build().await?;
//!
//!     // use handle for all NoSQL DB operations
//!     // ...
//! # Ok(())
//! # }
//! ```
//!
//! If the store is not secure then the username and password are not required, but the mode
//! is still required to differentiate between onprem and cloudsim installations:
//!
//! ```no_run
//! # use oracle_nosql_rust_sdk::{Handle, HandleMode};
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//!     let handle = Handle::builder()
//!         .mode(HandleMode::Onprem)?
//!         .endpoint("http://nosql.mycompany.com:8080")?
//!         .build().await?;
//!     // use handle for all NoSQL DB operations
//!     // ...
//! # Ok(())
//! # }
//! ```
//!
//! ## Quickstart
//!
//! Below is a complete simple example program that opens a NoSQLDB handle, creates a simple table if it does not already exist, puts, gets, and deletes a row, then drops the table.
//!
//! The quickstart program can be used as a template which you can copy and modify as desired:
//!
//! ```no_run
//! use oracle_nosql_rust_sdk::types::*;
//! use oracle_nosql_rust_sdk::DeleteRequest;
//! use oracle_nosql_rust_sdk::GetRequest;
//! use oracle_nosql_rust_sdk::Handle;
//! use oracle_nosql_rust_sdk::HandleMode;
//! use oracle_nosql_rust_sdk::NoSQLError;
//! use oracle_nosql_rust_sdk::PutRequest;
//! use oracle_nosql_rust_sdk::QueryRequest;
//! use oracle_nosql_rust_sdk::TableRequest;
//! use std::error::Error;
//! use std::time::Duration;
//! use tracing::info;
//!
//! // This method shows various ways to configure a NoSQL Handle.
//! async fn get_handle() -> Result<Handle, NoSQLError> {
//!     // Note: later methods called on this builder will override earlier methods.
//!     // This allows for setting desired defaults that can be overridden by, for example,
//!     // .from_environment().
//!     Handle::builder()
//!         // For local cloudsim: endpoint, Cloudsim mode:
//!         .endpoint("http://localhost:8080")?
//!         .mode(HandleMode::Cloudsim)?
//!         //
//!         // For on-premises: endpoint, Onprem mode:
//!         // .endpoint("https://my.company.com:8080")?
//!         // .mode(HandleMode::Onprem)?
//!         // Optional file with onprem secure login credentials
//!         // .onprem_auth_from_file("/path/to/user_pass_file")?
//!         // Optional path to x509 certificate in PEM format
//!         // .onprem_auth_from_file("/path/to/certificate.pem")?
//!         //
//!         // For cloud, using user-based file:
//!         // .cloud_auth_from_file("~/.oci/config")?
//!         // Optional region, if not specified in above file
//!         // .cloud_region("us-ashburn-1")?
//!         // Optional full endpoint, if region is not recognized
//!         //.endpoint("https://nosql.us-unknown-1.oci.oraclecloud.com")?
//!         //
//!         // For cloud, using Instance Principal:
//!         // .cloud_auth_from_instance()?
//!         //
//!         // For cloud, using Resource Principal:
//!         // .cloud_auth_from_resource()?
//!         //
//!         // To read all of the above from environment variables:
//!         .from_environment()?
//!         //
//!         // Optional: set a different default timeout (default is 30 seconds)
//!         .timeout(Duration::from_secs(15))?
//!         //
//!         // Build the handle
//!         .build()
//!         .await
//! }
//!
//! // Example way to use multiple threaded tokio runtime
//! #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     // Set up a tracing subscriber to see output based on RUST_LOG environment setting
//!     tracing_subscriber::fmt()
//!         .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
//!         .compact()
//!         .init();
//!
//!     // Create a handle. This should be used throughout the program, across all threads
//!     info!("Creating new handle...");
//!     let handle = get_handle().await?;
//!
//!     // Create an example table
//!     TableRequest::new("testusers")
//!         .statement(
//!             "create table if not exists testusers (id integer, name string,
//!             created timestamp(3), primary key(id))",
//!         )
//!         // the following line is only needed for Cloud mode
//!         .limits(&TableLimits::provisioned(1000, 1000, 10))
//!         .execute(&handle)
//!         .await?
//!         // wait up to 15 seconds for table to be created
//!         .wait_for_completion_ms(&handle, 15000, 500)
//!         .await?;
//!
//!     // Put a record into the table
//!     // Note: ttl is a Duration, but internally is converted to a whole number of
//!     // hours or days. Minimum duration is one hour.
//!     let putres = PutRequest::new("testusers")
//!         .timeout(&Duration::from_millis(3000))
//!         .value(MapValue::new().i32("id", 10).str("name", "jane"))
//!         .ttl(&Duration::new(7200, 0))
//!         .execute(&handle)
//!         .await?;
//!     println!("PutResult={:?}", putres);
//!     // PutResult should have a version
//!     if putres.version().is_none() {
//!         return Err("PutRequest should have returned a version, but did not".into());
//!     }
//!
//!     // Example of using if_version to overwrite existing record
//!     let putreq2 = PutRequest::new("testusers")
//!         .timeout(&Duration::from_millis(3000))
//!         .value(MapValue::new().i32("id", 10).str("name", "john"))
//!         .if_version(&putres.version().unwrap())
//!         .ttl(&Duration::new(7200, 0));
//!     println!("Putreq2={:?}", putreq2);
//!     let putres2 = putreq2.execute(&handle).await?;
//!     println!("PutResult2={:?}", putres2);
//!
//!     // Get the record back
//!     let getres = GetRequest::new("testusers")
//!         .key(MapValue::new().i32("id", 10))
//!         .consistency(Consistency::Eventual)
//!         .execute(&handle)
//!         .await?;
//!     println!("GetResult={:?}", getres);
//!     // GetResult should have a version
//!     if getres.version().is_none() {
//!         return Err("GetRequest should have returned a version, but did not".into());
//!     }
//!
//!     // write in some more records, so query below has more to return
//!     for i in 20..30 {
//!         //let name = format!("name{}", i);
//!         let name = "somename".to_string();
//!         let _ = PutRequest::new("testusers")
//!             .value(
//!                 MapValue::new()
//!                     .i32("id", i)
//!                     .str("name", name.as_str()),
//!             )
//!             .execute(&handle)
//!             .await?;
//!         // Optional delay between puts, if desired
//!         // tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
//!     }
//!
//!     // Simple query of all rows
//!     let qres = QueryRequest::new("select * from testusers")
//!         .execute(&handle)
//!         .await?;
//!     println!("QueryResult = {:?}", qres);
//!
//!     // Query with ordering
//!     let qres1 = QueryRequest::new("select * from testusers order by id")
//!         .execute(&handle)
//!         .await?;
//!     println!("QueryResult1 = {:?}", qres1);
//!
//!     // Example of how to remove a record with if_version
//!     let delres = DeleteRequest::new("testusers", MapValue::new().i32("id", 10))
//!         .if_version(&getres.version().unwrap())
//!         .execute(&handle)
//!         .await?;
//!     println!("delres={:?}", delres);
//!
//!     // Drop the table
//!     TableRequest::new("testusers")
//!         .statement("drop table if exists testusers")
//!         .execute(&handle)
//!         .await?
//!         .wait_for_completion_ms(&handle, 15000, 500)
//!         .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! Create a directory `quickstart` and `quickstart/src`, save the example program as `main.rs` in the `quickstart/src` directory.
//!
//! Add a `Cargo.toml` file to the `quickstart` directory:
//! ```text
//! [package]
//! name = "quickstart"
//! version = "0.1.0"
//! edition = "2021"
//!
//! [dependencies]
//! tokio = { version = "1.38.0", features = ["full"] }
//! chrono = { version = "0.4.31", features = ["alloc", "std"] }
//! oracle-nosql-rust-sdk = { version = "0.1" }
//! tracing = "0.1.40"
//! tracing-subscriber =  { version = "0.3", features = ["env-filter", "std"] }
//! ```
//! Run the example program with the commands:
//!
//! ```sh
//! cd quickstart
//!
//! cargo build
//!
//! cargo run
//! ```
//!
//! ## Examples
//!
//! Examples can be found at the [**examples**](https://github.com/oracle/nosql-rust-sdk/blob/main/examples)
//! directory. Examples include simple, standalone programs that show the Rust API usages.
//! They include comments about how they can be configured and run in the different
//! supported environments.
//!
//! ## Help
//!
//! There are a few ways to get help or report issues:
//! - Open an issue in the [Issues](https://github.com/oracle/nosql-rust-sdk/issues) page.
//! - Post your question on the [Oracle NoSQL Database Community](https://forums.oracle.com/ords/apexds/domain/dev-community/category/nosql_database).
//! - [Email to nosql\_sdk\_help\_grp@oracle.com](mailto:nosql_sdk_help_grp@oracle.com)
//!
//! When requesting help please be sure to include as much detail as possible,
//! including version of the SDK and **simple**, standalone example code as needed.
//!
//! ## Changes
//!
//! See the [Changelog](https://github.com/oracle/nosql-rust-sdk/blob/main/CHANGELOG.md).
//!
//! ## Contributing
//!
//! This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](./CONTRIBUTING.md)
//!
//! ## Security
//!
//! Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process
//!
//! ## License
//!
//! Copyright (C) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//!
//! This SDK is licensed under the Universal Permissive License 1.0. See
//! [LICENSE](https://github.com/oracle/nosql-rust-sdk/blob/main/LICENSE.txt) for
//! details.
//!

pub(crate) mod handle_builder;
pub use crate::handle_builder::{HandleBuilder, HandleMode};

pub(crate) mod handle;
pub use crate::handle::Handle;

pub(crate) mod aggr_iter;
pub(crate) mod arith_op_iter;
pub(crate) mod auth_common;
pub use crate::auth_common::authentication_provider::AuthenticationProvider;

pub(crate) mod collect_iter;
pub(crate) mod const_iter;
pub(crate) mod delete_request;
pub use crate::delete_request::{DeleteRequest, DeleteResult};

pub(crate) mod error;
pub use crate::error::{NoSQLError, NoSQLErrorCode};

pub(crate) mod ext_var_iter;
pub(crate) mod field_step_iter;
pub(crate) mod get_indexes_request;
pub use crate::get_indexes_request::{GetIndexesRequest, GetIndexesResult, IndexInfo};

pub(crate) mod get_request;
pub use crate::get_request::{GetRequest, GetResult};

pub(crate) mod group_iter;
pub(crate) mod list_tables_request;
pub use crate::list_tables_request::{ListTablesRequest, ListTablesResult};

#[cfg(test)]
pub(crate) mod mapvalue_tests;
pub(crate) mod multi_delete_request;
pub use crate::multi_delete_request::{FieldRange, MultiDeleteRequest, MultiDeleteResult};

pub(crate) mod nson;
pub(crate) mod packed_integer;
pub(crate) mod plan_iter;
pub(crate) mod prepared_statement;
pub use crate::prepared_statement::PreparedStatement;

pub(crate) mod put_request;
pub use crate::put_request::{PutRequest, PutResult};

#[cfg(test)]
pub(crate) mod qtf;
#[cfg(test)]
pub(crate) mod qtf_custom_setup;
#[cfg(test)]
pub(crate) mod qtf_tests;
pub(crate) mod query_request;
pub use crate::query_request::{QueryRequest, QueryResult};

pub(crate) mod reader;
pub(crate) mod receive_iter;
pub(crate) mod region;

#[cfg(test)]
pub(crate) mod request_tests;
#[cfg(test)]
pub(crate) mod rw_tests;
pub(crate) mod sfw_iter;
pub(crate) mod size_iter;
pub(crate) mod sort_iter;
pub(crate) mod system_request;
pub use crate::system_request::{SystemRequest, SystemResult};

pub(crate) mod table_request;
pub use crate::table_request::{GetTableRequest, TableRequest, TableResult};

pub(crate) mod table_usage_request;
pub use crate::table_usage_request::{TableUsage, TableUsageRequest, TableUsageResult};

pub mod types;
/// Type representing a specific version of a table row in the NoSQL Database.
pub type Version = Vec<u8>;
pub use crate::types::NoSQLColumnToFieldValue;

pub(crate) mod var_ref_iter;
pub(crate) mod write_multiple_request;
pub use crate::write_multiple_request::{
    SubOperationResult, WriteMultipleRequest, WriteMultipleResult,
};
pub(crate) mod writer;
