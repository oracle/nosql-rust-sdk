//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::error::NoSQLErrorCode::RequestTimeout;
use crate::error::{ia_err, NoSQLError};
use crate::handle::Handle;
use crate::handle::SendOptions;
use crate::nson::*;
use crate::reader::Reader;
use crate::types::{OpCode, TableLimits, TableState};
use crate::writer::Writer;
use std::result::Result;
use std::thread::sleep;
use std::time::{Duration, Instant};

/// Struct used for creating or modifying a table in the NoSQL Database.
///
/// This is the main method for creating, altering, and dropping tables in the
/// NoSQL Database. It can also be used to alter table limits for Cloud operation.
///
/// Example:
/// ```no_run
/// use oracle_nosql_rust_sdk::TableRequest;
/// use oracle_nosql_rust_sdk::types::*;
/// # use oracle_nosql_rust_sdk::Handle;
/// # use std::error::Error;
/// # #[tokio::main]
/// # pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let handle = Handle::builder().build().await?;
///     // Create an example table
///     TableRequest::new("testusers")
///         .statement(
///             "create table if not exists testusers (id integer, name string,
///             created timestamp(3), primary key(id))",
///         )
///         // the following line is only needed for Cloud mode
///         .limits(&TableLimits::provisioned(1000, 1000, 10))
///         .execute(&handle)
///         .await?
///         // wait up to 15 seconds for table to be created
///         .wait_for_completion_ms(&handle, 15000, 500)
///         .await?;
/// # Ok(())
/// # }
///```

#[derive(Default, Debug)]
pub struct TableRequest {
    pub(crate) table_name: String,
    pub(crate) compartment_id: String,
    pub(crate) namespace: String,
    pub(crate) timeout: Option<Duration>,
    pub(crate) statement: String,
    pub(crate) limits: Option<TableLimits>,
    pub(crate) match_etag: Option<String>,
    // TODO: tags
}

/// Struct used to get information about a table in the NoSQL Database.
#[derive(Default, Debug)]
pub struct GetTableRequest {
    pub(crate) table_name: String,
    pub(crate) compartment_id: String,
    pub(crate) namespace: String,
    pub(crate) operation_id: String,
    pub(crate) timeout: Option<Duration>,
    // TODO: tags
}

/// Struct representing the result of a [`TableRequest`] or a [`GetTableRequest`].
#[derive(Default, Debug)]
pub struct TableResult {
    pub(crate) table_name: String,
    pub(crate) compartment_id: String, // TODO: Option<>?
    pub(crate) namespace: String,      // TODO: Option<>?
    pub(crate) table_ocid: String,
    pub(crate) ddl: String,
    pub(crate) operation_id: String, // TODO: Option<>?
    pub(crate) schema: String,
    pub(crate) state: TableState,
    pub(crate) limits: Option<TableLimits>,
    pub(crate) match_etag: Option<String>,
    // TODO: MRT fields
}

impl TableRequest {
    /// Create a new TableRequest.
    ///
    /// `table_name` is required and must be non-empty.
    pub fn new(table_name: &str) -> TableRequest {
        TableRequest {
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// Specify the timeout value for the request.
    ///
    /// This is optional.
    /// If set, it must be greater than or equal to 1 millisecond, otherwise an
    /// IllegalArgument error will be returned.
    /// If not set, the default timeout value configured for the [`Handle`](crate::HandleBuilder::timeout()) is used.
    ///
    /// Note this is just the timeout for the initial request. The actual operation may take significantly longer,
    /// and its completion should be waited for by calling [`TableResult::wait_for_completion()`].
    pub fn timeout(mut self, t: &Duration) -> Self {
        self.timeout = Some(t.clone());
        self
    }

    /// Cloud Service only: set the name or id of a compartment to be used for this operation.
    ///
    /// If the associated handle authenticated as an Instance Principal, this value must be an OCID.
    /// In all other cases, the value may be specified as either a name (or path for nested compartments) or as an OCID.
    ///
    /// If no compartment is given, the default compartment id for the handle is used. If that value was
    /// not specified, the root compartment of the tenancy will be used.
    pub fn compartment_id(mut self, compartment_id: &str) -> Self {
        self.compartment_id = compartment_id.to_string();
        self
    }

    /// On-premises only: set the namespace for the operation.
    pub fn namespace(mut self, namespace: &str) -> TableRequest {
        self.namespace = namespace.to_string();
        self
    }

    /// Set the DDL statement for the table operation.
    ///
    /// This is required, unless the operation is used solely to change the table
    /// limits with [`TableRequest::limits()`].
    pub fn statement(mut self, stmt: &str) -> TableRequest {
        self.statement = stmt.to_string();
        self
    }

    /// Cloud only: specify table limits for the table.
    ///
    /// This method can be used when creating a table, or later to change the
    /// limits on an existing table.
    pub fn limits(mut self, limits: &TableLimits) -> TableRequest {
        self.limits = Some(limits.clone());
        self
    }

    /// Cloud only: set a matching tag for the operation to succeed.
    ///
    /// This method sets an ETag in the request that must be matched for the operation
    /// to proceed. The ETag must be non-empty and have been returned in a
    /// previous [`TableResult`]. This is a form of optimistic concurrency
    /// control, allowing an application to ensure that no unexpected modifications
    /// have been made to the table.
    pub fn match_etag(mut self, match_etag: &str) -> TableRequest {
        self.match_etag = Some(match_etag.to_string());
        self
    }

    /// Execute the table request.
    ///
    /// This starts the asynchronous execution of the request in the system. The returned result should be
    /// used to wait for completion by calling [`TableResult::wait_for_completion()`].
    pub async fn execute(&self, h: &Handle) -> Result<TableResult, NoSQLError> {
        // TODO: validate
        let mut w: Writer = Writer::new();
        w.write_i16(h.inner.serial_version);
        let timeout = h.get_timeout(&self.timeout);
        self.nson_serialize(&mut w, &timeout);
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: false,
            compartment_id: self.compartment_id.clone(),
            ..Default::default()
        };
        let mut r = h.send_and_receive(w, &mut opts).await?;
        let resp = TableRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    pub(crate) fn nson_serialize(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        ns.write_header(OpCode::TableRequest, timeout, &self.table_name);
        ns.end_header();

        // payload
        ns.start_payload();
        ns.write_string_field(STATEMENT, &self.statement);
        ns.write_limits(&self.limits);
        // TODO: freeform/defined tags
        if let Some(etag) = &self.match_etag {
            ns.write_string_field(ETAG, etag);
        }
        // TODO: these are currently only in http headers. Add to NSON?
        //ns.write_string_field(COMPARTMENT_OCID, &self.compartment_id);
        //ns.write_string_field(NAMESPACE, &self.namespace);
        ns.end_payload();

        ns.end_request();
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<TableResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: TableResult = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    walker.handle_error_code()?;
                }
                COMPARTMENT_OCID => {
                    res.compartment_id = walker.read_nson_string()?;
                    //println!(" comp_id={:?}", res.compartment_id);
                }
                NAMESPACE => {
                    res.namespace = walker.read_nson_string()?;
                    //println!(" namespace={:?}", res.namespace);
                }
                TABLE_OCID => {
                    res.table_ocid = walker.read_nson_string()?;
                    //println!(" table_ocid={:?}", res.table_ocid);
                }
                TABLE_NAME => {
                    res.table_name = walker.read_nson_string()?;
                    //println!(" table_name={:?}", res.table_name);
                }
                TABLE_SCHEMA => {
                    res.schema = walker.read_nson_string()?;
                    //println!(" schema={:?}", res.schema);
                }
                TABLE_DDL => {
                    res.ddl = walker.read_nson_string()?;
                    //println!(" ddl={:?}", res.ddl);
                }
                OPERATION_ID => {
                    res.operation_id = walker.read_nson_string()?;
                    //println!(" operation_id={:?}", res.operation_id);
                }
                LIMITS => {
                    res.limits = Some(walker.read_nson_limits()?);
                    //println!(" limits={:?}", res.limits);
                }
                TABLE_STATE => {
                    let s = walker.read_nson_i32()?;
                    res.state = TableState::from_int(s)?;
                    //println!(" state={:?}", res.state);
                }
                ETAG => {
                    res.match_etag = Some(walker.read_nson_string()?);
                }
                _ => {
                    //println!("   table_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }
}

impl NsonRequest for TableRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.nson_serialize(w, timeout);
    }
}

impl GetTableRequest {
    pub fn new(table_name: &str) -> GetTableRequest {
        GetTableRequest {
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// Specify the timeout value for the request.
    ///
    /// This is optional.
    /// If set, it must be greater than or equal to 1 millisecond, otherwise an
    /// IllegalArgument error will be returned.
    /// If not set, the default timeout value configured for the [`Handle`](crate::HandleBuilder::timeout()) is used.
    pub fn timeout(mut self, t: &Duration) -> Self {
        self.timeout = Some(t.clone());
        self
    }

    /// Cloud Service only: set the name or id of a compartment to be used for this operation.
    ///
    /// If the associated handle authenticated as an Instance Principal, this value must be an OCID.
    /// In all other cases, the value may be specified as either a name (or path for nested compartments) or as an OCID.
    ///
    /// If no compartment is given, the default compartment id for the handle is used. If that value was
    /// not specified, the root compartment of the tenancy will be used.
    pub fn compartment_id(mut self, compartment_id: &str) -> Self {
        self.compartment_id = compartment_id.to_string();
        self
    }

    pub fn operation_id(mut self, op_id: &str) -> GetTableRequest {
        self.operation_id = op_id.to_string();
        self
    }

    /// On-premises only: set the namespace for the operation.
    pub fn namespace(mut self, namespace: &str) -> GetTableRequest {
        self.namespace = namespace.to_string();
        self
    }

    pub async fn execute(&self, h: &Handle) -> Result<TableResult, NoSQLError> {
        // TODO: validate
        let mut w: Writer = Writer::new();
        w.write_i16(h.inner.serial_version);
        let timeout = h.get_timeout(&self.timeout);
        self.nson_serialize(&mut w, &timeout);
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: true,
            compartment_id: self.compartment_id.clone(),
            namespace: self.namespace.clone(),
            ..Default::default()
        };
        let mut r = h.send_and_receive(w, &mut opts).await?;
        let resp = TableRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    pub(crate) fn nson_serialize(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        ns.write_header(OpCode::GetTable, timeout, &self.table_name);
        ns.end_header();

        // payload
        ns.start_payload();
        ns.write_string_field(OPERATION_ID, &self.operation_id);
        // TODO: these are currently only in http headers. Add to NSON?
        //ns.write_string_field(COMPARTMENT_OCID, &self.compartment_id);
        //ns.write_string_field(NAMESPACE, &self.namespace);
        ns.end_payload();

        ns.end_request();
    }
}

impl NsonRequest for GetTableRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.nson_serialize(w, timeout);
    }
}

impl TableResult {
    /// Wait for a TableRequest to complete.
    ///
    /// This method will loop, polling the system for the status of the SystemRequest
    /// until it either succeeds, gets an error, or times out.
    pub async fn wait_for_completion(
        &mut self,
        h: &Handle,
        wait: Duration,
        delay: Duration,
    ) -> Result<(), NoSQLError> {
        if self.is_terminal() {
            return Ok(());
        }
        if wait < delay {
            return ia_err!("wait duration must be greater than delay duration");
        }

        let start_time = Instant::now();
        let mut first_loop = true;

        while self.is_terminal() == false {
            //println!("  table-request: elapsed={:?}", start_time.elapsed());
            if start_time.elapsed() > wait {
                return Err(NoSQLError::new(
                    RequestTimeout,
                    "operation not completed in expected time",
                ));
            }

            let get_request = GetTableRequest::new(self.table_name.as_str())
                .operation_id(self.operation_id.as_str())
                .compartment_id(self.compartment_id.as_str())
                .namespace(self.namespace.as_str());
            // TODO: namespace? Java sdk doesn't add it...?

            if !first_loop {
                sleep(delay);
            }

            let res = get_request.execute(h).await?;

            // TODO: copy_most method?
            self.state = res.state;
            self.limits = res.limits;
            self.schema = res.schema;
            self.ddl = res.ddl;
            // TODO: tags, MRT data

            first_loop = false;
        }

        Ok(())
    }

    /// Wait for a TableRequest to complete.
    ///
    /// This method will loop, polling the system for the status of the SystemRequest
    /// until it either succeeds, gets an error, or times out.
    ///
    /// This is a convenience method to allow direct millisecond values instead of creating
    /// `Duration` structs.
    pub async fn wait_for_completion_ms(
        &mut self,
        h: &Handle,
        wait_ms: u64,
        delay_ms: u64,
    ) -> Result<(), NoSQLError> {
        self.wait_for_completion(
            h,
            Duration::from_millis(wait_ms),
            Duration::from_millis(delay_ms),
        )
        .await
    }

    fn is_terminal(&self) -> bool {
        self.state == TableState::Active || self.state == TableState::Dropped
    }

    /// Get the table name.
    ///
    /// This is only valid for [`GetTableRequest`].
    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }
    /// Cloud only: get the compartment id of the table.
    ///
    /// This is only valid for [`GetTableRequest`].
    pub fn compartment_id(&self) -> String {
        self.compartment_id.clone()
    }
    /// On-premises only: get the namespace of the table.
    ///
    /// This is only valid for [`GetTableRequest`].
    pub fn namespace(&self) -> String {
        self.namespace.clone()
    }
    /// Cloud only: get the OCID of the table.
    ///
    /// This is only valid for [`GetTableRequest`].
    pub fn table_ocid(&self) -> String {
        self.table_ocid.clone()
    }
    /// Get the DDL statement that was used to create the table.
    ///
    /// Note this will reflect any `ALTER TABLE` operations as well.
    pub fn ddl(&self) -> String {
        self.ddl.clone()
    }
    /// Get the internal operation ID for an in-progress table request.
    ///
    /// This is typically not needed by applications; it is available for testing purposes only.
    /// Internally, [`TableResult::wait_for_completion()`] uses this value when polling the system.
    pub fn operation_id(&self) -> String {
        self.operation_id.clone()
    }
    /// Get the schema of the table.
    ///
    /// Note this will reflect any `ALTER TABLE` operations as well.
    pub fn schema(&self) -> String {
        self.schema.clone()
    }
    /// Get the current state of the table.
    pub fn state(&self) -> TableState {
        self.state.clone()
    }
    /// Cloud only: get the table limits.
    pub fn limits(&self) -> Option<TableLimits> {
        if let Some(l) = &self.limits {
            return Some(l.clone());
        }
        None
    }
    /// Cloud only: get the match ETag for the table.
    ///
    /// see [`TableRequest::match_etag()`] for more details.
    pub fn match_etag(&self) -> Option<String> {
        if let Some(etag) = &self.match_etag {
            return Some(etag.clone());
        }
        None
    }
}
