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
use crate::types::{OpCode, OperationState};
use crate::writer::Writer;
use std::result::Result;
use std::time::{Duration, Instant};

/// Struct used for on-premise-only requests.
///
/// This is used to perform any table-independent administrative operation such as
/// create/drop of namespaces and security-relevant operations (create/drop
/// users and roles). These operations are asynchronous and completion needs
/// to be checked.
///
/// Examples of statements used in this object include:
///  - CREATE NAMESPACE mynamespace
///  - CREATE USER some_user IDENTIFIED BY password
///  - CREATE ROLE some_role
///  - GRANT ROLE some_role TO USER some_user
///
/// Execution of operations specified by this request are implicitly asynchronous.
/// These are potentially long-running operations.
/// [`SystemRequest::execute()`] returns a [`SystemResult`] instance that
/// can be used to poll until the operation succeeds or fails.
#[derive(Default, Debug)]
pub struct SystemRequest {
    pub(crate) statement: String,
    pub(crate) timeout: Option<Duration>,
}

/// Struct used to query the status of an in-progress [`SystemRequest`].
#[derive(Default, Debug)]
pub(crate) struct SystemStatusRequest {
    pub operation_id: String,
    pub timeout: Option<Duration>,
}

/// Struct representing the result of a [`SystemRequest`].
#[derive(Default, Debug)]
pub struct SystemResult {
    pub(crate) operation_id: String, // TODO: Option<>?
    pub(crate) state: OperationState,
    pub(crate) statement: String,
    pub(crate) result_string: String,
}

impl SystemRequest {
    /// Create a new SystemRequest. `statement` must be non-empty.
    pub fn new(statement: &str) -> SystemRequest {
        SystemRequest {
            statement: statement.to_string(),
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

    /// Execute the system request.
    ///
    /// This starts the asynchronous execution of the request in the system. The returned result should be
    /// used to wait for completion by calling [`SystemResult::wait_for_completion()`].
    pub async fn execute(&self, h: &Handle) -> Result<SystemResult, NoSQLError> {
        // TODO: validate
        let mut w: Writer = Writer::new();
        w.write_i16(h.inner.serial_version);
        let timeout = h.get_timeout(&self.timeout);
        self.nson_serialize(&mut w, &timeout);
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: false,
            ..Default::default()
        };
        let mut r = h.send_and_receive(w, &mut opts).await?;
        let resp = SystemRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    pub(crate) fn nson_serialize(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        ns.write_header(OpCode::SystemRequest, timeout, "");
        ns.end_header();

        // payload
        ns.start_payload();
        ns.write_string_field(STATEMENT, &self.statement);
        ns.end_payload();

        ns.end_request();
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<SystemResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: SystemResult = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    walker.handle_error_code()?;
                }
                OPERATION_ID => {
                    res.operation_id = walker.read_nson_string()?;
                    //println!(" operation_id={:?}", res.operation_id);
                }
                STATEMENT => {
                    res.statement = walker.read_nson_string()?;
                    //println!(" statement={:?}", res.statement);
                }
                SYSOP_RESULT => {
                    res.result_string = walker.read_nson_string()?;
                    //println!(" result_string={:?}", res.result_string);
                }
                SYSOP_STATE => {
                    let s = walker.read_nson_i32()?;
                    res.state = OperationState::from_int(s)?;
                    //println!(" state={:?}", res.state);
                }
                _ => {
                    //println!("   system_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }
}

impl NsonRequest for SystemRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.nson_serialize(w, timeout);
    }
}

impl SystemStatusRequest {
    pub fn new(operation_id: &str) -> SystemStatusRequest {
        SystemStatusRequest {
            operation_id: operation_id.to_string(),
            ..Default::default()
        }
    }

    /// Specify the timeout value for the request.
    ///
    /// This is optional.
    /// If set, it must be greater than or equal to 1 millisecond, otherwise an
    /// IllegalArgument error will be returned.
    /// If not set, the default timeout value configured for the [`Handle`](crate::HandleBuilder::timeout()) is used.
    #[allow(dead_code)]
    pub fn timeout(mut self, t: &Duration) -> Self {
        self.timeout = Some(t.clone());
        self
    }

    pub async fn execute(&self, h: &Handle) -> Result<SystemResult, NoSQLError> {
        // TODO: validate
        let mut w: Writer = Writer::new();
        w.write_i16(h.inner.serial_version);
        let timeout = h.get_timeout(&self.timeout);
        self.nson_serialize(&mut w, &timeout);
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: true,
            ..Default::default()
        };
        let mut r = h.send_and_receive(w, &mut opts).await?;
        let resp = SystemRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    pub(crate) fn nson_serialize(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        ns.write_header(OpCode::SystemStatusRequest, timeout, "");
        ns.end_header();

        // payload
        ns.start_payload();
        ns.write_string_field(OPERATION_ID, &self.operation_id);
        ns.end_payload();

        ns.end_request();
    }
}

impl NsonRequest for SystemStatusRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.nson_serialize(w, timeout);
    }
}

impl SystemResult {
    /// Wait for a SystemRequest to complete.
    ///
    /// This method will loop, polling the system for the status of the SystemRequest
    /// until it either succeeds, gets an error, or times out.
    pub async fn wait_for_completion(
        &mut self,
        h: &Handle,
        wait: Duration,
        delay: Duration,
    ) -> Result<(), NoSQLError> {
        if self.state == OperationState::Complete {
            return Ok(());
        }
        if wait < delay {
            return ia_err!("wait duration must be greater than delay duration");
        }

        let start_time = Instant::now();
        let mut first_loop = true;

        while self.state != OperationState::Complete {
            if start_time.elapsed() > wait {
                return Err(NoSQLError::new(
                    RequestTimeout,
                    "Operation not completed in expected time",
                ));
            }

            if !first_loop {
                tokio::time::sleep(delay).await;
            }

            let res = SystemStatusRequest::new(self.operation_id.as_str())
                .execute(h)
                .await?;

            // operation_id and statement do not change
            self.state = res.state;
            self.result_string = res.result_string;

            first_loop = false;
        }

        Ok(())
    }

    /// Wait for a SystemRequest to complete.
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

    pub fn operation_id(&self) -> String {
        self.operation_id.clone()
    }

    pub fn state(&self) -> OperationState {
        self.state.clone()
    }

    pub fn statement(&self) -> String {
        self.statement.clone()
    }

    pub fn result_string(&self) -> String {
        self.result_string.clone()
    }
}
