//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::error::NoSQLError;
use crate::handle::Handle;
use crate::handle::SendOptions;
use crate::nson::*;
use crate::reader::Reader;
use crate::types::{FieldType, OpCode};
use crate::writer::Writer;
use chrono::{DateTime, FixedOffset};
use std::result::Result;
use std::time::Duration;

/// Struct used for querying the usage of a NoSQL Database table.
#[derive(Default, Debug)]
pub struct TableUsageRequest {
    pub(crate) table_name: String,
    pub(crate) compartment_id: String,
    pub(crate) start_time: Option<DateTime<FixedOffset>>,
    pub(crate) end_time: Option<DateTime<FixedOffset>>,
    pub(crate) limit: i32,
    pub(crate) start_index: i32,
    pub(crate) timeout: Option<Duration>,
}

/// A single time slice usage record for a specific table.
///
/// This contains information about read and write throughput consumed during that period
/// as well as the current information regarding storage capacity. In
/// addition the count of throttling exceptions for the period is reported.
#[derive(Default, Debug)]
pub struct TableUsage {
    pub start_time: DateTime<FixedOffset>,
    pub seconds_in_period: i32,
    pub read_units: i32,
    pub write_units: i32,
    pub storage_gb: i32,
    pub read_throttle_count: i32,
    pub write_throttle_count: i32,
    pub storage_throttle_count: i32,
    pub max_shard_usage_percent: i32,
    // One private field so we don't break SemVer if we add more public fields
    #[allow(dead_code)]
    singluar_private: bool,
}

/// Struct representing the result of a [`TableUsageRequest`] operation.
#[derive(Default, Debug)]
pub struct TableUsageResult {
    pub(crate) table_name: String,
    pub(crate) usage_records: Vec<TableUsage>,
    pub(crate) last_index_returned: i32,
}

/// Struct representing the result of a TableUsageRequest execution.
impl TableUsageResult {
    /// Get the table name.
    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }
    /// Get a reference to a vector of usage records.
    pub fn usage_records(&self) -> &Vec<TableUsage> {
        &self.usage_records
    }
    /// Take the Result's vector of usage records, leaving the Result with
    /// a new empty vector.
    pub fn take_usage_records(&mut self) -> Vec<TableUsage> {
        std::mem::take(&mut self.usage_records)
    }
    /// Get the last index returned.
    ///
    /// This is typically used when specifying a `limit` on the usage request. If this value
    /// is greater than the `start_index` plus `limit`, there are more records to retreive. In the
    /// next request, set the `start_index` to this value plus one.
    pub fn last_index_returned(&self) -> i32 {
        self.last_index_returned
    }
}

impl TableUsageRequest {
    /// Create a new TableUsageRequest.
    pub fn new(table_name: &str) -> TableUsageRequest {
        TableUsageRequest {
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

    /// Specify the start time for TableUsage records.
    pub fn start_time(mut self, t: DateTime<FixedOffset>) -> TableUsageRequest {
        self.start_time = Some(t);
        self
    }

    /// Specify the end time for TableUsage records.
    pub fn end_time(mut self, t: DateTime<FixedOffset>) -> TableUsageRequest {
        self.end_time = Some(t);
        self
    }

    /// Specify the limit of TableUsage records to return.
    pub fn limit(mut self, l: i32) -> TableUsageRequest {
        self.limit = l;
        self
    }

    /// Specify the starting index of TableUsage records.
    /// This is usually set by adding one to a previous [`TableUsageResult::last_index_returned()`] call.
    /// The indexes start at zero.
    pub fn start_index(mut self, i: i32) -> TableUsageRequest {
        self.start_index = i;
        self
    }

    /// Execute the request, returning a [`TableUsageResult`].
    pub async fn execute(&self, h: &Handle) -> Result<TableUsageResult, NoSQLError> {
        let mut w: Writer = Writer::new();
        w.write_i16(h.inner.serial_version);
        let timeout = h.get_timeout(&self.timeout);
        self.nson_serialize(&mut w, &timeout);
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: true,
            compartment_id: self.compartment_id.clone(),
            ..Default::default()
        };
        let mut r = h.send_and_receive(w, &mut opts).await?;
        let resp = TableUsageRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    pub(crate) fn nson_serialize(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        ns.write_header(OpCode::GetTableUsage, timeout, &self.table_name);
        ns.end_header();

        // payload
        ns.start_payload();
        // TODO: currently only in http headers. Add to NSON?
        //ns.write_string_field(COMPARTMENT_OCID, &self.compartment_id);
        if let Some(sval) = self.start_time {
            let s = sval.to_rfc3339();
            ns.write_string_field(START, &s);
        }
        if let Some(eval) = self.end_time {
            let s = eval.to_rfc3339();
            ns.write_string_field(END, &s);
        }
        ns.write_nonzero_i32_field(LIST_MAX_TO_READ, self.limit);
        ns.write_nonzero_i32_field(LIST_MAX_TO_READ, self.start_index);
        ns.end_payload();

        ns.end_request();
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<TableUsageResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: TableUsageResult = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    walker.handle_error_code()?;
                }
                TABLE_NAME => {
                    res.table_name = walker.read_nson_string()?;
                }
                LAST_INDEX => {
                    res.last_index_returned = walker.read_nson_i32()?;
                }
                TABLE_USAGE => {
                    // array of index info
                    MapWalker::expect_type(walker.r, FieldType::Array)?;
                    let _ = walker.r.read_i32()?; // skip array size in bytes
                    let num_elements = walker.r.read_i32()?;
                    res.usage_records = Vec::with_capacity(num_elements as usize);
                    for _n in 1..=num_elements {
                        res.usage_records
                            .push(TableUsageRequest::read_usage_record(walker.r)?);
                    }
                    //println!(" usage_records={:?}", res.usage_records);
                }
                _ => {
                    //println!("   table_usage_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }

    fn read_usage_record(r: &mut Reader) -> Result<TableUsage, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: TableUsage = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                START => {
                    let s = walker.read_nson_string()?;
                    res.start_time = DateTime::parse_from_rfc3339(&s)?;
                }
                TABLE_USAGE_PERIOD => {
                    // TODO: Duration
                    res.seconds_in_period = walker.read_nson_i32()?;
                }
                READ_UNITS => {
                    res.read_units = walker.read_nson_i32()?;
                }
                WRITE_UNITS => {
                    res.write_units = walker.read_nson_i32()?;
                }
                STORAGE_GB => {
                    res.storage_gb = walker.read_nson_i32()?;
                }
                READ_THROTTLE_COUNT => {
                    res.read_throttle_count = walker.read_nson_i32()?;
                }
                WRITE_THROTTLE_COUNT => {
                    res.write_throttle_count = walker.read_nson_i32()?;
                }
                STORAGE_THROTTLE_COUNT => {
                    res.storage_throttle_count = walker.read_nson_i32()?;
                }
                MAX_SHARD_USAGE_PERCENT => {
                    res.max_shard_usage_percent = walker.read_nson_i32()?;
                }
                _ => {
                    //println!("   read_usage_record: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }
}

impl NsonRequest for TableUsageRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.nson_serialize(w, timeout);
    }
}
