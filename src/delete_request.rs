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
use crate::types::{Capacity, MapValue, OpCode};
use crate::writer::Writer;
use crate::Version;
use std::result::Result;
use std::time::Duration;

/// Struct used for deleting a single row from a table in the NoSQL Database.
///
/// This request can be used to perform unconditional and conditional deletes:
///
/// - Delete any existing row. This is the default.
/// - Succeed only if the row exists and its Version matches a specific Version. Use
///   [`if_version()`](DeleteRequest::if_version()) for this case.
///
/// Information about the existing row can be returned from a delete operation using
/// [`return_row(true)`](DeleteRequest::return_row()). Requesting this information incurs
/// additional cost and may affect operation latency.
#[derive(Default, Debug)]
pub struct DeleteRequest {
    pub(crate) key: MapValue,
    pub(crate) table_name: String,
    pub(crate) timeout: Option<Duration>,
    pub(crate) compartment_id: String,
    pub(crate) abort_on_fail: bool,
    pub(crate) return_row: bool,
    // TODO: durability
    match_version: Version,
}

/// Struct representing the result of a [`DeleteRequest`] execution.
///
/// This struct is returned from a [`DeleteRequest::execute()`] call.
#[derive(Default, Debug)]
pub struct DeleteResult {
    pub(crate) success: bool,
    pub(crate) consumed: Option<Capacity>,
    pub(crate) existing_modification_time: i64,
    pub(crate) existing_value: Option<MapValue>,
    pub(crate) existing_version: Option<Version>,
    // TODO: stats, etc... (base)
}

impl DeleteResult {
    /// Get the result of the operation: `true` if the row was deleted from the table.
    pub fn success(&self) -> bool {
        self.success
    }
    /// Get the consumed capacity (read/write units) of the operation. This is only valid in the NoSQL Cloud Service.
    pub fn consumed(&self) -> Option<&Capacity> {
        if let Some(c) = &self.consumed {
            return Some(c);
        }
        None
    }

    /// Get the modification time of the deleted row if the delete operation succeeded, or the modification time of the
    /// current row if the operation failed due to a `if_version()` mismatch.
    ///
    /// In either case, this is only valid if [`return_row(true)`] was called on
    /// the [`DeleteRequest`] and a previous row existed.
    /// Its value is the number of milliseconds since the epoch (Jan 1 1970).
    // TODO: make this a Time field
    pub fn existing_modification_time(&self) -> i64 {
        self.existing_modification_time
    }
    /// Get the value of the deleted row if the delete operation succeeded, or the value of the
    /// current row if the operation failed due to a `if_version()` mismatch.
    ///
    /// In either case, this is only valid if [`return_row(true)`] was called on
    /// the [`DeleteRequest`] and a previous row existed.
    pub fn existing_value(&self) -> Option<&MapValue> {
        if let Some(v) = &self.existing_value {
            return Some(v);
        }
        None
    }
    /// Get the Version of the deleted row if the delete operation succeeded, or the Version of the
    /// current row if the operation failed due to a `if_version()` mismatch.
    ///
    /// In either case, this is only valid if [`return_row(true)`] was called on
    /// called on the [`DeleteRequest`] and a previous row existed.
    pub fn existing_version(&self) -> Option<&Version> {
        if let Some(v) = &self.existing_version {
            return Some(v);
        }
        None
    }
    // TODO: stats, etc... (base)
}

impl DeleteRequest {
    /// Create a new `DeleteRequest`.
    ///
    /// `table_name` and `key` are required and must be non-empty.
    ///
    /// `key` must contain all fields required to construct the primary key for the table.
    pub fn new(table_name: &str, key: MapValue) -> DeleteRequest {
        DeleteRequest {
            table_name: table_name.to_string(),
            key: key,
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

    /// Succeed only if the record already exists its version matches the given version.
    pub fn if_version(mut self, version: &Version) -> DeleteRequest {
        self.match_version = version.clone();
        self
    }

    /// Return information about the existing row. Requesting this information incurs
    /// additional cost and may affect operation latency.
    pub fn return_row(mut self, val: bool) -> DeleteRequest {
        self.return_row = val;
        self
    }

    pub fn set_abort_on_fail(mut self, val: bool) -> DeleteRequest {
        self.abort_on_fail = val;
        self
    }

    pub async fn execute(&self, h: &Handle) -> Result<DeleteResult, NoSQLError> {
        let mut w: Writer = Writer::new();
        w.write_i16(h.inner.serial_version);
        let timeout = h.get_timeout(&self.timeout);
        self.serialize_internal(&mut w, false, false, &timeout);
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: true,
            compartment_id: self.compartment_id.clone(),
            ..Default::default()
        };
        let mut r = h.send_and_receive(w, &mut opts).await?;
        let resp = DeleteRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    // TODO: when is add_table_name ever true??
    fn serialize_internal(
        &self,
        w: &mut Writer,
        is_sub_request: bool,
        add_table_name: bool,
        timeout: &Duration,
    ) {
        let mut ns = NsonSerializer::start_request(w);
        let mut opcode = OpCode::Delete;
        if self.match_version.len() > 0 {
            opcode = OpCode::DeleteIfVersion;
        }

        if is_sub_request {
            if add_table_name {
                ns.write_string_field(TABLE_NAME, &self.table_name);
            }
            ns.write_i32_field(OP_CODE, opcode as i32);
            if self.abort_on_fail {
                ns.write_bool_field(ABORT_ON_FAIL, true);
            }
        } else {
            ns.start_header();
            ns.write_header(opcode, timeout, &self.table_name);
            ns.end_header();
            ns.start_payload();
            // TODO: ns.write_durability(self.durability);
        }

        ns.write_true_bool_field(RETURN_ROW, true);
        // TODO identity cache size

        if self.match_version.len() > 0 {
            ns.write_binary_field(ROW_VERSION, &self.match_version);
        }

        ns.write_map_field(KEY, &self.key);

        // TODO others

        if is_sub_request == false {
            ns.end_payload();
            ns.end_request();
        }
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<DeleteResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: DeleteResult = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    //println!("   w: ERROR_CODE");
                    walker.handle_error_code()?;
                }
                CONSUMED => {
                    //println!("   w: CONSUMED");
                    res.consumed = Some(walker.read_nson_consumed_capacity()?);
                    //println!(" consumed={:?}", res.consumed);
                }
                SUCCESS => {
                    res.success = walker.read_nson_boolean()?;
                    //println!(" success={:?}", res.success);
                }
                RETURN_INFO => {
                    //println!("   w: RETURN_INFO");
                    read_return_info(walker.r, &mut res)?;
                }
                _ => {
                    //println!("   delete_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }
}

// TODO: make this common to all write results
fn read_return_info(r: &mut Reader, res: &mut DeleteResult) -> Result<(), NoSQLError> {
    let mut walker = MapWalker::new(r)?;
    while walker.has_next() {
        walker.next()?;
        let name = walker.current_name();
        match name.as_str() {
            EXISTING_MOD_TIME => {
                //println!("   read_ri: EXISTING_MOD_TIME");
                res.existing_modification_time = walker.read_nson_i64()?;
            }
            //EXISTING_EXPIRATION => {
            //println!("   read_ri: EXISTING_EXPIRATION");
            //res.existing_expiration_time = walker.read_nson_i64()?;
            //},
            EXISTING_VERSION => {
                //println!("   read_ri: EXISTING_VERSION");
                res.existing_version = Some(walker.read_nson_binary()?);
            }
            EXISTING_VALUE => {
                //println!("   read_ri: EXISTING_VALUE");
                res.existing_value = Some(walker.read_nson_map()?);
            }
            _ => {
                //println!( "   delete_result read_return_info: skipping field '{}'", name);
                walker.skip_nson_field()?;
            }
        }
    }
    Ok(())
}

impl NsonRequest for DeleteRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.serialize_internal(w, false, false, timeout);
    }
}

impl NsonSubRequest for DeleteRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.serialize_internal(w, true, false, timeout);
    }
}
