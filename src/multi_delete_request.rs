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
use crate::types::{Capacity, FieldValue, OpCode};
use crate::writer::Writer;
use std::result::Result;
use std::time::Duration;

/// A range of values to be used in a [`MultiDeleteRequest`] operation.
///
/// `FieldRange` is used as the least significant component in a partially
/// specified key value in order to create a value range for an operation that
/// returns multiple rows or keys. The data types supported by `FieldRange` are
/// limited to the atomic types which are valid for primary keys.
///
/// The least significant component of a key is the first component of the key
/// that is not fully specified. For example, if the primary key for a table is
/// defined as the tuple:
///
///   <a, b, c>
///
/// A `FieldRange` can be specified for:
///
///   "a" if the primary key supplied is empty.
///   "b" if the primary key supplied to the operation has a concrete value for "a" but not for "b" or "c".
///
/// The `field_path` specified must name a field in a table's primary key.
/// The `start` and `end` values used must be of the same type and that type must
/// match the type of the field specified.
///
/// Validation of this struct is performed when it is used in an operation.
/// Validation includes verifying that the field is in the required key and,
/// in the case of a composite key, that the field is in the proper order
/// relative to the key used in the operation.
#[derive(Default, Debug)]
pub struct FieldRange {
    // field_path specifies the path to the field used in the range.
    pub field_path: String,

    // start specifies the start value of the range.
    pub start: Option<FieldValue>,

    // start_inclusive specifies whether start value is included in the range,
    // i.e., start value is less than or equal to the first FieldValue in the range.
    //
    // This value is valid only if the start value is specified.
    pub start_inclusive: bool,

    // end specifies the end value of the range.
    pub end: Option<FieldValue>,

    // end_inclusive specifies whether end value is included in the range,
    // i.e., end value is greater than or equal to the last FieldValue in the range.
    //
    // This value is valid only if the end value is specified.
    pub end_inclusive: bool,
}

/// Struct used for deleting a range of rows from a NoSQL table.
#[derive(Default, Debug)]
pub struct MultiDeleteRequest {
    pub(crate) table_name: String,
    pub(crate) compartment_id: String,
    pub(crate) key: FieldValue,
    pub(crate) continuation_key: Option<Vec<u8>>,
    pub(crate) field_range: Option<FieldRange>,
    pub(crate) max_write_kb: i32,
    pub(crate) timeout: Option<Duration>,
    // Durability is currently only used in On-Prem installations.
    // Added in SDK Version 1.3.0
    // TODO durability: Option<Something>

    // namespace is used on-premises only. It defines a namespace to use
    // for the request. It is optional.
    // If a namespace is specified in the table name for the request
    // (using the namespace:tablename format), that value will override this
    // setting.
    // This is only available with on-premises installations using NoSQL
    // Server versions 23.3 and above.
    // TODO: pub namespace: String,

    // TODO: limiters, retry stats, etc
}

/// Struct representing the result of a [`MultiDeleteRequest`] operation.
#[derive(Default, Debug)]
pub struct MultiDeleteResult {
    pub(crate) num_deleted: i32,
    pub(crate) continuation_key: Option<Vec<u8>>,
    pub(crate) consumed: Option<Capacity>,
}

impl MultiDeleteResult {
    /// Get the number of records deleted by the operation.
    pub fn num_deleted(&self) -> i32 {
        self.num_deleted
    }
    /// Get a continuation key that can be used in a subsequent MultiDelete operation.
    /// This typically will be set when a `max_write_kb` is specified and there are more
    /// records to delete.
    pub fn continuation_key(&self) -> Option<Vec<u8>> {
        if let Some(ck) = &self.continuation_key {
            return Some(ck.clone());
        }
        None
    }
    /// Get the consumed capacity (read/write units) of the operation. This is only valid in the NoSQL Cloud Service.
    pub fn consumed(&self) -> Option<&Capacity> {
        if let Some(c) = &self.consumed {
            return Some(c);
        }
        None
    }
}

impl MultiDeleteRequest {
    /// Create a new `MultiDeleteRequest`.
    ///
    /// `table_name` specifies the name of table for the request.
    /// It is required and must be non-empty.
    ///
    /// `partial_key` specifies the partial key used for the request.
    /// It is required and must be non-nil.
    pub fn new(table_name: &str, partial_key: &FieldValue) -> MultiDeleteRequest {
        MultiDeleteRequest {
            table_name: table_name.to_string(),
            key: partial_key.clone_internal(),
            ..Default::default()
        }
    }

    /// Specify the timeout value for the request.
    ///
    /// This is optional.
    /// If set, it must be greater than or equal to 1 millisecond, otherwise an
    /// IllegalArgument error will be returned.
    /// If not set, the default timeout value configured for the [`Handle`](crate::HandleBuilder::timeout()) is used.
    pub fn timeout(mut self, t: &Duration) -> MultiDeleteRequest {
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
    pub fn compartment_id(mut self, compartment_id: &str) -> MultiDeleteRequest {
        self.compartment_id = compartment_id.to_string();
        self
    }

    /// Specifiy the [`FieldRange`] to be used for the operation.
    ///
    /// It is optional, but required to delete a specific range of rows.
    pub fn field_range(mut self, field_range: FieldRange) -> MultiDeleteRequest {
        self.field_range = Some(field_range);
        self
    }

    /// Specify the continuation key to use to continue the operation.
    ///
    /// This is typically populated from a previous [`MultiDeleteResult::continuation_key()`].
    pub fn continuation_key(mut self, key: Vec<u8>) -> MultiDeleteRequest {
        self.continuation_key = Some(key);
        self
    }

    /// Specify the limit on the total KB write during this operation.
    ///
    /// This is optional and has no effect for on-premise.
    /// When used for the cloud service, if this value is not set, or set to 0, there
    /// is no application-defined limit.
    ///
    ///  This value can only reduce the system defined limit. An attempt to increase the
    /// limit beyond the system defined limit will cause an IllegalArgument error.
    pub fn max_write_kb(mut self, max_write_kb: i32) -> MultiDeleteRequest {
        self.max_write_kb = max_write_kb;
        self
    }

    pub async fn execute(&self, h: &Handle) -> Result<MultiDeleteResult, NoSQLError> {
        // TODO: validate: size > 0, etc
        let mut w: Writer = Writer::new();
        w.write_i16(h.inner.serial_version);
        let timeout = h.get_timeout(&self.timeout);
        self.serialize_internal(&mut w, &timeout);
        // TODO: namespace in http header?
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: false,
            compartment_id: self.compartment_id.clone(),
            ..Default::default()
        };
        let mut r = h.send_and_receive(w, &mut opts).await?;
        let resp = MultiDeleteRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    fn serialize_internal(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        ns.write_header(OpCode::MultiDelete, timeout, &self.table_name);
        ns.end_header();

        ns.start_payload();
        //ns.write_i32_field(DURABILITY, 0); // TODO

        ns.write_i32_field(MAX_WRITE_KB, self.max_write_kb);

        ns.write_field(KEY, &self.key);

        if let Some(ckey) = &self.continuation_key {
            ns.write_binary_field(CONTINUATION_KEY, ckey);
        }

        if let Some(range) = &self.field_range {
            ns.start_map(RANGE);
            ns.write_string_field(RANGE_PATH, &range.field_path);
            if let Some(start) = &range.start {
                ns.write_field(VALUE, start);
                ns.write_bool_field(INCLUSIVE, range.start_inclusive);
            }
            if let Some(end) = &range.end {
                ns.write_field(VALUE, end);
                ns.write_bool_field(INCLUSIVE, range.end_inclusive);
            }
            ns.end_map(RANGE);
        }

        ns.end_payload();
        ns.end_request();
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<MultiDeleteResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: MultiDeleteResult = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    walker.handle_error_code()?;
                }
                CONSUMED => {
                    res.consumed = Some(walker.read_nson_consumed_capacity()?);
                    //println!(" consumed={:?}", res.consumed);
                }
                NUM_DELETIONS => {
                    res.num_deleted = walker.read_nson_i32()?;
                    //println!(" num_deleted={:?}", res.num_deleted);
                }
                CONTINUATION_KEY => {
                    res.continuation_key = Some(walker.read_nson_binary()?);
                    //println!(" continuation_key={:?}", res.continuation_key);
                }
                _ => {
                    //println!("   multi_delete_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }
}

impl NsonRequest for MultiDeleteRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.serialize_internal(w, timeout);
    }
}
