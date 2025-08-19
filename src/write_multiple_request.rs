//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::delete_request::DeleteRequest;
use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode::IllegalArgument;
use crate::handle::Handle;
use crate::handle::SendOptions;
use crate::nson::NsonSubRequest;
use crate::nson::*;
use crate::put_request::PutRequest;
use crate::reader::Reader;
use crate::types::{Capacity, FieldType, FieldValue, MapValue, NoSQLRow, OpCode};
use crate::writer::Writer;
use crate::Version;
use std::result::Result;
use std::time::Duration;

// For doc only
#[allow(unused_imports)]
use crate::{DeleteResult, PutResult};

/// Struct used to perform multiple [`PutRequest`]s and/or [`DeleteRequest`]s in a single operation.
#[derive(Default, Debug)]
pub struct WriteMultipleRequest {
    pub(crate) table_name: String,
    pub(crate) compartment_id: String,
    // TODO: pub(crate) namespace: String,
    pub(crate) timeout: Option<Duration>,
    pub(crate) sub_requests: Vec<Box<dyn NsonSubRequest>>,
    // TODO durability: Option<Vec<u8>>

    // TODO: limiters, retry stats, etc
}

/// Struct representing the result of a single sub-operation of a [`WriteMultipleRequest`].
#[derive(Default, Debug)]
pub struct SubOperationResult {
    pub(crate) success: bool,
    pub(crate) version: Option<Version>,
    pub(crate) consumed: Option<Capacity>,
    pub(crate) generated_value: Option<FieldValue>,
    pub(crate) existing_modification_time: i64,
    pub(crate) existing_value: Option<MapValue>,
    pub(crate) existing_version: Option<Version>,
    // TODO: stats, etc... (base)
}

impl SubOperationResult {
    /// Get the success result of the sub-operation.
    pub fn success(&self) -> bool {
        self.success
    }
    /// For `Put` operations,
    /// Get the Version of the now-current record. This value is `Some` if the put operation succeeded. It
    /// may be used in subsequent [`PutRequest::if_version()`] calls.
    /// This is valid for `Put` operations only.
    pub fn version(&self) -> Option<&Version> {
        if let Some(v) = &self.version {
            return Some(v);
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
    /// for `Put` operations, get the value generated if the operation created a new value. This can happen if the table contains an
    /// identity column or string column declared as a generated UUID. If the table has no such column, this value is `None`.
    /// This is valid for `Put` operations only.
    pub fn generated_value(&self) -> Option<&FieldValue> {
        if let Some(r) = &self.generated_value {
            return Some(r);
        }
        None
    }

    /// For `Put` operations, see [`PutResult::existing_modification_time()`]. For `Delete` operations,
    /// see [`DeleteResult::existing_modification_time()`].
    // TODO: make this a Time field
    pub fn existing_modification_time(&self) -> i64 {
        self.existing_modification_time
    }
    /// For `Put` operations, see [`PutResult::existing_value()`]. For `Delete` operations,
    /// see [`DeleteResult::existing_value()`].
    pub fn existing_value(&self) -> Option<&MapValue> {
        if let Some(v) = &self.existing_value {
            return Some(v);
        }
        None
    }
    /// For `Put` operations, see [`PutResult::existing_version()`]. For `Delete` operations,
    /// see [`DeleteResult::existing_version()`].
    pub fn existing_version(&self) -> Option<&Version> {
        if let Some(v) = &self.existing_version {
            return Some(v);
        }
        None
    }
}

/// Struct representing the combined results of a [`WriteMultipleRequest`] operation.
#[derive(Default, Debug)]
pub struct WriteMultipleResult {
    pub(crate) results: Vec<SubOperationResult>,
    pub(crate) failed_operation_index: i32,
    pub(crate) consumed: Option<Capacity>,
}

impl WriteMultipleResult {
    /// Get a vector of sub-operation results. This vector is ordered in the same order as
    /// put/delete items were added to the `WriteMultipleRequest`.
    pub fn results(&self) -> &Vec<SubOperationResult> {
        &self.results
    }
    /// Get the offset of the first failed operation.
    /// If there are no failures, -1 is returned.
    pub fn failed_operation_index(&self) -> i32 {
        self.failed_operation_index
    }
    /// Get the consumed capacity (read/write units) of the overall operation. This is only valid in the NoSQL Cloud Service.
    pub fn consumed(&self) -> Option<&Capacity> {
        if let Some(c) = &self.consumed {
            return Some(c);
        }
        None
    }
}

impl WriteMultipleRequest {
    pub fn new(table_name: &str) -> WriteMultipleRequest {
        WriteMultipleRequest {
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

    pub fn add(mut self, r: Box<dyn NsonSubRequest>) -> WriteMultipleRequest {
        self.sub_requests.push(r);
        self
    }

    // let mut data = Vec<MyStruct>::new();
    // ... fill in vector with many MyStructs ...
    //
    // let res: WriteMultipleResult = WriteMultipleRequest::new(tablename)
    //   .put(data)
    //   .execute(&handle);

    // Note this consumes the collection
    pub fn put<T>(mut self, collection: T) -> Result<WriteMultipleRequest, NoSQLError>
    where
        T: IntoIterator,
        T::Item: NoSQLRow,
    {
        for item in collection {
            // note: this implies collection.into_iter()
            self.sub_requests
                .push(Box::new(PutRequest::new("").put(item)?));
        }
        Ok(self)
    }

    // Note this consumes the collection
    pub fn delete<T>(mut self, collection: T) -> Result<WriteMultipleRequest, NoSQLError>
    where
        T: IntoIterator,
        T::Item: NoSQLRow,
    {
        for item in collection {
            // note: this implies collection.into_iter()
            match item.to_map_value() {
                Ok(value) => {
                    self.sub_requests
                        .push(Box::new(DeleteRequest::new("", value)));
                }
                Err(e) => {
                    // TODO: save error as source
                    return Err(NoSQLError::new(
                        IllegalArgument,
                        &format!("could not convert struct to MapValue: {}", e.to_string()),
                    ));
                }
            }
        }
        Ok(self)
    }

    pub async fn execute(&self, h: &Handle) -> Result<WriteMultipleResult, NoSQLError> {
        // TODO: validate: size > 0, etc
        let mut w: Writer = Writer::new();
        w.write_i16(h.inner.serial_version);
        let timeout = h.get_timeout(&self.timeout);
        self.serialize_internal(&mut w, &timeout);
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: false,
            compartment_id: self.compartment_id.clone(),
            ..Default::default()
        };
        let mut r = h.send_and_receive(w, &mut opts).await?;
        let resp = WriteMultipleRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    fn serialize_internal(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();

        // TableName
        // If all ops use the same table name, write that
        // single table name to the output stream.
        // If any of them are different, write all table
        // names to the individual ops.
        // Possible optimization: if most use one table,
        // write that in the header and only write minority
        // table names in specific ops.
        // TODO if self.is_single_table() {
        ns.write_header(OpCode::WriteMultiple, timeout, &self.table_name);
        //} else {
        //ns.write_header(OpCode::WriteMultiple, self.timeout_ms, "");
        //}
        ns.end_header();

        // TODO: compartment
        ns.start_payload();
        ns.write_i32_field(DURABILITY, 0); // TODO
        ns.write_i32_field(NUM_OPERATIONS, self.sub_requests.len() as i32);

        // OPERATIONS: array of maps
        ns.start_array(OPERATIONS);
        for rq in self.sub_requests.as_slice() {
            ns.write_subrequest(rq, timeout);
            ns.end_array_field(0);
        }
        ns.end_array(OPERATIONS);

        ns.end_payload();
        ns.end_request();
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<WriteMultipleResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: WriteMultipleResult = Default::default();
        res.failed_operation_index = -1;
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
                WM_SUCCESS => {
                    // array of operation result maps
                    MapWalker::expect_type(walker.r, FieldType::Array)?;
                    let _ = walker.r.read_i32()?; // skip array size in bytes
                    let num_elements = walker.r.read_i32()?;
                    res.results = Vec::with_capacity(num_elements as usize);
                    for _n in 1..=num_elements {
                        res.results
                            .push(WriteMultipleRequest::read_result(walker.r)?);
                    }
                }
                WM_FAILURE => {
                    WriteMultipleRequest::read_failed_results(walker.r, &mut res)?;
                }
                _ => {
                    //println!("   write_multiple_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }

    fn read_failed_results(
        r: &mut Reader,
        res: &mut WriteMultipleResult,
    ) -> Result<(), NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                WM_FAIL_INDEX => {
                    res.failed_operation_index = walker.read_nson_i32()?;
                }
                WM_FAIL_RESULT => {
                    res.results
                        .push(WriteMultipleRequest::read_result(walker.r)?);
                }
                _ => {
                    //println!("   read_failed_results: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(())
    }

    // TODO: make this common to all write result operations
    fn read_result(r: &mut Reader) -> Result<SubOperationResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: SubOperationResult = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                SUCCESS => {
                    //println!("   read_result: SUCCESS");
                    res.success = walker.read_nson_boolean()?;
                }
                ROW_VERSION => {
                    //println!("   read_result: ROW_VERSION");
                    res.version = Some(walker.read_nson_binary()?);
                }
                GENERATED => {
                    //println!("   read_result: GENERATED");
                    res.generated_value = Some(walker.read_nson_field_value()?);
                    //println!("generated_value={:?}", res.generated_value);
                }
                RETURN_INFO => {
                    //println!("   read_result: RETURN_INFO");
                    WriteMultipleRequest::read_return_info(walker.r, &mut res)?;
                }
                _ => {
                    //println!("   read_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }

    fn read_return_info(r: &mut Reader, res: &mut SubOperationResult) -> Result<(), NoSQLError> {
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
                    //println!("   read_ri: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(())
    }
}

impl NsonRequest for WriteMultipleRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.serialize_internal(w, timeout);
    }
}
