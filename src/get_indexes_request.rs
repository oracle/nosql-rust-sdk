//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode::BadProtocolMessage;
use crate::handle::Handle;
use crate::handle::SendOptions;
use crate::nson::*;
use crate::reader::Reader;
use crate::types::{FieldType, OpCode};
use crate::writer::Writer;
use std::result::Result;
use std::time::Duration;

/// Struct used for querying indexes for a NoSQL table.
#[derive(Default, Debug)]
pub struct GetIndexesRequest {
    pub(crate) table_name: String,
    pub(crate) index_name: String, // TODO: Option<String>
    pub(crate) compartment_id: String,
    pub(crate) namespace: String,
    pub(crate) timeout: Option<Duration>,
}

/// Information about a single index including its name and field names.
#[derive(Default, Debug)]
pub struct IndexInfo {
    pub index_name: String,
    pub field_names: Vec<String>,
    pub field_types: Vec<String>,
}

/// Struct representing the result of a [`GetIndexesRequest`].
#[derive(Default, Debug)]
pub struct GetIndexesResult {
    pub indexes: Vec<IndexInfo>,
}

impl GetIndexesRequest {
    pub fn new(table_name: &str) -> GetIndexesRequest {
        GetIndexesRequest {
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    // Name of the index to get. If this is empty, all indexes for
    // the table are returned.
    pub fn index_name(mut self, index_name: &str) -> GetIndexesRequest {
        self.index_name = index_name.to_string();
        self
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

    pub fn namespace(mut self, namespace: &str) -> GetIndexesRequest {
        self.namespace = namespace.to_string();
        self
    }

    pub async fn execute(&self, h: &Handle) -> Result<GetIndexesResult, NoSQLError> {
        // TODO: validate
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
        let resp = GetIndexesRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    pub(crate) fn nson_serialize(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        ns.write_header(OpCode::GetIndexes, timeout, &self.table_name);
        ns.end_header();

        // payload
        ns.start_payload();
        ns.write_string_field(INDEX, &self.index_name);
        // TODO: these are currently only in http headers. Add to NSON?
        //ns.write_string_field(COMPARTMENT_OCID, &self.compartment_id);
        //ns.write_string_field(NAMESPACE, &self.namespace);
        ns.end_payload();

        ns.end_request();
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<GetIndexesResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: GetIndexesResult = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    walker.handle_error_code()?;
                }
                INDEXES => {
                    // array of index info
                    MapWalker::expect_type(walker.r, FieldType::Array)?;
                    let _ = walker.r.read_i32()?; // skip array size in bytes
                    let num_elements = walker.r.read_i32()?;
                    res.indexes = Vec::with_capacity(num_elements as usize);
                    for _n in 1..=num_elements {
                        res.indexes
                            .push(GetIndexesRequest::read_index_info(walker.r)?);
                    }
                    //println!(" indexes={:?}", res.indexes);
                }
                _ => {
                    //println!("   get_indexes_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }

    fn read_index_info(r: &mut Reader) -> Result<IndexInfo, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: IndexInfo = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                NAME => {
                    res.index_name = walker.read_nson_string()?;
                }
                FIELDS => {
                    // array of maps with PATH, TYPE elements each
                    MapWalker::expect_type(walker.r, FieldType::Array)?;
                    let _ = walker.r.read_i32()?; // skip array size in bytes
                    let num_elements = walker.r.read_i32()?;
                    res.field_names = Vec::with_capacity(num_elements as usize);
                    res.field_types = Vec::with_capacity(num_elements as usize);
                    for _n in 1..=num_elements {
                        GetIndexesRequest::read_index_fields(walker.r, &mut res)?;
                    }
                }
                _ => {
                    //println!("   read_index_info: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }

    fn read_index_fields(r: &mut Reader, res: &mut IndexInfo) -> Result<(), NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        while walker.has_next() {
            walker.next()?;
            // ensure we get both fields
            let mut num_fields = 0;
            let name = walker.current_name();
            match name.as_str() {
                PATH => {
                    res.field_names.push(walker.read_nson_string()?);
                    num_fields += 1;
                }
                TYPE => {
                    res.field_types.push(walker.read_nson_string()?);
                    num_fields += 1;
                }
                _ => {
                    //println!("   read_index_fields: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
            if num_fields != 2 {
                return Err(NoSQLError::new(
                    BadProtocolMessage,
                    "response missing PATH or TYPE element(s)",
                ));
            }
        }
        Ok(())
    }
}

impl NsonRequest for GetIndexesRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.nson_serialize(w, timeout);
    }
}
