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
use crate::types::OpCode;
use crate::writer::Writer;
use std::result::Result;
use std::time::Duration;

/// Struct used for listing tables in the NoSQL Database.
#[derive(Default, Debug)]
pub struct ListTablesRequest {
    pub(crate) compartment_id: String,
    pub(crate) namespace: String,
    pub(crate) start_index: i32,
    pub(crate) limit: i32,
    pub(crate) timeout: Option<Duration>,
    // TODO: tags?
}

/// Struct representing the result of a [`ListTablesRequest`] operation.
#[derive(Default, Debug)]
pub struct ListTablesResult {
    pub table_names: Vec<String>,
    pub last_table_index: i32,
}

impl ListTablesRequest {
    pub fn new() -> ListTablesRequest {
        ListTablesRequest {
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

    pub fn namespace(mut self, namespace: &str) -> ListTablesRequest {
        self.namespace = namespace.to_string();
        self
    }

    pub fn limit(mut self, limit: i32) -> ListTablesRequest {
        self.limit = limit;
        self
    }

    pub fn start_index(mut self, start_index: i32) -> ListTablesRequest {
        self.start_index = start_index;
        self
    }

    pub async fn execute(&self, h: &Handle) -> Result<ListTablesResult, NoSQLError> {
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
        let resp = ListTablesRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    pub(crate) fn nson_serialize(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        ns.write_header(OpCode::ListTables, timeout, "");
        ns.end_header();

        // payload
        ns.start_payload();
        ns.write_nonempty_string_field(NAMESPACE, &self.namespace);
        ns.write_nonzero_i32_field(LIST_START_INDEX, self.start_index);
        ns.write_nonzero_i32_field(LIST_MAX_TO_READ, self.limit);
        // TODO: this is currently only in http headers. Add to NSON?
        //ns.write_string_field(COMPARTMENT_OCID, &self.compartment_id);
        ns.end_payload();

        ns.end_request();
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<ListTablesResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: ListTablesResult = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    walker.handle_error_code()?;
                }
                TABLES => {
                    res.table_names = walker.read_nson_string_array()?;
                    //println!(" table_names={:?}", res.table_names);
                }
                LAST_INDEX => {
                    res.last_table_index = walker.read_nson_i32()?;
                    //println!(" last_index={:?}", res.last_table_index);
                }
                _ => {
                    //println!("   list_tables_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }
}

impl NsonRequest for ListTablesRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.nson_serialize(w, timeout);
    }
}
