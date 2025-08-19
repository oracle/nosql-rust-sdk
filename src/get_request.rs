//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode::{IllegalArgument, ResourceNotFound};
use crate::handle::Handle;
use crate::handle::SendOptions;
use crate::nson::*;
use crate::reader::Reader;
use crate::types::{Capacity, Consistency, MapValue, NoSQLRow, OpCode};
use crate::writer::Writer;
use crate::Version;
use std::result::Result;
use std::time::Duration;

/// Struct used for getting a single row of data from a NoSQL table.
#[derive(Default, Debug)]
pub struct GetRequest {
    pub(crate) table_name: String,
    pub(crate) compartment_id: String,
    pub(crate) timeout: Option<Duration>,
    pub(crate) key: MapValue,
    pub(crate) consistency: Consistency,
    // TODO: limiters, retry stats, etc
}

/// Struct representing the result of a [`GetRequest`] operation.
///
/// This struct is returned from a [`GetRequest::execute()`] call.
#[derive(Default, Debug)]
pub struct GetResult {
    pub(crate) row: Option<MapValue>,
    pub(crate) consumed: Option<Capacity>,
    pub(crate) modification_time: i64, // TODO: Time
    pub(crate) expiration_time: i64,   // TODO: Time
    pub(crate) version: Option<Version>,
    // TODO: stats, rldelay, etc...
}

impl GetResult {
    /// Get the returned row. If the row does not exist in the table, this value will be `None`.
    pub fn row(&self) -> Option<&MapValue> {
        if let Some(r) = &self.row {
            return Some(r);
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
    /// Get the last modification time of the row. This is only valid if the operation succeeded.
    /// Its value is the number of milliseconds since the epoch (Jan 1 1970).
    pub fn modification_time(&self) -> i64 {
        self.modification_time
    }
    /// Get the expiration time of the row. This is only valid if the operation succeeded.
    /// Its value is the number of milliseconds since the epoch (Jan 1 1970).
    pub fn expiration_time(&self) -> i64 {
        self.expiration_time
    }
    /// Get the version of the row. This is only valid if the operation succeeded.
    pub fn version(&self) -> Option<&Version> {
        if let Some(v) = &self.version {
            return Some(v);
        }
        None
    }
    // TODO: stats, rldelay, etc...
}

impl GetRequest {
    /// Create a new `GetRequest`.
    ///
    /// `table_name` is required and must be non-empty.
    pub fn new(table_name: &str) -> GetRequest {
        GetRequest {
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

    /// Specify the primary key to use to find the row (record) in the table, from a [`MapValue`].
    ///
    /// `key` must contain all fields required to construct the primary key for the table.
    pub fn key(mut self, key: MapValue) -> GetRequest {
        self.key = key;
        self
    }

    /// Specify the primary key to use to find the row (record) in the table, from a native Rust struct.
    ///
    /// `row_key` must be an instance of a struct that implements the [`NoSQLRow`] trait, which is
    /// done by adding the [`derive@NoSQLRow`] derive to the struct definition. `row_key` must contain
    /// all fields required to construct the primary key for the table.
    ///
    /// See the [`GetRequest::execute_into()`] documentation below for an example of how to
    /// add the `NoSQLRow` derive to a struct.
    pub fn row_key(mut self, row_key: &dyn NoSQLRow) -> Result<GetRequest, NoSQLError> {
        match row_key.to_map_value() {
            Ok(value) => {
                self = self.key(value);
                return Ok(self);
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

    /// Specify the desired [`Consistency`] for the operation.
    pub fn consistency(mut self, c: Consistency) -> GetRequest {
        self.consistency = c;
        self
    }

    /// Execute the request, returning a [`GetResult`].
    ///
    /// If the record exists in the table, [`GetResult::row`] will be `Some()`.
    pub async fn execute(&self, h: &Handle) -> Result<GetResult, NoSQLError> {
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
        let resp = GetRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    /// Execute the request, populating an existing Rust native struct.
    ///
    /// `row` must be an instance of a struct that implements the [`NoSQLRow`] trait, which is
    /// done by adding the [`derive@NoSQLRow`] derive to the struct definition. `row` will have all
    /// of its fields populated if this method returns `Ok()`:
    /// ```no_run
    /// use oracle_nosql_rust_sdk::GetRequest;
    /// use oracle_nosql_rust_sdk::types::*;
    /// # use oracle_nosql_rust_sdk::Handle;
    /// # use oracle_nosql_rust_sdk::types::FieldValue;
    /// # use std::error::Error;
    /// # #[tokio::main]
    /// # pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let handle = Handle::builder().build().await?;
    /// // Assume a table was created with the following statement:
    /// // "CREATE TABLE people (id long, name string, street string,
    /// //                       city string, zip integer, primary_key(id))"
    /// // A corresponding Rust struct could be:
    /// #[derive(Default, Debug, NoSQLRow)]
    /// struct Person {
    ///     pub id: i64,
    ///     pub name: String,
    ///     pub street: String,
    ///     pub city: String,
    ///     pub zip: i32,
    /// }
    /// // To get a specific person from the table:
    /// let mut person = Person {
    ///     id: 123456,
    ///     ..Default::default()
    /// };
    /// GetRequest::new("people")
    ///     .row_key(&person)?
    ///     .execute_into(&handle, &mut person).await?;
    /// // person contains all fields
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See the [`PutRequest::put()`](crate::PutRequest::put()) documentation for an additional example of how to
    /// add the `NoSQLRow` derive to a struct.
    ///
    /// If it is desired to also get the row metadata (version, modification time, etc), use the the
    /// [`GetRequest::execute()`] method instead, and then populate the native struct using
    /// the [`NoSQLRow::from_map_value()`] method:
    /// ```no_run
    /// use oracle_nosql_rust_sdk::GetRequest;
    /// use oracle_nosql_rust_sdk::types::*;
    /// # use oracle_nosql_rust_sdk::Handle;
    /// # use oracle_nosql_rust_sdk::types::FieldValue;
    /// # use std::error::Error;
    /// # #[tokio::main]
    /// # pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let handle = Handle::builder().build().await?;
    /// // Assume a table was created with the following statement:
    /// // "CREATE TABLE people (id long, name string, street string,
    /// //                       city string, zip integer, primary_key(id))"
    /// // A corresponding Rust struct could be:
    /// #[derive(Default, Debug, NoSQLRow)]
    /// struct Person {
    ///     pub id: i64,
    ///     pub name: String,
    ///     pub street: String,
    ///     pub city: String,
    ///     pub zip: i32,
    /// }
    /// // To get a specific person from the table:
    /// let mut person = Person {
    ///     id: 123456,
    ///     ..Default::default()
    /// };
    /// let get_request = GetRequest::new("people").row_key(&person)?;
    /// let resp = get_request.execute(&handle).await?;
    /// if let Some(mv) = resp.row() {
    ///     // resp metadata (modification time, version, etc.) valid
    ///     let result = person.from_map_value(mv);
    ///     if result.is_ok() {
    ///         // person contains all fields
    ///     } else {
    ///         // There was some error deserializing the row MapValue to a Person struct
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_into(&self, h: &Handle, row: &mut dyn NoSQLRow) -> Result<(), NoSQLError> {
        let resp = self.execute(h).await?;
        if let Some(r) = resp.row() {
            match row.from_map_value(r) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    // TODO: save error as source
                    return Err(NoSQLError::new(
                        IllegalArgument,
                        &format!(
                            "could not convert MapValue to native struct: {}",
                            e.to_string()
                        ),
                    ));
                }
            }
        }
        Err(NoSQLError::new(ResourceNotFound, "NoSQL row not found"))
    }

    pub(crate) fn nson_serialize(&self, w: &mut Writer, timeout: &Duration) {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        ns.write_header(OpCode::Get, timeout, &self.table_name);
        ns.end_header();

        // payload
        ns.start_payload();
        ns.write_consistency(self.consistency);
        ns.write_map_field(KEY, &self.key);
        ns.end_payload();

        ns.end_request();
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<GetResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: GetResult = Default::default();
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    //println!("   get_result: ERROR_CODE");
                    walker.handle_error_code()?;
                }
                CONSUMED => {
                    //println!("   get_result: CONSUMED");
                    res.consumed = Some(walker.read_nson_consumed_capacity()?);
                    //println!(" consumed={:?}", res.consumed);
                }
                ROW => {
                    //println!("   get_result: ROW");
                    read_row(walker.r, &mut res)?;
                    //for (f,v) in res.row.iter() {
                    //println!("row: field={} value={:?}", f, v);
                    //}
                }
                _ => {
                    //println!("   get_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }
}

fn read_row(r: &mut Reader, res: &mut GetResult) -> Result<(), NoSQLError> {
    let mut walker = MapWalker::new(r)?;
    while walker.has_next() {
        walker.next()?;
        let name = walker.current_name();
        match name.as_str() {
            MODIFIED => {
                //println!("   read_row: MODIFIED");
                res.modification_time = walker.read_nson_i64()?;
            }
            EXPIRATION => {
                //println!("   read_row: EXPIRATION");
                res.expiration_time = walker.read_nson_i64()?;
            }
            ROW_VERSION => {
                //println!("   read_row: ROW_VERSION");
                res.version = Some(walker.read_nson_binary()?);
            }
            VALUE => {
                //println!("   read_row: VALUE");
                res.row = Some(walker.read_nson_map()?);
            }
            _ => {
                //println!("   read_row: skipping field '{}'", name);
                walker.skip_nson_field()?;
            }
        }
    }
    Ok(())
}

impl NsonRequest for GetRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.nson_serialize(w, timeout);
    }
}
