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
use crate::types::{Capacity, FieldValue, MapValue, NoSQLRow, OpCode};
use crate::writer::Writer;
use crate::NoSQLErrorCode::IllegalArgument;
use crate::Version;
use std::result::Result;
use std::time::Duration;

/// Struct used for inserting a single row of data into a NoSQL table.
///
/// This request can be used to insert data represented as a NoSQL [`MapValue`], or as
/// a native Rust struct using the [`macro@NoSQLRow`] derive macro.
///
/// This request can perform unconditional and conditional puts:
/// - Overwrite existing row. This is the default.
/// - Succeed only if the row does not exist. Use [`if_absent()`](PutRequest::if_absent()) for this case.
/// - Succeed only if the row exists. Use [`if_present()`](PutRequest::if_present()) for this case.
/// - Succeed only if the row exists and its [`Version`] matches a specific [`Version`]. Use [`if_version()`](PutRequest::if_version()) for this case.
///
/// Information about the existing row can be returned from a put operation using [`return_row(true)`](PutRequest::return_row()). Requesting this information incurs additional cost and may affect operation latency.
///
/// On successful operation, [`PutResult::version()`] is `Some`. This Version may
/// be used in subsequent PutRequests.
#[derive(Default, Debug)]
pub struct PutRequest {
    pub(crate) table_name: String,
    pub(crate) compartment_id: String,
    pub(crate) value: MapValue,
    pub(crate) timeout: Option<Duration>,
    pub(crate) abort_on_fail: bool,
    pub(crate) return_row: bool,
    if_present: bool,
    if_absent: bool,
    // TODO durability: Option<Version>
    pub(crate) ttl: Duration,
    pub(crate) use_table_ttl: bool,
    pub(crate) exact_match: bool,
    // TODO identity_cache_size,
    match_version: Version,
    // TODO: limiters, retry stats, etc
}

/// Struct representing the result of a [`PutRequest`] execution.
///
/// This struct is returned from a [`PutRequest::execute()`] call.
#[derive(Default, Debug)]
pub struct PutResult {
    pub(crate) version: Option<Version>,
    pub(crate) consumed: Option<Capacity>,
    pub(crate) generated_value: Option<FieldValue>,
    pub(crate) existing_modification_time: i64,
    pub(crate) existing_value: Option<MapValue>,
    pub(crate) existing_version: Option<Version>,
    // TODO: stats, etc... (base)
}

impl PutResult {
    /// Get the Version of the now-current record. This value is `Some` if the put operation succeeded. It
    /// may be used in subsequent [`PutRequest::if_version()`] calls.
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
    /// Get the value generated if the operation created a new value. This can happen if the table contains an
    /// identity column or string column declared as a generated UUID. If the table has no such column, this value is `None`.
    pub fn generated_value(&self) -> Option<&FieldValue> {
        if let Some(r) = &self.generated_value {
            return Some(r);
        }
        None
    }

    /// Get the modification time of the previous row if the put operation succeeded, or the modification time of the
    /// current row if the operation failed due to a `if_version()` or `if_absent()` mismatch.
    ///
    /// In either case, this is only valid if [`return_row(true)`] was called on
    /// the [`PutRequest`] and a previous row existed.
    /// Its value is the number of milliseconds since the epoch (Jan 1 1970).
    // TODO: make this a Time field
    pub fn existing_modification_time(&self) -> i64 {
        self.existing_modification_time
    }
    /// Get the value of the previous row if the put operation succeeded, or the value of the
    /// current row if the operation failed due to a `if_version()` or `if_absent()` mismatch.
    ///
    /// In either case, this is only valid if [`return_row(true)`] was called on
    /// the [`PutRequest`] and a previous row existed.
    pub fn existing_value(&self) -> Option<&MapValue> {
        if let Some(v) = &self.existing_value {
            return Some(v);
        }
        None
    }
    /// Get the Version of the previous row if the put operation succeeded, or the Version of the
    /// current row if the operation failed due to a `if_version()` or `if_absent()` mismatch.
    ///
    /// In either case, this is only valid if [`return_row(true)`] was called on
    /// called on the [`PutRequest`] and a previous row existed.
    pub fn existing_version(&self) -> Option<&Version> {
        if let Some(v) = &self.existing_version {
            return Some(v);
        }
        None
    }
    // TODO: stats, etc... (base)
}

impl PutRequest {
    /// Create a new PutRequest.
    ///
    /// `table_name` should be the name of the table to insert the record into. It is required to be non-empty.
    pub fn new(table_name: &str) -> PutRequest {
        PutRequest {
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// Set the row value to use for the put operation, from a [`MapValue`].
    ///
    /// Either this method or [`put()`](PutRequest::put()) must be called for the `PutRequest` to be valid.
    ///
    /// The fields of the given value will be mapped to their matching table columns on insertion:
    /// ```no_run
    /// use oracle_nosql_rust_sdk::PutRequest;
    /// use oracle_nosql_rust_sdk::types::*;
    /// use chrono::DateTime;
    ///
    /// # use oracle_nosql_rust_sdk::Handle;
    /// # use std::error::Error;
    /// # use std::collections::HashMap;
    /// # #[tokio::main]
    /// # pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let handle = Handle::builder().build().await?;
    /// // Assume a table was created with the following statement:
    /// // "CREATE TABLE users (shard integer, id long, name string, street string, city string,
    /// // zip integer, birth timestamp(3), numbers array(long), data binary, num_map map(integer),
    /// // primary key(shard(shard), id))"
    /// // A MapValue may be created a populated to represent the columns of the table as such:
    /// let user = MapValue::new()
    ///      .column("shard", 1)
    ///      .column("id", 123456788)
    ///      .column("name", "Jane".to_string())
    ///      .column("street", Some("321 Main Street".to_string()))
    ///      .column("city", "Anytown".to_string())
    ///      .column("zip", 12345)
    ///      .column("data", Option::<NoSQLBinary>::None)
    ///      .column("numbers", vec![12345i64, 654556578i64, 43543543543543i64, 23232i64])
    ///      .column("birth", Some(DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00")?))
    ///      .column("num_map", HashMap::from([("cars".to_string(), 1), ("pets".to_string(), 4)]));
    ///
    /// let put_result = PutRequest::new("users")
    ///                  .value(user)
    ///                  .execute(&handle).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn value(mut self, val: MapValue) -> PutRequest {
        self.value = val;
        self
    }

    /// Set the row value to use for the put operation, from a given native Rust struct.
    ///
    /// Either this method or [`value()`](PutRequest::value()) must be called for the `PutRequest` to be valid.
    ///
    /// The fields of the given value will be mapped to their matching table columns on insertion, based
    /// on the [`macro@NoSQLRow`] derive macro being specified on the given struct:
    ///
    /// ```no_run
    /// use oracle_nosql_rust_sdk::PutRequest;
    /// use oracle_nosql_rust_sdk::types::*;
    /// use chrono::{DateTime, FixedOffset};
    ///
    /// # use oracle_nosql_rust_sdk::Handle;
    /// # use std::error::Error;
    /// # use std::collections::HashMap;
    /// # #[tokio::main]
    /// # pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let handle = Handle::builder().build().await?;
    /// // Assume a table was created with the following statement:
    /// // "CREATE TABLE users (shard integer, id long, name string, street string, city string,
    /// // zip integer, birth timestamp(3), numbers array(long), data binary, num_map map(integer),
    /// // primary key(shard(shard), id))"
    /// // A corresponding Rust struct may look something like below. Adding the `NoSQLRow`
    /// // derive allows instances of this struct to be written into the table without
    /// // creating an equivalent `MapValue`.
    /// #[derive(Default, Debug, NoSQLRow)]
    /// struct Person {
    ///     pub shard: i32,
    ///     #[nosql(type=long, column=id)]
    ///     pub uuid: i64,
    ///     pub name: String,
    ///     pub birth: Option<DateTime<FixedOffset>>,
    ///     pub street: Option<String>,
    ///     pub data: Option<NoSQLBinary>,
    ///     pub city: String,
    ///     pub zip: i32,
    ///     pub numbers: Vec<i64>,
    ///     pub num_map: HashMap<String, i32>,
    /// }
    ///
    /// // Create an instance of the struct and insert it into the NoSQL database:
    /// let user = Person {
    ///      shard: 1,
    ///      uuid: 123456788,
    ///      name: "Jane".to_string(),
    ///      street: Some("321 Main Street".to_string()),
    ///      city: "Anytown".to_string(),
    ///      zip: 12345,
    ///      data: None,
    ///      numbers: vec![12345, 654556578, 43543543543543, 23232],
    ///      birth: Some(DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00")?),
    ///      num_map: HashMap::from([("cars".to_string(), 1), ("pets".to_string(), 4)]),
    ///      ..Default::default()
    ///  };
    ///
    /// let put_result = PutRequest::new("users")
    ///                  .put(user)?
    ///                  .execute(&handle).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See the [`GetRequest::execute_into()`](crate::GetRequest::execute_into()) documentation for
    /// another example of using native Rust structs with `NoSQLRow`.
    pub fn put(mut self, val: impl NoSQLRow) -> Result<PutRequest, NoSQLError> {
        match val.to_map_value() {
            Ok(value) => {
                self.value = value;
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

    /// Specify the timeout value for the request.
    ///
    /// This is optional.
    /// If set, it must be greater than or equal to 1 millisecond, otherwise an
    /// IllegalArgument error will be returned.
    /// If not set, the default timeout value configured for the [`Handle`](crate::HandleBuilder::timeout()) is used.
    pub fn timeout(mut self, t: &Duration) -> PutRequest {
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
    pub fn compartment_id(mut self, compartment_id: &str) -> PutRequest {
        self.compartment_id = compartment_id.to_string();
        self
    }

    /// Return information about the existing row, if present.
    /// Requesting this information incurs additional cost and may affect operation latency.
    pub fn return_row(mut self, val: bool) -> PutRequest {
        self.return_row = val;
        self
    }

    /// Specifies the optional time to live (TTL) value, causing the time to live on
    /// the row to be set to the specified value on put.
    ///
    /// Note: Internally, NoSQL uses a resolution of one hour for TTL values. This
    /// value, if given, will be converted to a whole number of hours. The minimum
    /// number of hours is 1.
    pub fn ttl(mut self, val: &Duration) -> PutRequest {
        self.ttl = val.clone();
        self
    }

    /// Specifies whether to use the table's default TTL for the row.
    /// If true, and there is an existing row, causes the operation to update
    /// the time to live (TTL) value of the row based on the table's default
    /// TTL if set. If the table has no default TTL this setting has no effect.
    /// By default updating an existing row has no effect on its TTL.
    pub fn use_table_ttl(mut self, val: bool) -> PutRequest {
        self.use_table_ttl = val;
        self
    }

    /// Succeed only if the given row exists and its version matches the given version.
    pub fn if_version(mut self, version: &Version) -> PutRequest {
        self.match_version = version.clone();
        self.if_present = false;
        self.if_absent = false;
        self
    }

    /// Succeed only of the given row does not already exist.
    pub fn if_absent(mut self) -> PutRequest {
        self.if_absent = true;
        self.if_present = false;
        self.match_version.clear();
        self
    }

    /// Succeed only of the given row already exists.
    pub fn if_present(mut self) -> PutRequest {
        self.if_present = true;
        self.if_absent = false;
        self.match_version.clear();
        self
    }

    pub async fn execute(&self, h: &Handle) -> Result<PutResult, NoSQLError> {
        let mut w: Writer = Writer::new();
        w.write_i16(h.inner.serial_version);
        let timeout = h.get_timeout(&self.timeout);
        self.serialize_internal(&mut w, false, false, &timeout);
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: false,
            compartment_id: self.compartment_id.clone(),
            ..Default::default()
        };
        let mut r = h.send_and_receive(w, &mut opts).await?;
        let resp = PutRequest::nson_deserialize(&mut r)?;
        Ok(resp)
    }

    fn serialize_internal(
        &self,
        w: &mut Writer,
        is_sub_request: bool,
        add_table_name: bool,
        timeout: &Duration,
    ) {
        let mut ns = NsonSerializer::start_request(w);
        let mut opcode = OpCode::Put;
        if self.match_version.len() > 0 {
            opcode = OpCode::PutIfVersion;
        } else if self.if_present {
            opcode = OpCode::PutIfPresent;
        } else if self.if_absent {
            opcode = OpCode::PutIfAbsent;
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
            //ns.write_i32_field(DURABILITY, 0); // TODO
        }

        ns.write_true_bool_field(RETURN_ROW, self.return_row);

        if self.match_version.len() > 0 {
            ns.write_binary_field(ROW_VERSION, &self.match_version);
        }

        if self.use_table_ttl {
            ns.write_bool_field(UPDATE_TTL, true);
        } else if self.ttl.as_secs() > 0 {
            // currently, NoSQL only allows DAYS or HOURS settings.
            // calculate a whole number of hours, and if it is evenly divisible by 24,
            // convert that to days.
            let mut hours = self.ttl.as_secs() / 3600;
            // minumum TTL
            if hours == 0 {
                hours = 1;
            }
            let mut ttl = format!("{} HOURS", hours);
            if (hours % 24) == 0 {
                ttl = format!("{} DAYS", hours / 24);
            }
            ns.write_string_field(TTL, &ttl);
            ns.write_bool_field(UPDATE_TTL, true);
        }

        ns.write_true_bool_field(EXACT_MATCH, self.exact_match);
        // TODO identity cache size

        ns.write_map_field(VALUE, &self.value);

        // TODO others

        if is_sub_request == false {
            ns.end_payload();
        }
        ns.end_request();
    }

    pub(crate) fn nson_deserialize(r: &mut Reader) -> Result<PutResult, NoSQLError> {
        let mut walker = MapWalker::new(r)?;
        let mut res: PutResult = Default::default();
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
                ROW_VERSION => {
                    //println!("   w: ROW_VERSION");
                    res.version = Some(walker.read_nson_binary()?);
                }
                GENERATED => {
                    //println!("   w: GENERATED");
                    res.generated_value = Some(walker.read_nson_field_value()?);
                    //println!("generated_value={:?}", res.generated_value);
                }
                RETURN_INFO => {
                    //println!("   w: RETURN_INFO");
                    read_return_info(walker.r, &mut res)?;
                }
                _ => {
                    //println!("   put_result: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                }
            }
        }
        Ok(res)
    }
}

// TODO: make this common to all write results
fn read_return_info(r: &mut Reader, res: &mut PutResult) -> Result<(), NoSQLError> {
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
                //println!("   put_result read_ri: skipping field '{}'", name);
                walker.skip_nson_field()?;
            }
        }
    }
    Ok(())
}

impl NsonRequest for PutRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.serialize_internal(w, false, true, timeout);
    }
}

impl NsonSubRequest for PutRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration) {
        self.serialize_internal(w, true, false, timeout);
    }
}
