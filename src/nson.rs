//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
#![allow(dead_code)]

use std::result::Result;
use std::str;
use std::time::Duration;

use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode::BadProtocolMessage;
use crate::error::NoSQLErrorCode::UnknownError;
use crate::reader::Reader;
use crate::types::Capacity;
use crate::types::CapacityMode;
use crate::types::Consistency;
use crate::types::FieldType;
use crate::types::FieldValue;
use crate::types::MapValue;
use crate::types::OpCode;
use crate::types::TableLimits;
use crate::types::TopologyInfo;
use crate::writer::Writer;

pub const V4_VERSION: i32 = 4;

// static field names
pub const ABORT_ON_FAIL: &str = "a";
pub const BIND_VARIABLES: &str = "bv";
pub const COMPARTMENT_OCID: &str = "cc";
pub const CONSISTENCY: &str = "co";
pub const CONSUMED: &str = "c";
pub const CONTINUATION_KEY: &str = "ck";
pub const DATA: &str = "d";
pub const DEFINED_TAGS: &str = "dt";
pub const DRIVER_QUERY_PLAN: &str = "dq";
pub const DURABILITY: &str = "du";
pub const END: &str = "en";
pub const ERROR_CODE: &str = "e";
pub const ETAG: &str = "et";
pub const EXACT_MATCH: &str = "ec";
pub const EXCEPTION: &str = "x";
pub const EXISTING_MOD_TIME: &str = "em";
pub const EXISTING_VALUE: &str = "el";
pub const EXISTING_VERSION: &str = "ev";
pub const EXPIRATION: &str = "xp";
pub const FIELDS: &str = "f";
pub const FREE_FORM_TAGS: &str = "ff";
pub const GENERATED: &str = "gn";
pub const GET_QUERY_PLAN: &str = "gq";
pub const GET_QUERY_SCHEMA: &str = "gs";
pub const HEADER: &str = "h";
pub const IDEMPOTENT: &str = "ip";
pub const IDENTITY_CACHE_SIZE: &str = "ic";
pub const INCLUSIVE: &str = "in";
pub const INDEX: &str = "i";
pub const INDEXES: &str = "ix";
pub const IS_JSON: &str = "j";
pub const IS_PREPARED: &str = "is";
pub const IS_SIMPLE_QUERY: &str = "iq";
pub const KEY: &str = "k";
pub const KV_VERSION: &str = "kv";
pub const LAST_INDEX: &str = "li";
pub const LIMITS: &str = "lm";
pub const LIMITS_MODE: &str = "mo";
pub const LIST_MAX_TO_READ: &str = "lx";
pub const LIST_START_INDEX: &str = "ls";
pub const MATCH_VERSION: &str = "mv";
pub const MATH_CONTEXT_CODE: &str = "mc";
pub const MATH_CONTEXT_PRECISION: &str = "cp";
pub const MATH_CONTEXT_ROUNDING_MODE: &str = "rm";
pub const MAX_READ_KB: &str = "mr";
pub const MAX_WRITE_KB: &str = "mw";
pub const MAX_SHARD_USAGE_PERCENT: &str = "ms";
pub const MODIFIED: &str = "md";
pub const NAME: &str = "m";
pub const NAMESPACE: &str = "ns";
pub const NOT_TARGET_TABLES: &str = "nt";
pub const NUMBER_LIMIT: &str = "nl";
pub const NUM_DELETIONS: &str = "nd";
pub const NUM_OPERATIONS: &str = "no";
pub const NUM_RESULTS: &str = "nr";
pub const OP_CODE: &str = "o";
pub const OPERATIONS: &str = "os";
pub const OPERATION_ID: &str = "od";
pub const PATH: &str = "pt";
pub const PAYLOAD: &str = "p";
pub const PREPARE: &str = "pp";
pub const PREPARED_QUERY: &str = "pq";
pub const PREPARED_STATEMENT: &str = "ps";
pub const PROXY_TOPO_SEQNUM: &str = "pn";
pub const QUERY: &str = "q";
pub const QUERY_OPERATION: &str = "qo";
pub const QUERY_PLAN_STRING: &str = "qs";
pub const QUERY_RESULTS: &str = "qr";
pub const QUERY_RESULT_SCHEMA: &str = "qc";
pub const QUERY_VERSION: &str = "qv";
pub const RANGE: &str = "rg";
pub const RANGE_PATH: &str = "rp";
pub const REACHED_LIMIT: &str = "re";
pub const READ_KB: &str = "rk";
pub const READ_THROTTLE_COUNT: &str = "rt";
pub const READ_UNITS: &str = "ru";
pub const RETRY_HINT: &str = "rh";
pub const RETURN_INFO: &str = "ri";
pub const RETURN_ROW: &str = "rr";
pub const ROW: &str = "r";
pub const ROW_VERSION: &str = "rv";
pub const SHARD_ID: &str = "si";
pub const SHARD_IDS: &str = "sa";
pub const SORT_PHASE1_RESULTS: &str = "p1";
pub const START: &str = "sr";
pub const STATEMENT: &str = "st";
pub const STORAGE_GB: &str = "sg";
pub const STORAGE_THROTTLE_COUNT: &str = "sl";
pub const SUCCESS: &str = "ss";
pub const SYSOP_RESULT: &str = "rs";
pub const SYSOP_STATE: &str = "ta";
pub const TABLES: &str = "tb";
pub const TABLE_ACCESS_INFO: &str = "ai";
pub const TABLE_DDL: &str = "td";
pub const TABLE_NAME: &str = "n";
pub const TABLE_OCID: &str = "to";
pub const TABLE_SCHEMA: &str = "ac";
pub const TABLE_STATE: &str = "as";
pub const TABLE_USAGE: &str = "u";
pub const TABLE_USAGE_PERIOD: &str = "pd";
pub const TIMEOUT: &str = "t";
pub const TOPOLOGY_INFO: &str = "tp";
pub const TOPO_SEQ_NUM: &str = "ts";
pub const TRACE_LEVEL: &str = "tl";
pub const TTL: &str = "tt";
pub const TYPE: &str = "y";
pub const UPDATE_TTL: &str = "ut";
pub const VALUE: &str = "l";
pub const VERSION: &str = "v";
pub const WM_FAILURE: &str = "wf";
pub const WM_FAIL_INDEX: &str = "wi";
pub const WM_FAIL_RESULT: &str = "wr";
pub const WM_SUCCESS: &str = "ws";
pub const WRITE_KB: &str = "wk";
pub const WRITE_MULTIPLE: &str = "wm";
pub const WRITE_THROTTLE_COUNT: &str = "wt";
pub const WRITE_UNITS: &str = "wu";

pub trait NsonRequest {
    fn serialize(&self, w: &mut Writer, timeout: &Duration);
    // TODO: fn opcode(&self) -> OpCode;
    // TODO: fn retryable(&self) -> bool;
}

pub trait NsonSubRequest: std::fmt::Debug {
    fn serialize(&self, w: &mut Writer, timeout: &Duration);
}

// The base struct used for all serialization.
pub struct NsonSerializer<'a> {
    // writer does the actual writing (to an internal byte array).
    writer: &'a mut Writer,
    // the number of bytes used for maps or arrays
    offset_stack: Vec<usize>,
    // the number of elements in maps or arrays
    size_stack: Vec<i32>,
}

// Maps and Arrays. These objects start with their total length,
// allowing them to be optionally skipped on deserialization.
//  1. start:
//    make a 4-byte space for the ultimate length of the serialized
//    object.
//  2. save the offset on a stack
//  3. start counting elements on a stack
//  4. ... entries are written
//  5. end:
//    a. pop the offset stack to get the original length offset
//    write the real length into the spot that was held
//    b. pop the size stack to get the number of elements
//    write the real number of elements the spot that was held
// NOTE: a full 4-byte integer is used to avoid the variable-length
// encoding used by compressed integers.
//
// It would be more efficient and avoid an extra stack with pop/push
// for each map/array element to rely on the size from the caller
// but counting elements here is safer and doesn't rely on the caller
// having access to the size information. For example, a caller may be
// turning a List (via iterator) into an array. That is less likely
// for a Map but it's simplest to keep them the same. Alternatively
// the caller could track number of elements and send it correctly in
// the end* calls but again, that relies on the caller.

impl<'a> NsonSerializer<'a> {
    pub fn new(writer: &'a mut Writer) -> NsonSerializer {
        NsonSerializer {
            writer: writer,
            offset_stack: Vec::new(),
            size_stack: Vec::new(),
        }
    }

    pub(crate) fn incr_size(&mut self, delta: i32) {
        if self.size_stack.len() > 0 {
            let mut i = self.size_stack.pop().unwrap();
            i += delta;
            self.size_stack.push(i);
        }
    }

    pub fn start_field(&mut self, key: &str) {
        self.writer.write_string(&key);
    }

    pub fn end_field(&mut self, _key: &str) {
        self.incr_size(1);
    }

    fn start_map_or_array(&mut self, field: &str, ftype: FieldType) {
        if field != "" {
            self.start_field(field);
        }
        self.writer.write_field_type(ftype);
        let off = self.writer.size();
        self.writer.write_i32(0); // size in bytes
        self.writer.write_i32(0); // number of elements
        self.offset_stack.push(off);
        self.size_stack.push(0);
    }

    pub fn start_map(&mut self, field: &str) {
        self.start_map_or_array(field, FieldType::Map);
    }

    pub fn start_array(&mut self, field: &str) {
        self.start_map_or_array(field, FieldType::Array);
    }

    pub fn end_map(&mut self, field: &str) {
        let length_offset = self.offset_stack.pop().unwrap();
        let num_elems = self.size_stack.pop().unwrap();
        let start = length_offset + 4;
        // write size in bytes, then number of elements into the space reserved
        //println!("EM({}): size={} start={} lo={} ne={}", field, self.writer.size(), start, length_offset, num_elems);
        self.writer
            .write_i32_at_offset((self.writer.size() - start) as i32, length_offset)
            .unwrap();
        self.writer
            .write_i32_at_offset(num_elems, length_offset + 4)
            .unwrap();
        if field != "" {
            self.end_field(field)
        }
    }

    pub fn write_subrequest(&mut self, rq: &Box<dyn NsonSubRequest>, timeout: &Duration) {
        rq.serialize(self.writer, timeout);
    }

    pub fn end_array(&mut self, field: &str) {
        self.end_map(field);
    }

    pub fn start_array_field(&mut self, _idx: i32) {
        // nothing to do
    }

    pub fn end_array_field(&mut self, _idx: i32) {
        self.incr_size(1);
    }

    pub fn write_field(&mut self, key: &str, val: &FieldValue) {
        self.start_field(key);
        self.writer.write_field_value(val);
        self.end_field(key);
    }

    pub fn write_i32_field(&mut self, key: &str, val: i32) {
        self.start_field(key);
        self.writer.write_field_type(FieldType::Integer);
        self.writer.write_packed_i32(val);
        self.end_field(key);
    }

    pub fn write_nonzero_i32_field(&mut self, key: &str, val: i32) {
        if val != 0 {
            self.write_i32_field(key, val);
        }
    }

    pub fn write_bool_field(&mut self, key: &str, val: bool) {
        self.start_field(key);
        self.writer.write_field_type(FieldType::Boolean);
        self.writer.write_bool(val);
        self.end_field(key);
    }

    // Write field only if boolean is true
    pub fn write_true_bool_field(&mut self, key: &str, val: bool) {
        if val {
            self.write_bool_field(key, val);
        }
    }

    pub fn write_binary_field(&mut self, key: &str, val: &[u8]) {
        self.start_field(key);
        self.writer.write_field_type(FieldType::Binary);
        self.writer.write_bytes(val);
        self.end_field(key);
    }

    pub fn write_optional_binary_field(&mut self, key: &str, val: Option<Vec<u8>>) {
        if let Some(v) = val {
            self.write_binary_field(key, &v);
        }
    }

    pub fn write_map_field(&mut self, key: &str, val: &MapValue) {
        self.start_field(key);
        self.writer.write_map_value(val);
        self.end_field(key);
    }

    pub fn write_nz_field(&mut self, key: &str, val: i32) {
        if val > 0 {
            self.write_i32_field(key, val);
        }
    }

    pub fn write_string_field(&mut self, key: &str, val: &str) {
        self.start_field(key);
        self.writer.write_field_type(FieldType::String);
        self.writer.write_string(val);
        self.end_field(key);
    }

    pub fn write_nonempty_string_field(&mut self, key: &str, val: &str) {
        if val != "" {
            self.write_string_field(key, val);
        }
    }

    pub fn write_consistency(&mut self, c: Consistency) {
        self.start_map(CONSISTENCY);
        let t = (c as i32) - 1;
        self.write_i32_field(TYPE, t);
        self.end_map(CONSISTENCY);
    }

    pub(crate) fn write_header(&mut self, op_code: OpCode, timeout: &Duration, table_name: &str) {
        self.write_i32_field(VERSION, V4_VERSION);
        if table_name != "" {
            self.write_string_field(TABLE_NAME, table_name);
        }
        self.write_i32_field(OP_CODE, op_code as i32);
        self.write_i32_field(TIMEOUT, timeout.as_millis() as i32);
    }

    pub(crate) fn write_limits(&mut self, limits: &Option<TableLimits>) {
        if let Some(l) = limits {
            self.start_map(LIMITS);
            self.write_i32_field(READ_UNITS, l.read_units);
            self.write_i32_field(WRITE_UNITS, l.write_units);
            self.write_i32_field(STORAGE_GB, l.storage_gb);
            self.write_i32_field(LIMITS_MODE, l.mode as i32);
            self.end_map(LIMITS);
        }
    }

    pub fn start_header(&mut self) {
        self.start_map(HEADER);
    }

    pub fn end_header(&mut self) {
        self.end_map(HEADER);
    }

    pub fn start_payload(&mut self) {
        self.start_map(PAYLOAD);
    }

    pub fn end_payload(&mut self) {
        self.end_map(PAYLOAD);
    }

    pub fn start_request(writer: &'a mut Writer) -> NsonSerializer {
        let mut ns = NsonSerializer::new(writer);
        ns.start_map("");
        ns
    }

    pub fn end_request(&mut self) {
        self.end_map("");
    }
}

pub struct MapWalker<'a> {
    pub(crate) r: &'a mut Reader,
    num_elements: i32,
    current_name: String,
    current_index: i32,
}

// To prevent infinte loops
const MAX_ELEMENTS: i32 = 100000000;

impl<'a> MapWalker<'a> {
    pub fn new(r: &'a mut Reader) -> Result<MapWalker, NoSQLError> {
        Self::expect_type(r, FieldType::Map)?;
        let _ = r.read_i32()?; // skip map size in bytes
        let num_elements = r.read_i32()?;
        if num_elements < 0 || num_elements > MAX_ELEMENTS {
            // TODO NoSQL error type
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "invalid num_elements in message",
            ));
        }
        //println!("MapWalker: num_elements={}", num_elements);
        Ok(MapWalker {
            r,
            num_elements,
            current_name: "".to_string(),
            current_index: 0,
        })
    }

    pub(crate) fn expect_type(r: &mut Reader, ft: FieldType) -> Result<(), NoSQLError> {
        let b = r.read_byte()?;
        let fb: u8 = ft as u8;
        if b != fb {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                format!("expected type {}, found {}", fb, b).as_str(),
            ));
        }
        Ok(())
    }

    pub fn has_next(&self) -> bool {
        self.num_elements > self.current_index
    }

    pub fn next(&mut self) -> Result<(), NoSQLError> {
        if self.has_next() == false {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "cannot call next with no elements remaining",
            ));
        }
        self.current_name = self.r.read_string()?;
        self.current_index += 1;
        Ok(())
    }

    pub fn current_name(&self) -> &String {
        // TODO: how to get UPPERCASE_FIELD from short field name?
        //println!("walker: name={}", self.current_name);
        &self.current_name
    }

    pub fn read_nson_map(&mut self) -> Result<MapValue, NoSQLError> {
        Self::expect_type(self.r, FieldType::Map)?;
        Ok(self.r.read_map()?)
    }

    pub fn read_nson_i64(&mut self) -> Result<i64, NoSQLError> {
        Self::expect_type(self.r, FieldType::Long)?;
        let i = self.r.read_packed_i64()?;
        //println!("read_nson_i64={}", i);
        Ok(i)
    }

    pub fn read_nson_i32(&mut self) -> Result<i32, NoSQLError> {
        Self::expect_type(self.r, FieldType::Integer)?;
        let i = self.r.read_packed_i32()?;
        //println!("read_nson_i32={}", i);
        Ok(i)
    }

    pub fn read_nson_string(&mut self) -> Result<String, NoSQLError> {
        Self::expect_type(self.r, FieldType::String)?;
        let i = self.r.read_string()?;
        //println!("read_nson_string={}", i);
        Ok(i)
    }

    pub fn read_nson_string_array(&mut self) -> Result<Vec<String>, NoSQLError> {
        Self::expect_type(self.r, FieldType::Array)?;
        let _ = self.r.read_i32()?; // skip array size in bytes
        let num_elements = self.r.read_i32()?;
        let mut v: Vec<String> = Vec::with_capacity(num_elements as usize);
        for _n in 1..=num_elements {
            v.push(self.read_nson_string()?);
        }
        //println!("read_nson_string_array={:?}", v);
        Ok(v)
    }

    pub fn read_nson_i32_array(&mut self) -> Result<Vec<i32>, NoSQLError> {
        Self::expect_type(self.r, FieldType::Array)?;
        let _ = self.r.read_i32()?; // skip array size in bytes
        let num_elements = self.r.read_i32()?;
        let mut v: Vec<i32> = Vec::with_capacity(num_elements as usize);
        for _n in 1..=num_elements {
            v.push(self.read_nson_i32()?);
        }
        //println!("read_nson_i32_array={:?}", v);
        Ok(v)
    }

    pub fn read_nson_binary(&mut self) -> Result<Vec<u8>, NoSQLError> {
        Self::expect_type(self.r, FieldType::Binary)?;
        let v = self.r.read_binary()?;
        Ok(v)
    }

    pub fn read_nson_boolean(&mut self) -> Result<bool, NoSQLError> {
        Self::expect_type(self.r, FieldType::Boolean)?;
        let b = self.r.read_bool()?;
        Ok(b)
    }

    pub fn read_nson_field_value(&mut self) -> Result<FieldValue, NoSQLError> {
        let fv = self.r.read_field_value()?;
        Ok(fv)
    }

    pub fn read_nson_consumed_capacity(&mut self) -> Result<Capacity, NoSQLError> {
        // consumed capacity is in its own map
        let mut mw = MapWalker::new(self.r)?;
        let mut c: Capacity = Default::default();
        while mw.has_next() {
            mw.next()?;
            let name = mw.current_name();
            match name.as_str() {
                READ_KB => {
                    c.read_kb = mw.read_nson_i32()?;
                }
                WRITE_KB => {
                    c.write_kb = mw.read_nson_i32()?;
                }
                READ_UNITS => {
                    c.read_units = mw.read_nson_i32()?;
                }
                _ => mw.skip_nson_field()?,
            }
        }
        Ok(c)
    }

    pub(crate) fn read_nson_topology_info(&mut self) -> Result<TopologyInfo, NoSQLError> {
        let mut mw = MapWalker::new(self.r)?;
        let mut ti: TopologyInfo = Default::default();
        ti.seq_num = -1;
        while mw.has_next() {
            mw.next()?;
            let name = mw.current_name();
            match name.as_str() {
                PROXY_TOPO_SEQNUM => {
                    ti.seq_num = mw.read_nson_i32()?;
                }
                SHARD_IDS => {
                    ti.shard_ids = mw.read_nson_i32_array()?;
                }
                _ => mw.skip_nson_field()?,
            }
        }
        if ti.is_valid() == false {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "Topology info missing field(s)",
            ));
        }
        Ok(ti)
    }

    pub fn read_nson_limits(&mut self) -> Result<TableLimits, NoSQLError> {
        let mut mw = MapWalker::new(self.r)?;
        let mut limits: TableLimits = Default::default();
        while mw.has_next() {
            mw.next()?;
            let name = mw.current_name();
            match name.as_str() {
                READ_UNITS => {
                    limits.read_units = mw.read_nson_i32()?;
                }
                WRITE_UNITS => {
                    limits.write_units = mw.read_nson_i32()?;
                }
                STORAGE_GB => {
                    limits.storage_gb = mw.read_nson_i32()?;
                }
                LIMITS_MODE => {
                    let mode = mw.read_nson_i32()?;
                    // default is Provisioned
                    if mode == (CapacityMode::OnDemand as i32) {
                        limits.mode = CapacityMode::OnDemand;
                    }
                }
                _ => mw.skip_nson_field()?,
            }
        }
        Ok(limits)
    }

    //pub fn handle_error_code(&mut self) -> Result<(), NoSQLError> {
    pub fn handle_error_code(&mut self) -> Result<(), NoSQLError> {
        let i = self.read_nson_i32()?;
        if i == 0 {
            return Ok(());
        }
        // read to end of walker
        while self.has_next() {
            self.next()?;
            let name = self.current_name();
            match name.as_str() {
                EXCEPTION => {
                    //println!("   error: EXCEPTION");
                    let msg = self.read_nson_string()?;
                    let err = NoSQLError::from_int(i, &msg);
                    //println!("Got error: {}", err);
                    return Err(err);
                }
                _ => {
                    //println!("   error: skipping field '{}'", name);
                    self.skip_nson_field()?;
                }
            }
        }
        let err = NoSQLError::new(UnknownError, "Unknown error");
        //println!("Got error: {}", err);
        Err(err)
    }

    pub fn skip_nson_field(&mut self) -> Result<(), NoSQLError> {
        // TODO: optimize to skip bytes instead of realizing them
        let _ = self.r.read_field_value()?;
        Ok(())
    }

    pub fn check_nson_for_error(&mut self) -> Result<(), NoSQLError> {
        while self.has_next() {
            self.next()?;
            let name = self.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    //println!("   w: ERROR_CODE");
                    self.handle_error_code()?;
                    // if we get to here, the error code was zero
                    return Ok(());
                }
                _ => {
                    self.skip_nson_field()?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn check_reader_for_error(reader: &mut Reader) -> Result<(), NoSQLError> {
        let mut w = MapWalker::new(reader)?;
        w.check_nson_for_error()?;
        reader.reset();
        Ok(())
    }
}
