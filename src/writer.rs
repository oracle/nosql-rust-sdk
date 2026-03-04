//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use chrono::{DateTime, FixedOffset};
use std::result::Result;
use std::str;

use crate::error::{ia_err, NoSQLError};
use crate::packed_integer;
use crate::types::FieldType;
use crate::types::FieldValue;
use crate::types::MapValue;

// Writer encodes data into the wire format for the Binary Protocol and writes
// to a buffer. The Binary Protocol defines the data exchange format between
// the Oracle NoSQL Database proxy and drivers.
//
// Writer implements the io:Write trait
pub struct Writer {
    // The underlying byte buffer.
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Writer {
        Writer {
            buf: Vec::with_capacity(256),
        }
    }

    // implementation for io::Write trait
    pub fn write(&mut self, val: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(val);
        return Ok(val.len());
    }

    pub fn write_byte(&mut self, val: u8) {
        self.buf.push(val);
    }

    pub(crate) fn write_field_type(&mut self, ft: FieldType) {
        self.write_byte(ft as u8);
    }

    pub fn write_bool(&mut self, val: bool) {
        self.write_byte(val as u8);
    }

    pub fn write_bytes(&mut self, val: &[u8]) {
        //println!("write_bytes: slice_len={}, cur_len={}", val.len(), self.buf.len());
        self.write_packed_i32(val.len() as i32);
        self.buf.extend_from_slice(val);
        //println!("write_bytes: new_len={}", self.buf.len());
    }

    pub fn write_i16(&mut self, val: i16) {
        self.buf.extend_from_slice(&val.to_be_bytes());
    }

    pub fn write_i32(&mut self, val: i32) {
        self.buf.extend_from_slice(&val.to_be_bytes());
    }

    pub fn write_float64(&mut self, val: f64) {
        self.buf.extend_from_slice(&val.to_be_bytes());
    }

    pub fn write_i32_at_offset(&mut self, val: i32, offset: usize) -> Result<(), NoSQLError> {
        if (offset + 4) > self.buf.len() {
            return ia_err!(
                "Invalid offset passed to write_i32_at_offset: len={} offset={}",
                self.buf.len(),
                offset
            );
        }
        let arr = val.to_be_bytes();
        for i in 1..4 {
            self.buf[offset + i] = arr[i];
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.buf.len()
    }

    pub fn bytes(&self) -> &[u8] {
        self.buf.as_slice()
    }

    pub fn reset(&mut self) {
        self.buf.clear();
    }

    pub fn write_packed_i32(&mut self, val: i32) {
        packed_integer::write_packed_i32(&mut self.buf, val);
    }

    pub fn write_packed_i64(&mut self, val: i64) {
        packed_integer::write_packed_i64(&mut self.buf, val);
    }

    pub fn write_string(&mut self, val: &str) {
        let slen = val.len() as i32;
        // TODO: write -1 for null string (???)
        packed_integer::write_packed_i32(&mut self.buf, slen);
        self.buf.extend_from_slice(val.as_bytes());
    }

    pub fn write_timestamp(&mut self, val: &DateTime<FixedOffset>) {
        self.write_string(&val.to_rfc3339());
    }

    pub fn write_binary(&mut self, val: &Vec<u8>) {
        let slen = val.len() as i32;
        // TODO: write -1 for null string (???)
        packed_integer::write_packed_i32(&mut self.buf, slen);
        self.buf.extend_from_slice(&val);
    }

    pub fn write_field_value(&mut self, val: &FieldValue) {
        match val {
            FieldValue::Integer(i) => {
                self.write_field_type(FieldType::Integer);
                self.write_packed_i32(*i);
            }
            FieldValue::Long(i) => {
                self.write_field_type(FieldType::Long);
                self.write_packed_i64(*i);
            }
            FieldValue::String(s) => {
                self.write_field_type(FieldType::String);
                self.write_string(s);
            }
            FieldValue::Array(a) => {
                self.write_field_type(FieldType::Array);
                self.write_array(a);
            }
            FieldValue::Binary(b) => {
                self.write_field_type(FieldType::Binary);
                self.write_binary(b);
            }
            FieldValue::Map(m) => {
                self.write_field_type(FieldType::Map);
                self.write_map(m);
            }
            FieldValue::Boolean(b) => {
                self.write_field_type(FieldType::Boolean);
                self.write_bool(*b);
            }
            FieldValue::Double(d) => {
                self.write_field_type(FieldType::Double);
                self.write_float64(*d);
            }
            FieldValue::Timestamp(ts) => {
                self.write_field_type(FieldType::Timestamp);
                self.write_timestamp(ts);
            }
            FieldValue::Number(s) => {
                self.write_field_type(FieldType::Number);
                self.write_string(&s.to_string());
            }
            FieldValue::Null => {
                self.write_field_type(FieldType::Null);
            }
            FieldValue::JsonNull => {
                self.write_field_type(FieldType::JsonNull);
            }
            //FieldValue::JsonNull => { self.write_field_type(FieldType::Null); },
            FieldValue::Empty => {
                self.write_field_type(FieldType::Empty);
            }
            FieldValue::Uninitialized => (),
        }
    }

    pub fn write_map_value(&mut self, val: &MapValue) {
        self.write_field_type(FieldType::Map);
        self.write_map(val);
    }

    pub fn write_array(&mut self, val: &Vec<FieldValue>) {
        // first 4 bytes is the overall size of this array, not including these 4 bytes
        // this will be set properly at the end of this function
        // get current offset
        let off = self.buf.len();
        self.write_i32(0);

        // next 4 bytes is the number of items in the array
        self.write_i32(val.len() as i32);

        // Then all the items
        for item in val.iter() {
            // FieldValue value
            self.write_field_value(item);
        }

        // when done, go back and write the actual map length
        let bsize = self.buf.len() - off - 4;
        //println!("write_array: offset={} size={}", off, bsize);
        self.write_i32_at_offset(bsize as i32, off).unwrap();
    }

    pub fn write_map(&mut self, val: &MapValue) {
        // first 4 bytes is the overall size of this map, not including these 4 bytes
        // this will be set properly at the end of this function
        // get current offset
        let off = self.buf.len();
        self.write_i32(0);

        // next 4 bytes is the number of items in the map
        self.write_i32(val.len() as i32);

        // Then all the items
        for (key, item) in val.iter() {
            self.write_string(key);
            self.write_field_value(item);
        }

        // when done, go back and write the actual map length
        let bsize = self.buf.len() - off - 4;
        self.write_i32_at_offset(bsize as i32, off).unwrap();
        //println!("WM: size={} start={} ne={}", bsize, off, val.len());
    }

    pub fn dump_binary(&self) {
        let s = self.buf.len();
        for i in 0..s {
            print!("{:#04x},", self.buf[i]);
        }
        println!("");
    }
}

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}
