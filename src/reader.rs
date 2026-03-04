//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use bigdecimal::BigDecimal;
use bigdecimal::Num;
use chrono::{DateTime, FixedOffset};
use std::result;
use std::str;

use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode::BadProtocolMessage;
use crate::error::NoSQLErrorCode::IllegalArgument;
use crate::packed_integer;
use crate::types::string_to_rfc3339;
use crate::types::FieldType;
use crate::types::FieldValue;
use crate::types::MapValue;

// Reader reads byte sequences from the underlying io.Reader and decodes the
// bytes to construct in-memory representations according to the Binary Protocol
// which defines the data exchange format between the Oracle NoSQL Database
// proxy and drivers.
pub struct Reader {
    // The underlying byte buffer.
    pub buf: Vec<u8>,
    pub offset: usize,
}

impl Reader {
    pub fn new() -> Reader {
        Reader {
            buf: Vec::with_capacity(256),
            offset: 0,
        }
    }

    pub fn from_bytes(mut self, val: &[u8]) -> Self {
        self.buf.clear();
        self.buf.extend_from_slice(val);
        self
    }

    pub fn read_byte(&mut self) -> result::Result<u8, NoSQLError> {
        //println!("Read_byte: offset={} len={}", self.offset, self.buf.len());
        if self.offset >= self.buf.len() {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "read_byte reached end of byte buffer",
            ));
        }
        let val: u8 = self.buf[self.offset];
        self.offset += 1;
        Ok(val)
    }

    pub fn read_bool(&mut self) -> result::Result<bool, NoSQLError> {
        let v = self.read_byte()?;
        Ok(v != 0)
    }

    pub fn read_i16(&mut self) -> result::Result<i16, NoSQLError> {
        if (self.offset + 2) > self.buf.len() {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "read_i16 reached end of byte buffer",
            ));
        }
        let val: [u8; 2] = [self.buf[self.offset], self.buf[self.offset + 1]];
        self.offset += 2;
        Ok(i16::from_be_bytes(val))
    }

    pub fn read_u16(&mut self) -> result::Result<u16, NoSQLError> {
        if (self.offset + 2) > self.buf.len() {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "read_u16 reached end of byte buffer",
            ));
        }
        let val: [u8; 2] = [self.buf[self.offset], self.buf[self.offset + 1]];
        self.offset += 2;
        Ok(u16::from_be_bytes(val))
    }

    pub fn read_i32(&mut self) -> result::Result<i32, NoSQLError> {
        if (self.offset + 4) > self.buf.len() {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "read_i32 reached end of byte buffer",
            ));
        }
        let val: [u8; 4] = [
            self.buf[self.offset],
            self.buf[self.offset + 1],
            self.buf[self.offset + 2],
            self.buf[self.offset + 3],
        ];
        self.offset += 4;
        Ok(i32::from_be_bytes(val))
    }

    pub fn read_i32_min(&mut self, min: i32) -> result::Result<i32, NoSQLError> {
        let i = self.read_i32()?;
        if i >= min {
            return Ok(i);
        }
        Err(NoSQLError::new(
            IllegalArgument,
            format!(
                "NoSQL: invalid integer value {}, must be greater than or equal to {}",
                i, min
            )
            .as_str(),
        ))
    }

    pub fn read_float64(&mut self) -> result::Result<f64, NoSQLError> {
        if (self.offset + 8) > self.buf.len() {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "read_float64 reached end of byte buffer",
            ));
        }
        let val: [u8; 8] = [
            self.buf[self.offset],
            self.buf[self.offset + 1],
            self.buf[self.offset + 2],
            self.buf[self.offset + 3],
            self.buf[self.offset + 4],
            self.buf[self.offset + 5],
            self.buf[self.offset + 6],
            self.buf[self.offset + 7],
        ];
        self.offset += 8;
        Ok(f64::from_be_bytes(val))
    }

    pub fn read_packed_i32(&mut self) -> Result<i32, NoSQLError> {
        packed_integer::read_packed_i32(&mut self.buf, &mut self.offset)
    }

    pub fn read_packed_i64(&mut self) -> Result<i64, NoSQLError> {
        packed_integer::read_packed_i64(&mut self.buf, &mut self.offset)
    }

    pub fn read_string(&mut self) -> Result<String, NoSQLError> {
        let slen = packed_integer::read_packed_i32(&mut self.buf, &mut self.offset)?;
        if slen <= 0 {
            // TODO: how to simulate null string for len < 0?
            return Ok("".to_string());
        }
        let ulen = slen as usize;
        if (self.offset + ulen) > self.buf.len() {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "read_string reached end of byte buffer",
            ));
        }
        //let s = str::from_utf8(&self.buf[self.offset..(self.offset+ulen)])?;
        //self.offset += ulen;
        //Ok(std::string::String::from(s))
        match str::from_utf8(&self.buf[self.offset..(self.offset + ulen)]) {
            Ok(s) => {
                self.offset += ulen;
                return Ok(std::string::String::from(s));
            }
            Err(_) => {
                return Err(NoSQLError::new(
                    BadProtocolMessage,
                    "invalid utf8 in read_string",
                ));
            }
        }
    }

    pub fn read_timestamp(&mut self) -> Result<DateTime<FixedOffset>, NoSQLError> {
        let slen = packed_integer::read_packed_i32(&mut self.buf, &mut self.offset)?;
        if slen <= 0 {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "empty read on timestamp value",
            ));
        }
        let ulen = slen as usize;
        if (self.offset + ulen) > self.buf.len() {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "read_timestamp reached end of byte buffer",
            ));
        }
        match str::from_utf8(&self.buf[self.offset..(self.offset + ulen)]) {
            Ok(s) => {
                self.offset += ulen;
                return string_to_rfc3339(s);
            }
            Err(_) => {
                return Err(NoSQLError::new(
                    BadProtocolMessage,
                    "invalid utf8 in read_timestamp",
                ));
            }
        }
    }

    pub fn read_binary(&mut self) -> Result<Vec<u8>, NoSQLError> {
        let slen = packed_integer::read_packed_i32(&mut self.buf, &mut self.offset)?;
        if slen <= 0 {
            return Ok(Vec::new());
        }
        let ulen = slen as usize;
        if (self.offset + ulen) > self.buf.len() {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "read_binary reached end of byte buffer",
            ));
        }
        self.offset += ulen;
        Ok(Vec::from(&self.buf[(self.offset - ulen)..self.offset]))
    }

    pub(crate) fn read_field_value(&mut self) -> Result<FieldValue, NoSQLError> {
        // read field type
        let u = self.read_byte()?;
        //println!(" read_byte={}", u);
        let ftype: FieldType = FieldType::try_from(u).map_err(|_| {
            NoSQLError::new(
                IllegalArgument,
                format!("can't convert field type byte {} to valid field type", u).as_str(),
            )
        })?;
        match ftype {
            FieldType::Integer => {
                let i32 = self.read_packed_i32()?;
                return Ok(FieldValue::Integer(i32));
            }
            FieldType::Long => {
                let i64 = self.read_packed_i64()?;
                return Ok(FieldValue::Long(i64));
            }
            FieldType::Double => {
                let f64 = self.read_float64()?;
                return Ok(FieldValue::Double(f64));
            }
            FieldType::String => {
                let str = self.read_string()?;
                return Ok(FieldValue::String(str));
            }
            FieldType::Array => {
                let arr = self.read_array()?;
                return Ok(FieldValue::Array(arr));
            }
            FieldType::Map => {
                let map = self.read_map()?;
                return Ok(FieldValue::Map(map));
            }
            FieldType::Boolean => {
                let b = self.read_bool()?;
                return Ok(FieldValue::Boolean(b));
            }
            FieldType::Binary => {
                let bin = self.read_binary()?;
                return Ok(FieldValue::Binary(bin));
            }
            FieldType::Timestamp => {
                let dt = self.read_timestamp()?;
                return Ok(FieldValue::Timestamp(dt));
            }
            FieldType::Number => {
                let num = self.read_string()?;
                return Ok(FieldValue::Number(
                    BigDecimal::from_str_radix(&num, 10).map_err(|_| {
                        NoSQLError::new(
                            IllegalArgument,
                            format!("can't convert string '{}' to valid BigDecimal", &num).as_str(),
                        )
                    })?,
                ));
            }
            FieldType::Null => {
                return Ok(FieldValue::Null);
            }
            FieldType::JsonNull => {
                return Ok(FieldValue::JsonNull);
                //return Ok(FieldValue::Null);
            }
            FieldType::Empty => {
                return Ok(FieldValue::Empty);
            }
        }
    }

    pub fn read_array(&mut self) -> Result<Vec<FieldValue>, NoSQLError> {
        // number of bytes consumed by the array.
        let _num_bytes = self.read_i32()?;
        // number of items in the array
        let num_items = self.read_i32()?;
        // walk items
        //println!("read_array: num_items={}", num_items);
        let mut arr = Vec::<FieldValue>::with_capacity(num_items as usize);
        for _i in 0..num_items {
            let v = self.read_field_value()?;
            //println!(" array element {}: {:?}", i, v);
            arr.push(v);
            //arr.push(self.read_field_value()?);
        }
        Ok(arr)
    }

    pub fn read_string_array(&mut self) -> Result<Vec<String>, NoSQLError> {
        let len = self.read_packed_i32()?;
        if len < -1 {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "Invalid array length in read_string_array",
            ));
        }
        if len <= 0 {
            return Ok(Vec::new());
        }
        let mut arr: Vec<String> = Vec::with_capacity(len as usize);
        for _i in 0..len {
            arr.push(self.read_string()?);
        }
        Ok(arr)
    }

    pub fn read_i32_array(&mut self) -> Result<Vec<i32>, NoSQLError> {
        let len = self.read_packed_i32()?;
        if len < -1 {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                "Invalid array length in read_i32_array",
            ));
        }
        if len <= 0 {
            return Ok(Vec::new());
        }
        //println!("read_i32_array: len={}", len);
        let mut arr: Vec<i32> = Vec::with_capacity(len as usize);
        for _i in 0..len {
            arr.push(self.read_packed_i32()?);
        }
        Ok(arr)
    }

    pub fn read_map(&mut self) -> Result<MapValue, NoSQLError> {
        // number of bytes consumed by the map.
        let _num_bytes = self.read_i32()?;
        // number of items in the map
        let num_items = self.read_i32()?;
        // walk items
        //println!("read_map: num_items={}", num_items);
        let mut mv = MapValue::new();
        for _i in 0..num_items {
            let key = self.read_string()?;
            //println!("Reading field '{}'", key);
            let val = self.read_field_value()?;
            //println!("read key '{}' with value {:?}", key, val);
            mv.put_field_value(&key, val);
        }
        Ok(mv)
    }

    pub(crate) fn reset(&mut self) {
        self.offset = 0;
    }
}
