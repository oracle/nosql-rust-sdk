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

// Bound recursive array/map decoding so malformed responses cannot exhaust the
// thread stack. Normal SDK payloads have far shallower nesting.
pub(crate) const MAX_FIELD_VALUE_NESTING_DEPTH: usize = 100;

// Reader reads byte sequences from the underlying io.Reader and decodes the
// bytes to construct in-memory representations according to the Binary Protocol
// which defines the data exchange format between the Oracle NoSQL Database
// proxy and drivers.
pub struct Reader {
    // The underlying byte buffer.
    pub buf: Vec<u8>,
    pub offset: usize,
    // Collected while decoding a driver query plan, then checked once the
    // plan's register count (which follows the serialized iterator tree) is read.
    query_plan_result_registers: Vec<i32>,
}

impl Reader {
    pub fn new() -> Reader {
        Reader {
            buf: Vec::with_capacity(256),
            offset: 0,
            query_plan_result_registers: Vec::new(),
        }
    }

    pub fn from_bytes(mut self, val: &[u8]) -> Self {
        self.buf.clear();
        self.buf.extend_from_slice(val);
        self.query_plan_result_registers.clear();
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

    pub(crate) fn read_query_plan_result_reg(&mut self) -> Result<i32, NoSQLError> {
        let result_reg = self.read_i32()?;
        self.query_plan_result_registers.push(result_reg);
        Ok(result_reg)
    }

    pub(crate) fn validate_query_plan_result_regs(
        &self,
        num_registers: i32,
    ) -> Result<(), NoSQLError> {
        if let Some(result_reg) = self
            .query_plan_result_registers
            .iter()
            .find(|result_reg| **result_reg < 0 || **result_reg >= num_registers)
        {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                format!(
                    "invalid query plan result register {}; number of registers is {}",
                    result_reg, num_registers
                )
                .as_str(),
            ));
        }
        Ok(())
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

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.offset)
    }

    pub(crate) fn checked_count(&self, count: i32, context: &str) -> Result<usize, NoSQLError> {
        Self::checked_count_with_limit(count, self.remaining(), context)
    }

    pub(crate) fn checked_count_with_limit(
        count: i32,
        max_count: usize,
        context: &str,
    ) -> Result<usize, NoSQLError> {
        if count < 0 {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                format!("invalid negative count in {}", context).as_str(),
            ));
        }
        let count = count as usize;
        if count > max_count {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                format!("invalid count in {}", context).as_str(),
            ));
        }
        Ok(count)
    }

    pub(crate) fn try_reserve_vec<T>(
        values: &mut Vec<T>,
        additional: usize,
        context: &str,
    ) -> Result<(), NoSQLError> {
        values.try_reserve(additional).map_err(|_| {
            NoSQLError::new(
                BadProtocolMessage,
                format!("unable to reserve decoded values for {}", context).as_str(),
            )
        })
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
        self.read_field_value_with_depth(0)
    }

    fn read_field_value_with_depth(
        &mut self,
        nesting_depth: usize,
    ) -> Result<FieldValue, NoSQLError> {
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
                let arr = self.read_array_with_depth(self.next_nesting_depth(nesting_depth)?)?;
                return Ok(FieldValue::Array(arr));
            }
            FieldType::Map => {
                let map = self.read_map_with_depth(self.next_nesting_depth(nesting_depth)?)?;
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

    fn next_nesting_depth(&self, nesting_depth: usize) -> Result<usize, NoSQLError> {
        if nesting_depth >= MAX_FIELD_VALUE_NESTING_DEPTH {
            return Err(NoSQLError::new(
                BadProtocolMessage,
                format!(
                    "field value nesting depth exceeds limit of {}",
                    MAX_FIELD_VALUE_NESTING_DEPTH
                )
                .as_str(),
            ));
        }
        Ok(nesting_depth + 1)
    }

    pub fn read_array(&mut self) -> Result<Vec<FieldValue>, NoSQLError> {
        self.read_array_with_depth(1)
    }

    fn read_array_with_depth(
        &mut self,
        nesting_depth: usize,
    ) -> Result<Vec<FieldValue>, NoSQLError> {
        // number of bytes consumed by the array.
        let _num_bytes = self.read_i32()?;
        // number of items in the array
        let num_items = self.read_i32()?;
        let num_items = self.checked_count(num_items, "array message")?;
        // walk items
        //println!("read_array: num_items={}", num_items);
        let mut arr = Vec::<FieldValue>::new();
        Self::try_reserve_vec(&mut arr, num_items, "array message")?;
        for _i in 0..num_items {
            let v = self.read_field_value_with_depth(nesting_depth)?;
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
        let ulen = len as usize;
        let ulen = Self::checked_count_with_limit(ulen as i32, self.remaining(), "string array")?;
        let mut arr: Vec<String> = Vec::new();
        Self::try_reserve_vec(&mut arr, ulen, "string array")?;
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
        let ulen = len as usize;
        let ulen = Self::checked_count_with_limit(ulen as i32, self.remaining(), "i32 array")?;
        //println!("read_i32_array: len={}", ulen);
        let mut arr: Vec<i32> = Vec::new();
        Self::try_reserve_vec(&mut arr, ulen, "i32 array")?;
        for _i in 0..len {
            arr.push(self.read_packed_i32()?);
        }
        Ok(arr)
    }

    pub fn read_map(&mut self) -> Result<MapValue, NoSQLError> {
        self.read_map_with_depth(1)
    }

    fn read_map_with_depth(&mut self, nesting_depth: usize) -> Result<MapValue, NoSQLError> {
        // number of bytes consumed by the map.
        let _num_bytes = self.read_i32()?;
        // number of items in the map
        let num_items = self.read_i32()?;
        let num_items = self.checked_count(num_items, "map message")?;
        // walk items
        //println!("read_map: num_items={}", num_items);
        let mut mv = MapValue::new();
        for _i in 0..num_items {
            let key = self.read_string()?;
            //println!("Reading field '{}'", key);
            let val = self.read_field_value_with_depth(nesting_depth)?;
            //println!("read key '{}' with value {:?}", key, val);
            mv.put_field_value(&key, val);
        }
        Ok(mv)
    }

    pub(crate) fn reset(&mut self) {
        self.offset = 0;
    }
}
