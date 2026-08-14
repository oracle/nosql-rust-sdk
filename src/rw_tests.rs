//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::{
    error::NoSQLErrorCode::BadProtocolMessage,
    reader::{Reader, MAX_FIELD_VALUE_NESTING_DEPTH},
    types::FieldType,
    writer::Writer,
};
use std::error::Error;
use std::result::Result;

#[test]
fn test_int32_rw() -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::new();
    writer.write_packed_i32(1234567);
    writer.write_packed_i32(120);
    writer.write_packed_i32(119);
    writer.write_packed_i32(0);
    writer.write_packed_i32(-119);
    writer.write_packed_i32(-120);
    writer.write_packed_i32(i32::MAX);
    writer.write_packed_i32(i32::MIN);
    let mut reader = Reader::new().from_bytes(writer.bytes());
    assert_eq!(reader.read_packed_i32()?, 1234567);
    assert_eq!(reader.read_packed_i32()?, 120);
    assert_eq!(reader.read_packed_i32()?, 119);
    assert_eq!(reader.read_packed_i32()?, 0);
    assert_eq!(reader.read_packed_i32()?, -119);
    assert_eq!(reader.read_packed_i32()?, -120);
    assert_eq!(reader.read_packed_i32()?, i32::MAX);
    assert_eq!(reader.read_packed_i32()?, i32::MIN);
    Ok(())
}

#[test]
fn test_int64_rw() -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::new();
    writer.write_packed_i64(1234567);
    writer.write_packed_i64(120);
    writer.write_packed_i64(119);
    writer.write_packed_i64(0);
    writer.write_packed_i64(-119);
    writer.write_packed_i64(-120);
    writer.write_packed_i64(i64::MAX);
    writer.write_packed_i64(i64::MIN);
    let mut reader = Reader::new().from_bytes(writer.bytes());
    assert_eq!(reader.read_packed_i64()?, 1234567);
    assert_eq!(reader.read_packed_i64()?, 120);
    assert_eq!(reader.read_packed_i64()?, 119);
    assert_eq!(reader.read_packed_i64()?, 0);
    assert_eq!(reader.read_packed_i64()?, -119);
    assert_eq!(reader.read_packed_i64()?, -120);
    assert_eq!(reader.read_packed_i64()?, i64::MAX);
    assert_eq!(reader.read_packed_i64()?, i64::MIN);
    Ok(())
}

#[test]
fn test_mixed_rw() -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::new();
    writer.write_i32(1234567);
    writer.write_i16(120);
    writer.write_packed_i32(545454);
    writer.write_i16(3456);
    writer.write_string("This is a test string");
    writer.write_packed_i64(98765432198765);
    writer.write_i32(1);
    writer.write_float64(234.56789);
    writer.write_string("");
    writer.write_i16(200);
    writer.write_i16(i16::MIN);
    writer.write_float64(234214312.321312);
    writer.write_string("Another test");
    let mut reader = Reader::new().from_bytes(writer.bytes());
    assert_eq!(reader.read_i32()?, 1234567);
    assert_eq!(reader.read_i16()?, 120);
    assert_eq!(reader.read_packed_i32()?, 545454);
    assert_eq!(reader.read_i16()?, 3456);
    assert_eq!(reader.read_string()?, "This is a test string");
    assert_eq!(reader.read_packed_i64()?, 98765432198765);
    assert_eq!(reader.read_i32()?, 1);
    assert_eq!(reader.read_float64()?, 234.56789);
    assert_eq!(reader.read_string()?, "");
    assert_eq!(reader.read_i16()?, 200);
    assert_eq!(reader.read_i16()?, i16::MIN);
    assert_eq!(reader.read_float64()?, 234214312.321312);
    assert_eq!(reader.read_string()?, "Another test");
    Ok(())
}

#[test]
fn test_rw_with_offsets() -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::new();
    writer.write_i32(1234567);
    writer.write_i16(120);
    writer.write_packed_i32(545454);
    let offset1 = writer.size();
    writer.write_i32(3456);
    writer.write_packed_i64(98765432198765);
    writer.write_i32(1);
    writer.write_i16(200);
    writer.write_packed_i32(222222);
    let offset2 = writer.size();
    writer.write_i32(0);
    writer.write_packed_i32(98765);
    writer.write_i32_at_offset(0x1234567, offset1)?;
    writer.write_i32_at_offset(0x7f000001, offset2)?;

    let mut reader = Reader::new().from_bytes(writer.bytes());
    assert_eq!(reader.read_i32()?, 1234567);
    assert_eq!(reader.read_i16()?, 120);
    assert_eq!(reader.read_packed_i32()?, 545454);
    assert_eq!(reader.read_i32()?, 0x1234567);
    assert_eq!(reader.read_packed_i64()?, 98765432198765);
    assert_eq!(reader.read_i32()?, 1);
    assert_eq!(reader.read_i16()?, 200);
    assert_eq!(reader.read_packed_i32()?, 222222);
    assert_eq!(reader.read_i32()?, 0x7f000001);
    assert_eq!(reader.read_packed_i32()?, 98765);
    Ok(())
}

#[test]
fn test_truncated_packed_integers_return_errors() {
    let mut reader = Reader::new().from_bytes(&[0xf8]);
    assert!(reader.read_packed_i32().is_err());

    let mut reader = Reader::new().from_bytes(&[0xf8]);
    assert!(reader.read_packed_i64().is_err());

    let mut reader = Reader::new().from_bytes(&[0x03, 0, 0, 0, 0, 0]);
    assert!(reader.read_packed_i32().is_err());
}

#[test]
fn test_malformed_collection_counts_return_errors() {
    let mut reader = Reader::new().from_bytes(&[0, 0, 0, 0, 0, 0, 0, 1]);
    assert!(reader.read_array().is_err());

    let mut reader = Reader::new().from_bytes(&[0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff]);
    assert!(reader.read_map().is_err());

    let mut reader = Reader::new().from_bytes(&[0, 0, 0, 0, 0, 0, 0, 1]);
    assert!(reader.read_map().is_err());
}

fn nested_field_value_bytes(nesting_depth: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(nesting_depth * 10 + 1);
    for depth in 0..nesting_depth {
        let is_map = depth % 2 != 0;
        bytes.push(if is_map {
            FieldType::Map as u8
        } else {
            FieldType::Array as u8
        });
        bytes.extend_from_slice(&0_i32.to_be_bytes());
        bytes.extend_from_slice(&1_i32.to_be_bytes());
        if is_map {
            // A packed zero is an empty map key.
            bytes.push(127);
        }
    }
    bytes.push(FieldType::Null as u8);
    bytes
}

#[test]
fn test_field_value_nesting_at_limit_is_accepted() {
    let bytes = nested_field_value_bytes(MAX_FIELD_VALUE_NESTING_DEPTH);
    let mut reader = Reader::new().from_bytes(&bytes);

    assert!(reader.read_field_value().is_ok());
}

#[test]
fn test_field_value_nesting_over_limit_returns_error() {
    let bytes = nested_field_value_bytes(MAX_FIELD_VALUE_NESTING_DEPTH + 1);
    let mut reader = Reader::new().from_bytes(&bytes);

    let error = reader.read_field_value().unwrap_err();
    assert_eq!(error.code, BadProtocolMessage);
    assert!(error.message.contains("nesting depth exceeds limit"));
}
