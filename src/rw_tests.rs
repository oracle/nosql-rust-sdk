//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::{reader::Reader, writer::Writer};
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
    writer.write_i32_at_offset(6543, offset1)?;
    writer.write_i32_at_offset(1111, offset2)?;

    let mut reader = Reader::new().from_bytes(writer.bytes());
    assert_eq!(reader.read_i32()?, 1234567);
    assert_eq!(reader.read_i16()?, 120);
    assert_eq!(reader.read_packed_i32()?, 545454);
    assert_eq!(reader.read_i32()?, 6543);
    assert_eq!(reader.read_packed_i64()?, 98765432198765);
    assert_eq!(reader.read_i32()?, 1);
    assert_eq!(reader.read_i16()?, 200);
    assert_eq!(reader.read_packed_i32()?, 222222);
    assert_eq!(reader.read_i32()?, 1111);
    assert_eq!(reader.read_packed_i32()?, 98765);
    Ok(())
}
