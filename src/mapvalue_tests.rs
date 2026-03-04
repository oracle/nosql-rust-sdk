//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::reader::Reader;
use crate::types;
use crate::types::FieldValue;
use crate::types::NoSQLColumnToFieldValue;
use crate::writer::Writer;
use std::error::Error;
use std::result::Result;

fn get_string(fv: &FieldValue) -> Option<&String> {
    if let FieldValue::String(s) = fv {
        Some(s)
    } else {
        None
    }
}
fn get_i64(fv: &FieldValue) -> Option<i64> {
    if let FieldValue::Long(i) = fv {
        Some(*i)
    } else {
        None
    }
}

#[test]
fn test_basic_mapvalue() -> Result<(), Box<dyn Error>> {
    let mut m = types::MapValue::new();
    m.put_i32("i32val", 5);
    m.put_bool("boolval", true);
    m.put_i64("i64val", 123456789);
    m.put_float64("floatval", 2345.0023456);
    m.put_str("strval", "This is a string value");
    let mut arr = Vec::<FieldValue>::new();
    arr.push("array element 1".to_field_value());
    arr.push("array element 2".to_field_value());
    arr.push("array element 3".to_field_value());
    arr.push(FieldValue::Long(12121212));
    m.put_array("arrval", arr);
    let vec: Vec<u8> = vec![0, 1, 2, 3, 4, 5];
    m.put_binary("binval", vec);
    assert_eq!(m.get_i32("i32val").ok_or("i32val doesn't exist")?, 5);
    assert_eq!(m.get_bool("boolval").ok_or("boolval doesn't exist")?, true);
    assert_eq!(
        m.get_float64("floatval").ok_or("floatval doesn't exist")?,
        2345.0023456
    );
    assert_eq!(
        m.get_i64("i64val").ok_or("i64val doesn't exist")?,
        123456789
    );
    assert_eq!(
        m.get_string("strval").ok_or("strval doesn't exist")?,
        "This is a string value"
    );
    let arr_result = m.get_array("arrval").ok_or("arrval doesn't exist")?;
    assert_eq!(arr_result.len(), 4);
    assert_eq!(
        get_string(&arr_result[0]).ok_or("arr[0] doesn't contain a string")?,
        "array element 1"
    );
    assert_eq!(
        get_string(&arr_result[1]).ok_or("arr[1] doesn't contain a string")?,
        "array element 2"
    );
    assert_eq!(
        get_string(&arr_result[2]).ok_or("arr[2] doesn't contain a string")?,
        "array element 3"
    );
    assert_eq!(
        get_i64(&arr_result[3]).ok_or("arr[3] doesn't contain a int64")?,
        12121212
    );
    let bin_result = m.get_binary("binval").ok_or("binval doesn't exist")?;
    assert_eq!(bin_result.len(), 6);

    let mut writer = Writer::new();
    writer.write_map(&m);
    println!("Map items={}, writer len={}", m.len(), writer.size());
    let mut reader = Reader::new().from_bytes(writer.bytes());
    let val = reader.read_map()?;
    assert_eq!(val.get_i32("i32val").ok_or("i32val doesn't exist")?, 5);
    assert_eq!(
        val.get_i64("i64val").ok_or("i64val doesn't exist")?,
        123456789
    );
    assert_eq!(
        val.get_string("strval").ok_or("strval doesn't exist")?,
        "This is a string value"
    );
    let arr_result = val.get_array("arrval").ok_or("arrval doesn't exist")?;
    assert_eq!(arr_result.len(), 4);
    assert_eq!(
        get_string(&arr_result[0]).ok_or("arr[0] doesn't contain a string")?,
        "array element 1"
    );
    assert_eq!(
        get_string(&arr_result[1]).ok_or("arr[1] doesn't contain a string")?,
        "array element 2"
    );
    assert_eq!(
        get_string(&arr_result[2]).ok_or("arr[2] doesn't contain a string")?,
        "array element 3"
    );
    assert_eq!(
        get_i64(&arr_result[3]).ok_or("arr[3] doesn't contain a int64")?,
        12121212
    );

    Ok(())
}
