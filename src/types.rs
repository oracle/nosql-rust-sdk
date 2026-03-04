//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use base64::prelude::{Engine as _, BASE64_STANDARD};
use bigdecimal::BigDecimal;
use bigdecimal::Num;
use chrono::{DateTime, FixedOffset};
use std::cmp::Ordering;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::result::Result;
use std::string::String;
use std::vec::Vec;

pub use oracle_nosql_rust_sdk_derive::*;

use num_enum::TryFromPrimitive;

use crate::error::ia_err;
use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode::BadProtocolMessage;
use crate::sort_iter::SortSpec;

type NoSQLDateTime = DateTime<FixedOffset>;

// Internal Oracle NoSQL database types used for field values.
#[derive(Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub(crate) enum FieldType {
    // Array represents the Array data type.
    // An array is an ordered collection of zero or more elements,
    // all elements of an array have the same type.
    Array = 0,

    // Binary represents the Binary data type.
    // A binary is an uninterpreted sequence of zero or more u8 bytes.
    Binary, // 1

    // Boolean data type has only two possible values: true and false.
    Boolean, // 2

    // Double data type represents the set of all IEEE-754 64-bit floating-point numbers.
    Double, // 3

    // Integer data type represents the set of all signed 32-bit integers (-2147483648 to 2147483647).
    Integer, // 4

    // Long data type represents the set of all signed 64-bit
    // integers (-9223372036854775808 to 9223372036854775807).
    Long, // 5

    // Map represents the Map data type.
    // A map is an unordered collection of zero or more key-value pairs,
    // where all keys are strings and all the values have the same type.
    Map, // 6

    // String represents the set of string values.
    String, // 7

    // Timestamp represents a point in time as a datetime with fixed offset timezone.
    Timestamp, // 8

    // Number represents arbitrary precision numbers.
    Number, // 9

    // JSONNull represents a special value that indicates the absence of
    // an actual value within a JSON data type.
    JsonNull, // 10

    // Null represents a special value that indicates the absence of
    // an actual value, or the fact that a value is unknown or inapplicable.
    Null, // 11

    // Empty represents the Empty data type.
    // It is used to describe the result of a query expression is empty.
    Empty, // 12
}

impl FieldType {
    pub(crate) fn try_from_u8(val: u8) -> Result<Self, NoSQLError> {
        match FieldType::try_from(val) {
            Ok(ft) => {
                return Ok(ft);
            }
            Err(_) => {
                return Err(NoSQLError::new(
                    BadProtocolMessage,
                    &format!("unrecognized field type {}", val),
                ));
            }
        }
    }
}

/// A specific struct to distingush between a contiguous array of bytes (Binary) versus
/// a database ARRAY of bytes (Array).
///
/// This is a simple wrapper struct around `Vec<u8>` so the `NoSQLColumnToFieldValue` and
/// `NoSQLColumnFromFieldValue` traits can correctly identify when a database field should
/// have binary data versus an array of bytes.
/// It is left as a pure public struct to allow direct setting and taking of the underlying
/// vector.
#[derive(Debug, Clone)]
pub struct NoSQLBinary {
    pub data: Vec<u8>,
}

/// The base struct for all data items in the Oracle NoSQL Database system.
///
/// Each data item is an instance of `FieldValue` allowing access to its type and its value as well as
/// additional utility methods that operate on `FieldValue`.
///
/// `FieldValue` instances are typed. The type system is similar to that of JSON with extensions. It is a
/// subset of the database types in Oracle NoSQL Database in that these objects do not inherently conform
/// to a fixed schema and some of the database types, such as RECORD and ENUM, require a schema. The
/// mappings of types is described below.
///
/// `FieldValue` instances used for put operations are not validated against the target table schema in
/// the Rust driver. Validation happens in the NoSQL server. If an instance does not match the target
/// table, an error is returned.
///
/// Returned `FieldValue` instances always conform to a table schema, or to the shape implied by a query projection.
///
/// `FieldValue` instances are created in several ways:
///
///  - From native Rust primitives or structs, using implementations of the [`NoSQLColumnToFieldValue`] trait.
///  - Inherently from [`MapValue::column()`] calls. This is the typical path when creating database
///    rows to be inserted into tables.
///  - Returned by operations on a table. These instances are created internally by operations that return
///    data and will have the schema implied by the table or query. They will typically be a [`MapValue`] that
///    maps string column names to `FieldValue` values.
///
/// `FieldValue` instances are not thread-safe. On input, they should not be reused until the operation that uses
/// them has returned.
// Note: do not derive Clone. Use clone_internal() when needed.
#[derive(Debug, Default)]
pub enum FieldValue {
    Array(Vec<FieldValue>),
    Binary(Vec<u8>),
    Boolean(bool),
    Double(f64),
    Integer(i32),
    Long(i64),
    Map(MapValue),
    String(String),
    Timestamp(DateTime<FixedOffset>),
    Number(BigDecimal),
    JsonNull,
    Null,
    Empty,
    #[default]
    Uninitialized,
}

impl Ord for FieldValue {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_field_values(self, other, false)
    }
}

impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for FieldValue {}

impl PartialOrd for FieldValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FieldValue {
    pub fn new() -> Self {
        FieldValue::Uninitialized
    }
    pub fn get_map_value(self) -> Result<MapValue, NoSQLError> {
        if let FieldValue::Map(mv) = self {
            return Ok(mv);
        }
        ia_err!("get_map_value: not a Map: {:?}", self)
    }
    pub fn get_map_value_ref(&self) -> Result<&MapValue, NoSQLError> {
        if let FieldValue::Map(mv) = self {
            return Ok(mv);
        }
        ia_err!("get_map_value_ref: not a Map: {:?}", self)
    }
    pub fn get_array_value(self) -> Result<Vec<FieldValue>, NoSQLError> {
        if let FieldValue::Array(av) = self {
            return Ok(av);
        }
        ia_err!("get_array_value: not an Array: {:?}", self)
    }
    pub fn get_array_value_ref(&self) -> Result<&Vec<FieldValue>, NoSQLError> {
        if let FieldValue::Array(av) = self {
            return Ok(av);
        }
        ia_err!("get_array_value: not an Array: {:?}", self)
    }
    pub fn is_atomic(&self) -> bool {
        match self {
            FieldValue::Array(_) => false,
            FieldValue::Map(_) => false,
            _ => true,
        }
    }
    pub(crate) fn get_type(&self) -> FieldType {
        match self {
            FieldValue::Array(_) => FieldType::Array,
            FieldValue::Map(_) => FieldType::Map,
            FieldValue::Integer(_) => FieldType::Integer,
            FieldValue::Long(_) => FieldType::Long,
            FieldValue::Number(_) => FieldType::Number,
            FieldValue::Double(_) => FieldType::Double,
            FieldValue::String(_) => FieldType::String,
            FieldValue::Boolean(_) => FieldType::Boolean,
            FieldValue::Binary(_) => FieldType::Binary,
            FieldValue::Timestamp(_) => FieldType::Timestamp,
            FieldValue::Empty => FieldType::Empty,
            FieldValue::JsonNull => FieldType::JsonNull,
            FieldValue::Null => FieldType::Null,
            FieldValue::Uninitialized => FieldType::Null,
        }
    }
    pub fn is_special(&self) -> bool {
        match self {
            FieldValue::Empty => true,
            FieldValue::JsonNull => true,
            FieldValue::Null => true,
            FieldValue::Uninitialized => true,
            _ => false,
        }
    }
    pub fn is_null(&self) -> bool {
        self.is_special()
    }
    pub fn is_numeric(&self) -> bool {
        match self {
            FieldValue::Integer(_) => true,
            FieldValue::Long(_) => true,
            FieldValue::Number(_) => true,
            FieldValue::Double(_) => true,
            _ => false,
        }
    }
    pub(crate) fn convert_empty_to_null(&mut self) {
        match self {
            FieldValue::Empty => {
                let _ = std::mem::replace(self, FieldValue::Null);
            }
            _ => (),
        }
    }
    // This exists because we want to be sure that FieldValues
    // never get cloned without us doing it specifically.
    pub(crate) fn clone_internal(&self) -> FieldValue {
        match self {
            FieldValue::Array(a) => {
                let mut v: Vec<FieldValue> = Vec::with_capacity(a.len());
                for i in a {
                    v.push(i.clone_internal());
                }
                return FieldValue::Array(v);
            }
            FieldValue::Map(m) => FieldValue::Map(m.clone_internal()),
            FieldValue::Integer(i) => FieldValue::Integer(i.clone()),
            FieldValue::Long(l) => FieldValue::Long(l.clone()),
            FieldValue::Number(n) => FieldValue::Number(n.clone()),
            FieldValue::Double(d) => FieldValue::Double(d.clone()),
            FieldValue::String(s) => FieldValue::String(s.clone()),
            FieldValue::Boolean(b) => FieldValue::Boolean(b.clone()),
            FieldValue::Binary(b) => FieldValue::Binary(b.clone()),
            FieldValue::Timestamp(t) => FieldValue::Timestamp(t.clone()),
            FieldValue::Empty => FieldValue::Empty,
            FieldValue::JsonNull => FieldValue::JsonNull,
            //FieldValue::JsonNull => FieldValue::Null,
            FieldValue::Null => FieldValue::Null,
            FieldValue::Uninitialized => FieldValue::Uninitialized,
        }
    }
    pub fn as_i32(&self) -> Result<i32, NoSQLError> {
        if let FieldValue::Integer(i) = self {
            return Ok(*i);
        }
        ia_err!("as_i32 called for {:?}", self)
    }
    pub fn as_i64(&self) -> Result<i64, NoSQLError> {
        match self {
            FieldValue::Integer(i) => {
                return Ok(*i as i64);
            }
            FieldValue::Long(l) => {
                return Ok(*l);
            }
            _ => {
                return ia_err!("as_i64 called for {:?}", self);
            }
        }
    }
    pub fn as_f64(&self) -> Result<f64, NoSQLError> {
        match self {
            FieldValue::Integer(i) => {
                return Ok(*i as f64);
            }
            FieldValue::Long(l) => {
                return Ok(*l as f64);
            }
            FieldValue::Double(d) => {
                return Ok(*d);
            }
            _ => {
                return ia_err!("as_f64 called for {:?}", self);
            }
        }
    }
    pub fn as_big_decimal(&self) -> Result<BigDecimal, NoSQLError> {
        match self {
            FieldValue::Integer(i) => {
                return Ok(bd_try_from_i32(*i)?);
            }
            FieldValue::Long(l) => {
                return Ok(bd_try_from_i64(*l)?);
            }
            FieldValue::Double(d) => {
                return Ok(bd_try_from_f64(*d)?);
            }
            FieldValue::Number(n) => {
                return Ok(n.clone());
            }
            FieldValue::String(s) => {
                return Ok(bd_try_from_str(s)?);
            }
            _ => {
                return ia_err!("as_big_decimal called for {:?}", self);
            }
        }
    }
}

pub(crate) fn bd_try_from_f64(val: f64) -> Result<BigDecimal, NoSQLError> {
    match BigDecimal::try_from(val) {
        Ok(bd) => {
            return Ok(bd);
        }
        Err(e) => {
            return ia_err!(
                "error converting f64({}) to BigDecimal: {}",
                val,
                e.to_string()
            );
        }
    }
}

pub(crate) fn bd_try_from_i32(val: i32) -> Result<BigDecimal, NoSQLError> {
    match BigDecimal::try_from(val) {
        Ok(bd) => {
            return Ok(bd);
        }
        Err(e) => {
            return ia_err!(
                "error converting i32({}) to BigDecimal: {}",
                val,
                e.to_string()
            );
        }
    }
}

pub(crate) fn bd_try_from_i64(val: i64) -> Result<BigDecimal, NoSQLError> {
    match BigDecimal::try_from(val) {
        Ok(bd) => {
            return Ok(bd);
        }
        Err(e) => {
            return ia_err!(
                "error converting i64({}) to BigDecimal: {}",
                val,
                e.to_string()
            );
        }
    }
}

pub(crate) fn bd_try_from_str(val: &str) -> Result<BigDecimal, NoSQLError> {
    match BigDecimal::from_str_radix(val, 10) {
        Ok(bd) => {
            return Ok(bd);
        }
        Err(e) => {
            return ia_err!(
                "error converting str({}) to BigDecimal: {}",
                val,
                e.to_string()
            );
        }
    }
}

pub trait NoSQLColumnToFieldValue {
    fn to_field_value(&self) -> FieldValue;
}

impl NoSQLColumnToFieldValue for FieldValue {
    fn to_field_value(&self) -> FieldValue {
        self.clone_internal()
    }
}
impl NoSQLColumnToFieldValue for f64 {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Double(*self)
    }
}
impl NoSQLColumnToFieldValue for i64 {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Long(*self)
    }
}
impl NoSQLColumnToFieldValue for i32 {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Integer(*self)
    }
}
impl NoSQLColumnToFieldValue for i8 {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Integer(*self as i32)
    }
}
impl NoSQLColumnToFieldValue for i16 {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Integer(*self as i32)
    }
}
impl NoSQLColumnToFieldValue for String {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::String(self.to_string())
    }
}
impl NoSQLColumnToFieldValue for &str {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::String(self.to_string())
    }
}
impl NoSQLColumnToFieldValue for bool {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Boolean(*self)
    }
}
impl NoSQLColumnToFieldValue for BigDecimal {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Number(self.clone())
    }
}
impl NoSQLColumnToFieldValue for NoSQLBinary {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Binary(self.data.to_vec())
    }
}
impl NoSQLColumnToFieldValue for MapValue {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Map(self.clone_internal())
    }
}
impl NoSQLColumnToFieldValue for DateTime<FixedOffset> {
    fn to_field_value(&self) -> FieldValue {
        FieldValue::Timestamp(self.clone())
    }
}

impl NoSQLColumnToFieldValue for serde_json::value::Value {
    fn to_field_value(&self) -> FieldValue {
        match self {
            serde_json::Value::Bool(b) => {
                return FieldValue::Boolean(*b);
            }
            serde_json::Value::String(s) => {
                return FieldValue::String(s.clone());
            }
            serde_json::Value::Null => {
                return FieldValue::JsonNull;
            }
            //serde_json::Value::Null => { return FieldValue::Null; },
            serde_json::Value::Number(n) => {
                if n.is_f64() {
                    return FieldValue::Double(n.as_f64().unwrap());
                } else if n.is_i64() {
                    let nv64 = n.as_i64().unwrap();
                    if let Ok(nv32) = i32::try_from(nv64) {
                        return FieldValue::Integer(nv32);
                    } else {
                        return FieldValue::Long(nv64);
                    }
                } else {
                    // try converting the string representation to a BigDecimal number
                    if let Ok(bd) = BigDecimal::from_str_radix(&n.to_string(), 10) {
                        return FieldValue::Number(bd);
                    }
                    // For now, just set a String
                    //println!(
                    //"WARN: Number value '{:?}' is not an f64 or i64 or BigDecimal",
                    //n
                    //);
                    return FieldValue::String(n.to_string());
                    //return ia_err!("number value '{:?}' is not an f64 or i64", n);
                }
            }
            serde_json::Value::Array(a) => {
                // a = Vec<Value>
                // TODO: there's a map | collect one-liner here...
                let mut arr: Vec<FieldValue> = Vec::new();
                for v in a {
                    arr.push(v.to_field_value());
                }
                return FieldValue::Array(arr);
            }
            serde_json::Value::Object(_) => {
                // m = serde::Map<String, serde::Value>
                return FieldValue::Map(MapValue::from_json_object(self).unwrap());
            }
        }
    }
}

impl<T: NoSQLColumnToFieldValue> NoSQLColumnToFieldValue for Option<T> {
    fn to_field_value(&self) -> FieldValue {
        if let Some(v) = self {
            v.to_field_value()
        } else {
            FieldValue::Null
        }
    }
}

impl<T: NoSQLColumnToFieldValue> NoSQLColumnToFieldValue for &T {
    fn to_field_value(&self) -> FieldValue {
        let v = *self;
        v.to_field_value()
    }
}

impl<T: NoSQLColumnToFieldValue> NoSQLColumnToFieldValue for Vec<T> {
    fn to_field_value(&self) -> FieldValue {
        let v: Vec<FieldValue> = self.iter().map(|i| i.to_field_value()).collect();
        FieldValue::Array(v)
    }
}

impl<T: NoSQLColumnToFieldValue> NoSQLColumnToFieldValue for HashMap<String, T> {
    fn to_field_value(&self) -> FieldValue {
        //let v: HashMap<String, FieldValue> = self.iter().map(|i| (i.0.to_string(), i.1.to_field_value())).collect();
        let mut m = MapValue::new();
        for (k, v) in self {
            m.put(k, v);
        }
        FieldValue::Map(m)
    }
}
impl<T: NoSQLColumnToFieldValue> NoSQLColumnToFieldValue for BTreeMap<String, T> {
    fn to_field_value(&self) -> FieldValue {
        //let v: HashMap<String, FieldValue> = self.iter().map(|i| (i.0.to_string(), i.1.to_field_value())).collect();
        let mut m = MapValue::new();
        for (k, v) in self {
            m.put(k, v);
        }
        FieldValue::Map(m)
    }
}

pub trait NoSQLColumnFromFieldValue {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError>
    where
        Self: Sized;
}

// a simple macro to make the following impls easier to read
macro_rules! ffv {
    ($f:expr, $a:path, $b:literal) => {{
        if let $a(v) = $f {
            return Ok(v.clone());
        }
        ia_err!(
            "NoSQL: wrong type for field: expected {}, actual: {:?}",
            $b,
            $f
        )
    }};
}

impl NoSQLColumnFromFieldValue for i32 {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        ffv! {fv, FieldValue::Integer, "Integer"}
    }
}
impl NoSQLColumnFromFieldValue for i64 {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        ffv! {fv, FieldValue::Long, "Long"}
    }
}
impl NoSQLColumnFromFieldValue for f64 {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        ffv! {fv, FieldValue::Double, "Double"}
    }
}
impl NoSQLColumnFromFieldValue for String {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        ffv! {fv, FieldValue::String, "String"}
    }
}
impl NoSQLColumnFromFieldValue for BigDecimal {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        ffv! {fv, FieldValue::Number, "Number"}
    }
}
impl NoSQLColumnFromFieldValue for NoSQLDateTime {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        if let FieldValue::Timestamp(v) = fv {
            return Ok(v.clone());
        }
        // allow string-->Timestamp if it fits with RFC3339
        if let FieldValue::String(s) = fv {
            return Ok(string_to_rfc3339(s)?);
        }
        ia_err!(
            "NoSQL: wrong type for field: expected FieldValue::Timestamp, actual: {:?}",
            fv
        )
    }
}
impl NoSQLColumnFromFieldValue for bool {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        ffv! {fv, FieldValue::Boolean, "Boolean"}
    }
}
impl NoSQLColumnFromFieldValue for NoSQLBinary {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        if let FieldValue::Binary(v) = fv {
            return Ok(NoSQLBinary { data: v.clone() });
        }
        ia_err!(
            "NoSQL: wrong type for field: expected Binary, actual: {:?}",
            fv
        )
    }
}
impl<T: NoSQLColumnFromFieldValue> NoSQLColumnFromFieldValue for Option<T> {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        match fv {
            FieldValue::Null => return Ok(None),
            FieldValue::JsonNull => return Ok(None),
            _ => (),
        }
        Ok(Some(T::from_field(fv)?))
    }
}
impl<T: NoSQLColumnFromFieldValue> NoSQLColumnFromFieldValue for Vec<T> {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        if let FieldValue::Array(v) = fv {
            let mut v1: Vec<T> = Vec::new();
            for i in v {
                v1.push(T::from_field(i)?);
            }
            return Ok(v1);
        }
        return ia_err!(
            "NoSQL: wrong type for field: expected Array, actual: {:?}",
            fv
        );
    }
}
impl<T: NoSQLColumnFromFieldValue> NoSQLColumnFromFieldValue for HashMap<String, T> {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        if let FieldValue::Map(v) = fv {
            // TODO: map....collect()?
            let mut m1: HashMap<String, T> = HashMap::new();
            for (s, v) in v.iter() {
                m1.insert(s.to_string(), T::from_field(v)?);
            }
            return Ok(m1);
        }
        return ia_err!(
            "NoSQL: wrong type for field: expected Map, actual: {:?}",
            fv
        );
    }
}
impl<T: NoSQLColumnFromFieldValue> NoSQLColumnFromFieldValue for BTreeMap<String, T> {
    fn from_field(fv: &FieldValue) -> Result<Self, NoSQLError> {
        if let FieldValue::Map(v) = fv {
            // TODO: map....collect()?
            let mut m1: BTreeMap<String, T> = BTreeMap::new();
            for (s, v) in v.iter() {
                m1.insert(s.to_string(), T::from_field(v)?);
            }
            return Ok(m1);
        }
        return ia_err!(
            "NoSQL: wrong type for field: expected Map, actual: {:?}",
            fv
        );
    }
}

pub trait NoSQLColumnFromMapValue {
    fn from_map(&self, key: &str, mv: &MapValue) -> Result<Self, NoSQLError>
    where
        Self: Sized;
}

const UNINITIALIZED_FIELD_VALUE: FieldValue = FieldValue::Uninitialized;

impl<T: NoSQLColumnFromFieldValue> NoSQLColumnFromMapValue for T {
    fn from_map(&self, key: &str, mv: &MapValue) -> Result<Self, NoSQLError> {
        if let Some(fv) = mv.get_field_value(key) {
            return T::from_field(fv);
        }
        T::from_field(&UNINITIALIZED_FIELD_VALUE)
    }
}

/// Struct representing a single row (record) in a NoSQL Database table.
///
/// This struct is basically a Map of `String` to [`FieldValue`]. It is the primary
/// struct for specifying the data in a single NoSQL table row.
///
// Note: do not derive Clone. Use clone_internal() when needed.
#[derive(Default, Debug)]
pub struct MapValue {
    pub(crate) m: BTreeMap<String, FieldValue>,
}

impl MapValue {
    pub fn new() -> Self {
        Default::default()
    }

    pub(crate) fn clone_internal(&self) -> MapValue {
        let mut m = BTreeMap::new();
        for i in &self.m {
            m.insert(i.0.to_string(), i.1.clone_internal());
        }
        MapValue { m: m }
    }

    pub fn from_json_map(
        json: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Self, NoSQLError> {
        let mut mv = MapValue::new();
        for (key, val) in json {
            if val.is_null() {
                mv.put_field_value(key, FieldValue::JsonNull);
            } else {
                mv.put(key, val);
            }
        }
        return Ok(mv);
    }

    pub fn from_json_object(json: &serde_json::value::Value) -> Result<Self, NoSQLError> {
        if let serde_json::Value::Object(o) = json {
            return Self::from_json_map(o);
        }
        ia_err!("from_json_object: json value is not an Object: {:#?}", json)
    }

    pub fn len(&self) -> usize {
        self.m.len()
    }

    pub fn iter(&self) -> Iter<String, FieldValue> {
        self.m.iter()
    }

    pub fn put(&mut self, key: &str, val: impl NoSQLColumnToFieldValue) {
        self.m.insert(key.to_string(), val.to_field_value());
    }

    pub fn column(mut self, key: &str, val: impl NoSQLColumnToFieldValue) -> MapValue {
        self.m.insert(key.to_string(), val.to_field_value());
        self
    }

    pub fn put_i32(&mut self, key: &str, val: i32) {
        self.m.insert(key.to_string(), FieldValue::Integer(val));
    }
    pub fn i32(mut self, key: &str, val: i32) -> MapValue {
        self.put_i32(key, val);
        self
    }
    pub fn get_i32(&self, key: &str) -> Option<i32> {
        if let FieldValue::Integer(i) = self.m.get(key)? {
            Some(*i)
        } else {
            None
        }
    }

    pub fn put_i64(&mut self, key: &str, val: i64) {
        self.m.insert(key.to_string(), FieldValue::Long(val));
    }
    pub fn i64(mut self, key: &str, val: i64) -> MapValue {
        self.put_i64(key, val);
        self
    }
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        if let FieldValue::Long(i) = self.m.get(key)? {
            Some(*i)
        } else {
            None
        }
    }

    pub fn put_float64(&mut self, key: &str, val: f64) {
        self.m.insert(key.to_string(), FieldValue::Double(val));
    }
    pub fn get_float64(&self, key: &str) -> Option<f64> {
        if let FieldValue::Double(i) = self.m.get(key)? {
            Some(*i)
        } else {
            None
        }
    }

    pub fn put_str(&mut self, key: &str, val: &str) {
        self.put_string(key, val.to_string())
    }
    pub fn str(mut self, key: &str, val: &str) -> MapValue {
        self.put_str(key, val);
        self
    }
    pub fn put_string(&mut self, key: &str, val: String) {
        self.m.insert(key.to_string(), FieldValue::String(val));
    }
    pub fn string(mut self, key: &str, val: String) -> MapValue {
        self.put_string(key, val);
        self
    }
    pub fn get_string(&self, key: &str) -> Option<String> {
        if let FieldValue::String(s) = self.m.get(key)? {
            Some(s.clone())
        } else {
            None
        }
    }

    pub fn put_timestamp(&mut self, key: &str, val: &DateTime<FixedOffset>) {
        self.m
            .insert(key.to_string(), FieldValue::Timestamp(val.clone()));
    }
    pub fn timestamp(mut self, key: &str, val: &DateTime<FixedOffset>) -> MapValue {
        self.put_timestamp(key, val);
        self
    }
    pub fn get_timestamp(&self, key: &str) -> Option<DateTime<FixedOffset>> {
        if let FieldValue::Timestamp(t) = self.m.get(key)? {
            Some(t.clone())
        } else {
            None
        }
    }

    pub fn put_bool(&mut self, key: &str, val: bool) {
        self.m.insert(key.to_string(), FieldValue::Boolean(val));
    }
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        if let FieldValue::Boolean(b) = self.m.get(key)? {
            Some(*b)
        } else {
            None
        }
    }

    pub fn put_binary(&mut self, key: &str, val: Vec<u8>) {
        self.m.insert(key.to_string(), FieldValue::Binary(val));
    }
    pub fn get_binary(&self, key: &str) -> Option<&Vec<u8>> {
        if let FieldValue::Binary(b) = self.m.get(key)? {
            Some(b)
        } else {
            None
        }
    }

    pub fn put_array(&mut self, key: &str, val: Vec<FieldValue>) {
        self.m.insert(key.to_string(), FieldValue::Array(val));
    }
    pub fn get_array(&self, key: &str) -> Option<&Vec<FieldValue>> {
        if let FieldValue::Array(a) = self.m.get(key)? {
            Some(a)
        } else {
            None
        }
    }

    pub fn put_field_value(&mut self, key: &str, val: FieldValue) {
        self.m.insert(key.to_string(), val);
    }
    pub fn get_field_value(&self, key: &str) -> Option<&FieldValue> {
        Some(self.m.get(key)?)
    }
    pub fn get_field_value_clone(&self, key: &str) -> Option<FieldValue> {
        Some(self.m.get(key)?.clone_internal())
    }
    pub fn take_field_value(&mut self, key: &str) -> Result<FieldValue, NoSQLError> {
        if let Some(v) = self.m.remove(key) {
            return Ok(v);
        }
        ia_err!("field '{}' does not exist in map", key)
    }

    pub fn get_map(&self, key: &str) -> Option<&MapValue> {
        if let FieldValue::Map(a) = self.m.get(key)? {
            Some(a)
        } else {
            None
        }
    }

    pub fn put_json_map_value(&mut self, key: &str, json: &str) -> Result<(), NoSQLError> {
        let vr: Result<serde_json::Value, serde_json::Error> = serde_json::from_str(json);
        match vr {
            Ok(v) => {
                let mv = MapValue::from_json_object(&v)?;
                self.put_field_value(key, FieldValue::Map(mv));
                return Ok(());
            }
            Err(e) => {
                return ia_err!("error parsing json into MapValue: {}", e.to_string());
            }
        }
    }

    pub(crate) fn convert_empty_to_null(&mut self) {
        for (_k, v) in self.m.iter_mut() {
            v.convert_empty_to_null();
        }
    }
}

impl Ord for MapValue {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_map_values_sortspec(self, other, &SortSpec::default(), false)
    }
}

impl PartialEq for MapValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for MapValue {}

impl PartialOrd for MapValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for MapValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

/// Trait that defines methods to convert to and from a NoSQL [`MapValue`].
///
/// It is typically not necessary to implement this trait for structs. Instead,
/// use the [`derive@NoSQLRow`] derive macro to automatically
/// have a struct implement this trait.
pub trait NoSQLRow {
    /// Create a new [`MapValue`] based on the contents of `Self`.
    fn to_map_value(&self) -> Result<MapValue, NoSQLError>;
    /// Populate `Self` with the contents of a given [`MapValue`].
    fn from_map_value(&mut self, value: &MapValue) -> Result<(), NoSQLError>;
}

/// Consistency is used to provide consistency guarantees for read operations.
///
/// There are two consistency values available: Eventual and Absolute.
///
/// 1. Eventual consistency means that the values read may be very slightly out of date. This is the default.
///
/// 2. Absolute consistency may be specified to guarantee that current values are read.
///
/// Absolute consistency results in higher cost, consuming twice the number of
/// read units for the same data relative to Eventual consistency, and should
/// only be used when required.
///
/// Consistency can be specified as an optional argument to most read operations.
///
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Consistency {
    // Absolute consistency.
    Absolute = 1,
    // Eventual consistency.
    #[default]
    Eventual = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
#[allow(dead_code)]
pub(crate) enum OpCode {
    // Delete is used for the operation that deletes a row from table.
    Delete = 0,

    // DeleteIfVersion is used for the operation that deletes a row from table
    // if the row matches the specified version.
    DeleteIfVersion, // 1

    // Get is used for the operation that retrieves a row from table.
    Get, // 2

    // Put is used for the operation that unconditionally puts a row to table.
    Put, // 3

    // PutIfAbsent is used for the operation that puts a row to table if the row
    // is absent.
    PutIfAbsent, // 4

    // PutIfPresent is used for the operation that puts a row to table if the row
    // is present.
    PutIfPresent, // 5

    // PutIfVersion is used for the operation that puts a row to table if the row
    // matches the specified version.
    PutIfVersion, // 6

    // Query is used for the query operation.
    // A query operation can perform select, insert, update and delete operations
    // over an SQL statement.
    Query, // 7

    // Prepare is used for the operation that compiles/prepares an SQL statement
    // before execution.
    Prepare, // 8

    // WriteMultiple is used to perform multiple write operations associated
    // with a table in a single transaction.
    WriteMultiple, // 9

    // MultiDelete is used for the operation that deletes multiple rows from a
    // table in a single transaction.
    MultiDelete, // 10

    // GetTable is used for the operation that retrieves static information about a table.
    GetTable, // 11

    // GetIndexes is used for the operation that retrieves information about an index.
    GetIndexes, // 12

    // GetTableUsage is used for the operation that retrieves usage information on a table.
    GetTableUsage, // 13

    // ListTables is used for the operation that lists all available table names.
    ListTables, // 14

    // TableRequest is used for the operation that manages table schema or
    // changes table limits.
    TableRequest, // 15

    // Scan is reserved for internal use.
    Scan, // 16

    // IndexScan is reserved for internal use.
    IndexScan, // 17

    // CreateTable represents the operation that creates a table.
    CreateTable, // 18

    // AlterTable represents the operation that modifies the table schema.
    AlterTable, // 19

    // DropTable represents the operation that drops a table.
    DropTable, // 20

    // CreateIndex represents the operation that creates an index on a table.
    CreateIndex, // 21

    // SystemRequest is used to perform system operations such as
    // administrative operations that do not affect a specific table.
    SystemRequest, // 23

    // SystemStatusRequest is used to retrieve the operation status of a SystemRequest.
    SystemStatusRequest, // 24
}

// Capacity represents the read/write throughput consumed by an operation.
#[derive(Clone, Copy, Default, Debug)]
pub struct Capacity {
    // read_kb represents the number of kilobytes consumed for reads.
    pub read_kb: i32,

    // write_kb represents the number of kilobytes consumed for writes.
    pub write_kb: i32,

    // read_units represents the number of read units consumed for reads.
    //
    // A read unit represents 1 eventually consistent read per second for data
    // up to 1 KB in size. A read that is absolutely consistent is double that,
    // consuming 2 read units for a read of up to 1 KB in size.
    pub read_units: i32,
}

impl Capacity {
    pub(crate) fn add(&mut self, c: &Capacity) {
        self.read_kb += c.read_kb;
        self.read_units += c.read_units;
        self.write_kb += c.write_kb;
    }
}

#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum TableState {
    // The table is ready to be used. This is the steady state after
    // creation or modification.
    #[default]
    Active = 0,
    // The table is being created and cannot yet be used
    Creating = 1,
    // The table has been dropped or does not exist
    Dropped = 2,
    // The table is being dropped and cannot be used
    Dropping = 3,
    // The table is being updated. It is available for normal use, but
    // additional table modification operations are not permitted
    // while the table is in this state.
    Updating = 4,
}

impl TableState {
    pub fn from_int(icode: i32) -> Result<TableState, NoSQLError> {
        match icode {
            0 => return Ok(TableState::Active),
            1 => return Ok(TableState::Creating),
            2 => return Ok(TableState::Dropped),
            3 => return Ok(TableState::Dropping),
            4 => return Ok(TableState::Updating),
            _ => {
                return Err(NoSQLError::new(
                    BadProtocolMessage,
                    format!("Invalid TableState {} in result response", icode).as_str(),
                ));
            }
        }
    }
}

#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum OperationState {
    // The table is ready to be used. This is the steady state after
    // creation or modification.
    #[default]
    Complete = 0,
    // The table is being created and cannot yet be used
    Working = 1,
}

impl OperationState {
    pub fn from_int(icode: i32) -> Result<OperationState, NoSQLError> {
        match icode {
            0 => return Ok(OperationState::Complete),
            1 => return Ok(OperationState::Working),
            _ => {
                return Err(NoSQLError::new(
                    BadProtocolMessage,
                    "Invalid OperationState in result response",
                ));
            }
        }
    }
}

#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
pub enum CapacityMode {
    // Provisioned
    #[default]
    Provisioned = 1,
    // On-demand
    OnDemand = 2,
}

#[derive(Default, Debug, Clone)]
pub struct TableLimits {
    pub read_units: i32,
    pub write_units: i32,
    pub storage_gb: i32,
    pub mode: CapacityMode,
}

impl TableLimits {
    // Create a new TableLimits with Provisioned values
    pub fn provisioned(read_units: i32, write_units: i32, storage_gb: i32) -> TableLimits {
        TableLimits {
            read_units,
            write_units,
            storage_gb,
            mode: CapacityMode::Provisioned,
        }
    }

    // create a new TableLimits with on-demand values
    pub fn on_demand(storage_gb: i32) -> TableLimits {
        TableLimits {
            read_units: 0,
            write_units: 0,
            storage_gb,
            mode: CapacityMode::OnDemand,
        }
    }
}

pub(crate) fn string_to_rfc3339(s: &str) -> Result<DateTime<FixedOffset>, NoSQLError> {
    // try as-is
    let ret = DateTime::parse_from_rfc3339(s);
    match ret {
        Ok(dt) => return Ok(dt),
        _ => {}
    }
    //println!("Adding Z to end of \"{}\"", s);
    // if that didn't work, try adding a "Z" to the end
    let mut ds = std::string::String::from(s);
    ds.push('Z');
    let ret1 = DateTime::parse_from_rfc3339(&ds);
    match ret1 {
        Ok(dt) => return Ok(dt),
        Err(_) => {
            return ia_err!("error parsing string '{}' into RFC3339 timestamp", s);
        }
    }
}

// TopologyInfo represents the NoSQL database topology information required for
// query execution.
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct TopologyInfo {
    // seq_num represents the sequence number of the topology.
    pub seq_num: i32,

    // shard_ids specifies an array of int values that represent the shard IDs.
    pub shard_ids: Vec<i32>,
}

impl TopologyInfo {
    pub(crate) fn is_valid(&self) -> bool {
        self.seq_num >= 0 && self.shard_ids.len() > 0
    }
    // TODO: in go, equals() sorts the arrays of shard IDs... Hmmm.
    // Sort the slice of shard IDs and compare.
    //sort.Ints(ti.shardIDs)
    //sort.Ints(otherTopo.shardIDs)
    //return reflect.DeepEqual(ti.shardIDs, otherTopo.shardIDs)
}

impl Default for TopologyInfo {
    fn default() -> Self {
        Self {
            seq_num: -1,
            shard_ids: Vec::default()
        }
    }
}

pub(crate) fn compare_results(
    r1: &MapValue,
    r2: &MapValue,
    sort_fields: &Vec<String>,
    sort_specs: &Vec<SortSpec>,
) -> Ordering {
    for i in 0..sort_fields.len() {
        let ov1 = r1.get_field_value(&sort_fields[i]);
        let ov2 = r2.get_field_value(&sort_fields[i]);
        if ov1.is_none() && ov2.is_none() {
            continue;
        }
        if ov1.is_none() {
            return Ordering::Less;
        }
        if ov2.is_none() {
            return Ordering::Greater;
        }
        let comp =
            compare_atomics_total_order_sortspec(ov1.unwrap(), ov2.unwrap(), &sort_specs[i], false);
        if comp != Ordering::Equal {
            return comp;
        }
    }
    return Ordering::Equal;
}

// Implements a total order among atomic values. The following order is
// used among values that are not normally comparable with each other:
//
// numerics < timestamps < strings < booleans < binaries < empty < json null < null
pub(crate) fn compare_atomics_total_order(
    v0: &FieldValue,
    v1: &FieldValue,
    nulls_equal: bool,
) -> Ordering {
    let tc0 = v0.get_type();
    let tc1 = v1.get_type();

    // This is typically only set in QTF tests, where null/jsonnull/empty compare to equal
    if nulls_equal {
        if tc0 == FieldType::Null || tc0 == FieldType::JsonNull || tc0 == FieldType::Empty {
            if tc1 == FieldType::Null || tc1 == FieldType::JsonNull || tc1 == FieldType::Empty {
                return Ordering::Equal;
            }
        }
    }

    match tc0 {
        FieldType::Array => {
            panic!("Cannot compare atomics on Array type");
        }
        FieldType::Map => {
            panic!("Cannot compare atomics on Map type");
        }
        FieldType::Null => {
            if tc1 == FieldType::Null {
                return Ordering::Equal;
            }
            return Ordering::Greater;
        }
        FieldType::JsonNull => {
            if tc1 == FieldType::Null {
                return Ordering::Less;
            }
            if tc1 == FieldType::JsonNull {
                return Ordering::Equal;
            }
            return Ordering::Greater;
        }
        FieldType::Empty => {
            if tc1 == FieldType::Null {
                return Ordering::Less;
            }
            if tc1 == FieldType::JsonNull {
                return Ordering::Less;
            }
            if tc1 == FieldType::Empty {
                return Ordering::Equal;
            }
            return Ordering::Greater;
        }
        FieldType::Integer => {
            let iv0 = i32::from_field(v0).unwrap();
            match tc1 {
                FieldType::Integer => {
                    let iv1 = i32::from_field(v1).unwrap();
                    return iv0.cmp(&iv1);
                }
                FieldType::Long => {
                    let iv1 = i64::from_field(v1).unwrap();
                    let tmp0 = iv0 as i64;
                    return tmp0.cmp(&iv1);
                }
                FieldType::Double => {
                    let iv1 = f64::from_field(v1).unwrap();
                    let v = iv0 as f64;
                    return compare_floats(&v, &iv1);
                }
                FieldType::Number => {
                    let bd1 = BigDecimal::from_field(v1).unwrap();
                    let bd0 = BigDecimal::default() + iv0;
                    return bd0.cmp(&bd1);
                }
                _ => {
                    return Ordering::Less;
                }
            }
        }
        FieldType::Long => {
            let iv0 = i64::from_field(v0).unwrap();
            match tc1 {
                FieldType::Integer => {
                    let iv1 = i32::from_field(v1).unwrap() as i64;
                    return iv0.cmp(&iv1);
                }
                FieldType::Long => {
                    let iv1 = i64::from_field(v1).unwrap();
                    return iv0.cmp(&iv1);
                }
                FieldType::Double => {
                    let iv1 = f64::from_field(v1).unwrap();
                    let v = iv0 as f64;
                    return compare_floats(&v, &iv1);
                }
                FieldType::Number => {
                    let bd1 = BigDecimal::from_field(v1).unwrap();
                    let bd0 = BigDecimal::default() + iv0;
                    return bd0.cmp(&bd1);
                }
                _ => {
                    return Ordering::Less;
                }
            }
        }
        FieldType::Double => {
            let fv0 = f64::from_field(v0).unwrap();
            match tc1 {
                FieldType::Integer => {
                    let fv1 = i32::from_field(v1).unwrap() as f64;
                    return compare_floats(&fv0, &fv1);
                }
                FieldType::Long => {
                    let fv1 = i64::from_field(v1).unwrap() as f64;
                    return compare_floats(&fv0, &fv1);
                }
                FieldType::Double => {
                    let fv1 = f64::from_field(v1).unwrap();
                    return compare_floats(&fv0, &fv1);
                }
                FieldType::Number => {
                    let bd1 = BigDecimal::from_field(v1).unwrap();
                    let bd0 = BigDecimal::try_from(fv0).unwrap();
                    return bd0.cmp(&bd1);
                }
                _ => {
                    return Ordering::Less;
                }
            }
        }
        FieldType::Number => {
            let bd0 = BigDecimal::from_field(v0).unwrap();
            match tc1 {
                FieldType::Integer => {
                    let iv1 = i32::from_field(v1).unwrap();
                    let bd1 = BigDecimal::default() + iv1;
                    return bd0.cmp(&bd1);
                }
                FieldType::Long => {
                    let iv1 = i64::from_field(v1).unwrap();
                    let bd1 = BigDecimal::default() + iv1;
                    return bd0.cmp(&bd1);
                }
                FieldType::Double => {
                    let fv1 = f64::from_field(v1).unwrap();
                    let bd1 = BigDecimal::try_from(fv1).unwrap();
                    return bd0.cmp(&bd1);
                }
                FieldType::Number => {
                    let bd1 = BigDecimal::from_field(v1).unwrap();
                    return bd0.cmp(&bd1);
                }
                _ => {
                    return Ordering::Less;
                }
            }
        }
        FieldType::Timestamp => {
            let tv0 = NoSQLDateTime::from_field(v0).unwrap();
            match tc1 {
                FieldType::Timestamp => {
                    let tv1 = NoSQLDateTime::from_field(v1).unwrap();
                    return tv0.cmp(&tv1);
                }
                FieldType::Integer => {
                    return Ordering::Greater;
                }
                FieldType::Long => {
                    return Ordering::Greater;
                }
                FieldType::Double => {
                    return Ordering::Greater;
                }
                FieldType::Number => {
                    return Ordering::Greater;
                }
                FieldType::String => {
                    if nulls_equal == false {
                        return Ordering::Less;
                    }
                    if let Ok(tv1) = NoSQLDateTime::from_field(v1) {
                        return tv0.cmp(&tv1);
                    }
                    return Ordering::Less;
                }
                _ => return Ordering::Less,
            }
        }
        FieldType::String => {
            let sv0 = String::from_field(v0).unwrap();
            match tc1 {
                FieldType::String => {
                    let sv1 = String::from_field(v1).unwrap();
                    return sv0.cmp(&sv1);
                }
                FieldType::Integer => {
                    return Ordering::Greater;
                }
                FieldType::Long => {
                    return Ordering::Greater;
                }
                FieldType::Double => {
                    return Ordering::Greater;
                }
                FieldType::Number => {
                    return Ordering::Greater;
                }
                FieldType::Timestamp => {
                    if nulls_equal == false {
                        return Ordering::Greater;
                    }
                    let tv1 = NoSQLDateTime::from_field(v1).unwrap();
                    if let Ok(tv0) = NoSQLDateTime::from_field(v0) {
                        return tv0.cmp(&tv1);
                    }
                    return Ordering::Greater;
                }
                FieldType::Binary => {
                    if nulls_equal == false {
                        return Ordering::Less;
                    }
                    let bv1 = NoSQLBinary::from_field(v1).unwrap();
                    let sv1 = BASE64_STANDARD.encode(bv1.data);
                    return sv0.cmp(&sv1);
                }
                _ => return Ordering::Less,
            }
        }
        FieldType::Boolean => {
            let bv0 = bool::from_field(v0).unwrap();
            match tc1 {
                FieldType::Boolean => {
                    let bv1 = bool::from_field(v1).unwrap();
                    return bv0.cmp(&bv1);
                }
                FieldType::Integer => {
                    return Ordering::Greater;
                }
                FieldType::Long => {
                    return Ordering::Greater;
                }
                FieldType::Double => {
                    return Ordering::Greater;
                }
                FieldType::Number => {
                    return Ordering::Greater;
                }
                FieldType::Timestamp => {
                    return Ordering::Greater;
                }
                FieldType::String => {
                    return Ordering::Greater;
                }
                _ => return Ordering::Less,
            }
        }
        FieldType::Binary => {
            let bv0 = NoSQLBinary::from_field(v0).unwrap();
            match tc1 {
                FieldType::Binary => {
                    let bv1 = NoSQLBinary::from_field(v1).unwrap();
                    return bv0.data.cmp(&bv1.data);
                }
                FieldType::Integer => {
                    return Ordering::Greater;
                }
                FieldType::Long => {
                    return Ordering::Greater;
                }
                FieldType::Double => {
                    return Ordering::Greater;
                }
                FieldType::Number => {
                    return Ordering::Greater;
                }
                FieldType::Timestamp => {
                    return Ordering::Greater;
                }
                FieldType::String => {
                    if nulls_equal == false {
                        return Ordering::Greater;
                    }
                    let sv1 = String::from_field(v1).unwrap();
                    let sv0 = BASE64_STANDARD.encode(bv0.data);
                    return sv0.cmp(&sv1);
                }
                FieldType::Boolean => {
                    return Ordering::Greater;
                }
                _ => return Ordering::Less,
            }
        }
    }
}

fn compare_atomics_total_order_sortspec(
    v1: &FieldValue,
    v2: &FieldValue,
    ss: &SortSpec,
    nulls_equal: bool,
) -> Ordering {
    let mut comp = compare_atomics_total_order(v1, v2, nulls_equal);

    if ss.is_desc {
        comp = comp.reverse();
    }

    if ss.is_desc == false && ss.nulls_first == true {
        if v1.is_special() && !v2.is_special() {
            comp = Ordering::Less;
        }
        if !v1.is_special() && v2.is_special() {
            comp = Ordering::Greater;
        }
    } else if ss.is_desc == true && ss.nulls_first == false {
        if v1.is_special() && !v2.is_special() {
            comp = Ordering::Greater;
        }
        if !v1.is_special() && v2.is_special() {
            comp = Ordering::Less;
        }
    }
    comp
}

pub(crate) fn compare_field_values(
    v1: &FieldValue,
    v2: &FieldValue,
    nulls_equal: bool,
) -> Ordering {
    let ss = SortSpec::default();
    compare_total_order(v1, v2, &ss, nulls_equal)
}

// Implements a total order among all kinds of values
pub(crate) fn compare_total_order(
    v1: &FieldValue,
    v2: &FieldValue,
    ss: &SortSpec,
    nulls_equal: bool,
) -> Ordering {
    let tc1 = v1.get_type();
    let tc2 = v2.get_type();

    match tc1 {
        FieldType::Map => match tc2 {
            FieldType::Map => {
                return compare_maps(v1, v2, ss, nulls_equal);
            }
            FieldType::Array => {
                return modify_order(Ordering::Less, ss);
            }
            _ => {
                return modify_order(Ordering::Greater, ss);
            }
        },
        FieldType::Array => match tc2 {
            FieldType::Map => {
                return modify_order(Ordering::Greater, ss);
            }
            FieldType::Array => {
                return compare_arrays(v1, v2, ss, nulls_equal);
            }
            _ => {
                return modify_order(Ordering::Greater, ss);
            }
        },
        _ => match tc2 {
            FieldType::Map | FieldType::Array => {
                return modify_order(Ordering::Less, ss);
            }
            _ => {
                return compare_atomics_total_order_sortspec(v1, v2, ss, nulls_equal);
            }
        },
    }
}

fn compare_maps(v1: &FieldValue, v2: &FieldValue, ss: &SortSpec, nulls_equal: bool) -> Ordering {
    let mv1 = v1.get_map_value_ref().unwrap();
    let mv2 = v2.get_map_value_ref().unwrap();
    compare_map_values_sortspec(mv1, mv2, ss, nulls_equal)
}

pub(crate) fn compare_map_values_sortspec(
    mv1: &MapValue,
    mv2: &MapValue,
    ss: &SortSpec,
    nulls_equal: bool,
) -> Ordering {
    let inner_ss = SortSpec::default();

    // iterate through map keys in sorted order
    // map is a btree, so keys are already sorted
    let sorted_keys1 = mv1.m.keys();
    let sk1len = sorted_keys1.len();

    let mut sorted_keys2 = mv2.m.keys();
    let sk2len = sorted_keys2.len();

    for k1 in sorted_keys1 {
        let ok2 = sorted_keys2.next();
        if ok2.is_none() {
            break;
        }
        let k2 = ok2.unwrap();
        let ord = k1.cmp(k2);
        if ord != Ordering::Equal {
            return modify_order(ord, ss);
        }

        let fv1 = mv1.m.get(k1).unwrap();
        let fv2 = mv2.m.get(k1).unwrap();
        let comp = compare_total_order(fv1, fv2, &inner_ss, nulls_equal);
        if comp != Ordering::Equal {
            if nulls_equal {
                //println!("Diff key={}\nv1={:?}\nv2={:?}", k, fv1, fv2);
            }
            return modify_order(comp, ss);
        }
    }
    if sk1len == sk2len {
        return Ordering::Equal;
    }
    if sk1len < sk2len {
        return modify_order(Ordering::Less, ss);
    }
    return modify_order(Ordering::Greater, ss);
}

fn modify_order(o: Ordering, ss: &SortSpec) -> Ordering {
    if ss.is_desc == false {
        return o;
    }
    o.reverse()
}

fn compare_arrays(v1: &FieldValue, v2: &FieldValue, ss: &SortSpec, nulls_equal: bool) -> Ordering {
    let inner_ss = SortSpec::default();
    let av1 = v1.get_array_value_ref().unwrap();
    let av2 = v2.get_array_value_ref().unwrap();

    let mut min = av1.len();
    if av2.len() < min {
        min = av2.len();
    }
    for i in 0..min {
        let comp = compare_total_order(&av1[i], &av2[i], &inner_ss, nulls_equal);
        if comp != Ordering::Equal {
            return modify_order(comp, ss);
        }
    }
    if av1.len() == av2.len() {
        return Ordering::Equal;
    }
    if av2.len() > av1.len() {
        return modify_order(Ordering::Greater, ss);
    }
    modify_order(Ordering::Less, ss)
}

fn compare_floats(v0: &f64, v1: &f64) -> Ordering {
    v0.total_cmp(v1)
}
