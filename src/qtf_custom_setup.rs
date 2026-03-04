//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
#![allow(dead_code)]

use std::collections::HashMap;

use crate::qtf::Random;
use crate::types::MapValue;
use serde_json::value::{Number, Value};

// SetupQTFSuite represents an interface that wraps the functions used to setup
// and teardown test assets for QTF test suite.
#[derive(Debug, Clone)]
pub(crate) enum SetupQTFSuite {
    Setup(PrimIndexSetup),
    Setup2(PrimIndexSetup2),
    Setup3(PrimIndexSetup3),
    Table(UserTable),
    Data1(Data1Setup),
    Data2(Data2Setup),
}

impl SetupQTFSuite {
    // before_ddls returns the DDL statements that need to execute before tests.
    pub fn before_ddls(&self) -> Vec<String> {
        match self {
            SetupQTFSuite::Setup(s) => s.before_ddls(),
            SetupQTFSuite::Setup2(s) => s.before_ddls(),
            SetupQTFSuite::Setup3(s) => s.before_ddls(),
            SetupQTFSuite::Table(s) => s.before_ddls(),
            SetupQTFSuite::Data1(s) => s.before_ddls(),
            SetupQTFSuite::Data2(s) => s.before_ddls(),
        }
    }

    // after_ddls returns the DDL statements that need to execute after tests.
    pub fn after_ddls(&self) -> Vec<String> {
        match self {
            SetupQTFSuite::Setup(s) => s.after_ddls(),
            SetupQTFSuite::Setup2(s) => s.after_ddls(),
            SetupQTFSuite::Setup3(s) => s.after_ddls(),
            SetupQTFSuite::Table(s) => s.after_ddls(),
            SetupQTFSuite::Data1(s) => s.after_ddls(),
            SetupQTFSuite::Data2(s) => s.after_ddls(),
        }
    }

    // before_data returns the data for each table that need to insert before tests.
    // This function returns a map whose key is table name and the associated
    // value which is a vector of MapValue.
    pub fn before_data(&self) -> HashMap<String, Vec<MapValue>> {
        match self {
            SetupQTFSuite::Setup(s) => s.before_data(),
            SetupQTFSuite::Setup2(s) => s.before_data(),
            SetupQTFSuite::Setup3(s) => s.before_data(),
            SetupQTFSuite::Table(s) => s.before_data(),
            SetupQTFSuite::Data1(s) => s.before_data(),
            SetupQTFSuite::Data2(s) => s.before_data(),
        }
    }
}

// PrimIndexSetup is used to setup and teardown test assets for the test
// suites that has the "before-class" property set to "PrimIndexSetup".
//
// It is an equivalent implementation to the Java PrimIndexSetup class in QTF.
#[derive(Debug, Default, Clone)]
pub(crate) struct PrimIndexSetup {
    pub num1: i32,
    pub num2: i32,
    pub num3: i32,
}

impl PrimIndexSetup {
    fn before_ddls(&self) -> Vec<String> {
        vec!["CREATE TABLE Foo( \
			id1 INTEGER, \
			id2 DOUBLE, \
			id3 ENUM(tok0, tok1, tok2), \
			firstName STRING,  \
			lastName STRING, \
			age INTEGER, \
			id4 STRING, \
			primary key (id1, id2, id3, id4))"
            .to_string()]
    }

    fn after_ddls(&self) -> Vec<String> {
        vec!["DROP TABLE IF EXISTS Foo".to_string()]
    }

    fn before_data(&self) -> HashMap<String, Vec<MapValue>> {
        let mut data: Vec<MapValue> = Vec::new();
        for i in 1..6 {
            for j in 0..3 {
                let mut jm = serde_json::Map::new();
                jm.insert("id1".to_string(), Value::Number(Number::from(i)));
                jm.insert(
                    "id2".to_string(),
                    Value::Number(Number::from_f64((i as f64 * 10.0) + j as f64).unwrap()),
                );
                jm.insert("id3".to_string(), Value::String(format!("tok{}", i % 3)));
                jm.insert("id4".to_string(), Value::String(format!("id4-{}", i)));
                jm.insert(
                    "firstName".to_string(),
                    Value::String(format!("first{}", i)),
                );
                jm.insert("lastName".to_string(), Value::String(format!("last{}", i)));
                jm.insert("age".to_string(), Value::Number(Number::from(i + 10)));
                data.push(MapValue::from_json_map(&jm).unwrap());
            }
        }
        let mut hm: HashMap<String, Vec<MapValue>> = HashMap::new();
        hm.insert("Foo".to_string(), data);
        hm
    }
}

// PrimIndexSetup2 is used to setup and teardown test assets for the test
// suites that has the "before-class" property set to "PrimIndexSetup2".
//
// It is an equivalent implementation to the Java PrimIndexSetup2 class in QTF.
#[derive(Debug, Default, Clone)]
pub(crate) struct PrimIndexSetup2 {
    pub num1: i32,
    pub num2: i32,
    pub num3: i32,
}

impl PrimIndexSetup2 {
    fn before_ddls(&self) -> Vec<String> {
        vec!["CREATE TABLE Foo(\
			id1 INTEGER, \
			id2 INTEGER, \
			id3 INTEGER, \
			firstName STRING,  \
			lastName STRING, \
			age INTEGER, \
			id4 STRING, \
			primary key (shard(id1, id2), id3, id4))"
            .to_string()]
    }

    fn after_ddls(&self) -> Vec<String> {
        vec!["DROP TABLE IF EXISTS Foo".to_string()]
    }

    fn before_data(&self) -> HashMap<String, Vec<MapValue>> {
        let mut data: Vec<MapValue> = Vec::new();
        let mut rnd = Random::new(1);
        for i in 0..self.num1 {
            for _j in 0..self.num2 {
                for _k in 0..self.num3 {
                    let mut jm = serde_json::Map::new();
                    jm.insert("id1".to_string(), Value::Number(rnd.next_int(20).into()));
                    jm.insert("id2".to_string(), Value::Number(rnd.next_int(5).into()));
                    jm.insert("id3".to_string(), Value::Number(rnd.next_int(5).into()));
                    jm.insert("id4".to_string(), Value::String(format!("id4-{}", i)));
                    jm.insert(
                        "firstName".to_string(),
                        Value::String(format!("first{}", i)),
                    );
                    jm.insert("lastName".to_string(), Value::String(format!("last{}", i)));
                    jm.insert("age".to_string(), Value::Number(Number::from(i + 10)));
                    data.push(MapValue::from_json_map(&jm).unwrap());
                }
            }
        }
        let mut hm: HashMap<String, Vec<MapValue>> = HashMap::new();
        hm.insert("Foo".to_string(), data);
        hm
    }
}

// PrimIndexSetup3 is used to setup and teardown test assets for the test
// suites that has the "before-class" property set to "PrimIndexSetup3".
//
// It is an equivalent implementation to the Java PrimIndexSetup3 class in QTF.
#[derive(Debug, Default, Clone)]
pub(crate) struct PrimIndexSetup3 {
    pub p2: PrimIndexSetup2,
}

impl PrimIndexSetup3 {
    fn before_ddls(&self) -> Vec<String> {
        let mut v = self.p2.before_ddls();
        v.push("CREATE INDEX idx1 on Foo (age, firstName)".to_string());
        v.push("CREATE INDEX idx2 on Foo (age)".to_string());
        v
    }

    fn after_ddls(&self) -> Vec<String> {
        self.p2.after_ddls()
    }

    fn before_data(&self) -> HashMap<String, Vec<MapValue>> {
        self.p2.before_data()
    }
}

// userTable is used to setup and teardown test assets for the test
// suites that has the "before-class" property set to "UserTable".
//
// It is an equivalent implementation to the Java UserTable class in QTF.
#[derive(Debug, Default, Clone)]
pub(crate) struct UserTable {
    pub num1: i32,
    pub num2: i32,
    pub num3: i32,
}

impl UserTable {
    fn before_ddls(&self) -> Vec<String> {
        vec!["CREATE TABLE Users \
			(id INTEGER, firstName STRING, lastName STRING, age INTEGER,  \
			address RECORD( \
			city STRING,  \
			state STRING,  \
			phones ARRAY(RECORD(work INTEGER, home INTEGER)),  \
			ptr STRING),  \
			children MAP(RECORD(age LONG, friends ARRAY(STRING))),  \
			primary key (id))"
            .to_string()]
    }

    fn after_ddls(&self) -> Vec<String> {
        vec!["DROP TABLE IF EXISTS Users".to_string()]
    }

    fn before_data(&self) -> HashMap<String, Vec<MapValue>> {
        let mut data: Vec<MapValue> = Vec::new();
        for i in 0..10 {
            let mut mv = MapValue::new();
            mv.put("id", i);
            mv.put("firstName", format!("first{}", i));
            mv.put("lastName", format!("last{}", i));
            mv.put("age", i + 10);

            let json1 = r#"{"city":"Boston", "state":"MA", "phones":[{"work":111, "home":222}], "ptr":null}"#;
            mv.put_json_map_value("address", json1).unwrap();

            let json2 =
                r#"{"john": {"age":3, "friends":["f1"]}, "cory": {"age":4, "friends":["f2"]}}"#;
            mv.put_json_map_value("children", json2).unwrap();

            data.push(mv);
        }

        let mut hm: HashMap<String, Vec<MapValue>> = HashMap::new();
        hm.insert("Users".to_string(), data);
        hm
    }
}

// data1Setup is used to setup and teardown test assets for the test
// suites that has the "before-class" property set to "Data1Setup".
//
// It is an equivalent implementation to the Java Data1Setup class in QTF.
#[derive(Debug, Clone)]
pub(crate) struct Data1Setup {}

impl Data1Setup {
    fn before_ddls(&self) -> Vec<String> {
        vec!["CREATE TABLE Data1Users \
			(id INTEGER, firstName STRING, lastName STRING, age INTEGER, \
			primary key (id))"
            .to_string()]
    }

    fn after_ddls(&self) -> Vec<String> {
        vec!["DROP TABLE IF EXISTS Data1Users".to_string()]
    }

    fn before_data(&self) -> HashMap<String, Vec<MapValue>> {
        let mut data: Vec<MapValue> = Vec::new();
        for i in 0..10 {
            let mut mv = MapValue::new();
            mv.put("id", i);
            mv.put("firstName", format!("first{}", i));
            mv.put("lastName", format!("last{}", i));
            mv.put("age", i + 10);
            data.push(mv);
        }

        let mut hm: HashMap<String, Vec<MapValue>> = HashMap::new();
        hm.insert("Data1Users".to_string(), data);
        hm
    }
}

// Data2Setup is used to setup and teardown test assets for the test
// suites that has the "before-class" property set to "Data2Setup".
//
// It is an equivalent implementation to the Java Data2Setup class in QTF.
#[derive(Debug, Clone)]
pub(crate) struct Data2Setup {}

impl Data2Setup {
    fn before_ddls(&self) -> Vec<String> {
        Vec::new()
    }

    fn after_ddls(&self) -> Vec<String> {
        Vec::new()
    }

    fn before_data(&self) -> HashMap<String, Vec<MapValue>> {
        HashMap::new()
    }
}
