//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
#![allow(dead_code)]

use crate::qtf_custom_setup::{
    Data1Setup, Data2Setup, PrimIndexSetup, PrimIndexSetup2, PrimIndexSetup3, SetupQTFSuite,
    UserTable,
};
use crate::types::{FieldValue, MapValue, NoSQLColumnToFieldValue};

use std::collections::HashMap;
use std::collections::VecDeque;
use std::error::Error;
use std::result::Result;
use std::{
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
};

use bigdecimal::{BigDecimal, Num};
use serde_json::Value;
//use tracing::debug;

// TestSuite represents a query test suite used to test a specific functionality
// and usually contains multiple query test cases.
//
// in QTF, a test suite contains test configurations, test case files, test
// result files, etc. that are organized in a sub directory in
// qtf_root_dir (kv/kvstore/test/query/cases).
#[derive(Debug, Default)]
pub struct TestSuite {
    // test suite name, which is the name of sub directory in qtf_root_dir.
    pub name: String,

    // the path to the directory that contains test configurations.
    // for example, ~/kv/kvstore/test/query/cases/gb for the test suite that
    // tests the group-by clause.
    pub dir: String,

    // the path to the directory that contains test case files.
    // for example, ~/kv/kvstore/test/query/cases/gb/q
    pub test_case_dir: String,

    // the path to the directory that contains test result files.
    // for example, ~/kv/kvstore/test/query/cases/gb/expres
    pub test_result_dir: String,

    // the ddl statements that need to execute before tests.
    // these are read from the file specified in the "before-ddl-file" property
    // in test.config file.
    pub before_ddls: Vec<String>,

    // the ddl statements that need to execute after tests complete.
    // these are read from the file specified in the "after-ddl-file" property
    // in test.config file.
    pub after_ddls: Vec<String>,

    // the initial data that need to insert into tables before tests.
    // the map's key is table name and the associated value is a vector of
    // MapValue, which represent the data to insert into that table.
    //
    // these are read from the file specified in the "before-data-file" property
    // in test.config file.
    pub before_data: HashMap<String, Vec<MapValue>>,

    // external variable name and values.
    // these come from the external variable settings in the form of
    // "var-$name=value" in test.config file.
    pub external_vars: HashMap<String, FieldValue>,

    // the test cases that are excluded because they are not applicable for
    // the test configuration or environment.
    //
    // there are test cases that are expected to fail when run against cloudsim
    // or on-premise, these are specified in
    //
    //   testdata/expectedQTFfailure.cloudsim.txt
    //   testdata/expectedQTFfailure.onprem.txt
    //
    // the map's key is test case name. As a special case, if the map contains
    // a "*" key, all test cases in this test suite are not applicable.
    pub excluded_test_cases: HashMap<String, bool>,

    // derived from "TEST_CASES" env variable
    pub included_test_cases: HashMap<String, bool>,

    // is_set_up indicates if the test suite has been setup or not.
    pub is_set_up: bool,

    // dependencies specifies the dependency test suites.
    pub dependencies: Vec<TestSuite>,
}

pub fn get_subdirs(path: &str, dirs: bool) -> Result<Vec<String>, Box<dyn Error>> {
    let fnres = get_subdirs_inner(path, dirs);
    if let Err(e) = fnres {
        return Err(format!("Error reading directory '{}': {}", path, e).into());
    }
    return fnres;
}

fn get_subdirs_inner(path: &str, dirs: bool) -> Result<Vec<String>, Box<dyn Error>> {
    let mut file_names: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(path)? {
        if let Ok(e) = &entry {
            let path = e.path();
            if path.is_dir() {
                if dirs == true {
                    let f = e.file_name();
                    let p = f.to_str().unwrap();
                    file_names.push(p.to_string());
                }
            } else {
                if dirs == false {
                    let f = e.file_name();
                    let p = f.to_str().unwrap();
                    file_names.push(p.to_string());
                }
            }
        }
    }
    Ok(file_names)
}

impl TestSuite {
    // get_test_case_names returns test case names in the test suite.
    // the returned test cases names are sorted in lexical order.
    pub fn get_test_case_names(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let file_names = get_subdirs(&self.test_case_dir, false)?;
        let mut tc_names: Vec<String> = Vec::new();
        for name in file_names {
            // test case file name extention must be ".q".
            if name.ends_with(".q") {
                tc_names.push(name);
            }
        }
        tc_names.sort();
        Ok(tc_names)
    }

    // get_test_case parses test configurations for the specified test case, which
    // contains all required information such as the query statements,
    // expected query results, expected compile error or runtime error, etc.
    pub fn get_test_case(&self, name: &str) -> Result<TestCase, Box<dyn Error>> {
        if !name.ends_with(".q") || name.len() <= 2 {
            return Err(format!(
                "test case file name extention must be .q, \
                got invalid name {name}"
            )
            .into());
        }

        // test result file name extention must be ".r".
        let offset = name.len() - 1;
        let mut file_name = name.to_string();
        file_name.replace_range(offset..offset + 1, "r");
        let test_result_file = sjoin(&self.test_result_dir, &file_name);
        let mut results = VecDeque::from(read_lines_from_file(&test_result_file, true, true)?);
        if results.len() == 0 {
            return Err(format!("test result file {} is empty", test_result_file.clone()).into());
        }

        let mut tc = TestCase {
            name: name.to_string(),
            ..Default::default()
        };

        let test_case_file = sjoin(&self.test_case_dir, &name);
        tc.query_stmts = read_lines_from_file(&test_case_file, true, true)?.join(" ");

        let result_type = results.pop_front().unwrap().to_lowercase();

        if result_type.contains("unordered-result") {
            tc.expect_ordered_result = false;
        } else if result_type.contains("ordered-result") {
            tc.expect_ordered_result = true;
        } else if result_type.contains("compile-exception") {
            tc.expect_compile_err = true;
        } else if result_type.contains("runtime-exception") {
            tc.expect_runtime_err = true;
        } else {
            return Err(format!("unknown result type {result_type}").into());
        }

        if !tc.expect_compile_err && !tc.expect_runtime_err {
            tc.expect_results = Vec::from(results);
        } else if tc.expect_runtime_err {
            tc.expect_err_messages = Vec::from(results);
        }
        Ok(tc)
    }

    // is_excluded_test_case reports whether the specified test case is excluded from
    // the test suite because of not applicable for the test configuration or environment.
    pub fn is_excluded_test_case(&self, name: &str) -> bool {
        if self.included_test_cases.len() > 0
            && self.included_test_cases.contains_key(name) == false
        {
            return true;
        }
        if self.excluded_test_cases.contains_key(name) {
            return true;
        }
        // check for name + ".q"
        let mut qname = name.to_string();
        qname.push_str(".q");
        self.excluded_test_cases.contains_key(&qname)
    }
}

// TestCase represents a query test case in QTF.
//
// in QTF, a test case consists of test input and test output that are specified
// in a *.q and *.r file respectively.
#[derive(Debug, Default)]
pub struct TestCase {
    // test case name.
    // it is the test case file name.
    pub name: String,

    // the query statements for the test.
    // these are read from the test case file *.q.
    // TODO: Vec<String>?
    pub query_stmts: String,

    // the following 5 fields are read from the test result file *.r

    // the expected query results, in JSON format
    pub expect_results: Vec<String>,

    // this indicates if ordered query result is expected.
    // by default the desired query result is un-ordered.
    pub expect_ordered_result: bool,

    // this indicates if the query should cause a compile error.
    pub expect_compile_err: bool,

    // this indicates if the query should cause a runtime error.
    pub expect_runtime_err: bool,

    // the expected error messages.
    // this is only applicable when _expect_runtime_err is true.
    pub expect_err_messages: Vec<String>,
}

// test_runner is used to run QTF tests.
#[derive(Debug, Default)]
pub struct TestRunner {
    // QTF test configuration.
    pub qtf_root_dir: String,

    // sub directory names of the qtf_root_dir.
    // usually each sub directory represent a test suite.
    pub dir_names: Vec<String>,

    // the test suites/cases that are excluded because they are not applicable
    // for the test configuration or environment.
    //
    // there are test cases that are currently only applicable for on-premise,
    // not applicable for cloud.
    // these are specified in testdata/onprem_only_testcases.txt, which is
    // copied from test/oracle/nosql/query/expectedQTF_failure.xml in httpproxy repository.
    //
    // the map's key is test suite name and the associated value is a map that
    // contains test cases not applicable for that test suite.
    pub excluded_tests: HashMap<String, HashMap<String, bool>>,

    // include lists, processed by environment variables
    pub included_suites: HashMap<String, bool>,
    pub included_tests: HashMap<String, bool>,
}

impl TestRunner {
    // new creates a QTF test runner with the specified configuration.
    pub fn new(root_dir: &str) -> Result<Self, Box<dyn Error>> {
        let mut r = TestRunner {
            qtf_root_dir: root_dir.to_string(),
            ..Default::default()
        };
        r.dir_names = get_subdirs(root_dir, true)?;

        //let mut expect_failure_file = String::new();
        // TODO: if test.is_cloud()
        //if true {
        let expect_failure_file = "testdata/expectedQTFfailure.cloudsim.txt".to_string();
        //} else {
        // TODO: if test.is_onprem()
        //expect_failure_file = "testdata/expectedQTFfailure.onprem.txt".to_string();
        //}

        if expect_failure_file.len() > 0 {
            // check and exclude test cases that are not applicable for the test environment.
            r.excluded_tests = parse_excluded_tests(&expect_failure_file)?;
        }

        // parse env variable "QTF_TEST_SUITES" to get an include list of wuite names to run
        if let Ok(env) = std::env::var("QTF_TEST_SUITES") {
            for name in env.split(",") {
                println!("Including any suite named '{}'", name);
                r.included_suites.insert(name.to_string(), true);
            }
        }

        // parse env variable "QTF_TEST_CASES" to get an include list of test names to run
        // these must include the ".q" suffix
        if let Ok(env) = std::env::var("QTF_TEST_CASES") {
            for name in env.split(",") {
                println!("Including any test named '{}'", name);
                r.included_tests.insert(name.to_string(), true);
            }
        }

        Ok(r)
    }

    pub fn get_num_tests(&self, suite_name: &str) -> i32 {
        let sdir = sjoin(&self.qtf_root_dir, suite_name);
        let tdir = sjoin(&sdir, "q");
        if let Ok(files) = get_subdirs(&tdir, false) {
            return files.len() as i32;
        }
        return 0;
    }

    // is_excluded_test_suite reports whether the specified test suite is excluded
    // because of not applicable for the test configuration or environment.
    pub fn is_excluded_test_suite(&self, name: &str) -> bool {
        if self.included_suites.len() > 0 && self.included_suites.contains_key(name) == false {
            return true;
        }
        if let Some(m) = self.excluded_tests.get(name) {
            if let Some(n) = m.get("*") {
                return *n;
            }
        }
        return false;
    }

    // get_test_suite parses test configurations for the specified test suite, which
    // contains all required information such as the ddl statements to execute
    // before and after tests, the initial data to insert into tables, etc.
    pub fn get_test_suite(&self, name: &str) -> Result<TestSuite, Box<dyn Error>> {
        let ts_dir = sjoin(&self.qtf_root_dir, name);
        //let tc_dir = sjoin(&ts_dir, "q");
        let lines = read_lines_from_file(&sjoin(&ts_dir, "test.config"), true, true)?;

        let mut ts = TestSuite {
            name: name.to_string(),
            dir: ts_dir,
            included_test_cases: self.included_tests.clone(),
            //test_case_dir: tc_dir,
            ..Default::default()
        };

        for l in lines {
            let line = l.trim();
            if line.len() == 0 || line.starts_with("#") || line.starts_with("//") {
                continue;
            }

            let sub_strs = ssplit(line, "=");
            if sub_strs.len() < 2 {
                continue;
            }

            let s1 = sub_strs[0].trim();
            let s2 = sub_strs[1].trim();
            let fname = sjoin(&ts.dir, s2);
            match s1 {
                "before-ddl-file" => {
                    ts.before_ddls = read_blocks_from_file(&fname)?;
                }
                "before-data-file" => {
                    ts.before_data = read_data_file(&fname)?;
                }
                "after-ddl-file" => {
                    ts.after_ddls = read_blocks_from_file(&fname)?;
                }
                // assume the same class is specified for "before-class" and "after-class",
                // it would suffice to process the "before-class" only.
                "before-class" => {
                    let mut found = true;
                    let mut p = SetupQTFSuite::Setup(PrimIndexSetup::default());
                    match s2 {
                        "PrimIndexSetup" => (), // already set up above
                        "PrimIndexSetup2" => {
                            p = SetupQTFSuite::Setup2(PrimIndexSetup2 {
                                num1: 20,
                                num2: 5,
                                num3: 3,
                            });
                        }
                        "PrimIndexSetup3" => {
                            p = SetupQTFSuite::Setup3(PrimIndexSetup3 {
                                p2: PrimIndexSetup2 {
                                    num1: 20,
                                    num2: 15,
                                    num3: 3,
                                },
                            });
                        }
                        "UserTable" => {
                            p = SetupQTFSuite::Table(UserTable::default());
                        }
                        "Data1Setup" => {
                            p = SetupQTFSuite::Data1(Data1Setup {});
                        }
                        "Data2Setup" => {
                            p = SetupQTFSuite::Data2(Data2Setup {});
                        }
                        _ => {
                            found = false;
                        }
                    }
                    if found {
                        ts.before_ddls = p.before_ddls();
                        ts.after_ddls = p.after_ddls();
                        ts.before_data = p.before_data();
                    }
                }
                _ => (),
            }
            if s1.starts_with("run-") {
                if sub_strs.len() < 3 {
                    return Err(format!("invalid test suite config: {line}").into());
                }

                //Parsing s1=run-sec-index: s2=q(), sub_strs=["run-sec-index ", " q() ", " expres"]

                //println!("Parsing s1={}: s2={}, sub_strs={:?}", s1, s2, sub_strs);
                ts.test_result_dir = sjoin(&ts.dir, sub_strs[2].trim());

                // example: run-gb = q(dep_dir1, dep_dir2, //dep_dir3) = expres
                let idx1_opt = s2.find("(");
                let idx2_opt = s2.find(")");
                if let Some(idx1) = idx1_opt {
                    if let Some(idx2) = idx2_opt {
                        ts.test_case_dir = sjoin(&ts.dir, &s2[..idx1]);
                        // resolve the dependencies.
                        if idx2 > idx1 {
                            let sub = ssplit(&s2[idx1 + 1..idx2], ",");
                            for d in &sub {
                                let dir_name = d.trim();
                                if dir_name.len() == 0 {
                                    continue;
                                }
                                if dir_name.starts_with("//") {
                                    //ts.dependencies.push(self.get_test_suite(&dir_name[2..])?);
                                    ts.dependencies
                                        .push(self.get_test_suite(&dir_name[2..]).unwrap());
                                } else {
                                    let p = sjoin(&ts.name, dir_name);
                                    //ts.dependencies.push(self.get_test_suite(&p)?);
                                    ts.dependencies.push(self.get_test_suite(&p).unwrap());
                                }
                            }
                        }
                    }
                }
            } else if s1.starts_with("var-") {
                let var_name = s1[4..].to_string();

                // the value for Binary or FixBinary is represented as a base64
                // encoded: String, which may contain the "=" character, so we cannot
                // use s2 as the value, we have to retrieve the value from the
                // original text.
                let off = line.find("=").unwrap_or(10000);
                if off == 10000 {
                    return Err(format!("invalid test suite config: {line}").into());
                }
                let value = line[off + 1..].trim().to_string();
                let ext_var = parse_variables(var_name, value)?;
                ts.external_vars.insert(ext_var.name, ext_var.value);
            }
        }
        Ok(ts)
    }
}

// ExtVariable represents an external variable defined in the test.config file.
#[derive(Default, Debug)]
pub struct ExtVariable {
    // variable name.
    pub name: String,

    // variable value.
    pub value: FieldValue,
    // variable type.
    //var_type types.db_type
}

pub fn parse_variables(name: String, value: String) -> Result<ExtVariable, Box<dyn Error>> {
    let mut ext_var = ExtVariable::default();
    // the desired format is $name or type-$name
    let sub = ssplit(&name, "-");
    if sub.len() == 1 {
        ext_var.name = sub[0].clone();
    } else {
        // type_name = sub[0]
        ext_var.name = sub[1].clone();
    }

    if value.len() == 0 || value == "null" {
        ext_var.value = FieldValue::Null;
        return Ok(ext_var);
    }

    if value == "jnull" {
        ext_var.value = FieldValue::JsonNull;
        //ext_var.value = FieldValue::Null;
        return Ok(ext_var);
    }

    if value.starts_with("type:") {
        let mut vals = snsplit(3, &value, ":");
        if vals.len() < 3 {
            return Err(format!("Invalid type in test config: {}", value).into());
        }
        if vals[1] == "int" {
            ext_var.value = FieldValue::Integer(vals[2].parse::<i32>()?);
        } else if vals[1] == "long" {
            ext_var.value = FieldValue::Long(vals[2].parse::<i64>()?);
        } else if vals[1] == "number" {
            ext_var.value = FieldValue::Number(BigDecimal::from_str_radix(&vals[2], 10)?);
        } else if vals[1] == "json" {
            if vals[2].starts_with("\"\"") {
                // strip the enclosing double quotes.
                vals[2] = vals[2][1..vals[2].len() - 1].to_string();
            }
            let v: Value = serde_json::from_str(&vals[2])?;
            ext_var.value = v.to_field_value();
        } else if vals[1] == "string" {
            if vals[2].starts_with("\"") {
                // strip the enclosing double quotes.
                vals[2] = vals[2][1..vals[2].len() - 1].to_string();
            }
            ext_var.value = FieldValue::String(vals[2].clone());
        } else if vals[1] == "double" {
            ext_var.value = FieldValue::Double(vals[2].parse::<f64>()?);
        } else if vals[1] == "boolean" {
            ext_var.value = FieldValue::Boolean(vals[2].parse::<bool>()?);
        } else {
            return Err(format!("unsupported type in bindvar: {}", value).into());
        }
        return Ok(ext_var);
    }

    // infer the type from specified value.
    let c = value.as_str().chars().nth(0).unwrap();
    let ec = value.as_str().chars().nth(value.len() - 1).unwrap();
    match c {
        '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' | '+' | '-' => {
            if let Ok(v) = value.parse::<i32>() {
                ext_var.value = FieldValue::Integer(v);
                return Ok(ext_var);
            }
            if let Ok(v) = value.parse::<i64>() {
                ext_var.value = FieldValue::Long(v);
                return Ok(ext_var);
            }
            if let Ok(v) = value.parse::<f64>() {
                ext_var.value = FieldValue::Double(v);
                return Ok(ext_var);
            }
            if let Ok(n) = BigDecimal::from_str_radix(&value, 10) {
                ext_var.value = FieldValue::Number(n);
                return Ok(ext_var);
            }
            return Err(format!(
                "cannot parse {value} as a numeric value for external variable {0}",
                ext_var.name
            )
            .into());
        }
        '"' => {
            if value.len() < 2 {
                return Err(format!(
                    "invalid string value {value} for external variable {0}",
                    ext_var.name
                )
                .into());
            }
            // strip the enclosing double quotes.
            ext_var.value = FieldValue::String(value[1..value.len() - 1].to_string());
            return Ok(ext_var);
        }
        '{' => {
            if value.len() < 2 || ec != '}' {
                return Err(format!(
                    "invalid JSON value {value} for external variable {0}",
                    ext_var.name
                )
                .into());
            }
            let v: Value = serde_json::from_str(&value)?;
            ext_var.value = v.to_field_value();
            return Ok(ext_var);
        }
        '[' => {
            if value.len() < 2 || ec != ']' {
                return Err(format!(
                    "invalid JSON value {value} for external variable {0}",
                    ext_var.name
                )
                .into());
            }
            let v: Value = serde_json::from_str(&value)?;
            // this should be an Array
            if let serde_json::value::Value::Array(json_array) = v {
                let mut arr: Vec<FieldValue> = Vec::new();
                for val in json_array {
                    arr.push(val.to_field_value());
                }
                ext_var.value = FieldValue::Array(arr);
                return Ok(ext_var);
            }
            return Err(format!("Invalid JSON value {value} expected Array, got {:?}", v).into());
        }
        't' | 'T' => {
            ext_var.value = FieldValue::Boolean(true);
            return Ok(ext_var);
        }
        'f' | 'F' => {
            ext_var.value = FieldValue::Boolean(false);
            return Ok(ext_var);
        }
        _ => (),
    }
    Err(format!(
        "unsupported value {value} for external variable {0}",
        ext_var.name
    )
    .into())
}

// read_data_file reads the intial data that need to insert into tables from the specified file.
pub fn read_data_file(file: &str) -> Result<HashMap<String, Vec<MapValue>>, Box<dyn Error>> {
    let lines = read_lines_from_file(file, true, false)?;

    let mut table = String::new();
    let mut values: Vec<MapValue> = Vec::new();
    let mut cnt: i32 = 0;
    let mut b = String::new();
    let mut data: HashMap<String, Vec<MapValue>> = HashMap::new();

    for line in lines {
        let s = line.trim();

        if s.len() == 0 {
            // the empty line is part of the JSON.
            if cnt != 0 {
                b.push_str(&line);
            }
            continue;
        }

        if s.to_lowercase().starts_with("table") {
            let sub_strs = snsplit(2, s, ":");
            if sub_strs.len() == 2 {
                // set data for the previous table.
                if table != "" {
                    data.insert(table, values);
                }
                // reset values and begin with a new table.
                values = Vec::new();
                table = sub_strs[1].trim().to_string();
            }
            continue;
        }

        b.push_str(&line);
        cnt += num_curly_brace_unmatched(&line);
        // if the concatenation of previous lines is a valid JSON object,
        // we have read a complete record data.
        if cnt == 0 {
            //let v: Value = serde_json::from_str(&b)?;
            //if let Ok(v) = serde_json::from_str(&b) {
            match serde_json::from_str(&b) {
                Ok(v) => {
                    let val = MapValue::from_json_object(&v)?;
                    //debug!("read value from json: {:?}", val);
                    values.push(val);
                }
                Err(e) => {
                    println!(
                        "WARN: error converting expected value to json in {} record {}: {e}",
                        file,
                        values.len()
                    );
                    println!(" json with error: {}", b);
                }
            }
            b = String::new();
        }
    }

    // set data for the last table.
    if table != "" && values.len() > 0 {
        data.insert(table, values);
    }

    Ok(data)
}

// num_curly_brace_unmatched checks and reports the number of unmatched curly braces
// in the specified JSON string. A zero number indicates the specified JSON
// string represents a complete JSON object.
pub fn num_curly_brace_unmatched(s: &str) -> i32 {
    let mut in_quotes: bool = false;
    let mut prev: char = ' ';
    let mut cnt: i32 = 0;
    for r in s.chars() {
        match r {
            '{' => {
                if in_quotes == false {
                    cnt += 1;
                }
            }
            '}' => {
                if in_quotes == false {
                    cnt -= 1;
                }
            }
            '"' => {
                if prev != '\\' {
                    in_quotes = !in_quotes
                }
            }
            _ => (),
        }
        prev = r;
    }
    return cnt;
}

// read_lines_from_file reads contents line by line from the specified file.
// the lines that are empty or begin with // or # are discarded.
fn read_lines_from_file(
    filename: &str,
    strip_comments: bool,
    strip_empty: bool,
) -> Result<Vec<String>, Box<dyn Error>> {
    match read_lines_from_file_inner(filename, strip_comments, strip_empty) {
        Ok(l) => {
            return Ok(l);
        }
        Err(e) => {
            return Err(format!("Error reading file '{}': {}", filename, e).into());
        }
    }
}

fn read_lines_from_file_inner(
    filename: impl AsRef<Path>,
    strip_comments: bool,
    strip_empty: bool,
) -> io::Result<Vec<String>> {
    let raw = BufReader::new(File::open(filename)?).lines();
    let mut lines: Vec<String> = Vec::new();
    for i in raw {
        if let Ok(mut s) = i {
            if strip_comments == false && strip_empty == false {
                lines.push(s);
                continue;
            }
            if strip_comments {
                if s.starts_with("#") || s.starts_with("//") {
                    continue;
                }
                //  strip "//" comments from end of lines
                if let Some(index) = s.find("//") {
                    s.truncate(index);
                }
            }
            if strip_empty {
                if s.trim().len() == 0 {
                    continue;
                }
            }
            lines.push(s);
        }
    }
    io::Result::Ok(lines)
    //BufReader::new(File::open(filename)?).lines().collect()
}

// read_blocks_from_file reads contents in blocks from the specified file.
// a block of content ends with an empty line.
pub fn read_blocks_from_file(filename: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let lines = read_lines_from_file(filename, false, false)?;
    let mut blocks: Vec<String> = Vec::new();
    let mut s = String::new();
    for l in lines.into_iter() {
        let i = l.trim();
        if i.len() == 0 {
            if s.len() > 0 {
                blocks.push(std::mem::take(&mut s));
            }
            continue;
        }
        if i.starts_with("//") || i.starts_with("#") {
            continue;
        }
        if s.len() > 0 {
            s.push_str(" ");
        }
        s.push_str(&i);
    }
    if s.len() > 0 {
        blocks.push(s);
    }
    Ok(blocks)
}

// parse_excluded_tests reads from the specified file a list of test cases that
// are not applicable for the tests.
//
// the test cases specified in the file is in one of the forms:
//
//   1. TestSuite_name
//   2. TestSuite_name/test_case_dir
//   3. TestSuite_name/test_case_dir/test_case_name.q
//
// the first two forms indicate the entire test suite is not applicable.
// the third form indicates the specified test case is not applicable.
pub fn parse_excluded_tests(
    filename: &str,
) -> Result<HashMap<String, HashMap<String, bool>>, Box<dyn Error>> {
    println!("parse_excluded_tests: {}", filename);
    let lines = read_lines_from_file(filename, true, true)?;

    let mut excluded_tests: HashMap<String, HashMap<String, bool>> = HashMap::new();
    for x in lines {
        if x.starts_with('#') { continue };
        let sub_strs = ssplit(&x, "/");
        let testsuite = sub_strs[0].clone();
        let testcase: String;
        if sub_strs.len() <= 2 {
            testcase = "*".to_string();
        } else {
            testcase = sub_strs[2].clone();
        }
        if let Some(m) = excluded_tests.get_mut(&testsuite) {
            m.insert(testcase, true);
        } else {
            let mut sm: HashMap<String, bool> = HashMap::new();
            sm.insert(testcase, true);
            excluded_tests.insert(testsuite, sm);
        }
    }
    Ok(excluded_tests)
}

// Random is a pseudorandom number generator that implements the same algorithm
// as that used in java's Random class.
//
// it is required to use the Random type, rather than use rust's rand crate
// because some QTF test cases rely on java's Random class to generate the data
// that are inserted into the table before tests.
pub struct Random {
    seed: i64,
}

const MULTIPLIER: i64 = 0x5DFFCE66D;
const ADDEND: i64 = 0xB;
const MASK: i64 = (1 << 48) - 1;

impl Random {
    // creates a random number generator with the seed provided.
    pub fn new(seed: i64) -> Self {
        Random {
            seed: (seed ^ MULTIPLIER) & MASK,
        }
    }

    // next_int returns a pseudorandom, uniformly distributed i32 value
    // between 0 (inclusive) and the specified value (exclusive).
    pub fn next_int(&mut self, bound: i32) -> i32 {
        let mut x = self.next(31);
        let m = bound - 1;
        // n is a power of 2
        if bound % m == 0 {
            return ((bound as i64 * x as i64) >> 31) as i32;
        }

        let mut u = x;
        loop {
            x = u % bound;
            if u - x + m >= 0 {
                break;
            }
            u = self.next(31);
        }

        return x;
    }

    // TODO: make this thread-safe with atomics
    pub fn next(&mut self, bits: i32) -> i32 {
        let nextseed = ((self.seed.wrapping_mul(MULTIPLIER)) + ADDEND) & MASK;
        self.seed = nextseed;

        return (nextseed >> (48 - bits as u32)) as i32;
    }
}

fn ssplit(s: &str, i: &str) -> Vec<String> {
    let mut arr: Vec<String> = Vec::new();
    for ss in s.split(i) {
        arr.push(ss.to_string());
    }
    arr
}

fn snsplit(n: usize, s: &str, i: &str) -> Vec<String> {
    let mut arr: Vec<String> = Vec::new();
    for ss in s.splitn(n, i) {
        arr.push(ss.to_string());
    }
    arr
}

fn sjoin(s1: &str, s2: &str) -> String {
    let mut s = s1.to_string();
    s.push('/');
    s.push_str(s2);
    s
}
