//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::qtf::{TestCase, TestRunner, TestSuite};
use crate::rate_limiter::RateLimiter;
use crate::sort_iter::SortSpec;
use crate::types::*;
use crate::Handle;
use crate::HandleMode;
use crate::NoSQLError;
use crate::PutRequest;
use crate::QueryRequest;
use crate::SystemRequest;
use crate::TableRequest;
use async_recursion::async_recursion;
use core::cmp::Ordering;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

#[derive(Debug, Default)]
struct SuiteResults {
    passed: i32,
    failed: i32,
    skipped: i32,
}

async fn get_handle() -> Result<Handle, NoSQLError> {
    Handle::builder()
        .endpoint("http://localhost:8080")?
        .mode(HandleMode::Cloudsim)?
        .timeout(Duration::new(30, 0))?
        //.cloud_auth_from_file("~/.oci/config")?
        .from_environment()?
        .build()
        .await
}

#[tokio::test]
async fn qtf_test() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .without_time()
        .with_ansi(false)
        .with_level(false)
        .compact()
        .init();

    let handle = get_handle().await?;

    let qtf_root_dir = std::env::var("QTF_ROOT_DIR").unwrap_or("".to_string());
    if qtf_root_dir.is_empty() {
        return Err("please set QTF_ROOT_DIR to the resources/cases directory".into());
    }

    let runner = TestRunner::new(&qtf_root_dir)?;
    let mut totals = SuiteResults::default();

    // TODO: cmdline arg? environment?
    let mut rl = RateLimiter::new(3.0);

    for name in &runner.dir_names {
        if runner.is_excluded_test_suite(name) {
            // The entire test suite is excluded.
            // Create an instance of the test suite, do not read test configurations.
            totals.skipped += runner.get_num_tests(name);
            //println!("Skipping test suite {}", name);
            continue;
        }
        // Read configurations for the test suite.
        let ts_opt = runner.get_test_suite(name);
        if let Err(e) = ts_opt {
            println!("FAIL: get_test_suite({name}) got error: {e}");
            totals.failed += runner.get_num_tests(name);
            continue;
        }
        let mut ts = ts_opt.unwrap();

        // Some test suites such as "identity" has a corresponding sub directory in
        // QTFRootDir, but all properties in test.config are commented out.
        // Skip such test suites.
        if ts.test_case_dir == "" || ts.test_result_dir == "" {
            println!("Skipping directory {name} as it does not represent a test suite");
            totals.skipped += runner.get_num_tests(name);
            continue;
        }

        if let Some(m) = runner.excluded_tests.get(name) {
            ts.excluded_test_cases = m.clone();
        }

        match run_test_suite(&mut ts, &handle, &mut rl).await {
            Ok(r) => {
                println!("Suite {}: {:?}", name, r);
                totals.passed += r.passed;
                totals.failed += r.failed;
                totals.skipped += r.skipped;
            }
            Err(e) => {
                println!("FAIL: Test suite {name} failed: {}", e);
                totals.failed += runner.get_num_tests(name);
                tear_down_test_suite(&mut ts, &handle, &mut rl).await?;
            }
        }
    }
    println!(
        "Total test suites: {}, {:?}",
        runner.dir_names.len(),
        totals
    );
    // TODO: Err if failed > 0
    Ok(())
}

async fn run_test_suite(
    ts: &mut TestSuite,
    handle: &Handle,
    rl: &mut RateLimiter,
) -> Result<SuiteResults, Box<dyn Error>> {
    let mut tr = SuiteResults::default();
    let tc_names = ts.get_test_case_names()?;
    if ts.excluded_test_cases.get("*").is_some() {
        println!("skip test suite {} as it is not applicable", ts.name);
        tr.skipped += tc_names.len() as i32;
        return Ok(tr);
    }

    println!(
        "total number of testcases in suite {} is {}",
        ts.name,
        tc_names.len()
    );

    for tc_name in tc_names {
        if ts.is_excluded_test_case(&tc_name) {
            //println!("Skipping testcase {}", tc_name);
            tr.skipped += 1;
            continue;
        }
        let tc_opt = ts.get_test_case(&tc_name);
        if let Err(e) = tc_opt {
            println!("FAIL: get_test_case({tc_name}) got error: {e}");
            tr.failed += 1;
            continue;
        }
        let mut tc = tc_opt.unwrap();

        // The "delete" test suite requires setting up test assets for each test case
        // otherwise the test result may be affected by the previous tests.
        if ts.name == "delete" || ts.is_set_up == false {
            set_up_test_suite(ts, handle, rl).await?;
            ts.is_set_up = true;
        }

        run_test_case(ts, &mut tc, &mut tr, handle).await;
    }

    tear_down_test_suite(ts, handle, rl).await?;
    Ok(tr)
}

#[async_recursion(?Send)]
async fn set_up_test_suite(
    ts: &TestSuite,
    handle: &Handle,
    rl: &mut RateLimiter,
) -> Result<(), Box<dyn Error>> {
    // Do cleanup in case the previous run did not drop tables/indexes.
    clean_up(ts, handle, rl).await?;

    for dep in &ts.dependencies {
        set_up_test_suite(dep, handle, rl).await?;
    }

    // Set a table limit that are required for cloud tests.
    let units = std::env::var("QTF_LIMIT_UNITS")
        .ok()
        .and_then(|units| units.parse::<u16>().ok())
        .unwrap_or(500);
    let limits = TableLimits::provisioned(units, units, 3);

    if ts.before_ddls.len() > 0 {
        println!("Executing suite {} DDLs on setup:", ts.name);
    }

    // TODO: if total units for table creation exceeds max, set limit for each to 1/Nth

    for stmt in &ts.before_ddls {
        println!("  {}", stmt);
        let substrs = snsplit(10, stmt, " ");
        if substrs.len() < 2 {
            continue;
        }

        let s1 = substrs[0].to_uppercase();
        let s2 = substrs[1].to_uppercase();
        if s1 == "CREATE" && s2 == "TABLE" {
            // don't set limits on child tables
            if substrs.len() > 5 {
                let s3 = substrs[2].to_uppercase();
                let s4 = substrs[3].to_uppercase();
                let s5 = substrs[4].to_uppercase();
                if s3 == "IF" && s4 == "NOT" && s5 == "EXISTS" && substrs[5].find(".").is_some() {
                    execute_table_ddl(ts, &stmt, handle, rl).await?;
                    continue;
                }
            }
            if substrs.len() > 2 && substrs[2].find(".").is_some() {
                execute_table_ddl(ts, &stmt, handle, rl).await?;
            } else {
                create_table(ts, &stmt, &limits, handle, rl).await?;
            }
        } else if s2 == "NAMESPACE" || s2 == "USER" || s2 == "ROLE" {
            // CREATE/DROP NAMESPACE/USER/ROLE operations should use SystemRequest.
            execute_ddl(ts, &stmt, handle, rl).await?;
        } else {
            execute_table_ddl(ts, &stmt, handle, rl).await?;
        }
    }

    for (table, rows) in &ts.before_data {
        println!("Inserting {} rows into table '{}'", rows.len(), table);
        for mv in rows {
            //println!("row={:?}", mv);
            let pres = PutRequest::new(&table)
                .value(mv.clone_internal())
                .execute(handle)
                .await;
            if let Err(e) = pres {
                //println!("WARN: Error inserting value {:#?} into table {}: {e}", mv, table);
                println!(
                    "WARN: Error inserting before.data value into table {}: {e}",
                    table
                );
                //return Err(e);
                continue;
            }
            let ver = pres?.version;
            if ver.is_none() {
                return Err(
                    format!("Put(table={table}, row={:?}) returned no version", &mv).into(),
                );
            }
        }
    }
    Ok(())
}

async fn tear_down_test_suite(
    ts: &mut TestSuite,
    handle: &Handle,
    rl: &mut RateLimiter,
) -> Result<(), Box<dyn Error>> {
    // TODO
    //if !suite.DropTablesOnTearDown {
    //return
    //}

    clean_up(ts, handle, rl).await
}

#[async_recursion(?Send)]
async fn clean_up(
    ts: &TestSuite,
    handle: &Handle,
    rl: &mut RateLimiter,
) -> Result<(), Box<dyn Error>> {
    for dep in &ts.dependencies {
        clean_up(dep, handle, rl).await?;
    }

    if ts.after_ddls.len() > 0 {
        println!("Executing {} DDLs to clean up:", ts.after_ddls.len());
    }

    for stmt in &ts.after_ddls {
        println!("  {}", stmt);
        let substrs = snsplit(3, stmt, " ");
        if substrs.len() < 2 {
            continue;
        }

        let s2 = substrs[1].to_uppercase();
        if s2 == "NAMESPACE" || s2 == "USER" || s2 == "ROLE" {
            // CREATE/DROP NAMESPACE/USER/ROLE operations should use SystemRequest.
            match execute_ddl(ts, &stmt, handle, rl).await {
                Ok(_) => (),
                Err(e) => {
                    // ignore ResourceNotFound error
                    let msg = format!("{e}");
                    if !msg.contains("ResourceNotFound") {
                        return Err(e);
                    }
                }
            }
        } else {
            match execute_table_ddl(ts, &stmt, handle, rl).await {
                Ok(_) => (),
                Err(e) => {
                    // Ignore TableNotFound and IndexNotFound errors.
                    let msg = format!("{e}");
                    if !msg.contains("TableNotFound") && !msg.contains("IndexNotFound") {
                        return Err(e);
                    }
                }
            }
        }
    }
    Ok(())
}

fn json_to_map_value(json: &str) -> Result<MapValue, Box<dyn Error>> {
    let v: Value = serde_json::from_str(json)?;
    let mv = MapValue::from_json_object(&v)?;
    Ok(mv)
}

fn compare_map_values(mv1: &MapValue, mv2: &MapValue, nulls_equal: bool) -> Ordering {
    compare_map_values_sortspec(mv1, mv2, &SortSpec::default(), nulls_equal)
}

fn compare_json_to_mapvalue(exp_json: &str, mv: &MapValue) -> Result<bool, Box<dyn Error>> {
    let exp_mv = json_to_map_value(exp_json)?;
    Ok(compare_map_values(&exp_mv, mv, true) == Ordering::Equal)
}

async fn run_test_case(
    ts: &mut TestSuite,
    tc: &mut TestCase,
    tr: &mut SuiteResults,
    handle: &Handle,
) {
    let display_name = format!("{}/{}", ts.name, tc.name);
    println!("\n TEST CASE: {}\n", display_name);
    if ts.excluded_test_cases.contains_key(&tc.name) {
        // This test case is not applicable.
        println!("skip test case {display_name} as it is not applicable");
        tr.skipped += 1;
        return;
    }

    if tc.query_stmts.len() == 0 {
        println!("no query statements found for test case {display_name}");
        tr.skipped += 1;
        return;
    }

    let mut stmt = tc.query_stmts.clone();
    println!("stmt={}", stmt);

    // Check if the test case contains multiple query statements.
    // A QTF test case may contain one of the INSERT/DELETE/UPDATE statements
    // followed with a SELECT statement.
    let tmp_stmt = stmt.to_lowercase();
    let mut upd_pos = tmp_stmt.find("update");
    let ins_pos = tmp_stmt.find("insert");
    let del_pos = tmp_stmt.find("delete");
    let sel_pos = tmp_stmt.find("select");

    if ins_pos.is_some() {
        upd_pos = ins_pos;
    } else if del_pos.is_some() {
        upd_pos = del_pos;
    }

    let mut upd_stmt = "".to_string();
    if upd_pos.is_some() && sel_pos.is_some() && sel_pos.unwrap() > upd_pos.unwrap() {
        let tmp = stmt.split_off(sel_pos.unwrap());
        upd_stmt = stmt;
        stmt = tmp;
    }

    //let mut consistency = Consistency::Eventual;
    let mut consistency = Consistency::Absolute;
    if upd_stmt.len() > 0 {
        if do_query_test(ts, tc, tr, handle, &consistency, &upd_stmt)
            .await
            .is_none()
        {
            return;
        }
        consistency = Consistency::Absolute;
    }

    let results_opt = do_query_test(ts, tc, tr, handle, &consistency, &stmt).await;
    if results_opt.is_none() {
        return;
    }

    let mut actual_results = results_opt.unwrap();
    //if actual_results.len() == 0 {
    //println!("Query:\n{}\ngot 0 results.", stmt);
    //} else {
    //println!("Query:\n{}\ngot results as follows: {:?}", stmt, actual_results);
    //}

    let totals_match = tc.expect_results.len() == actual_results.len();

    //if totals_match == false {
    //println!("{display_name} got unexpected number of results: expected={} actual={}", tc.expect_results.len(), actual_results.len());
    //tr.failed += 1;
    //return;
    //}

    // These tests have non-deterministic results. We can only check that
    // the number of results is the expected one.
    if totals_match {
        match display_name.as_str() {
            "gb/noidx09.q" | "gb/noidx12.q" | "gb/noidx15.q" | "gb/distinct02.q" => {
                tr.passed += 1;
                return;
            }
            _ => (),
        }
    }

    //if tc.expect_ordered_result {
    if false {
        if totals_match == false {
            println!(
                "FAIL: {display_name} got unexpected number of results: expected={} actual={}",
                tc.expect_results.len(),
                actual_results.len()
            );
            println!("   expected: {:?}", tc.expect_results);
            println!("   actual  : {:?}", actual_results);
            tr.failed += 1;
            return;
        }
        let mut num_not_match: i32 = 0;
        let mut j = 0;
        for json in &tc.expect_results {
            match compare_json_to_mapvalue(json, &actual_results[j]) {
                Ok(v) => {
                    if v == false {
                        num_not_match += 1;
                    }
                }
                Err(e) => {
                    println!("{display_name} error converting expected json to MapValue: {e}");
                    num_not_match += 1;
                }
            }
            j += 1;
        }

        if num_not_match > 0 {
            println!("FAIL: {display_name}: {num_not_match} rows do not match");
            tr.failed += 1;
            return;
        }

        tr.passed += 1;
        return;
    }

    // Check un-ordered results.

    let mut expected_results: Vec<MapValue> = Vec::new();

    for json in &tc.expect_results {
        match json_to_map_value(json) {
            Err(e) => {
                println!(
                    "WARN: {display_name} error converting expected json '{}' to MapValue: {e}",
                    json
                );
                expected_results.push(MapValue::new());
            }
            Ok(exp_mv) => {
                expected_results.push(exp_mv.clone_internal());
            }
        }
    }

    let mut matched: usize = 0;
    for i in 0..actual_results.len() {
        if actual_results[i].len() == 0 {
            continue;
        }
        for j in 0..expected_results.len() {
            if expected_results[j].len() == 0 {
                continue;
            }
            if compare_map_values(&actual_results[i], &expected_results[j], true) == Ordering::Equal
            {
                actual_results[i] = MapValue::new();
                expected_results[j] = MapValue::new();
                matched += 1;
                break;
            }
        }
    }

    if matched == expected_results.len() {
        tr.passed += 1;
        return;
    }

    println!(
        "FAIL: {display_name} got mismatch in unordered results: expected={} actual={}",
        expected_results.len(),
        actual_results.len()
    );
    tr.failed += 1;
    for i in &expected_results {
        if i.len() > 0 {
            println!(" expected: {:?}", i);
        }
    }
    for i in &actual_results {
        if i.len() > 0 {
            println!(" actual: {:?}", i);
        }
    }
}

async fn do_query_test(
    ts: &mut TestSuite,
    tc: &mut TestCase,
    tr: &mut SuiteResults,
    handle: &Handle,
    cons: &Consistency,
    stmt: &str,
) -> Option<Vec<MapValue>> {
    let msg_prefix = format!("Testcase {}/{}: Query(stmt={}) ", ts.name, tc.name, stmt);

    let mut prep_req = QueryRequest::new(&stmt).prepare_only();
    let prep_res = prep_req.execute(handle).await;

    if tc.expect_compile_err {
        match prep_res {
            Ok(_) => {
                println!("FAIL: {} should have failed to compile", msg_prefix);
                tr.failed += 1;
            }
            Err(_e) => {
                //println!("Prepare({}) got expected compile error: {}", stmt, e);
                tr.passed += 1;
            }
        }
        return None;
    }

    if let Err(e) = prep_res {
        // special-case error code for proxy-based tests
        let errmsg = format!("{e}");
        if errmsg.contains("Cannot execute this kind of query in a proxy-based environment") {
            tr.skipped += 1;
            return None;
        }
        println!("FAIL: {} prepare() got unexpected error {}", msg_prefix, e);
        tr.failed += 1;
        return None;
    }

    let ps = prep_res.unwrap().prepared_statement();
    if ps.is_empty() {
        println!(
            "FAIL: {} prepare() did not return a prepared statement",
            msg_prefix
        );
        tr.failed += 1;
        return None;
    }

    let mut query_req = QueryRequest::new_prepared(&ps).consistency(cons);

    let mut vars = get_external_vars(stmt);
    //println!(" vars={:?}\nextvars={:?}", vars, ts.external_vars);
    if vars.len() > 0 {
        // Bind external variables with values.
        //println!("Bind vars: {:?}", vars);
        for (name, value) in &ts.external_vars {
            // The statement contains a declared external variable that
            // has not been bound with a value.
            if let Some(bound) = vars.get_mut(name) {
                *bound = true;
                //println!("SetVariable(name={}, value={:?}", name, value);
                query_req.set_variable(name, value).unwrap();
            }
        }
    }

    let query_res = query_req.execute(handle).await;

    if let Err(e) = query_res {
        if tc.expect_runtime_err == false {
            println!(
                "FAIL: {}/{} query({}) got unexpected error {}",
                ts.name, tc.name, stmt, e
            );
            tr.failed += 1;
            return None;
        }

        if tc.expect_err_messages.len() == 0 {
            //println!("{}/{} query({}) got expected error {}", ts.name, tc.name, stmt, e);
            tr.passed += 1;
            return None;
        }

        // Check error messages.
        let actual_msg = format!("{e}");
        for msg in &tc.expect_err_messages {
            if actual_msg.contains(msg) {
                println!("Query({}) got expected error {}", stmt, e);
                tr.passed += 1;
                return None;
            }
        }

        println!(
            "FAIL: {msg_prefix} should have failed with an error \
            that contains one of the messages: {:?}",
            tc.expect_err_messages
        );
        tr.failed += 1;
        return None;
    }

    // TODO: increment passed here?
    Some(query_res.unwrap().take_rows())
}

// get_external_vars looks up external variables from the SQL statement.
// It returns a map whose keys are variable names (values are initialized with
// a false flag indicating the variable is not bound), and a true flag if
// the statement contains any external variables, otherwise, it returns a nil
// map and a false flag.
fn get_external_vars(stmt: &str) -> HashMap<String, bool> {
    let mut vars = HashMap::new();
    // Look up external variables.
    let mut i: usize = 1000000;
    for (j, c) in stmt.chars().enumerate() {
        if c == '$' {
            i = j;
            continue;
        }

        if i == 1000000 {
            continue;
        }

        if c != '_' && c.is_alphanumeric() == false {
            let name = stmt[i..j].to_string();
            vars.insert(name, false);
            i = 1000000;
        }
    }
    vars
}

fn snsplit(n: usize, s: &str, i: &str) -> Vec<String> {
    let mut arr: Vec<String> = Vec::new();
    for ss in s.splitn(n, i) {
        arr.push(ss.to_string());
    }
    arr
}

async fn create_table(
    _ts: &TestSuite,
    stmt: &str,
    limits: &TableLimits,
    handle: &Handle,
    rl: &mut RateLimiter,
) -> Result<(), Box<dyn Error>> {
    rl.consume_units_with_timeout(1, 60000, true).await?;

    TableRequest::new("")
        .statement(stmt)
        .limits(limits)
        .execute(handle)
        .await?
        .wait_for_completion_ms(handle, 30000, 200)
        .await?;
    Ok(())
}

async fn execute_ddl(
    _ts: &TestSuite,
    stmt: &str,
    handle: &Handle,
    rl: &mut RateLimiter,
) -> Result<(), Box<dyn Error>> {
    rl.consume_units_with_timeout(1, 60000, true).await?;
    SystemRequest::new(stmt)
        .execute(handle)
        .await?
        .wait_for_completion_ms(handle, 30000, 200)
        .await?;
    Ok(())
}

async fn execute_table_ddl(
    _ts: &TestSuite,
    stmt: &str,
    handle: &Handle,
    rl: &mut RateLimiter,
) -> Result<(), Box<dyn Error>> {
    rl.consume_units_with_timeout(1, 60000, true).await?;
    TableRequest::new("")
        .statement(stmt)
        .execute(handle)
        .await?
        .wait_for_completion_ms(handle, 30000, 200)
        .await?;
    Ok(())
}
