//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use chrono::{DateTime, FixedOffset};

use oracle_nosql_rust_sdk::types::NoSQLRow;
//use oracle_nosql_rust_sdk::types::*;
use oracle_nosql_rust_sdk::types::NoSQLColumnFromMapValue;
use oracle_nosql_rust_sdk::types::{Consistency, NoSQLBinary, TableLimits};
use oracle_nosql_rust_sdk::types::{FieldValue, MapValue};
use oracle_nosql_rust_sdk::types::{NoSQLColumnFromFieldValue, NoSQLColumnToFieldValue};
use oracle_nosql_rust_sdk::DeleteRequest;
use oracle_nosql_rust_sdk::GetRequest;
use oracle_nosql_rust_sdk::Handle;
use oracle_nosql_rust_sdk::HandleBuilder;
use oracle_nosql_rust_sdk::ListTablesRequest;
use oracle_nosql_rust_sdk::NoSQLError;
use oracle_nosql_rust_sdk::PutRequest;
use oracle_nosql_rust_sdk::QueryRequest;
use oracle_nosql_rust_sdk::TableRequest;
use oracle_nosql_rust_sdk::WriteMultipleRequest;

use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

fn get_builder() -> Result<HandleBuilder, NoSQLError> {
    Handle::builder()
        // default: try localhost:8080
        .endpoint("http://localhost:8080")?
        //.endpoint("http://localhost:8080/V2/nosql/data")?
        //.endpoint("https://nosql.us-phoenix-1.oci.oc-test.com/V2/nosql/data")?
        .timeout(Duration::from_secs(30))?
        //.cloud_auth_from_file("~/.oci/jpconnel_config")
        //.cloud_auth_from_file("~/nosql/git/nosql-sdk-int/go/testint/oci_config")
        //.cloud_auth_from_instance()
        // this will override any defaults above
        .from_environment()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smoke_test() -> Result<(), Box<dyn Error>> {
    //let _ = env_logger::builder().is_test(true).try_init();
    // Set up a tracing subscriber to see output based on RUST_LOG environment setting
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .with_ansi(false)
        .compact()
        .init();

    let handle = get_builder()?.build().await?;

    TableRequest::new("testusers")
        .statement(
            "create table if not exists testusers (id integer, name string, 
            created timestamp(3), primary key(id))",
        )
        .limits(&TableLimits::provisioned(1000, 1000, 10))
        .execute(&handle)
        .await?
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    let putres = PutRequest::new("testusers")
        .timeout(&Duration::from_millis(3000))
        .value(MapValue::new().i32("id", 10).str("name", "jane"))
        .ttl(&Duration::new(7200, 0))
        .execute(&handle)
        .await?;
    println!("PutResult={:?}", putres);

    //let putres2 = PutRequest::new("testusers")
    let putreq2 = PutRequest::new("testusers")
        .timeout(&Duration::from_millis(3000))
        .value(MapValue::new().i32("id", 10).str("name", "john"))
        .if_version(putres.version().unwrap())
        .ttl(&Duration::new(7200, 0));
    println!("Putreq2={:?}", putreq2);
    let putres2 = putreq2.execute(&handle).await?;
    println!("PutResult2={:?}", putres2);

    let getres = GetRequest::new("testusers")
        .key(MapValue::new().i32("id", 10))
        .consistency(Consistency::Eventual)
        .execute(&handle)
        .await?;
    println!("GetResult={:?}", getres);
    // the record should exist, so the result should have a version
    let getver = getres.version().unwrap().clone();

    for i in 30..35 {
        //let name = format!("name{}", i);
        let name = "somename".to_string();
        let _ = PutRequest::new("testusers")
            .value(MapValue::new().i32("id", i).str("name", name.as_str()))
            .execute(&handle)
            .await?;
    }

    // Insert with named bind variables
    let prep_result = QueryRequest::new(
        "declare $id integer; $name string; insert into testusers(id, name) values($id, $name)",
    )
    .prepare_only()
    .execute(&handle)
    .await?;
    let data = vec!["jane", "john", "jasper"];
    let mut qreq = QueryRequest::new_prepared(&prep_result.prepared_statement());
    for i in 0..data.len() {
        let id = (i as i32) + 1;
        qreq.set_variable("$id", &id)?;
        qreq.set_variable("$name", &data[i])?;
        let result = qreq.execute(&handle).await?;
        println!("Insert result = {:?}", result);
    }

    // Insert with positional bind variables
    let prep1_result = QueryRequest::new("insert into testusers(id, name) values(?, ?)")
        .prepare_only()
        .execute(&handle)
        .await?;
    let data = vec!["jane", "john", "jasper"];
    let mut qreq1 = QueryRequest::new_prepared(&prep1_result.prepared_statement());
    for i in 0..data.len() {
        let id = (i as i32) + 100;
        qreq1.set_variable_by_id(1, &id)?;
        qreq1.set_variable_by_id(2, &data[i])?;
        let result = qreq1.execute(&handle).await?;
        println!("Insert result = {:?}", result);
    }

    // Run a small variety of queries. Note that the QTF tests do an exhaustive set of
    // queries, so this doesn't need to cover a lot. THis is just to see that the base
    // system is functioning properly.
    let qres = QueryRequest::new("select * from testusers")
        .execute(&handle)
        .await?;
    println!("QueryResult: rows={} res={:?}", qres.rows().len(), qres);
    if qres.rows().len() != 12 {
        return Err(format!("Expected 12 rows, actual={}", qres.rows().len())
            .as_str()
            .into());
    }

    let qres1 = QueryRequest::new("select * from testusers order by id")
        .execute(&handle)
        .await?;
    println!("QueryResult1: rows={} res={:?}", qres1.rows().len(), qres1);
    if qres1.rows().len() != 12 {
        return Err(format!("Expected 12 rows, actual={}", qres1.rows().len())
            .as_str()
            .into());
    }

    let mut qreq2 = QueryRequest::new("select id, name, created from testusers order by name");
    let qres2 = qreq2.execute(&handle).await?;
    //println!("QueryRequest2 = {:?}", qreq2);
    println!("QueryResult2: rows={} res={:?}", qres2.rows().len(), qres2);
    if qres2.rows().len() != 12 {
        return Err(format!("Expected 12 rows, actual={}", qres2.rows().len())
            .as_str()
            .into());
    }

    let mut qreq3 = QueryRequest::new("select sum(id), name from testusers group by name");
    let qres3 = qreq3.execute(&handle).await?;
    //println!("QueryRequest3 = {:?}", qreq3);
    println!("QueryResult3: rows={} res={:?}", qres3.rows().len(), qres3);
    if qres3.rows().len() != 4 {
        return Err(format!("Expected 4 rows, actual={}", qres3.rows().len())
            .as_str()
            .into());
    }

    let delres = DeleteRequest::new("testusers", MapValue::new().i32("id", 10))
        .if_version(&getver)
        .execute(&handle)
        .await?;
    println!("delres={:?}", delres);

    let ltres = ListTablesRequest::new()
        .compartment_id("TODO")
        .execute(&handle)
        .await?;
    println!("ltres={:?}", ltres);

    TableRequest::new("testusers")
        .statement("drop table if exists testusers")
        .timeout(&Duration::from_millis(30000))
        .execute(&handle)
        .await?
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    Ok(())
}

// TODO: SystemRequest (create namespace, etc)
// TODO: TableUsageRequest (verify rfc3339 semantics)
// TODO: WriteMultiple with all pass, some pass, all fail, if present, etc.
// TODO: MultiDeleteRequest

#[derive(Default, Debug, NoSQLRow)]
struct Person {
    pub shard: i32,
    #[nosql(type=long, column=id)]
    pub uuid: i64,
    pub name: String,
    pub birth: Option<DateTime<FixedOffset>>,
    pub street: Option<String>,
    pub data: Option<NoSQLBinary>,
    pub city: String,
    pub zip: i32,
    pub numbers: Vec<i64>,
    pub num_map: HashMap<String, i32>,
    //pub complex_map: Option<HashMap<String, Vec<i64>>>,
}

#[tokio::test]
async fn write_multiple_test() -> Result<(), Box<dyn Error>> {
    let handle = get_builder()?.build().await?;

    TableRequest::new("testpeople")
        .statement("create table if not exists testpeople (shard integer, id long, name string, street string, city string, zip integer, birth timestamp(3), numbers array(long), data binary, num_map map(integer), primary key(shard(shard), id))")
        .limits(&TableLimits::provisioned(100, 100, 10))
        .execute(&handle).await?
        .wait_for_completion_ms(&handle, 15000, 500).await?;

    // TODO: write multi, put 50 Persons programmatically
    let mut data: Vec<Person> = Vec::new();
    data.push(Person {
        shard: 1,
        uuid: 123456789,
        name: "John".to_string(),
        street: Some("123 Main Street".to_string()),
        city: "Anytown".to_string(),
        zip: 12345,
        data: None,
        birth: Some(DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00")?),
        num_map: HashMap::from([("cars".to_string(), 1), ("pets".to_string(), 4)]),
        ..Default::default()
    });
    data.push(Person {
        shard: 1,
        uuid: 123456788,
        name: "Jane".to_string(),
        street: Some("321 Main Street".to_string()),
        city: "Anytown".to_string(),
        zip: 12345,
        data: None,
        numbers: vec![12345, 654556578, 43543543543543, 23232],
        birth: None,
        ..Default::default()
    });
    data.push(Person {
        shard: 1,
        uuid: 123456787,
        name: "Joe".to_string(),
        street: None,
        city: "Anytown".to_string(),
        zip: 12345,
        data: None,
        birth: Some(DateTime::parse_from_rfc3339("1999-12-19T16:39:57-08:00")?),
        ..Default::default()
    });

    let res = WriteMultipleRequest::new("testpeople")
        .put(data)?
        .execute(&handle)
        .await;
    println!("write_multi result={:?}", res);

    let mut person: Person = Person::default();
    person.shard = 1;
    person.uuid = 123456788;
    let getres = GetRequest::new("testpeople")
        .row_key(&person)?
        .execute_into(&handle, &mut person)
        .await;
    println!("get into result={:?}", getres);
    println!("Returned row: {:?}", person);

    person.uuid = 1;
    let getres1 = GetRequest::new("testpeople")
        .row_key(&person)?
        .execute_into(&handle, &mut person)
        .await;
    println!("get into result={:?}", getres1);
    println!("Returned row: {:?}", person);

    Ok(())
}

#[derive(Default, Debug, Clone, NoSQLRow)]
struct PortionA {
    #[nosql(column=fielda)]
    pub a: i64,
    #[nosql(column=fieldb)]
    pub b: String,
    #[nosql(column=fieldc)]
    pub c: Option<DateTime<FixedOffset>>,
}

#[derive(Default, Debug, Clone, NoSQLRow)]
struct PortionB {
    #[nosql(column=fieldx)]
    pub x: i32,
    #[nosql(column=fieldy)]
    pub y: Option<String>,
    #[nosql(column=fieldz)]
    pub z: Vec<i32>,
}

#[derive(Default, Debug, NoSQLRow)]
struct ComplexA {
    pub id: i64,
    #[nosql(column=portiona)]
    pub a: Vec<PortionA>,
    #[nosql(column=portionb)]
    pub b: Vec<PortionB>,
}

#[tokio::test]
async fn complex_struct_test() -> Result<(), Box<dyn Error>> {
    let handle = get_builder()?.build().await?;

    TableRequest::new("complexdata")
        .statement(
            "create table if not exists complexdata (id long,
            portiona ARRAY ( RECORD ( fielda LONG, fieldb STRING, fieldc TIMESTAMP(6) ) ),
            portionb ARRAY ( RECORD ( fieldx INTEGER, fieldy STRING, fieldz ARRAY ( INTEGER ) ) ),
            primary key(id))",
        )
        .limits(&TableLimits::provisioned(10, 10, 10))
        .execute(&handle)
        .await?
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    let mut portion_a: Vec<PortionA> = Vec::new();
    portion_a.push(PortionA {
        a: 1000,
        b: "testing".to_string(),
        c: Some(DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00")?),
    });
    let mut portion_b: Vec<PortionB> = Vec::new();
    portion_b.push(PortionB {
        x: 25,
        y: None,
        z: vec![1, 5, 8, 23, 56],
    });
    portion_b.push(PortionB {
        x: 26,
        y: Some("foo".to_string()),
        z: vec![27, 59],
    });
    let mut data: Vec<ComplexA> = Vec::new();
    data.push(ComplexA {
        id: 1,
        a: portion_a.clone(),
        b: portion_b.clone(),
    });

    let res = WriteMultipleRequest::new("complexdata")
        .put(data)?
        .execute(&handle)
        .await;
    println!("write_multi result={:?}", res);

    let mut a = ComplexA {
        id: 1,
        ..Default::default()
    };
    let getres = GetRequest::new("complexdata")
        .row_key(&a)?
        .execute_into(&handle, &mut a)
        .await;
    println!("get into result={:?}", getres);
    println!("Returned row: {:?}", a);

    a.id = 2;
    let getres1 = GetRequest::new("complexdata")
        .row_key(&a)?
        .execute_into(&handle, &mut a)
        .await;
    println!("get into result={:?}", getres1);
    println!("Returned row: {:?}", a);

    Ok(())
}

#[tokio::test]
async fn complex_json_test() -> Result<(), Box<dyn Error>> {
    let handle = get_builder()?.build().await?;

    TableRequest::new("complexjson")
        .statement(
            "create table if not exists complexjson (id long, complex_data JSON,
            primary key(id))",
        )
        .limits(&TableLimits::provisioned(10, 10, 10))
        .execute(&handle)
        .await?
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    let mut portion_a: Vec<PortionA> = Vec::new();
    portion_a.push(PortionA {
        a: 1000,
        b: "testing".to_string(),
        c: Some(DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00")?),
    });
    let mut portion_b: Vec<PortionB> = Vec::new();
    portion_b.push(PortionB {
        x: 25,
        y: None,
        z: vec![1, 5, 8, 23, 56],
    });
    portion_b.push(PortionB {
        x: 26,
        y: Some("foo".to_string()),
        z: vec![27, 59],
    });
    let data = ComplexA {
        id: 1,
        a: portion_a.clone(),
        b: portion_b.clone(),
    };

    let res = PutRequest::new("complexjson")
        .value(
            MapValue::new()
                .column("id", 1i32)
                .column("complex_data", data),
        )
        .execute(&handle)
        .await;
    println!("put result={:?}", res);

    let getres = GetRequest::new("complexjson")
        .key(MapValue::new().column("id", 1i32))
        .execute(&handle)
        .await?;
    println!("get result={:?}", getres);
    println!("Returned row: {:?}", getres.row());
    if let Some(row) = getres.row() {
        if let Some(data) = row.get_map("complex_data") {
            let mut a = ComplexA {
                id: 1,
                ..Default::default()
            };
            a.from_map_value(data)?;
            println!("Returned data = {:?}", a);
        } else {
            return Err("No 'complex_data' filed found in returned row!".into());
        }
    }

    Ok(())
}

#[derive(Default, Debug, NoSQLRow)]
struct ComplexData {
    pub id: i64,
    #[nosql(column=complex_data)]
    pub data: Option<ComplexA>,
}

#[tokio::test]
async fn complex_json_test2() -> Result<(), Box<dyn Error>> {
    let handle = get_builder()?.build().await?;

    TableRequest::new("complexjson2")
        .statement(
            "create table if not exists complexjson2 (id long, complex_data JSON,
            primary key(id))",
        )
        .limits(&TableLimits::provisioned(10, 10, 10))
        .execute(&handle)
        .await?
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    let mut portion_a: Vec<PortionA> = Vec::new();
    portion_a.push(PortionA {
        a: 1000,
        b: "testing".to_string(),
        c: Some(DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00")?),
    });
    let mut portion_b: Vec<PortionB> = Vec::new();
    portion_b.push(PortionB {
        x: 25,
        y: None,
        z: vec![1, 5, 8, 23, 56],
    });
    portion_b.push(PortionB {
        x: 26,
        y: Some("foo".to_string()),
        z: vec![27, 59],
    });
    let complex_data = ComplexData {
        id: 10,
        data: Some(ComplexA {
            id: 1,
            a: portion_a.clone(),
            b: portion_b.clone(),
        }),
    };

    let res = PutRequest::new("complexjson2")
        .put(complex_data)?
        .execute(&handle)
        .await;
    println!("put result={:?}", res);

    let mut a = ComplexData {
        id: 10,
        ..Default::default()
    };
    let getres = GetRequest::new("complexjson2")
        .row_key(&a)?
        .execute_into(&handle, &mut a)
        .await;
    println!("get result={:?}", getres);
    println!("Returned row: {:?}", a);

    Ok(())
}

// NOTE: this will fail until we resolve a way to have Timestamps in json collections.
// Maybe a #[NoSQL(type=string)] attribute could resolve it?
#[tokio::test]
async fn json_collection_test() -> Result<(), Box<dyn Error>> {
    let handle = get_builder()?.build().await?;

    TableRequest::new("noschema")
        .statement(
            "create table if not exists noschema (id long,
            primary key(id)) as json collection",
        )
        .limits(&TableLimits::provisioned(10, 10, 10))
        .execute(&handle)
        .await?
        .wait_for_completion_ms(&handle, 15000, 500)
        .await?;

    let mut portion_a: Vec<PortionA> = Vec::new();
    portion_a.push(PortionA {
        a: 1000,
        b: "testing".to_string(),
        c: Some(DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00")?),
    });
    let mut portion_b: Vec<PortionB> = Vec::new();
    portion_b.push(PortionB {
        x: 25,
        y: None,
        z: vec![1, 5, 8, 23, 56],
    });
    portion_b.push(PortionB {
        x: 26,
        y: Some("foo".to_string()),
        z: vec![27, 59],
    });
    let data = ComplexA {
        id: 100,
        a: portion_a.clone(),
        b: portion_b.clone(),
    };

    let res = PutRequest::new("noschema")
        .put(data)?
        .execute(&handle)
        .await;
    println!("put result={:?}", res);

    let mut a = ComplexData {
        id: 100,
        ..Default::default()
    };
    GetRequest::new("noschema")
        .row_key(&a)?
        .execute_into(&handle, &mut a)
        .await?;
    println!("Returned data = {:?}", a);

    Ok(())
}
