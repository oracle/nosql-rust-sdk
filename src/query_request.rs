//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::error::ia_err;
use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode;
use crate::handle::Handle;
use crate::handle::SendOptions;
use crate::nson::*;
use crate::plan_iter::{deserialize_plan_iter, PlanIterKind, PlanIterState};
use crate::prepared_statement::{PreparedStatement, PreparedStatementBranch};
use crate::reader::Reader;
use crate::receive_iter::ReceiveIterData;
use crate::stats::QueryStatsMetadata;
use crate::types::NoSQLColumnToFieldValue;
use crate::types::{Capacity, Consistency, FieldType, FieldValue, MapValue, OpCode, TopologyInfo};
use crate::writer::Writer;

use std::collections::HashMap;
use std::result::Result;
use std::time::Duration;
use tracing::trace;

const DRIVER_QUERY_VERSION: i32 = 6;

fn is_retryable_query_statement(statement: &str) -> bool {
    matches!(
        first_sql_operation_keyword(statement).as_deref(),
        Some("select")
    )
}

fn first_sql_operation_keyword(statement: &str) -> Option<String> {
    let (keyword, mut remaining) = split_leading_sql_keyword(statement)?;
    let keyword = keyword.to_ascii_lowercase();
    if keyword != "declare" {
        return Some(keyword);
    }

    loop {
        let declaration_end = remaining.find(';')?;
        remaining = trim_sql_leading_noise(&remaining[declaration_end + 1..]);
        if remaining.starts_with('$') {
            continue;
        }
        return first_sql_operation_keyword(remaining);
    }
}

fn split_leading_sql_keyword(statement: &str) -> Option<(&str, &str)> {
    let statement = trim_sql_leading_noise(statement);
    let end = statement
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(statement.len());
    if end == 0 {
        return None;
    }
    Some((&statement[..end], &statement[end..]))
}

fn trim_sql_leading_noise(mut statement: &str) -> &str {
    loop {
        let trimmed = statement.trim_start();
        if let Some(comment) = trimmed.strip_prefix("--") {
            if let Some(end) = comment.find('\n') {
                statement = &comment[end + 1..];
                continue;
            }
            return "";
        }
        if let Some(comment) = trimmed.strip_prefix("/*") {
            if let Some(end) = comment.find("*/") {
                statement = &comment[end + 2..];
                continue;
            }
            return "";
        }
        return trimmed;
    }
}

/// Encapsulates a SQL query of a NoSQL Database table.
///
/// A query may be either a string query
/// statement or a prepared query, which may include bind variables.
/// A query request cannot have both a string statement and prepared query, but
/// it must have one or the other.
///
/// See the [SQL for NoSQL Database Guide](https://docs.oracle.com/en/database/other-databases/nosql-database/24.1/sqlfornosql/introduction-sql.html) for details on creating and using queries.
///
/// ## Simple Example
/// Here is a simple example of running a query that will return every row in a table named `users`:
///
/// ```no_run
/// # use oracle_nosql_rust_sdk::{Handle, QueryRequest};
/// # #[tokio::main]
/// # pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let handle = Handle::builder().build().await?;
/// let results = QueryRequest::new("select * from users")
///               .execute(&handle).await?;
/// for row in results.rows() {
///     println!("Row = {}", row);
/// }
/// # Ok(())
/// # }
/// ```
///
/// For performance reasons, prepared queries are preferred for queries that may
/// be reused. Prepared queries bypass compilation of the query. They also allow
/// for parameterized queries using bind variables.
#[derive(Default, Debug)]
pub struct QueryRequest {
    pub(crate) prepare_only: bool,
    //pub(crate) limit: u32,
    pub(crate) max_read_kb: u32,
    pub(crate) max_write_kb: u32,
    pub(crate) consistency: Consistency,
    pub(crate) timeout: Option<Duration>,
    pub(crate) compartment_id: String,

    // max_memory_consumption specifies the maximum amount of memory in bytes that
    // may be consumed by the query at the client for operations such as
    // duplicate elimination (which may be required due to the use of an index
    // on an array or map) and sorting. Such operations may consume a lot of
    // memory as they need to cache the full result set or a large subset of
    // it at the client memory.
    //
    // The default value is 1GB (1,000,000,000).
    // TODO pub max_memory_consumption: i64,

    // Durability is currently only used in On-Prem installations.
    // This setting only applies if the query modifies
    // a row using an INSERT, UPSERT, or DELETE statement. If the query is
    // read-only it is ignored.
    // Added in SDK Version 1.4.0
    // TODO Durability types.Durability

    // private fields: driver and RCB data

    // statement specifies a query statement.
    statement: Option<String>,

    get_query_plan: bool,
    get_query_schema: bool,

    // prepared_statement specifies the prepared query statement.
    pub(crate) prepared_statement: PreparedStatement,

    // shortcuts
    has_driver: bool,

    pub(crate) is_done: bool,

    // created/used by internal iterators
    is_internal: bool,

    // reached_limit indicates if the query execution reached the size-based or
    // number-based limit. If so, query execution must stop and a batch of
    // results (potentially empty) must be returned to the application.
    pub(crate) reached_limit: bool,

    pub(crate) consumed_capacity: Capacity,

    // memory_consumption represents the amount of memory in bytes that were
    // consumed by the query at the client for operations such as duplicate
    // elimination and sorting.
    // TODO pub memory_consumption: i64,

    // sql_hash_tag is a portion of the hash value of SQL text, used as a tag
    // for query tracing.
    pub(crate) sql_hash_tag: Vec<u8>,

    // err represents a non-retryable error returned by a query batch.
    err: Option<NoSQLError>,

    pub(crate) continuation_key: Option<Vec<u8>>,

    pub(crate) shard_id: i32,

    // total number of batches executed
    pub(crate) batch_counter: i32,

    // for "advanced" queries using plan iterators
    pub(crate) num_registers: i32,
    pub(crate) registers: Vec<FieldValue>,

    pub(crate) topology_info: TopologyInfo,

    // Active UNION branch for internal ReceiveIter requests. When set, the
    // copied internal QueryRequest uses the matching branch prepared query.
    active_query_branch: Option<usize>,
}

/// Struct representing the result of a query operation.
#[derive(Default, Debug)]
pub struct QueryResult {
    pub(crate) rows: Vec<MapValue>,
    pub(crate) prepared_statement: PreparedStatement,
    pub(crate) consumed: Capacity,
    // TODO: stats, consumed, etc.
}

impl QueryResult {
    /// Get the query result rows, if any.
    ///
    /// If the query returned no rows, this will return a reference to an empty vector.
    /// Otherwise, it will return a reference to the rows in the order specified by the query.
    pub fn rows(&self) -> &Vec<MapValue> {
        &self.rows
    }
    /// Take the query result rows, setting the result back to an empty vector.
    ///
    /// If the query returned no rows, this will return an empty vector.
    /// Otherwise, it will return the rows in the vector, giving the ownership
    /// of the rows to the caller.
    pub fn take_rows(&mut self) -> Vec<MapValue> {
        std::mem::take(&mut self.rows)
    }
    /// Get the prepared statement after execution of a query.
    ///
    /// The prepared statement can then be used in subsequent query requests, saving the
    /// extra step of preparing each query again.
    pub fn prepared_statement(&self) -> PreparedStatement {
        let mut ps = self.prepared_statement.clone();
        let _ = ps.reset();
        ps
    }
    /// Return the total capacity that was consumed during the execution of the query.
    ///
    /// This is only relevant for NoSQL Cloud operation. It returns a [`Capacity`] struct which
    /// contains the total read units, read KB, and write units used by the query execution.
    pub fn consumed(&self) -> Capacity {
        self.consumed.clone()
    }
}

impl QueryRequest {
    /// Create a new QueryRequest from a SQL query string.
    ///
    /// While this struct is named `QueryRequest`, the SQL supplied to it does not
    /// necessarily have to be a `SELECT` query. It could also be one of `INSERT`, `UPDATE`,
    /// or `DELETE`.
    ///
    /// See the [SQL for NoSQL Database Guide](https://docs.oracle.com/en/database/other-databases/nosql-database/24.1/sqlfornosql/introduction-sql.html) for details on creating and using queries.
    ///
    /// Note: this request should not be used for DDL statements (those that create or modify tables or indexes, such as `CREATE TABLE`). For DDL statements, use [`TableRequest`](crate::TableRequest) instead.
    ///
    pub fn new(statement: &str) -> Self {
        QueryRequest {
            statement: Some(statement.to_string()),
            shard_id: -1,
            ..Default::default()
        }
    }

    /// Create a new QueryRequest from a previously prepared query statement.
    ///
    /// Use of this method is recommended when executing the same type of query multiple
    /// times with different values for parameters. Doing so will save resources by not
    /// re-preparing the query on every execution.
    ///
    /// To set bind variables for query execution, first create the request with this method,
    /// then call [`QueryRequest::set_variable()`] for all desired bind variables. Then execute the
    /// query with [`QueryRequest::execute()`].
    pub fn new_prepared(prepared_statement: &PreparedStatement) -> Self {
        let ti: TopologyInfo;
        if let Some(t) = &prepared_statement.topology_info {
            ti = t.clone();
        } else {
            ti = TopologyInfo::default();
            //panic!(
            //"Invalid prepared statement passed to new_prepared! Missing toploogy info. ps={:?}",
            //prepared_statement
            //);
        }
        QueryRequest {
            prepared_statement: prepared_statement.clone(),
            shard_id: -1,
            topology_info: ti,
            ..Default::default()
        }
    }

    /// Specify that this query execution should only prepare the query.
    ///
    /// Setting this value to true and then calling [`QueryRequest::execute()`]
    /// will result in only the query being prepared, and no result rows being returned.
    /// The prepared statement can then be retrieved using [`QueryResult::prepared_statement()`]
    /// and can be used in subsequent query calls using [`QueryRequest::new_prepared()`].
    pub fn prepare_only(mut self) -> Self {
        self.prepare_only = true;
        self
    }

    /// Specify whether a prepare-only request should return the string query plan.
    pub fn get_query_plan(mut self, get_query_plan: bool) -> Self {
        self.get_query_plan = get_query_plan;
        self
    }

    /// Specify whether a prepare-only request should return the JSON result schema.
    pub fn get_query_schema(mut self, get_query_schema: bool) -> Self {
        self.get_query_schema = get_query_schema;
        self
    }

    /// Specify the timeout value for the request.
    ///
    /// This is optional.
    /// If set, it must be greater than or equal to 1 millisecond, otherwise an
    /// IllegalArgument error will be returned.
    /// If not set, the default timeout value configured for the [`Handle`](crate::HandleBuilder::timeout()) is used.
    pub fn timeout(mut self, t: &Duration) -> Self {
        self.timeout = Some(t.clone());
        self
    }

    /// Cloud Service only: set the name or id of a compartment to be used for this operation.
    ///
    /// If the associated handle authenticated as an Instance Principal, this value must be an OCID.
    /// In all other cases, the value may be specified as either a name (or path for nested compartments) or as an OCID.
    ///
    /// If no compartment is given, the default compartment id for the handle is used. If that value was
    /// not specified, the root compartment of the tenancy will be used.
    pub fn compartment_id(mut self, compartment_id: &str) -> Self {
        self.compartment_id = compartment_id.to_string();
        self
    }

    // Specify a limit on number of items returned by the operation.
    //
    // This allows an operation to return less than the default amount of data.
    //pub fn limit(mut self, l: u32) -> Self {
    //self.limit = l;
    //self
    //}

    /// Specify the desired consistency policy for the request.
    ///
    /// If not set, the default consistency of [`Consistency::Eventual`] is used.
    pub fn consistency(mut self, c: &Consistency) -> Self {
        self.consistency = c.clone();
        self
    }

    /// Specify the limit on the total data read during a single batch operation, in KB.
    ///
    /// For cloud service, this value can only reduce the system defined limit.
    /// An attempt to increase the limit beyond the system defined limit will
    /// cause an IllegalArgument error. This limit is independent of read units
    /// consumed by the operation.
    ///
    /// It is recommended that for tables with relatively low provisioned read
    /// throughput that this limit be set to less than or equal to one half
    /// of the provisioned throughput in order to reduce the possibility of throttling
    /// errors.
    pub fn max_read_kb(mut self, max: u32) -> Self {
        self.max_read_kb = max;
        self
    }

    /// Specify the limit on the total data written during a single batch operation, in KB.
    ///
    /// For cloud service, this value can only reduce the system defined limit.
    /// An attempt to increase the limit beyond the system defined limit will
    /// cause an IllegalArgument error. This limit is independent of write units
    /// consumed by the operation.
    ///
    /// This limit is independent of write units consumed by the operation.
    pub fn max_write_kb(mut self, max: u32) -> Self {
        self.max_write_kb = max;
        self
    }

    fn rate_limit_table_name(&self) -> String {
        self.prepared_statement
            .table_name
            .clone()
            .unwrap_or_default()
    }

    fn does_writes_for_rate_limiting(&self) -> bool {
        self.prepared_statement.does_writes()
    }

    fn is_retryable(&self) -> bool {
        if self.prepare_only {
            return true;
        }
        if !self.prepared_statement.is_empty() {
            return !self.prepared_statement.does_writes();
        }
        self.statement
            .as_deref()
            .map(is_retryable_query_statement)
            .unwrap_or(false)
    }

    // used by ext_var_ref_iter
    pub(crate) fn get_external_var(&self, id: i32) -> Option<&FieldValue> {
        if self.prepared_statement.is_empty() {
            return None;
        }
        self.prepared_statement.get_variable_by_id(id)
    }

    // note private
    async fn get_results(
        &mut self,
        handle: &Handle,
        results: &mut Vec<MapValue>,
    ) -> Result<(), NoSQLError> {
        // this is where everything happens

        if let Some(e) = &self.err {
            return Err(e.clone());
        }

        if self.prepare_only == true || self.prepared_statement.is_simple() {
            // results already fetched from nson_deserialize()
            //println!("get_results: prepare or simple: returning");
            return Ok(());
        }

        //let driver_plan = &mut self.prepared_statement.driver_query_plan;
        let mut driver_plan = std::mem::take(&mut self.prepared_statement.driver_query_plan);

        if driver_plan.get_state() == PlanIterState::Uninitialized {
            //println!("get_results: initializing driver plan");
            self.reached_limit = false;
            //self.memory_consumption = 0;
            self.consumed_capacity = Capacity::default();
            self.sql_hash_tag = Default::default();

            self.consumed_capacity.read_kb += 1; // prep cost
            self.consumed_capacity.read_units += 1; // prep cost
            driver_plan.open(self, handle)?;
        }

        let mut more;
        loop {
            //println!("get_results: calling driver_plan.next()");
            more = driver_plan.next(self, handle).await?;
            if more == false {
                //println!("get_results: no more results: breaking");
                break;
            }
            //println!("get_results: pushing 1 result");
            results.push(driver_plan.get_result(self).get_map_value()?);
            //if self.limit > 0 && results.len() >= self.limit as usize {
            //println!(
            //"get_results: reached limit: results_size={}, limit={}",
            //results.len(),
            //self.limit
            //);
            //self.reached_limit = true;
            //break;
            //}
        }

        self.prepared_statement.driver_query_plan = driver_plan;

        if more {
            // non-advanced queries just need Some/None, value not used
            self.continuation_key = Some(Vec::new());
            self.is_done = false;
        } else {
            if self.reached_limit {
                // there is more to do, but we reached a limit
                self.continuation_key = Some(Vec::new());
                self.reached_limit = false;
                self.is_done = false;
            } else {
                self.continuation_key = None;
                self.is_done = true;
            }
        }

        Ok(())
    }

    pub(crate) fn copy_for_internal(&self) -> Self {
        if self.prepared_statement.is_empty() {
            panic!("prepared statement is empty in copy_for_internal");
        }
        QueryRequest {
            is_internal: true,
            max_read_kb: self.max_read_kb,
            max_write_kb: self.max_write_kb,
            consistency: self.consistency,
            compartment_id: self.compartment_id.clone(),
            prepared_statement: self
                .prepared_statement
                .copy_for_internal(self.active_query_branch),
            shard_id: self.shard_id,
            //limit: self.limit,
            // purposefully not copying registers
            num_registers: -1,
            timeout: self.timeout.clone(),
            topology_info: self.topology_info.clone(),
            ..Default::default()
        }
    }

    pub(crate) fn set_active_query_branch(&mut self, branch: Option<usize>) -> Option<usize> {
        let previous = self.active_query_branch;
        self.active_query_branch = branch;
        previous
    }

    pub(crate) fn reset(&mut self) -> Result<(), NoSQLError> {
        self.is_done = false;
        self.reached_limit = false;
        self.batch_counter = 0;
        self.consumed_capacity = Capacity::default();
        // clear prepared statement iterators
        self.prepared_statement.reset()
    }

    fn query_stats_metadata(&self) -> QueryStatsMetadata {
        let is_prepared = !self.prepared_statement.is_empty();
        let query = self
            .statement
            .clone()
            .or_else(|| {
                let sql_text = self.prepared_statement.sql_text();
                if sql_text.is_empty() {
                    None
                } else {
                    Some(sql_text.to_string())
                }
            })
            .unwrap_or_else(|| "null".to_string());

        QueryStatsMetadata::new(
            query,
            !is_prepared,
            is_prepared && self.prepared_statement.is_simple_query(),
            is_prepared && self.prepared_statement.does_writes(),
            if is_prepared {
                self.prepared_statement.print_driver_plan()
            } else {
                None
            },
        )
    }

    fn remember_sql_text_for_stats(&mut self) {
        if let Some(statement) = &self.statement {
            self.prepared_statement.set_sql_text(statement);
        }
    }

    fn validate_query_input_for_stats(&self) -> Result<(), NoSQLError> {
        if self.statement.is_none() && self.prepared_statement.is_empty() {
            return ia_err!("no statement or prepared statement");
        }
        Ok(())
    }

    /// Set a named bind variable for execution of a prepared query.
    ///
    /// See [`PreparedStatement`] for an example of using this method.
    pub fn set_variable(
        &mut self,
        name: &str,
        value: &impl NoSQLColumnToFieldValue,
    ) -> Result<(), NoSQLError> {
        if self.prepared_statement.is_empty() {
            return ia_err!("cannot set bind variables: no prepared statement in QueryRequest");
        }
        let fv = value.to_field_value();
        self.prepared_statement.set_variable(name, &fv)
    }

    /// Set a positional bind variable for execution of a prepared query.
    ///
    /// This is similar to [`set_variable()`](QueryRequest::set_variable()) but uses integer-based positional parameters:
    /// ```no_run
    /// # use oracle_nosql_rust_sdk::{Handle, QueryRequest, NoSQLColumnToFieldValue};
    /// # #[tokio::main]
    /// # pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let handle = Handle::builder().build().await?;
    /// let prep_result = QueryRequest::new("insert into testusers(id, name) values(?, ?)")
    ///     .prepare_only()
    ///     .execute(&handle)
    ///     .await?;
    /// let data = vec!["jane", "john", "jasper"];
    /// let mut qreq = QueryRequest::new_prepared(&prep_result.prepared_statement());
    /// for i in 0..data.len() {
    ///     let id = (i as i32) + 100;
    ///     qreq.set_variable_by_id(1, &id)?;
    ///     qreq.set_variable_by_id(2, &data[i])?;
    ///     let result = qreq.execute(&handle).await?;
    ///     println!("Insert result = {:?}", result);
    /// }
    /// # Ok(())
    /// # }
    pub fn set_variable_by_id(
        &mut self,
        id: i32,
        value: &impl NoSQLColumnToFieldValue,
    ) -> Result<(), NoSQLError> {
        if self.prepared_statement.is_empty() {
            return ia_err!("cannot set bind variables: no prepared statement in QueryRequest");
        }
        let fv = value.to_field_value();
        self.prepared_statement.set_variable_by_id(id, &fv)
    }

    /// Execute the query to full completion.
    ///
    /// This is the preferred method for execution of a query. Internally, this method will loop
    /// calling `execute_batch()` until all results are returned and all post-processing (sorting,
    /// grouping, aggregations, etc) are complete.
    ///
    /// If the query has no rows to return, [`QueryResult::rows()`] will return an empty vector.
    /// Otherwise it will return a vector of
    /// [`MapValue`](crate::types::MapValue) structs in the order specified by the
    /// query statement.
    pub async fn execute(&mut self, h: &Handle) -> Result<QueryResult, NoSQLError> {
        let mut iter_data = ReceiveIterData::default();
        let mut results: Vec<MapValue> = Vec::new();
        self.reset()?;
        let mut observe_logical_query = !self.is_internal && !self.prepare_only;
        while self.is_done == false {
            //println!("execute_internal doing next batch");
            self.execute_batch_internal(h, &mut results, &mut iter_data, observe_logical_query)
                .await?;
            observe_logical_query = false;
            self.batch_counter += 1;
            if self.batch_counter > 10000 {
                panic!("Batch_internal infinite loop detected: self={:?}", self);
            }
        }

        if self.prepared_statement.is_empty() {
            panic!("empty prepared statement after execute!");
        }

        // TODO: retries, stats, etc
        let mut qres = QueryResult {
            prepared_statement: self.prepared_statement.clone(),
            consumed: self.consumed_capacity.clone(),
            rows: results,
        };
        let _ = qres.prepared_statement.reset();
        Ok(qres)
    }

    /// Execute one batch of a query.
    ///
    /// This will execute at most one round-trip to the server. It should be called in a loop
    /// until `is_done()` returns `true`. Note that any one batch execution may not set any results,
    /// since some queries require many server round trips to finish (sorting, for example).
    ///
    /// It is recommended to use [`execute()`](QueryRequest::execute()) instead of this method.
    /// *This method may be deprecated in future releases*.
    pub async fn execute_batch(
        &mut self,
        handle: &Handle,
        results: &mut Vec<MapValue>,
    ) -> Result<(), NoSQLError> {
        let mut _data = ReceiveIterData::default();
        let observe_logical_query = !self.is_internal && !self.prepare_only;
        self.execute_batch_internal(handle, results, &mut _data, observe_logical_query)
            .await
    }

    /// Determine if the query is complete.
    ///
    /// If using [`QueryRequest::execute_batch()`] in a loop, this method determines when
    /// to terminate the loop, specifying that no more results exist for this query execution.
    /// This is only necessary if executing queries in batch looping mode.
    pub fn is_done(&self) -> bool {
        self.is_done
    }

    pub(crate) async fn execute_batch_internal(
        &mut self,
        handle: &Handle,
        results: &mut Vec<MapValue>,
        iter_data: &mut ReceiveIterData,
        observe_logical_query: bool,
    ) -> Result<(), NoSQLError> {
        trace!(
            "EBI: batch_counter={} num_results={}",
            self.batch_counter,
            results.len()
        );

        self.reached_limit = false;
        if observe_logical_query {
            self.validate_query_input_for_stats()?;
            // Java records the logical query call after local validation but
            // before the request goes on the wire, so network failures still
            // count while malformed local requests do not.
            handle
                .inner
                .stats_control
                .observe_query(self.query_stats_metadata());
        }

        // internal queries do not use plan iterators/etc - they just return plain results.
        if self.is_internal == false {
            /*
             * The following "if" may be true for advanced queries only. For
             * such queries, the "if" will be true (i.e., the QueryRequest will
             * be bound with a QueryDriver) if and only if this is not the 1st
             * execute() call for this query. In this case we just return a new,
             * empty QueryResult. Actual computation of a result batch will take
             * place when the app calls getResults() on the QueryResult.
             */
            if self.has_driver {
                //trace("QueryRequest has QueryDriver", 2);
                return self.get_results(handle, results).await;
            }

            /*
             * If it is an advanced query and we are here, then this must be
             * the 1st execute() call for the query. If the query has been
             * prepared before, we create a QueryDriver and bind it with the
             * QueryRequest. Then, we create and return an empty QueryResult.
             * Actual computation of a result batch will take place when the
             * app calls getResults() on the QueryResult.
             */
            if self.prepared_statement.is_empty() == false
                && self.prepared_statement.is_simple() == false
            {
                //trace("QueryRequest has no QueryDriver, but is prepared", 2);
                self.num_registers = self.prepared_statement.num_registers;
                self.registers = Vec::new();
                for _i in 0..self.num_registers {
                    self.registers.push(FieldValue::Uninitialized);
                }
                self.has_driver = true;
                return self.get_results(handle, results).await;
            }

            /*
             * If we are here, then this is either (a) a simple query or (b) an
             * advanced query that has not been prepared already, which also
             * implies that this is the 1st execute() call on this query. For
             * a non-prepared advanced query, the effect of this 1st execute()
             * call is to send the query to the proxy for compilation, get back
             * the prepared query, but no query results, create a QueryDriver,
             * and bind it with the QueryRequest (see
             * QueryRequestSerializer.deserialize()), and return an empty
             * QueryResult.
             * a non-prepared simple query will return results.
             */
            //trace("QueryRequest has no QueryDriver and is not prepared", 2);
            //self.batch_counter += 1;
        }

        let mut w: Writer = Writer::new();
        w.write_i16(handle.inner.serial_version);
        let timeout = handle.get_timeout(&self.timeout);
        self.serialize_internal(&mut w, &timeout)?;
        let consumed_before = self.consumed_capacity;
        let mut opts = SendOptions {
            timeout: timeout,
            retryable: self.is_retryable(),
            request_name: if self.prepare_only {
                "Prepare"
            } else {
                "Query"
            },
            // A prepare-only request contributes to aggregate Prepare stats,
            // but Java does not create an ALL-profile query entry for it.
            // Non-internal Query requests carry metadata that links each HTTP
            // request back to the logical query entry.
            query_stats: if self.is_internal || self.prepare_only {
                None
            } else {
                Some(self.query_stats_metadata())
            },
            compartment_id: self.compartment_id.clone(),
            table_name: self.rate_limit_table_name(),
            does_reads: true,
            does_writes: self.does_writes_for_rate_limiting(),
            ..Default::default()
        };
        let mut r = handle.send_and_receive(w, &mut opts).await?;
        self.continuation_key = None;
        self.nson_deserialize(&mut r, results, iter_data)?;
        let batch_consumed = self.consumed_capacity.delta_since(&consumed_before);
        let table_name = self.rate_limit_table_name();
        if !table_name.is_empty() {
            handle
                .consume_rate_limited_capacity(&mut opts, &table_name, &batch_consumed)
                .await;
        }
        self.remember_sql_text_for_stats();
        if !self.is_internal && !self.prepare_only {
            // Response metadata can fill in query attributes that are only
            // known after prepare/deserialize, without incrementing the
            // logical query count again.
            handle
                .inner
                .stats_control
                .observe_query_metadata(self.query_stats_metadata());
        }
        if self.continuation_key.is_none() {
            trace!("continuation key is None, setting is_done");
            self.is_done = true;
        }
        Ok(())
    }

    fn serialize_internal(&self, w: &mut Writer, timeout: &Duration) -> Result<(), NoSQLError> {
        let mut ns = NsonSerializer::start_request(w);
        ns.start_header();
        if self.prepare_only {
            ns.write_header(OpCode::Prepare, timeout, "");
        } else {
            ns.write_header(OpCode::Query, timeout, "");
        }
        ns.end_header();
        ns.start_payload();

        //TODO writeConsistency(ns, rq.getConsistency());
        //if (rq.getDurability() != null) {
        //writeMapField(ns, DURABILITY,
        //getDurability(rq.getDurability()));
        //}

        if self.max_read_kb > 0 {
            ns.write_i32_field(MAX_READ_KB, self.max_read_kb as i32);
        }
        if self.max_write_kb > 0 {
            ns.write_i32_field(MAX_WRITE_KB, self.max_write_kb as i32);
        }
        //if self.limit > 0 {
        //ns.write_i32_field(NUMBER_LIMIT, self.limit as i32);
        //}

        //writeMapFieldNZ(ns, TRACE_LEVEL, rq.getTraceLevel());
        //if (rq.getTraceLevel() > 0) {
        //writeMapField(ns, TRACE_AT_LOG_FILES, rq.getLogFileTracing());
        //writeMapField(ns, BATCH_COUNTER, rq.getBatchCounter());

        ns.write_i32_field(QUERY_VERSION, DRIVER_QUERY_VERSION);
        if self.prepare_only {
            if self.get_query_plan {
                ns.write_bool_field(GET_QUERY_PLAN, true);
            }
            if self.get_query_schema {
                ns.write_bool_field(GET_QUERY_SCHEMA, true);
            }
        }
        if self.prepared_statement.is_empty() == false {
            ns.write_bool_field(IS_PREPARED, true);
            ns.write_bool_field(IS_SIMPLE_QUERY, self.prepared_statement.is_simple());
            ns.write_binary_field(PREPARED_QUERY, &self.prepared_statement.statement);
            if self.prepared_statement.data.bind_variables.len() > 0 {
                ns.start_array(BIND_VARIABLES);
                for (k, v) in &self.prepared_statement.data.bind_variables {
                    ns.start_map("");
                    trace!(" BIND: name={} value={:?}", k, v);
                    ns.write_string_field(NAME, k);
                    ns.write_field(VALUE, v);
                    ns.end_map("");
                    ns.incr_size(1);
                }
                ns.end_array(BIND_VARIABLES);
            }
        } else {
            if let Some(s) = &self.statement {
                ns.write_string_field(STATEMENT, &s);
            } else {
                return ia_err!("no statement or prepared statement");
            }
        }
        if let Some(ck) = &self.continuation_key {
            if ck.len() > 0 {
                ns.write_binary_field(CONTINUATION_KEY, ck);
                //println!("Wrote {} byte continuation key", ck.len());
            }
        }

        //writeLongMapFieldNZ(ns, SERVER_MEMORY_CONSUMPTION,
        //rq.getMaxServerMemoryConsumption());
        //writeMathContext(ns, rq.getMathContext());

        if self.shard_id > -1 {
            //println!("Q: SHARD_ID={}", self.shard_id);
            ns.write_i32_field(SHARD_ID, self.shard_id);
        }
        //if (queryVersion >= QueryDriver.QUERY_V4) {
        //if (rq.getQueryName() != null) {
        //writeMapField(ns, QUERY_NAME, rq.getQueryName());
        //}
        //if (rq.getVirtualScan() != null) {
        //writeVirtualScan(ns, rq.getVirtualScan());
        //}
        //}

        ns.end_payload();
        ns.end_request();
        Ok(())
    }

    // TODO
    //theRCB.tallyRateLimitDelayedMs(result.getRateLimitDelayedMs());
    //theRCB.tallyRetryStats(result.getRetryStats());
    // TODO: support deduping of results

    pub(crate) fn add_results(
        &self,
        walker: &mut MapWalker,
        results: &mut Vec<MapValue>,
    ) -> Result<(), NoSQLError> {
        let t = FieldType::try_from_u8(walker.r.read_byte()?)?;
        if t != FieldType::Array {
            return ia_err!("bad type in queryResults: {:?}, should be Array", t);
        }
        walker.r.read_i32()?; // length of array in bytes
        let num_elements = walker.r.read_i32()?;
        trace!("read_results: num_results={}", num_elements);
        let num_elements = walker.r.checked_count(num_elements, "query results")?;
        if num_elements == 0 {
            return Ok(());
        }
        Reader::try_reserve_vec(results, num_elements, "query results")?;
        for _i in 0..num_elements {
            if let FieldValue::Map(m) = walker.r.read_field_value()? {
                //println!("Result: {:?}", m);
                results.push(m);
            } else {
                return ia_err!("got invalid type of value in query results");
            }
        }
        Ok(())
    }

    // Deserialize results for a QueryRequest.
    fn nson_deserialize(
        &mut self,
        r: &mut Reader,
        results: &mut Vec<MapValue>,
        iter_data: &mut ReceiveIterData,
    ) -> Result<(), NoSQLError> {
        // TODO short serialVersion
        // TODO short queryVersion

        let is_prepared_request = !self.prepared_statement.is_empty();

        let mut ti = TopologyInfo::default();
        self.continuation_key = None;
        iter_data.continuation_key = None;

        let mut walker = MapWalker::new(r)?;
        while walker.has_next() {
            walker.next()?;
            let name = walker.current_name();
            match name.as_str() {
                ERROR_CODE => {
                    walker.handle_error_code()?;
                }
                CONSUMED => {
                    let cap = walker.read_nson_consumed_capacity()?;
                    self.consumed_capacity.add(&cap);
                }
                QUERY_RESULTS => {
                    self.add_results(&mut walker, results)?;
                }
                CONTINUATION_KEY => {
                    let ck = walker.read_nson_binary()?;
                    if ck.len() > 0 {
                        trace!("Read {} byte continuation key", ck.len());
                        iter_data.continuation_key = Some(ck.clone());
                        self.continuation_key = Some(ck);
                    }
                }
                SORT_PHASE1_RESULTS => {
                    self.read_phase_1_results(iter_data, &walker.read_nson_binary()?)?;
                }
                PREPARED_QUERY => {
                    if is_prepared_request {
                        return ia_err!("got prepared query in result for already prepared query");
                    }
                    self.prepared_statement.statement = walker.read_nson_binary()?;
                }
                QUERY_BRANCHES => {
                    if is_prepared_request {
                        return ia_err!("got query branches in result for already prepared query");
                    }
                    self.prepared_statement.branches = Self::read_query_branches(&mut walker)?;
                    if self.prepared_statement.statement.is_empty()
                        && self.prepared_statement.branches.len() > 0
                    {
                        let branch = &self.prepared_statement.branches[0];
                        self.prepared_statement.statement = branch.statement.clone();
                        self.prepared_statement.table_name = branch.table_name.clone();
                        self.prepared_statement.namespace = branch.namespace.clone();
                    }
                }
                DRIVER_QUERY_PLAN => {
                    if is_prepared_request {
                        return ia_err!("got driver plan in result for already prepared query");
                    }
                    let v = walker.read_nson_binary()?;
                    self.get_driver_plan_info(&v)?;
                }
                REACHED_LIMIT => {
                    self.reached_limit = walker.read_nson_boolean()?;
                    trace!("REACHED_LIMIT={}", self.reached_limit);
                }
                TABLE_NAME => {
                    self.prepared_statement.table_name = Some(walker.read_nson_string()?);
                }
                NAMESPACE => {
                    self.prepared_statement.namespace = Some(walker.read_nson_string()?);
                }
                QUERY_PLAN_STRING => {
                    self.prepared_statement.query_plan = walker.read_nson_string()?;
                }
                QUERY_RESULT_SCHEMA => {
                    self.prepared_statement.query_schema = walker.read_nson_string()?;
                }
                QUERY_OPERATION => {
                    // TODO: is this an enum? try_from()?
                    self.prepared_statement.operation = walker.read_nson_i32()? as u8;
                }
                TOPOLOGY_INFO => {
                    //println!("deser: TOPOLOGY_INFO");
                    self.prepared_statement.topology_info = Some(walker.read_nson_topology_info()?);
                }
                /* QUERY_V3 and earlier return topo differently */
                PROXY_TOPO_SEQNUM => {
                    //println!("deser: PROXY_TOPO_SEQNUM");
                    ti.seq_num = walker.read_nson_i32()?;
                }
                SHARD_IDS => {
                    //println!("deser: SHARD_IDS");
                    ti.shard_ids = walker.read_nson_i32_array()?;
                }
                _ => {
                    trace!("   query_response: skipping field '{}'", name);
                    walker.skip_nson_field()?;
                } /*
                  // added in QUERY_V4
                  else if (name.equals(VIRTUAL_SCANS)) {
                      readType(in, Nson.TYPE_ARRAY);
                      in.readInt(); // length of array in bytes
                      int numScans = in.readInt(); // number of array elements
                      virtualScans = new VirtualScan[numScans];
                      for (int i = 0; i < numScans; ++i) {
                          virtualScans[i] = readVirtualScan(in);
                      }

                  /* added in QUERY_V4 */
                  } else if (name.equals(QUERY_BATCH_TRACES)) {
                      readType(in, Nson.TYPE_ARRAY);
                      in.readInt(); // length of array in bytes
                      int numTraces = in.readInt() / 2; // number of array elements
                      queryTraces = new TreeMap<String,String>();
                      for (int i = 0; i < numTraces; ++i) {
                          String batchName = Nson.readNsonString(in);
                          String batchTrace = Nson.readNsonString(in);
                          queryTraces.put(batchName, batchTrace);
                      }
                  }
                  */
            }
        }

        if ti.is_valid() {
            self.prepared_statement.topology_info = Some(ti);
        }

        if let Some(ti) = &self.prepared_statement.topology_info {
            //println!("deser: got TI={:?}", ti);
            self.topology_info = ti.clone();
        } else {
            trace!("deser: NO VALID TOPOLOGY RECEIVED");
        }

        if self.prepare_only == true {
            if self.prepared_statement.is_empty() {
                return ia_err!("got no prepared statement when prepare_only was set");
            }
            self.is_done = true;
        } else {
            if !self.prepared_statement.is_simple() && self.continuation_key.is_none() {
                // dummy cont key so is_done won't be set
                trace!("Adding dummy continuation key");
                self.continuation_key = Some(Vec::new());
            }
        }

        Ok(())
    }

    fn read_query_branches(
        walker: &mut MapWalker,
    ) -> Result<Vec<PreparedStatementBranch>, NoSQLError> {
        let fv = walker.read_nson_field_value()?;
        let branches = match fv {
            FieldValue::Array(branches) => branches,
            _ => {
                return ia_err!("bad type in query branches: expected Array");
            }
        };

        let mut result = Vec::with_capacity(branches.len());
        for branch in branches {
            let mv = match branch {
                FieldValue::Map(mv) => mv,
                _ => {
                    return ia_err!("bad type in query branch: expected Map");
                }
            };
            let statement = match mv.get_field_value(PREPARED_QUERY) {
                Some(FieldValue::Binary(bytes)) => bytes.clone(),
                _ => {
                    return ia_err!("query branch is missing prepared query");
                }
            };
            result.push(PreparedStatementBranch {
                statement,
                table_name: mv.get_string(TABLE_NAME),
                namespace: mv.get_string(NAMESPACE),
            });
        }
        Ok(result)
    }

    fn get_driver_plan_info(&mut self, v: &Vec<u8>) -> Result<(), NoSQLError> {
        if v.len() == 0 {
            return Ok(());
        }
        let mut r = Reader::new().from_bytes(v);
        self.prepared_statement.driver_query_plan = deserialize_plan_iter(&mut r)?;
        trace!(
            "driver query plan:\n{:?}",
            self.prepared_statement.driver_query_plan
        );
        if self.prepared_statement.driver_query_plan.get_kind() == PlanIterKind::Empty {
            return Ok(());
        }
        let num_iterators = r.read_i32()?;
        Reader::checked_count_with_limit(num_iterators, v.len(), "driver plan iterators")?;
        self.prepared_statement.num_iterators = num_iterators;
        //println!(
        //"   QUERY_PLAN: iterators={}",
        //self.prepared_statement.num_iterators
        //);
        let num_registers = r.read_i32()?;
        Reader::checked_count_with_limit(num_registers, v.len(), "driver plan registers")?;
        r.validate_query_plan_result_regs(num_registers)?;
        self.prepared_statement.num_registers = num_registers;
        //println!(
        //"   QUERY_PLAN: registers={}",
        //self.prepared_statement.num_registers
        //);
        let len = r.read_i32()?;
        let len = r.checked_count(len, "driver plan variables")?;
        if len == 0 {
            return Ok(());
        }
        let mut hm: HashMap<String, i32> = HashMap::new();
        hm.try_reserve(len).map_err(|_| {
            NoSQLError::new(
                NoSQLErrorCode::BadProtocolMessage,
                "unable to reserve decoded values for driver plan variables",
            )
        })?;
        for _i in 0..len {
            let name = r.read_string()?;
            let id = r.read_i32()?;
            hm.insert(name, id);
        }
        self.prepared_statement.variable_to_ids = Some(hm);
        Ok(())
    }

    fn read_phase_1_results(
        &mut self,
        iter_data: &mut ReceiveIterData,
        arr: &Vec<u8>,
    ) -> Result<(), NoSQLError> {
        let mut r: Reader = Reader::new().from_bytes(arr);
        iter_data.in_sort_phase_1 = r.read_bool()?;
        iter_data.pids = r.read_i32_array()?;
        if iter_data.pids.len() > 0 {
            iter_data.num_results_per_pid = r.read_i32_array()?;
            iter_data.part_continuation_keys = Vec::new();
            for _x in 0..iter_data.num_results_per_pid.len() {
                iter_data.part_continuation_keys.push(r.read_binary()?);
            }
        }
        Ok(())
    }

    pub(crate) fn get_result(&mut self, reg: i32) -> FieldValue {
        let reg = self.register_index(reg, "GET");
        //println!(
        //" get_result register {}: {:?}",
        //reg, self.registers[reg as usize]
        //);
        std::mem::take(&mut self.registers[reg])
    }

    pub(crate) fn get_result_ref(&self, reg: i32) -> &FieldValue {
        let reg = self.register_index(reg, "GET");
        //println!(
        //" get_result_ref register {}: {:?}",
        //reg, self.registers[reg as usize]
        //);
        &self.registers[reg]
    }

    pub(crate) fn set_result(&mut self, reg: i32, val: FieldValue) {
        let reg = self.register_index(reg, "SET");
        //println!(" set_result register {}: {:?}", reg, val);
        self.registers[reg] = val;
    }

    fn register_index(&self, reg: i32, op: &str) -> usize {
        if reg < 0 || self.num_registers <= reg {
            panic!("INVALID {} REGISTER ACCESS", op);
        }
        reg as usize
    }
}

#[cfg(test)]
mod retryability_tests {
    use super::*;

    #[test]
    fn query_retryability_classifies_statement_text() {
        assert!(QueryRequest::new("select * from users").is_retryable());
        assert!(QueryRequest::new("  -- comment\n/* block */ SELECT * from users").is_retryable());
        assert!(QueryRequest::new(
            "declare $id integer; $name string; select * from users where id = $id"
        )
        .is_retryable());

        assert!(!QueryRequest::new("insert into users(id) values(1)").is_retryable());
        assert!(!QueryRequest::new("UPSERT into users(id) values(1)").is_retryable());
        assert!(!QueryRequest::new("update users set name = 'a' where id = 1").is_retryable());
        assert!(!QueryRequest::new("delete from users where id = 1").is_retryable());
        assert!(!QueryRequest::new(
            "declare $id integer; $name string; insert into users(id, name) values($id, $name)"
        )
        .is_retryable());
        assert!(!QueryRequest::new("").is_retryable());
    }

    #[test]
    fn prepare_only_queries_are_retryable_without_executing_mutation() {
        assert!(QueryRequest::new("insert into users(id) values(1)")
            .prepare_only()
            .is_retryable());
    }

    #[test]
    fn query_retryability_uses_prepared_operation_code() {
        let mut prepared_select = PreparedStatement::default();
        prepared_select.statement = vec![1];
        prepared_select.operation = 5;
        assert!(QueryRequest::new_prepared(&prepared_select).is_retryable());

        let mut prepared_mutation = prepared_select.clone();
        prepared_mutation.operation = 0;
        assert!(!QueryRequest::new_prepared(&prepared_mutation).is_retryable());
    }

    fn driver_plan_with_result_reg(result_reg: i32, num_registers: i32) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.write_byte(PlanIterKind::AndOr as u8);
        writer.write_i32(result_reg);
        writer.write_i32(0); // unused state position
        for _ in 0..4 {
            writer.write_i32(0); // source location
        }
        writer.write_i16(0); // FuncCode::OpAnd
        writer.write_packed_i32(0); // argument iterators
        writer.write_i32(1); // number of iterators
        writer.write_i32(num_registers);
        writer.write_i32(0); // external variables
        writer.bytes().to_vec()
    }

    #[test]
    fn invalid_query_plan_register_returns_protocol_error() {
        for result_reg in [-1, 1] {
            let mut request = QueryRequest::default();
            let error = request
                .get_driver_plan_info(&driver_plan_with_result_reg(result_reg, 1))
                .unwrap_err();

            assert_eq!(error.code, NoSQLErrorCode::BadProtocolMessage);
            assert!(error.message.contains("invalid query plan result register"));
        }
    }

    #[test]
    fn valid_query_plan_register_is_accepted() {
        let mut request = QueryRequest::default();

        request
            .get_driver_plan_info(&driver_plan_with_result_reg(0, 1))
            .unwrap();
        assert_eq!(request.prepared_statement.num_registers, 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nson::{GET_QUERY_PLAN, GET_QUERY_SCHEMA, PAYLOAD, QUERY_VERSION, STATEMENT};
    use crate::{Handle, HandleMode, StatsProfile};
    use serde_json::Value;
    use std::collections::HashMap;

    fn serialized_payload_fields(request: &QueryRequest) -> (Vec<String>, HashMap<String, bool>) {
        let mut writer = Writer::new();
        request
            .serialize_internal(&mut writer, &Duration::from_secs(30))
            .unwrap();
        let mut reader = Reader::new().from_bytes(writer.bytes());
        let mut root = MapWalker::new(&mut reader).unwrap();

        while root.has_next() {
            root.next().unwrap();
            let name = root.current_name().clone();
            if name != PAYLOAD {
                root.skip_nson_field().unwrap();
                continue;
            }

            let mut payload = MapWalker::new(root.r).unwrap();
            let mut fields = Vec::new();
            let mut bools = HashMap::new();
            while payload.has_next() {
                payload.next().unwrap();
                let field = payload.current_name().clone();
                fields.push(field.clone());
                match field.as_str() {
                    GET_QUERY_PLAN | GET_QUERY_SCHEMA => {
                        bools.insert(field, payload.read_nson_boolean().unwrap());
                    }
                    _ => payload.skip_nson_field().unwrap(),
                }
            }
            return (fields, bools);
        }

        panic!("serialized query request did not contain payload");
    }

    fn contains_field(fields: &[String], field: &str) -> bool {
        fields.iter().any(|name| name == field)
    }

    #[test]
    fn prepare_only_serializes_query_plan_and_schema_flags_when_requested() {
        let request = QueryRequest::new("select * from users")
            .prepare_only()
            .get_query_plan(true)
            .get_query_schema(true);

        let (fields, bools) = serialized_payload_fields(&request);

        assert!(contains_field(&fields, QUERY_VERSION));
        assert!(contains_field(&fields, STATEMENT));
        assert_eq!(bools.get(GET_QUERY_PLAN), Some(&true));
        assert_eq!(bools.get(GET_QUERY_SCHEMA), Some(&true));

        let request = QueryRequest::new("select * from users").prepare_only();
        let (fields, bools) = serialized_payload_fields(&request);

        assert!(!contains_field(&fields, GET_QUERY_PLAN));
        assert!(!contains_field(&fields, GET_QUERY_SCHEMA));
        assert!(bools.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_batch_rejects_invalid_request_without_query_stats(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let handle = Handle::builder()
            .endpoint("http://localhost:8080")?
            .mode(HandleMode::Cloudsim)?
            .stats_profile(StatsProfile::All)?
            .stats_enable_log(false)?
            .build()
            .await?;

        let mut request = QueryRequest::default();
        let mut results = Vec::new();

        // Java validates query input before observeQuery(), so malformed local
        // requests must not create an ALL-profile logical query entry.
        assert!(request.execute_batch(&handle, &mut results).await.is_err());
        assert!(request.execute_batch(&handle, &mut results).await.is_err());

        let snapshot: Value = serde_json::from_str(
            handle
                .get_stats_control()
                .emit_interval_for_test()
                .unwrap()
                .as_json(),
        )?;

        assert!(snapshot.get("queries").is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_batch_counts_each_valid_public_batch_attempt_as_logical_query(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let handle = Handle::builder()
            .endpoint("http://127.0.0.1:9")?
            .mode(HandleMode::Cloudsim)?
            .timeout(Duration::from_millis(50))?
            .stats_profile(StatsProfile::All)?
            .stats_enable_log(false)?
            .build()
            .await?;

        let mut request = QueryRequest::new("select * from users");
        let mut results = Vec::new();

        // The request is locally valid, so each public execute_batch() attempt
        // counts as a logical query even when the local test endpoint is absent.
        assert!(request.execute_batch(&handle, &mut results).await.is_err());
        assert!(request.execute_batch(&handle, &mut results).await.is_err());

        let snapshot: Value = serde_json::from_str(
            handle
                .get_stats_control()
                .emit_interval_for_test()
                .unwrap()
                .as_json(),
        )?;

        let query = query_entry(&snapshot, "select * from users").unwrap();
        assert_eq!(query["query"], "select * from users");
        assert_eq!(query["count"], 2);
        assert_eq!(query["unprepared"], 2);
        assert_eq!(query["httpRequestCount"], 2);
        assert_eq!(query["errors"], 2);
        assert_eq!(query["simple"], false);

        Ok(())
    }

    fn query_entry<'a>(snapshot: &'a Value, sql: &str) -> Option<&'a Value> {
        snapshot["queries"]
            .as_array()?
            .iter()
            .find(|query| query["query"] == sql)
    }
}
