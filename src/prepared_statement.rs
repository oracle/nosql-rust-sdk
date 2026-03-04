//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::error::NoSQLError;
use crate::plan_iter::{PlanIter, PlanIterKind};
use crate::types::{FieldValue, TopologyInfo};

use std::collections::HashMap;
use std::result::Result;

/// A prepared query statement for use in a [`QueryRequest`](crate::QueryRequest).
///
/// PreparedStatement encapsulates a prepared query statement. It includes state
/// that can be sent to a server and executed without re-parsing the query.
///
/// The details of a prepared query are purposefully opaque, as its internal
/// data and implementation may change over time.
///
/// PreparedStatement is only created by calling [`QueryRequest::execute()`](crate::QueryRequest::execute()) followed by
/// [`QueryResult::prepared_statement()`](crate::QueryResult::prepared_statement()).
///
/// The main purpose of a prepared statement is to parse a query for execution many times,
/// with different variables. Here is a simple example using `QueryRequest` and `PreparedStatement` to
/// insert a set of rows into a table (note: it may be more optimal to use [`WriteMultipleRequest`](crate::WriteMultipleRequest)
/// for this specific case, this example is just to show the use of variables in prepared statetments):
/// ```no_run
/// # use oracle_nosql_rust_sdk::{Handle, QueryRequest, NoSQLColumnToFieldValue};
/// # #[tokio::main]
/// # pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let handle = Handle::builder().build().await?;
/// let prep_result = QueryRequest::new(
///     "declare $id integer; $name string; insert into testusers(id, name) values($id, $name)",
/// )
/// .prepare_only()
/// .execute(&handle)
/// .await?;
/// let data = vec!["jane", "john", "jasper"];
/// let mut qreq = QueryRequest::new_prepared(&prep_result.prepared_statement());
/// for i in 0..data.len() {
///     let id = (i as i32) + 1;
///     qreq.set_variable("$id", &id)?;
///     qreq.set_variable("$name", &data[i])?;
///     let result = qreq.execute(&handle).await?;
///     println!("Insert result = {:?}", result);
/// }
/// # Ok(())
/// # }
#[derive(Default, Clone)]
pub struct PreparedStatement {
    // sql_text represents the application provided SQL text.
    #[allow(dead_code)]
    pub(crate) sql_text: String,

    // query_plan is the string representation of query plan.
    pub(crate) query_plan: String,

    // query_schema is the string representation of query schema.
    pub(crate) query_schema: String,

    // table_name is the table name returned from a prepared query result, if any.
    pub(crate) table_name: Option<String>,

    // namespace is the namespace returned from a prepared query result, if any.
    pub(crate) namespace: Option<String>,

    // operation is the operation code for the query.
    pub(crate) operation: u8,

    // driver_query_plan represents the part of query plan that must be executed at the driver.
    // It is received from the NoSQL database proxy when the query is prepared there.
    // It is deserialized by the driver and not sent back to the database proxy.
    // This is only used for advanced queries.
    pub(crate) driver_query_plan: Box<PlanIter>,

    // topology_info represents the NoSQL database topology information that
    // are required for query execution.
    // This is only used for advanced queries.
    pub(crate) topology_info: Option<TopologyInfo>,

    // statement represents the serialized PreparedStatement created at the backend store.
    // It is opaque for the driver.
    // It is received from the NoSQL database proxy and sent back to the proxy
    // every time a new batch of results is needed.
    pub(crate) statement: Vec<u8>,

    // variable_to_ids maps the name of each external variable to its id, which is
    // a position in a FieldValue array stored in the QueryRequest and
    // holding the values of the variables.
    // This is only used for advanced queries.
    // It is only created when deserializing a prepared statement from a query response.
    pub(crate) variable_to_ids: Option<HashMap<String, i32>>,

    // for driver plans
    pub(crate) num_registers: i32,
    pub(crate) num_iterators: i32,

    pub(crate) data: PreparedStatementData,
}

impl std::fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PreparedStatement: size={}, data={:?}",
            self.statement.len(),
            self.data
        )
    }
}

#[derive(Debug, Default)]
pub struct PreparedStatementData {
    // bind_variables is a map that associates the name to the value for external
    // variables used in the query.
    //
    // This map is populated by the application using the SetVariable() method.
    // It is sent to the NoSQL database proxy every time a new batch of results is needed.
    // The values in this map are also placed in the runtimeControlBlock
    // FieldValue array, just before the query starts its execution at the driver.
    pub bind_variables: HashMap<String, FieldValue>,
}

// We do this manually because we do not want to clone the bind variables
impl Clone for PreparedStatementData {
    fn clone(&self) -> Self {
        //println!("ps.data.clone(): clearing");
        PreparedStatementData {
            bind_variables: Default::default(),
        }
    }
    fn clone_from(&mut self, _source: &Self) {
        //println!("ps.data.clone_from(): clearing");
        self.bind_variables.clear();
    }
}

impl PreparedStatement {
    pub(crate) fn is_simple(&self) -> bool {
        self.driver_query_plan.get_kind() == PlanIterKind::Empty
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.statement.len() == 0
    }
    // set iterators/etc to their initial values, as if
    // they had just been deserialized
    pub(crate) fn reset(&mut self) -> Result<(), NoSQLError> {
        self.driver_query_plan.reset()?;
        // Do not clear bound variables: that's in a different call
        //self.data = PreparedStatementData::default();
        Ok(())
    }
    pub(crate) fn copy_for_internal(&self) -> Self {
        let mut data = PreparedStatementData::default();
        for (k, v) in &self.data.bind_variables {
            data.bind_variables.insert(k.clone(), v.clone_internal());
        }
        PreparedStatement {
            // we only keep the actual binary prepared statement, all other
            // fields get their defaults
            statement: self.statement.clone(),
            data: data,
            ..Default::default()
        }
    }

    pub(crate) fn set_variable(
        &mut self,
        name: &str,
        value: &FieldValue,
    ) -> Result<(), NoSQLError> {
        // TODO: verify variable should start with '$'
        self.data
            .bind_variables
            .insert(name.to_string(), value.clone_internal());
        Ok(())
    }

    pub(crate) fn set_variable_by_id(
        &mut self,
        id: i32,
        value: &FieldValue,
    ) -> Result<(), NoSQLError> {
        /*
                if let Some(vars) = &self.variable_to_ids {
                    for (k, v) in vars {
                        if *v == id {
                            self.data
                                .bind_variables
                                .insert(k.clone(), value.clone_internal());
                            return Ok(());
                        }
                    }
                    return Err(format!(
                        "prepared statement does not have variable at position {}",
                        id
                    )
                    .as_str()
                    .into());
                }
                Err("prepared statement does not have positional variables".into())
        */
        self.data
            .bind_variables
            .insert(format!("#{}", id), value.clone_internal());
        Ok(())
    }

    pub(crate) fn get_variable_by_id(&self, id: i32) -> Option<&FieldValue> {
        if let Some(vars) = &self.variable_to_ids {
            for (k, v) in vars {
                if *v == id {
                    return self.data.bind_variables.get(k);
                }
            }
        }
        None
    }
}
