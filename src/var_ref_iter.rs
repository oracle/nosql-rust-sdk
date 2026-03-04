//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use oracle_nosql_rust_sdk_derive::add_planiter_fields;

use crate::error::NoSQLError;
use crate::handle::Handle;
use crate::plan_iter::{Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::FieldValue;

use std::result::Result;
use tracing::trace;

// VarRefIter represents a reference to a non-external variable in the query.
// It simply returns the value that the variable is currently bound to. This
// value is computed by the variable's "domain iterator" (the iterator that
// evaluates the domain expression of the variable). The domain iterator stores
// the value in theResultReg of this VarRefIter.
//
// In the context of the driver, an implicit internal variable is used
// to represent the results arriving from the proxy. All other expressions that
// are computed at the driver operate on these results, so all such expressions
// reference this variable. This is analogous to the internal variable used in
// kvstore to represent the table alias in the FROM clause.
//
// var_name:
// The name of the variable. Used only when displaying the execution plan.
#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct VarRefIter {
    var_name: String,
    data: VarRefIterData,
}

#[derive(Debug, Default)]
struct VarRefIterData {
    state: PlanIterState,
}

impl Clone for VarRefIterData {
    // clone of iter data never copies its actual data
    fn clone(&self) -> Self {
        VarRefIterData::default()
    }
    fn clone_from(&mut self, _source: &Self) {
        self.state = PlanIterState::Uninitialized;
    }
}

impl VarRefIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        trace!("\nVarRefIter: result_reg={} state_pos={}\n", rr, sp);
        let v = VarRefIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // fields specific to VarRefIter
            var_name: r.read_string()?,

            data: VarRefIterData::default(),
        };
        //println!(" {:?}", v);
        Ok(v)
    }
}

impl VarRefIter {
    pub fn open(&mut self, _req: &QueryRequest, _handle: &Handle) -> Result<(), NoSQLError> {
        self.data.state = PlanIterState::Open;
        Ok(())
    }
    /*
        pub fn display_content(&self, sb: &mut String, f: &PlanFormatter) {
            f.print_indent(sb);
            sb.push_str(format!("{:?}", self).as_str());
        }
        pub fn get_plan(&self) -> String {
            format!("{:?}", self)
        }
    */
    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::VarRef
    }
    pub async fn next(
        &mut self,
        _req: &mut QueryRequest,
        _handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            //if (rcb.getTraceLevel() >= 4) {
            //rcb.trace("No Value for variable " + theName + " in register " +
            //theResultReg);
            return Ok(false);
        }

        //if (rcb.getTraceLevel() >= 4) {
        //rcb.trace("Value for variable " + theName + " in register " +
        //theResultReg + ":\n" + rcb.getRegVal(theResultReg));
        //}
        self.data.state = PlanIterState::Done;
        return Ok(true);
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        //println!("VarRefIter.get_result");
        // TODO: parse out this clone. It seems like some cases get multiple references to the same varRefIter.
        // example: select sum(id), name from foo group by name

        // TODO: this clone gets used all over in sorting. We should not be cloning here!
        req.get_result_ref(self.result_reg).clone_internal()
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        req.set_result(self.result_reg, result);
    }
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.data.state = PlanIterState::Open;
        Ok(())
    }
    /*
        pub fn close(&mut self, _req: &mut QueryRequest) -> Result<(), NoSQLError> {
            self.data.state = PlanIterState::Closed;
            Ok(())
        }
    */
    pub fn get_state(&self) -> PlanIterState {
        self.data.state
    }
    /*
        pub fn get_func_code(&self) -> Option<FuncCode> {
            None
        }
    */
    pub fn get_aggr_value(
        &self,
        _req: &QueryRequest,
        _reset: bool,
    ) -> Result<Option<FieldValue>, NoSQLError> {
        Ok(None)
    }
}
