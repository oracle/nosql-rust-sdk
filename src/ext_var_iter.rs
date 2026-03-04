//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use oracle_nosql_rust_sdk_derive::add_planiter_fields;

use crate::error::ia_err;
use crate::error::NoSQLError;
use crate::handle::Handle;
use crate::plan_iter::{Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::FieldValue;

use std::result::Result;
use tracing::trace;

// In general, ExternalVarRefIter represents a reference to an external variable
// in the query. Such a reference will need to be "executed" at the driver side
// when the variable appears in the OFFSET or LIMIT clause.
//
// ExternalVarRefIter simply returns the value that the variable is currently
// bound to. This value is set by the app via the methods of QueryRequest.
//
// var_name:
// The name of the variable. Used only when displaying the execution plan.
// and in error messages.
//
// id:
// The variable id. It is used as an index into an array of FieldValues
// in the QueryRequest that stores the values of the external vars.
#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct ExtVarIter {
    var_name: String,
    id: i32,
    data: ExtVarIterData,
}

#[derive(Debug, Default)]
struct ExtVarIterData {
    state: PlanIterState,
}

impl Clone for ExtVarIterData {
    // clone of iter data never copies its actual data
    fn clone(&self) -> Self {
        ExtVarIterData::default()
    }
    fn clone_from(&mut self, _source: &Self) {
        self.state = PlanIterState::Uninitialized;
    }
}

impl ExtVarIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        trace!("\nExtVarIter: result_reg={} state_pos={}\n", rr, sp);
        let v = ExtVarIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // fields specific to ExtVarIter
            var_name: r.read_string()?,
            id: r.read_i32()?,

            data: ExtVarIterData::default(),
        };
        //println!(" {:?}", v);
        Ok(v)
    }
}

impl ExtVarIter {
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
        PlanIterKind::ExtVar
    }
    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
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
        if let Some(val) = req.get_external_var(self.id) {
            self.set_result(req, val.clone_internal());
        } else {
            return ia_err!("variable at id {} does not exist", self.id);
        }

        self.data.state = PlanIterState::Done;
        return Ok(true);
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        //println!("ExtVarIter.get_result");
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
