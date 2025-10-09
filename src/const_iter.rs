//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
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
use tracing::debug;

// ConstIter represents a reference to a constant value in the query.
// Such a reference will need to be "executed" at the driver side when
// the constant appears in the OFFSET or LIMIT clause.
#[add_planiter_fields]
#[derive(Debug, Default)]
pub(crate) struct ConstIter {
    state: PlanIterState,
    value: FieldValue,
}

impl Clone for ConstIter {
    fn clone(&self) -> Self {
        ConstIter {
            result_reg: self.result_reg,
            loc: self.loc.clone(),
            state: PlanIterState::Uninitialized,
            value: self.value.clone_internal(),
        }
    }
    fn clone_from(&mut self, source: &Self) {
        self.state = PlanIterState::Uninitialized;
        self.result_reg = source.result_reg;
        self.loc = source.loc.clone();
        self.value = source.value.clone_internal();
    }
}

impl ConstIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nConstIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(ConstIter {
            // fields common to all PlanIters
            result_reg: rr,
            state: PlanIterState::Uninitialized,
            loc: Location::from_reader(r)?,
            // Specific to ConstIter
            value: r.read_field_value()?,
        })
    }
}

impl ConstIter {
    pub fn open(&mut self, _req: &mut QueryRequest, _handle: &Handle) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Open;
        //self.set_result(req, self.value.clone_internal());
        Ok(())
    }
    /*
        pub fn display_content(&self, sb: &mut String, f: &PlanFormatter) {
            f.print_indent(sb);
            sb.push_str(format!("{:?}", self.value).as_str());
        }
        pub fn get_plan(&self) -> String {
            format!("{:?}", self)
        }
    */
    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::Const
    }
    pub async fn next(&mut self, _req: &QueryRequest) -> Result<bool, NoSQLError> {
        if self.state == PlanIterState::Done {
            return Ok(false);
        }
        self.state = PlanIterState::Done;
        Ok(true)
    }
    pub fn get_result(&self, _req: &mut QueryRequest) -> FieldValue {
        // return clone of value so we don't reset our value to Uninitialized
        self.value.clone_internal()
    }
    pub fn set_result(&self, _req: &mut QueryRequest, _result: FieldValue) {
        // const iter never changes its initial value
    }
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Open;
        Ok(())
    }
    /*
        pub fn close(&mut self, _req: &QueryRequest) -> Result<(), NoSQLError> {
            self.state = PlanIterState::Closed;
            Ok(())
        }
    */
    pub fn get_state(&self) -> PlanIterState {
        self.state
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

// EmptyIter exists only to allow the creation of a PlanIter on setup
#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct EmptyIter {}

impl EmptyIter {
    pub fn new() -> Self {
        EmptyIter::default()
    }
    pub fn open(&mut self, _req: &QueryRequest, _handle: &Handle) -> Result<(), NoSQLError> {
        Ok(())
    }
    /*
        pub fn display_content(&self, sb: &mut String, f: &PlanFormatter) {
            f.print_indent(sb);
            sb.push_str("<Empty>");
        }
        pub fn get_plan(&self) -> String {
            "<No Plan>".to_string()
        }
    */
    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::Empty
    }
    pub async fn next(&mut self, _req: &QueryRequest) -> Result<bool, NoSQLError> {
        Ok(false)
    }
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        Ok(())
    }
    /*
        pub fn close(&mut self, _req: &QueryRequest) -> Result<(), NoSQLError> {
            Ok(())
        }
    */
    pub fn get_result(&self, _req: &mut QueryRequest) -> FieldValue {
        FieldValue::Uninitialized
    }
    pub fn set_result(&self, _req: &mut QueryRequest, _result: FieldValue) {}
    pub fn get_state(&self) -> PlanIterState {
        PlanIterState::Uninitialized
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
