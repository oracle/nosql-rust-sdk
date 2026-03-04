//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use oracle_nosql_rust_sdk_derive::add_planiter_fields;

use crate::error::NoSQLError;
use crate::handle::Handle;
use crate::plan_iter::{deserialize_plan_iter, PlanIter};
use crate::plan_iter::{Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::FieldValue;

use std::result::Result;
use tracing::debug;

// FieldStepIter returns the value of a field in an input MapValue. It is
// used by the driver to implement column references in the SELECT
// list (see SFWIter).
#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct FieldStepIter {
    input_iter: Box<PlanIter>,
    field_name: String,
    data: FieldStepIterData,
}

#[derive(Debug, Default)]
struct FieldStepIterData {
    state: PlanIterState,
}

impl Clone for FieldStepIterData {
    fn clone(&self) -> Self {
        FieldStepIterData {
            state: PlanIterState::Uninitialized,
        }
    }
    fn clone_from(&mut self, _source: &Self) {
        self.state = PlanIterState::Uninitialized;
    }
}

impl FieldStepIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nFieldStepIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(FieldStepIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // fields specific to FieldStepIter
            input_iter: deserialize_plan_iter(r)?,
            field_name: r.read_string()?,
            data: FieldStepIterData::default(),
        })
    }
}

impl FieldStepIter {
    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.data.state = PlanIterState::Open;
        self.input_iter.open(req, handle)
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
        PlanIterKind::FieldStep
    }
    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            return Ok(false);
        }
        loop {
            //FieldValue result;

            let more = self.input_iter.next(req, handle).await?;
            let ctx_item = self.input_iter.get_result(req);

            if more == false || ctx_item == FieldValue::Uninitialized {
                self.data.state = PlanIterState::Done;
                return Ok(false);
            }

            if ctx_item.is_atomic() {
                continue;
            }

            if ctx_item == FieldValue::Null {
                self.set_result(req, ctx_item);
                return Ok(true);
            }

            let mv = ctx_item.get_map_value()?;
            let result = mv.get_field_value(&self.field_name);
            debug!(
                "FSI{} field '{}' value={:?}",
                self.result_reg, self.field_name, result
            );
            if result.is_none() {
                continue;
            }
            self.set_result(req, result.unwrap().clone_internal());
            return Ok(true);
        }
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("FSI{} get_result={:?}", self.result_reg, fv);
        // TODO: investigate why get_result is called without resetting
        req.set_result(self.result_reg, fv.clone_internal());
        fv
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("FSI{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.data.state = PlanIterState::Open;
        self.input_iter.reset()
    }
    /*
        pub fn close(&mut self, req: &mut QueryRequest) -> Result<(), NoSQLError> {
            self.data.state = PlanIterState::Closed;
            self.input_iter.close(req)
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
