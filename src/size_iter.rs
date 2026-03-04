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
use crate::plan_iter::{deserialize_plan_iter, PlanIter};
use crate::plan_iter::{Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::FieldValue;

use std::result::Result;
use tracing::trace;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct SizeIter {
    input_iter: Box<PlanIter>,
    data: SizeIterData,
}

#[derive(Debug, Default)]
struct SizeIterData {
    state: PlanIterState,
}

impl Clone for SizeIterData {
    fn clone(&self) -> Self {
        SizeIterData::default()
    }
    fn clone_from(&mut self, _source: &Self) {
        self.state = PlanIterState::Uninitialized;
    }
}

impl SizeIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        trace!("\nSizeIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(SizeIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // fields specific to SizeIter
            input_iter: deserialize_plan_iter(r)?,
            data: SizeIterData::default(),
        })
    }
}

impl SizeIter {
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
        PlanIterKind::Size
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

            if self.input_iter.next(req, handle).await? == false {
                self.data.state = PlanIterState::Done;
                return Ok(false);
            }

            let val = self.input_iter.get_result(req);
            if val.is_null() {
                self.set_result(req, FieldValue::Null);
                self.data.state = PlanIterState::Done;
                return Ok(true);
            }

            let size: usize;
            match val {
                FieldValue::Array(a) => {
                    size = a.len();
                }
                FieldValue::Map(m) => {
                    size = m.len();
                }
                _ => {
                    return ia_err!(
                        "Input to the size() function has wrong type\n\
						Expected complex type, actual type is: {:?}, {:?}",
                        val.get_type(),
                        self.loc
                    );
                }
            }

            self.set_result(req, FieldValue::Long(size as i64));
            return Ok(true);
        }
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        //println!("SizeIter.get_result");
        req.get_result(self.result_reg)
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
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
