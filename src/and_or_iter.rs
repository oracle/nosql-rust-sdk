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
use crate::plan_iter::PlanIterState;
use crate::plan_iter::{deserialize_plan_iters, FuncCode, Location, PlanIter, PlanIterKind};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::FieldValue;

use std::result::Result;
use tracing::debug;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct AndOrIter {
    code: FuncCode,
    arg_iters: Vec<Box<PlanIter>>,
    state: PlanIterState,
}

impl AndOrIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        let rr = r.read_query_plan_result_reg()?;
        let sp = r.read_i32()?;
        debug!("\nAndOrIter: result_reg={} state_pos={}\n", rr, sp);
        let code = FuncCode::try_from_u16(r.read_i16()? as u16)?;
        Ok(AndOrIter {
            result_reg: rr,
            loc: Location::from_reader(r)?,
            code,
            arg_iters: deserialize_plan_iters(r)?,
            state: PlanIterState::Uninitialized,
        })
    }

    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Open;
        for iter in &mut self.arg_iters {
            iter.open(req, handle)?;
        }
        Ok(())
    }

    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::AndOr
    }

    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.state == PlanIterState::Done {
            return Ok(false);
        }

        let is_and = match self.code {
            FuncCode::OpAnd => true,
            FuncCode::OpOr => false,
            _ => return ia_err!("invalid function code for AndOrIter: {:?}", self.code),
        };
        let mut result = is_and;
        let mut have_null = false;

        for iter in &mut self.arg_iters {
            let more = iter.next(req, handle).await?;
            let arg_result = if more {
                let value = iter.get_result(req);
                if value.is_null() {
                    have_null = true;
                    continue;
                }
                match value {
                    FieldValue::Boolean(b) => b,
                    _ => return ia_err!("AND/OR operand is not boolean: {:?}", value),
                }
            } else {
                false
            };

            if is_and {
                result &= arg_result;
                if !result {
                    have_null = false;
                    break;
                }
            } else {
                result |= arg_result;
                if result {
                    have_null = false;
                    break;
                }
            }
        }

        if have_null {
            self.set_result(req, FieldValue::Null);
        } else {
            self.set_result(req, FieldValue::Boolean(result));
        }
        self.state = PlanIterState::Done;
        Ok(true)
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("AO{} get_result={:?}", self.result_reg, fv);
        fv
    }

    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("AO{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }

    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Uninitialized;
        for iter in &mut self.arg_iters {
            iter.reset()?;
        }
        Ok(())
    }

    pub fn get_state(&self) -> PlanIterState {
        self.state
    }

    pub fn get_aggr_value(
        &self,
        _req: &QueryRequest,
        _reset: bool,
    ) -> Result<Option<FieldValue>, NoSQLError> {
        Ok(None)
    }
}
