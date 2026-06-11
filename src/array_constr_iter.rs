//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use oracle_nosql_rust_sdk_derive::add_planiter_fields;

use crate::error::NoSQLError;
use crate::handle::Handle;
use crate::plan_iter::{deserialize_plan_iters, Location, PlanIter, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::FieldValue;

use std::result::Result;
use tracing::debug;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct ArrayConstrIter {
    is_conditional: bool,
    arg_iters: Vec<Box<PlanIter>>,
    state: PlanIterState,
}

impl ArrayConstrIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        let rr = r.read_i32()?;
        let sp = r.read_i32()?;
        debug!("\nArrayConstrIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(ArrayConstrIter {
            result_reg: rr,
            loc: Location::from_reader(r)?,
            is_conditional: r.read_bool()?,
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
        PlanIterKind::ArrayConstr
    }

    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.state == PlanIterState::Done {
            return Ok(false);
        }

        let mut array = Vec::new();

        if self.is_conditional && !self.arg_iters.is_empty() {
            let more = self.arg_iters[0].next(req, handle).await?;
            if !more {
                self.state = PlanIterState::Done;
                return Ok(false);
            }

            let first = self.arg_iters[0].get_result(req);
            let more = self.arg_iters[0].next(req, handle).await?;
            if !more {
                self.set_result(req, first);
                self.state = PlanIterState::Done;
                return Ok(true);
            }

            if !first.is_null() {
                array.push(first);
            }
            let second = self.arg_iters[0].get_result(req);
            if !second.is_null() {
                array.push(second);
            }
        }

        for iter in &mut self.arg_iters {
            loop {
                let more = iter.next(req, handle).await?;
                if !more {
                    break;
                }
                let value = iter.get_result(req);
                if !value.is_null() {
                    array.push(value);
                }
            }
        }

        self.set_result(req, FieldValue::Array(array));
        self.state = PlanIterState::Done;
        Ok(true)
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("AC{} get_result={:?}", self.result_reg, fv);
        fv
    }

    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("AC{} set_result({:?})", self.result_reg, result);
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
