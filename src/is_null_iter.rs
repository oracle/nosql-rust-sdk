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
use crate::plan_iter::{deserialize_plan_iter, FuncCode, Location, PlanIter, PlanIterKind};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::FieldValue;

use std::result::Result;
use tracing::debug;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct IsNullIter {
    code: FuncCode,
    arg_iter: Box<PlanIter>,
    state: PlanIterState,
}

impl IsNullIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        let rr = r.read_i32()?;
        let sp = r.read_i32()?;
        debug!("\nIsNullIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(IsNullIter {
            result_reg: rr,
            loc: Location::from_reader(r)?,
            code: FuncCode::try_from_u16(r.read_i16()? as u16)?,
            arg_iter: deserialize_plan_iter(r)?,
            state: PlanIterState::Uninitialized,
        })
    }

    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Open;
        self.arg_iter.open(req, handle)
    }

    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::IsNull
    }

    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.state == PlanIterState::Done {
            return Ok(false);
        }

        let more = self.arg_iter.next(req, handle).await?;
        let is_null = if more {
            self.arg_iter.get_result(req).is_null()
        } else {
            false
        };

        let result = match self.code {
            FuncCode::OpIsNull => is_null,
            FuncCode::OpIsNotNull => !is_null,
            _ => return ia_err!("invalid function code for IsNullIter: {:?}", self.code),
        };

        self.set_result(req, FieldValue::Boolean(result));
        self.state = PlanIterState::Done;
        Ok(true)
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("IN{} get_result={:?}", self.result_reg, fv);
        fv
    }

    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("IN{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }

    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Uninitialized;
        self.arg_iter.reset()
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
