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
use crate::plan_iter::{deserialize_plan_iter, FuncCode, PlanIter};
use crate::plan_iter::{Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::{compare_field_values, FieldValue};

use std::cmp::Ordering;
use std::result::Result;
use tracing::debug;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct CompOpIter {
    state: PlanIterState,
    func_code: FuncCode,
    left_iter: Box<PlanIter>,
    right_iter: Box<PlanIter>,
}

impl CompOpIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_query_plan_result_reg()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nCompOpIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(CompOpIter {
            // fields common to all PlanIters
            result_reg: rr,
            state: PlanIterState::Uninitialized,
            loc: Location::from_reader(r)?,

            // specific to CompOpIter
            func_code: FuncCode::try_from_u16(r.read_u16()?)?,
            left_iter: deserialize_plan_iter(r)?,
            right_iter: deserialize_plan_iter(r)?,
        })
    }

    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Open;
        self.left_iter.open(req, handle)?;
        self.right_iter.open(req, handle)?;
        Ok(())
    }

    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::ValueCompare
    }

    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.state == PlanIterState::Done {
            return Ok(false);
        }

        let left_has_value = self.left_iter.next(req, handle).await?;
        if left_has_value && self.left_iter.next(req, handle).await? {
            return ia_err!(
                "left operand of comparison {:?} produced more than one item",
                self.func_code
            );
        }

        let right_has_value = self.right_iter.next(req, handle).await?;
        if right_has_value && self.right_iter.next(req, handle).await? {
            return ia_err!(
                "right operand of comparison {:?} produced more than one item",
                self.func_code
            );
        }

        let result = match (left_has_value, right_has_value) {
            (false, false) => self.result_from_ordering(Ordering::Equal)?,
            (false, true) | (true, false) => FieldValue::Boolean(self.func_code == FuncCode::OpNeq),
            (true, true) => {
                let left = self.left_iter.get_result(req);
                let right = self.right_iter.get_result(req);
                self.compare_values(&left, &right)?
            }
        };

        self.set_result(req, result);
        self.state = PlanIterState::Done;
        Ok(true)
    }

    fn compare_values(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<FieldValue, NoSQLError> {
        if left.is_null() || right.is_null() {
            return Ok(FieldValue::Null);
        }

        self.result_from_ordering(compare_field_values(left, right, false))
    }

    fn result_from_ordering(&self, ord: Ordering) -> Result<FieldValue, NoSQLError> {
        let result = match self.func_code {
            FuncCode::OpEq => ord == Ordering::Equal,
            FuncCode::OpNeq => ord != Ordering::Equal,
            FuncCode::OpGt => ord == Ordering::Greater,
            FuncCode::OpGe => ord != Ordering::Less,
            FuncCode::OpLt => ord == Ordering::Less,
            FuncCode::OpLe => ord != Ordering::Greater,
            _ => {
                return ia_err!(
                    "invalid function code for comparison iterator: {:?}",
                    self.func_code
                );
            }
        };
        Ok(FieldValue::Boolean(result))
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        req.get_result(self.result_reg)
    }

    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        req.set_result(self.result_reg, result);
    }

    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Open;
        self.left_iter.reset()?;
        self.right_iter.reset()
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
