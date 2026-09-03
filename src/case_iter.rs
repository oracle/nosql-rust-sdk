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
use crate::plan_iter::{deserialize_plan_iter, deserialize_plan_iters, PlanIter};
use crate::plan_iter::{Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::FieldValue;

use std::result::Result;
use tracing::debug;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct CaseIter {
    cond_iters: Vec<Box<PlanIter>>,
    then_iters: Vec<Box<PlanIter>>,
    else_iter: Box<PlanIter>,
    data: CaseIterData,
}

impl CaseIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_query_plan_result_reg()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nCaseIter: result_reg={} state_pos={}\n", rr, sp);

        let mut iter = CaseIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // specific to CaseIter
            cond_iters: deserialize_plan_iters(r)?,
            then_iters: deserialize_plan_iters(r)?,
            else_iter: deserialize_plan_iter(r)?,

            ..Default::default()
        };

        if iter.cond_iters.len() != iter.then_iters.len() {
            return ia_err!(
                "CaseIter mismatched condition and then iterator counts: {} != {}",
                iter.cond_iters.len(),
                iter.then_iters.len()
            );
        }

        iter.data.reset_to_uninitialized();
        Ok(iter)
    }
}

#[derive(Debug, Default)]
struct CaseIterData {
    state: PlanIterState,
    active_iter: ActiveCaseIter,
}

impl Clone for CaseIterData {
    fn clone(&self) -> Self {
        CaseIterData::default()
    }

    fn clone_from(&mut self, _source: &Self) {
        self.reset_to_uninitialized();
    }
}

impl CaseIterData {
    fn reset_to_open(&mut self) {
        self.state = PlanIterState::Open;
        self.active_iter = ActiveCaseIter::None;
    }

    fn reset_to_uninitialized(&mut self) {
        self.state = PlanIterState::Uninitialized;
        self.active_iter = ActiveCaseIter::None;
    }
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
enum ActiveCaseIter {
    #[default]
    None,
    Then(usize),
    Else,
}

impl CaseIter {
    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.data.reset_to_open();
        for iter in &mut self.cond_iters {
            iter.open(req, handle)?;
        }
        for iter in &mut self.then_iters {
            iter.open(req, handle)?;
        }
        self.else_iter.open(req, handle)?;
        Ok(())
    }

    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::Case
    }

    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            return Ok(false);
        }

        if self.data.state != PlanIterState::Running {
            self.choose_active_iter(req, handle).await?;
            if self.data.active_iter == ActiveCaseIter::None {
                self.done();
                return Ok(false);
            }
            self.data.state = PlanIterState::Running;
        }

        let active_iter = self.data.active_iter;
        let (more, result) = match active_iter {
            ActiveCaseIter::Then(i) => {
                if self.then_iters[i].next(req, handle).await? == false {
                    (false, FieldValue::Uninitialized)
                } else {
                    (true, self.then_iters[i].get_result(req))
                }
            }
            ActiveCaseIter::Else => {
                if self.else_iter.next(req, handle).await? == false {
                    (false, FieldValue::Uninitialized)
                } else {
                    (true, self.else_iter.get_result(req))
                }
            }
            ActiveCaseIter::None => (false, FieldValue::Uninitialized),
        };

        if more == false {
            self.done();
            return Ok(false);
        }

        self.set_result(req, result);
        Ok(true)
    }

    async fn choose_active_iter(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<(), NoSQLError> {
        for i in 0..self.cond_iters.len() {
            if self.cond_iters[i].next(req, handle).await? == false {
                continue;
            }

            match self.cond_iters[i].get_result(req) {
                FieldValue::Boolean(true) => {
                    self.data.active_iter = ActiveCaseIter::Then(i);
                    return Ok(());
                }
                FieldValue::Boolean(false) | FieldValue::Null => (),
                val => {
                    return ia_err!(
                        "CASE condition must evaluate to boolean or null, got {:?}",
                        val
                    );
                }
            }
        }

        if self.else_iter.get_kind() != PlanIterKind::Empty {
            self.data.active_iter = ActiveCaseIter::Else;
        }
        Ok(())
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        req.get_result(self.result_reg)
    }

    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        req.set_result(self.result_reg, result);
    }

    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        for iter in &mut self.cond_iters {
            iter.reset()?;
        }
        for iter in &mut self.then_iters {
            iter.reset()?;
        }
        self.else_iter.reset()?;
        self.data.reset_to_open();
        Ok(())
    }

    fn done(&mut self) {
        self.data.state = PlanIterState::Done;
        self.data.active_iter = ActiveCaseIter::None;
    }

    pub fn get_state(&self) -> PlanIterState {
        self.data.state
    }

    pub fn get_aggr_value(
        &self,
        _req: &QueryRequest,
        _reset: bool,
    ) -> Result<Option<FieldValue>, NoSQLError> {
        Ok(None)
    }
}
