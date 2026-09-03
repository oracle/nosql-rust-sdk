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
use crate::plan_iter::{deserialize_plan_iters, Location, PlanIter, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::sort_iter::SortSpec;
use crate::types::{compare_results, FieldValue, MapValue};

use std::cmp::Ordering;
use std::result::Result;
use tracing::debug;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct UnionIter {
    branches: Vec<Box<PlanIter>>,
    sort_fields: Vec<String>,
    sort_specs: Vec<SortSpec>,
    data: UnionIterData,
}

#[derive(Debug, Default)]
struct UnionIterData {
    state: PlanIterState,
    current_branch: usize,
    sorted_branches: Vec<SortedBranch>,
}

#[derive(Debug, Default)]
struct SortedBranch {
    branch_no: usize,
    current_result: Option<MapValue>,
    done: bool,
}

impl Clone for UnionIterData {
    fn clone(&self) -> Self {
        UnionIterData::default()
    }
    fn clone_from(&mut self, _source: &Self) {
        self.reset();
    }
}

impl UnionIterData {
    fn reset(&mut self) {
        self.state = PlanIterState::Uninitialized;
        self.current_branch = 0;
        self.sorted_branches.clear();
    }
}

impl UnionIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        let rr = r.read_query_plan_result_reg()?;
        let sp = r.read_i32()?;
        debug!("\nUnionIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(UnionIter {
            result_reg: rr,
            loc: Location::from_reader(r)?,
            branches: deserialize_plan_iters(r)?,
            sort_fields: r.read_string_array()?,
            sort_specs: SortSpec::read_sort_specs(r)?,
            data: UnionIterData::default(),
        })
    }

    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.data.reset();
        self.data.state = PlanIterState::Open;

        if self.branches.is_empty() {
            self.data.state = PlanIterState::Done;
            return Ok(());
        }

        if self.does_sort() {
            self.data.sorted_branches = (0..self.branches.len())
                .map(|branch_no| SortedBranch {
                    branch_no,
                    ..Default::default()
                })
                .collect();
            for branch in &mut self.branches {
                branch.open(req, handle)?;
            }
        } else {
            self.branches[0].open(req, handle)?;
        }

        Ok(())
    }

    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::Union
    }

    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            return Ok(false);
        }

        if self.does_sort() {
            return self.sorting_next(req, handle).await;
        }

        self.simple_next(req, handle).await
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("UI{} get_result={:?}", self.result_reg, fv);
        fv
    }

    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("UI{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }

    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.data.reset();
        for branch in &mut self.branches {
            branch.reset()?;
        }
        Ok(())
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

    fn does_sort(&self) -> bool {
        self.sort_fields.len() > 0
    }

    async fn simple_next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        while self.data.current_branch < self.branches.len() {
            let branch_no = self.data.current_branch;
            let more = self.next_branch(branch_no, req, handle).await?;

            if more {
                let result = self.branches[branch_no].get_result(req);
                self.set_result(req, result);
                self.data.state = PlanIterState::Running;
                return Ok(true);
            }

            if req.reached_limit {
                return Ok(false);
            }

            self.data.current_branch += 1;
            if self.data.current_branch < self.branches.len() {
                self.branches[self.data.current_branch].open(req, handle)?;
            }
        }

        self.data.state = PlanIterState::Done;
        Ok(false)
    }

    async fn sorting_next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if !self.ensure_sorted_results(req, handle).await? {
            return Ok(false);
        }

        let next_branch = self.smallest_sorted_branch();
        if let Some(branch_no) = next_branch {
            let mv = self.data.sorted_branches[branch_no]
                .current_result
                .take()
                .unwrap();
            self.set_result(req, FieldValue::Map(mv));
            self.data.state = PlanIterState::Running;
            return Ok(true);
        }

        self.data.state = PlanIterState::Done;
        Ok(false)
    }

    async fn ensure_sorted_results(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        for branch_no in 0..self.data.sorted_branches.len() {
            if self.data.sorted_branches[branch_no].done
                || self.data.sorted_branches[branch_no]
                    .current_result
                    .is_some()
            {
                continue;
            }

            let more = self.next_branch(branch_no, req, handle).await?;
            if more {
                let mv = self.branches[branch_no].get_result(req).get_map_value()?;
                self.validate_sort_result(&mv)?;
                self.data.sorted_branches[branch_no].current_result = Some(mv);
            } else if req.reached_limit {
                return Ok(false);
            } else {
                self.data.sorted_branches[branch_no].done = true;
            }
        }

        Ok(true)
    }

    async fn next_branch(
        &mut self,
        branch_no: usize,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        let previous_branch = req.set_active_query_branch(Some(branch_no));
        let result = self.branches[branch_no].next(req, handle).await;
        req.set_active_query_branch(previous_branch);
        result
    }

    fn smallest_sorted_branch(&self) -> Option<usize> {
        let mut smallest: Option<usize> = None;

        for branch_no in 0..self.data.sorted_branches.len() {
            let Some(candidate) = self.data.sorted_branches[branch_no].current_result.as_ref()
            else {
                continue;
            };

            let Some(current_smallest) = smallest else {
                smallest = Some(branch_no);
                continue;
            };

            let current = self.data.sorted_branches[current_smallest]
                .current_result
                .as_ref()
                .unwrap();
            let cmp = compare_results(candidate, current, &self.sort_fields, &self.sort_specs);
            if cmp == Ordering::Less
                || (cmp == Ordering::Equal
                    && self.data.sorted_branches[branch_no].branch_no
                        < self.data.sorted_branches[current_smallest].branch_no)
            {
                smallest = Some(branch_no);
            }
        }

        smallest
    }

    fn validate_sort_result(&self, mv: &MapValue) -> Result<(), NoSQLError> {
        for field in &self.sort_fields {
            if let Some(fv) = mv.get_field_value(field) {
                if !fv.is_atomic() {
                    return ia_err!("sort expression does not return a single atomic value");
                }
            }
        }
        Ok(())
    }
}
