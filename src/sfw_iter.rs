//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use oracle_nosql_rust_sdk_derive::add_planiter_fields;

use crate::error::ia_err;
use crate::error::NoSQLError;
use crate::handle::Handle;
use crate::plan_iter::{deserialize_plan_iter, deserialize_plan_iters};
use crate::plan_iter::{Location, PlanIter, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::{FieldValue, MapValue};

use std::result::Result;
use tracing::debug;

// SfwIter is used for:
// (a) project out result columns that do not appear in the SELECT list of
//     the query, but are included in the results fetched from the proxy,
//     because the are order-by columns or primary-key columns used for
//     duplicate elimination.
// (b) For group-by and aggregation queries, regroup and reaggregate the
//     partial gropus/aggregates received from the proxy.
// (c) implement offset and limit.
//
// Note: "sfw" = "Select From Where", but that doesn't really seem to
// line up with what this does?
#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct SfwIter {
    from_iter: Box<PlanIter>,
    from_var_name: String,
    column_iters: Vec<Box<PlanIter>>,
    column_names: Vec<String>,
    is_select_star: bool,
    num_gb_columns: i32,
    offset_iter: Box<PlanIter>,
    limit_iter: Box<PlanIter>,

    data: SfwIterData,
}

impl SfwIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nSfwIter: result_reg={} state_pos={}\n", rr, sp);
        let s = SfwIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // specific to SfwIter
            column_names: r.read_string_array()?,
            num_gb_columns: r.read_i32()?,
            from_var_name: r.read_string()?,
            is_select_star: r.read_bool()?,
            column_iters: deserialize_plan_iters(r)?,
            from_iter: deserialize_plan_iter(r)?,
            offset_iter: deserialize_plan_iter(r)?,
            limit_iter: deserialize_plan_iter(r)?,

            ..Default::default()
        };
        debug!("SFW{}: from_iter={:?}", s.result_reg, s.from_iter);
        Ok(s)
    }
}

#[derive(Debug, Default)]
struct SfwIterData {
    state: PlanIterState,
    offset: i64,
    limit: i64,
    num_results: i64,
    gb_tuple: Vec<FieldValue>,
    orig_offset: i64, // from offset iterator
    orig_limit: i64,  // from limit iterator
}

impl Clone for SfwIterData {
    // clone of iter data never copies its actual data
    fn clone(&self) -> Self {
        let mut s = SfwIterData::default();
        s.reset();
        s
    }
    fn clone_from(&mut self, _source: &Self) {
        self.reset();
    }
}

impl SfwIterData {
    fn reset(&mut self) {
        debug!("SfwIterData:data.reset()");
        self.state = PlanIterState::Uninitialized;
        self.num_results = 0;
        self.gb_tuple = Vec::new();
        self.offset = self.orig_offset;
        self.limit = self.orig_limit;
    }
}

impl SfwIter {
    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.data.state = PlanIterState::Open;
        self.from_iter.open(req, handle)?;
        for i in &mut self.column_iters {
            i.open(req, handle)?;
        }
        self.compute_offset_limit(req, handle)?;
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
        PlanIterKind::Sfw
    }
    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("SFW{} get_result={:?}", self.result_reg, fv);
        fv
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("SFW{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }
    pub fn get_state(&self) -> PlanIterState {
        self.data.state.clone()
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
    fn done(&mut self) {
        self.data.state = PlanIterState::Done;
        //self.data.reset();
    }
    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            debug!("SfwIter.next(): already done");
            return Ok(false);
        }
        if self.data.num_results >= self.data.limit {
            debug!("SfwIter.next(): num results exceeds limit");
            self.done();
            return Ok(false);
        }
        // while loop for skipping offset results
        loop {
            debug!("SFW{} next(): computing next result", self.result_reg);
            let more = self.compute_next_result(req, handle).await?;
            if more == false {
                debug!("SWF{} next(): computing returned false", self.result_reg);
                return Ok(false);
            }
            // Even though we have a result, the state may be DONE. This is the
            // case when the result is the last group tuple in a grouping SFW.
            // In this case, if we have not reached the offset yet, we should
            // ignore this result and return false.
            if self.data.state.is_done() && self.data.offset > 0 {
                return Ok(false);
            }
            if self.data.offset == 0 {
                self.data.num_results += 1;
                break;
            }
            self.data.offset -= 1;
        }
        Ok(true)
    }

    async fn compute_next_result(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        debug!("SFW{} compute_next_result: looping...", self.result_reg);
        // while loop for group by
        loop {
            let mut more = self.from_iter.next(req, handle).await?;
            if more == false {
                if req.reached_limit == false {
                    debug!(
                        "SFW{} compute_next(): reached limit == false: setting done()",
                        self.result_reg
                    );
                    self.done();
                }
                debug!(
                    "SFW{}: more==false num_gb_cols={}",
                    self.result_reg, self.num_gb_columns
                );
                if self.num_gb_columns >= 0 {
                    return self.produce_last_group(req, handle);
                }
                return Ok(false);
            }

            // Compute the exprs in the SELECT list. If this is a grouping
            // SFW, compute only the group-by columns. However, skip this
            // computation if this is not a grouping SFW and it has an offset
            // that has not been reached yet.
            if self.num_gb_columns < 0 && self.data.offset > 0 {
                debug!("SFW{}: offset={}", self.result_reg, self.data.offset);
                return Ok(true);
            }
            let mut num_cols = self.column_iters.len();
            if self.num_gb_columns >= 0 {
                num_cols = self.num_gb_columns as usize;
            }

            debug!(
                "SFW{} num_cols={} col_iters.len()={} num_gb_cols={}",
                self.result_reg,
                num_cols,
                self.column_iters.len(),
                self.num_gb_columns
            );
            let mut i = 0;
            while i < num_cols {
                more = self.column_iters[i].next(req, handle).await?;
                if more == false {
                    if self.num_gb_columns > 0 {
                        debug!(
                            "SFW{} col={} more==false, num_gb_columns={}, resetting",
                            self.result_reg, i, self.num_gb_columns
                        );
                        self.column_iters[i].reset()?;
                        break;
                    }
                    // TODO: why Null here?
                    debug!(
                        "SFW{} col={} more==false, num_gb_columns={}, setting result to Null",
                        self.result_reg, i, self.num_gb_columns
                    );
                    self.column_iters[i].set_result(req, FieldValue::Null);
                }
                debug!("SFW{} col={} resetting column_iter", self.result_reg, i);
                self.column_iters[i].reset()?;
                i += 1;
            }

            if i < num_cols {
                debug!("SFW{} i={}, continuing", self.result_reg, i);
                continue;
            }

            if self.num_gb_columns < 0 {
                if self.is_select_star {
                    break;
                }
                let mut m = MapValue::new();
                for i in 0..self.column_iters.len() {
                    debug!(
                        "SFW{} col[{}]='{}' kind={:?}",
                        self.result_reg,
                        i,
                        self.column_names[i],
                        self.column_iters[i].get_kind()
                    );
                    m.put_field_value(
                        self.column_names[i].as_str(),
                        self.column_iters[i].get_result(req),
                    );
                }
                debug!("SFW{} num_gb_cols<0: set result={:?}", self.result_reg, m);
                self.set_result(req, FieldValue::Map(m));
                break;
            }

            if self.group_input_tuple(req, handle).await? {
                break;
            }
        }

        Ok(true)
    }

    // This method checks whether the current input tuple (a) starts the
    // first group, i.e. it is the very 1st tuple in the input stream, or
    // (b) belongs to the current group, or (c) starts a new group otherwise.
    // The method returns true in case (c), indicating that an output tuple
    // is ready to be returned to the consumer of this SFW. Otherwise, false
    // is returned.
    async fn group_input_tuple(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        let num_cols = self.column_iters.len();
        let gb_cols = self.num_gb_columns as usize;

        debug!(
            "SFW{} GIT: num_cols={num_cols} gb_cols={gb_cols} tuple_len={}",
            self.result_reg,
            self.data.gb_tuple.len()
        );
        // If this is the very first input tuple, start the first group and
        // go back to compute next input tuple.
        if self.data.gb_tuple.len() == 0 {
            for _i in 0..num_cols {
                self.data.gb_tuple.push(FieldValue::Uninitialized);
            }

            for i in 0..gb_cols {
                self.data.gb_tuple[i] = self.column_iters[i].get_result(req);
            }

            for i in gb_cols..num_cols {
                let _ = self.column_iters[i].next(req, handle).await?;
                let _ = self.column_iters[i].reset()?;
            }

            return Ok(false);
        }

        // Compare the current input tuple with the current group tuple.
        let mut j = 0;
        for i in 0..gb_cols {
            // move the value out of the column iterator
            let newval = self.column_iters[j].get_result(req);
            let curval = &self.data.gb_tuple[j];
            let equals = &newval == curval;
            debug!(
                "comparing col={} {:?} to {:?} returns {}",
                i, newval, curval, equals
            );
            // ...and then put it back
            self.column_iters[j].set_result(req, newval);
            if equals == false {
                break;
            }
            j += 1;
        }

        debug!("j={j} gb_cols={gb_cols}");

        // If the input tuple is in current group, update the aggregate
        // functions and go back to compute the next input tuple.
        if j == gb_cols {
            for i in gb_cols..num_cols {
                let _ = self.column_iters[i].next(req, handle).await?;
                let _ = self.column_iters[i].reset()?;
            }

            return Ok(false);
        }

        // Input tuple starts new group. We must finish up the current group,
        // produce a result (output tuple) from it, and init the new group.
        debug!("SFW{} new group: aggregating", self.result_reg);

        // 1. Get the final aggregate values for the current group and store
        //    them in theGBTuple.
        for i in gb_cols..num_cols {
            debug!(
                "SFW{} column_iters[{}].kind={:?}",
                self.result_reg,
                i,
                self.column_iters[i].get_kind()
            );
            if let Some(v) = self.column_iters[i].get_aggr_value(req, true)? {
                debug!(
                    "SFW{} aggr_value for column {i} is {:?}",
                    self.result_reg, v
                );
                self.data.gb_tuple[i] = v;
            } else {
                return ia_err!("no aggr value in column iterator");
            }
        }

        // 2. Create a result MapValue out of the GB tuple
        let mut m = MapValue::new();
        for i in 0..num_cols {
            m.put_field_value(
                self.column_names[i].as_str(),
                std::mem::take(&mut self.data.gb_tuple[i]),
            );
        }
        debug!("SFW{} new MV={:?}", self.result_reg, m);
        self.set_result(req, FieldValue::Map(m));

        // 3. Put the values of the grouping columns into the GB tuple
        for i in 0..gb_cols {
            self.data.gb_tuple[i] = self.column_iters[i].get_result(req);
            debug!(
                "SFW{} gb_tuple[{}]={:?}",
                self.result_reg, i, self.data.gb_tuple[i]
            );
        }

        // 4. Compute the values of the aggregate functions.
        for i in gb_cols..num_cols as usize {
            debug!("SFW{} compute aggr i={}", self.result_reg, i);
            let _ = self.column_iters[i].next(req, handle).await?;
            let _ = self.column_iters[i].reset()?;
        }

        Ok(true)
    }

    fn produce_last_group(
        &mut self,
        req: &mut QueryRequest,
        _handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if req.reached_limit {
            debug!("SFW{} PLG: reached limit", self.result_reg);
            return Ok(false);
        }

        // If there is no group, return false.
        if self.data.gb_tuple.len() == 0 {
            debug!("SFW{} PLG: no last group", self.result_reg);
            return Ok(false);
        }
        let num_cols = self.column_iters.len();
        let gb_cols = self.num_gb_columns as usize;

        let mut m = MapValue::new();
        for i in 0..gb_cols as usize {
            m.put_field_value(
                &self.column_names[i],
                std::mem::take(&mut self.data.gb_tuple[i]),
            );
        }

        for i in gb_cols..num_cols {
            if let Some(v) = self.column_iters[i].get_aggr_value(req, true)? {
                debug!(
                    "SFW{} aggr_value for column {i} is {:?}",
                    self.result_reg, v
                );
                m.put_field_value(&self.column_names[i], v);
            } else {
                return ia_err!("no aggr value in column iterator");
            }
        }
        debug!("SFW{} PLG: result={:?}", self.result_reg, m);
        self.set_result(req, FieldValue::Map(m));

        return Ok(true);
    }

    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.from_iter.reset()?;
        for i in 0..self.column_iters.len() {
            self.column_iters[i].reset()?;
        }
        self.offset_iter.reset()?;
        self.limit_iter.reset()?;
        self.data.reset();

        Ok(())
    }

    /*
        pub fn close(&mut self, req: &mut QueryRequest) -> Result<(), NoSQLError> {
            self.from_iter.close(req)?;
            for i in 0..self.column_iters.len() {
                self.column_iters[i].close(req)?;
            }
            self.offset_iter.close(req)?;
            self.limit_iter.close(req)?;
            self.data.reset();
            Ok(())
        }
    */

    fn compute_offset_limit(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<(), NoSQLError> {
        let mut offset: i64 = 0;
        let mut limit: i64 = i32::MAX as i64;

        if self.offset_iter.get_kind() != PlanIterKind::Empty {
            if self.offset_iter.get_kind() != PlanIterKind::Const {
                return ia_err!("expected const iter for offset");
            }
            self.offset_iter.open(req, handle)?;
            // move value out of iterator
            let val = self.offset_iter.get_result(req);
            match val {
                FieldValue::Long(l) => offset = l,
                FieldValue::Integer(i) => offset = i as i64,
                _ => {
                    return ia_err!("got unexpected value for offset: {:?}", val);
                }
            }
            // ...and put it back
            self.offset_iter.set_result(req, val);
            if offset < 0 {
                return ia_err!("offset can not be a negative number");
            }
            if offset > i32::MAX as i64 {
                return ia_err!("offset can not be greater than i32.MAX");
            }
        }

        if self.limit_iter.get_kind() != PlanIterKind::Empty {
            if self.limit_iter.get_kind() != PlanIterKind::Const {
                return ia_err!("expected const iter for limit");
            }
            self.limit_iter.open(req, handle)?;
            // move value out of iterator
            let val = self.limit_iter.get_result(req);
            match val {
                FieldValue::Long(l) => limit = l,
                FieldValue::Integer(i) => limit = i as i64,
                _ => {
                    return ia_err!("got unexpected value for limit: {:?}", val);
                }
            }
            // ...and put it back
            self.limit_iter.set_result(req, val);
            if limit < 0 {
                return ia_err!("limit can not be a negative number");
            }
            if limit > i32::MAX as i64 {
                return ia_err!("limit can not be greater than i32.MAX");
            }
        }

        self.data.offset = offset;
        self.data.limit = limit;
        // to allow data.reset()
        self.data.orig_offset = offset;
        self.data.orig_limit = limit;
        Ok(())
    }
}
