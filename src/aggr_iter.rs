//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::error::ia_err;
use crate::error::NoSQLError;
use crate::handle::Handle;
use crate::plan_iter::{deserialize_plan_iter, PlanIter};
use crate::plan_iter::{FuncCode, Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::bd_try_from_f64;
use crate::types::compare_atomics_total_order;
use crate::types::{FieldType, FieldValue, NoSQLColumnFromFieldValue};
use oracle_nosql_rust_sdk_derive::add_planiter_fields;

use bigdecimal::BigDecimal;
use std::cmp::Ordering;
use std::result::Result;
use tracing::debug;

// AggrIterData is a struct that is common to several aggregation iterators.
#[derive(Debug)]
struct AggrIterData {
    state: PlanIterState,
    count: i64,
    long_sum: i64,
    double_sum: f64,
    number_sum: BigDecimal,
    sum_type: FieldType,
    null_input_only: bool,
    min_max: FieldValue,
}

impl Clone for AggrIterData {
    // clone of iter data never copies its actual data
    fn clone(&self) -> Self {
        AggrIterData::default()
    }
    fn clone_from(&mut self, _source: &Self) {
        self.reset();
    }
}

impl Default for AggrIterData {
    fn default() -> Self {
        AggrIterData {
            state: PlanIterState::Uninitialized,
            count: 0,
            long_sum: 0,
            double_sum: 0.0,
            number_sum: BigDecimal::default(),
            sum_type: FieldType::Long,
            null_input_only: true,
            min_max: FieldValue::Null,
        }
    }
}

impl AggrIterData {
    fn reset(&mut self) {
        // replace self with default
        let _ = std::mem::take(self);
    }
}

// Implements the SUM aggregate function. It is needed by the driver to
// re-sum partial sums and counts received from the proxy.
//
// Note: The next() method does not actually return a value; it just adds a new
// value (if it is of a numeric type) to the running sum kept in the state. Also
// the reset() method resets the input iter (so that the next input value can be
// computed), but does not reset the FuncSumState. The state is reset, and the
// current sum value is returned, by the getAggrValue() method.
#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct FuncSumIter {
    input_iter: Box<PlanIter>,
    data: AggrIterData,
}

impl FuncSumIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nFuncSumIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(FuncSumIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // fields specific to FuncSumIter
            input_iter: deserialize_plan_iter(r)?,

            data: AggrIterData::default(),
        })
    }

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
        PlanIterKind::SumFunc
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
            if self.input_iter.next(req, handle).await? == false {
                return Ok(true);
            }
            let val = self.input_iter.get_result(req);
            //if (rcb.getTraceLevel() >= 2) {
            //rcb.trace("Summing up value " + val);
            //}
            if val.is_null() {
                continue;
            }
            self.data.null_input_only = false;
            self.sum_new_value(val)?;
        }
    }

    fn sum_new_value(&mut self, val: FieldValue) -> Result<(), NoSQLError> {
        if val.get_type() == FieldType::Integer {
            self.data.count += 1;
            match self.data.sum_type {
                FieldType::Long => {
                    self.data.long_sum += i32::from_field(&val)? as i64;
                }
                FieldType::Double => {
                    self.data.double_sum += i32::from_field(&val)? as f64;
                }
                FieldType::Number => {
                    self.data.number_sum += i32::from_field(&val)?;
                }
                _ => {
                    return ia_err!("invalid sum_type in FuncSumIter: {:?}", self.data.sum_type);
                }
            }
            return Ok(());
        } else if val.get_type() == FieldType::Long {
            self.data.count += 1;
            match self.data.sum_type {
                FieldType::Long => {
                    self.data.long_sum += i64::from_field(&val)?;
                }
                FieldType::Double => {
                    self.data.double_sum += i64::from_field(&val)? as f64;
                }
                FieldType::Number => {
                    self.data.number_sum += i64::from_field(&val)?;
                }
                _ => {
                    return ia_err!("invalid sum_type in FuncSumIter: {:?}", self.data.sum_type);
                }
            }
            return Ok(());
        } else if val.get_type() == FieldType::Double {
            self.data.count += 1;
            match self.data.sum_type {
                FieldType::Long => {
                    self.data.double_sum += self.data.long_sum as f64;
                    self.data.double_sum += f64::from_field(&val)?;
                    self.data.sum_type = FieldType::Double;
                }
                FieldType::Double => {
                    self.data.double_sum += f64::from_field(&val)?;
                }
                FieldType::Number => {
                    let fval = f64::from_field(&val)?;
                    self.data.number_sum += bd_try_from_f64(fval)?;
                }
                _ => {
                    return ia_err!("invalid sum_type in FuncSumIter: {:?}", self.data.sum_type);
                }
            }
            return Ok(());
        } else if val.get_type() == FieldType::Number {
            self.data.count += 1;
            match self.data.sum_type {
                FieldType::Long => {
                    self.data.number_sum += self.data.long_sum;
                    self.data.number_sum += BigDecimal::from_field(&val)?;
                    self.data.sum_type = FieldType::Number;
                }
                FieldType::Double => {
                    self.data.number_sum = bd_try_from_f64(self.data.double_sum)?;
                    self.data.number_sum += BigDecimal::from_field(&val)?;
                    self.data.sum_type = FieldType::Number;
                }
                FieldType::Number => {
                    self.data.number_sum += BigDecimal::from_field(&val)?;
                }
                _ => {
                    return ia_err!("invalid sum_type in FuncSumIter: {:?}", self.data.sum_type);
                }
            }
            return Ok(());
        }
        // silently skip all other input types
        Ok(())
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("AI{} get_result={:?}", self.result_reg, fv);
        fv
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("AI{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.input_iter.reset()
        // Don't reset the state of "self". Resetting the state is done in
        // method get_aggr_value below.
        // self.data.reset();
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
    // This method is called twice when a group completes and a new group
    // starts. In both cases it returns the current value of the SUM that is
    // stored in the FuncSumState. The 1st time, the SUM value is the final
    // SUM value for the just completed group. In this case the "reset" param
    // is true in order to reset the running sum in the state. The 2nd time
    // the SUM value is the inital SUM value computed from the 1st tuple of
    // the new group.
    pub fn get_aggr_value(
        &mut self,
        _req: &QueryRequest,
        reset: bool,
    ) -> Result<Option<FieldValue>, NoSQLError> {
        if self.data.null_input_only {
            return Ok(Some(FieldValue::Null));
        }
        let val: FieldValue;
        match self.data.sum_type {
            FieldType::Long => {
                val = FieldValue::Long(self.data.long_sum);
            }
            FieldType::Double => {
                val = FieldValue::Double(self.data.double_sum);
            }
            FieldType::Number => {
                val = FieldValue::Number(self.data.number_sum.clone());
            }
            _ => {
                return ia_err!(
                    "invalid result type for Sum function: {:?}",
                    self.data.sum_type
                );
            }
        }
        //if (rcb.getTraceLevel() >= 4) {
        //rcb.trace("Computed sum = " + res);
        //}
        if reset {
            self.data.reset();
        }
        Ok(Some(val))
    }
}

// Implements the MIN/MAX aggregate functions. It is needed by the driver
// to compute the total min/max from the partial mins/maxs received from the
// proxy.
#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct FuncMinMaxIter {
    input_iter: Box<PlanIter>,
    func_code: FuncCode,
    data: AggrIterData,
}

impl FuncMinMaxIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let _ = r.read_i32()?; // state_pos
        Ok(FuncMinMaxIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // fields specific to FuncMinMaxIter
            func_code: FuncCode::try_from_u16(r.read_u16()?)?,
            input_iter: deserialize_plan_iter(r)?,

            data: AggrIterData::default(),
        })
    }

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
        PlanIterKind::MinMaxFunc
    }
    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            return Ok(false);
        }

        debug!(
            "MMF{} input_iter.kind={:?}",
            self.result_reg,
            self.input_iter.get_kind()
        );
        loop {
            if self.input_iter.next(req, handle).await? == false {
                return Ok(true);
            }
            let val = self.input_iter.get_result(req);
            //if (rcb.getTraceLevel() >= 2) {
            //rcb.trace("Summing up value " + val);
            //}
            debug!("MMF{} summing value={:?}", self.result_reg, val);
            self.min_max_new_value(val)?;
        }
    }

    fn min_max_new_value(&mut self, val: FieldValue) -> Result<(), NoSQLError> {
        match val.get_type() {
            FieldType::Binary
            | FieldType::Array
            | FieldType::Map
            | FieldType::Null
            | FieldType::Empty
            | FieldType::JsonNull => {
                return Ok(());
            }
            _ => (),
        }
        if self.data.min_max == FieldValue::Null {
            self.data.min_max = val;
            return Ok(());
        }
        let cmp = compare_atomics_total_order(&val, &self.data.min_max, false);
        debug!(
            "MMF{} comp({:?}, {:?})={:?}",
            self.result_reg, val, self.data.min_max, cmp
        );
        if self.func_code == FuncCode::FnMin {
            if cmp != Ordering::Less {
                return Ok(());
            }
        } else {
            if cmp != Ordering::Greater {
                return Ok(());
            }
        }
        debug!("MMF{} setting min_max={:?}", self.result_reg, val);
        self.data.min_max = val;
        Ok(())
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        req.get_result(self.result_reg)
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        req.set_result(self.result_reg, result);
    }
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.input_iter.reset()
        // Don't reset the state of "self". Resetting the state is done in
        // method get_aggr_value below.
        // self.data.reset();
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
            Some(self.func_code)
        }
    */
    pub fn get_aggr_value(
        &mut self,
        _req: &QueryRequest,
        reset: bool,
    ) -> Result<Option<FieldValue>, NoSQLError> {
        if reset {
            self.reset()?;
            self.data.state = PlanIterState::Uninitialized;
            return Ok(Some(std::mem::take(&mut self.data.min_max)));
        }
        Ok(Some(self.data.min_max.clone_internal()))
    }
}
