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
use crate::plan_iter::{deserialize_plan_iters, PlanIter};
use crate::plan_iter::{FuncCode, Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::{FieldType, FieldValue};

use bigdecimal::BigDecimal;
use std::result::Result;
use tracing::debug;

// An instance of this iterator implements either addition/substraction among
// two or more input values, or multiplication/division among two or more input
// values. For example, arg1 + arg2 - arg3 + arg4, or arg1 * arg2 * arg3 / arg4.
//
// The only arithmetic op that is strictly needed for the driver is the div
// (real division) op, to compute an AVG aggregate function as the division of
// a SUM by a COUNT. However, having all the arithmetic ops implemented allows
// for expressions in the SELECT list that do arithmetic among aggregate
// functions (for example: select a, sum(x) + sum(y) from foo group by a).
#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct ArithOpIter {
    state: PlanIterState,
    func_code: FuncCode,
    arg_iters: Vec<Box<PlanIter>>,
    // If theCode == FuncCode.OP_ADD_SUB, theOps is a string of "+" and/or "-"
    // chars, containing one such char per input value. For example, if the
    // arithmetic expression is (arg1 + arg2 - arg3 + arg4) theOps is "++-+".
    //
    // If theCode == FuncCode.OP_MULT_DIV, theOps is a string of "*", "/",
    // and/or "d" chars, containing one such char per input value. For example,
    // if the arithmetic expression is (arg1 * arg2 * arg3 / arg4) theOps
    // is "***\/". The "d" char is used for the div operator.
    ops: Vec<u8>,
    init_result: i32,
    have_real_div: bool,
}

// We don't need to manually impl Clone because there's no variable data kept in
// this iterator other than its register (which is in the QueryRequest data).

impl ArithOpIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nArithOpIter: result_reg={} state_pos={}\n", rr, sp);
        let mut a = ArithOpIter {
            // fields common to all PlanIters
            result_reg: rr,
            state: PlanIterState::Uninitialized,
            loc: Location::from_reader(r)?,
            // Specific to ArithOpIter
            func_code: FuncCode::try_from_u16(r.read_u16()?)?,
            arg_iters: deserialize_plan_iters(r)?,
            ..Default::default()
        };
        let s = r.read_string()?;
        a.have_real_div = s.contains("d");
        a.ops = s.into_bytes();
        if a.func_code == FuncCode::OpAddSub {
            a.init_result = 0;
        } else {
            a.init_result = 1;
        }
        if a.ops.len() != a.arg_iters.len() {
            return ia_err!("ArithOpIter mismatched ops and args lengths");
        }
        Ok(a)
    }

    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Open;
        for i in &mut self.arg_iters {
            i.open(req, handle)?;
        }
        Ok(())
    }
    /*
        pub fn display_content(&self, sb: &mut String, f: &PlanFormatter) {
            f.print_indent(sb);
            // TODO: how to display content without query request?
            sb.push_str(format!("{:?}", self).as_str());
        }
        pub fn get_plan(&self) -> String {
            format!("{:?}", self)
        }
    */
    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::ArithOp
    }
    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.state == PlanIterState::Done {
            return Ok(false);
        }
        // Determine the type of the result for the expression by iterating
        // its components, enforcing the promotion rules for numeric types.
        //
        // Start with INTEGER, unless we have any div operator, in which case
        // start with DOUBLE.
        let mut result_type = FieldType::Integer;
        if self.have_real_div {
            result_type = FieldType::Double;
        }

        for i in 0..self.arg_iters.len() {
            if self.arg_iters[i].next(req, handle).await? == false {
                self.state = PlanIterState::Done;
                return Ok(false);
            }
            // Take the value from the register...
            debug!(
                "AOI{} arg_iter[{}].kind={:?}",
                self.result_reg,
                i,
                self.arg_iters[i].get_kind()
            );
            let arg_value = self.arg_iters[i].get_result(req);
            debug!(
                "AOI{} arg_iter[{}].result={:?}",
                self.result_reg, i, arg_value
            );
            if arg_value.is_null() {
                self.set_result(req, FieldValue::Null);
                self.state = PlanIterState::Done;
                return Ok(true);
            }
            let arg_type = arg_value.get_type();
            // ...then put it back after we got its type
            self.arg_iters[i].set_result(req, arg_value);
            match arg_type {
                FieldType::Integer => (),
                FieldType::Long => {
                    if result_type == FieldType::Integer {
                        result_type = FieldType::Long;
                    }
                }
                FieldType::Double => {
                    if result_type == FieldType::Integer || result_type == FieldType::Long {
                        result_type = FieldType::Double;
                    }
                }
                FieldType::Number => {
                    result_type = FieldType::Number;
                }
                _ => {
                    return ia_err!(
                        "operand in arithmentic operation has illegal type\n \
						operand: {} type: {:?} location: {:?}",
                        i,
                        arg_type,
                        self.loc
                    );
                }
            }
        }
        let mut i_res: i32 = self.init_result;
        let mut l_res: i64 = self.init_result as i64;
        let mut d_res: f64 = self.init_result as f64;
        let mut n_res = BigDecimal::default() + self.init_result;
        for i in 0..self.arg_iters.len() {
            let arg_value = self.arg_iters[i].get_result(req);
            if arg_value == FieldValue::Uninitialized {
                return ia_err!("found uninitialized field value in arg register {i}");
            }
            if self.func_code == FuncCode::OpAddSub {
                if self.ops[i] == b'+' {
                    match result_type {
                        FieldType::Integer => {
                            i_res += arg_value.as_i32()?;
                        }
                        FieldType::Long => {
                            l_res += arg_value.as_i64()?;
                        }
                        FieldType::Double => {
                            d_res += arg_value.as_f64()?;
                        }
                        FieldType::Number => {
                            n_res += arg_value.as_big_decimal()?;
                        }
                        _ => {
                            return ia_err!("invalid result type: {:?}", result_type);
                        }
                    }
                } else {
                    match result_type {
                        FieldType::Integer => {
                            i_res -= arg_value.as_i32()?;
                        }
                        FieldType::Long => {
                            l_res -= arg_value.as_i64()?;
                        }
                        FieldType::Double => {
                            d_res -= arg_value.as_f64()?;
                        }
                        FieldType::Number => {
                            n_res -= arg_value.as_big_decimal()?;
                        }
                        _ => {
                            return ia_err!("invalid result type: {:?}", result_type);
                        }
                    }
                }
            } else {
                if self.ops[i] == b'*' {
                    match result_type {
                        FieldType::Integer => {
                            i_res *= arg_value.as_i32()?;
                        }
                        FieldType::Long => {
                            l_res *= arg_value.as_i64()?;
                        }
                        FieldType::Double => {
                            d_res *= arg_value.as_f64()?;
                        }
                        FieldType::Number => {
                            n_res *= arg_value.as_big_decimal()?;
                        }
                        _ => {
                            return ia_err!("invalid result type: {:?}", result_type);
                        }
                    }
                } else if self.ops[i] == b'/' {
                    match result_type {
                        FieldType::Integer => {
                            i_res /= arg_value.as_i32()?;
                        }
                        FieldType::Long => {
                            l_res /= arg_value.as_i64()?;
                        }
                        FieldType::Double => {
                            d_res /= arg_value.as_f64()?;
                        }
                        FieldType::Number => {
                            n_res = n_res / arg_value.as_big_decimal()?;
                        }
                        _ => {
                            return ia_err!("invalid result type: {:?}", result_type);
                        }
                    }
                } else {
                    match result_type {
                        FieldType::Double => {
                            d_res /= arg_value.as_f64()?;
                        }
                        FieldType::Number => {
                            n_res = n_res / arg_value.as_big_decimal()?;
                        }
                        _ => {
                            return ia_err!(
                                "invalid result type: {:?} (i={} ops={:?}",
                                result_type,
                                i,
                                self.ops
                            );
                        }
                    }
                }
            }
        }
        match result_type {
            FieldType::Integer => {
                self.set_result(req, FieldValue::Integer(i_res));
            }
            FieldType::Long => {
                self.set_result(req, FieldValue::Long(l_res));
            }
            FieldType::Double => {
                self.set_result(req, FieldValue::Double(d_res));
            }
            FieldType::Number => {
                self.set_result(req, FieldValue::Number(n_res));
            }
            _ => {
                return ia_err!("invalid result type: {:?}", result_type);
            }
        }
        self.state = PlanIterState::Done;
        Ok(true)
    }
    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("AOI{} get_result={:?}", self.result_reg, fv);
        fv
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("AOI{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.state = PlanIterState::Uninitialized;
        for i in 0..self.arg_iters.len() {
            self.arg_iters[i].reset()?;
        }
        Ok(())
    }
    /*
        pub fn close(&mut self, req: &mut QueryRequest) -> Result<(), NoSQLError> {
            self.state = PlanIterState::Closed;
            for i in 0..self.arg_iters.len() {
                self.arg_iters[i].close(req)?;
            }
            Ok(())
        }
    */
    pub fn get_state(&self) -> PlanIterState {
        self.state
    }
    /*
        pub fn get_func_code(&self) -> Option<FuncCode> {
            Some(self.func_code)
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
