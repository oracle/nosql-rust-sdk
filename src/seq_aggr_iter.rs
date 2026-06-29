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
use crate::types::{bd_try_from_f64, compare_atomics_total_order, FieldType, FieldValue};

use bigdecimal::BigDecimal;
use std::cmp::Ordering;
use std::result::Result;
use tracing::debug;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct FuncSeqAggrIter {
    input_iter: Box<PlanIter>,
    code: FuncCode,
    data: SeqAggrData,
}

#[derive(Debug)]
struct SeqAggrData {
    state: PlanIterState,
    count: i64,
    long_sum: i64,
    double_sum: f64,
    number_sum: BigDecimal,
    sum_type: FieldType,
    got_numeric_input: bool,
    min_max: FieldValue,
}

impl Clone for SeqAggrData {
    fn clone(&self) -> Self {
        SeqAggrData::default()
    }
    fn clone_from(&mut self, _source: &Self) {
        self.reset();
    }
}

impl Default for SeqAggrData {
    fn default() -> Self {
        SeqAggrData {
            state: PlanIterState::Uninitialized,
            count: 0,
            long_sum: 0,
            double_sum: 0.0,
            number_sum: BigDecimal::default(),
            sum_type: FieldType::Long,
            got_numeric_input: false,
            min_max: FieldValue::Null,
        }
    }
}

impl SeqAggrData {
    fn reset(&mut self) {
        *self = SeqAggrData::default();
    }
}

impl FuncSeqAggrIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        let rr = r.read_i32()?;
        let sp = r.read_i32()?;
        debug!("\nFuncSeqAggrIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(FuncSeqAggrIter {
            result_reg: rr,
            loc: Location::from_reader(r)?,
            code: FuncCode::try_from_u16(r.read_i16()? as u16)?,
            input_iter: deserialize_plan_iter(r)?,
            data: SeqAggrData::default(),
        })
    }

    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.data.state = PlanIterState::Open;
        self.input_iter.open(req, handle)
    }

    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::SeqAggr
    }

    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            return Ok(false);
        }

        let mut more = self.input_iter.next(req, handle).await?;
        if !more {
            self.data.state = PlanIterState::Done;
            if matches!(self.code, FuncCode::FnSeqCount | FuncCode::FnSeqCountI) {
                self.set_result(req, FieldValue::Long(0));
                return Ok(true);
            }
            return Ok(false);
        }

        while more {
            let val = self.input_iter.get_result(req);
            match self.code {
                FuncCode::FnSeqCount | FuncCode::FnSeqCountI => self.next_count(val)?,
                FuncCode::FnSeqCountNumbersI => self.next_count_numbers(val),
                FuncCode::FnSeqSum | FuncCode::FnSeqAvg => self.sum_new_value(val)?,
                FuncCode::FnSeqMin
                | FuncCode::FnSeqMax
                | FuncCode::FnSeqMinI
                | FuncCode::FnSeqMaxI => {
                    if self.minmax_new_value(val)? {
                        break;
                    }
                }
                _ => return ia_err!("invalid function code for SeqAggrIter: {:?}", self.code),
            }
            more = self.input_iter.next(req, handle).await?;
        }

        let result = self.finish_value()?;
        self.set_result(req, result);
        self.data.state = PlanIterState::Done;
        Ok(true)
    }

    fn next_count(&mut self, val: FieldValue) -> Result<(), NoSQLError> {
        if val.is_null() {
            if self.code == FuncCode::FnSeqCount {
                self.data.min_max = FieldValue::Null;
                self.data.got_numeric_input = false;
                self.data.count = -1;
            }
            return Ok(());
        }
        if self.data.count >= 0 {
            self.data.count += 1;
        }
        Ok(())
    }

    fn next_count_numbers(&mut self, val: FieldValue) {
        if val.is_numeric() {
            self.data.count += 1;
        }
    }

    fn sum_new_value(&mut self, val: FieldValue) -> Result<(), NoSQLError> {
        if val.is_null() || !val.is_numeric() {
            return Ok(());
        }
        self.data.got_numeric_input = true;
        self.data.count += 1;
        match val {
            FieldValue::Integer(i) => match self.data.sum_type {
                FieldType::Long => self.data.long_sum += i as i64,
                FieldType::Double => self.data.double_sum += i as f64,
                FieldType::Number => self.data.number_sum += i,
                _ => return ia_err!("invalid sum type: {:?}", self.data.sum_type),
            },
            FieldValue::Long(l) => match self.data.sum_type {
                FieldType::Long => self.data.long_sum += l,
                FieldType::Double => self.data.double_sum += l as f64,
                FieldType::Number => self.data.number_sum += l,
                _ => return ia_err!("invalid sum type: {:?}", self.data.sum_type),
            },
            FieldValue::Double(d) => match self.data.sum_type {
                FieldType::Long => {
                    self.data.double_sum = self.data.long_sum as f64 + d;
                    self.data.sum_type = FieldType::Double;
                }
                FieldType::Double => self.data.double_sum += d,
                FieldType::Number => self.data.number_sum += bd_try_from_f64(d)?,
                _ => return ia_err!("invalid sum type: {:?}", self.data.sum_type),
            },
            FieldValue::Number(n) => match self.data.sum_type {
                FieldType::Long => {
                    self.data.number_sum += self.data.long_sum;
                    self.data.number_sum += n;
                    self.data.sum_type = FieldType::Number;
                }
                FieldType::Double => {
                    self.data.number_sum = bd_try_from_f64(self.data.double_sum)?;
                    self.data.number_sum += n;
                    self.data.sum_type = FieldType::Number;
                }
                FieldType::Number => self.data.number_sum += n,
                _ => return ia_err!("invalid sum type: {:?}", self.data.sum_type),
            },
            _ => (),
        }
        Ok(())
    }

    fn minmax_new_value(&mut self, val: FieldValue) -> Result<bool, NoSQLError> {
        if val.is_null() && matches!(self.code, FuncCode::FnSeqMin | FuncCode::FnSeqMax) {
            self.data.min_max = FieldValue::Null;
            return Ok(true);
        }
        match val.get_type() {
            FieldType::Binary
            | FieldType::Array
            | FieldType::Map
            | FieldType::Null
            | FieldType::Empty
            | FieldType::JsonNull => return Ok(false),
            _ => (),
        }
        if self.data.min_max == FieldValue::Null {
            self.data.min_max = val;
            return Ok(false);
        }
        let cmp = compare_atomics_total_order(&val, &self.data.min_max, false);
        if matches!(self.code, FuncCode::FnSeqMin | FuncCode::FnSeqMinI) {
            if cmp == Ordering::Less {
                self.data.min_max = val;
            }
        } else if cmp == Ordering::Greater {
            self.data.min_max = val;
        }
        Ok(false)
    }

    fn finish_value(&self) -> Result<FieldValue, NoSQLError> {
        match self.code {
            FuncCode::FnSeqCount => {
                if self.data.count < 0 {
                    Ok(FieldValue::Null)
                } else {
                    Ok(FieldValue::Long(self.data.count))
                }
            }
            FuncCode::FnSeqCountI | FuncCode::FnSeqCountNumbersI => {
                Ok(FieldValue::Long(self.data.count))
            }
            FuncCode::FnSeqSum | FuncCode::FnSeqAvg => self.finish_sum_avg(),
            FuncCode::FnSeqMin | FuncCode::FnSeqMax | FuncCode::FnSeqMinI | FuncCode::FnSeqMaxI => {
                Ok(self.data.min_max.clone_internal())
            }
            _ => ia_err!("invalid function code for SeqAggrIter: {:?}", self.code),
        }
    }

    fn finish_sum_avg(&self) -> Result<FieldValue, NoSQLError> {
        if !self.data.got_numeric_input {
            return Ok(FieldValue::Null);
        }

        if self.code == FuncCode::FnSeqAvg {
            return match self.data.sum_type {
                FieldType::Long => Ok(FieldValue::Double(
                    self.data.long_sum as f64 / self.data.count as f64,
                )),
                FieldType::Double => Ok(FieldValue::Double(
                    self.data.double_sum / self.data.count as f64,
                )),
                FieldType::Number => Ok(FieldValue::Number(
                    self.data.number_sum.clone() / BigDecimal::from(self.data.count),
                )),
                _ => ia_err!("invalid sum type: {:?}", self.data.sum_type),
            };
        }

        match self.data.sum_type {
            FieldType::Long => Ok(FieldValue::Long(self.data.long_sum)),
            FieldType::Double => Ok(FieldValue::Double(self.data.double_sum)),
            FieldType::Number => Ok(FieldValue::Number(self.data.number_sum.clone())),
            _ => ia_err!("invalid sum type: {:?}", self.data.sum_type),
        }
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("SA{} get_result={:?}", self.result_reg, fv);
        fv
    }

    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("SA{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }

    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.input_iter.reset()?;
        self.data.reset();
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
}
