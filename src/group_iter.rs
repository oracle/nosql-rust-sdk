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
use crate::plan_iter::deserialize_plan_iter;
use crate::plan_iter::{FuncCode, Location, PlanIter, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::{bd_try_from_f64, compare_atomics_total_order, FieldType, FieldValue, MapValue};

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::mem::take;
use std::result::Result;
use tracing::debug;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct GroupIter {
    input_iter: Box<PlanIter>,
    num_gb_columns: usize,
    column_names: Vec<String>,
    is_distinct: bool,
    aggr_funcs: Vec<FuncCode>,
    remove_produced_result: bool,
    count_memory: bool,

    data: GroupIterData,
}

impl GroupIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nGroupIter: result_reg={} state_pos={}\n", rr, sp);
        let mut gi = GroupIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // specific to GroupIter
            input_iter: deserialize_plan_iter(r)?,
            num_gb_columns: r.read_i32()? as usize,
            column_names: r.read_string_array()?,

            ..Default::default()
        };
        let num_aggrs = gi.column_names.len() - gi.num_gb_columns;
        gi.aggr_funcs = Vec::with_capacity(num_aggrs);
        for _i in 0..num_aggrs {
            let kvcode = r.read_i16()?;
            gi.aggr_funcs.push(FuncCode::try_from_u16(kvcode as u16)?);
        }
        gi.is_distinct = r.read_bool()?;
        gi.remove_produced_result = r.read_bool()?;
        gi.count_memory = r.read_bool()?;
        Ok(gi)
    }
}

#[derive(Debug, Default, PartialEq, PartialOrd, Eq, Ord)]
struct GroupTuple {
    values: Vec<FieldValue>,
}

impl GroupTuple {
    fn clone_internal(&self) -> Self {
        let mut v: Vec<FieldValue> = Vec::with_capacity(self.values.len());
        for i in &self.values {
            v.push(i.clone_internal());
        }
        GroupTuple { values: v }
    }
}

#[derive(Debug, Default, PartialEq, PartialOrd)]
enum AggrValueEnum {
    //Long(i64),
    Array(Vec<FieldValue>),
    Set(BTreeSet<FieldValue>),
    Field(FieldValue),
    #[default]
    Uninitialized,
}

#[derive(Debug, Default, PartialEq, PartialOrd)]
struct AggrValue {
    func: FuncCode,
    value: AggrValueEnum,
    got_numeric_input: bool,
}

impl AggrValue {
    pub fn new(func: FuncCode) -> Self {
        let mut av = AggrValue::default();
        av.func = func;
        match av.func {
            FuncCode::FnCountStar
            | FuncCode::FnCount
            | FuncCode::FnCountNumbers
            | FuncCode::FnSum => {
                av.value = AggrValueEnum::Field(FieldValue::Long(0));
            }
            FuncCode::FnMin | FuncCode::FnMax => {
                // Don't initialize min/max
                //av.value = AggrValueEnum::Field(FieldValue::Null);
            }
            FuncCode::ArrayCollect => {
                av.value = AggrValueEnum::Array(Vec::new());
            }
            FuncCode::ArrayCollectDistinct => {
                av.value = AggrValueEnum::Set(BTreeSet::new());
            }
            _ => {
                panic!("Invalid function code in AggrValue::new: {:?}", func);
            }
        }
        av
    }

    pub fn collect(&mut self, _req: &QueryRequest, val: FieldValue, _count_memory: bool) {
        if val.is_null() {
            return;
        }
        if val.get_type() != FieldType::Array {
            panic!(
                "Invalid FieldValue type in AggrValue::collect: expected Array, got {:?}",
                val
            );
        }
        if let AggrValueEnum::Set(set) = &mut self.value {
            // ArrayCollectDistinct
            if let FieldValue::Array(arr) = val {
                set.extend(arr.into_iter());
                //if count_memory {
                //rcb.incMemoryConsumption(welem.sizeof() +
                //SizeOf.HASHSET_ENTRY_OVERHEAD);
                //}
            }
        } else if let AggrValueEnum::Array(arr) = &mut self.value {
            // ArrayCollect
            if let FieldValue::Array(varr) = val {
                for i in varr.into_iter() {
                    arr.push(i);
                }
                //if count_memory
                //rcb.incMemoryConsumption(val.sizeof() +
                //SizeOf.OBJECT_REF_OVERHEAD *
                //arrayVal.size());
                //}
            }
        } else {
            panic!(
                "Invalid type of aggregator in AggrValue::collect: {:?}",
                self
            );
        }
    }

    pub fn increment(
        &mut self,
        _req: &QueryRequest,
        _count_memory: bool,
    ) -> Result<(), NoSQLError> {
        if let AggrValueEnum::Field(sum_value) = &mut self.value {
            match sum_value {
                FieldValue::Integer(i) => {
                    *i += 1;
                }
                FieldValue::Long(l) => {
                    *l += 1;
                }
                FieldValue::Double(d) => {
                    *d += 1.0;
                }
                FieldValue::Number(n) => {
                    *n += 1;
                }
                _ => {
                    return ia_err!("can't increment sum_value: not numeric");
                }
            }
            return Ok(());
        }
        if self.value == AggrValueEnum::Uninitialized {
            self.value = AggrValueEnum::Field(FieldValue::Long(1));
            return Ok(());
        }
        ia_err!("can't increment aggrValue: not a Field")
    }

    // TODO: add MathContext
    pub fn add(
        &mut self,
        _req: &QueryRequest,
        val: &FieldValue,
        _count_memory: bool,
    ) -> Result<(), NoSQLError> {
        if val.is_numeric() == false {
            // TODO: should this be an error?
            return Ok(());
        }
        if self.value == AggrValueEnum::Uninitialized {
            self.value = AggrValueEnum::Field(val.clone_internal());
            return Ok(());
        }
        if let AggrValueEnum::Field(sum_value) = &mut self.value {
            match sum_value {
                FieldValue::Integer(i) => match val {
                    FieldValue::Integer(vi) => {
                        *i += *vi;
                    }
                    FieldValue::Long(vl) => {
                        let l = *i as i64 + *vl;
                        self.value = AggrValueEnum::Field(FieldValue::Long(l));
                    }
                    FieldValue::Double(vd) => {
                        let d = *i as f64 + *vd;
                        self.value = AggrValueEnum::Field(FieldValue::Double(d));
                    }
                    FieldValue::Number(n) => {
                        self.value = AggrValueEnum::Field(FieldValue::Number(n + *i));
                    }
                    _ => {
                        return ia_err!("can't add non-numeric to numeric");
                    }
                },
                FieldValue::Long(l) => match val {
                    FieldValue::Integer(vi) => {
                        *l += *vi as i64;
                    }
                    FieldValue::Long(vl) => {
                        *l += *vl;
                    }
                    FieldValue::Double(vd) => {
                        let d = *l as f64 + *vd;
                        self.value = AggrValueEnum::Field(FieldValue::Double(d));
                    }
                    FieldValue::Number(n) => {
                        self.value = AggrValueEnum::Field(FieldValue::Number(n + *l));
                    }
                    _ => {
                        return ia_err!("can't add non-numeric to numeric");
                    }
                },
                FieldValue::Double(d) => match val {
                    FieldValue::Integer(vi) => {
                        *d += *vi as f64;
                    }
                    FieldValue::Long(vl) => {
                        *d += *vl as f64;
                    }
                    FieldValue::Double(vd) => {
                        *d += *vd;
                    }
                    FieldValue::Number(n) => {
                        let bd = bd_try_from_f64(*d)?;
                        self.value = AggrValueEnum::Field(FieldValue::Number(n + bd));
                    }
                    _ => {
                        return ia_err!("can't add non-numeric to numeric");
                    }
                },
                FieldValue::Number(n) => match val {
                    FieldValue::Integer(vi) => {
                        *n += *vi;
                    }
                    FieldValue::Long(vl) => {
                        *n += *vl;
                    }
                    FieldValue::Double(vd) => {
                        *n += bd_try_from_f64(*vd)?;
                    }
                    FieldValue::Number(vn) => {
                        *n += vn;
                    }
                    _ => {
                        return ia_err!("can't add non-numeric to numeric");
                    }
                },
                _ => {
                    return ia_err!(
                        "can't add() to self: expected numeric FieldValue, got {:?}",
                        sum_value
                    );
                }
            }
        } else {
            return ia_err!("can't add() to self: not a FieldValue ({:?})", self);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct GroupIterData {
    state: PlanIterState,
    results: BTreeMap<GroupTuple, Vec<AggrValue>>,
    results_valid: bool,
    gb_tuple: GroupTuple,
}

impl Clone for GroupIterData {
    // clone of iter data never copies its actual data
    fn clone(&self) -> Self {
        GroupIterData::default()
    }
    fn clone_from(&mut self, _source: &Self) {
        self.reset();
    }
}

impl GroupIterData {
    fn reset(&mut self) {
        self.state = PlanIterState::Uninitialized;
        self.results.clear();
        self.results_valid = false;
        self.gb_tuple = GroupTuple::default();
    }
}

impl GroupIter {
    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        self.data.state = PlanIterState::Open;
        self.input_iter.open(req, handle)?;
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
        PlanIterKind::Group
    }
    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("GI{} get_result={:?}", self.result_reg, fv);
        fv
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("GI{} set_result({:?})", self.result_reg, result);
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
        self.data.reset();
    }
    fn get_aggr_value_internal(
        &mut self,
        _req: &QueryRequest,
        aggr_tuple: &Vec<AggrValue>,
        value: AggrValueEnum,
        column: usize,
    ) -> FieldValue {
        let offset = column - self.num_gb_columns;
        let aggr_kind = aggr_tuple[offset].func;

        if aggr_kind == FuncCode::FnSum && aggr_tuple[offset].got_numeric_input == false {
            return FieldValue::Null;
        }

        if aggr_kind == FuncCode::ArrayCollect {
            if let AggrValueEnum::Array(arr) = value {
                return FieldValue::Array(arr);
                // TODO
                //let mut varr = FieldValue::Array(arr);
                // if in_test_mode {
                //varr.sort();
                //}
                //return varr;
            }
        }
        if aggr_kind == FuncCode::ArrayCollectDistinct {
            if let AggrValueEnum::Set(arr) = value {
                //let collect_array: = Vec::FieldValue::from(arr);
                let mut collect_array: Vec<FieldValue> = Vec::new();
                for i in arr.into_iter() {
                    collect_array.push(i);
                }
                return FieldValue::Array(collect_array);
            }
        }
        if let AggrValueEnum::Field(f) = value {
            return f;
        }
        return FieldValue::Null;
    }

    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            debug!("GI{} next(): already done", self.result_reg);
            return Ok(false);
        }

        loop {
            if self.data.results_valid {
                if let Some(tuple) = self.data.results.pop_first() {
                    debug!("GI{} popped first result: {:?}", self.result_reg, tuple);
                    debug!(
                        "GI{} num_gb_cols={} column_names={:?}",
                        self.result_reg, self.num_gb_columns, self.column_names
                    );
                    let mut gb_tuple = tuple.0;
                    let mut aggr_tuple = tuple.1;
                    let mut mv = MapValue::new();

                    let gb_cols = self.num_gb_columns;
                    let max_cols = self.column_names.len();

                    for i in 0..gb_cols {
                        mv.put_field_value(&self.column_names[i], take(&mut gb_tuple.values[i]));
                    }
                    for i in gb_cols..max_cols {
                        // TODO: rewrite this cleaner....
                        let value = take(&mut aggr_tuple[i - gb_cols].value);
                        let aggr = self.get_aggr_value_internal(req, &aggr_tuple, value, i);
                        mv.put_field_value(&self.column_names[i], aggr);
                    }

                    self.set_result(req, FieldValue::Map(mv));

                    if self.remove_produced_result {
                        // ooops - already removed!
                        //self.data.results.remove(gb_tuple);
                    } else {
                        debug!(
                            "GI{} NOTE: Removed produced result when told not to...",
                            self.result_reg
                        );
                        // TODO: re-insert the tuple?
                    }

                    return Ok(true);
                }

                self.done();
                return Ok(false);
            }

            let more = self.input_iter.next(req, handle).await?;
            if more == false {
                if req.reached_limit {
                    return Ok(false);
                }
                if self.num_gb_columns == self.column_names.len() {
                    self.done();
                    return Ok(false);
                }
                self.data.results_valid = true;
                continue;
            }

            let mut i: usize = 0;
            let mut in_tuple: MapValue = self.input_iter.get_result(req).get_map_value()?;

            while i < self.num_gb_columns {
                let mut col_value = in_tuple.get_field_value_clone(&self.column_names[i]);
                if col_value.is_none() {
                    if self.is_distinct {
                        col_value = Some(FieldValue::Null);
                    } else {
                        break;
                    }
                }
                if self.data.gb_tuple.values.len() > i {
                    self.data.gb_tuple.values[i] = col_value.unwrap();
                } else {
                    self.data.gb_tuple.values.push(col_value.unwrap());
                }
                i += 1;
            }

            if i < self.num_gb_columns {
                continue;
            }

            debug!(
                "GI{} Looking for gb_tuple={:?} in results...",
                self.result_reg, self.data.gb_tuple
            );
            let mut results = take(&mut self.data.results);
            if let Some(aggr_tuple) = results.get_mut(&self.data.gb_tuple) {
                debug!(
                    "GI{} Got from results, tuple={:?}",
                    self.result_reg, aggr_tuple
                );
                for i in self.num_gb_columns..self.column_names.len() {
                    self.aggregate(
                        req,
                        aggr_tuple,
                        i,
                        in_tuple.take_field_value(&self.column_names[i])?,
                    )?;
                }
                //if (rcb.getTraceLevel() >= 3) {
                //rcb.trace("Updated existing group:\n" +
                //printResult(state.theGBTuple, aggrTuple));
                //}
                debug!(
                    "GI{} After aggregation: tuple={:?}",
                    self.result_reg, aggr_tuple
                );
                self.data.results = results;
                continue;
            }

            let num_aggr_columns = self.column_names.len() - self.num_gb_columns;
            let mut gb_tuple = GroupTuple::default();
            let mut aggr_tuple: Vec<AggrValue> = Vec::new();
            //let mut aggr_tuple_size: i64 = 0;

            for i in 0..num_aggr_columns {
                aggr_tuple.push(AggrValue::new(self.aggr_funcs[i]));
                if self.count_memory {
                    // TODO aggrTupleSize += aggrTuple[i].sizeof();
                }
            }

            for i in 0..self.num_gb_columns {
                // TODO: should this move/take?
                //gb_tuple.values.push(take(&mut self.data.gb_tuple.values[i]));
                gb_tuple
                    .values
                    .push(self.data.gb_tuple.values[i].clone_internal());
            }

            if self.count_memory {
                //long sz = (gbTuple.sizeof() + aggrTupleSize +
                //SizeOf.HASHMAP_ENTRY_OVERHEAD);
                //rcb.incMemoryConsumption(sz);
            }

            for i in self.num_gb_columns..self.column_names.len() {
                self.aggregate(
                    req,
                    &mut aggr_tuple,
                    i,
                    in_tuple.take_field_value(&self.column_names[i])?,
                )?;
            }

            //if (rcb.getTraceLevel() >= 3) {
            //rcb.trace("Started new group:\n" +
            //printResult(gbTuple, aggrTuple));
            //}

            if self.num_gb_columns == self.column_names.len() {
                debug!(
                    "GI{} Results: inserting tuple {:?} with value {:?}",
                    self.result_reg, gb_tuple, aggr_tuple
                );
                results.insert(gb_tuple.clone_internal(), aggr_tuple);
                self.data.results = results;
                let mut res = MapValue::new();
                for i in 0..self.num_gb_columns {
                    res.put_field_value(&self.column_names[i], take(&mut gb_tuple.values[i]));
                }
                self.set_result(req, FieldValue::Map(res));
                return Ok(true);
            }

            debug!(
                "GI{} Results_1: inserting tuple {:?} with value {:?}",
                self.result_reg, gb_tuple, aggr_tuple
            );
            results.insert(gb_tuple, aggr_tuple);
            self.data.results = results;
        }
    }

    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.input_iter.reset()?;
        self.data.reset();
        Ok(())
    }

    /*
        pub fn close(&mut self, req: &mut QueryRequest) -> Result<(), NoSQLError> {
            self.input_iter.close(req)?;
            self.data.reset();
            Ok(())
        }
    */

    fn aggregate(
        &self,
        req: &mut QueryRequest,
        aggr_values: &mut Vec<AggrValue>,
        column: usize,
        val: FieldValue,
    ) -> Result<(), NoSQLError> {
        let offset = column - self.num_gb_columns;
        let aggr_kind = aggr_values[offset].func;
        let val_type = val.get_type();

        debug!(
            "GI{} aggregate: column={column} kind={:?} val={:?}",
            self.result_reg, aggr_kind, val
        );

        match aggr_kind {
            FuncCode::FnCount => {
                if val.is_null() {
                    return Ok(());
                }
                // TODO: math context
                aggr_values[offset].increment(req, self.count_memory)?;
            }
            FuncCode::FnCountNumbers => {
                if val.is_null() || !val.is_numeric() {
                    return Ok(());
                }
                // TODO: math context
                aggr_values[offset].increment(req, self.count_memory)?;
            }
            FuncCode::FnCountStar => {
                // TODO: math context
                aggr_values[offset].increment(req, self.count_memory)?;
            }
            FuncCode::FnSum => {
                if val.is_null() || !val.is_numeric() {
                    return Ok(());
                }
                // TODO: math context
                aggr_values[offset].add(req, &val, self.count_memory)?;
                aggr_values[offset].got_numeric_input = true;
            }
            FuncCode::FnMin | FuncCode::FnMax => {
                match val_type {
                    FieldType::Binary | FieldType::Array | FieldType::Map | FieldType::Empty => {
                        return Ok(());
                    }
                    _ => (),
                }
                if aggr_values[offset].value == AggrValueEnum::Uninitialized {
                    //if (rcb.getTraceLevel() >= 3) {
                    //rcb.trace("Setting min/max to " + val);
                    //}
                    //if (theCountMemory) {
                    //rcb.incMemoryConsumption(val.sizeof() - minmaxValue.sizeof());
                    //}
                    debug!("GI{} setting {:?}={:?}", self.result_reg, aggr_kind, val);
                    aggr_values[offset].value = AggrValueEnum::Field(val);
                    return Ok(());
                }
                let cmp: Ordering;
                if let AggrValueEnum::Field(aval) = &aggr_values[offset].value {
                    cmp = compare_atomics_total_order(&val, aval, false);
                } else {
                    return ia_err!("can't do MIN/MAX: existing value not a Field");
                }
                //if (rcb.getTraceLevel() >= 3) {
                //rcb.trace("Compared values: \n" + minmaxValue + "\n" +
                //val + "\ncomp res = " + cmp);
                //}
                if aggr_kind == FuncCode::FnMin {
                    if cmp != Ordering::Less {
                        return Ok(());
                    }
                } else {
                    if cmp != Ordering::Greater {
                        return Ok(());
                    }
                }
                //if (rcb.getTraceLevel() >= 3) {
                //rcb.trace("Setting min/max to " + val);
                //}
                //if (theCountMemory &&
                //val.getType() != minmaxValue.getType()) {
                //rcb.incMemoryConsumption(val.sizeof() - minmaxValue.sizeof());
                //}
                debug!("GI{} setting {:?}={:?}", self.result_reg, aggr_kind, val);
                aggr_values[offset].value = AggrValueEnum::Field(val);
            }
            FuncCode::ArrayCollect | FuncCode::ArrayCollectDistinct => {
                aggr_values[offset].collect(req, val, self.count_memory);
            }
            _ => {
                return ia_err!("method not implemented for {:?}", aggr_kind);
            }
        }
        Ok(())
    }
}
