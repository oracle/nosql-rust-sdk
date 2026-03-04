//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use oracle_nosql_rust_sdk_derive::add_planiter_fields;

use crate::error::NoSQLError;
use crate::handle::Handle;
use crate::plan_iter::deserialize_plan_iter;
use crate::plan_iter::{Location, PlanIter, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::{compare_field_values, FieldValue};

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::result::Result;
use tracing::debug;

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct CollectIter {
    is_distinct: bool,
    input_iter: Box<PlanIter>,

    data: CollectIterData,
}

impl CollectIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nCollectIter: result_reg={} state_pos={}\n", rr, sp);
        Ok(CollectIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // specific to CollectIter
            is_distinct: r.read_bool()?,
            input_iter: deserialize_plan_iter(r)?,

            ..Default::default()
        })
    }
}

#[derive(Debug, Default)]
struct CollectIterData {
    state: PlanIterState,
    array: Vec<FieldValue>,
    set: BTreeSet<FieldValue>,
    memory_consumption: u64,
}

impl Clone for CollectIterData {
    // clone of iter data never copies its actual data
    fn clone(&self) -> Self {
        CollectIterData::default()
    }
    fn clone_from(&mut self, _source: &Self) {
        self.reset();
    }
}

impl CollectIterData {
    fn reset(&mut self) {
        self.state = PlanIterState::Uninitialized;
        self.array.clear();
        self.set.clear();
        self.memory_consumption = 0;
    }
}

impl CollectIter {
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
        PlanIterKind::Collect
    }
    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        debug!("CollectIter.get_result");
        req.get_result(self.result_reg)
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
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
    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            debug!("CollectIter.next(): already done");
            return Ok(false);
        }

        loop {
            if self.input_iter.next(req, handle).await? == false {
                return Ok(true);
            }

            let val = self.input_iter.get_result(req);

            //if (rcb.getTraceLevel() >= 2) {
            //rcb.trace("Collecting value " + val);
            //}
            self.aggregate(req, val)?;
        }
    }

    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.input_iter.reset()?;
        // Don't reset the state of self.data Resetting the state is done in
        // method getAggrValue below.
        //self.data.reset();
        Ok(())
    }

    /*
        pub fn close(&mut self, req: &mut QueryRequest) -> Result<(), NoSQLError> {
            self.input_iter.close(req)?;
            self.data.reset();
            Ok(())
        }
    */

    fn aggregate(&mut self, _req: &mut QueryRequest, val: FieldValue) -> Result<(), NoSQLError> {
        if val.is_null() {
            return Ok(());
        }

        if let FieldValue::Array(arr) = val {
            if self.is_distinct {
                // ArrayCollectDistinct
                self.data.set.extend(arr.into_iter());
            //if count_memory {
            //rcb.incMemoryConsumption(welem.sizeof() +
            //SizeOf.HASHSET_ENTRY_OVERHEAD);
            //}
            } else {
                // ArrayCollect
                for i in arr.into_iter() {
                    self.data.array.push(i);
                }
                //if count_memory {
                //rcb.incMemoryConsumption(val.sizeof() +
                //SizeOf.OBJECT_REF_OVERHEAD *
                //arrayVal.size());
                //}
            }
        } else {
            panic!(
                "Invalid FieldValue type in CollectIter::aggregate: expected Array, got {:?}",
                val
            );
        }
        Ok(())
    }

    pub fn get_aggr_value(
        &mut self,
        _req: &QueryRequest,
        reset: bool,
    ) -> Result<Option<FieldValue>, NoSQLError> {
        if self.is_distinct {
            let s = std::mem::take(&mut self.data.set);
            for v in s.into_iter() {
                self.data.array.push(v);
            }
        }

        // TODO if in_test_mode
        self.data.array.sort_unstable_by(sort_func);

        let vals = std::mem::take(&mut self.data.array);

        //if (rcb.getTraceLevel() >= 3) {
        //rcb.trace("Collected values " + res);
        //}

        // we already moved all values out
        if reset == false {
            debug!("WARN: CollectIter.get_aggr_value called with reset==false");
        }
        self.data.reset();
        Ok(Some(FieldValue::Array(vals)))
    }
}

fn sort_func(v1: &FieldValue, v2: &FieldValue) -> Ordering {
    compare_field_values(v2, v1, false)
}
