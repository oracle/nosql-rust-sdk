//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::error::ia_err;
use crate::error::NoSQLError;
use crate::handle::Handle;
use crate::plan_iter::{deserialize_plan_iter, PlanIter};
use crate::plan_iter::{Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::types::{compare_results, FieldValue, MapValue};
use oracle_nosql_rust_sdk_derive::add_planiter_fields;

use std::result::Result;
use tracing::debug;

// SortSpec specifies criterias for sorting the values.
//
// The order-by clause, for each sort expression allows for an optional "sort spec",
// which specifies the relative order of NULLs (less than or greater than all other values)
// and whether the values returned by the sort expression should be sorted in ascending or descending order.
#[derive(Debug, Default, Clone)]
pub(crate) struct SortSpec {
    // is_desc specifies if the desired sorting order is in descending order.
    pub is_desc: bool,

    // nulls_first specifies if NULL values should sort before all other values.
    pub nulls_first: bool,
}

impl SortSpec {
    pub fn from_reader(r: &mut Reader) -> Result<SortSpec, NoSQLError> {
        let desc = r.read_bool()?;
        let nulls = r.read_bool()?;
        Ok(SortSpec {
            is_desc: desc,
            nulls_first: nulls,
        })
    }
    pub fn read_sort_specs(r: &mut Reader) -> Result<Vec<SortSpec>, NoSQLError> {
        let num = r.read_packed_i32()?;
        if num <= 0 {
            return Ok(Vec::new());
        }
        let mut v: Vec<SortSpec> = Vec::with_capacity(num as usize);
        for _i in 0..num {
            v.push(SortSpec::from_reader(r)?);
        }
        Ok(v)
    }
}

// SortIter sorts MapValues based on their values on a specified set of top-level
// fields. It is also used by the driver to implement the geo_near function,
// which sorts results by distance.
#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct SortIter {
    input_iter: Box<PlanIter>,
    sort_fields: Vec<String>,
    sort_specs: Vec<SortSpec>,
    count_memory: bool,
    data: SortIterData,
}

#[derive(Debug, Default)]
struct SortIterData {
    state: PlanIterState,
    results: Vec<MapValue>,
    current_result: i32,
}

impl Clone for SortIterData {
    // clone of iter data never copies its actual data
    fn clone(&self) -> Self {
        SortIterData::default()
    }
    fn clone_from(&mut self, _source: &Self) {
        self.reset();
    }
}

impl SortIterData {
    fn reset(&mut self) {
        self.state = PlanIterState::Uninitialized;
        self.current_result = 0;
        self.results.clear();
    }
}

impl SortIter {
    pub fn new(r: &mut Reader, kind: PlanIterKind) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\nSortIter: result_reg={} state_pos={}\n", rr, sp);
        let mut s = SortIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // fields specific to SortIter
            input_iter: deserialize_plan_iter(r)?,
            sort_fields: r.read_string_array()?,
            sort_specs: SortSpec::read_sort_specs(r)?,
            count_memory: true,

            data: SortIterData::default(),
        };
        if kind == PlanIterKind::Sorting2 {
            s.count_memory = r.read_bool()?;
        }
        Ok(s)
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
        PlanIterKind::Sorting
    }
    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state == PlanIterState::Done {
            return Ok(false);
        }

        if self.data.state == PlanIterState::Open {
            let mut more = self.input_iter.next(req, handle).await?;

            while more == true {
                let mv = self.input_iter.get_result(req).get_map_value()?;
                for field in &self.sort_fields {
                    let fvopt = mv.get_field_value(field);
                    if let Some(fv) = fvopt {
                        if fv.is_atomic() == false {
                            //TODO QueryException(... self.location)
                            return ia_err!(
                                "sort expression does not return a single atomic value"
                            );
                        }
                    }
                }

                self.data.results.push(mv);
                more = self.input_iter.next(req, handle).await?;
            }

            if req.reached_limit {
                return Ok(false);
            }

            debug!("Calling sort: results={:?}", self.data.results);
            // TODO: sorting of MapValue vectors
            self.data.results.sort_unstable_by(|a, b| {
                compare_results(a, b, &self.sort_fields, &self.sort_specs)
            });
            debug!("After sort: results={:?}", self.data.results);

            // TODO: all state settings should do state.set_state(foo)
            self.data.state = PlanIterState::Running;
        }

        if self.data.current_result < self.data.results.len() as i32 {
            let mut mv = std::mem::take(&mut self.data.results[self.data.current_result as usize]);
            mv.convert_empty_to_null();
            self.set_result(req, FieldValue::Map(mv));
            self.data.current_result += 1;
            return Ok(true);
        }

        self.data.state = PlanIterState::Done;
        Ok(false)
    }

    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("SI{} get_result={:?}", self.result_reg, fv);
        fv
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("SI{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.data.reset();
        self.input_iter.reset()
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
    pub fn get_aggr_value(
        &self,
        _req: &QueryRequest,
        _reset: bool,
    ) -> Result<Option<FieldValue>, NoSQLError> {
        Ok(None)
    }

    //protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
    //theInput.display(sb, formatter);
    //formatter.indent(sb);
    //sb.append("Sort Fields : ");
    //for (int i = 0; i < theSortFields.length; ++i) {
    //sb.append(theSortFields[i]);
    //if (i < theSortFields.length - 1) {
    //sb.append(", ");
    //}
    //}
    //sb.append(",\n");
    //}
}
