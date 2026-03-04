//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::aggr_iter::{FuncMinMaxIter, FuncSumIter};
use crate::arith_op_iter::ArithOpIter;
use crate::collect_iter::CollectIter;
use crate::const_iter::ConstIter;
use crate::const_iter::EmptyIter;
use crate::error::ia_err;
use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode::BadProtocolMessage;
use crate::ext_var_iter::ExtVarIter;
use crate::field_step_iter::FieldStepIter;
use crate::group_iter::GroupIter;
use crate::handle::Handle;
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::receive_iter::ReceiveIter;
use crate::sfw_iter::SfwIter;
use crate::size_iter::SizeIter;
use crate::sort_iter::SortIter;
use crate::types::FieldValue;
use crate::var_ref_iter::VarRefIter;

use core::fmt::Debug;
use num_enum::TryFromPrimitive;
use std::result::Result;

use async_recursion::async_recursion;

// PlanIterKind represents the kind of plan iterators.
#[derive(Debug, Clone, Default, Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub(crate) enum PlanIterKind {
    #[default]
    Empty = 255,
    Const = 0,
    VarRef = 1,
    ExtVar = 2,
    ArithOp = 8,
    FieldStep = 11,
    Sfw = 14,
    Size = 15,
    Recv = 17,
    SumFunc = 39,
    MinMaxFunc = 41,
    Sorting = 47,
    Group = 65,
    Sorting2 = 66,
    Collect = 78,
}

impl PlanIterKind {
    pub(crate) fn try_from_u8(val: u8) -> Result<Self, NoSQLError> {
        match PlanIterKind::try_from(val) {
            Ok(fc) => {
                return Ok(fc);
            }
            Err(_) => {
                return Err(NoSQLError::new(
                    BadProtocolMessage,
                    &format!("unrecognized plan iter kind {}", val),
                ));
            }
        }
    }
}

// FuncCode represents a function code that used for the built-in function.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, PartialOrd, TryFromPrimitive)]
#[repr(u16)]
pub(crate) enum FuncCode {
    OpAddSub = 14,
    OpMultDiv = 15,
    FnCountStar = 42,
    #[default]
    FnCount = 43,
    FnCountNumbers = 44,
    FnSum = 45,
    FnMin = 47,
    FnMax = 48,
    ArrayCollect = 91,
    ArrayCollectDistinct = 92,
}

impl FuncCode {
    pub(crate) fn try_from_u16(val: u16) -> Result<Self, NoSQLError> {
        match FuncCode::try_from(val) {
            Ok(fc) => {
                return Ok(fc);
            }
            Err(_) => {
                return Err(NoSQLError::new(
                    BadProtocolMessage,
                    &format!("unrecognized function code {}", val),
                ));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum PlanIter {
    Empty(EmptyIter),
    Const(ConstIter),
    ArithOp(ArithOpIter),
    Receive(ReceiveIter),
    Sfw(SfwIter),
    Size(SizeIter),
    FieldStep(FieldStepIter),
    VarRef(VarRefIter),
    ExtVar(ExtVarIter),
    Sorting(SortIter),
    Group(GroupIter),
    SumFunc(FuncSumIter),
    MinMaxFunc(FuncMinMaxIter),
    Collect(CollectIter),
}

impl Default for PlanIter {
    fn default() -> Self {
        PlanIter::Empty(EmptyIter::new())
    }
}

// TODO: derive all methods automatically
impl PlanIter {
    // set up iterator for looping
    pub fn open(&mut self, req: &mut QueryRequest, handle: &Handle) -> Result<(), NoSQLError> {
        match self {
            PlanIter::Empty(ref mut e) => e.open(req, handle),
            PlanIter::Const(ref mut c) => c.open(req, handle),
            PlanIter::Receive(ref mut r) => r.open(req, handle),
            PlanIter::Sfw(ref mut r) => r.open(req, handle),
            PlanIter::FieldStep(ref mut r) => r.open(req, handle),
            PlanIter::VarRef(ref mut r) => r.open(req, handle),
            PlanIter::ExtVar(ref mut r) => r.open(req, handle),
            PlanIter::Sorting(ref mut r) => r.open(req, handle),
            PlanIter::Group(ref mut r) => r.open(req, handle),
            PlanIter::SumFunc(ref mut r) => r.open(req, handle),
            PlanIter::MinMaxFunc(ref mut r) => r.open(req, handle),
            PlanIter::ArithOp(ref mut r) => r.open(req, handle),
            PlanIter::Collect(ref mut r) => r.open(req, handle),
            PlanIter::Size(ref mut r) => r.open(req, handle),
        }
    }
    // get next single FieldValue (retrieved by get_result)
    // return:
    // true: there are more results to get
    // false: no more results
    #[async_recursion]
    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        match self {
            PlanIter::Empty(ref mut e) => e.next(req).await,
            PlanIter::Const(ref mut c) => c.next(req).await,
            PlanIter::Receive(ref mut r) => r.next(req, handle).await,
            PlanIter::Sfw(ref mut r) => r.next(req, handle).await,
            PlanIter::FieldStep(ref mut r) => r.next(req, handle).await,
            PlanIter::VarRef(ref mut r) => r.next(req, handle).await,
            PlanIter::ExtVar(ref mut r) => r.next(req, handle).await,
            PlanIter::Sorting(ref mut r) => r.next(req, handle).await,
            PlanIter::Group(ref mut r) => r.next(req, handle).await,
            PlanIter::SumFunc(ref mut r) => r.next(req, handle).await,
            PlanIter::MinMaxFunc(ref mut r) => r.next(req, handle).await,
            PlanIter::ArithOp(ref mut r) => r.next(req, handle).await,
            PlanIter::Collect(ref mut r) => r.next(req, handle).await,
            PlanIter::Size(ref mut r) => r.next(req, handle).await,
        }
    }
    // close and release any resources used during looping
    /*
        pub fn close(&mut self, req: &mut QueryRequest) -> Result<(), NoSQLError> {
            match self {
                PlanIter::Empty(ref mut e) => e.close(req),
                PlanIter::Const(ref mut c) => c.close(req),
                PlanIter::Receive(ref mut r) => r.close(req),
                PlanIter::Sfw(ref mut r) => r.close(req),
                PlanIter::FieldStep(ref mut r) => r.close(req),
                PlanIter::VarRef(ref mut r) => r.close(req),
                PlanIter::ExtVar(ref mut r) => r.close(req),
                PlanIter::Sorting(ref mut r) => r.close(req),
                PlanIter::Group(ref mut r) => r.close(req),
                PlanIter::SumFunc(ref mut r) => r.close(req),
                PlanIter::MinMaxFunc(ref mut r) => r.close(req),
                PlanIter::ArithOp(ref mut r) => r.close(req),
                PlanIter::Collect(ref mut r) => r.close(req),
                PlanIter::Size(ref mut r) => r.close(req),
            }
        }
    */
    // set iterator to exactly the same as if it was just created through deserialization
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        match self {
            PlanIter::Empty(ref mut e) => e.reset(),
            PlanIter::Const(ref mut c) => c.reset(),
            PlanIter::Receive(ref mut r) => r.reset(),
            PlanIter::Sfw(ref mut r) => r.reset(),
            PlanIter::FieldStep(ref mut r) => r.reset(),
            PlanIter::VarRef(ref mut r) => r.reset(),
            PlanIter::ExtVar(ref mut r) => r.reset(),
            PlanIter::Sorting(ref mut r) => r.reset(),
            PlanIter::Group(ref mut r) => r.reset(),
            PlanIter::SumFunc(ref mut r) => r.reset(),
            PlanIter::MinMaxFunc(ref mut r) => r.reset(),
            PlanIter::ArithOp(ref mut r) => r.reset(),
            PlanIter::Collect(ref mut r) => r.reset(),
            PlanIter::Size(ref mut r) => r.reset(),
        }
    }
    pub fn get_kind(&self) -> PlanIterKind {
        match self {
            PlanIter::Empty(ref e) => e.get_kind(),
            PlanIter::Const(ref c) => c.get_kind(),
            PlanIter::Receive(ref r) => r.get_kind(),
            PlanIter::Sfw(ref r) => r.get_kind(),
            PlanIter::FieldStep(ref r) => r.get_kind(),
            PlanIter::VarRef(ref r) => r.get_kind(),
            PlanIter::ExtVar(ref r) => r.get_kind(),
            PlanIter::Sorting(ref r) => r.get_kind(),
            PlanIter::Group(ref r) => r.get_kind(),
            PlanIter::SumFunc(ref r) => r.get_kind(),
            PlanIter::MinMaxFunc(ref r) => r.get_kind(),
            PlanIter::ArithOp(ref r) => r.get_kind(),
            PlanIter::Collect(ref r) => r.get_kind(),
            PlanIter::Size(ref r) => r.get_kind(),
        }
    }
    // this will move the result out of the iterator (owned by the caller)
    // This will return FieldValue::Uninitialized if nothing is available
    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        match self {
            PlanIter::Empty(e) => e.get_result(req),
            PlanIter::Const(c) => c.get_result(req),
            PlanIter::Receive(r) => r.get_result(req),
            PlanIter::Sfw(r) => r.get_result(req),
            PlanIter::FieldStep(r) => r.get_result(req),
            PlanIter::VarRef(r) => r.get_result(req),
            PlanIter::ExtVar(r) => r.get_result(req),
            PlanIter::Sorting(r) => r.get_result(req),
            PlanIter::Group(r) => r.get_result(req),
            PlanIter::SumFunc(r) => r.get_result(req),
            PlanIter::MinMaxFunc(r) => r.get_result(req),
            PlanIter::ArithOp(r) => r.get_result(req),
            PlanIter::Collect(r) => r.get_result(req),
            PlanIter::Size(r) => r.get_result(req),
        }
    }
    // this will move the result into the iterator (owned by the iterator)
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        match self {
            PlanIter::Empty(e) => e.set_result(req, result),
            PlanIter::Const(c) => c.set_result(req, result),
            PlanIter::Receive(r) => r.set_result(req, result),
            PlanIter::Sfw(r) => r.set_result(req, result),
            PlanIter::FieldStep(r) => r.set_result(req, result),
            PlanIter::VarRef(r) => r.set_result(req, result),
            PlanIter::ExtVar(r) => r.set_result(req, result),
            PlanIter::Sorting(r) => r.set_result(req, result),
            PlanIter::Group(r) => r.set_result(req, result),
            PlanIter::SumFunc(r) => r.set_result(req, result),
            PlanIter::MinMaxFunc(r) => r.set_result(req, result),
            PlanIter::ArithOp(r) => r.set_result(req, result),
            PlanIter::Collect(r) => r.set_result(req, result),
            PlanIter::Size(r) => r.set_result(req, result),
        }
    }
    pub fn get_state(&self) -> PlanIterState {
        match self {
            PlanIter::Empty(ref e) => e.get_state(),
            PlanIter::Const(ref c) => c.get_state(),
            PlanIter::Receive(ref r) => r.get_state(),
            PlanIter::Sfw(ref r) => r.get_state(),
            PlanIter::FieldStep(ref r) => r.get_state(),
            PlanIter::VarRef(ref r) => r.get_state(),
            PlanIter::ExtVar(ref r) => r.get_state(),
            PlanIter::Sorting(ref r) => r.get_state(),
            PlanIter::Group(ref r) => r.get_state(),
            PlanIter::SumFunc(ref r) => r.get_state(),
            PlanIter::MinMaxFunc(ref r) => r.get_state(),
            PlanIter::ArithOp(ref r) => r.get_state(),
            PlanIter::Collect(ref r) => r.get_state(),
            PlanIter::Size(ref r) => r.get_state(),
        }
    }

    /*
        pub fn get_plan(&self) -> String {
            match self {
                PlanIter::Empty(ref e) => e.get_plan(),
                PlanIter::Const(ref c) => c.get_plan(),
                PlanIter::Receive(ref r) => r.get_plan(),
                PlanIter::Sfw(ref r) => r.get_plan(),
                PlanIter::FieldStep(ref r) => r.get_plan(),
                PlanIter::VarRef(ref r) => r.get_plan(),
                PlanIter::ExtVar(ref r) => r.get_plan(),
                PlanIter::Sorting(ref r) => r.get_plan(),
                PlanIter::Group(ref r) => r.get_plan(),
                PlanIter::SumFunc(ref r) => r.get_plan(),
                PlanIter::MinMaxFunc(ref r) => r.get_plan(),
                PlanIter::ArithOp(ref r) => r.get_plan(),
                PlanIter::Collect(ref r) => r.get_plan(),
                PlanIter::Size(ref r) => r.get_plan(),
            }
        }
    */
    /*
        pub fn display_content(&self, sb: &mut String, f: &PlanFormatter) {
            match self {
                PlanIter::Empty(ref e) => e.display_content(sb, f),
                PlanIter::Const(ref c) => c.display_content(sb, f),
                PlanIter::Receive(ref r) => r.display_content(sb, f),
                PlanIter::Sfw(ref r) => r.display_content(sb, f),
                PlanIter::FieldStep(ref r) => r.display_content(sb, f),
                PlanIter::VarRef(ref r) => r.display_content(sb, f),
                PlanIter::ExtVar(ref r) => r.display_content(sb, f),
                PlanIter::Sorting(ref r) => r.display_content(sb, f),
                PlanIter::Group(ref r) => r.display_content(sb, f),
                PlanIter::SumFunc(ref r) => r.display_content(sb, f),
                PlanIter::MinMaxFunc(ref r) => r.display_content(sb, f),
                PlanIter::ArithOp(ref r) => r.display_content(sb, f),
                PlanIter::Collect(ref r) => r.display_content(sb, f),
                PlanIter::Size(ref r) => r.display_content(sb, f),
            }
        }
    */
    // only applicable to Func iterators
    /*
        pub fn get_func_code(&self) -> Option<FuncCode> {
            match self {
                PlanIter::Empty(ref e) => e.get_func_code(),
                PlanIter::Const(ref c) => c.get_func_code(),
                PlanIter::Receive(ref r) => r.get_func_code(),
                PlanIter::Sfw(ref r) => r.get_func_code(),
                PlanIter::FieldStep(ref r) => r.get_func_code(),
                PlanIter::VarRef(ref r) => r.get_func_code(),
                PlanIter::ExtVar(ref r) => r.get_func_code(),
                PlanIter::Sorting(ref r) => r.get_func_code(),
                PlanIter::Group(ref r) => r.get_func_code(),
                PlanIter::SumFunc(ref r) => r.get_func_code(),
                PlanIter::MinMaxFunc(ref r) => r.get_func_code(),
                PlanIter::ArithOp(ref r) => r.get_func_code(),
                PlanIter::Collect(ref r) => r.get_func_code(),
                PlanIter::Size(ref r) => r.get_func_code(),
            }
        }
    */
    // only applicable to Aggr iterators
    pub fn get_aggr_value(
        &mut self,
        req: &QueryRequest,
        reset: bool,
    ) -> Result<Option<FieldValue>, NoSQLError> {
        match self {
            PlanIter::Empty(ref e) => e.get_aggr_value(req, reset),
            PlanIter::Const(ref c) => c.get_aggr_value(req, reset),
            PlanIter::Receive(ref r) => r.get_aggr_value(req, reset),
            PlanIter::Sfw(ref r) => r.get_aggr_value(req, reset),
            PlanIter::FieldStep(ref r) => r.get_aggr_value(req, reset),
            PlanIter::VarRef(ref r) => r.get_aggr_value(req, reset),
            PlanIter::ExtVar(ref r) => r.get_aggr_value(req, reset),
            PlanIter::Sorting(ref r) => r.get_aggr_value(req, reset),
            PlanIter::Group(ref r) => r.get_aggr_value(req, reset),
            PlanIter::SumFunc(ref mut r) => r.get_aggr_value(req, reset),
            PlanIter::MinMaxFunc(ref mut r) => r.get_aggr_value(req, reset),
            PlanIter::ArithOp(ref mut r) => r.get_aggr_value(req, reset),
            PlanIter::Collect(ref mut r) => r.get_aggr_value(req, reset),
            PlanIter::Size(ref mut r) => r.get_aggr_value(req, reset),
        }
    }
}

pub(crate) fn read_sequence_length(r: &mut Reader) -> Result<i32, NoSQLError> {
    let n = r.read_packed_i32()?;
    if n < -1 {
        return ia_err!("invalid sequence length: {}", n);
    }
    Ok(n)
}

pub(crate) fn deserialize_plan_iters(r: &mut Reader) -> Result<Vec<Box<PlanIter>>, NoSQLError> {
    let n = read_sequence_length(r)?;
    if n == -1 {
        return Ok(Vec::new());
    }

    let mut iters: Vec<Box<PlanIter>> = Vec::with_capacity(n as usize);
    for _i in 0..n {
        let iter = deserialize_plan_iter(r)?;
        if iter.get_kind() != PlanIterKind::Empty {
            iters.push(iter);
        }
    }
    Ok(iters)
}

pub(crate) fn deserialize_plan_iter(r: &mut Reader) -> Result<Box<PlanIter>, NoSQLError> {
    let b = r.read_byte()?;

    let v = b as i8;
    if v == -1 {
        return Ok(Box::new(PlanIter::default()));
    }

    let kind: PlanIterKind = PlanIterKind::try_from_u8(b)?;
    match kind {
        PlanIterKind::Empty => Ok(Box::new(PlanIter::default())),
        PlanIterKind::Const => Ok(Box::new(PlanIter::Const(ConstIter::new(r)?))),
        PlanIterKind::VarRef => Ok(Box::new(PlanIter::VarRef(VarRefIter::new(r)?))),
        PlanIterKind::ExtVar => Ok(Box::new(PlanIter::ExtVar(ExtVarIter::new(r)?))),
        PlanIterKind::ArithOp => Ok(Box::new(PlanIter::ArithOp(ArithOpIter::new(r)?))),
        PlanIterKind::FieldStep => Ok(Box::new(PlanIter::FieldStep(FieldStepIter::new(r)?))),
        PlanIterKind::SumFunc => Ok(Box::new(PlanIter::SumFunc(FuncSumIter::new(r)?))),
        PlanIterKind::MinMaxFunc => Ok(Box::new(PlanIter::MinMaxFunc(FuncMinMaxIter::new(r)?))),
        PlanIterKind::Collect => Ok(Box::new(PlanIter::Collect(CollectIter::new(r)?))),
        PlanIterKind::Size => Ok(Box::new(PlanIter::Size(SizeIter::new(r)?))),
        PlanIterKind::Sorting => Ok(Box::new(PlanIter::Sorting(SortIter::new(r, kind)?))),
        PlanIterKind::Sorting2 => Ok(Box::new(PlanIter::Sorting(SortIter::new(r, kind)?))),
        PlanIterKind::Sfw => Ok(Box::new(PlanIter::Sfw(SfwIter::new(r)?))),
        PlanIterKind::Recv => Ok(Box::new(PlanIter::Receive(ReceiveIter::new(r)?))),
        PlanIterKind::Group => Ok(Box::new(PlanIter::Group(GroupIter::new(r)?))),
    }
}

/*
impl PlanIterDelegate {
    fn display_exec_plan(&self, iter: &PlanIter) -> String {
        let mut s: String = Default::default();
        let mut f = PlanFormatter::new();
        self.display_plan(iter, &mut s, &mut f);
        return s;
    }

    fn display_plan(&self, iter: &PlanIter, sb: &mut String, f: &mut PlanFormatter) {
        f.print_indent(sb);
        if let Some(fc) = iter.get_func_code() {
            sb.push_str(format!("{:?}", fc).as_str());
        } else {
            sb.push_str(format!("{:?}", iter.get_kind()).as_str());
        }
        self.display_reg(sb);
        sb.push('\n');
        f.print_indent(sb);
        sb.push_str("[\n");

        f.inc_indent();
        iter.display_content(sb, f);
        f.dec_indent();
        sb.push('\n');

        f.print_indent(sb);
        sb.push(']');
    }

    fn display_reg(&self, sb: &mut String) {
        sb.push_str(format!("([{}])", self.result_reg).as_str());
    }

    // isDone returns whether the iterator is in the DONE or CLOSED state.
    // CLOSED is included because, in the order of states, a CLOSED iterator is also DONE.
    fn is_done(&self, req: &QueryRequest) -> bool {
        let state = req.get_state(self.state_pos);
        match state {
            Ok(s) => s.is_done() || s.is_closed(),
            _ => false
        }
    }
}
*/

#[derive(Debug, Default, Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct Location {
    pub start_line: i32,
    pub start_column: i32,
    pub end_line: i32,
    pub end_column: i32,
}

impl Location {
    pub fn from_reader(r: &mut Reader) -> Result<Self, NoSQLError> {
        Ok(Location {
            start_line: r.read_i32_min(0)?,
            start_column: r.read_i32_min(0)?,
            end_line: r.read_i32_min(0)?,
            end_column: r.read_i32_min(0)?,
        })
    }
}

/*
#[derive(Debug, Clone, Copy)]
pub(crate) struct PlanFormatter {
    pub indent_increment: u16,
    pub indent: u16,
}

impl PlanFormatter {
    pub(crate) fn new() -> PlanFormatter {
        PlanFormatter {
            indent_increment: 2,
            indent: 0,
        }
    }

    pub(crate) fn print_indent(&self, s: &mut String) {
        if self.indent == 0 || self.indent_increment == 0 {
            return;
        }
        for _i in 0..self.indent {
            s.push(' ');
        }
    }

    pub(crate) fn inc_indent(&mut self) {
        self.indent += self.indent_increment;
    }

    pub(crate) fn dec_indent(&mut self) {
        self.indent -= self.indent_increment;
    }
}
*/

// PlanIterState represents dynamic state for an iterator.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum PlanIterState {
    #[default]
    Uninitialized = 255,
    Open = 0,
    Running = 1,
    Done = 2,
}

impl std::fmt::Display for PlanIterState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl PlanIterState {
    /*
        pub fn is_open(self) -> bool {
            self == PlanIterState::Open
        }
    */

    pub fn is_done(self) -> bool {
        self == PlanIterState::Done
    }
}
