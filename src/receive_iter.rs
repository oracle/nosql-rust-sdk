//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use oracle_nosql_rust_sdk_derive::add_planiter_fields;

use crate::error::ia_err;
use crate::error::NoSQLError;
use crate::error::NoSQLErrorCode::BadProtocolMessage;
use crate::handle::Handle;
use crate::plan_iter::{Location, PlanIterKind, PlanIterState};
use crate::query_request::QueryRequest;
use crate::reader::Reader;
use crate::sort_iter::SortSpec;
use crate::types::{compare_results, FieldValue, MapValue};
use crate::writer::Writer;

use num_enum::TryFromPrimitive;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::mem::take;
use std::result::Result;
use tracing::debug;

// ReceiveIter requests and receives results from the proxy. For sorting
// queries, it performs a merge sort of the received results. It also
// performs duplicate elimination for queries that require it (note:
// a query can do both sorting and dup elimination).

#[add_planiter_fields]
#[derive(Debug, Default, Clone)]
pub(crate) struct ReceiveIter {
    // Following fields are created in deserialization and are immutable
    distribution_kind: DistributionKind,
    // Used for sorting queries. It specifies the names of the top-level
    // fields that contain the values on which to sort the received results.
    sort_fields: Vec<String>,
    sort_specs: Vec<SortSpec>,
    // Used for duplicate elimination. It specifies the names of the top-level
    // fields that contain the primary-key values within the received results.
    // if this is empty, duplicates are allowed.
    prim_key_fields: Vec<String>,

    // Following fields are created/updated during iteration
    // and should be cleared by reset()
    data: ReceiveIterData,
}

// Note: purposefully does not derive or implement Clone
#[derive(Debug, Default)]
pub(crate) struct ReceiveIterData {
    state: PlanIterState,

    // Used for sorting all-partition queries. It specifies whether the
    // query execution is in sort phase 1.
    pub(crate) in_sort_phase_1: bool,
    pub(crate) continuation_key: Option<Vec<u8>>,

    // The following 4 fields are used during phase 1 of a sorting AllPartitions
    // query (see javadoc of PartitionUnionIter in kvstore for description of
    // the algorithm used to execute such queries). In this case, the results may
    // store results from multiple partitions. If so, the results are grouped by
    // partition and the pids, numResultsPerPid, and continuationKeys fields
    // store the partition id, the number of results, and the continuation key
    // per partition. Finally, the isInPhase1 specifies whether phase 1 is done.
    pub(crate) pids: Vec<i32>,
    pub(crate) num_results_per_pid: Vec<i32>,
    pub(crate) part_continuation_keys: Vec<Vec<u8>>,

    base_vsid: i32,

    // Hash set used for duplicate elimination. It stores the primary
    // keys (in binary format) of all the results seen so far.
    prim_key_set: HashSet<Vec<u8>>,

    // The memory consumed by this ReceiveIter. Memory consumption is
    // counted for sorting all-partiton queries and/or queries that do
    // duplicate elimination. We count the memory taken by results cached
    // in theSortedScanners and/or primary keys stored in thePrimKeysSet.
    memory_consumption: i64,

    // The memory consumed for duplicate elimination
    dup_elim_memory: i64,

    // theTotalResultsSize and theTotalNumResults store the total size
    // and number of results fetched by this ReceiveIter so far. They
    // are used to compute the average result size, which is then used
    // to compute the max number of results to fetch from a partition
    // during a sort-phase-2 request for a sorting, all-partition query.
    total_results_size: i64,

    total_num_results: i64,

    // The remote scanner used for non-sorting queries.
    scanner: RemoteScanner,

    // The remote scanners used for sorting queries. For all-shard queries
    // there is one RemoteScanner per shard. For all-partition queries
    // a RemoteScanner is created for each partition that has at least one
    // result. See the javadoc of PartitionUnionIter in kvstore for a
    // description of how all-partition queries are executed in the 3-tier
    // architecture.
    sorted_scanners: BinaryHeap<RemoteScanner>,
}

impl ReceiveIterData {
    pub fn reset(&mut self) {
        self.state = PlanIterState::Uninitialized;
        self.prim_key_set.clear();
        self.in_sort_phase_1 = true;
        self.continuation_key = None;
        self.pids = Vec::new();
        self.num_results_per_pid = Vec::new();
        self.part_continuation_keys = Vec::new();
        self.memory_consumption = 0;
        self.dup_elim_memory = 0;
        self.total_results_size = 0;
        self.total_num_results = 0;
        self.base_vsid = 0;
        self.scanner = RemoteScanner::default();
        self.sorted_scanners = BinaryHeap::default();
    }
}

impl Clone for ReceiveIterData {
    fn clone(&self) -> Self {
        // clone does NOT copy the ephemeral data
        let mut rid = ReceiveIterData::default();
        rid.reset();
        rid
    }
    fn clone_from(&mut self, _source: &Self) {
        self.reset();
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, TryFromPrimitive)]
#[repr(i16)]
pub(crate) enum DistributionKind {
    // The query predicates specify a complete shard key, and as a result,
    // the query goes to a single partition and uses the primary index for
    // its execution.
    #[default]
    SinglePartition = 0,
    // The query uses the primary index for its execution, but does not
    // specify a complete shard key. As a result, it must be sent to all
    // partitions.
    AllPartitions = 1,
    // The query uses a secondary index for its execution. As a result,
    // it must be sent to all shards.
    AllShards = 2,
}

impl DistributionKind {
    pub(crate) fn try_from_i16(val: i16) -> Result<Self, NoSQLError> {
        match DistributionKind::try_from(val) {
            Ok(fc) => {
                return Ok(fc);
            }
            Err(_) => {
                return Err(NoSQLError::new(
                    BadProtocolMessage,
                    &format!("unrecognized distribution kind {}", val),
                ));
            }
        }
    }
}

#[derive(Debug, Default)]
struct RemoteScanner {
    pub is_for_shard: bool,
    pub shard_or_part_id: i32,
    pub continuation_key: Option<Vec<u8>>,
    pub more_remote_results: bool,
    // pub virtual_scans: Vec<VirtualScan>,
    // pub virtual_scan: VirtualScan,
    pub(crate) results: VecDeque<MapValue>,

    // these need to be copied for RemoteScanner::Ord
    sort_fields: Vec<String>,
    sort_specs: Vec<SortSpec>,
}

impl Ord for RemoteScanner {
    fn cmp(&self, other: &Self) -> Ordering {
        // Note all of the values returned are REVERSED due to
        // BinaryHeap being a max_heap, but we want a min_heap.

        if self.has_local_results() == false {
            if other.has_local_results() {
                return Ordering::Greater;
            }
            if self.shard_or_part_id < other.shard_or_part_id {
                return Ordering::Greater;
            }
            return Ordering::Less;
        }
        if other.has_local_results() == false {
            return Ordering::Less;
        }
        let v1 = self.results.front().unwrap();
        let v2 = other.results.front().unwrap();
        let comp = compare_results(v1, v2, &self.sort_fields, &self.sort_specs);
        debug!(
            "RI: cmp: ss={:?} v1={:?} v2={:?} result={:?}",
            self.sort_specs, v1, v2, comp
        );
        if comp == Ordering::Greater {
            return Ordering::Less;
        }
        if comp == Ordering::Less {
            return Ordering::Greater;
        }
        Ordering::Equal
    }
}

impl PartialEq for RemoteScanner {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RemoteScanner {}

impl PartialOrd for RemoteScanner {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl RemoteScanner {
    pub fn new(
        is_for_shard: bool,
        shard_or_part_id: i32,
        sort_fields: &Vec<String>,
        sort_specs: &Vec<SortSpec>,
    ) -> Self {
        RemoteScanner {
            is_for_shard,
            shard_or_part_id,
            more_remote_results: true,
            continuation_key: None,
            results: VecDeque::new(),
            sort_fields: sort_fields.clone(),
            sort_specs: sort_specs.clone(),
        }
    }

    //pub fn reset(&mut self) {
    //self.more_remote_results = true;
    //self.continuation_key = None;
    //self.results = VecDeque::new();
    //}

    pub fn is_done(&self) -> bool {
        self.results.len() == 0 && self.more_remote_results == false
    }

    pub fn has_local_results(&self) -> bool {
        self.results.len() > 0
    }

    pub fn add_results(&mut self, results: VecDeque<MapValue>, cont_key: Option<Vec<u8>>) {
        debug!("add_results: cur={:?} new={:?}\n", self.results, results);
        self.results = results;
        self.continuation_key = None;
        if let Some(ck) = cont_key {
            if ck.len() > 0 {
                self.continuation_key = Some(ck);
            }
        }
        self.more_remote_results = self.continuation_key.is_some();
        // add memory consumption
    }

    pub fn next_local(&mut self) -> Option<MapValue> {
        self.results.pop_front()
    }

    // RemoteScanner
    pub async fn next(
        &mut self,
        iter_data: &mut ReceiveIterData,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<Option<MapValue>, NoSQLError> {
        if self.results.len() > 0 {
            debug!("RemoteScanner.next(): popping front result");
            return Ok(self.results.pop_front());
        }

        if self.more_remote_results == false || req.reached_limit == true {
            debug!("RemoteScanner.next(): no more results");
            return Ok(None);
        }

        debug!("RemoteScanner.next(): fetching more results");
        self.fetch(iter_data, req, handle).await?;

        if self.results.len() == 0 {
            debug!("RemoteScanner.next(): fetch returned no results");
            return Ok(None);
        }

        debug!("RemoteScanner.next(): popping first fetched result");
        Ok(self.results.pop_front())
    }

    async fn fetch(
        &mut self,
        data: &mut ReceiveIterData,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<(), NoSQLError> {
        req.batch_counter += 1;
        let mut req_copy = req.copy_for_internal();
        req_copy.continuation_key = self.continuation_key.clone();
        if self.is_for_shard {
            req_copy.shard_id = self.shard_or_part_id;
        } else {
            req_copy.shard_id = -1;
        }

        // TODO: reduce limit to meet max memory setting

        // TODO: if (theVirtualScan != null) {
        //assert(theIsForShard);
        //assert(doesSort());
        //reqCopy.setVirtualScan(theVirtualScan);
        //}

        debug!(
            "\nReceiveIter executing internal request copy (shard={}):\n",
            req_copy.shard_id
        );
        let mut vr: Vec<MapValue> = Vec::new();
        req_copy
            .execute_batch_internal(handle, &mut vr, data)
            .await?;
        debug!(
            "EBI returned {} results (shard={}): {:?}",
            vr.len(),
            req_copy.shard_id,
            vr
        );
        self.add_results(VecDeque::from(vr), req_copy.continuation_key);
        req.consumed_capacity.add(&req_copy.consumed_capacity);

        // TODO: if (theVirtualScan != null && theVirtualScan.isFirstBatch()) {
        // theVirtualScan.theFirstBatch = false;
        // }
        // theVirtualScans = result.getVirtualScans();

        //theRCB.tallyRateLimitDelayedMs(result.getRateLimitDelayedMs());
        //theRCB.tallyRetryStats(result.getRetryStats());
        //origRequest.addQueryTraces(result.getQueryTraces());

        if self.more_remote_results == true && req.reached_limit == false {
            return ia_err!("didn't reach limit but more results exist");
        }

        Ok(())
    }
}

// We must implement Clone manually for ReceiveIter because its data
// has items that are only created in ReceiveIter::new(), and clone should
// reset that data...
//impl Clone for ReceiveIter {
//fn clone(&self) -> Self {
//let mut iter = ReceiveIter {
//state: PlanIterState::Uninitialized,
//result_reg: self.result_reg.clone(),
//loc: self.loc.clone(),
//distribution_kind: self.distribution_kind.clone(),
//sort_fields: self.sort_fields.clone(),
//sort_specs: self.sort_specs.clone(),
//prim_key_fields: self.prim_key_fields.clone(),
//
//data: ReceiveIterData::default(),
//};
//let _ = iter.reset();
//iter
//}
//fn clone_from(&mut self, source: &Self) {
//self.state = PlanIterState::Uninitialized;
//self.result_reg = source.result_reg.clone();
//self.loc = source.loc.clone();
//self.distribution_kind = source.distribution_kind.clone();
//self.sort_fields = source.sort_fields.clone();
//self.sort_specs = source.sort_specs.clone();
//self.prim_key_fields = source.prim_key_fields.clone();
//
//self.data = ReceiveIterData::default();
//let _ = self.reset();
//}
//}

impl ReceiveIter {
    pub fn new(r: &mut Reader) -> Result<Self, NoSQLError> {
        // state_pos is now ignored, in the rust driver implementation
        let rr = r.read_i32()?; // result_reg
        let sp = r.read_i32()?; // state_pos
        debug!("\n . ReceiveIter: result_reg={} state_pos={}\n", rr, sp);
        let mut iter = ReceiveIter {
            // fields common to all PlanIters
            result_reg: rr,
            loc: Location::from_reader(r)?,

            // specific to ReceiveIter: immutable afterwards
            distribution_kind: DistributionKind::try_from_i16(r.read_i16()?)?,
            sort_fields: r.read_string_array()?,
            sort_specs: SortSpec::read_sort_specs(r)?,
            prim_key_fields: r.read_string_array()?,

            ..Default::default()
        };
        // note this creates/resets the iter data (scanners, etc)
        let _ = iter.reset();
        Ok(iter)
    }

    pub fn open(&mut self, req: &QueryRequest, _handle: &Handle) -> Result<(), NoSQLError> {
        debug!("ReceiveIter.open(): current state = {:?}", self.data.state);
        if self.data.state == PlanIterState::Open {
            return Ok(());
        }
        if self.does_sort() && self.distribution_kind == DistributionKind::AllPartitions {
            // Nothing to do; done later
        } else if self.does_sort() && self.distribution_kind == DistributionKind::AllShards {
            let ti = &req.topology_info;
            if !ti.is_valid() {
                return ia_err!("invalid TopologyInfo passed into ReceiveIterData::reset");
            }
            let num_shards = ti.shard_ids.len();
            for i in 0..num_shards {
                self.data.sorted_scanners.push(RemoteScanner::new(
                    true,
                    ti.shard_ids[i],
                    &self.sort_fields,
                    &self.sort_specs,
                ));
            }
            self.data.base_vsid = ti.shard_ids[num_shards - 1];
        } else {
            self.data.scanner = RemoteScanner::new(false, -1, &self.sort_fields, &self.sort_specs);
        }
        self.data.state = PlanIterState::Open;
        Ok(())
    }
    /*
        pub fn display_content(&self, sb: &mut String, f: &PlanFormatter) {
            f.print_indent(sb);
            sb.push_str("ReceiveIter: display_content(): TODO");
            //sb.push_str(format!("{:?}", self.value).as_str());
        }
        pub fn get_plan(&self) -> String {
            format!("{:?}", self)
        }
    */
    pub fn get_kind(&self) -> PlanIterKind {
        PlanIterKind::Recv
    }
    pub async fn next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.data.state.is_done() {
            debug!("ReceiveIter.next(): is_done");
            return Ok(false);
        }

        if self.does_sort() == false {
            return self.simple_next(req, handle).await;
        }

        self.sorting_next(req, handle).await
    }
    fn does_sort(&self) -> bool {
        self.sort_fields.len() > 0
    }
    pub fn get_result(&self, req: &mut QueryRequest) -> FieldValue {
        let fv = req.get_result(self.result_reg);
        debug!("RI{} get_result={:?}", self.result_reg, fv);
        fv
    }
    pub fn set_result(&self, req: &mut QueryRequest, result: FieldValue) {
        debug!("RI{} set_result({:?})", self.result_reg, result);
        req.set_result(self.result_reg, result);
    }
    // Default all values, as if this was just created by deserialization
    pub fn reset(&mut self) -> Result<(), NoSQLError> {
        self.data.reset();
        Ok(())
    }
    /*
        pub fn close(&mut self, _req: &QueryRequest) -> Result<(), NoSQLError> {
            self.data.state = PlanIterState::Closed;
            Ok(())
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

    async fn simple_next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        debug!("ReceiveIter::simple_next()");
        loop {
            let mut scanner = take(&mut self.data.scanner);
            let ret = scanner.next(&mut self.data, req, handle).await?;
            self.data.scanner = scanner;
            if ret.is_none() {
                break;
            }
            let mv = ret.unwrap();
            debug!("simple_next: mv={:?}", mv);
            if self.check_duplicate(&mv)? {
                continue;
            }
            debug!("ReceiveIter.simple_next(): adding 1 result: {:?}", mv);
            self.set_result(req, FieldValue::Map(mv));
            return Ok(true);
        }

        if req.reached_limit == false {
            self.done();
        }

        return Ok(false);
    }

    fn done(&mut self) {
        self.data.reset();
    }

    async fn sorting_next(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<bool, NoSQLError> {
        if self.distribution_kind == DistributionKind::AllPartitions
            && self.data.in_sort_phase_1 == true
        {
            debug!("ReceiveIter.sorting_next(): calling init_partition_sort");
            self.init_partition_sort(req, handle).await?;
            return Ok(false);
        }

        debug!("ReceiveIter::sorting_next()");
        loop {
            debug!("  getting next scanner\n");
            let sc = self.data.sorted_scanners.pop();
            if sc.is_none() {
                debug!("  no more scanners\n");
                self.done();
                return Ok(false);
            }

            let mut scanner = sc.unwrap();
            debug!("  working with scanner({})\n", scanner.shard_or_part_id);
            if let Some(mv) = scanner.next_local() {
                if scanner.is_done() == false {
                    debug!("  putting scanner({}) back", scanner.shard_or_part_id);
                    self.data.sorted_scanners.push(scanner);
                }
                // TODO mv.convert_empty_to_null()
                if self.check_duplicate(&mv)? {
                    continue;
                }
                debug!("ReceiveIter.sorting_next(): adding 1 result: {:?}", mv);
                self.set_result(req, FieldValue::Map(mv));
                return Ok(true);
            }

            // Scanner had no cached results. If it may have remote results,
            // send a request to fetch more results. Otherwise, throw it away
            // (by leaving it outside theSortedScanners) and continue with
            // another scanner.
            if scanner.is_done() {
                debug!("  scanner({}) is done\n", scanner.shard_or_part_id);
                continue;
            }

            let mut data = take(&mut self.data);

            debug!(
                "ReceiveIter.sorting_next() calling scanner({}) fetch",
                scanner.shard_or_part_id
            );
            match scanner.fetch(&mut data, req, handle).await {
                Err(e) => {
                    // TODO: if err is retryable, put back and try again
                    // if err.is_retryable()
                    // self.data.sorted_scanners.push(scanner);
                    return Err(e);
                }
                _ => (),
            }
            self.data = data;

            // We executed a remote fetch. If we got any result or the scanner
            // may have more remote results, put the scanner back into
            // theSortedScanner. Otherwise, throw it away.
            if scanner.is_done() == false {
                debug!(
                    "  scanner({}) has more: putting back in",
                    scanner.shard_or_part_id
                );
                self.data.sorted_scanners.push(scanner);
            }

            // For simplicity, we don't want to allow the possibility of
            // another remote fetch during the same batch, so whether or not
            // the batch limit was reached during the above fetch, we set
            // limit flag to true and return false, thus terminating the
            // current batch.
            debug!("ReceiveIter.sorting_next() setting reached_limit=true");
            req.reached_limit = true;
            break;
        }
        Ok(false)
    }

    async fn init_partition_sort(
        &mut self,
        req: &mut QueryRequest,
        handle: &Handle,
    ) -> Result<(), NoSQLError> {
        // Create and execute a request to get at least one result from
        // the partition whose id is specified in theContinuationKey and
        // from any other partition that is co-located with that partition.
        let mut req_copy = req.copy_for_internal();
        req_copy.continuation_key = self.data.continuation_key.clone();

        debug!("ReceiveIter init_partition_sort executing internal request copy:\n");
        let mut vr: Vec<MapValue> = Vec::new();
        req_copy
            .execute_batch_internal(handle, &mut vr, &mut self.data)
            .await?;
        let mut results = VecDeque::from(vr);
        req.consumed_capacity.add(&req_copy.consumed_capacity);

        //rcb.tallyRateLimitDelayedMs(result.getRateLimitDelayedMs());
        //rcb.tallyRetryStats(result.getRetryStats());

        // For each partition P that was accessed during the execution of
        // the above QueryRequest, collect the results for P and create a
        // scanner that will be used during phase 2 to collect further
        // results from P only.
        for p in 0..self.data.pids.len() {
            let pid = self.data.pids[p];
            let num_results = self.data.num_results_per_pid[p];
            let cont_key = take(&mut self.data.part_continuation_keys[p]);

            debug!("  pid={} nr={} cont_key={:?}\n", pid, num_results, cont_key);

            if num_results <= 0 {
                return ia_err!("expected at least one result for partition");
            }

            if num_results > results.len() as i32 {
                return ia_err!("expected more results than we got");
            }

            let mut part_results: VecDeque<MapValue> =
                VecDeque::with_capacity(num_results as usize);
            for _j in 0..num_results {
                if let Some(r) = results.pop_front() {
                    part_results.push_back(r);
                } else {
                    return ia_err!("got None when trying to read partition results");
                }
            }

            let mut scanner = RemoteScanner::new(
                false,
                pid,
                &self.sort_fields.clone(),
                &self.sort_specs.clone(),
            );

            scanner.add_results(part_results, Some(cont_key));
            debug!("created new scanner: {:?}", scanner);
            self.data.sorted_scanners.push(scanner);
        }

        // For simplicity, if the size limit was not reached during this
        // batch of sort phase 1, we don't start a new batch. We let the
        // app do it. Furthermore, this means that each remote fetch will
        // be done with the max amount of read limit, which will reduce the
        // total number of fetches.
        debug!("ReceiveIter.init_partition_sort setting reached_limit=true");
        req.reached_limit = true;

        Ok(())
    }

    // return true if this value is a duplicate, and dups should be eliminated
    fn check_duplicate(&mut self, mv: &MapValue) -> Result<bool, NoSQLError> {
        if self.prim_key_fields.len() == 0 {
            return Ok(false);
        }
        let v = self.create_binary_prim_key(mv)?;
        if self.data.prim_key_set.insert(v) == false {
            return Ok(true);
        }
        Ok(false)
    }

    fn create_binary_prim_key(&self, mv: &MapValue) -> Result<Vec<u8>, NoSQLError> {
        let mut w = Writer::new();
        for i in 0..self.prim_key_fields.len() {
            let fv = mv.get_field_value(&self.prim_key_fields[i]);
            match fv {
                Some(f) => {
                    w.write_field_value(f);
                }
                None => {
                    return ia_err!(
                        "can't create binary primary key: no field '{}' in record",
                        self.prim_key_fields[i]
                    );
                }
            }
        }
        Ok(w.buf)
    }
}
