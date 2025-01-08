/*
 * Copyright (c) 2024 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{
    collections::{HashMap, VecDeque},
    ffi::CStr,
    slice,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use log::{debug, trace, warn};
use procfs::process::Process;
use zstd::bulk::compress;

use public::{proto::metric, queue::DebugSender};

use super::{string_from_null_terminated_c_str, POLICY_GETTER};

use crate::ebpf;
use crate::integration_collector::Profile;

const QUEUE_BATCH_SIZE: usize = 1024;

#[derive(Debug, Default, Eq, PartialEq)]
struct AllocInfo {
    stack_id: u32,
    size: u64,
}

#[derive(Debug, Default)]
struct AddressRecord {
    allocs: VecDeque<AllocInfo>,
    frees: usize,
}

#[derive(Debug, Default)]
struct ProcessAllocInfo {
    allocated_addrs: HashMap<u64, AddressRecord>,

    alloc: HashMap<u32, metric::Profile>, // allocs between report interval
    in_use: HashMap<u32, metric::Profile>,
}

impl ProcessAllocInfo {
    unsafe fn update_stack(
        stacks: &mut HashMap<u32, metric::Profile>,
        data: &ebpf::stack_profile_data,
        update_trace_str: bool,
    ) {
        let entry = stacks.entry(data.u_stack_id).or_insert_with(|| {
            let mut profile = metric::Profile::default();
            profile.sample_rate = 1; // reported every second
            profile.stime = data.stime;
            profile.pid = data.pid;
            profile.tid = data.tid;
            profile.thread_name = string_from_null_terminated_c_str(data.comm.as_ptr());
            profile.process_name = string_from_null_terminated_c_str(data.process_name.as_ptr());
            profile.u_stack_id = data.u_stack_id;
            profile.cpu = data.cpu;
            profile.wide_count = 0;
            let profile_data =
                slice::from_raw_parts(data.stack_data as *mut u8, data.stack_data_len as usize);
            profile.data = profile_data.to_vec();

            let container_id =
                CStr::from_ptr(data.container_id.as_ptr() as *const libc::c_char).to_string_lossy();
            if let Some(policy_getter) = POLICY_GETTER.as_ref() {
                profile.pod_id = policy_getter.lookup_pod_id(&container_id);
            }
            profile
        });
        entry.wide_count += data.count;
        if update_trace_str {
            let trace_str =
                slice::from_raw_parts(data.stack_data as *mut u8, data.stack_data_len as usize);
            if trace_str != &entry.data {
                entry.data = trace_str.to_vec();
            }
        }
    }

    unsafe fn update(&mut self, data: &ebpf::stack_profile_data) {
        // Memory allocations and frees may come in any order in a multi-core envrionment.
        // Make best effort to handle such situation.
        if data.count == 0 {
            // frees
            if let Some(info) = self.allocated_addrs.get_mut(&data.mem_addr) {
                if let Some(allocated) = info.allocs.pop_front() {
                    if let Some(p) = self.in_use.get_mut(&allocated.stack_id) {
                        if p.wide_count > allocated.size as u64 {
                            p.wide_count -= allocated.size as u64;
                        } else {
                            self.in_use.remove(&allocated.stack_id);
                        }
                    }
                    if info.allocs.is_empty() {
                        assert_eq!(info.frees, 0);
                        self.allocated_addrs.remove(&data.mem_addr);
                    }
                } else {
                    // out of order free without corresponding alloc, add free count
                    info.frees += 1;
                }
            } else {
                self.allocated_addrs.insert(
                    data.mem_addr,
                    AddressRecord {
                        allocs: VecDeque::new(),
                        frees: 1,
                    },
                );
            }
        } else {
            // allocs
            Self::update_stack(&mut self.alloc, data, false);
            // for languages without free (i.e. JAVA), not recording in_use info, only allocs
            if data.mem_addr == 0 {
                return;
            }

            if let Some(info) = self.allocated_addrs.get_mut(&data.mem_addr) {
                if info.frees != 0 {
                    // recorded free before any allocs, decrease free count
                    info.frees -= 1;
                    if info.frees == 0 {
                        assert!(info.allocs.is_empty());
                        self.allocated_addrs.remove(&data.mem_addr);
                    }
                } else {
                    // multiple allocs without frees
                    // unordered allocations will make in_use value inaccurate, but there's no easy fix at the moment
                    info.allocs.push_back(AllocInfo {
                        stack_id: data.u_stack_id,
                        size: data.count,
                    });
                    Self::update_stack(&mut self.in_use, data, true);
                }
            } else {
                self.allocated_addrs.insert(
                    data.mem_addr,
                    AddressRecord {
                        allocs: VecDeque::from([AllocInfo {
                            stack_id: data.u_stack_id,
                            size: data.count,
                        }]),
                        frees: 0,
                    },
                );
                Self::update_stack(&mut self.in_use, data, true);
            }
        }
    }
}

pub struct MemoryContext {
    processes: HashMap<u32, ProcessAllocInfo>,

    report_interval_secs: Arc<AtomicU8>,
    last_report: Duration,
    stack_compression: bool,
}

impl MemoryContext {
    // There's a small difference (less than 1s) between start time from profiler and from procfs rust lib.
    // If cached stime from profiler plus THRESHOLD is smaller than start time from procfs, it's a process restart
    const PROCESS_RESTART_THRESHOLD: Duration = Duration::from_secs(3);

    pub fn new(report_interval: Duration, stack_compression: bool) -> Self {
        Self {
            processes: Default::default(),
            report_interval_secs: Arc::new(AtomicU8::new(report_interval.as_secs() as u8)),
            last_report: Default::default(),
            stack_compression: stack_compression,
        }
    }

    pub fn settings(&self) -> MemoryContextSettings {
        MemoryContextSettings {
            report_interval_secs: self.report_interval_secs.clone(),
        }
    }

    pub unsafe fn update(&mut self, data: &ebpf::stack_profile_data) {
        assert_eq!(data.profiler_type, ebpf::PROFILER_TYPE_MEMORY);
        self.processes
            .entry(data.pid)
            .or_insert(Default::default())
            .update(data);
    }

    pub fn report(&mut self, timestamp: Duration, sender: &mut DebugSender<Profile>) {
        if self.last_report.is_zero() {
            self.last_report = timestamp;
            return;
        }
        let report_interval =
            Duration::from_secs(self.report_interval_secs.load(Ordering::Relaxed) as u64);
        if timestamp < self.last_report + report_interval {
            return;
        }
        self.last_report = timestamp;

        let mut batch = Vec::with_capacity(QUEUE_BATCH_SIZE);
        let mut dead_pids = vec![];

        'outer: for (pid, info) in self.processes.iter_mut() {
            let process_info = match Process::new(*pid as i32) {
                Ok(p) => p,
                Err(e) => {
                    trace!("process #{pid} likely exited: {e}");
                    dead_pids.push(*pid);
                    continue;
                }
            };
            let stime = match process_info.stat().and_then(|s| s.starttime()) {
                Ok(s) => s.timestamp_millis() as u64,
                Err(e) => {
                    debug!("process #{pid} get start time failed: {e}");
                    dead_pids.push(*pid);
                    continue;
                }
            } - Self::PROCESS_RESTART_THRESHOLD.as_millis() as u64;

            let exited = info.alloc.values().any(|p| p.stime < stime)
                || info.in_use.values().any(|p| p.stime < stime);
            if exited {
                trace!("process #{pid} restarted");
                dead_pids.push(*pid);
                continue;
            }

            // clean up alloc map every report
            for (_, mut p) in info.alloc.drain() {
                if batch.len() >= QUEUE_BATCH_SIZE {
                    if let Err(e) = sender.send_all(&mut batch) {
                        warn!("output queue failed to send data: {e}");
                        batch.clear();
                        break 'outer;
                    }
                }
                p.event_type = metric::ProfileEventType::EbpfMemAlloc.into();
                p.timestamp = (timestamp - report_interval).as_nanos() as u64;
                p.count = p.wide_count as u32;
                if self.stack_compression {
                    match compress(&p.data, 0) {
                        Ok(compressed_data) => {
                            p.data_compressed = true;
                            p.data = compressed_data;
                        }
                        Err(e) => {
                            debug!("failed to compress ebpf memory profile: {:?}", e);
                        }
                    }
                }
                batch.push(Profile(p));
            }

            // keep in_use map
            for rp in info.in_use.values() {
                if batch.len() >= QUEUE_BATCH_SIZE {
                    if let Err(e) = sender.send_all(&mut batch) {
                        warn!("output queue failed to send data: {e}");
                        batch.clear();
                        break 'outer;
                    }
                }
                let mut p = rp.clone();
                p.event_type = metric::ProfileEventType::EbpfMemInUse.into();
                p.timestamp = (timestamp - report_interval).as_nanos() as u64;
                p.count = p.wide_count as u32;
                if self.stack_compression {
                    match compress(&p.data, 0) {
                        Ok(compressed_data) => {
                            p.data_compressed = true;
                            p.data = compressed_data;
                        }
                        Err(e) => {
                            debug!("failed to compress ebpf memory profile: {:?}", e);
                        }
                    }
                }
                batch.push(Profile(p));
            }
        }

        if !batch.is_empty() {
            if let Err(e) = sender.send_all(&mut batch) {
                warn!("output queue failed to send data: {e}");
                batch.clear();
            }
        }

        for pid in dead_pids {
            self.processes.remove(&pid);
        }
    }
}

pub struct MemoryContextSettings {
    report_interval_secs: Arc<AtomicU8>,
}

impl MemoryContextSettings {
    pub fn set_report_interval(&self, interval: Duration) {
        self.report_interval_secs
            .store(interval.as_secs() as u8, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ebpf;

    struct AllocData {
        stack_id: u32,
        addr: u64,
        size: u64,
    }

    impl ProcessAllocInfo {
        fn update_alloc_data(&mut self, data: AllocData) {
            let mut buf = [0u8; 0];
            unsafe {
                self.update(&ebpf::stack_profile_data {
                    u_stack_id: data.stack_id,
                    mem_addr: data.addr,
                    count: data.size,

                    profiler_type: 0,
                    timestamp: 0,
                    pid: 0,
                    tid: 0,
                    stime: 0,
                    netns_id: 0,
                    k_stack_id: 0,
                    cpu: 0,
                    comm: [0; ebpf::PACKET_KNAME_MAX_PADDING + 1],
                    process_name: [0; ebpf::PACKET_KNAME_MAX_PADDING + 1],
                    container_id: [0; ebpf::CONTAINER_ID_SIZE],
                    stack_data_len: 0,
                    stack_data: buf.as_mut_ptr() as *mut libc::c_char,
                });
            }
        }
    }

    #[test]
    fn normal_alloc_free() {
        let mut info = ProcessAllocInfo::default();
        info.update_alloc_data(AllocData {
            stack_id: 0x1234,
            addr: 0xdead,
            size: 0xc0de,
        });
        info.update_alloc_data(AllocData {
            stack_id: 0x1234,
            addr: 0xbeef,
            size: 0xc0de,
        });

        assert_eq!(
            info.allocated_addrs
                .get(&0xdead)
                .and_then(|r| r.allocs.front())
                .unwrap(),
            &AllocInfo {
                stack_id: 0x1234,
                size: 0xc0de
            }
        );
        assert_eq!(info.in_use.get(&0x1234).unwrap().wide_count, 2 * 0xc0de);

        info.update_alloc_data(AllocData {
            stack_id: 0x1234,
            addr: 0xdead,
            size: 0,
        });

        assert!(info.allocated_addrs.get(&0xdead).is_none());
        assert_eq!(info.in_use.get(&0x1234).unwrap().wide_count, 0xc0de);

        info.update_alloc_data(AllocData {
            stack_id: 0x4321,
            addr: 0xbeef,
            size: 0,
        });

        assert!(info.allocated_addrs.get(&0xbeef).is_none());
        assert_eq!(info.in_use.get(&0x1234), None);
    }

    #[test]
    fn unordered_alloc_free() {
        let mut info = ProcessAllocInfo::default();
        info.update_alloc_data(AllocData {
            stack_id: 0x1234,
            addr: 0xdeadbeef,
            size: 0xc0de,
        });
        // alloc at same address
        info.update_alloc_data(AllocData {
            stack_id: 0x1234,
            addr: 0xdeadbeef,
            size: 0xc0dec0de,
        });

        assert_eq!(
            info.allocated_addrs
                .get(&0xdeadbeef)
                .and_then(|r| r.allocs.get(0))
                .unwrap(),
            &AllocInfo {
                stack_id: 0x1234,
                size: 0xc0de
            }
        );
        assert_eq!(
            info.allocated_addrs
                .get(&0xdeadbeef)
                .and_then(|r| r.allocs.get(1))
                .unwrap(),
            &AllocInfo {
                stack_id: 0x1234,
                size: 0xc0dec0de
            }
        );
        assert_eq!(
            info.in_use.get(&0x1234).unwrap().wide_count,
            0xc0de + 0xc0dec0de
        );

        info.update_alloc_data(AllocData {
            stack_id: 0x1234,
            addr: 0xdeadbeef,
            size: 0,
        });

        assert_eq!(
            info.allocated_addrs
                .get(&0xdeadbeef)
                .and_then(|r| r.allocs.get(0))
                .unwrap(),
            &AllocInfo {
                stack_id: 0x1234,
                size: 0xc0dec0de
            }
        );
        assert_eq!(info.in_use.get(&0x1234).unwrap().wide_count, 0xc0dec0de);

        info.update_alloc_data(AllocData {
            stack_id: 0x1234,
            addr: 0xdeadbeef,
            size: 0,
        });
        // one more free
        info.update_alloc_data(AllocData {
            stack_id: 0x1234,
            addr: 0xdeadbeef,
            size: 0,
        });

        assert!(info
            .allocated_addrs
            .get(&0xdeadbeef)
            .unwrap()
            .allocs
            .is_empty());
        assert_eq!(info.allocated_addrs.get(&0xdeadbeef).unwrap().frees, 1);
        assert_eq!(info.in_use.get(&0x1234), None);

        info.update_alloc_data(AllocData {
            stack_id: 0x1234,
            addr: 0xdeadbeef,
            size: 0xc0dec0de,
        });

        assert!(info.allocated_addrs.get(&0xdeadbeef).is_none());
        assert_eq!(info.in_use.get(&0x1234), None);
    }
}
