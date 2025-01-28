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
    boxed::Box,
    cell::RefCell,
    collections::HashSet,
    mem,
    net::Ipv4Addr,
    num::NonZeroUsize,
    rc::Rc,
    str::FromStr,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Weak,
    },
    time::{Duration, SystemTime},
};

use ahash::AHashMap;
use log::{debug, warn};
use lru::LruCache;
use pnet::packet::icmp::IcmpTypes;

use super::{
    app_table::AppTable,
    error::Error,
    flow_state::{StateMachine, StateValue},
    perf::{tcp::TcpPerf, FlowLog, FlowPerfCounter, L7ProtocolChecker},
    pool::MemoryPool,
    protocol_logs::{
        sql::{ObfuscateCache, OBFUSCATE_CACHE_SIZE},
        AppProto, MetaAppProto,
    },
    service_table::{ServiceKey, ServiceTable},
    FlowMapKey, FlowNode, FlowState, FlowTimeout, COUNTER_FLOW_ID_MASK, FLOW_METRICS_PEER_DST,
    FLOW_METRICS_PEER_SRC, QUEUE_BATCH_SIZE, SERVICE_TABLE_IPV4_CAPACITY,
    SERVICE_TABLE_IPV6_CAPACITY, STATISTICAL_INTERVAL, THREAD_FLOW_ID_MASK, TIMER_FLOW_ID_MASK,
    TIME_UNIT,
};

use crate::{
    common::{
        ebpf::EbpfType,
        endpoint::{EndpointData, EndpointDataPov, EndpointInfo, EPC_DEEPFLOW, EPC_INTERNET},
        enums::{CaptureNetworkType, EthernetType, HeaderType, IpProtocol, TcpFlags},
        flow::{
            CloseType, Flow, FlowKey, FlowMetricsPeer, FlowPerfStats, L4Protocol, L7PerfStatsKey,
            L7Protocol, L7Stats, PacketDirection, SignalSource, TunnelField,
        },
        l7_protocol_info::{L7ProtocolInfo, L7ProtocolInfoInterface},
        l7_protocol_log::{
            L7PerfCache, L7ProtocolBitmap, L7ProtocolParser, L7ProtocolParserInterface,
        },
        lookup_key::LookupKey,
        meta_packet::{MetaPacket, MetaPacketTcpHeader, ProtocolData},
        tagged_flow::TaggedFlow,
        tap_port::TapPort,
        Timestamp,
    },
    config::{
        handler::{CollectorConfig, LogParserConfig, PluginConfig},
        FlowConfig, ModuleConfig, UserConfig,
    },
    flow_generator::{protocol_logs::PseudoAppProto, LogMessageType},
    metric::document::TapSide,
    plugin::wasm::WasmVm,
    policy::{Policy, PolicyGetter},
    rpc::get_timestamp,
    utils::stats::{self, Countable, StatsOption},
};
#[cfg(any(target_os = "linux", target_os = "android"))]
use crate::{config::handler::EbpfConfig, plugin::c_ffi::SoPluginFunc};
use public::{
    buffer::{Allocator, BatchedBox},
    counter::{Counter, CounterType, CounterValue, RefCountable},
    debug::QueueDebugger,
    l7_protocol::L7ProtocolEnum,
    packet::SECONDS_IN_MINUTE,
    proto::agent::AgentType,
    queue::{self, DebugSender, Receiver},
    utils::net::MacAddr,
};

use packet_segmentation_reassembly::PacketSegmentationReassembly;
use packet_sequence_block::PacketSequenceBlock;

const DEFAULT_SOCKET_CLOSE_TIMEOUT: Timestamp = Timestamp::from_secs(1);

pub struct Config<'a> {
    pub flow: &'a FlowConfig,
    pub log_parser: &'a LogParserConfig,
    pub collector: &'a CollectorConfig,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub ebpf: Option<&'a EbpfConfig>, // TODO: We only need its epc_id，epc_id is not only useful for ebpf, consider moving it to FlowConfig
}

struct PluginStats<'a> {
    id: u32,
    plugin_name: &'a str,
    plugin_type: &'static str,
    export_func: &'static str,
}

impl stats::Module for PluginStats<'_> {
    fn name(&self) -> &'static str {
        "plugin"
    }

    fn tags(&self) -> Vec<StatsOption> {
        vec![
            StatsOption::Tag("id", self.id.to_string()),
            StatsOption::Tag("plugin_name", self.plugin_name.to_owned()),
            StatsOption::Tag("plugin_type", self.plugin_type.to_owned()),
            StatsOption::Tag("export_func", self.export_func.to_owned()),
        ]
    }
}

struct AllocatorStats {
    id: u32,
    obj_type: &'static str,
}

impl stats::Module for AllocatorStats {
    fn name(&self) -> &'static str {
        "allocator"
    }

    fn tags(&self) -> Vec<StatsOption> {
        vec![
            StatsOption::Tag("id", self.id.to_string()),
            StatsOption::Tag("type", self.obj_type.to_owned()),
        ]
    }
}

// not thread-safe
pub struct FlowMap {
    // The original std HashMap uses SipHash-1-3 and is slow.
    // Use ahash for better performance.
    //
    //     https://github.com/tkaitchuck/aHash/blob/master/FAQ.md#how-is-ahash-so-fast
    //
    // Strangely, using AES reduces performance.
    node_map: Option<(
        AHashMap<FlowMapKey, Vec<Box<FlowNode>>>,
        Vec<HashSet<FlowMapKey>>,
    )>,
    id: u32,
    state_machine_master: StateMachine,
    state_machine_slave: StateMachine,
    service_table: ServiceTable,
    app_table: AppTable,
    policy_getter: PolicyGetter,
    start_time: Duration,    // 时间桶中的最早时间
    start_time_in_unit: u64, // 时间桶中的最早时间，以TIME_SLOT_UNIT为单位
    hash_slots: usize,
    time_window_size: usize,
    total_flow: usize,
    time_set_slot_size: usize,
    capacity: usize,
    size: isize,

    tagged_flow_allocator: Allocator<TaggedFlow>,
    l7_stats_allocator: Allocator<L7Stats>,
    output_queue: Option<DebugSender<Arc<BatchedBox<TaggedFlow>>>>,
    l7_stats_output_queue: DebugSender<BatchedBox<L7Stats>>,
    out_log_queue: DebugSender<Box<AppProto>>,
    output_buffer: Vec<Arc<BatchedBox<TaggedFlow>>>,
    l7_stats_buffer: Vec<BatchedBox<L7Stats>>,
    protolog_buffer: Vec<Box<AppProto>>,
    last_queue_flush: Duration,
    perf_cache: Rc<RefCell<L7PerfCache>>,
    flow_perf_counter: Arc<FlowPerfCounter>,
    ntp_diff: Arc<AtomicI64>,
    packet_sequence_queue: Option<DebugSender<Box<PacketSequenceBlock>>>, // Enterprise Edition Feature: packet-sequence
    packet_sequence_enabled: bool,
    stats_counter: Arc<FlowMapCounter>,
    system_time: Duration,

    l7_protocol_checker: L7ProtocolChecker,

    time_key_buffer: Option<Vec<(u64, FlowMapKey)>>,

    // for change detection
    plugin_digest: u64,
    wasm_vm: Rc<RefCell<Option<WasmVm>>>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    so_plugin: Rc<RefCell<Option<Vec<SoPluginFunc>>>>,

    tcp_perf_pool: MemoryPool<TcpPerf>,
    flow_node_pool: MemoryPool<FlowNode>,

    stats_collector: Arc<stats::Collector>,

    obfuscate_cache: Option<ObfuscateCache>,
}

impl FlowMap {
    const MICROS_IN_SECONDS: u64 = 1_000_000;
    pub fn new(
        id: u32,
        output_queue: Option<DebugSender<Arc<BatchedBox<TaggedFlow>>>>,
        l7_stats_output_queue: DebugSender<BatchedBox<L7Stats>>,
        policy_getter: PolicyGetter,
        app_proto_log_queue: DebugSender<Box<AppProto>>,
        ntp_diff: Arc<AtomicI64>,
        config: &FlowConfig,
        packet_sequence_queue: Option<DebugSender<Box<PacketSequenceBlock>>>, // Enterprise Edition Feature: packet-sequence
        stats_collector: Arc<stats::Collector>,
        from_ebpf: bool,
    ) -> Self {
        let flow_perf_counter = Arc::new(FlowPerfCounter::default());
        let stats_counter = Arc::new(FlowMapCounter::default());
        let packet_sequence_enabled = config.packet_sequence_flag > 0 && !from_ebpf;
        let time_window_size = {
            let max_timeout = config.flow_timeout.max;
            let size = config.packet_delay.as_secs() + max_timeout.as_secs() + 1;
            size.next_power_of_two() as usize
        };

        stats_collector.register_countable(
            &stats::SingleTagModule("flow-map", "id", id),
            Countable::Ref(Arc::downgrade(&stats_counter) as Weak<dyn RefCountable>),
        );

        stats_collector.register_countable(
            &stats::SingleTagModule("flow-perf", "id", id),
            Countable::Ref(Arc::downgrade(&flow_perf_counter) as Weak<dyn RefCountable>),
        );
        let system_time = get_timestamp(ntp_diff.load(Ordering::Relaxed));
        let start_time = system_time - config.packet_delay - Duration::from_secs(1);
        let time_set_slot_size = config.hash_slots as usize / time_window_size;

        Self {
            node_map: Some((
                AHashMap::with_capacity(config.hash_slots as usize),
                vec![HashSet::with_capacity(time_set_slot_size); time_window_size],
            )),
            id,
            state_machine_master: StateMachine::new_master(&config.flow_timeout),
            state_machine_slave: StateMachine::new_slave(&config.flow_timeout),
            service_table: ServiceTable::new(
                SERVICE_TABLE_IPV4_CAPACITY,
                SERVICE_TABLE_IPV6_CAPACITY,
                &config.server_ports,
            ),
            app_table: AppTable::new(
                config.l7_protocol_inference_max_fail_count,
                config.l7_protocol_inference_ttl,
            ),
            policy_getter,
            start_time,
            start_time_in_unit: start_time.as_secs(),
            hash_slots: config.hash_slots as usize,
            time_window_size,
            total_flow: 0,
            time_set_slot_size,
            tagged_flow_allocator: {
                let n = (config.batched_buffer_size_limit - 1) / mem::size_of::<TaggedFlow>();
                let allocator = Allocator::new(n.max(1));
                stats_collector.register_countable(
                    &AllocatorStats {
                        id,
                        obj_type: "TaggedFlow",
                    },
                    Countable::Ref(allocator.counter()),
                );
                allocator
            },
            l7_stats_allocator: {
                let n = (config.batched_buffer_size_limit - 1) / mem::size_of::<L7Stats>();
                let allocator = Allocator::new(n.max(1));
                stats_collector.register_countable(
                    &AllocatorStats {
                        id,
                        obj_type: "L7Stats",
                    },
                    Countable::Ref(allocator.counter()),
                );
                allocator
            },
            output_queue,
            out_log_queue: app_proto_log_queue,
            output_buffer: Vec::with_capacity(QUEUE_BATCH_SIZE),
            l7_stats_buffer: Vec::with_capacity(QUEUE_BATCH_SIZE),
            protolog_buffer: Vec::with_capacity(QUEUE_BATCH_SIZE),
            last_queue_flush: Duration::ZERO,
            perf_cache: Rc::new(RefCell::new(L7PerfCache::new(config.capacity as usize))),
            flow_perf_counter,
            ntp_diff,
            packet_sequence_queue, // Enterprise Edition Feature: packet-sequence
            packet_sequence_enabled,
            stats_counter,
            system_time,
            l7_protocol_checker: L7ProtocolChecker::new(
                &config.l7_protocol_enabled_bitmap,
                &config
                    .l7_protocol_parse_port_bitmap
                    .iter()
                    .filter_map(|(name, bitmap)| {
                        L7ProtocolParser::try_from(name.as_ref())
                            .ok()
                            .map(|p| (p.protocol(), bitmap.clone()))
                    })
                    .collect(),
            ),
            time_key_buffer: None,
            plugin_digest: 0, // force initial load
            wasm_vm: Default::default(),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            so_plugin: Default::default(),
            tcp_perf_pool: MemoryPool::new(config.memory_pool_size),
            flow_node_pool: MemoryPool::new(config.memory_pool_size),
            l7_stats_output_queue,
            obfuscate_cache: if config.obfuscate_enabled_protocols != L7ProtocolBitmap::default() {
                Some(Rc::new(RefCell::new(LruCache::new(
                    NonZeroUsize::new(OBFUSCATE_CACHE_SIZE).unwrap(),
                ))))
            } else {
                None
            },
            stats_collector,
            capacity: config.flow_capacity() as usize,
            size: 0,
        }
    }

    fn load_plugins(&mut self, config: &PluginConfig) {
        if self.plugin_digest == config.digest {
            return;
        }
        self.plugin_digest = config.digest;

        // although stats::Counter auto removes obsolete referenced countables
        // on counter registration and routine reports, it might be delayed because
        // FlowLog can hold these references
        if let Some(vm) = self.wasm_vm.take() {
            let counters = vm
                .counters()
                .into_iter()
                .map(|info| PluginStats {
                    id: self.id,
                    plugin_name: info.plugin_name,
                    plugin_type: info.plugin_type,
                    export_func: info.function_name,
                })
                .collect::<Vec<_>>();
            self.stats_collector
                .deregister_countables(counters.iter().map(|c| c as &dyn stats::Module));
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(ps) = self.so_plugin.take() {
            let mut counters = vec![];
            for p in ps.iter() {
                p.counters_in(&mut counters);
            }
            let counters = counters
                .into_iter()
                .map(|info| PluginStats {
                    id: self.id,
                    plugin_name: info.plugin_name,
                    plugin_type: info.plugin_type,
                    export_func: info.function_name,
                })
                .collect::<Vec<_>>();
            self.stats_collector
                .deregister_countables(counters.iter().map(|c| c as &dyn stats::Module));
        }

        debug!("reload plugins");

        let wasm_vm = if config.wasm_plugins.is_empty() {
            None
        } else {
            let vm = WasmVm::new(&config.wasm_plugins);
            if vm.is_empty() {
                None
            } else {
                for counter in vm.counters() {
                    self.stats_collector.register_countable(
                        &PluginStats {
                            id: self.id,
                            plugin_name: counter.plugin_name,
                            plugin_type: counter.plugin_type,
                            export_func: counter.function_name,
                        },
                        counter.counter,
                    );
                }
                Some(vm)
            }
        };

        #[cfg(any(target_os = "linux", target_os = "android"))]
        let so_plugins = {
            let plugins = config
                .so_plugins
                .iter()
                .filter_map(|(name, prog)| {
                    match crate::plugin::shared_obj::load_plugin(prog.as_slice(), name) {
                        Ok(p) => Some(p),
                        Err(e) => {
                            warn!("load so plugin {} fail: {}", name, e);
                            None
                        }
                    }
                })
                .collect::<Vec<SoPluginFunc>>();
            if plugins.is_empty() {
                None
            } else {
                let mut counters = vec![];
                for p in plugins.iter() {
                    p.counters_in(&mut counters);
                }
                for counter in counters {
                    self.stats_collector.register_countable(
                        &PluginStats {
                            id: self.id,
                            plugin_name: counter.plugin_name,
                            plugin_type: counter.plugin_type,
                            export_func: counter.function_name,
                        },
                        counter.counter,
                    );
                }
                Some(plugins)
            }
        };

        #[cfg(target_os = "linux")]
        log::info!(
            "loaded {} wasm and {} so plugins",
            wasm_vm.as_ref().map(|vm| vm.len()).unwrap_or_default(),
            so_plugins.as_ref().map(|so| so.len()).unwrap_or_default(),
        );
        #[cfg(target_os = "windows")]
        log::info!(
            "loaded {} wasm plugins",
            wasm_vm.as_ref().map(|vm| vm.len()).unwrap_or_default()
        );
        self.wasm_vm.replace(wasm_vm);
        #[cfg(any(target_os = "linux", target_os = "android"))]
        self.so_plugin.replace(so_plugins);

        // remove all l7_stats and l7_log_parser in existing flow nodes that is of type Custom
        if let Some((nodes, _)) = self.node_map.as_mut() {
            for node_vecs in nodes.values_mut() {
                for node in node_vecs.iter_mut() {
                    node.reset_on_plugin_reload();
                }
            }
        }
        // remove protocol cache
        self.app_table.clear();
    }

    // sort nodes by swapping timed out nodes to right
    // the result will be like:
    //     nodes: [A1, A4, A3, A2, A5, B4, B1, B2, B3]
    //     A refers to nodes after time_in_unit or not timed out
    //     B refers to nodes timed out
    // returned index will be the first timed out node in nodes
    fn sort_nodes_by_timeout(
        nodes: &mut Vec<Box<FlowNode>>,
        timestamp: Duration,
        time_in_unit: u64,
    ) -> usize {
        let mut left = 0 as isize;
        let mut right = nodes.len() as isize - 1;

        while left <= right {
            let node = &nodes[left as usize];

            // find first timeout node from left
            let timeout = node.recent_time + node.timeout;
            if node.timestamp_key > time_in_unit || timestamp < timeout {
                // not timeout
                left += 1;
                continue;
            }

            // find first not timeout node from right
            while left <= right {
                let node = &nodes[right as usize];
                let timeout = node.recent_time + node.timeout;
                if node.timestamp_key > time_in_unit || timestamp < timeout {
                    break;
                }
                right -= 1;
            }

            // swap nodes
            if left < right {
                nodes.swap(left as usize, right as usize);
                left += 1;
                right -= 1;
            }
        }
        left as usize
    }

    pub fn inject_flush_ticker(&mut self, config: &Config, mut timestamp: Duration) -> bool {
        let is_tick = timestamp.is_zero();
        if is_tick {
            timestamp = get_timestamp(self.ntp_diff.load(Ordering::Relaxed));
        } else if timestamp < self.start_time {
            self.stats_counter
                .drop_by_window
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let flow_config = &config.flow;

        // FlowMap 时间窗口无法推动
        if timestamp - flow_config.packet_delay - TIME_UNIT < self.start_time {
            return true;
        }

        self.system_time = if is_tick {
            timestamp
        } else {
            // calculate packet delay only when window will be pushed forward
            // to avoid calling `get_timestamp` for each packet
            let ts = get_timestamp(self.ntp_diff.load(Ordering::Relaxed));
            self.stats_counter.packet_delay.fetch_max(
                ts.as_nanos() as i64 - timestamp.as_nanos() as i64,
                Ordering::Relaxed,
            );
            ts
        };

        self.stats_counter.flush_delay.fetch_max(
            self.system_time.as_nanos() as i64 - self.start_time.as_nanos() as i64,
            Ordering::Relaxed,
        );
        // 根据包到达时间的容差调整
        let next_start_time_in_unit =
            ((timestamp - flow_config.packet_delay).as_nanos() / TIME_UNIT.as_nanos()) as u64;
        debug!(
            "flow_map#{} ticker flush [{:?}, {:?}) at {:?} time diff is {:?}",
            self.id,
            self.start_time,
            Duration::from_nanos(next_start_time_in_unit * TIME_UNIT.as_nanos() as u64),
            timestamp,
            self.system_time - self.start_time
        );
        self.start_time =
            Duration::from_nanos(next_start_time_in_unit * TIME_UNIT.as_nanos() as u64);
        timestamp = self.start_time - Duration::from_nanos(1);

        let Some((mut node_map, mut time_set)) = self.node_map.take() else {
            warn!("cannot get node map and time set");
            return false;
        };

        let mut moved_key = self
            .time_key_buffer
            .take()
            .unwrap_or(Vec::with_capacity(self.hash_slots / self.time_window_size));
        moved_key.clear();
        // at most self.time_windows_size slots in time_set
        for time_in_unit in self.start_time_in_unit
            ..next_start_time_in_unit.min(self.start_time_in_unit + self.time_window_size as u64)
        {
            let time_hashset = &mut time_set[time_in_unit as usize & (self.time_window_size - 1)];
            for flow_key in time_hashset.drain() {
                let Some(nodes) = node_map.get_mut(&flow_key) else {
                    continue;
                };
                self.stats_counter
                    .total_scan
                    .fetch_add(nodes.len() as u64, Ordering::Relaxed);
                self.stats_counter
                    .slot_max_depth
                    .fetch_max(nodes.len() as u64, Ordering::Relaxed);

                // nodes are partitioned by timed out or not
                // nodes not in this time_set or not timed out are moved to front
                // timed out nodes are move to back
                // the returned index refers to the first node that is timed out
                let index = Self::sort_nodes_by_timeout(nodes, timestamp, time_in_unit);
                // handle and remove timeout nodes
                for node in nodes.drain(index..) {
                    let timeout = node.recent_time + node.timeout;
                    // Only node whose SignalSource is EBPF needs to send close event when it is timeout.
                    if node.tagged_flow.flow.signal_source == SignalSource::EBPF {
                        self.send_socket_close_event(&node);
                    }
                    self.node_removed_aftercare(&config, node, timeout.into(), None);
                }
                if nodes.is_empty() {
                    node_map.remove(&flow_key);
                    continue;
                }

                // handle rest nodes
                for node in nodes.iter_mut() {
                    if node.timestamp_key > time_in_unit {
                        continue;
                    }
                    // 未超时Flow的统计信息发送到队列下游
                    self.node_updated_aftercare(&flow_config, node, timestamp, None);
                    // Enterprise Edition Feature: packet-sequence
                    if self.packet_sequence_enabled {
                        if let Some(block) = node.packet_sequence_block.take() {
                            // flush the packet_sequence_block at the regular time
                            if let Err(e) = self.packet_sequence_queue.as_ref().unwrap().send(block)
                            {
                                warn!("packet sequence block to queue failed, because {:?}", e);
                            }
                        }
                    }

                    // 若流统计信息已输出，将节点移动至最终超时的时间
                    let timeout = node.recent_time + node.timeout;
                    if node.timestamp_key != timeout.as_secs() {
                        node.timestamp_key = timeout.as_secs();
                        moved_key.push((node.timestamp_key, flow_key));
                    }
                }
            }
            if time_hashset.capacity() > 2 * self.time_set_slot_size {
                self.stats_counter
                    .time_set_shrinks
                    .fetch_add(1, Ordering::Relaxed);
                time_hashset.shrink_to(self.time_set_slot_size);
            }
            for key in moved_key.drain(..) {
                time_set[key.0 as usize & (self.time_window_size - 1)].insert(key.1);
            }
        }
        Self::update_stats_counter(&self.stats_counter, node_map.len() as u64, 0);

        self.time_key_buffer.replace(moved_key);
        self.node_map.replace((node_map, time_set));

        self.start_time_in_unit = next_start_time_in_unit;
        self.flush_queue(&flow_config, timestamp);

        self.flush_app_protolog();

        true
    }

    fn lookup_without_flow(
        &mut self,
        #[allow(unused)] config: &Config,
        meta_packet: &mut MetaPacket,
    ) {
        // 补充由于超时导致未查询策略，用于其它流程（如PCAP存储）
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let local_epc_id = match config.ebpf.as_ref() {
            Some(c) => c.epc_id as i32,
            _ => 0,
        };
        #[cfg(target_os = "windows")]
        let local_epc_id = 0;
        (self.policy_getter).lookup(meta_packet, self.id as usize, local_epc_id);
    }

    pub fn inject_meta_packet(&mut self, config: &Config, meta_packet: &mut MetaPacket) {
        if !self.inject_flush_ticker(config, meta_packet.lookup_key.timestamp.into()) {
            self.lookup_without_flow(config, meta_packet);
            return;
        }

        let flow_config = &config.flow;

        self.load_plugins(&flow_config.plugins);

        let pkt_key = FlowMapKey::new(&meta_packet.lookup_key, meta_packet.tap_port);

        let Some((mut node_map, mut time_set)) = self.node_map.take() else {
            warn!("cannot get node map and time set");
            return;
        };

        let pkt_timestamp = meta_packet.lookup_key.timestamp;
        let mut max_depth = 1;
        match node_map.get_mut(&pkt_key) {
            // 找到一组可能的 FlowNode
            Some(nodes) => {
                max_depth = nodes.len();
                let ignore_l2_end = flow_config.ignore_l2_end;
                let ignore_tor_mac = flow_config.ignore_tor_mac;
                let ignore_idc_vlan = flow_config.ignore_idc_vlan;
                let agent_type = flow_config.agent_type;
                let index = nodes.iter().position(|node| {
                    node.match_node(
                        meta_packet,
                        ignore_l2_end,
                        ignore_tor_mac,
                        ignore_idc_vlan,
                        agent_type,
                    )
                });
                let Some(index) = index else {
                    // If no exact match of FlowNode is found and close event is received, there is no need to create a new FlowNode
                    if meta_packet.ebpf_type == EbpfType::SocketCloseEvent {
                        self.node_map.replace((node_map, time_set));
                        return;
                    }
                    // No exact match of FlowNode was found, insert new Node
                    let node = self.new_flow_node(config, meta_packet);
                    if let Some(node) = node {
                        time_set[node.timestamp_key as usize & (self.time_window_size - 1)]
                            .insert(pkt_key);
                        nodes.push(node);
                        max_depth += 1;
                    }
                    Self::update_stats_counter(
                        &self.stats_counter,
                        node_map.len() as u64,
                        max_depth as u64,
                    );
                    self.stats_counter
                        .total_scan
                        .fetch_add(max_depth as u64, Ordering::Relaxed);
                    self.node_map.replace((node_map, time_set));
                    return;
                };
                self.stats_counter
                    .total_scan
                    .fetch_add(1 + index as u64, Ordering::Relaxed);

                let node = &mut nodes[index];
                // 1. 输出上一个统计周期的统计信息
                self.node_updated_aftercare(
                    &flow_config,
                    node,
                    pkt_timestamp.into(),
                    Some(meta_packet),
                );

                let flow = &node.tagged_flow.flow;
                // PCAP and L7 Log
                meta_packet.flow_id = flow.flow_id;
                // For PCAP
                meta_packet.second_in_minute =
                    (flow.start_time.as_secs() % SECONDS_IN_MINUTE) as u8;
                // 2. 更新Flow状态，判断是否已结束
                // 设置timestamp_key为流的相同，time_set根据key来删除
                let flow_closed = match meta_packet.lookup_key.proto {
                    IpProtocol::TCP => self.update_tcp_node(config, node, meta_packet),
                    IpProtocol::UDP => self.update_udp_node(config, node, meta_packet),
                    _ => self.update_other_node(config, node, meta_packet),
                };

                if flow_closed {
                    let node = nodes.swap_remove(index);
                    self.send_socket_close_event(&node);
                    self.node_removed_aftercare(
                        &config,
                        node,
                        meta_packet.lookup_key.timestamp.into(),
                        Some(meta_packet),
                    );

                    if nodes.is_empty() {
                        node_map.remove(&pkt_key);
                    }
                } else {
                    if node.timestamp_key != pkt_timestamp.as_secs() {
                        // Because pkt_key is shared by multiple nodes, we have no low-cost way to
                        // delete it from time_set. In fact, we can invalidate the flow_key in the
                        // old slot in time_set by updating node.timestamp_key.
                        //
                        // Therefore, it is only necessary to insert the node into the new slot of
                        // time_set, so that the statistical data can be output in time. However,
                        // the following operations are only required when the slot needs to be
                        // changed.
                        node.timestamp_key = pkt_timestamp.as_secs();
                        time_set[node.timestamp_key as usize & (self.time_window_size - 1)]
                            .insert(pkt_key);
                    }
                }
            }
            // No exact match of FlowNode was found, insert new Node
            None => {
                if meta_packet.ebpf_type == EbpfType::SocketCloseEvent {
                    self.node_map.replace((node_map, time_set));
                    return;
                }
                let node = self.new_flow_node(config, meta_packet);
                if let Some(node) = node {
                    time_set[node.timestamp_key as usize & (self.time_window_size - 1)]
                        .insert(pkt_key);
                    node_map.insert(pkt_key, vec![node]);
                }
            }
        }
        Self::update_stats_counter(&self.stats_counter, node_map.len() as u64, max_depth as u64);
        self.node_map.replace((node_map, time_set));
        // go实现只有插入node的时候，插入的节点数目大于ring buffer 的capacity 才会执行policy_getter,
        // rust 版本用了std的hashmap自动处理扩容，所以无需执行policy_gettelr
    }

    fn append_to_block(&self, config: &FlowConfig, node: &mut FlowNode, meta_packet: &MetaPacket) {
        const MINUTE: u64 = 60;
        let packet_sequence_start_time = node.tagged_flow.flow.start_time_in_minute();
        if node.packet_sequence_block.is_some() {
            if !node.packet_sequence_block.as_ref().unwrap().is_available(
                config.packet_sequence_block_size,
                packet_sequence_start_time as u32,
            ) {
                // if the packet_sequence_block is no enough to push one more packet, then send it to the queue
                if let Err(e) = self
                    .packet_sequence_queue
                    .as_ref()
                    .unwrap()
                    .send(node.packet_sequence_block.take().unwrap())
                {
                    warn!("packet sequence block to queue failed, because {:?}", e);
                }

                node.packet_sequence_block = Some(Box::new(PacketSequenceBlock::new(
                    packet_sequence_start_time as u32,
                )));
            }
        } else {
            node.packet_sequence_block = Some(Box::new(PacketSequenceBlock::new(
                packet_sequence_start_time as u32,
            )));
        }

        if let ProtocolData::TcpHeader(tcp_data) = &meta_packet.protocol_data {
            let mini_meta_packet = packet_sequence_block::MiniMetaPacket::new(
                node.tagged_flow.flow.flow_id,
                meta_packet.lookup_key.direction as u8,
                meta_packet.lookup_key.timestamp.into(),
                meta_packet.payload_len,
                tcp_data.seq,
                tcp_data.ack,
                tcp_data.win_size,
                tcp_data.mss,
                tcp_data.flags.bits(),
                tcp_data.win_scale,
                tcp_data.sack_permitted,
                &tcp_data.sack,
            );
            node.packet_sequence_block
                .as_mut()
                .unwrap()
                .append_packet(mini_meta_packet, config.packet_sequence_flag);
        }
    }

    fn update_tcp_node(
        &mut self,
        config: &Config,
        node: &mut FlowNode,
        meta_packet: &mut MetaPacket,
    ) -> bool {
        node.last_cap_seq = meta_packet.cap_end_seq as u32;
        // For short connections, in order to quickly get the node flush of the closed socket out, set a short timeout
        // FIXME: At present, the purpose of flush is to set the timeout, and different node type may be set in the future.
        if meta_packet.ebpf_type == EbpfType::SocketCloseEvent {
            node.timeout = DEFAULT_SOCKET_CLOSE_TIMEOUT;
            return false;
        }
        let flow_config = config.flow;
        let flow_closed = self.update_tcp_flow(config, meta_packet, node);

        if flow_config.collector_enabled {
            if let Some(tcp_segments) = node.tcp_segments.as_mut() {
                if let Some(mut packets) = tcp_segments.inject(meta_packet.to_owned_segment()) {
                    let mut packets = packets
                        .drain(..)
                        .map(|x| x.into_any().downcast::<MetaPacket>().unwrap())
                        .collect::<Vec<Box<MetaPacket>>>();
                    for packet in &mut packets {
                        let direction =
                            packet.lookup_key.direction == PacketDirection::ClientToServer;
                        self.collect_metric(config, node, packet, direction, false);
                    }
                }
            } else {
                let direction = meta_packet.lookup_key.direction == PacketDirection::ClientToServer;
                self.collect_metric(config, node, meta_packet, direction, false);
            }
        }

        // After collect_metric() is called for eBPF MetaPacket, its direction is determined.
        if node.tagged_flow.flow.signal_source == SignalSource::EBPF {
            if meta_packet.lookup_key.direction == PacketDirection::ClientToServer {
                node.residual_request += 1;
            } else {
                node.residual_request -= 1;
            }
            // For eBPF data, timeout as soon as possible when there are no unaggregated requests.
            // Considering that eBPF data may be out of order, wait for an additional 5s(default) timeout.
            if node.residual_request == 0 {
                node.timeout = flow_config.flow_timeout.opening;
            } else {
                node.timeout = config.log_parser.l7_log_session_aggr_timeout.into();
            }
        }

        // Enterprise Edition Feature: packet-sequence
        if self.packet_sequence_enabled {
            self.append_to_block(flow_config, node, meta_packet);
        }

        flow_closed
    }

    fn update_udp_node(
        &mut self,
        config: &Config,
        node: &mut FlowNode,
        meta_packet: &mut MetaPacket,
    ) -> bool {
        let flow_config = config.flow;
        self.update_flow(config, node, meta_packet);
        let peers = &node.tagged_flow.flow.flow_metrics_peers;
        if peers[FLOW_METRICS_PEER_SRC].packet_count > 0
            && peers[FLOW_METRICS_PEER_DST].packet_count > 0
        {
            // For udp, eBPF and Packet data use the same timeout
            node.timeout = flow_config.flow_timeout.closing;
        }
        meta_packet.is_active_service = node.tagged_flow.flow.is_active_service;
        if flow_config.collector_enabled {
            self.collect_metric(
                config,
                node,
                meta_packet,
                meta_packet.lookup_key.direction == PacketDirection::ClientToServer,
                false,
            );
        }

        if node.tagged_flow.flow.signal_source == SignalSource::EBPF {
            self.update_udp_is_active(node, meta_packet.lookup_key.direction);
        }

        if meta_packet.need_reverse_flow {
            self.update_l4_direction(meta_packet, node, false);
        }

        false
    }

    fn update_other_node(
        &mut self,
        config: &Config,
        node: &mut FlowNode,
        meta_packet: &mut MetaPacket,
    ) -> bool {
        self.update_flow(config, node, meta_packet);
        let peers = &node.tagged_flow.flow.flow_metrics_peers;
        if peers[FLOW_METRICS_PEER_SRC].packet_count > 0
            && peers[FLOW_METRICS_PEER_DST].packet_count > 0
        {
            node.timeout = config.flow.flow_timeout.established_rst;
        }
        if config.flow.collector_enabled {
            self.collect_metric(config, node, meta_packet, true, true);
        }
        false
    }

    fn generate_flow_id(&mut self, timestamp: Timestamp, thread_id: u32) -> u64 {
        self.total_flow += 1;
        (timestamp.as_nanos() as u64 >> 30 & TIMER_FLOW_ID_MASK) << 32
            | (thread_id as u64 & THREAD_FLOW_ID_MASK) << 24
            | self.total_flow as u64 & COUNTER_FLOW_ID_MASK
    }

    fn update_tcp_flow(
        &mut self,
        config: &Config,
        meta_packet: &mut MetaPacket,
        node: &mut FlowNode,
    ) -> bool {
        let flow_config = config.flow;
        let direction = meta_packet.lookup_key.direction;
        let pkt_tcp_flags = if let ProtocolData::TcpHeader(tcp_data) = &meta_packet.protocol_data {
            tcp_data.flags
        } else {
            unreachable!();
        };
        node.tagged_flow.flow.flow_metrics_peers[direction as usize].tcp_flags |= pkt_tcp_flags;
        node.tagged_flow.flow.flow_metrics_peers[direction as usize].total_tcp_flags |=
            pkt_tcp_flags;
        self.update_flow(config, node, meta_packet);
        if node.tagged_flow.flow.signal_source == SignalSource::EBPF {
            // Because eBPF data does not have L4 information, the remaining steps of direction
            // correction, state machine maintenance, SEQ acquisition, etc., do not need to be
            // performed.
            return false;
        }

        // 有特殊包时更新ServiceTable并矫正流方向：SYN+ACK、SYN或need_reverse_flow为true
        if pkt_tcp_flags.bits() & TcpFlags::SYN.bits() != 0 || meta_packet.need_reverse_flow {
            self.update_l4_direction(meta_packet, node, false);
            self.update_syn_or_syn_ack_seq(node, meta_packet);
        }

        self.update_tcp_keepalive_seq(node, meta_packet);
        meta_packet.is_active_service = node.tagged_flow.flow.is_active_service;

        if pkt_tcp_flags.is_invalid() {
            // exception timeout
            node.timeout = flow_config.flow_timeout.exception;
            node.flow_state = FlowState::Exception;
            return false;
        }

        self.update_flow_state_machine(flow_config, node, pkt_tcp_flags, direction)
    }

    // 协议参考：https://datatracker.ietf.org/doc/html/rfc1122#section-4.2.3.6
    // TCP Keepalive报文特征：
    //      1.payloadLen为0/1
    //      2.非FIN、SYN、RST
    //      3.TCP保活探测报文序列号(Seq)为前一个TCP报文序列号(Seq)减一
    fn update_tcp_keepalive_seq(&mut self, node: &mut FlowNode, meta_packet: &MetaPacket) {
        // 保存TCP Seq用于TCP Keepalive报文判断

        let (next_tcp_seq0, next_tcp_seq1) = (node.next_tcp_seq0, node.next_tcp_seq1);

        let tcp_data = if let ProtocolData::TcpHeader(tcp_data) = &meta_packet.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        // 记录下一次TCP Seq
        match meta_packet.lookup_key.direction {
            PacketDirection::ClientToServer => node.next_tcp_seq1 = tcp_data.ack,
            PacketDirection::ServerToClient => node.next_tcp_seq0 = tcp_data.ack,
        }
        // TCP Keepalive报文判断，并记录其TCP Seq
        if meta_packet.payload_len > 1 {
            return;
        }

        if tcp_data.flags & (TcpFlags::SYN | TcpFlags::FIN | TcpFlags::RST) != TcpFlags::empty() {
            return;
        }

        if meta_packet.lookup_key.direction == PacketDirection::ClientToServer
            && tcp_data.seq.wrapping_add(1) == next_tcp_seq0
            || meta_packet.lookup_key.direction == PacketDirection::ServerToClient
                && tcp_data.seq.wrapping_add(1) == next_tcp_seq1
        {
            let flow = &mut node.tagged_flow.flow;
            flow.last_keepalive_seq = tcp_data.seq;
            flow.last_keepalive_ack = tcp_data.ack;
        }
    }

    fn update_syn_or_syn_ack_seq(&mut self, node: &mut FlowNode, meta_packet: &mut MetaPacket) {
        let tcp_data = if let ProtocolData::TcpHeader(tcp_data) = &meta_packet.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        let flow = &mut node.tagged_flow.flow;
        if tcp_data.flags == TcpFlags::SYN {
            flow.syn_seq = tcp_data.seq;
        } else if tcp_data.flags == TcpFlags::SYN_ACK && meta_packet.payload_len == 0 {
            flow.synack_seq = tcp_data.seq;
        }
    }

    //flow_state.rs 有个state_machine的测试用到该方法, 要用super
    pub(super) fn update_flow_state_machine(
        &mut self,
        config: &FlowConfig,
        node: &mut FlowNode,
        flags: TcpFlags,
        direction: PacketDirection,
    ) -> bool {
        let &StateValue {
            mut timeout,
            mut state,
            mut closed,
        } = self
            .state_machine_master
            .get(node.flow_state, flags)
            .unwrap_or(&StateValue::new(
                // exception timeout,
                config.flow_timeout.exception,
                FlowState::Exception,
                false,
            ));

        if direction == PacketDirection::ServerToClient {
            if let Some(v) = self.state_machine_slave.get(node.flow_state, flags) {
                timeout = v.timeout;
                state = v.state;
                closed = v.closed;
            }
        }

        node.flow_state = state;
        let flow = &node.tagged_flow.flow;
        let peer_src = &flow.flow_metrics_peers[FLOW_METRICS_PEER_SRC];
        let peer_dst = &flow.flow_metrics_peers[FLOW_METRICS_PEER_DST];
        if peer_src.total_packet_count == 0 || peer_dst.total_packet_count == 0 {
            //single direction timeout
            node.timeout = config.flow_timeout.single_direction;
        } else {
            node.timeout = timeout;
        }

        closed
    }

    fn l7_metrics_enabled(config: &FlowConfig) -> bool {
        config.l7_metrics_enabled
    }

    fn l7_log_parse_enabled(config: &FlowConfig, lookup_key: &LookupKey) -> bool {
        // parse tap_type any or tap_type in config
        config.app_proto_log_enabled
            && (lookup_key.proto == IpProtocol::TCP || lookup_key.proto == IpProtocol::UDP)
            && (config.l7_log_tap_types[u16::from(CaptureNetworkType::Any) as usize]
                || lookup_key.tap_type <= CaptureNetworkType::Max
                    && config.l7_log_tap_types[u16::from(lookup_key.tap_type) as usize])
    }

    fn l4_metrics_enabled(config: &FlowConfig) -> bool {
        config.l4_performance_enabled
    }

    fn init_nat_info(flow: &mut Flow, meta_packet: &MetaPacket) {
        if meta_packet.lookup_key.src_nat_source != TapPort::NAT_SOURCE_NONE {
            flow.flow_metrics_peers[0].nat_source = meta_packet.lookup_key.src_nat_source;
            flow.flow_metrics_peers[0].nat_real_ip = meta_packet.lookup_key.src_nat_ip;
            flow.flow_metrics_peers[0].nat_real_port = meta_packet.lookup_key.src_nat_port;
        } else {
            flow.flow_metrics_peers[0].nat_source = TapPort::NAT_SOURCE_NONE;
            flow.flow_metrics_peers[0].nat_real_ip = flow.flow_key.ip_src;
            flow.flow_metrics_peers[0].nat_real_port = flow.flow_key.port_src;
        }
        if meta_packet.lookup_key.dst_nat_source != TapPort::NAT_SOURCE_NONE {
            flow.flow_metrics_peers[1].nat_source = meta_packet.lookup_key.dst_nat_source;
            flow.flow_metrics_peers[1].nat_real_ip = meta_packet.lookup_key.dst_nat_ip;
            flow.flow_metrics_peers[1].nat_real_port = meta_packet.lookup_key.dst_nat_port;
        } else {
            flow.flow_metrics_peers[1].nat_source = TapPort::NAT_SOURCE_NONE;
            flow.flow_metrics_peers[1].nat_real_ip = flow.flow_key.ip_dst;
            flow.flow_metrics_peers[1].nat_real_port = flow.flow_key.port_dst;
        }
    }

    fn init_flow(&mut self, config: &Config, meta_packet: &mut MetaPacket) -> Box<FlowNode> {
        let flow_config = config.flow;

        let mut tagged_flow = TaggedFlow::default();
        let lookup_key = &meta_packet.lookup_key;
        let is_active_service = if meta_packet.signal_source == SignalSource::EBPF {
            match lookup_key.proto {
                IpProtocol::TCP => true, // Tcp data coming from eBPF means it must be an active service
                _ => false,
            }
        } else {
            false
        };
        let flags = if let ProtocolData::TcpHeader(tcp_data) = &meta_packet.protocol_data {
            tcp_data.flags
        } else {
            TcpFlags::default()
        };
        let flow = Flow {
            flow_key: FlowKey {
                agent_id: flow_config.agent_id,
                mac_src: lookup_key.src_mac,
                mac_dst: lookup_key.dst_mac,
                ip_src: lookup_key.src_ip,
                ip_dst: lookup_key.dst_ip,
                port_src: lookup_key.src_port,
                port_dst: lookup_key.dst_port,
                proto: lookup_key.proto,
                tap_type: lookup_key.tap_type,
                tap_port: meta_packet.tap_port,
            },
            tunnel: if let Some(tunnel) = meta_packet.tunnel {
                TunnelField {
                    tx_ip0: tunnel.src,
                    tx_ip1: tunnel.dst,
                    tx_mac0: tunnel.mac_src,
                    tx_mac1: tunnel.mac_dst,
                    tx_id: tunnel.id,
                    tier: tunnel.tier,
                    tunnel_type: tunnel.tunnel_type,
                    is_ipv6: tunnel.is_ipv6,
                    ..Default::default()
                }
            } else {
                TunnelField::default()
            },
            flow_id: if meta_packet.signal_source == SignalSource::EBPF {
                meta_packet.generate_ebpf_flow_id()
            } else {
                self.generate_flow_id(lookup_key.timestamp, self.id)
            },
            start_time: lookup_key.timestamp.into(),
            flow_stat_time: lookup_key.timestamp.round_to(TIME_UNIT.into()),
            vlan: meta_packet.vlan,
            eth_type: lookup_key.eth_type,
            queue_hash: meta_packet.queue_hash,
            is_new_flow: true,
            // 统计量
            flow_metrics_peers: [
                FlowMetricsPeer {
                    total_packet_count: 1,
                    packet_count: 1,
                    total_byte_count: meta_packet.packet_len as u64,
                    byte_count: meta_packet.packet_len as u64,
                    l3_byte_count: meta_packet.l3_payload_len() as u64,
                    l4_byte_count: meta_packet.l4_payload_len() as u64,
                    first: lookup_key.timestamp.into(),
                    last: lookup_key.timestamp.into(),
                    tcp_flags: flags,
                    total_tcp_flags: flags,
                    ..Default::default()
                },
                FlowMetricsPeer::default(),
            ],
            signal_source: meta_packet.signal_source,
            is_active_service,
            ..Default::default()
        };
        tagged_flow.flow = flow;

        // FlowMap信息
        let mut policy_in_tick = [false; 2];
        policy_in_tick[meta_packet.lookup_key.direction as usize] = true;

        let mut node = self
            .flow_node_pool
            .get()
            .unwrap_or_else(|| Default::default());
        node.timestamp_key = lookup_key.timestamp.as_secs();
        node.tagged_flow = tagged_flow;
        node.min_arrived_time = lookup_key.timestamp;
        node.recent_time = lookup_key.timestamp;
        node.timeout = Timestamp::ZERO;
        node.packet_in_tick = true;
        node.policy_in_tick = policy_in_tick;
        node.flow_state = FlowState::Raw;
        node.meta_flow_log = None;
        node.next_tcp_seq0 = 0;
        node.next_tcp_seq1 = 0;
        node.last_cap_seq = meta_packet.cap_end_seq as u32;
        node.policy_data_cache = Default::default();
        node.endpoint_data_cache = Default::default();
        node.packet_sequence_block = None; // Enterprise Edition Feature: packet-sequence
        node.residual_request = 0;

        if PacketSegmentationReassembly::does_support()
            && meta_packet.lookup_key.proto == IpProtocol::TCP
            && meta_packet.ebpf_type == EbpfType::None
            && config
                .flow
                .need_to_reassemble(lookup_key.src_port, lookup_key.dst_port)
        {
            node.tcp_segments = Some(PacketSegmentationReassembly::default())
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        let local_epc_id = match config.ebpf.as_ref() {
            Some(c) => c.epc_id as i32,
            _ => 0,
        };
        #[cfg(target_os = "windows")]
        let local_epc_id = 0;

        // tag
        (self.policy_getter).lookup(meta_packet, self.id as usize, local_epc_id);
        self.update_endpoint_and_policy_data(&mut node, meta_packet);
        node.tagged_flow.flow.need_to_store = (self.packet_sequence_enabled
            && meta_packet.lookup_key.proto == IpProtocol::TCP)
            || node.contain_pcap_policy();
        // Currently, only virtual traffic's tap_side is counted
        node.tagged_flow
            .flow
            .set_tap_side(flow_config.agent_type, flow_config.cloud_gateway_traffic);

        Self::init_nat_info(&mut node.tagged_flow.flow, meta_packet);

        node.tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_SRC].gpid = meta_packet.gpid_0;
        node.tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_DST].gpid = meta_packet.gpid_1;

        let nat_source = meta_packet.lookup_key.get_nat_source();
        meta_packet.tap_port.set_nat_source(nat_source);
        node.tagged_flow
            .flow
            .flow_key
            .tap_port
            .set_nat_source(nat_source);

        // direction rectify
        if meta_packet.signal_source == SignalSource::EBPF {
            let (src_l3_epc_id, dst_l3_epc_id) = if let Some(ep) = node.endpoint_data_cache.as_ref()
            {
                (
                    ep.src_info().l3_epc_id as i16,
                    ep.dst_info().l3_epc_id as i16,
                )
            } else {
                (0, 0)
            };
            let flow_src_key = ServiceKey::new(
                meta_packet.lookup_key.src_ip,
                src_l3_epc_id,
                meta_packet.lookup_key.src_port,
            );
            let flow_dst_key = ServiceKey::new(
                meta_packet.lookup_key.dst_ip,
                dst_l3_epc_id,
                meta_packet.lookup_key.dst_port,
            );
            let (direction_score, need_reverse) = self.service_table.get_ebpf_tcp_score(
                meta_packet.socket_role,
                meta_packet.lookup_key.l2_end_0,
                meta_packet.lookup_key.l2_end_1,
                flow_src_key,
                flow_dst_key,
            );
            if need_reverse {
                node.tagged_flow.flow.reverse(true);
                if let Some(tcp_segments) = node.tcp_segments.as_mut() {
                    tcp_segments.reverse()
                }
            }
            node.tagged_flow.flow.direction_score = direction_score;
        } else if let ProtocolData::IcmpData(icmp_data) = &meta_packet.protocol_data {
            if icmp_data.icmp_type == IcmpTypes::EchoReply.0 {
                node.tagged_flow.flow.reverse(true);
            }
        }

        /*
            ebpf will pass the server port to FlowPerf use for adjuest packet direction.
            non ebpf not need this field, FlowPerf::server_port always 0.
        */
        let (l7_proto_enum, port, is_skip, l7_failed_count, last) = match meta_packet.signal_source
        {
            SignalSource::EBPF => {
                let (local_epc, remote_epc) = if meta_packet.lookup_key.l2_end_0 {
                    (local_epc_id, 0)
                } else {
                    (0, local_epc_id)
                };
                /*
                    For eBPF, `server_port` is determined in the following order:
                    1. Look it up in the `app_table` if the protocol parsed before
                    2. Try to use eBPF `socket_role` and `direction` to determine the `server_port`
                    3. If all above failed, use the first packet's `dst_port` as `server_port`
                */
                self.app_table
                    .get_protocol_from_ebpf(meta_packet, local_epc, remote_epc)
                    .map(|(proto, port, fail_count, last)| {
                        let is_skip = proto.get_l7_protocol() == L7Protocol::Unknown;
                        (proto, port, is_skip, fail_count, is_skip.then_some(last))
                    })
                    .unwrap_or_else(|| {
                        let server_port = match (
                            meta_packet.socket_role,
                            meta_packet.lookup_key.l2_end_0,
                            meta_packet.lookup_key.l2_end_1,
                        ) {
                            (1, true, false) => meta_packet.lookup_key.dst_port,
                            (1, false, true) => meta_packet.lookup_key.src_port,
                            (2, false, true) => meta_packet.lookup_key.dst_port,
                            (2, true, false) => meta_packet.lookup_key.src_port,
                            _ => 0,
                        };
                        (L7ProtocolEnum::default(), server_port, false, 0, None)
                    })
            }
            _ => self
                .app_table
                .get_protocol(meta_packet)
                .map(|(proto, fail_count, last)| {
                    let is_skip = proto.get_l7_protocol() == L7Protocol::Unknown;
                    (proto, 0u16, is_skip, fail_count, is_skip.then_some(last))
                })
                .unwrap_or((L7ProtocolEnum::default(), 0, false, 0, None)),
        };

        let l4_enabled = node.tagged_flow.flow.signal_source == SignalSource::Packet
            && Self::l4_metrics_enabled(flow_config);
        let l7_enabled = Self::l7_metrics_enabled(flow_config)
            || Self::l7_log_parse_enabled(flow_config, &meta_packet.lookup_key);
        if l4_enabled || l7_enabled {
            node.tagged_flow.flow.flow_perf_stats = Some(FlowPerfStats {
                l7_failed_count,
                ..Default::default()
            });
        }

        if flow_config.collector_enabled {
            node.meta_flow_log = FlowLog::new(
                l4_enabled,
                &mut self.tcp_perf_pool,
                l7_enabled,
                self.perf_cache.clone(),
                L4Protocol::from(meta_packet.lookup_key.proto),
                l7_proto_enum,
                is_skip,
                self.flow_perf_counter.clone(),
                port,
                Rc::clone(&self.wasm_vm),
                #[cfg(any(target_os = "linux", target_os = "android"))]
                Rc::clone(&self.so_plugin),
                self.stats_counter.clone(),
                match meta_packet.lookup_key.proto {
                    IpProtocol::TCP => flow_config.rrt_tcp_timeout,
                    IpProtocol::UDP => flow_config.rrt_udp_timeout,
                    _ => flow_config.rrt_udp_timeout,
                },
                flow_config.l7_protocol_inference_ttl as u64,
                last,
                self.ntp_diff.clone(),
                self.obfuscate_cache.as_ref().map(|o| o.clone()),
            )
            .map(|o| Box::new(o));
        }
        node
    }

    // suppress warning under windows
    #[allow(unused_variables)]
    fn update_flow(&mut self, config: &Config, node: &mut FlowNode, meta_packet: &mut MetaPacket) {
        let pkt_timestamp = meta_packet.lookup_key.timestamp;
        let flow = &mut node.tagged_flow.flow;
        if pkt_timestamp > node.recent_time {
            node.recent_time = pkt_timestamp;
            flow.duration = (node.recent_time - node.min_arrived_time).into();
            // Duration仅使用包的时间计算，不包括超时时间
        }

        if !node.packet_in_tick {
            // FlowStatTime取整至统计时间的开始，只需要赋值一次，且使用包的时间戳
            node.packet_in_tick = true;
            flow.flow_stat_time = pkt_timestamp.round_to(STATISTICAL_INTERVAL.into());
        }

        if !node.policy_in_tick[meta_packet.lookup_key.direction as usize] {
            node.policy_in_tick[meta_packet.lookup_key.direction as usize] = true;
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let local_epc_id = match config.ebpf.as_ref() {
                Some(c) => c.epc_id as i32,
                _ => 0,
            };
            #[cfg(target_os = "windows")]
            let local_epc_id = 0;
            if node.tagged_flow.flow.flow_key.tap_port.get_nat_source() == TapPort::NAT_SOURCE_TOA
                && meta_packet.lookup_key.direction == PacketDirection::ClientToServer
                && meta_packet.lookup_key.src_nat_port == 0
            {
                let metric = &node.tagged_flow.flow.flow_metrics_peers
                    [PacketDirection::ClientToServer as usize];
                meta_packet.lookup_key.src_nat_ip = metric.nat_real_ip;
                meta_packet.lookup_key.src_nat_port = metric.nat_real_port;
                meta_packet.lookup_key.src_nat_source = TapPort::NAT_SOURCE_TOA;
            }
            (self.policy_getter).lookup(meta_packet, self.id as usize, local_epc_id);
            self.update_endpoint_and_policy_data(node, meta_packet);
            // Currently, only virtual traffic's tap_side is counted
            node.tagged_flow
                .flow
                .set_tap_side(config.flow.agent_type, config.flow.cloud_gateway_traffic);
            node.tagged_flow.flow.need_to_store = (self.packet_sequence_enabled
                && meta_packet.lookup_key.proto == IpProtocol::TCP)
                || node.contain_pcap_policy();
        } else {
            // copy endpoint and policy data
            meta_packet.policy_data =
                node.policy_data_cache[meta_packet.lookup_key.direction as usize].clone();
            match meta_packet.lookup_key.direction {
                PacketDirection::ClientToServer => {
                    meta_packet.endpoint_data = node.endpoint_data_cache.clone();
                }
                PacketDirection::ServerToClient => {
                    meta_packet.endpoint_data =
                        node.endpoint_data_cache.as_ref().map(|ep| ep.reversed());
                }
            }
            if let Some(endpoint_data) = meta_packet.endpoint_data.as_ref() {
                meta_packet.lookup_key.l3_end_0 = endpoint_data.src_info().l3_end;
                meta_packet.lookup_key.l3_end_1 = endpoint_data.dst_info().l3_end;
            }
        }

        let flow = &mut node.tagged_flow.flow;

        if meta_packet.gpid_0 > 0 {
            flow.flow_metrics_peers[meta_packet.lookup_key.direction as usize].gpid =
                meta_packet.gpid_0;
        }
        if meta_packet.gpid_1 > 0 {
            flow.flow_metrics_peers[meta_packet.lookup_key.direction.reversed() as usize].gpid =
                meta_packet.gpid_1;
        }
        if meta_packet.lookup_key.src_nat_source != TapPort::NAT_SOURCE_NONE
            && meta_packet.lookup_key.src_nat_source
                >= flow.flow_metrics_peers[meta_packet.lookup_key.direction as usize].nat_source
        {
            flow.flow_metrics_peers[meta_packet.lookup_key.direction as usize].nat_source =
                meta_packet.lookup_key.src_nat_source;
            flow.flow_metrics_peers[meta_packet.lookup_key.direction as usize].nat_real_ip =
                meta_packet.lookup_key.src_nat_ip;
            flow.flow_metrics_peers[meta_packet.lookup_key.direction as usize].nat_real_port =
                meta_packet.lookup_key.src_nat_port;
        }
        if meta_packet.lookup_key.dst_nat_source != TapPort::NAT_SOURCE_NONE
            && meta_packet.lookup_key.dst_nat_source
                >= flow.flow_metrics_peers[meta_packet.lookup_key.direction.reversed() as usize]
                    .nat_source
        {
            flow.flow_metrics_peers[meta_packet.lookup_key.direction.reversed() as usize]
                .nat_source = meta_packet.lookup_key.dst_nat_source;
            flow.flow_metrics_peers[meta_packet.lookup_key.direction.reversed() as usize]
                .nat_real_ip = meta_packet.lookup_key.dst_nat_ip;
            flow.flow_metrics_peers[meta_packet.lookup_key.direction.reversed() as usize]
                .nat_real_port = meta_packet.lookup_key.dst_nat_port;
        }

        let nat_source = meta_packet.lookup_key.get_nat_source();
        meta_packet.tap_port.set_nat_source(nat_source);
        if nat_source > flow.flow_key.tap_port.get_nat_source() {
            flow.flow_key.tap_port.set_nat_source(nat_source);
        }

        // The ebpf data has no l3 and l4 information, so it can be returned directly
        if flow.signal_source == SignalSource::EBPF {
            return;
        }

        let flow_metrics_peer =
            &mut flow.flow_metrics_peers[meta_packet.lookup_key.direction as usize];
        flow_metrics_peer.packet_count += 1;
        flow_metrics_peer.total_packet_count += 1;
        flow_metrics_peer.byte_count += meta_packet.packet_len as u64;
        flow_metrics_peer.l3_byte_count += meta_packet.l3_payload_len() as u64;
        flow_metrics_peer.l4_byte_count += meta_packet.l4_payload_len() as u64;
        flow_metrics_peer.total_byte_count += meta_packet.packet_len as u64;
        flow_metrics_peer.last = pkt_timestamp.into();
        if flow_metrics_peer.first.is_zero() {
            flow_metrics_peer.first = pkt_timestamp.into();
        }

        if meta_packet.vlan > 0 {
            flow.vlan = meta_packet.vlan;
        }
        if let Some(tunnel) = meta_packet.tunnel {
            match meta_packet.lookup_key.direction {
                PacketDirection::ClientToServer => {
                    flow.tunnel.tx_ip0 = tunnel.src;
                    flow.tunnel.tx_ip1 = tunnel.dst;
                    flow.tunnel.tx_mac0 = tunnel.mac_src;
                    flow.tunnel.tx_mac1 = tunnel.mac_dst;
                    flow.tunnel.tx_id = tunnel.id;
                }
                PacketDirection::ServerToClient => {
                    flow.tunnel.rx_ip0 = tunnel.src;
                    flow.tunnel.rx_ip1 = tunnel.dst;
                    flow.tunnel.rx_mac0 = tunnel.mac_src;
                    flow.tunnel.rx_mac1 = tunnel.mac_dst;
                    flow.tunnel.rx_id = tunnel.id;
                }
            }
            flow.tunnel.tier = tunnel.tier;
            flow.tunnel.tunnel_type = tunnel.tunnel_type;
            flow.tunnel.is_ipv6 = tunnel.is_ipv6;
        }
        // 这里需要查询策略，建立ARP表
        if meta_packet.is_ndp_response() {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let local_epc_id = match config.ebpf.as_ref() {
                Some(c) => c.epc_id as i32,
                _ => 0,
            };
            #[cfg(target_os = "windows")]
            let local_epc_id = 0;

            (self.policy_getter).lookup(meta_packet, self.id as usize, local_epc_id);
        }
    }

    fn collect_l7_stats(
        &mut self,
        node: &mut FlowNode,
        meta_flow_log: &mut Box<FlowLog>,
        l7_info: &L7ProtocolInfo,
        consistent_timestamp_in_l7_metrics: bool,
        time_in_micros: u64,
    ) {
        if let Some(flow_perf_stats) = node.tagged_flow.flow.flow_perf_stats.as_mut() {
            let flow_id = &node.tagged_flow.flow.flow_id;
            let l7_timeout_count = self
                .perf_cache
                .borrow_mut()
                .pop_timeout_count(flow_id, false); // TODO: flow_end is most likely false, but may also be true
            let (l7_perf_stats, _l7_protocol) =
                meta_flow_log.copy_and_reset_l7_perf_data(l7_timeout_count as u32);
            let app_proto_head = l7_info.app_proto_head().unwrap();
            let time_span = if consistent_timestamp_in_l7_metrics
                && app_proto_head.msg_type == LogMessageType::Response
                && app_proto_head.rrt != 0
            {
                node.tagged_flow.flow.flow_stat_time.as_secs()
                    - ((time_in_micros - app_proto_head.rrt) / Self::MICROS_IN_SECONDS)
            } else {
                0
            };
            // FIXME: the endpoint may be None after parsed
            let endpoint = if let Some(new_endpoint) = l7_info.get_endpoint() {
                node.tagged_flow.flow.last_endpoint = Some(new_endpoint.clone());
                Some(new_endpoint)
            } else {
                node.tagged_flow.flow.last_endpoint.clone()
            };

            let key = L7PerfStatsKey {
                endpoint,
                biz_type: l7_info.get_biz_type(),
                time_span: time_span as u32,
            };

            if let Some(l7_perf) = flow_perf_stats.l7.get_mut(&key) {
                l7_perf.sequential_merge(&l7_perf_stats);
            } else {
                flow_perf_stats.l7.insert(key, l7_perf_stats);
            }
        }
    }

    fn collect_metric(
        &mut self,
        config: &Config,
        node: &mut FlowNode,
        meta_packet: &mut MetaPacket,
        is_first_packet_direction: bool,
        is_first_packet: bool,
    ) {
        let flow_config = &config.flow;
        let log_parser_config = &config.log_parser;
        let consistent_timestamp_in_l7_metrics = config.flow.consistent_timestamp_in_l7_metrics;
        if let Some(mut log) = node.meta_flow_log.take() {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let local_epc_id = match config.ebpf.as_ref() {
                Some(c) => c.epc_id as i32,
                _ => 0,
            };
            #[cfg(target_os = "windows")]
            let local_epc_id = 0; // just for ebpf, Windows does not need this value
            let (local_epc, remote_epc) = if meta_packet.lookup_key.l2_end_0 {
                (local_epc_id, 0)
            } else {
                (0, local_epc_id)
            };
            let ip_protocol = meta_packet.lookup_key.proto;

            for packet in meta_packet {
                let ret = if ip_protocol == IpProtocol::UDP || ip_protocol == IpProtocol::TCP {
                    log.parse(
                        flow_config,
                        log_parser_config,
                        packet,
                        is_first_packet_direction,
                        Self::l7_metrics_enabled(flow_config),
                        Self::l7_log_parse_enabled(flow_config, &packet.lookup_key),
                        &mut self.app_table,
                        local_epc,
                        remote_epc,
                        &self.l7_protocol_checker,
                    )
                } else {
                    log.parse_l3(
                        flow_config,
                        log_parser_config,
                        packet,
                        Self::l7_metrics_enabled(flow_config),
                        Self::l7_log_parse_enabled(flow_config, &packet.lookup_key),
                        &mut self.app_table,
                        local_epc,
                        remote_epc,
                        &self.l7_protocol_checker,
                    )
                };

                match ret {
                    Ok(info) => {
                        if let Some(perf_stats) = node.tagged_flow.flow.flow_perf_stats.as_mut() {
                            perf_stats.l7_protocol = log.l7_protocol_enum.get_l7_protocol();
                        }
                        if node.tagged_flow.flow.direction_score != ServiceTable::MAX_SCORE {
                            // After perf.parse() success, meta_packet's direction is determined.
                            // Here we determine whether to reverse flow.
                            self.rectify_flow_direction(node, packet, is_first_packet);
                        }
                        match info {
                            crate::common::l7_protocol_log::L7ParseResult::Single(s) => {
                                let timestamp = packet.lookup_key.timestamp.as_micros();
                                self.collect_l7_stats(
                                    node,
                                    &mut log,
                                    &s,
                                    consistent_timestamp_in_l7_metrics,
                                    timestamp,
                                );
                                self.write_to_app_proto_log(flow_config, node, &packet, s);
                            }
                            crate::common::l7_protocol_log::L7ParseResult::Multi(m) => {
                                let timestamp = packet.lookup_key.timestamp.as_micros();
                                for i in m.into_iter() {
                                    self.collect_l7_stats(
                                        node,
                                        &mut log,
                                        &i,
                                        consistent_timestamp_in_l7_metrics,
                                        timestamp,
                                    );
                                    self.write_to_app_proto_log(flow_config, node, &packet, i);
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(Error::L7ProtocolUnknown) => {
                        self.flow_perf_counter
                            .unknown_l7_protocol
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => log::trace!("unhandled log parse error: {}", e),
                }
            }
            node.meta_flow_log = Some(log);
        }
    }

    fn new_tcp_node(&mut self, config: &Config, meta_packet: &mut MetaPacket) -> Box<FlowNode> {
        let flow_config = &config.flow;
        let mut node = self.init_flow(config, meta_packet);
        meta_packet.flow_id = node.tagged_flow.flow.flow_id;
        meta_packet.second_in_minute =
            (node.tagged_flow.flow.start_time.as_secs() % SECONDS_IN_MINUTE) as u8;
        meta_packet.is_active_service = node.tagged_flow.flow.is_active_service;
        let mut reverse = false;
        if node.tagged_flow.flow.signal_source == SignalSource::EBPF {
            if meta_packet.ebpf_type == EbpfType::SocketCloseEvent {
                // When receiving the socket close event, set the node timeout to 1s to reduce the memory occupation
                node.timeout = DEFAULT_SOCKET_CLOSE_TIMEOUT;
            } else {
                // Initialize a timeout long enough for eBPF Flow to enable successful session aggregation.
                node.timeout = config.log_parser.l7_log_session_aggr_timeout.into();
            }
        } else {
            reverse = self.update_l4_direction(meta_packet, &mut node, true);

            let tcp_data = if let ProtocolData::TcpHeader(tcp_data) = &meta_packet.protocol_data {
                tcp_data
            } else {
                unreachable!()
            };
            if tcp_data.flags.is_invalid() {
                // exception timeout
                node.timeout = flow_config.flow_timeout.exception;
                node.flow_state = FlowState::Exception;
            }
            self.update_flow_state_machine(
                flow_config,
                &mut node,
                tcp_data.flags,
                meta_packet.lookup_key.direction,
            );
            self.update_syn_or_syn_ack_seq(&mut node, meta_packet);
        }

        if flow_config.collector_enabled {
            if let Some(tcp_segments) = node.tcp_segments.as_mut() {
                if let Some(mut packets) = tcp_segments.inject(meta_packet.to_owned_segment()) {
                    let mut packets = packets
                        .drain(..)
                        .map(|x| x.into_any().downcast::<MetaPacket>().unwrap())
                        .collect::<Vec<Box<MetaPacket>>>();
                    for packet in &mut packets {
                        let direction =
                            packet.lookup_key.direction == PacketDirection::ClientToServer;
                        self.collect_metric(config, &mut node, packet, direction, false);
                    }
                }
            } else {
                self.collect_metric(config, &mut node, meta_packet, !reverse, true);
            }
        }

        // After collect_metric() is called for eBPF MetaPacket, its direction is determined.
        if node.tagged_flow.flow.signal_source == SignalSource::EBPF
            && meta_packet.ebpf_type != EbpfType::SocketCloseEvent
        {
            if meta_packet.lookup_key.direction == PacketDirection::ClientToServer {
                node.residual_request += 1;
            } else {
                node.residual_request -= 1;
            }
        }

        // Enterprise Edition Feature: packet-sequence
        if self.packet_sequence_enabled {
            self.append_to_block(flow_config, &mut node, meta_packet);
        }
        node
    }

    fn new_udp_node(&mut self, config: &Config, meta_packet: &mut MetaPacket) -> Box<FlowNode> {
        let flow_config = config.flow;
        let mut node = self.init_flow(config, meta_packet);
        meta_packet.flow_id = node.tagged_flow.flow.flow_id;
        meta_packet.second_in_minute =
            (node.tagged_flow.flow.start_time.as_secs() % SECONDS_IN_MINUTE) as u8;
        meta_packet.is_active_service = node.tagged_flow.flow.is_active_service;
        node.flow_state = FlowState::Established;
        // For eBPF UDP Flow, there is no special treatment for timeout.
        node.timeout = flow_config.flow_timeout.opening; // use opening timeout
        let mut reverse = false;
        if node.tagged_flow.flow.signal_source != SignalSource::EBPF {
            // eBPF Flow only use server_port to correct the direction.
            reverse = self.update_l4_direction(meta_packet, &mut node, true);
        }
        if flow_config.collector_enabled {
            self.collect_metric(config, &mut node, meta_packet, !reverse, true);
        }
        node
    }

    fn new_other_node(&mut self, config: &Config, meta_packet: &mut MetaPacket) -> Box<FlowNode> {
        let mut node = self.init_flow(config, meta_packet);
        meta_packet.flow_id = node.tagged_flow.flow.flow_id;
        meta_packet.second_in_minute =
            (node.tagged_flow.flow.start_time.as_secs() % SECONDS_IN_MINUTE) as u8;
        node.flow_state = FlowState::Established;
        // opening timeout
        node.timeout = config.flow.flow_timeout.opening;
        if config.flow.collector_enabled {
            self.collect_metric(
                config,
                &mut node,
                meta_packet,
                meta_packet.lookup_key.direction == PacketDirection::ClientToServer,
                true,
            );
        }
        node
    }

    fn new_flow_node(
        &mut self,
        config: &Config,
        meta_packet: &mut MetaPacket,
    ) -> Option<Box<FlowNode>> {
        if self.size as usize >= self.capacity {
            self.stats_counter
                .drop_by_capacity
                .fetch_add(1, Ordering::Relaxed);
            self.lookup_without_flow(config, meta_packet);
            return None;
        }

        self.stats_counter.new.fetch_add(1, Ordering::Relaxed);
        let mut node = match meta_packet.lookup_key.proto {
            IpProtocol::TCP => self.new_tcp_node(config, meta_packet),
            IpProtocol::UDP => self.new_udp_node(config, meta_packet),
            _ => self.new_other_node(config, meta_packet),
        };

        if meta_packet.signal_source == SignalSource::EBPF {
            node.tagged_flow.flow.pod_id = meta_packet.pod_id;
            if node.meta_flow_log.is_some() && node.meta_flow_log.as_ref().unwrap().server_port == 0
            {
                // For ebpf data, if server_port is 0, it means that parsed data failed,
                // the info in node maybe wrong, we should not create this node.
                self.stats_counter.closed.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }

        self.size += 1;
        self.stats_counter
            .concurrent
            .fetch_add(1, Ordering::Relaxed);
        Some(node)
    }

    fn flush_queue(&mut self, config: &FlowConfig, now: Duration) {
        if now > config.flush_interval + self.last_queue_flush {
            if self.l7_stats_buffer.len() > 0 {
                if let Err(e) = self
                    .l7_stats_output_queue
                    .send_all(&mut self.l7_stats_buffer)
                {
                    warn!("flow-map push l7 stats to queue failed, because {:?}", e);
                    self.l7_stats_buffer.clear();
                }
            }
            if self.output_queue.is_some() && self.output_buffer.len() > 0 {
                if let Err(e) = self
                    .output_queue
                    .as_ref()
                    .unwrap()
                    .send_all(&mut self.output_buffer)
                {
                    warn!(
                        "flow-map push tagged flows to queue failed, because {:?}",
                        e
                    );
                    self.output_buffer.clear();
                }
            }
            self.last_queue_flush = now;
        }
    }

    fn push_to_flow_stats_queue(&mut self, tagged_flow: Arc<BatchedBox<TaggedFlow>>) {
        if self.l7_stats_buffer.len() >= QUEUE_BATCH_SIZE {
            if let Err(e) = self
                .l7_stats_output_queue
                .send_all(&mut self.l7_stats_buffer)
            {
                warn!("flow-map push l7 stats to queue failed, because {:?}", e);
                self.l7_stats_buffer.clear();
            }
        }

        self.stats_counter.flow_delay.fetch_max(
            self.system_time.as_nanos() as i64 - tagged_flow.flow.flow_stat_time.as_nanos() as i64,
            Ordering::Relaxed,
        );

        if let Some(output_queue) = &mut self.output_queue {
            self.output_buffer.push(tagged_flow);
            if self.output_buffer.len() >= QUEUE_BATCH_SIZE {
                if let Err(e) = output_queue.send_all(&mut self.output_buffer) {
                    warn!(
                        "flow-map push tagged flows to queue failed, because {:?}",
                        e
                    );
                    self.output_buffer.clear();
                }
            }
        }
    }

    fn flush_l7_perf_stats(
        &mut self,
        collect_stats: bool,
        tagged_flow: Arc<BatchedBox<TaggedFlow>>,
    ) {
        if collect_stats {
            let flow = &tagged_flow.flow;
            if let Some(flow_perf) = flow.flow_perf_stats.as_ref() {
                let l7_protocol = flow_perf.l7_protocol;
                let count = flow_perf.l7.len();
                for (index, (l7_perf_stats_key, l7_perf_stats)) in flow_perf.l7.iter().enumerate() {
                    let mut l7_stats = L7Stats::default();
                    l7_stats.stats = l7_perf_stats.clone();
                    l7_stats.endpoint = l7_perf_stats_key.endpoint.clone();
                    l7_stats.flow_id = flow.flow_id;
                    l7_stats.signal_source = flow.signal_source;
                    l7_stats.time_in_second = flow.flow_stat_time.into();
                    l7_stats.l7_protocol = l7_protocol;
                    l7_stats.time_span = l7_perf_stats_key.time_span;
                    if index + 1 == count {
                        l7_stats.flow = Some(tagged_flow.clone());
                    } else {
                        l7_stats.flow = None;
                    }
                    self.l7_stats_buffer
                        .push(self.l7_stats_allocator.allocate_one_with(l7_stats));
                }
            }
        }
    }

    // go 版本的removeAndOutput
    fn node_removed_aftercare(
        &mut self,
        config: &Config,
        mut node: Box<FlowNode>,
        timeout: Duration,
        meta_packet: Option<&mut MetaPacket>,
    ) {
        if config.flow.collector_enabled {
            if let Some(tcp_segments) = node.tcp_segments.as_mut() {
                if let Some(mut packets) = tcp_segments.flush() {
                    let mut packets = packets
                        .drain(..)
                        .map(|x| x.into_any().downcast::<MetaPacket>().unwrap())
                        .collect::<Vec<Box<MetaPacket>>>();
                    for packet in &mut packets {
                        let direction =
                            packet.lookup_key.direction == PacketDirection::ClientToServer;
                        self.collect_metric(config, &mut node, packet, direction, false);
                    }
                }
            }
        }

        // 统计数据输出前矫正流方向
        self.update_flow_direction(&mut node, meta_packet);

        let mut flow = &mut node.tagged_flow.flow;
        if flow.signal_source == SignalSource::EBPF {
            // the flow which from eBPF, it's close_type always be CloseType::Timeout
            flow.close_type = CloseType::Timeout;
        } else {
            flow.update_close_type(node.flow_state);
        }
        flow.end_time = timeout.into();
        flow.flow_stat_time = Timestamp::from_nanos(
            (timeout.as_nanos() / STATISTICAL_INTERVAL.as_nanos() * STATISTICAL_INTERVAL.as_nanos())
                as u64,
        );

        // Enterprise Edition Feature: packet-sequence
        if self.packet_sequence_enabled && flow.flow_key.proto == IpProtocol::TCP {
            if let Some(block) = node.packet_sequence_block.take() {
                if let Err(e) = self.packet_sequence_queue.as_ref().unwrap().send(block) {
                    warn!("packet sequence block to queue failed, because {:?}", e);
                }
            }
        }

        let mut collect_stats = false;
        if config.flow.collector_enabled
            && (flow.flow_key.proto == IpProtocol::TCP
                || flow.flow_key.proto == IpProtocol::UDP
                || flow.flow_key.proto == IpProtocol::ICMPV4
                || flow.flow_key.proto == IpProtocol::ICMPV6)
        {
            if let Some(perf) = node.meta_flow_log.as_mut() {
                perf.copy_and_reset_l4_perf_data(flow.reversed, &mut flow);

                collect_stats = true;
            }
        }

        self.size -= 1;
        self.stats_counter
            .concurrent
            .fetch_sub(1, Ordering::Relaxed);
        self.stats_counter.closed.fetch_add(1, Ordering::Relaxed);

        let tagged_flow = Arc::new(
            self.tagged_flow_allocator
                .allocate_one_with(node.tagged_flow.clone()),
        );
        self.flush_l7_perf_stats(collect_stats, tagged_flow.clone());
        self.push_to_flow_stats_queue(tagged_flow);
        if let Some(log) = node.meta_flow_log.take() {
            FlowLog::recycle(&mut self.tcp_perf_pool, *log);
        }
        self.flow_node_pool.put(node);
    }

    // go 版本的copyAndOutput
    fn node_updated_aftercare(
        &mut self,
        config: &FlowConfig,
        node: &mut FlowNode,
        timestamp: Duration,
        meta_packet: Option<&mut MetaPacket>,
    ) {
        // if this function is called by inject_flush_ticker (no meta_packet),
        // skip statistical interval check because timestamp will be equal to
        // flow_stat_time
        if node.packet_in_tick
            && (meta_packet.is_none()
                || timestamp >= node.tagged_flow.flow.flow_stat_time + STATISTICAL_INTERVAL
                || timestamp < node.tagged_flow.flow.flow_stat_time)
        {
            self.update_flow_direction(node, meta_packet); // 每个流统计数据输出前矫正流方向
            node.tagged_flow.flow.close_type = CloseType::ForcedReport;
            let flow = &mut node.tagged_flow.flow;
            if !config.collector_enabled {
                return;
            }
            flow.set_tap_side(config.agent_type, config.cloud_gateway_traffic);

            let mut collect_stats = false;
            if flow.flow_key.proto == IpProtocol::TCP
                || flow.flow_key.proto == IpProtocol::UDP
                || flow.flow_key.proto == IpProtocol::ICMPV4
                || flow.flow_key.proto == IpProtocol::ICMPV6
            {
                if let Some(perf) = node.meta_flow_log.as_mut() {
                    perf.copy_and_reset_l4_perf_data(flow.reversed, flow);
                    collect_stats = true;
                }
            }

            let tagged_flow = Arc::new(
                self.tagged_flow_allocator
                    .allocate_one_with(node.tagged_flow.clone()),
            );
            self.flush_l7_perf_stats(collect_stats, tagged_flow.clone());
            self.push_to_flow_stats_queue(tagged_flow);
            node.reset_flow_stat_info();
        }
    }

    // When a socket close event is received, the event is sent to the SessionAggregator to prevent the accumulation of the SessionAggregator
    fn send_socket_close_event(&mut self, node: &FlowNode) {
        match node.meta_flow_log.as_ref() {
            Some(l) => {
                let l7_protocol = l.l7_protocol_enum.get_l7_protocol();
                // If this protocol has session_id, the AppProto in SessionAggregator cannot be found based on flow_id alone.
                if !l7_protocol.has_session_id() {
                    let app_proto = PseudoAppProto::new(
                        PseudoAppProto::session_key(
                            node.tagged_flow.flow.flow_id,
                            node.last_cap_seq,
                            node.tagged_flow.flow.signal_source,
                            l7_protocol,
                        ),
                        node.recent_time,
                        node.tagged_flow.flow.tap_side,
                    );
                    self.protolog_buffer
                        .push(Box::new(AppProto::PseudoAppProto(app_proto)));
                    if self.protolog_buffer.len() >= QUEUE_BATCH_SIZE {
                        self.flush_app_protolog();
                    }
                }
            }
            _ => {}
        }
    }

    fn write_to_app_proto_log(
        &mut self,
        config: &FlowConfig,
        node: &mut FlowNode,
        meta_packet: &MetaPacket,
        l7_info: L7ProtocolInfo,
    ) {
        let domain = l7_info.get_request_domain();
        if !domain.is_empty() {
            node.tagged_flow.flow.request_domain = domain;
        }

        if let Some(head) = l7_info.app_proto_head() {
            node.tagged_flow
                .flow
                .set_tap_side(config.agent_type, config.cloud_gateway_traffic);

            if let Some(app_proto) =
                MetaAppProto::new(&node.tagged_flow, meta_packet, l7_info, head)
            {
                self.protolog_buffer
                    .push(Box::new(AppProto::MetaAppProto(app_proto)));
                if self.protolog_buffer.len() >= QUEUE_BATCH_SIZE {
                    self.flush_app_protolog();
                }
            }
        }
    }

    fn flush_app_protolog(&mut self) {
        if self.protolog_buffer.len() > 0 {
            if let Err(e) = self.out_log_queue.send_all(&mut self.protolog_buffer) {
                warn!(
                    "flow-map push MetaAppProto to queue failed, because {:?}",
                    e
                );
                self.protolog_buffer.clear();
            }
        }
    }

    fn update_l4_direction(
        &mut self,
        meta_packet: &mut MetaPacket,
        node: &mut FlowNode,
        is_first_packet: bool,
    ) -> bool {
        let lookup_key = &meta_packet.lookup_key;
        let (src_l3_epc_id, dst_l3_epc_id) = if let Some(ep) = node.endpoint_data_cache.as_ref() {
            (
                ep.src_info().l3_epc_id as i16,
                ep.dst_info().l3_epc_id as i16,
            )
        } else {
            (0, 0)
        };
        let flow_src_key = ServiceKey::new(lookup_key.src_ip, src_l3_epc_id, lookup_key.src_port);
        let flow_dst_key = ServiceKey::new(lookup_key.dst_ip, dst_l3_epc_id, lookup_key.dst_port);
        let (mut flow_src_score, mut flow_dst_score) = match lookup_key.proto {
            // TCP/UDP
            IpProtocol::TCP => {
                let tcp_data = if let ProtocolData::TcpHeader(tcp_data) = &meta_packet.protocol_data
                {
                    tcp_data
                } else {
                    unreachable!()
                };
                self.service_table.get_tcp_score(
                    is_first_packet,
                    meta_packet.need_reverse_flow,
                    lookup_key.direction,
                    tcp_data.flags,
                    false,
                    false,
                    flow_src_key,
                    flow_dst_key,
                )
            }
            IpProtocol::UDP => self.service_table.get_udp_score(
                is_first_packet,
                meta_packet.need_reverse_flow,
                lookup_key.direction,
                flow_src_key,
                flow_dst_key,
            ),
            _ => unimplemented!(),
        };

        if PacketDirection::ServerToClient == meta_packet.lookup_key.direction {
            mem::swap(&mut flow_src_score, &mut flow_dst_score);
        }

        let mut reverse = false;
        if !ServiceTable::is_client_to_server(flow_src_score, flow_dst_score) {
            mem::swap(&mut flow_src_score, &mut flow_dst_score);
            Self::reverse_flow(node, is_first_packet);
            meta_packet.lookup_key.direction = meta_packet.lookup_key.direction.reversed();
            reverse = true;
        }

        node.tagged_flow.flow.direction_score = flow_dst_score;
        node.tagged_flow.flow.is_active_service = ServiceTable::is_active_service(flow_dst_score);
        return reverse;
    }

    // just for ebpf, tcp flow.is_active_service is always true,
    // but udp flow.is_active_service still needs to continue to judge.
    fn update_udp_is_active(&mut self, node: &mut FlowNode, direction: PacketDirection) {
        // If it is already an active service, we do not need to continue to query.
        if !node.tagged_flow.flow.is_active_service {
            let flow_key = &node.tagged_flow.flow.flow_key;
            let (src_l3_epc_id, dst_l3_epc_id) = if let Some(ep) = node.endpoint_data_cache.as_ref()
            {
                (
                    ep.src_info().l3_epc_id as i16,
                    ep.dst_info().l3_epc_id as i16,
                )
            } else {
                (0, 0)
            };
            // Because the flow direction is already correct, we can use flow_key's
            // ip_src, port_src and ip_dst, port_dst directly without swapping them.
            let src_key = ServiceKey::new(flow_key.ip_src, src_l3_epc_id, flow_key.port_src);
            let dst_key = ServiceKey::new(flow_key.ip_dst, dst_l3_epc_id, flow_key.port_dst);

            node.tagged_flow.flow.is_active_service = self
                .service_table
                .is_ebpf_active_udp_service(src_key, dst_key, direction);
        }
    }

    fn update_flow_direction(&mut self, node: &mut FlowNode, meta_packet: Option<&mut MetaPacket>) {
        if node.tagged_flow.flow.signal_source == SignalSource::EBPF {
            // The direction of eBPF Flow is determined when FlowPerf::l7_parse is called for the
            // first time, and no further correction is required.
            return;
        }
        let flow_key = &node.tagged_flow.flow.flow_key;
        let src_epc_id = node.tagged_flow.flow.flow_metrics_peers[0].l3_epc_id as i16;
        let dst_epc_id = node.tagged_flow.flow.flow_metrics_peers[1].l3_epc_id as i16;

        let flow_src_key = ServiceKey::new(flow_key.ip_src, src_epc_id, flow_key.port_src);
        let flow_dst_key = ServiceKey::new(flow_key.ip_dst, dst_epc_id, flow_key.port_dst);
        let (mut flow_src_score, mut flow_dst_score) = match flow_key.proto {
            IpProtocol::TCP => {
                let toa_sent_by_src = node.tagged_flow.flow.flow_metrics_peers[0].nat_source
                    == TapPort::NAT_SOURCE_TOA;
                let toa_sent_by_dst = node.tagged_flow.flow.flow_metrics_peers[1].nat_source
                    == TapPort::NAT_SOURCE_TOA;
                self.service_table.get_tcp_score(
                    false,
                    false,
                    PacketDirection::ClientToServer,
                    TcpFlags::empty(),
                    toa_sent_by_src,
                    toa_sent_by_dst,
                    flow_src_key,
                    flow_dst_key,
                )
            }
            IpProtocol::UDP => self.service_table.get_udp_score(
                false,
                false,
                PacketDirection::ClientToServer,
                flow_src_key,
                flow_dst_key,
            ),
            _ => return,
        };

        if !ServiceTable::is_client_to_server(flow_src_score, flow_dst_score) {
            mem::swap(&mut flow_src_score, &mut flow_dst_score);
            Self::reverse_flow(node, false);
            if let Some(pkt) = meta_packet {
                pkt.lookup_key.direction = pkt.lookup_key.direction.reversed();
            }
        }

        node.tagged_flow.flow.direction_score = flow_dst_score;
        node.tagged_flow.flow.is_active_service = ServiceTable::is_active_service(flow_dst_score);
    }

    fn reverse_flow(node: &mut FlowNode, is_first_packet: bool) {
        node.policy_in_tick.swap(0, 1);
        node.policy_data_cache.swap(0, 1);
        if let Some(ep) = node.endpoint_data_cache.as_mut() {
            ep.reverse();
        }
        node.tagged_flow.flow.reverse(is_first_packet);
        node.tagged_flow.tag.reverse();
        if let Some(tcp_segments) = node.tcp_segments.as_mut() {
            tcp_segments.reverse();
        }
        // Enterprise Edition Feature: packet-sequence
        if node.packet_sequence_block.is_some() {
            node.packet_sequence_block
                .as_mut()
                .unwrap()
                .reverse_needed_for_new_packet();
        }
    }

    fn rectify_flow_direction(
        &mut self,
        node: &mut FlowNode,
        meta_packet: &mut MetaPacket,
        is_first_packet: bool,
    ) {
        let need_reverse = if node.tagged_flow.flow.flow_key.ip_src == meta_packet.lookup_key.src_ip
            && node.tagged_flow.flow.flow_key.port_src == meta_packet.lookup_key.src_port
        {
            // If flow_key.ip_src and flow_key.port_src of node.tagged_flow.flow are the same as
            // that of meta_packet, but the direction of meta_packet is S2C, reverse flow
            meta_packet.lookup_key.direction == PacketDirection::ServerToClient
        } else {
            // If flow_key.ip_src or flow_key.port_src of node.tagged_flow.flow is different
            // from that of meta_packet, and the direction of meta_packet is C2S, reverse flow
            meta_packet.lookup_key.direction == PacketDirection::ClientToServer
        };

        if need_reverse {
            Self::reverse_flow(node, is_first_packet);
            // After modifying the flow direction, it is necessary to synchronize the service table to
            // avoid incorrect directions in subsequent queries
            let flow = &node.tagged_flow.flow;
            if flow.signal_source != SignalSource::EBPF {
                let src_epc_id = flow.flow_metrics_peers[0].l3_epc_id as i16;
                let dst_epc_id = flow.flow_metrics_peers[1].l3_epc_id as i16;
                let flow_key = &flow.flow_key;
                self.service_table.reset_score(
                    ServiceKey::new(flow_key.ip_src, src_epc_id, flow_key.port_src),
                    ServiceKey::new(flow_key.ip_dst, dst_epc_id, flow_key.port_dst),
                )
            }
        }
    }

    fn update_endpoint_and_policy_data(
        &mut self,
        node: &mut FlowNode,
        meta_packet: &mut MetaPacket,
    ) {
        // update endpoint
        if let Some(data) = meta_packet.endpoint_data.as_ref() {
            match meta_packet.lookup_key.direction {
                PacketDirection::ClientToServer => {
                    node.endpoint_data_cache = Some(data.clone());
                }
                PacketDirection::ServerToClient => {
                    node.endpoint_data_cache = Some(data.reversed());
                }
            }
        }

        if let Some(ep) = node.endpoint_data_cache.as_ref() {
            let src_info = ep.src_info();
            let peer_src = &mut node.tagged_flow.flow.flow_metrics_peers[0];
            let mut reset_tap_side =
                peer_src.is_l2_end != src_info.l2_end || peer_src.is_l3_end != src_info.l3_end;
            peer_src.is_device = src_info.is_device;
            peer_src.is_vip_interface = src_info.is_vip_interface;
            peer_src.is_l2_end = src_info.l2_end;
            peer_src.is_l3_end = src_info.l3_end;
            peer_src.l3_epc_id = src_info.l3_epc_id;
            peer_src.is_vip = src_info.is_vip;
            peer_src.is_local_mac = src_info.is_local_mac;
            peer_src.is_local_ip = src_info.is_local_ip;
            if !src_info.real_ip.is_unspecified()
                && TapPort::NAT_SOURCE_VIP > meta_packet.lookup_key.dst_nat_source
            {
                meta_packet.lookup_key.src_nat_ip = src_info.real_ip;
                meta_packet.lookup_key.src_nat_source = TapPort::NAT_SOURCE_VIP;
            }

            let dst_info = ep.dst_info();
            let peer_dst = &mut node.tagged_flow.flow.flow_metrics_peers[1];
            reset_tap_side = reset_tap_side
                || peer_dst.is_l2_end != dst_info.l2_end
                || peer_dst.is_l3_end != dst_info.l3_end;
            peer_dst.is_device = dst_info.is_device;
            peer_dst.is_vip_interface = dst_info.is_vip_interface;
            peer_dst.is_l2_end = dst_info.l2_end;
            peer_dst.is_l3_end = dst_info.l3_end;
            peer_dst.l3_epc_id = dst_info.l3_epc_id;
            peer_dst.is_vip = dst_info.is_vip;
            peer_dst.is_local_mac = dst_info.is_local_mac;
            peer_dst.is_local_ip = dst_info.is_local_ip;
            if !dst_info.real_ip.is_unspecified()
                && TapPort::NAT_SOURCE_VIP > meta_packet.lookup_key.dst_nat_source
            {
                meta_packet.lookup_key.dst_nat_ip = dst_info.real_ip;
                meta_packet.lookup_key.dst_nat_source = TapPort::NAT_SOURCE_VIP;
            }
            // When there is a change in l2end or l3end, the tap side needs to be recalculated
            if reset_tap_side {
                node.tagged_flow.flow.tap_side = TapSide::Rest;
            }
        }

        // update policy data
        if let Some(policy_data) = meta_packet.policy_data.as_ref() {
            node.policy_data_cache[meta_packet.lookup_key.direction as usize] =
                Some(policy_data.clone());
        }
        node.tagged_flow.tag.policy_data = node.policy_data_cache.clone();
    }

    fn update_stats_counter(c: &FlowMapCounter, slots: u64, max_depth: u64) {
        c.slots.swap(slots, Ordering::Relaxed);
        c.slot_max_depth.fetch_max(max_depth, Ordering::Relaxed);
    }
}

#[rustfmt::skip]
#[derive(Default)]
pub struct FlowMapCounter {
    new: AtomicU64,                      // the number of created flow
    closed: AtomicU64,                   // the number of closed flow
    drop_by_window: AtomicU64,           // times of flush which drop by window
    drop_by_capacity: AtomicU64,         // packet counter which drop by capacity
    packet_delay: AtomicI64,             // inject_meta_packet delay compared to ntp corrected system time
    flush_delay: AtomicI64,              // inject_flush_ticker delay compared to ntp corrected system time
    flow_delay: AtomicI64,               // output flow `flow_stat_time` delay compared to ntp corrected system time
    concurrent: AtomicU64,               // current the number of FlowNode
    slots: AtomicU64,                    // current the length of HashMap
    slot_max_depth: AtomicU64,           // the max length of Vec<FlowNode>
    total_scan: AtomicU64,               // the total number of iteration to scan over Vec<FlowNode>
    time_set_shrinks: AtomicU64,         // the total number of time_set HashSet shrinks
    pub l7_perf_cache_len: AtomicU64,    // the number of struct L7PerfCache::rrt_cache length
    pub l7_timeout_cache_len: AtomicU64, // the number of struct L7PerfCache::timeout_cache length
}

impl RefCountable for FlowMapCounter {
    fn get_counters(&self) -> Vec<Counter> {
        let concurrent = self.concurrent.load(Ordering::Relaxed);
        let slots = self.slots.swap(0, Ordering::Relaxed);

        vec![
            (
                "new",
                CounterType::Gauged,
                CounterValue::Unsigned(self.new.swap(0, Ordering::Relaxed)),
            ),
            (
                "closed",
                CounterType::Gauged,
                CounterValue::Unsigned(self.closed.swap(0, Ordering::Relaxed)),
            ),
            (
                "drop_by_window",
                CounterType::Gauged,
                CounterValue::Unsigned(self.drop_by_window.swap(0, Ordering::Relaxed)),
            ),
            (
                "drop_by_capacity",
                CounterType::Gauged,
                CounterValue::Unsigned(self.drop_by_capacity.swap(0, Ordering::Relaxed)),
            ),
            (
                "packet_delay",
                CounterType::Gauged,
                CounterValue::Signed(self.packet_delay.swap(0, Ordering::Relaxed)),
            ),
            (
                "flush_delay",
                CounterType::Gauged,
                CounterValue::Signed(self.flush_delay.swap(0, Ordering::Relaxed)),
            ),
            (
                "flow_delay",
                CounterType::Gauged,
                CounterValue::Signed(self.flow_delay.swap(0, Ordering::Relaxed)),
            ),
            (
                "concurrent",
                CounterType::Gauged,
                CounterValue::Unsigned(concurrent),
            ),
            (
                "slot_max_depth",
                CounterType::Gauged,
                CounterValue::Unsigned(self.slot_max_depth.swap(0, Ordering::Relaxed)),
            ),
            (
                "total_scan",
                CounterType::Gauged,
                CounterValue::Unsigned(self.total_scan.swap(0, Ordering::Relaxed)),
            ),
            ("slots", CounterType::Gauged, CounterValue::Unsigned(slots)),
            (
                "time_set_shrinks",
                CounterType::Gauged,
                CounterValue::Unsigned(self.time_set_shrinks.swap(0, Ordering::Relaxed)),
            ),
            (
                "l7_perf_cache_len",
                CounterType::Gauged,
                CounterValue::Unsigned(self.l7_perf_cache_len.swap(0, Ordering::Relaxed)),
            ),
            (
                "l7_timeout_cache_len",
                CounterType::Gauged,
                CounterValue::Unsigned(self.l7_timeout_cache_len.swap(0, Ordering::Relaxed)),
            ),
        ]
    }
}

pub fn _reverse_meta_packet(packet: &mut MetaPacket) {
    let lookup_key = &mut packet.lookup_key;
    mem::swap(&mut lookup_key.src_ip, &mut lookup_key.dst_ip);
    mem::swap(&mut lookup_key.src_port, &mut lookup_key.dst_port);
    mem::swap(&mut lookup_key.src_mac, &mut lookup_key.dst_mac);
    mem::swap(&mut lookup_key.l2_end_0, &mut lookup_key.l2_end_1);
    if let Some(ep) = packet.endpoint_data.as_mut() {
        ep.reverse();
    }
}

pub fn _new_flow_map_and_receiver(
    agent_type: AgentType,
    flow_timeout: Option<FlowTimeout>,
    ignore_idc_vlan: bool,
) -> (ModuleConfig, FlowMap, Receiver<Arc<BatchedBox<TaggedFlow>>>) {
    let (_, mut policy_getter) = Policy::new(1, 0, 1 << 10, 1 << 14, false, false);
    policy_getter.disable();
    let queue_debugger = QueueDebugger::new();
    let (output_queue_sender, output_queue_receiver, _) =
        queue::bounded_with_debug(256, "", &queue_debugger);
    let (l7_stats_output_queue_sender, _, _) = queue::bounded_with_debug(256, "", &queue_debugger);
    let (app_proto_log_queue, _, _) = queue::bounded_with_debug(256, "", &queue_debugger);
    let (packet_sequence_queue, _, _) = queue::bounded_with_debug(256, "", &queue_debugger); // Enterprise Edition Feature: packet-sequence
    let mut config = ModuleConfig {
        flow: FlowConfig {
            agent_type,
            collector_enabled: true,
            l4_performance_enabled: true,
            l7_metrics_enabled: true,
            app_proto_log_enabled: true,
            ignore_idc_vlan: ignore_idc_vlan,
            flow_timeout: flow_timeout.unwrap_or(super::TcpTimeout::default().into()),
            ..(&UserConfig::standalone_default()).into()
        },
        ..Default::default()
    };
    // Any
    config.flow.l7_log_tap_types[0] = true;
    config.flow.agent_type = agent_type;
    let flow_map = FlowMap::new(
        0,
        Some(output_queue_sender),
        l7_stats_output_queue_sender,
        policy_getter,
        app_proto_log_queue,
        Arc::new(AtomicI64::new(0)),
        &config.flow,
        Some(packet_sequence_queue), // Enterprise Edition Feature: packet-sequence
        Arc::new(stats::Collector::new("", Arc::new(AtomicI64::new(0)))),
        false,
    );

    (config, flow_map, output_queue_receiver)
}

pub fn _new_meta_packet<'a>() -> MetaPacket<'a> {
    let mut packet = MetaPacket::default();
    packet.lookup_key = LookupKey {
        timestamp: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .into(),
        src_mac: MacAddr::from_str("12:34:56:78:9A:BC").unwrap(),
        dst_mac: MacAddr::from_str("21:43:65:87:A9:CB").unwrap(),
        eth_type: EthernetType::IPV4,
        proto: IpProtocol::TCP,
        src_ip: Ipv4Addr::new(8, 8, 8, 8).into(),
        dst_ip: Ipv4Addr::new(114, 114, 114, 114).into(),
        src_port: 12345,
        dst_port: 22,
        tap_type: CaptureNetworkType::Idc(1),
        ..Default::default()
    };
    packet.header_type = HeaderType::Ipv4Tcp;
    packet.tap_port = TapPort(65533);
    packet.packet_len = 128;
    packet.protocol_data = ProtocolData::TcpHeader(MetaPacketTcpHeader {
        data_offset: 5,
        flags: TcpFlags::SYN,
        ack: 0,
        seq: 0,
        ..Default::default()
    });
    packet.endpoint_data = Some(EndpointDataPov::new(Arc::new(EndpointData {
        src_info: EndpointInfo {
            real_ip: Ipv4Addr::UNSPECIFIED.into(),
            l2_end: false,
            l3_end: false,
            is_device: false,
            is_vip_interface: false,
            is_vip: false,
            is_local_mac: false,
            is_local_ip: false,

            l2_epc_id: EPC_DEEPFLOW,
            l3_epc_id: 1,
        },
        dst_info: EndpointInfo {
            real_ip: Ipv4Addr::UNSPECIFIED.into(),
            l2_end: false,
            l3_end: false,
            is_device: false,
            is_vip_interface: false,
            is_vip: false,
            is_local_mac: false,
            is_local_ip: false,

            l2_epc_id: EPC_DEEPFLOW,
            l3_epc_id: EPC_INTERNET,
        },
    })));
    packet
}

// 对应 flow_generator_test.go
#[cfg(test)]
mod tests {
    use std::{net::IpAddr, ops::Add, time};

    use super::*;

    use crate::{
        common::{enums::EthernetType, flow::CloseType, tap_port::TapPort},
        utils::test::Capture,
    };
    use npb_pcap_policy::{DirectionType, NpbAction, NpbTunnelType, PolicyData, TapSide};
    use public::utils::net::MacAddr;

    const DEFAULT_DURATION: Duration = Duration::from_millis(10);

    impl FlowMap {
        fn reset_start_time(&mut self, d: Duration) {
            self.start_time = d;
            self.start_time_in_unit = (d.as_nanos() / TIME_UNIT.as_nanos()) as u64;
        }
    }

    #[test]
    fn syn_rst() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let mut packet0 = _new_meta_packet();
        flow_map.inject_meta_packet(&config, &mut packet0);
        let mut packet1 = _new_meta_packet();
        if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data.flags = TcpFlags::RST;
        }
        _reverse_meta_packet(&mut packet1);
        packet1.lookup_key.timestamp += DEFAULT_DURATION.into();
        let flush_timestamp = packet1.lookup_key.timestamp.into();
        flow_map.inject_meta_packet(&config, &mut packet1);

        // 没到期删除，所以下游queue等不到flow
        flow_map.inject_flush_ticker(&config, flush_timestamp);

        flow_map.inject_flush_ticker(&config, flush_timestamp + Duration::from_secs(10));

        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(tagged_flow.flow.close_type, CloseType::ServerReset);
            // 生成包需要时间，因为 pkt0 - pk1 的duration = DEFAULT_DURATION 加上生成包的时间.
            //  assert!(tagged_flow.flow().duration <= DEFAULT_DURATION);

            let peer_src = &tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_SRC];
            let peer_dst = &tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_DST];
            assert!(peer_src.tcp_flags == TcpFlags::SYN && peer_dst.tcp_flags == TcpFlags::RST);
        }
    }

    #[test]
    fn syn_fin() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let mut packet0 = _new_meta_packet();
        flow_map.inject_meta_packet(&config, &mut packet0);

        let mut packet1 = _new_meta_packet();
        if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data.flags = TcpFlags::PSH_ACK;
        }
        flow_map.inject_meta_packet(&config, &mut packet1);

        let mut packet2 = _new_meta_packet();
        if let ProtocolData::TcpHeader(tcp_data) = &mut packet2.protocol_data {
            tcp_data.flags = TcpFlags::FIN_ACK;
        }
        packet2.lookup_key.timestamp += Timestamp::from_millis(10);
        _reverse_meta_packet(&mut packet2);
        let flush_timestamp = packet2.lookup_key.timestamp.into();
        flow_map.inject_meta_packet(&config, &mut packet2);

        flow_map.inject_flush_ticker(&config, flush_timestamp);
        flow_map.inject_flush_ticker(&config, flush_timestamp + Duration::from_secs(10));

        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(tagged_flow.flow.close_type, CloseType::ClientHalfClose);

            let peer_src = &tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_SRC];
            let peer_dst = &tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_DST];
            assert!(
                peer_src.tcp_flags == TcpFlags::SYN_ACK | TcpFlags::PSH
                    || peer_dst.tcp_flags == TcpFlags::FIN_ACK
            );
        }
    }

    #[test]
    fn platform_data() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let mut packet1 = _new_meta_packet();
        if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data.seq = 1111;
            tcp_data.ack = 112;
        }
        packet1.lookup_key.timestamp = packet1.lookup_key.timestamp.round_to(TIME_UNIT.into());
        let flush_timestamp = packet1.lookup_key.timestamp.into();
        flow_map.inject_meta_packet(&config, &mut packet1);

        flow_map.inject_flush_ticker(&config, flush_timestamp);
        flow_map.inject_flush_ticker(&config, flush_timestamp + Duration::from_secs(10));
        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(tagged_flow.flow.close_type, CloseType::ClientSynRepeat);
            let peer_src = &tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_SRC];
            assert_eq!(peer_src.l3_epc_id, 1);
        }
    }

    #[test]
    fn handshake_perf() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let mut packet0 = _new_meta_packet();
        let tcp_data0 = if let ProtocolData::TcpHeader(tcp_data) = &mut packet0.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        tcp_data0.flags = TcpFlags::SYN;
        tcp_data0.seq = 111;
        tcp_data0.ack = 0;
        flow_map.inject_meta_packet(&config, &mut packet0);

        let mut packet1 = _new_meta_packet();
        let tcp_data1 = if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        tcp_data1.flags = TcpFlags::SYN_ACK;
        packet1.lookup_key.timestamp += Timestamp::from_millis(10);
        _reverse_meta_packet(&mut packet1);
        let tcp_data1 = if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        tcp_data1.seq = 1111;
        tcp_data1.ack = 112;

        flow_map.inject_meta_packet(&config, &mut packet1);

        let mut packet2 = _new_meta_packet();
        let tcp_data2 = if let ProtocolData::TcpHeader(tcp_data) = &mut packet2.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        tcp_data2.flags = TcpFlags::ACK;
        packet2.lookup_key.timestamp += Timestamp::from_millis(10 * 2);
        tcp_data2.seq = 112;
        tcp_data2.ack = 1112;
        let flush_timestamp = packet2.lookup_key.timestamp.into();
        flow_map.inject_meta_packet(&config, &mut packet2);

        flow_map.inject_flush_ticker(&config, flush_timestamp);
        flow_map.inject_flush_ticker(&config, flush_timestamp + Duration::from_secs(10));

        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(tagged_flow.flow.close_type, CloseType::ForcedReport);
        }
    }

    #[test]
    fn reverse_new_cycle() {
        let (module_config, mut flow_map, _) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let npb_action = NpbAction::new(
            0,
            10,
            IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40)),
            1,
            NpbTunnelType::VxLan,
            TapSide::SRC,
            DirectionType::ALL,
            123,
        );
        let mut policy_data0 = PolicyData::default();
        policy_data0.merge_npb_actions(&vec![npb_action], 10, DirectionType::FORWARD);
        let mut packet0 = _new_meta_packet();
        packet0.policy_data.replace(Arc::new(policy_data0));

        let npb_action = NpbAction::new(
            0,
            11,
            IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40)),
            1,
            NpbTunnelType::VxLan,
            TapSide::SRC,
            DirectionType::ALL,
            123,
        );
        let mut policy_data1 = PolicyData::default();
        policy_data1.merge_npb_actions(&vec![npb_action], 11, DirectionType::FORWARD);
        let mut packet1 = _new_meta_packet();
        if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data.flags = TcpFlags::SYN_ACK;
        }
        _reverse_meta_packet(&mut packet1);
        packet1.lookup_key.direction = PacketDirection::ServerToClient;
        packet1.policy_data.replace(Arc::new(policy_data1));

        let mut node = flow_map.init_flow(&config, &mut packet0);
        node.policy_in_tick.fill(false);
        flow_map.update_flow(&config, &mut node, &mut packet1);

        let tap_side = node.tagged_flow.tag.policy_data[0]
            .as_ref()
            .unwrap()
            .npb_actions[0]
            .tap_side();
        let acl_id = node.tagged_flow.tag.policy_data[1].as_ref().unwrap().acl_id;
        assert_eq!(tap_side, TapSide::SRC);
        assert_eq!(acl_id, 11);
    }

    #[test]
    fn force_report() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let mut packet0 = _new_meta_packet();
        flow_map.inject_meta_packet(&config, &mut packet0);

        let mut packet1 = _new_meta_packet();
        if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data.flags = TcpFlags::SYN_ACK;
        }
        packet1.lookup_key.timestamp += Timestamp::from_millis(10);
        _reverse_meta_packet(&mut packet1);
        flow_map.inject_meta_packet(&config, &mut packet1);

        let mut packet2 = _new_meta_packet();
        if let ProtocolData::TcpHeader(tcp_data) = &mut packet2.protocol_data {
            tcp_data.flags = TcpFlags::ACK;
        }
        packet2.lookup_key.timestamp += Timestamp::from_millis(10);
        let flush_timestamp = packet2.lookup_key.timestamp.into();
        flow_map.inject_meta_packet(&config, &mut packet2);

        flow_map.inject_flush_ticker(&config, flush_timestamp);
        flow_map.inject_flush_ticker(&config, flush_timestamp + Duration::from_secs(10));

        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(tagged_flow.flow.close_type, CloseType::ForcedReport);
            let total_flow = flow_map
                .node_map
                .as_ref()
                .map(|(map, _)| map.len())
                .unwrap_or_default();
            assert_eq!(total_flow, 1);
        }
    }

    #[test]
    fn udp_arp_short_flow() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let mut packet0 = _new_meta_packet();
        packet0.lookup_key.proto = IpProtocol::UDP;
        let flush_timestamp = packet0.lookup_key.timestamp.into();
        flow_map.inject_meta_packet(&config, &mut packet0);

        flow_map.inject_flush_ticker(&config, flush_timestamp);
        flow_map.inject_flush_ticker(&config, flush_timestamp + Duration::from_secs(10));

        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(tagged_flow.flow.close_type, CloseType::Timeout);
        }

        let mut packet1 = _new_meta_packet();
        packet1.lookup_key.eth_type = EthernetType::ARP;
        let flush_timestamp = packet1.lookup_key.timestamp.into();
        flow_map.inject_meta_packet(&config, &mut packet1);

        flow_map.inject_flush_ticker(&config, flush_timestamp);
        flow_map.inject_flush_ticker(&config, flush_timestamp + Duration::from_secs(10));

        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(tagged_flow.flow.close_type, CloseType::Timeout);
        }
    }

    #[test]
    fn port_equal_tor() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtHyperVCompute, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let mut packet0 = _new_meta_packet();
        packet0.lookup_key.tap_type = CaptureNetworkType::Cloud;
        flow_map.inject_meta_packet(&config, &mut packet0);

        let mut packet1 = _new_meta_packet();
        packet1.lookup_key.tap_type = CaptureNetworkType::Cloud;
        if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data.flags = TcpFlags::RST;
        }
        _reverse_meta_packet(&mut packet1);
        let flush_timestamp = packet1.lookup_key.timestamp.into();
        flow_map.inject_meta_packet(&config, &mut packet1);

        flow_map.inject_flush_ticker(&config, flush_timestamp);
        flow_map.inject_flush_ticker(&config, flush_timestamp + Duration::from_secs(10));

        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(
                tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_DST].packet_count,
                1
            );
        }

        let mut packet2 = _new_meta_packet();
        packet2.lookup_key.src_ip = Ipv4Addr::new(192, 168, 1, 2).into();
        packet2.lookup_key.tap_type = CaptureNetworkType::Cloud;
        packet2.tap_port = TapPort(0x1234);
        flow_map.inject_meta_packet(&config, &mut packet2);

        let mut packet3 = _new_meta_packet();
        packet3.lookup_key.src_ip = Ipv4Addr::new(192, 168, 1, 3).into();
        packet3.lookup_key.dst_mac = MacAddr::from([0x21, 0x43, 0x65, 0xaa, 0xaa, 0xaa]);
        packet3.lookup_key.tap_type = CaptureNetworkType::Cloud;
        packet3.tap_port = TapPort(0x1234);
        packet3.lookup_key.l2_end_0 = true;
        packet3.lookup_key.l2_end_1 = false;
        if let ProtocolData::TcpHeader(tcp_data) = &mut packet3.protocol_data {
            tcp_data.flags = TcpFlags::RST;
        }
        _reverse_meta_packet(&mut packet3);
        let flush_timestamp = packet3.lookup_key.timestamp.into();
        flow_map.inject_meta_packet(&config, &mut packet3);

        flow_map.inject_flush_ticker(&config, flush_timestamp);
        flow_map.inject_flush_ticker(&config, flush_timestamp + Duration::from_secs(10));

        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(
                tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_DST].packet_count,
                1
            );
        }
    }

    #[test]
    fn flow_state_machine() {
        let (module_config, mut flow_map, _) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };

        let mut packet0 = _new_meta_packet();
        // test handshake
        let mut node = flow_map.init_flow(&config, &mut packet0);
        let peer_src = &mut node.tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_SRC];
        peer_src.tcp_flags = TcpFlags::SYN;
        flow_map.update_flow_state_machine(
            &config.flow,
            &mut node,
            TcpFlags::SYN_ACK,
            PacketDirection::ServerToClient,
        );
        assert_eq!(node.flow_state, FlowState::Opening2);
        node.flow_state = FlowState::Opening1;
        let peer_src = &mut node.tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_SRC];
        peer_src.tcp_flags = TcpFlags::SYN;
        flow_map.update_flow_state_machine(
            &config.flow,
            &mut node,
            TcpFlags::SYN_ACK,
            PacketDirection::ServerToClient,
        );
        assert_eq!(node.flow_state, FlowState::Opening2);
        flow_map.update_flow_state_machine(
            &config.flow,
            &mut node,
            TcpFlags::ACK,
            PacketDirection::ClientToServer,
        );
        assert_eq!(node.flow_state, FlowState::Established);
        // test fin
        let peer_src = &mut node.tagged_flow.flow.flow_metrics_peers[FLOW_METRICS_PEER_SRC];
        peer_src.tcp_flags = TcpFlags::FIN;
        node.flow_state = FlowState::ClosingTx1;
        flow_map.update_flow_state_machine(
            &config.flow,
            &mut node,
            TcpFlags::ACK,
            PacketDirection::ServerToClient,
        );
        assert_eq!(node.flow_state, FlowState::ClosingTx1);
        flow_map.update_flow_state_machine(
            &config.flow,
            &mut node,
            TcpFlags::FIN_ACK,
            PacketDirection::ServerToClient,
        );
        assert_eq!(node.flow_state, FlowState::ClosingTx2);
        flow_map.update_flow_state_machine(
            &config.flow,
            &mut node,
            TcpFlags::ACK,
            PacketDirection::ClientToServer,
        );
        assert_eq!(node.flow_state, FlowState::Closed);
    }

    #[test]
    fn double_fin_from_server() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        // SYN
        let mut packet0 = _new_meta_packet();
        packet0.lookup_key.timestamp = packet0
            .lookup_key
            .timestamp
            .round_to(STATISTICAL_INTERVAL.into());
        let flush_timestamp = packet0.lookup_key.timestamp;
        flow_map.inject_meta_packet(&config, &mut packet0);

        // SYN|ACK
        let mut packet1 = _new_meta_packet();
        let tcp_data1 = if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        packet1.lookup_key.timestamp = flush_timestamp;
        tcp_data1.flags = TcpFlags::SYN_ACK;
        _reverse_meta_packet(&mut packet1);
        flow_map.inject_meta_packet(&config, &mut packet1);

        // ACK
        let mut packet1 = _new_meta_packet();
        let tcp_data1 = if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        packet1.lookup_key.timestamp = flush_timestamp;
        tcp_data1.flags = TcpFlags::ACK;
        flow_map.inject_meta_packet(&config, &mut packet1);

        // FIN
        let mut packet1 = _new_meta_packet();
        let tcp_data1 = if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        packet1.lookup_key.timestamp = flush_timestamp;
        tcp_data1.flags = TcpFlags::FIN;
        _reverse_meta_packet(&mut packet1);
        flow_map.inject_meta_packet(&config, &mut packet1);
        // FIN
        let mut packet1 = _new_meta_packet();
        let tcp_data1 = if let ProtocolData::TcpHeader(tcp_data) = &mut packet1.protocol_data {
            tcp_data
        } else {
            unreachable!()
        };
        packet1.lookup_key.timestamp = flush_timestamp;
        tcp_data1.flags = TcpFlags::FIN;
        _reverse_meta_packet(&mut packet1);
        flow_map.inject_meta_packet(&config, &mut packet1);

        flow_map.inject_flush_ticker(&config, flush_timestamp.into());
        flow_map.inject_flush_ticker(&config, (flush_timestamp + Duration::from_secs(10)).into());

        if let Ok(tagged_flow) = output_queue_receiver.recv(Some(TIME_UNIT)) {
            assert_eq!(tagged_flow.flow.close_type, CloseType::ClientHalfClose);
        }
    }

    #[test]
    fn l3_l4_payload() {
        let (module_config, mut flow_map, output_queue_receiver) = _new_flow_map_and_receiver(
            AgentType::TtProcess,
            Some(FlowTimeout {
                opening: Timestamp::ZERO,
                established: Timestamp::from_secs(300),
                closing: Timestamp::ZERO,
                established_rst: Timestamp::from_secs(30),
                opening_rst: Timestamp::from_secs(1),
                exception: Timestamp::from_secs(5),
                closed_fin: Timestamp::ZERO,
                single_direction: Timestamp::from_millis(10),
                max: Timestamp::from_secs(300),
                min: Timestamp::ZERO,
            }),
            false,
        );
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };

        let capture = Capture::load_pcap("resources/test/flow_generator/ip-fragment.pcap", None);
        let packets = capture.as_meta_packets();

        let dst_mac = packets[0].lookup_key.dst_mac;
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap();
        for mut packet in packets {
            packet.lookup_key.timestamp = timestamp.into();
            packet.lookup_key.direction = if packet.lookup_key.dst_mac == dst_mac {
                PacketDirection::ClientToServer
            } else {
                PacketDirection::ServerToClient
            };
            flow_map.inject_meta_packet(&config, &mut packet);
        }

        flow_map.inject_flush_ticker(&config, timestamp.add(Duration::from_secs(3)));
        let flow_1 = output_queue_receiver.recv(Some(TIME_UNIT)).unwrap();

        flow_map.inject_flush_ticker(&config, timestamp.add(Duration::from_secs(10)));
        let flow_2 = output_queue_receiver.recv(Some(TIME_UNIT)).unwrap();

        let l3_payload = flow_1.flow.flow_metrics_peers[0].l3_byte_count
            + flow_2.flow.flow_metrics_peers[0].l3_byte_count;
        let l4_payload = flow_1.flow.flow_metrics_peers[0].l4_byte_count
            + flow_2.flow.flow_metrics_peers[0].l4_byte_count;

        assert_eq!(l3_payload, 3008 * 6);
        assert_eq!(l4_payload, 3000 * 6);
    }

    #[test]
    fn ignore_vlan() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let mut packet_0 = _new_meta_packet();
        let mut packet_1 = _new_meta_packet();
        packet_1.vlan = 100;
        flow_map.inject_meta_packet(&config, &mut packet_0);
        flow_map.inject_meta_packet(&config, &mut packet_1);
        flow_map.inject_flush_ticker(
            &config,
            packet_0
                .lookup_key
                .timestamp
                .add(Duration::from_secs(120))
                .into(),
        );
        let tagged_flow = output_queue_receiver.recv(Some(TIME_UNIT)).unwrap();
        assert_eq!(tagged_flow.flow.flow_metrics_peers[0].packet_count, 1);
        let tagged_flow = output_queue_receiver.recv(Some(TIME_UNIT)).unwrap();
        assert_eq!(tagged_flow.flow.flow_metrics_peers[0].packet_count, 1);

        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, true);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };
        let mut packet_0 = _new_meta_packet();
        let mut packet_1 = _new_meta_packet();
        packet_1.vlan = 100;
        flow_map.inject_meta_packet(&config, &mut packet_0);
        flow_map.inject_meta_packet(&config, &mut packet_1);
        flow_map.inject_flush_ticker(
            &config,
            packet_0
                .lookup_key
                .timestamp
                .add(Duration::from_secs(120))
                .into(),
        );
        let tagged_flow = output_queue_receiver.recv(Some(TIME_UNIT)).unwrap();
        assert_eq!(tagged_flow.flow.flow_metrics_peers[0].packet_count, 2);
    }

    #[test]
    fn tcp_perf() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };

        let capture = Capture::load_pcap("resources/test/flow_generator/http.pcap", None);
        let packets = capture.as_meta_packets();

        flow_map.reset_start_time(packets[0].lookup_key.timestamp.into());
        let dst_mac = packets[0].lookup_key.dst_mac;
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap();
        for mut packet in packets {
            packet.lookup_key.timestamp = Duration::new(
                timestamp.as_secs(),
                Duration::from(packet.lookup_key.timestamp).subsec_nanos(),
            )
            .into();
            packet.lookup_key.direction = if packet.lookup_key.dst_mac == dst_mac {
                PacketDirection::ClientToServer
            } else {
                PacketDirection::ServerToClient
            };
            flow_map.inject_meta_packet(&config, &mut packet);
        }

        flow_map.inject_flush_ticker(&config, timestamp.add(Duration::from_secs(120)));

        let tagged_flow = output_queue_receiver.recv(Some(TIME_UNIT)).unwrap();
        let perf_stats = &tagged_flow.flow.flow_perf_stats.as_ref().unwrap().tcp;
        assert_eq!(perf_stats.rtt_client_max, 114);
        assert_eq!(perf_stats.rtt_server_max, 44);
        assert_eq!(perf_stats.srt_max, 12);
    }

    #[test]
    fn tcp_syn_ack_zerowin() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };

        let capture = Capture::load_pcap(
            "resources/test/flow_generator/tcp-syn-ack-zerowin.pcap",
            None,
        );
        let packets = capture.as_meta_packets();

        flow_map.reset_start_time(packets[0].lookup_key.timestamp.into());
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap();
        for mut packet in packets {
            packet.lookup_key.timestamp = timestamp.into();
            flow_map.inject_meta_packet(&config, &mut packet);
        }

        flow_map.inject_flush_ticker(&config, timestamp.add(Duration::from_secs(120)));

        let tagged_flow = output_queue_receiver.recv(Some(TIME_UNIT)).unwrap();
        let perf_stats = &tagged_flow.flow.flow_perf_stats.as_ref().unwrap().tcp;
        assert_eq!(perf_stats.counts_peers[0].zero_win_count, 0);
        assert_eq!(perf_stats.counts_peers[1].zero_win_count, 1);
    }

    #[test]
    fn sort_nodes_by_timeout() {
        let mut nodes = vec![
            Box::new(FlowNode {
                timestamp_key: 120,
                ..Default::default()
            }),
            Box::new(FlowNode {
                timestamp_key: 60,
                recent_time: Timestamp::from_secs(40),
                ..Default::default()
            }),
            Box::new(FlowNode {
                timestamp_key: 60,
                recent_time: Timestamp::from_secs(10),
                ..Default::default()
            }),
            Box::new(FlowNode {
                timestamp_key: 60,
                recent_time: Timestamp::from_secs(60),
                ..Default::default()
            }),
            Box::new(FlowNode {
                timestamp_key: 180,
                ..Default::default()
            }),
            Box::new(FlowNode {
                timestamp_key: 60,
                recent_time: Timestamp::from_secs(60),
                ..Default::default()
            }),
            Box::new(FlowNode {
                timestamp_key: 120,
                ..Default::default()
            }),
            Box::new(FlowNode {
                timestamp_key: 60,
                recent_time: Timestamp::from_secs(20),
                ..Default::default()
            }),
            Box::new(FlowNode {
                timestamp_key: 60,
                recent_time: Timestamp::from_secs(60),
                ..Default::default()
            }),
        ];
        let index = FlowMap::sort_nodes_by_timeout(&mut nodes, Duration::from_secs(50), 70);
        assert_eq!(
            6,
            index,
            "{}\n{:?}",
            index,
            nodes
                .iter()
                .map(|n| (n.timestamp_key, n.recent_time))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_handshake_retrans() {
        let (module_config, mut flow_map, output_queue_receiver) =
            _new_flow_map_and_receiver(AgentType::TtProcess, None, false);
        let config = Config {
            flow: &module_config.flow,
            log_parser: &module_config.log_parser,
            collector: &module_config.collector,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: None,
        };

        let capture =
            Capture::load_pcap("resources/test/flow_generator/handshake-retrans.pcap", None);
        let packets = capture.as_meta_packets();

        flow_map.reset_start_time(packets[0].lookup_key.timestamp.into());
        let dst_mac = packets[0].lookup_key.dst_mac;
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap();
        for mut packet in packets {
            packet.lookup_key.timestamp = Duration::new(
                timestamp.as_secs(),
                Duration::from(packet.lookup_key.timestamp).subsec_nanos(),
            )
            .into();
            packet.lookup_key.direction = if packet.lookup_key.dst_mac == dst_mac {
                PacketDirection::ClientToServer
            } else {
                PacketDirection::ServerToClient
            };
            flow_map.inject_meta_packet(&config, &mut packet);
        }

        flow_map.inject_flush_ticker(&config, timestamp.add(Duration::from_secs(120)));

        let tagged_flow = output_queue_receiver.recv(Some(TIME_UNIT)).unwrap();
        let perf_stats = &tagged_flow.flow.flow_perf_stats.as_ref().unwrap().tcp;
        assert_eq!(perf_stats.rtt_client_max, 1567);
        assert_eq!(perf_stats.rtt_client_sum, 2822);
        assert_eq!(perf_stats.rtt_client_count, 2);
        assert_eq!(perf_stats.rtt_server_max, 1886);
        assert_eq!(perf_stats.rtt_server_sum, 2829);
        assert_eq!(perf_stats.rtt_server_count, 2);
        assert_eq!(perf_stats.rtt, 2510);
    }
}
