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

use std::borrow::Cow;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, u32};

use arc_swap::{access::Map, ArcSwap};
use base64::{prelude::BASE64_STANDARD, Engine};
use bytesize::ByteSize;
use flexi_logger::{
    writers::FileLogWriter, Age, Cleanup, Criterion, FileSpec, FlexiLoggerError, LoggerHandle,
    Naming,
};
use http2::get_expected_headers;
use log::{debug, error, info, warn, Level};
#[cfg(any(target_os = "linux", target_os = "android"))]
use nix::{
    sched::{sched_setaffinity, CpuSet},
    unistd::Pid,
};
use sysinfo::SystemExt;
#[cfg(any(target_os = "linux", target_os = "android"))]
use sysinfo::{CpuRefreshKind, RefreshKind, System};
use tokio::runtime::Runtime;

use super::config::ProcessorsFlowLogTunning;
#[cfg(any(target_os = "linux", target_os = "android"))]
use super::config::{Ebpf, EbpfFileIoEvent, ProcessMatcher, SymbolTable};
use super::{
    config::{
        ApiResources, Config, DpdkSource, ExtraLogFields, ExtraLogFieldsInfo, HttpEndpoint,
        HttpEndpointMatchRule, OracleConfig, PcapStream, PortConfig, TagFilterOperator, UserConfig,
    },
    ConfigError, KubernetesPollerType,
};
use crate::flow_generator::protocol_logs::decode_new_rpc_trace_context_with_type;
use crate::rpc::Session;
use crate::{
    common::{
        decapsulate::TunnelTypeBitmap, enums::CaptureNetworkType, l7_protocol_log::L7ProtocolBitmap,
    },
    exception::ExceptionHandler,
    flow_generator::{protocol_logs::SOFA_NEW_RPC_TRACE_CTX_KEY, FlowTimeout, TcpTimeout},
    handler::PacketHandlerBuilder,
    metric::document::TapSide,
    trident::{AgentComponents, RunningMode},
    utils::environment::{free_memory_check, running_in_container},
};
#[cfg(any(target_os = "linux", target_os = "android"))]
use crate::{
    dispatcher::recv_engine::af_packet::OptTpacketVersion,
    ebpf::CAP_LEN_MAX,
    utils::environment::{
        get_container_resource_limits, get_ctrl_ip_and_mac, is_tt_workload,
        set_container_resource_limit,
    },
};
#[cfg(target_os = "linux")]
use crate::{
    platform::{kubernetes::Poller, ApiWatcher, GenericPoller},
    utils::environment::is_tt_pod,
};
use crate::{trident::AgentId, utils::cgroups::is_kernel_available_for_cgroups};

use public::bitmap::Bitmap;
use public::l7_protocol::L7Protocol;
use public::proto::agent::{self, AgentType, PacketCaptureType};
use public::utils::net::MacAddr;

const MB: u64 = 1048576;

type Access<C> = Map<Arc<ArcSwap<ModuleConfig>>, ModuleConfig, fn(&ModuleConfig) -> &C>;

pub type CollectorAccess = Access<CollectorConfig>;

pub type EnvironmentAccess = Access<EnvironmentConfig>;

pub type SenderAccess = Access<SenderConfig>;

pub type NpbAccess = Access<NpbConfig>;

pub type PlatformAccess = Access<PlatformConfig>;

pub type HandlerAccess = Access<HandlerConfig>;

pub type DispatcherAccess = Access<DispatcherConfig>;

pub type DiagnoseAccess = Access<DiagnoseConfig>;

pub type LogAccess = Access<LogConfig>;

pub type FlowAccess = Access<FlowConfig>;

pub type LogParserAccess = Access<LogParserConfig>;

pub type PcapAccess = Access<PcapStream>;

pub type DebugAccess = Access<DebugConfig>;

pub type SynchronizerAccess = Access<SynchronizerConfig>;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub type EbpfAccess = Access<EbpfConfig>;

pub type MetricServerAccess = Access<MetricServerConfig>;

pub type PortAccess = Access<PortConfig>;

#[derive(Clone, PartialEq, Eq)]
pub struct CollectorConfig {
    pub enabled: bool,
    pub inactive_server_port_aggregation: bool,
    pub inactive_ip_aggregation: bool,
    pub vtap_flow_1s_enabled: bool,
    pub l4_log_collect_nps_threshold: u64,
    pub l4_log_store_tap_types: [bool; 256],
    pub l4_log_ignore_tap_sides: [bool; TapSide::MAX as usize + 1],
    pub l7_metrics_enabled: bool,
    pub agent_type: AgentType,
    pub agent_id: u16,
    pub cloud_gateway_traffic: bool,
    pub packet_delay: Duration,
    pub npm_metrics_concurrent: bool,
}

impl fmt::Debug for CollectorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectorConfig")
            .field("enabled", &self.enabled)
            .field(
                "inactive_server_port_aggregation",
                &self.inactive_server_port_aggregation,
            )
            .field("inactive_ip_aggregation", &self.inactive_ip_aggregation)
            .field("vtap_flow_1s_enabled", &self.vtap_flow_1s_enabled)
            .field(
                "l4_log_store_tap_types",
                &self
                    .l4_log_store_tap_types
                    .iter()
                    .enumerate()
                    .filter(|&(_, b)| *b)
                    .collect::<Vec<_>>(),
            )
            .field(
                "l4_log_ignore_tap_sides",
                &self
                    .l4_log_ignore_tap_sides
                    .iter()
                    .enumerate()
                    .filter_map(|(i, b)| {
                        if *b {
                            TapSide::try_from(i as u8).ok()
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
            )
            .field(
                "l4_log_collect_nps_threshold",
                &self.l4_log_collect_nps_threshold,
            )
            .field("l7_metrics_enabled", &self.l7_metrics_enabled)
            .field("agent_type", &self.agent_type)
            .field("agent_id", &self.agent_id)
            .field("cloud_gateway_traffic", &self.cloud_gateway_traffic)
            .field("packet_delay", &self.packet_delay)
            .field("npm_metrics_concurrent", &self.npm_metrics_concurrent)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EnvironmentConfig {
    pub max_memory: u64,
    pub max_millicpus: u32,
    pub process_threshold: u32,
    pub thread_threshold: u32,
    pub sys_memory_limit: u32,
    pub sys_memory_metric: agent::SysMemoryMetric,
    pub log_file_size: u32,
    pub capture_mode: PacketCaptureType,
    pub guard_interval: Duration,
    pub system_load_circuit_breaker_threshold: f32,
    pub system_load_circuit_breaker_recover: f32,
    pub system_load_circuit_breaker_metric: agent::SystemLoadMetric,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SenderConfig {
    pub dest_ip: String,
    pub agent_id: u16,
    pub team_id: u32,
    pub organize_id: u32,
    pub dest_port: u16,
    pub npb_port: u16,
    pub vxlan_flags: u8,
    pub npb_enable_qos_bypass: bool,
    pub npb_vlan: u16,
    pub npb_vlan_mode: agent::VlanMode,
    pub npb_dedup_enabled: bool,
    pub npb_bps_threshold: u64,
    pub npb_socket_type: agent::SocketType,
    pub multiple_sockets_to_ingester: bool,
    pub collector_socket_type: agent::SocketType,
    pub standalone_data_file_size: u32,
    pub standalone_data_file_dir: String,
    pub server_tx_bandwidth_threshold: u64,
    pub bandwidth_probe_interval: Duration,
    pub enabled: bool,
}

impl Default for SenderConfig {
    fn default() -> Self {
        let module_config = ModuleConfig::default();
        return module_config.sender.clone();
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NpbConfig {
    pub underlay_is_ipv6: bool,
    pub vxlan_flags: u8,
    pub npb_port: u16,
    pub dedup_enabled: bool,
    pub enable_qos_bypass: bool,
    pub output_vlan: u16,
    pub mtu: u32,
    pub vlan_mode: agent::VlanMode,
    pub socket_type: agent::SocketType,
    pub ignore_overlay_vlan: bool,
    pub queue_size: usize,
}

impl Default for NpbConfig {
    fn default() -> Self {
        let module_config = ModuleConfig::default();
        return module_config.npb.clone();
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OsProcScanConfig {
    pub os_proc_root: String,
    pub os_proc_socket_sync_interval: u32, // for sec
    pub os_proc_socket_min_lifetime: u32,  // for sec
    pub os_app_tag_exec_user: String,
    pub os_app_tag_exec: Vec<String>,
    // whether to sync os socket and proc info
    // only make sense when process_info_enabled() == true
    pub os_proc_sync_enabled: bool,
    // sync os socket and proc info only when the process has been tagged.
    pub os_proc_sync_tagged_only: bool,
}
#[cfg(target_os = "windows")]
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OsProcScanConfig;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PlatformConfig {
    pub sync_interval: Duration,
    pub kubernetes_cluster_id: String,
    pub libvirt_xml_path: PathBuf,
    pub kubernetes_poller_type: KubernetesPollerType,
    pub agent_id: u16,
    pub enabled: bool,
    pub agent_type: AgentType,
    pub epc_id: u32,
    pub kubernetes_api_enabled: bool,
    pub kubernetes_api_list_limit: u32,
    pub kubernetes_api_list_interval: Duration,
    pub kubernetes_resources: Vec<ApiResources>,
    pub max_memory: u64,
    pub namespace: Option<String>,
    pub thread_threshold: u32,
    pub capture_mode: PacketCaptureType,
    pub os_proc_scan_conf: OsProcScanConfig,
    pub agent_enabled: bool,
    #[cfg(target_os = "linux")]
    pub extra_netns_regex: String,
}

#[derive(Clone, PartialEq, Debug, Eq)]
pub struct HandlerConfig {
    pub npb_dedup_enabled: bool,
    pub agent_type: AgentType,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DispatcherConfig {
    pub global_pps_threshold: u64,
    pub capture_packet_size: u32,
    pub l7_log_packet_size: u32,
    pub tunnel_type_bitmap: TunnelTypeBitmap,
    pub tunnel_type_trim_bitmap: TunnelTypeBitmap,
    pub agent_type: AgentType,
    pub agent_id: u16,
    pub capture_socket_type: agent::CaptureSocketType,
    #[cfg(target_os = "linux")]
    pub extra_netns_regex: String,
    pub tap_interface_regex: String,
    pub if_mac_source: agent::IfMacSource,
    pub analyzer_ip: String,
    pub analyzer_port: u16,
    pub proxy_controller_ip: String,
    pub proxy_controller_port: u16,
    pub capture_bpf: String,
    pub max_memory: u64,
    pub af_packet_blocks: usize,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub af_packet_version: OptTpacketVersion,
    pub capture_mode: PacketCaptureType,
    pub region_id: u32,
    pub pod_cluster_id: u32,
    pub enabled: bool,
    pub npb_dedup_enabled: bool,
    pub dpdk_source: DpdkSource,
    pub dispatcher_queue: bool,
    pub bond_group: Vec<String>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub cpu_set: CpuSet,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LogConfig {
    pub log_level: Level,
    pub log_threshold: u32,
    pub log_retention: u32,
    pub rsyslog_enabled: bool,
    pub host: String,
}

#[derive(Clone)]
pub struct PluginConfig {
    pub last_updated: u32,
    pub digest: u64, // for change detection
    pub names: Vec<(String, agent::PluginType)>,
    // name, data
    pub wasm_plugins: Vec<(String, Vec<u8>)>,
    pub so_plugins: Vec<(String, Vec<u8>)>,
}

impl PartialEq for PluginConfig {
    fn eq(&self, other: &PluginConfig) -> bool {
        self.last_updated == other.last_updated
            && self.digest == other.digest
            && self.names == other.names
    }
}

impl Eq for PluginConfig {}

impl fmt::Debug for PluginConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginConfig")
            .field("last_updated", &self.last_updated)
            .field("digest", &self.digest)
            .field("names", &self.names)
            .finish()
    }
}

impl PluginConfig {
    fn fill_plugin_prog_from_server(
        &mut self,
        rt: &Runtime,
        session: &Session,
        agent_id: &AgentId,
    ) {
        self.wasm_plugins.clear();
        self.so_plugins.clear();

        rt.block_on(async {
            for (name, ptype) in self.names.iter() {
                log::trace!("get {:?} plugin {}", ptype, name);
                match session.grpc_get_plugin(name, *ptype, agent_id).await {
                    Ok(prog) => match ptype {
                        agent::PluginType::Wasm => self.wasm_plugins.push((name.clone(), prog)),
                        #[cfg(any(target_os = "linux", target_os = "android"))]
                        agent::PluginType::So => self.so_plugins.push((name.clone(), prog)),
                        #[cfg(any(target_os = "windows"))]
                        _ => (),
                    },
                    Err(err) => {
                        warn!("get {:?} plugin {} fail: {}", ptype, name, err);
                        continue;
                    }
                }
            }
        });

        info!(
            "{} wasm and {} so plugins pulled from server",
            self.wasm_plugins.len(),
            self.so_plugins.len()
        );
    }
}

fn generate_tap_types_array(types: &[i16]) -> [bool; 256] {
    let mut tap_types = [false; 256];
    for &t in types {
        if t == -1 {
            return [false; 256];
        } else if t < 0 || (t as u16) >= u16::from(CaptureNetworkType::Max) {
            warn!("invalid tap type: {}", t);
        } else {
            tap_types[t as usize] = true;
        }
    }
    tap_types
}

#[derive(Clone, PartialEq, Eq)]
pub struct FlowConfig {
    pub agent_id: u16,
    pub agent_type: AgentType,
    pub cloud_gateway_traffic: bool,
    pub collector_enabled: bool,
    pub l7_log_tap_types: [bool; 256],
    pub capture_mode: PacketCaptureType,

    pub capacity: u32,
    pub hash_slots: u32,
    pub packet_delay: Duration,
    pub flush_interval: Duration,
    pub flow_timeout: FlowTimeout,
    pub ignore_tor_mac: bool,
    pub ignore_l2_end: bool,
    pub ignore_idc_vlan: bool,

    pub memory_pool_size: usize,

    pub l7_metrics_enabled: bool,
    pub app_proto_log_enabled: bool,
    pub l4_performance_enabled: bool,
    pub l7_log_packet_size: u32,

    pub l7_protocol_inference_max_fail_count: usize,
    pub l7_protocol_inference_ttl: usize,

    // Enterprise Edition Feature: packet-sequence
    pub packet_sequence_flag: u8,
    pub packet_sequence_block_size: usize,

    pub l7_protocol_enabled_bitmap: L7ProtocolBitmap,

    // vec<protocolName, port bitmap>
    pub l7_protocol_parse_port_bitmap: Arc<Vec<(String, Bitmap)>>,

    pub plugins: PluginConfig,

    pub rrt_tcp_timeout: usize, //micro sec
    pub rrt_udp_timeout: usize, //micro sec

    pub batched_buffer_size_limit: usize,

    pub oracle_parse_conf: OracleConfig,

    pub obfuscate_enabled_protocols: L7ProtocolBitmap,
    pub server_ports: Vec<u16>,
    pub consistent_timestamp_in_l7_metrics: bool,

    pub packet_segmentation_reassembly: HashSet<u16>,
}

impl From<&UserConfig> for FlowConfig {
    fn from(conf: &UserConfig) -> Self {
        FlowConfig {
            agent_id: conf.global.common.agent_id as u16,
            agent_type: conf.global.common.agent_type,
            capture_mode: conf.inputs.cbpf.common.capture_mode,
            cloud_gateway_traffic: conf
                .inputs
                .cbpf
                .physical_mirror
                .private_cloud_gateway_traffic,
            collector_enabled: conf.outputs.flow_metrics.enabled,
            l7_log_tap_types: generate_tap_types_array(
                &conf.outputs.flow_log.filters.l7_capture_network_types,
            ),
            capacity: conf.processors.flow_log.tunning.concurrent_flow_limit,
            hash_slots: conf.processors.flow_log.tunning.flow_map_hash_slots,
            packet_delay: conf
                .processors
                .flow_log
                .time_window
                .max_tolerable_packet_delay,
            flush_interval: conf.processors.packet.pcap_stream.flush_interval,
            flow_timeout: FlowTimeout::from(TcpTimeout {
                established: conf
                    .processors
                    .flow_log
                    .conntrack
                    .timeouts
                    .established
                    .into(),
                closing_rst: conf
                    .processors
                    .flow_log
                    .conntrack
                    .timeouts
                    .closing_rst
                    .into(),
                others: conf.processors.flow_log.conntrack.timeouts.others.into(),
                opening_rst: conf
                    .processors
                    .flow_log
                    .conntrack
                    .timeouts
                    .opening_rst
                    .into(),
            }),
            ignore_tor_mac: conf
                .processors
                .flow_log
                .conntrack
                .flow_generation
                .cloud_traffic_ignore_mac,
            ignore_l2_end: conf
                .processors
                .flow_log
                .conntrack
                .flow_generation
                .ignore_l2_end,
            ignore_idc_vlan: conf
                .processors
                .flow_log
                .conntrack
                .flow_generation
                .idc_traffic_ignore_vlan,
            memory_pool_size: conf.processors.flow_log.tunning.memory_pool_size,
            l7_metrics_enabled: conf.outputs.flow_metrics.filters.apm_metrics,
            app_proto_log_enabled: !conf
                .outputs
                .flow_log
                .filters
                .l7_capture_network_types
                .is_empty(),
            l4_performance_enabled: conf.outputs.flow_metrics.filters.npm_metrics,
            l7_log_packet_size: conf.processors.request_log.tunning.payload_truncation,
            l7_protocol_inference_max_fail_count: conf
                .processors
                .request_log
                .application_protocol_inference
                .inference_max_retries,
            l7_protocol_inference_ttl: conf
                .processors
                .request_log
                .application_protocol_inference
                .inference_result_ttl
                .as_secs() as usize,
            packet_sequence_flag: conf.processors.packet.tcp_header.header_fields_flag, // Enterprise Edition Feature: packet-sequence
            packet_sequence_block_size: conf.processors.packet.tcp_header.block_size, // Enterprise Edition Feature: packet-sequence
            l7_protocol_enabled_bitmap: L7ProtocolBitmap::from(
                conf.processors
                    .request_log
                    .application_protocol_inference
                    .enabled_protocols
                    .as_slice(),
            ),
            l7_protocol_parse_port_bitmap: Arc::new(conf.get_protocol_port_parse_bitmap()),
            plugins: PluginConfig {
                last_updated: conf.plugins.update_time.as_secs() as u32,
                digest: {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    if !conf.plugins.so_plugins.is_empty() || !conf.plugins.wasm_plugins.is_empty()
                    {
                        conf.plugins.update_time.hash(&mut hasher);
                        for plugin in conf.plugins.wasm_plugins.iter() {
                            plugin.hash(&mut hasher);
                            agent::PluginType::Wasm.hash(&mut hasher);
                        }
                        for plugin in conf.plugins.so_plugins.iter() {
                            plugin.hash(&mut hasher);
                            agent::PluginType::So.hash(&mut hasher);
                        }
                    }
                    hasher.finish()
                },
                names: {
                    let mut plugins = vec![];
                    plugins.extend(
                        conf.plugins
                            .wasm_plugins
                            .iter()
                            .map(|p| (p.clone(), agent::PluginType::Wasm)),
                    );
                    plugins.extend(
                        conf.plugins
                            .so_plugins
                            .iter()
                            .map(|p| (p.clone(), agent::PluginType::So)),
                    );
                    plugins
                },
                wasm_plugins: vec![],
                so_plugins: vec![],
            },
            rrt_tcp_timeout: conf
                .processors
                .request_log
                .timeouts
                .tcp_request_timeout
                .as_micros() as usize,
            rrt_udp_timeout: conf
                .processors
                .request_log
                .timeouts
                .udp_request_timeout
                .as_micros() as usize,
            batched_buffer_size_limit: conf.processors.flow_log.tunning.max_batched_buffer_size,
            oracle_parse_conf: conf
                .processors
                .request_log
                .application_protocol_inference
                .protocol_special_config
                .oracle
                .clone(),
            obfuscate_enabled_protocols: L7ProtocolBitmap::from(
                conf.processors
                    .request_log
                    .tag_extraction
                    .obfuscate_protocols
                    .as_slice(),
            ),
            server_ports: conf
                .processors
                .flow_log
                .conntrack
                .flow_generation
                .server_ports
                .clone(),
            consistent_timestamp_in_l7_metrics: conf
                .processors
                .request_log
                .tunning
                .consistent_timestamp_in_l7_metrics,
            packet_segmentation_reassembly: HashSet::from_iter(
                conf.inputs
                    .cbpf
                    .preprocess
                    .packet_segmentation_reassembly
                    .clone()
                    .into_iter(),
            ),
        }
    }
}

impl FlowConfig {
    pub fn need_to_reassemble(&self, src_port: u16, dst_port: u16) -> bool {
        self.packet_segmentation_reassembly.contains(&src_port)
            || self.packet_segmentation_reassembly.contains(&dst_port)
    }

    pub fn flow_capacity(&self) -> u32 {
        let default_capacity = ProcessorsFlowLogTunning::default().concurrent_flow_limit;
        match self.capture_mode {
            PacketCaptureType::Analyzer if self.capacity <= default_capacity => u32::MAX,
            _ => self.capacity,
        }
    }
}

impl fmt::Debug for FlowConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlowConfig")
            .field("agent_id", &self.agent_id)
            .field("agent_type", &self.agent_type)
            .field("cloud_gateway_traffic", &self.cloud_gateway_traffic)
            .field("collector_enabled", &self.collector_enabled)
            .field(
                "l7_log_tap_types",
                &self
                    .l7_log_tap_types
                    .iter()
                    .enumerate()
                    .filter(|&(_, b)| *b)
                    .collect::<Vec<_>>(),
            )
            .field("capacity", &self.capacity)
            .field("flow_capacity", &self.flow_capacity())
            .field("hash_slots", &self.hash_slots)
            .field("packet_delay", &self.packet_delay)
            .field("flush_interval", &self.flush_interval)
            .field("flow_timeout", &self.flow_timeout)
            .field("ignore_tor_mac", &self.ignore_tor_mac)
            .field("ignore_l2_end", &self.ignore_l2_end)
            .field("l7_metrics_enabled", &self.l7_metrics_enabled)
            .field("app_proto_log_enabled", &self.app_proto_log_enabled)
            .field("l4_performance_enabled", &self.l4_performance_enabled)
            .field("l7_log_packet_size", &self.l7_log_packet_size)
            .field(
                "l7_protocol_inference_max_fail_count",
                &self.l7_protocol_inference_max_fail_count,
            )
            .field("l7_protocol_inference_ttl", &self.l7_protocol_inference_ttl)
            .field("packet_sequence_flag", &self.packet_sequence_flag)
            .field(
                "packet_sequence_block_size",
                &self.packet_sequence_block_size,
            )
            .field(
                "l7_protocol_enabled_bitmap",
                &self.l7_protocol_enabled_bitmap,
            )
            // FIXME: this field is too long to log
            // .field("l7_protocol_parse_port_bitmap", &self.l7_protocol_parse_port_bitmap)
            .field("plugins", &self.plugins)
            .field("server_ports", &self.server_ports)
            .field(
                "packet_segmentation_reassembly",
                &self.packet_segmentation_reassembly,
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
struct TrieNode {
    children: HashMap<char, Box<TrieNode>>,
    keep_segments: Option<usize>,
}

impl TrieNode {
    fn new() -> Self {
        TrieNode {
            children: HashMap::new(),
            keep_segments: None,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpEndpointTrie {
    root: TrieNode,
}

impl HttpEndpointTrie {
    pub fn new() -> Self {
        Self {
            root: TrieNode::new(),
        }
    }

    pub fn insert(&mut self, rule: &HttpEndpointMatchRule) {
        let mut node = &mut self.root;
        for ch in rule.url_prefix.chars() {
            node = node
                .children
                .entry(ch)
                .or_insert_with(|| Box::new(TrieNode::new()));
        }
        node.keep_segments = Some(rule.keep_segments);
    }

    pub fn find_matching_rule(&self, input: &str) -> usize {
        const DEFAULT_KEEP_SEGMENTS: usize = 2;
        let mut node = &self.root;
        let mut keep_segments = node.keep_segments.unwrap_or(DEFAULT_KEEP_SEGMENTS);
        let has_rules = node.keep_segments.is_some() || !node.children.is_empty(); // if no rules are set, keep_segments defaults to DEFAULT_KEEP_SEGMENTS: 2
        let mut matched = node.keep_segments.is_some(); // if it has a rule, and the prefix is "", any path is matched
        for c in input.chars() {
            if let Some(child) = node.children.get(&c) {
                keep_segments = child.keep_segments.unwrap_or(keep_segments);
                if !matched {
                    matched = child.keep_segments.is_some(); // if the child is a leaf, then matched
                }
                node = child.as_ref();
            } else {
                break;
            }
        }
        if !matched && has_rules {
            0
        } else {
            keep_segments
        }
    }
}

impl From<&HttpEndpoint> for HttpEndpointTrie {
    fn from(v: &HttpEndpoint) -> Self {
        let mut t = Self::new();
        v.match_rules
            .iter()
            .filter(|r| r.keep_segments > 0)
            .for_each(|r| t.insert(r));
        t
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Operator {
    Equal,
    Prefix,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlacklistTrieNode {
    children: HashMap<char, Box<BlacklistTrieNode>>,
    operator: Option<Operator>,
}

impl BlacklistTrieNode {
    pub fn is_on_blacklist(&self, input: &str) -> bool {
        if input.is_empty() {
            return false;
        }
        let mut node = self;
        for c in input.chars() {
            node = match node.children.get(&c) {
                Some(child) => child,
                None => return false,
            };
            if let Some(op) = &node.operator {
                if op == &Operator::Prefix {
                    return true;
                }
            }
        }
        // If we've reached the end of the input and the last node has an operator,
        // it must be because we matched a complete word, not a prefix.
        if let Some(o) = node.operator {
            o == Operator::Equal
        } else {
            false
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlacklistTrie {
    pub endpoint: BlacklistTrieNode,
    pub request_type: BlacklistTrieNode,
    pub request_domain: BlacklistTrieNode,
    pub request_resource: BlacklistTrieNode,
}

impl BlacklistTrie {
    // Currently, the following field names are supported:
    const ENDPOINT: &'static str = "endpoint";
    const REQUEST_TYPE: &'static str = "request_type";
    const REQUEST_DOMAIN: &'static str = "request_domain";
    const REQUEST_RESOURCE: &'static str = "request_resource";

    // Currently, the following matching operations are supported:
    const EQUAL: &'static str = "equal";
    const PREFIX: &'static str = "prefix";

    pub fn new(blacklists: &Vec<TagFilterOperator>) -> Option<BlacklistTrie> {
        if blacklists.is_empty() {
            return None;
        }

        let mut b = BlacklistTrie::default();
        for i in blacklists.iter() {
            b.insert(i);
        }
        Some(b)
    }

    pub fn insert(&mut self, rule: &TagFilterOperator) {
        let mut node = match rule.name.to_ascii_lowercase().as_str() {
            Self::ENDPOINT => &mut self.endpoint,
            Self::REQUEST_TYPE => &mut self.request_type,
            Self::REQUEST_DOMAIN => &mut self.request_domain,
            Self::REQUEST_RESOURCE => &mut self.request_resource,
            _ => {
                warn!("Unsupported field_name: {}, only supports endpoint, request_type, request_domain, request_resource.", rule.name.as_str());
                return;
            }
        };

        let operator = match rule.operator.to_ascii_lowercase().as_str() {
            Self::EQUAL => Operator::Equal,
            Self::PREFIX => Operator::Prefix,
            _ => {
                warn!(
                    "Unsupported operator: {}, only supports equal, prefix.",
                    rule.operator.as_str()
                );
                return;
            }
        };

        for ch in rule.value.chars() {
            node = node
                .children
                .entry(ch)
                .or_insert_with(|| Box::new(BlacklistTrieNode::default()));
        }
        node.operator = Some(operator);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DnsNxdomainTrieNode {
    children: HashMap<char, Box<DnsNxdomainTrieNode>>,
    unconcerned: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DnsNxdomainTrie {
    root: DnsNxdomainTrieNode,
}

impl DnsNxdomainTrie {
    pub fn insert(&mut self, rule: &String) {
        let mut node = &mut self.root;
        // the reversal is because what is matched is the suffix of the domain name
        for ch in rule.chars().rev() {
            node = node
                .children
                .entry(ch)
                .or_insert_with(|| Box::new(DnsNxdomainTrieNode::default()));
        }
        node.unconcerned = true;
    }

    pub fn is_unconcerned(&self, input: &str) -> bool {
        if input.is_empty() {
            return false;
        }
        let mut node = &self.root;
        // the reversal is because what is matched is the suffix of the domain name
        for c in input.chars().rev() {
            match node.children.get(&c) {
                Some(child) => {
                    if child.unconcerned {
                        return true;
                    }
                    node = child.as_ref();
                }
                None => {
                    break;
                }
            }
        }
        false
    }
}

impl From<&Vec<String>> for DnsNxdomainTrie {
    fn from(v: &Vec<String>) -> Self {
        let mut t = Self::default();
        v.iter().for_each(|r| t.insert(r));
        t
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LogParserConfig {
    pub l7_log_collect_nps_threshold: u64,
    pub l7_log_session_aggr_timeout: Duration,
    pub l7_log_session_slot_capacity: usize,
    pub l7_log_dynamic: L7LogDynamicConfig,
    pub l7_log_ignore_tap_sides: [bool; TapSide::MAX as usize + 1],
    pub http_endpoint_disabled: bool,
    pub http_endpoint_trie: HttpEndpointTrie,
    pub obfuscate_enabled_protocols: L7ProtocolBitmap,
    pub l7_log_blacklist: HashMap<String, Vec<TagFilterOperator>>,
    pub l7_log_blacklist_trie: HashMap<L7Protocol, BlacklistTrie>,
    pub unconcerned_dns_nxdomain_response_suffixes: Vec<String>,
    pub unconcerned_dns_nxdomain_trie: DnsNxdomainTrie,
}

impl Default for LogParserConfig {
    fn default() -> Self {
        Self {
            l7_log_collect_nps_threshold: 0,
            l7_log_session_aggr_timeout: Duration::ZERO,
            l7_log_session_slot_capacity: 1024,
            l7_log_dynamic: L7LogDynamicConfig::default(),
            l7_log_ignore_tap_sides: [false; TapSide::MAX as usize + 1],
            http_endpoint_disabled: false,
            http_endpoint_trie: HttpEndpointTrie::new(),
            obfuscate_enabled_protocols: L7ProtocolBitmap::default(),
            l7_log_blacklist: HashMap::new(),
            l7_log_blacklist_trie: HashMap::new(),
            unconcerned_dns_nxdomain_response_suffixes: vec![],
            unconcerned_dns_nxdomain_trie: DnsNxdomainTrie::default(),
        }
    }
}

impl fmt::Debug for LogParserConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogParserConfig")
            .field(
                "l7_log_collect_nps_threshold",
                &self.l7_log_collect_nps_threshold,
            )
            .field(
                "l7_log_session_aggr_timeout",
                &self.l7_log_session_aggr_timeout,
            )
            .field(
                "l7_log_session_slot_capacity",
                &self.l7_log_session_slot_capacity,
            )
            .field("l7_log_dynamic", &self.l7_log_dynamic)
            .field(
                "l7_log_ignore_tap_sides",
                &self
                    .l7_log_ignore_tap_sides
                    .iter()
                    .enumerate()
                    .filter_map(|(i, b)| {
                        if *b {
                            TapSide::try_from(i as u8).ok()
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
            )
            .field("l7_log_blacklist_trie", &self.l7_log_blacklist)
            .field(
                "unconcerned_dns_nxdomain_trie",
                &self.unconcerned_dns_nxdomain_response_suffixes,
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DebugConfig {
    pub agent_id: u16,
    pub enabled: bool,
    pub controller_ips: Vec<IpAddr>,
    pub controller_port: u16,
    pub listen_port: u16,
    pub agent_mode: RunningMode,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DiagnoseConfig {
    pub enabled: bool,
    pub libvirt_xml_path: PathBuf,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StatsConfig {
    pub interval: Duration,
    pub host: String,
    pub analyzer_ip: String,
    pub analyzer_port: u16,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SynchronizerConfig {
    pub sync_interval: Duration,
    pub ntp_enabled: bool,
    pub max_escape: Duration,
    pub output_vlan: u16,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[derive(Clone, PartialEq, Eq)]
pub struct EbpfConfig {
    // 动态配置
    pub collector_enabled: bool,
    pub l7_metrics_enabled: bool,
    pub agent_id: u16,
    pub epc_id: u32,
    pub l7_log_packet_size: usize,
    // 静态配置
    pub l7_log_session_timeout: Duration,
    pub l7_protocol_inference_max_fail_count: usize,
    pub l7_protocol_inference_ttl: usize,
    pub l7_log_tap_types: [bool; 256],
    pub ctrl_mac: MacAddr,
    pub l7_protocol_enabled_bitmap: L7ProtocolBitmap,
    pub l7_protocol_parse_port_bitmap: Arc<Vec<(String, Bitmap)>>,
    pub l7_protocol_ports: std::collections::HashMap<String, String>,
    pub queue_size: usize,
    pub ebpf: Ebpf,
    pub symbol_table: SymbolTable,
    pub process_matcher: Vec<ProcessMatcher>,
    pub io_event: EbpfFileIoEvent,
    pub dpdk_enabled: bool,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
impl fmt::Debug for EbpfConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EbpfConfig")
            .field("collector_enabled", &self.collector_enabled)
            .field("l7_metrics_enabled", &self.l7_metrics_enabled)
            .field("agent_id", &self.agent_id)
            .field("epc_id", &self.epc_id)
            .field("l7_log_packet_size", &self.l7_log_packet_size)
            .field("l7_log_session_timeout", &self.l7_log_session_timeout)
            .field(
                "l7_protocol_inference_max_fail_count",
                &self.l7_protocol_inference_max_fail_count,
            )
            .field("l7_protocol_inference_ttl", &self.l7_protocol_inference_ttl)
            .field(
                "l7_log_tap_types",
                &self
                    .l7_log_tap_types
                    .iter()
                    .enumerate()
                    .filter(|&(_, b)| *b)
                    .collect::<Vec<_>>(),
            )
            .field("ctrl_mac", &self.ctrl_mac)
            .field(
                "l7_protocol_enabled_bitmap",
                &self.l7_protocol_enabled_bitmap,
            )
            .field("queue_size", &self.queue_size)
            .field("l7_protocol_ports", &self.l7_protocol_ports)
            .field("ebpf", &self.ebpf)
            .field("dpdk_enabled", &self.dpdk_enabled)
            .finish()
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
impl EbpfConfig {
    pub fn l7_log_enabled(&self) -> bool {
        // disabled when metrics collection is turned off
        if !self.l7_metrics_enabled || !self.collector_enabled {
            return false;
        }
        // eBPF data is only collected from Cloud-type TAP
        return self.l7_log_tap_types[u16::from(CaptureNetworkType::Any) as usize]
            || self.l7_log_tap_types[u16::from(CaptureNetworkType::Cloud) as usize];
    }
}

// Span/Trace 共用一套TypeMap
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TraceType {
    Disabled, // 业务表示关闭
    XB3,
    XB3Span,
    Uber,
    Sw3,
    Sw6,
    Sw8,
    TraceParent,
    NewRpcTraceContext,
    XTingyun(String),
    Customize(String),
}

// The value here must be lower case
const TRACE_TYPE_XB3: &str = "x-b3-traceid";
const TRACE_TYPE_XB3SPAN: &str = "x-b3-spanid";
const TRACE_TYPE_UBER: &str = "uber-trace-id";
const TRACE_TYPE_SW3: &str = "sw3";
const TRACE_TYPE_SW6: &str = "sw6";
const TRACE_TYPE_SW8: &str = "sw8";
const TRACE_TYPE_TRACE_PARENT: &str = "traceparent";
const TRACE_TYPE_X_TINGYUN: &str = "x-tingyun";

impl From<&str> for TraceType {
    // The parameter supports the following two formats:
    // Example 1: "xxx"
    // Example 2: "xxx.x"
    fn from(t: &str) -> TraceType {
        let tag_lowercase = t.trim().to_lowercase();
        let (tag, sub_tag) = if let Some(i) = tag_lowercase.find('.') {
            (
                tag_lowercase[..i].to_string(),
                tag_lowercase[i + 1..].to_string(),
            )
        } else {
            (tag_lowercase, String::new())
        };
        match tag.as_str() {
            TRACE_TYPE_XB3 => TraceType::XB3,
            TRACE_TYPE_XB3SPAN => TraceType::XB3Span,
            TRACE_TYPE_UBER => TraceType::Uber,
            TRACE_TYPE_SW3 => TraceType::Sw3,
            TRACE_TYPE_SW6 => TraceType::Sw6,
            TRACE_TYPE_SW8 => TraceType::Sw8,
            TRACE_TYPE_TRACE_PARENT => TraceType::TraceParent,
            SOFA_NEW_RPC_TRACE_CTX_KEY => TraceType::NewRpcTraceContext,
            TRACE_TYPE_X_TINGYUN => TraceType::XTingyun(sub_tag),
            _ if tag.len() > 0 => TraceType::Customize(tag),
            _ => TraceType::Disabled,
        }
    }
}

impl TraceType {
    pub fn check(&self, context: &str) -> bool {
        match &*self {
            TraceType::XB3 => context.eq_ignore_ascii_case(TRACE_TYPE_XB3),
            TraceType::XB3Span => context.eq_ignore_ascii_case(TRACE_TYPE_XB3SPAN),
            TraceType::Uber => context.eq_ignore_ascii_case(TRACE_TYPE_UBER),
            TraceType::Sw3 => context.eq_ignore_ascii_case(TRACE_TYPE_SW3),
            TraceType::Sw6 => context.eq_ignore_ascii_case(TRACE_TYPE_SW6),
            TraceType::Sw8 => context.eq_ignore_ascii_case(TRACE_TYPE_SW8),
            TraceType::TraceParent => context.eq_ignore_ascii_case(TRACE_TYPE_TRACE_PARENT),
            TraceType::NewRpcTraceContext => {
                context.eq_ignore_ascii_case(SOFA_NEW_RPC_TRACE_CTX_KEY)
            }
            TraceType::XTingyun(_) => context.eq_ignore_ascii_case(TRACE_TYPE_X_TINGYUN),
            TraceType::Customize(tag) => context.eq_ignore_ascii_case(&tag),
            _ => false,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            TraceType::XB3 => TRACE_TYPE_XB3,
            TraceType::XB3Span => TRACE_TYPE_XB3SPAN,
            TraceType::Uber => TRACE_TYPE_UBER,
            TraceType::Sw3 => TRACE_TYPE_SW3,
            TraceType::Sw6 => TRACE_TYPE_SW6,
            TraceType::Sw8 => TRACE_TYPE_SW8,
            TraceType::TraceParent => TRACE_TYPE_TRACE_PARENT,
            TraceType::NewRpcTraceContext => SOFA_NEW_RPC_TRACE_CTX_KEY,
            TraceType::XTingyun(_) => TRACE_TYPE_X_TINGYUN,
            TraceType::Customize(tag) => &tag,
            _ => "",
        }
    }

    pub const TRACE_ID: u8 = 0;
    pub const SPAN_ID: u8 = 1;

    // uber-trace-id: TRACEID:SPANID:PARENTSPANID:FLAGS
    // separeted by ':'
    // extract TRACEID from the first field and SPANID from the third field
    fn decode_uber_id(value: &str, id_type: u8) -> Option<&str> {
        let mut segs = value.split(":");
        if id_type == Self::TRACE_ID {
            segs.nth(0)
        } else if id_type == Self::SPAN_ID {
            segs.nth(2)
        } else {
            unreachable!()
        }
    }

    // sw3: SEGMENTID|SPANID|100|100|#IPPORT|#PARENT_ENDPOINT|#ENDPOINT|TRACEID|SAMPLING
    // sw3 values are separeted by '|'
    // extract "SEGMENTID-SPANID" as span_id
    fn decode_skywalking3_id(value: &str, id_type: u8) -> Option<Cow<'_, str>> {
        let mut segs = value.split("|");
        if id_type == Self::TRACE_ID {
            segs.nth(7).map(|s| s.into())
        } else if id_type == Self::SPAN_ID {
            let seg0 = segs.next();
            let seg1 = segs.next();
            seg0.zip(seg1)
                .map(|(seg_id, span_id)| format!("{}-{}", seg_id, span_id).into())
        } else {
            unreachable!()
        }
    }

    // sw6: 1-TRACEID-SEGMENTID-3-5-2-IPPORT-ENTRYURI-PARENTURI
    // sw8: 1-TRACEID-SEGMENTID-3-PARENT_SERVICE-PARENT_INSTANCE-PARENT_ENDPOINT-IPPORT
    // sw6 and sw8 values are separeted by '-'
    // trace id and segment id are encoded in base64
    // extract "SEGMENTID-SPANID" as span_id
    fn decode_skywalking_id(value: &str, id_type: u8) -> Option<Cow<'_, str>> {
        let mut segs = value.split("-");
        if id_type == Self::TRACE_ID {
            let id = segs.nth(1)?;
            Some(
                BASE64_STANDARD
                    .decode(id)
                    .ok()
                    .and_then(|v| String::from_utf8(v).ok())
                    .map(|s| Cow::Owned(s))
                    .unwrap_or(Cow::Borrowed(id)),
            )
        } else if id_type == Self::SPAN_ID {
            let seg_id = segs.nth(2)?;
            let span_id = segs.next()?;
            let mut result = Vec::new();
            if BASE64_STANDARD.decode_vec(seg_id, &mut result).is_err() {
                result.clear();
                result.extend_from_slice(seg_id.as_bytes());
            };
            let Ok(mut s) = String::from_utf8(result) else {
                return Some(format!("{}-{}", seg_id, span_id).into());
            };
            s.push('-');
            s.push_str(span_id);
            Some(s.into())
        } else {
            unreachable!()
        }
    }

    // OTel HTTP Trace format:
    // traceparent: 00-TRACEID-SPANID-01
    fn decode_traceparent(value: &str, id_type: u8) -> Option<&str> {
        let mut segs = value.split("-");
        if id_type == Self::TRACE_ID {
            segs.nth(1)
        } else if id_type == Self::SPAN_ID {
            segs.nth(2)
        } else {
            unreachable!()
        }
    }

    fn decode_tingyun<'a, 'b>(value: &'a str, sub_tag: &'b str) -> Option<Cow<'a, str>> {
        cloud_platform::tingyun::decode_trace_id(value, sub_tag)
    }

    fn decode_id<'a, 'b>(&'b self, value: &'a str, id_type: u8) -> Option<Cow<'a, str>> {
        let value = value.trim();
        match self {
            TraceType::Disabled => None,
            TraceType::XB3 | TraceType::XB3Span | TraceType::Customize(_) => Some(value.into()),
            TraceType::Uber => Self::decode_uber_id(value, id_type).map(|s| s.into()),
            TraceType::Sw3 => Self::decode_skywalking3_id(value, id_type),
            TraceType::Sw6 | TraceType::Sw8 => Self::decode_skywalking_id(value, id_type),
            TraceType::TraceParent => Self::decode_traceparent(value, id_type).map(|s| s.into()),
            /*
                referer https://github.com/sofastack/sofa-rpc/blob/7931102255d6ea95ee75676d368aad37c56b57ee/tracer/tracer-opentracing-resteasy/src/main/java/com/alipay/sofa/rpc/tracer/sofatracer/RestTracerAdapter.java#L75
                in new version of sofarpc, use new_rpc_trace_context header store trace info
            */
            TraceType::NewRpcTraceContext => {
                decode_new_rpc_trace_context_with_type(value.as_bytes(), id_type)
            }
            TraceType::XTingyun(sub_tag) => Self::decode_tingyun(value, sub_tag),
        }
    }

    pub fn decode_trace_id<'a, 'b>(&'b self, value: &'a str) -> Option<Cow<'a, str>> {
        self.decode_id(value, Self::TRACE_ID)
    }

    pub fn decode_span_id<'a, 'b>(&'b self, value: &'a str) -> Option<Cow<'a, str>> {
        self.decode_id(value, Self::SPAN_ID)
    }
}

impl Default for TraceType {
    fn default() -> Self {
        Self::Disabled
    }
}

#[derive(Default, Clone)]
pub struct L7LogDynamicConfig {
    // in lowercase
    pub proxy_client: HashSet<String>,
    // in lowercase
    pub x_request_id: HashSet<String>,

    pub trace_types: Vec<TraceType>,
    pub span_types: Vec<TraceType>,

    trace_set: HashSet<String>,
    span_set: HashSet<String>,
    pub expected_headers_set: Arc<HashSet<Vec<u8>>>,
    pub extra_log_fields: ExtraLogFields,
}

impl fmt::Debug for L7LogDynamicConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("L7LogDynamicConfig")
            .field("proxy_client", &self.proxy_client)
            .field("x_request_id", &self.x_request_id)
            .field("trace_types", &self.trace_types)
            .field("span_types", &self.span_types)
            .field("trace_set", &self.trace_set)
            .field("span_set", &self.span_set)
            .field(
                "expected_headers_set",
                &self
                    .expected_headers_set
                    .iter()
                    .map(|v| String::from_utf8_lossy(v).to_string())
                    .collect::<HashSet<_>>(),
            )
            .field("extra_log_fields", &self.extra_log_fields)
            .finish()
    }
}

impl PartialEq for L7LogDynamicConfig {
    fn eq(&self, other: &Self) -> bool {
        self.proxy_client == other.proxy_client
            && self.x_request_id == other.x_request_id
            && self.trace_types == other.trace_types
            && self.span_types == other.span_types
            && self.extra_log_fields == other.extra_log_fields
    }
}

impl Eq for L7LogDynamicConfig {}

impl L7LogDynamicConfig {
    pub fn new(
        proxy_client: Vec<String>,
        x_request_id: Vec<String>,
        trace_types: Vec<TraceType>,
        span_types: Vec<TraceType>,
        mut extra_log_fields: ExtraLogFields,
    ) -> Self {
        let mut expected_headers_set = get_expected_headers();

        let mut proxy_client_set = HashSet::new();
        for client in proxy_client.iter() {
            let client = client.trim();
            expected_headers_set.insert(client.as_bytes().to_vec());
            proxy_client_set.insert(client.to_string());
        }
        let mut x_request_id_set = HashSet::new();
        for t in x_request_id.iter() {
            let t = t.trim();
            expected_headers_set.insert(t.as_bytes().to_vec());
            x_request_id_set.insert(t.to_string());
        }

        let mut trace_set = HashSet::new();
        for t in trace_types.iter() {
            let t = t.as_str();
            expected_headers_set.insert(t.as_bytes().to_vec());
            trace_set.insert(t.to_owned());
        }

        let mut span_set = HashSet::new();
        for t in span_types.iter() {
            let t = t.as_str();
            expected_headers_set.insert(t.as_bytes().to_vec());
            span_set.insert(t.to_owned());
        }

        extra_log_fields.deduplicate();

        for f in extra_log_fields.http2.iter() {
            expected_headers_set.insert(f.field_name.as_bytes().to_vec());
        }

        Self {
            proxy_client: proxy_client_set,
            x_request_id: x_request_id_set,
            trace_types,
            span_types,
            trace_set,
            span_set,
            expected_headers_set: Arc::new(expected_headers_set),
            extra_log_fields,
        }
    }

    pub fn is_trace_id(&self, context: &str) -> bool {
        self.trace_set.contains(context)
    }

    pub fn is_span_id(&self, context: &str) -> bool {
        self.span_set.contains(context)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricServerConfig {
    pub enabled: bool,
    pub port: u16,
    pub compressed: bool,
    pub profile_compressed: bool,
    pub application_log_compressed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModuleConfig {
    pub enabled: bool,
    pub capture_mode: PacketCaptureType,
    pub user_config: UserConfig,
    pub collector: CollectorConfig,
    pub environment: EnvironmentConfig,
    pub platform: PlatformConfig,
    pub dispatcher: DispatcherConfig,
    pub flow: FlowConfig,
    pub log_parser: LogParserConfig,
    pub pcap: PcapStream,
    pub debug: DebugConfig,
    pub diagnose: DiagnoseConfig,
    pub stats: StatsConfig,
    pub sender: SenderConfig,
    pub npb: NpbConfig,
    pub handler: HandlerConfig,
    pub log: LogConfig,
    pub synchronizer: SynchronizerConfig,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub ebpf: EbpfConfig,
    pub agent_type: AgentType,
    pub metric_server: MetricServerConfig,
    pub port_config: PortConfig,
}

impl Default for ModuleConfig {
    fn default() -> Self {
        Self::try_from((
            Config {
                controller_ips: vec!["127.0.0.1".into()],
                ..Default::default()
            },
            UserConfig::standalone_default(),
        ))
        .unwrap()
    }
}

impl TryFrom<(Config, UserConfig)> for ModuleConfig {
    type Error = ConfigError;

    fn try_from(conf: (Config, UserConfig)) -> Result<Self, Self::Error> {
        let (static_config, conf) = conf;
        let controller_ip = static_config.controller_ips[0].parse::<IpAddr>().unwrap();
        let dest_ip = if conf.global.communication.ingester_ip.len() > 0 {
            conf.global.communication.ingester_ip.clone()
        } else {
            match controller_ip {
                IpAddr::V4(_) => Ipv4Addr::UNSPECIFIED.to_string(),
                IpAddr::V6(_) => Ipv6Addr::UNSPECIFIED.to_string(),
            }
        };
        let proxy_controller_ip = if conf.global.communication.proxy_controller_ip.len() > 0 {
            conf.global.communication.proxy_controller_ip.clone()
        } else {
            static_config.controller_ips[0].clone()
        };

        let max_memory = conf.global.limits.max_memory;
        let af_packet_blocks =
            conf.get_af_packet_blocks(conf.inputs.cbpf.common.capture_mode, max_memory);
        let capture_socket_type = conf.inputs.cbpf.af_packet.tunning.socket_version;
        let config = ModuleConfig {
            enabled: conf.global.common.enabled,
            user_config: conf.clone(),
            capture_mode: conf.inputs.cbpf.common.capture_mode,
            diagnose: DiagnoseConfig {
                enabled: conf.global.common.enabled,
                libvirt_xml_path: conf
                    .inputs
                    .resources
                    .private_cloud
                    .vm_xml_directory
                    .parse()
                    .unwrap_or_default(),
            },
            environment: EnvironmentConfig {
                max_memory,
                max_millicpus: conf.global.limits.max_millicpus,
                process_threshold: conf.global.alerts.process_threshold,
                thread_threshold: conf.global.alerts.thread_threshold,
                sys_memory_limit: conf
                    .global
                    .circuit_breakers
                    .sys_memory_percentage
                    .trigger_threshold,
                sys_memory_metric: conf.global.circuit_breakers.sys_memory_percentage.metric,
                log_file_size: conf.global.limits.max_local_log_file_size,
                capture_mode: conf.inputs.cbpf.common.capture_mode,
                guard_interval: conf.global.tunning.resource_monitoring_interval,
                system_load_circuit_breaker_threshold: conf
                    .global
                    .circuit_breakers
                    .relative_sys_load
                    .trigger_threshold,
                system_load_circuit_breaker_recover: conf
                    .global
                    .circuit_breakers
                    .relative_sys_load
                    .recovery_threshold,
                system_load_circuit_breaker_metric: conf
                    .global
                    .circuit_breakers
                    .relative_sys_load
                    .metric,
            },
            synchronizer: SynchronizerConfig {
                sync_interval: conf.global.communication.proactive_request_interval,
                output_vlan: conf.outputs.npb.raw_udp_vlan_tag,
                ntp_enabled: conf.global.ntp.enabled,
                max_escape: conf.global.communication.max_escape_duration,
            },
            stats: StatsConfig {
                interval: Duration::from_secs(10), // TODO: make it configurable
                host: conf.global.self_monitoring.hostname.clone(),
                analyzer_ip: dest_ip.clone(),
                analyzer_port: conf.global.communication.ingester_port,
            },
            dispatcher: DispatcherConfig {
                global_pps_threshold: conf.inputs.cbpf.tunning.max_capture_pps,
                capture_packet_size: conf.inputs.cbpf.tunning.max_capture_packet_size,
                dpdk_source: conf.inputs.cbpf.special_network.dpdk.source,
                dispatcher_queue: conf.inputs.cbpf.tunning.dispatcher_queue_enabled,
                l7_log_packet_size: conf.processors.request_log.tunning.payload_truncation,
                tunnel_type_bitmap: TunnelTypeBitmap::from_slices(
                    &conf.inputs.cbpf.preprocess.tunnel_decap_protocols,
                    &conf.inputs.cbpf.preprocess.tunnel_trim_protocols,
                ),
                tunnel_type_trim_bitmap: TunnelTypeBitmap::from_strings(
                    &conf.inputs.cbpf.preprocess.tunnel_trim_protocols,
                ),
                agent_type: conf.global.common.agent_type,
                agent_id: conf.global.common.agent_id as u16,
                capture_socket_type,
                #[cfg(target_os = "linux")]
                extra_netns_regex: conf.inputs.cbpf.af_packet.extra_netns_regex.clone(),
                tap_interface_regex: conf.inputs.cbpf.af_packet.interface_regex.clone(),
                if_mac_source: conf.inputs.resources.private_cloud.vm_mac_source.into(),
                analyzer_ip: dest_ip.clone(),
                analyzer_port: conf.global.communication.ingester_port,
                proxy_controller_ip,
                proxy_controller_port: conf.global.communication.proxy_controller_port,
                capture_bpf: conf.inputs.cbpf.af_packet.extra_bpf_filter.clone(),
                max_memory,
                af_packet_blocks,
                #[cfg(any(target_os = "linux", target_os = "android"))]
                af_packet_version: capture_socket_type.into(),
                capture_mode: conf.inputs.cbpf.common.capture_mode,
                region_id: conf.global.common.region_id,
                pod_cluster_id: conf.global.common.pod_cluster_id,
                enabled: conf.global.common.enabled,
                npb_dedup_enabled: conf.outputs.npb.traffic_global_dedup,
                bond_group: if conf.inputs.cbpf.af_packet.bond_interfaces.is_empty() {
                    vec![]
                } else {
                    conf.inputs.cbpf.af_packet.bond_interfaces[0]
                        .slave_interfaces
                        .clone()
                },
                #[cfg(any(target_os = "linux", target_os = "android"))]
                cpu_set: CpuSet::new(),
            },
            sender: SenderConfig {
                dest_ip: dest_ip.clone(),
                agent_id: conf.global.common.agent_id as u16,
                team_id: conf.global.common.team_id,
                organize_id: conf.global.common.organize_id,
                dest_port: conf.global.communication.ingester_port,
                npb_port: conf.outputs.npb.target_port,
                vxlan_flags: conf.outputs.npb.custom_vxlan_flags,
                npb_enable_qos_bypass: conf.outputs.socket.raw_udp_qos_bypass,
                npb_vlan: conf.outputs.npb.raw_udp_vlan_tag,
                npb_vlan_mode: conf.outputs.npb.extra_vlan_header.into(),
                npb_dedup_enabled: conf.outputs.npb.traffic_global_dedup,
                npb_bps_threshold: conf.outputs.npb.max_tx_throughput,
                npb_socket_type: conf.outputs.socket.npb_socket_type,
                server_tx_bandwidth_threshold: conf
                    .global
                    .circuit_breakers
                    .tx_throughput
                    .trigger_threshold,
                bandwidth_probe_interval: conf
                    .global
                    .circuit_breakers
                    .tx_throughput
                    .throughput_monitoring_interval,
                multiple_sockets_to_ingester: conf.outputs.socket.multiple_sockets_to_ingester,
                collector_socket_type: conf.outputs.socket.data_socket_type,
                standalone_data_file_size: conf.global.standalone_mode.max_data_file_size,
                standalone_data_file_dir: conf.global.standalone_mode.data_file_dir.clone(),
                enabled: conf.outputs.flow_metrics.enabled,
            },
            npb: NpbConfig {
                mtu: conf.outputs.npb.max_mtu,
                underlay_is_ipv6: controller_ip.is_ipv6(),
                npb_port: conf.outputs.npb.target_port,
                vxlan_flags: conf.outputs.npb.custom_vxlan_flags,
                ignore_overlay_vlan: conf.outputs.npb.overlay_vlan_header_trimming,
                enable_qos_bypass: conf.outputs.socket.raw_udp_qos_bypass,
                output_vlan: conf.outputs.npb.raw_udp_vlan_tag,
                vlan_mode: conf.outputs.npb.extra_vlan_header,
                dedup_enabled: conf.outputs.npb.traffic_global_dedup,
                socket_type: conf.outputs.socket.npb_socket_type,
                queue_size: conf.outputs.flow_metrics.tunning.sender_queue_size,
            },
            collector: CollectorConfig {
                enabled: conf.outputs.flow_metrics.enabled,
                inactive_server_port_aggregation: conf
                    .outputs
                    .flow_metrics
                    .filters
                    .inactive_server_port_aggregation,
                inactive_ip_aggregation: conf.outputs.flow_metrics.filters.inactive_ip_aggregation,
                vtap_flow_1s_enabled: conf.outputs.flow_metrics.filters.second_metrics,
                l4_log_collect_nps_threshold: conf.outputs.flow_log.throttles.l4_throttle,
                l7_metrics_enabled: conf.outputs.flow_metrics.filters.apm_metrics,
                agent_type: conf.global.common.agent_type,
                agent_id: conf.global.common.agent_id as u16,
                l4_log_store_tap_types: generate_tap_types_array(
                    &conf.outputs.flow_log.filters.l4_capture_network_types,
                ),
                l4_log_ignore_tap_sides: {
                    let mut tap_sides = [false; TapSide::MAX as usize + 1];
                    for t in conf
                        .outputs
                        .flow_log
                        .filters
                        .l4_ignored_observation_points
                        .iter()
                    {
                        // TapSide values will be in range [0, TapSide::MAX]
                        tap_sides[*t as usize] = true;
                    }
                    tap_sides
                },
                cloud_gateway_traffic: conf
                    .inputs
                    .cbpf
                    .physical_mirror
                    .private_cloud_gateway_traffic,
                packet_delay: conf
                    .processors
                    .flow_log
                    .time_window
                    .max_tolerable_packet_delay,
                npm_metrics_concurrent: conf.outputs.flow_metrics.filters.npm_metrics_concurrent,
            },
            handler: HandlerConfig {
                npb_dedup_enabled: conf.outputs.npb.traffic_global_dedup,
                agent_type: conf.global.common.agent_type,
            },
            pcap: conf.processors.packet.pcap_stream,
            platform: PlatformConfig {
                sync_interval: conf.inputs.resources.push_interval,
                kubernetes_cluster_id: static_config.kubernetes_cluster_id.clone(),
                libvirt_xml_path: conf
                    .inputs
                    .resources
                    .private_cloud
                    .vm_xml_directory
                    .parse()
                    .unwrap_or_default(),
                kubernetes_poller_type: conf.inputs.resources.kubernetes.pod_mac_collection_method,
                agent_id: conf.global.common.agent_id as u16,
                enabled: conf
                    .inputs
                    .resources
                    .private_cloud
                    .hypervisor_resource_enabled,
                agent_type: conf.global.common.agent_type,
                epc_id: conf.global.common.vpc_id,
                kubernetes_api_enabled: conf.global.common.kubernetes_api_enabled,
                kubernetes_api_list_limit: conf.inputs.resources.kubernetes.api_list_page_size,
                kubernetes_api_list_interval: conf
                    .inputs
                    .resources
                    .kubernetes
                    .api_list_max_interval,
                kubernetes_resources: conf.inputs.resources.kubernetes.api_resources.clone(),
                max_memory,
                namespace: if conf
                    .inputs
                    .resources
                    .kubernetes
                    .kubernetes_namespace
                    .is_empty()
                {
                    None
                } else {
                    Some(
                        conf.inputs
                            .resources
                            .kubernetes
                            .kubernetes_namespace
                            .clone(),
                    )
                },
                thread_threshold: conf.global.alerts.thread_threshold,
                capture_mode: conf.inputs.cbpf.common.capture_mode,
                #[cfg(any(target_os = "linux", target_os = "android"))]
                os_proc_scan_conf: OsProcScanConfig {
                    os_proc_root: conf.inputs.proc.proc_dir_path.clone(),
                    os_proc_socket_sync_interval: conf
                        .inputs
                        .proc
                        .socket_info_sync_interval
                        .as_secs() as u32,
                    os_proc_socket_min_lifetime: conf.inputs.proc.min_lifetime.as_secs() as u32,
                    os_app_tag_exec_user: conf.inputs.proc.tag_extraction.exec_username.clone(),
                    os_app_tag_exec: conf.inputs.proc.tag_extraction.script_command.clone(),
                    os_proc_sync_enabled: conf.inputs.proc.enabled,
                    os_proc_sync_tagged_only: conf
                        .inputs
                        .proc
                        .process_matcher
                        .iter()
                        .find(|m| {
                            m.enabled_features
                                .iter()
                                .find(|s| s.as_str() == "proc.gprocess_info")
                                .is_some()
                                && m.only_with_tag
                        })
                        .is_some(),
                },
                #[cfg(target_os = "windows")]
                os_proc_scan_conf: OsProcScanConfig {},
                agent_enabled: conf.global.common.enabled,
                #[cfg(target_os = "linux")]
                extra_netns_regex: conf.inputs.cbpf.af_packet.extra_netns_regex.to_string(),
            },
            flow: (&conf).into(),
            log_parser: LogParserConfig {
                l7_log_collect_nps_threshold: conf.outputs.flow_log.throttles.l7_throttle,
                l7_log_session_aggr_timeout: conf
                    .processors
                    .request_log
                    .timeouts
                    .session_aggregate_window_duration,
                l7_log_session_slot_capacity: conf
                    .processors
                    .request_log
                    .tunning
                    .session_aggregate_slot_capacity,
                l7_log_dynamic: L7LogDynamicConfig::new(
                    conf.processors
                        .request_log
                        .tag_extraction
                        .tracing_tag
                        .http_real_client
                        .iter()
                        .map(|x| x.to_ascii_lowercase())
                        .collect(),
                    conf.processors
                        .request_log
                        .tag_extraction
                        .tracing_tag
                        .x_request_id
                        .iter()
                        .map(|x| x.to_ascii_lowercase())
                        .collect(),
                    conf.processors
                        .request_log
                        .tag_extraction
                        .tracing_tag
                        .apm_trace_id
                        .iter()
                        .map(|item| TraceType::from(item.as_str()))
                        .collect(),
                    conf.processors
                        .request_log
                        .tag_extraction
                        .tracing_tag
                        .apm_span_id
                        .iter()
                        .map(|item| TraceType::from(item.as_str()))
                        .collect(),
                    ExtraLogFields {
                        http: conf
                            .processors
                            .request_log
                            .tag_extraction
                            .custom_fields
                            .get("HTTP")
                            .map(|c| c.iter().map(|f| ExtraLogFieldsInfo::from(f)).collect())
                            .unwrap_or(vec![]),
                        http2: conf
                            .processors
                            .request_log
                            .tag_extraction
                            .custom_fields
                            .get("HTTP2")
                            .map(|c| c.iter().map(|f| ExtraLogFieldsInfo::from(f)).collect())
                            .unwrap_or(vec![]),
                    },
                ),
                l7_log_ignore_tap_sides: {
                    let mut tap_sides = [false; TapSide::MAX as usize + 1];
                    for t in conf
                        .outputs
                        .flow_log
                        .filters
                        .l7_ignored_observation_points
                        .iter()
                    {
                        // TapSide values will be in range [0, TapSide::MAX]
                        tap_sides[*t as usize] = true;
                    }
                    tap_sides
                },
                http_endpoint_disabled: conf
                    .processors
                    .request_log
                    .tag_extraction
                    .http_endpoint
                    .extraction_disabled,
                http_endpoint_trie: HttpEndpointTrie::from(
                    &conf.processors.request_log.tag_extraction.http_endpoint,
                ),
                obfuscate_enabled_protocols: L7ProtocolBitmap::from(
                    conf.processors
                        .request_log
                        .tag_extraction
                        .obfuscate_protocols
                        .as_slice(),
                ),
                l7_log_blacklist: conf.processors.request_log.filters.tag_filters.clone(),
                l7_log_blacklist_trie: {
                    let mut blacklist_trie = HashMap::new();
                    for (k, v) in conf.processors.request_log.filters.tag_filters.iter() {
                        let l7_protocol = L7Protocol::from(k.to_string());
                        if l7_protocol == L7Protocol::Unknown {
                            warn!("Unsupported l7_protocol: {:?}", k);
                            continue;
                        }
                        BlacklistTrie::new(v).map(|x| blacklist_trie.insert(l7_protocol, x));
                    }
                    blacklist_trie
                },
                unconcerned_dns_nxdomain_response_suffixes: conf
                    .processors
                    .request_log
                    .filters
                    .unconcerned_dns_nxdomain_response_suffixes
                    .clone(),
                unconcerned_dns_nxdomain_trie: DnsNxdomainTrie::from(
                    &conf
                        .processors
                        .request_log
                        .filters
                        .unconcerned_dns_nxdomain_response_suffixes,
                ),
            },
            debug: DebugConfig {
                agent_id: conf.global.common.agent_id as u16,
                enabled: conf.global.self_monitoring.debug.enabled,
                controller_ips: static_config
                    .controller_ips
                    .iter()
                    .map(|c| c.parse::<IpAddr>().unwrap())
                    .collect(),
                listen_port: conf.global.self_monitoring.debug.local_udp_port,
                controller_port: static_config.controller_port,
                agent_mode: static_config.agent_mode,
            },
            log: LogConfig {
                log_level: conf.global.self_monitoring.log.log_level,
                log_threshold: conf.global.limits.max_log_backhaul_rate,
                log_retention: conf.global.limits.local_log_retention.as_secs() as u32,
                rsyslog_enabled: {
                    if dest_ip == Ipv4Addr::UNSPECIFIED.to_string()
                        || dest_ip == Ipv6Addr::UNSPECIFIED.to_string()
                    {
                        info!("analyzer_ip not set, remote log disabled");
                        false
                    } else {
                        conf.global.self_monitoring.log.log_backhaul_enabled
                    }
                },
                host: conf.global.self_monitoring.hostname.clone(),
            },
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: EbpfConfig {
                collector_enabled: conf.outputs.flow_metrics.enabled,
                l7_metrics_enabled: conf.outputs.flow_metrics.filters.apm_metrics,
                agent_id: conf.global.common.agent_id as u16,
                epc_id: conf.global.common.vpc_id,
                l7_log_session_timeout: conf
                    .processors
                    .request_log
                    .timeouts
                    .session_aggregate_window_duration,
                l7_log_packet_size: CAP_LEN_MAX
                    .min(conf.processors.request_log.tunning.payload_truncation as usize),
                l7_log_tap_types: generate_tap_types_array(
                    &conf.outputs.flow_log.filters.l7_capture_network_types,
                ),
                l7_protocol_inference_max_fail_count: conf
                    .processors
                    .request_log
                    .application_protocol_inference
                    .inference_max_retries,
                l7_protocol_inference_ttl: conf
                    .processors
                    .request_log
                    .application_protocol_inference
                    .inference_result_ttl
                    .as_secs() as usize,
                ctrl_mac: if is_tt_workload(conf.global.common.agent_type) {
                    fn get_ctrl_mac(ip: &IpAddr) -> MacAddr {
                        // use host mac
                        #[cfg(target_os = "linux")]
                        if let Err(e) =
                            public::netns::open_named_and_setns(&public::netns::NsFile::Root)
                        {
                            warn!(
                                "agent must have CAP_SYS_ADMIN to run without 'hostNetwork: true'."
                            );
                            warn!("setns error: {}", e);
                            crate::utils::notify_exit(-1);
                            return MacAddr::ZERO;
                        }
                        let ctrl_mac = match get_ctrl_ip_and_mac(ip) {
                            Ok((_, mac)) => mac,
                            Err(e) => {
                                warn!("get_ctrl_ip_and_mac error: {}", e);
                                crate::utils::notify_exit(-1);
                                return MacAddr::ZERO;
                            }
                        };
                        #[cfg(target_os = "linux")]
                        if let Err(e) = public::netns::reset_netns() {
                            warn!("reset setns error: {}", e);
                            crate::utils::notify_exit(-1);
                            return MacAddr::ZERO;
                        };
                        ctrl_mac
                    }

                    get_ctrl_mac(&static_config.controller_ips[0].parse().unwrap())
                } else {
                    MacAddr::ZERO
                },
                l7_protocol_enabled_bitmap: L7ProtocolBitmap::from(
                    conf.processors
                        .request_log
                        .application_protocol_inference
                        .enabled_protocols
                        .as_slice(),
                ),
                l7_protocol_parse_port_bitmap: Arc::new(conf.get_protocol_port_parse_bitmap()),
                l7_protocol_ports: conf.get_protocol_port(),
                queue_size: conf.inputs.ebpf.tunning.collector_queue_size,
                ebpf: conf.inputs.ebpf.clone(),
                symbol_table: conf.inputs.proc.symbol_table,
                process_matcher: conf.inputs.proc.process_matcher.clone(),
                io_event: conf.inputs.ebpf.file.io_event,
                dpdk_enabled: conf.inputs.cbpf.special_network.dpdk.source == DpdkSource::Ebpf,
            },
            metric_server: MetricServerConfig {
                enabled: conf.inputs.integration.enabled,
                port: conf.inputs.integration.listen_port,
                compressed: conf.inputs.integration.compression.trace,
                profile_compressed: conf.inputs.integration.compression.profile,
                application_log_compressed: conf.outputs.compression.application_log,
            },
            agent_type: conf.global.common.agent_type,
            port_config: PortConfig {
                analyzer_port: conf.global.communication.ingester_port,
                proxy_controller_port: conf.global.communication.proxy_controller_port,
            },
        };
        Ok(config)
    }
}

pub struct ConfigHandler {
    pub ctrl_ip: IpAddr,
    pub ctrl_mac: MacAddr,
    pub container_cpu_limit: u32, // unit: milli-core
    pub container_mem_limit: u64, // unit: bytes
    pub logger_handle: Option<LoggerHandle>,
    // need update
    pub static_config: Config,
    pub candidate_config: ModuleConfig,
    pub current_config: Arc<ArcSwap<ModuleConfig>>,
}

impl ConfigHandler {
    pub fn new(config: Config, ctrl_ip: IpAddr, ctrl_mac: MacAddr) -> Self {
        let candidate_config =
            ModuleConfig::try_from((config.clone(), UserConfig::standalone_default())).unwrap();
        let current_config = Arc::new(ArcSwap::from_pointee(candidate_config.clone()));

        #[cfg(any(target_os = "linux", target_os = "android"))]
        let (container_cpu_limit, container_mem_limit) = get_container_resource_limits();
        #[cfg(target_os = "windows")]
        let (container_cpu_limit, container_mem_limit) = (0, 0);

        Self {
            static_config: config,
            ctrl_ip,
            ctrl_mac,
            container_cpu_limit,
            container_mem_limit,
            candidate_config,
            current_config,
            logger_handle: None,
        }
    }

    pub fn set_logger_handle(&mut self, handle: LoggerHandle) {
        self.logger_handle.replace(handle);
    }

    pub fn collector(&self) -> CollectorAccess {
        Map::new(self.current_config.clone(), |config| -> &CollectorConfig {
            &config.collector
        })
    }

    pub fn environment(&self) -> EnvironmentAccess {
        Map::new(
            self.current_config.clone(),
            |config| -> &EnvironmentConfig { &config.environment },
        )
    }

    pub fn handler(&self) -> HandlerAccess {
        Map::new(self.current_config.clone(), |config| -> &HandlerConfig {
            &config.handler
        })
    }

    pub fn sender(&self) -> SenderAccess {
        Map::new(self.current_config.clone(), |config| -> &SenderConfig {
            &config.sender
        })
    }

    pub fn npb(&self) -> NpbAccess {
        Map::new(self.current_config.clone(), |config| -> &NpbConfig {
            &config.npb
        })
    }

    pub fn platform(&self) -> PlatformAccess {
        Map::new(self.current_config.clone(), |config| -> &PlatformConfig {
            &config.platform
        })
    }

    pub fn dispatcher(&self) -> DispatcherAccess {
        Map::new(self.current_config.clone(), |config| -> &DispatcherConfig {
            &config.dispatcher
        })
    }

    pub fn diagnose(&self) -> DiagnoseAccess {
        Map::new(self.current_config.clone(), |config| -> &DiagnoseConfig {
            &config.diagnose
        })
    }

    pub fn log(&self) -> LogAccess {
        Map::new(self.current_config.clone(), |config| -> &LogConfig {
            &config.log
        })
    }

    pub fn flow(&self) -> FlowAccess {
        Map::new(self.current_config.clone(), |config| -> &FlowConfig {
            &config.flow
        })
    }

    pub fn log_parser(&self) -> LogParserAccess {
        Map::new(self.current_config.clone(), |config| -> &LogParserConfig {
            &config.log_parser
        })
    }

    pub fn pcap(&self) -> PcapAccess {
        Map::new(self.current_config.clone(), |config| -> &PcapStream {
            &config.pcap
        })
    }

    pub fn debug(&self) -> DebugAccess {
        Map::new(self.current_config.clone(), |config| -> &DebugConfig {
            &config.debug
        })
    }

    pub fn synchronizer(&self) -> SynchronizerAccess {
        Map::new(
            self.current_config.clone(),
            |config| -> &SynchronizerConfig { &config.synchronizer },
        )
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn ebpf(&self) -> EbpfAccess {
        Map::new(self.current_config.clone(), |config| -> &EbpfConfig {
            &config.ebpf
        })
    }

    pub fn metric_server(&self) -> MetricServerAccess {
        Map::new(
            self.current_config.clone(),
            |config| -> &MetricServerConfig { &config.metric_server },
        )
    }

    pub fn port(&self) -> PortAccess {
        Map::new(self.current_config.clone(), |config| -> &PortConfig {
            &config.port_config
        })
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn set_process_scheduling_priority(process_scheduling_priority: usize) {
        let pid = std::process::id();
        unsafe {
            if libc::setpriority(
                libc::PRIO_PROCESS,
                pid,
                process_scheduling_priority as libc::c_int,
            ) != 0
            {
                warn!(
                    "Process scheduling priority set {} to pid {} error.",
                    process_scheduling_priority, pid
                );
            }
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn set_cpu_affinity(cpu_affinity: &Vec<usize>, cpu_set: &mut CpuSet) {
        let mut invalid_config = false;
        let system =
            System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::everything()));
        let cpu_count = system.cpus().len() as usize;
        if cpu_affinity.len() > 0 {
            for id in cpu_affinity {
                if *id < cpu_count {
                    if let Err(e) = cpu_set.set(*id) {
                        warn!(
                            "Invalid CPU Affinity config {:?}, error: {:?}",
                            cpu_affinity, e
                        );
                        invalid_config = true;
                    }
                } else {
                    invalid_config = true;
                    break;
                }
            }
        } else {
            for i in 0..cpu_count {
                let _ = cpu_set.set(i);
            }
        }

        if invalid_config {
            warn!("Invalid CPU Affinity config {:?}.", cpu_affinity);
        } else {
            let pid = std::process::id() as i32;
            if let Err(e) = sched_setaffinity(Pid::from_raw(pid), &cpu_set) {
                warn!("CPU Affinity({:?}) bind error: {:?}.", &cpu_set, e);
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn set_netns_regex(kubernetes_poller: &GenericPoller, last: &String, now: &String) {
        let old_regex = if !last.is_empty() {
            regex::Regex::new(&last).ok()
        } else {
            None
        };

        let regex = now.as_ref();
        let regex = if regex != "" {
            match regex::Regex::new(regex) {
                Ok(re) => {
                    info!("platform monitoring extra netns: /{}/", regex);
                    Some(re)
                }
                Err(_) => {
                    warn!(
                        "platform monitoring no extra netns because regex /{}/ is invalid",
                        regex
                    );
                    None
                }
            }
        } else {
            info!("platform monitoring no extra netns");
            None
        };

        let old_netns = old_regex.map(|re| public::netns::find_ns_files_by_regex(&re));
        let new_netns = regex
            .as_ref()
            .map(|re| public::netns::find_ns_files_by_regex(&re));
        if old_netns != new_netns {
            info!("query net namespaces changed from {:?} to {:?}, restart agent to create dispatcher for extra namespaces, deepflow-agent restart...", old_netns, new_netns);
            crate::utils::notify_exit(public::consts::NORMAL_EXIT_WITH_RESTART);
            return;
        }

        kubernetes_poller.set_netns_regex(regex);
    }

    #[cfg(target_os = "windows")]
    fn switch_recv_engine(handler: &ConfigHandler, comp: &mut AgentComponents) {
        for d in comp.dispatcher_components.iter() {
            if let Err(e) = d
                .dispatcher
                .switch_recv_engine(&handler.candidate_config.dispatcher)
            {
                log::error!("switch RecvEngine error: {}, deepflow-agent restart...", e);
                crate::utils::notify_exit(-1);
                return;
            }
        }
    }

    fn start_dispatcher(handler: &ConfigHandler, components: &mut AgentComponents) {
        match handler.candidate_config.capture_mode {
            PacketCaptureType::Analyzer => {
                for d in components.dispatcher_components.iter_mut() {
                    d.start();
                }
            }
            _ => {
                if !running_in_container() && !is_kernel_available_for_cgroups() {
                    // In the environment where cgroups is not supported, we need to check free memory
                    match free_memory_check(
                        // fixme: It can skip this check because it has been checked before
                        handler.candidate_config.environment.max_memory,
                        &components.exception_handler,
                    ) {
                        Ok(()) => {
                            for d in components.dispatcher_components.iter_mut() {
                                d.start();
                            }
                        }
                        Err(e) => {
                            warn!("{}", e);
                        }
                    }
                }
            }
        }
    }

    fn stop_dispatcher(_: &ConfigHandler, components: &mut AgentComponents) {
        for d in components.dispatcher_components.iter_mut() {
            d.stop();
        }
    }

    fn leaky_bucket_callback(handler: &ConfigHandler, components: &mut AgentComponents) {
        match handler.candidate_config.capture_mode {
            PacketCaptureType::Analyzer => {
                components.rx_leaky_bucket.set_rate(None);
                info!("dispatcher.global pps set ulimit when capture_mode=analyzer");
            }
            _ => {
                components.rx_leaky_bucket.set_rate(Some(
                    handler.candidate_config.dispatcher.global_pps_threshold,
                ));
                info!(
                    "dispatcher.global pps threshold change to {}",
                    handler.candidate_config.dispatcher.global_pps_threshold
                );
            }
        }
    }

    fn set_log_level(logger_handle: &mut Option<LoggerHandle>, log_level: &Level) -> bool {
        match logger_handle.as_mut() {
            Some(h) => match h.parse_and_push_temp_spec(log_level.as_str().to_lowercase()) {
                Ok(_) => true,
                Err(e) => {
                    warn!("failed to set log_level: {}", e);
                    false
                }
            },
            None => {
                warn!("logger_handle not set");
                false
            }
        }
    }

    fn set_log_retention(
        logger_handle: &mut Option<LoggerHandle>,
        log_retention: &Duration,
        log_file: &String,
    ) -> bool {
        let log_retention = (log_retention.as_secs() / 3600 / 24).min(1);
        match logger_handle.as_mut() {
            Some(h) => match h.flw_config() {
                Err(FlexiLoggerError::NoFileLogger) => {
                    info!("no file logger, skipped log_retention change");
                    false
                }
                _ => match h.reset_flw(
                    &FileLogWriter::builder(FileSpec::try_from(log_file).unwrap())
                        .rotate(
                            Criterion::Age(Age::Day),
                            Naming::Timestamps,
                            Cleanup::KeepLogFiles(log_retention as usize),
                        )
                        .create_symlink(log_file)
                        .append(),
                ) {
                    Ok(_) => true,
                    Err(e) => {
                        warn!("failed to set log_retention: {}", e);
                        false
                    }
                },
            },
            None => {
                warn!("logger_handle not set");
                false
            }
        }
    }

    fn set_stats(handler: &ConfigHandler, components: &mut AgentComponents) {
        let c = &components.stats_collector;
        c.set_hostname(handler.candidate_config.stats.host.clone());
        c.set_min_interval(handler.candidate_config.stats.interval);
    }

    fn set_debug(handler: &ConfigHandler, components: &mut AgentComponents) {
        if handler.candidate_config.debug.enabled {
            components.debugger.start();
        } else {
            components.debugger.stop();
        }
    }

    #[cfg(target_os = "linux")]
    fn set_platform(handler: &ConfigHandler, components: &mut AgentComponents) {
        let conf = &handler.candidate_config.platform;

        if conf.agent_enabled
            && (conf.capture_mode == PacketCaptureType::Local || is_tt_pod(conf.agent_type))
        {
            if is_tt_pod(conf.agent_type) {
                components.kubernetes_poller.start();
            } else {
                components.kubernetes_poller.stop();
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn set_ebpf(handler: &ConfigHandler, components: &mut AgentComponents) {
        if let Some(d) = components.ebpf_dispatcher_component.as_mut() {
            d.ebpf_collector
                .on_config_change(&handler.candidate_config.ebpf);
        }
    }

    fn set_metric_server(handler: &ConfigHandler, components: &mut AgentComponents) {
        components
            .metrics_server_component
            .external_metrics_server
            .enable_compressed(handler.candidate_config.metric_server.compressed);
        components
            .metrics_server_component
            .external_metrics_server
            .enable_profile_compressed(handler.candidate_config.metric_server.profile_compressed);
        // 当端口更新后，在enabled情况下需要重启服务器重新监听
        components
            .metrics_server_component
            .external_metrics_server
            .set_port(handler.candidate_config.metric_server.port);
    }

    fn set_npb(handler: &ConfigHandler, components: &mut AgentComponents) {
        let dispatcher_builders = &components.dispatcher_components;
        for e in dispatcher_builders {
            let mut builders = e.handler_builders.write().unwrap();
            for e in builders.iter_mut() {
                match e {
                    PacketHandlerBuilder::Npb(n) => {
                        n.on_config_change(
                            &handler.candidate_config.npb,
                            &components.debugger.clone_queue(),
                        );
                    }
                    _ => {}
                }
            }
        }
        components.npb_arp_table.set_need_resolve_mac(
            handler.candidate_config.npb.socket_type == agent::SocketType::RawUdp,
        );
    }

    fn set_vector(handler: &ConfigHandler, components: &mut AgentComponents) {
        components.vector_component.on_config_change(
            handler.candidate_config.user_config.inputs.vector.enabled,
            handler
                .candidate_config
                .user_config
                .inputs
                .vector
                .config
                .clone(),
        );
    }

    fn set_restart_dispatcher(handler: &ConfigHandler, components: &mut AgentComponents) {
        for d in components.dispatcher_components.iter_mut() {
            d.stop();
        }
        if handler.candidate_config.capture_mode != PacketCaptureType::Analyzer
            && !running_in_container()
            && !is_kernel_available_for_cgroups()
        // In the environment where cgroups is not supported, we need to check free memory
        {
            match free_memory_check(
                // fixme: It can skip this check because it has been checked before
                handler.candidate_config.environment.max_memory,
                &components.exception_handler,
            ) {
                Ok(()) => {
                    for d in components.dispatcher_components.iter_mut() {
                        d.start();
                    }
                }
                Err(e) => {
                    warn!("{}", e);
                }
            }
        } else {
            for d in components.dispatcher_components.iter_mut() {
                d.start();
            }
        }
    }

    pub fn on_config(
        &mut self,
        user_config: UserConfig,
        exception_handler: &ExceptionHandler,
        mut components: Option<&mut AgentComponents>,
        #[cfg(target_os = "linux")] api_watcher: &Arc<ApiWatcher>,
        runtime: &Runtime,
        session: &Session,
        agent_id: &AgentId,
        first_run: bool,
    ) -> Vec<fn(&ConfigHandler, &mut AgentComponents)> {
        let candidate_config = &mut self.candidate_config;
        let static_config = &self.static_config;
        let config = &mut candidate_config.user_config;
        let mut new_config: ModuleConfig = (static_config.clone(), user_config).try_into().unwrap();
        let mut callbacks: Vec<fn(&ConfigHandler, &mut AgentComponents)> = vec![];
        let mut restart_dispatcher = false;
        let mut restart_agent = false;
        #[cfg(target_os = "windows")]
        let capture_mode = new_config.user_config.inputs.cbpf.common.capture_mode;
        let log_file = new_config
            .user_config
            .global
            .self_monitoring
            .log
            .log_file
            .clone();
        let logger_handle = &mut self.logger_handle;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let mut cpu_set = CpuSet::new();

        if first_run {
            info!("{:#?}", &new_config.user_config);
        }

        // inputs
        let af_packet = &mut config.inputs.cbpf.af_packet;
        let new_af_packet = &mut new_config.user_config.inputs.cbpf.af_packet;
        if af_packet.bpf_filter_disabled != new_af_packet.bpf_filter_disabled {
            info!(
                "Update inputs.cbpf.af_packet.bpf_filter_disabled from {:?} to {:?}.",
                af_packet.bpf_filter_disabled, new_af_packet.bpf_filter_disabled
            );
            af_packet.bpf_filter_disabled = new_af_packet.bpf_filter_disabled;
            restart_agent = !first_run;
        }
        if af_packet.bond_interfaces != new_af_packet.bond_interfaces {
            info!(
                "Update inputs.cbpf.af_packet.bond_interfaces from {:?} to {:?}.",
                af_packet.bond_interfaces, new_af_packet.bond_interfaces
            );
            af_packet.bond_interfaces = new_af_packet.bond_interfaces.clone();
            restart_agent = !first_run;
        }
        if af_packet.extra_bpf_filter != new_af_packet.extra_bpf_filter {
            info!(
                "Update inputs.cbpf.af_packet.extra_bpf_filter from {:?} to {:?}.",
                af_packet.extra_bpf_filter, new_af_packet.extra_bpf_filter
            );
            af_packet.extra_bpf_filter = new_af_packet.extra_bpf_filter.clone();
            restart_agent = !first_run;
        }
        if af_packet.extra_netns_regex != new_af_packet.extra_netns_regex {
            info!(
                "Update inputs.cbpf.af_packet.extra_netns_regex from {:?} to {:?}.",
                af_packet.extra_netns_regex, new_af_packet.extra_netns_regex
            );
            #[cfg(target_os = "linux")]
            if let Some(c) = components.as_ref() {
                Self::set_netns_regex(
                    &c.kubernetes_poller,
                    &af_packet.extra_netns_regex,
                    &new_af_packet.extra_netns_regex,
                );
            }
            af_packet.extra_netns_regex = new_af_packet.extra_netns_regex.clone();
        }
        if af_packet.interface_regex != new_af_packet.interface_regex {
            info!(
                "Update inputs.cbpf.af_packet.interface_regex from {:?} to {:?}.",
                af_packet.interface_regex, new_af_packet.interface_regex
            );
            af_packet.interface_regex = new_af_packet.interface_regex.clone();
            #[cfg(target_os = "windows")]
            if capture_mode == PacketCaptureType::Local {
                callbacks.push(Self::switch_recv_engine);
            }
        }
        if af_packet.src_interfaces != new_af_packet.src_interfaces {
            info!(
                "Update inputs.cbpf.af_packet.src_interfaces from {:?} to {:?}.",
                af_packet.src_interfaces, new_af_packet.src_interfaces
            );
            af_packet.src_interfaces = new_af_packet.src_interfaces.clone();
            restart_agent = !first_run;
        }
        if af_packet.vlan_pcp_in_physical_mirror_traffic
            != new_af_packet.vlan_pcp_in_physical_mirror_traffic
        {
            info!("Update inputs.cbpf.af_packet.vlan_pcp_in_physical_mirror_traffic from {:?} to {:?}.",
                af_packet.vlan_pcp_in_physical_mirror_traffic, new_af_packet.vlan_pcp_in_physical_mirror_traffic);
            af_packet.vlan_pcp_in_physical_mirror_traffic =
                new_af_packet.vlan_pcp_in_physical_mirror_traffic;
            restart_agent = !first_run;
        }
        let tunning = &mut af_packet.tunning;
        let new_tunning = &mut new_af_packet.tunning;
        if tunning.ring_blocks_enabled != new_tunning.ring_blocks_enabled {
            info!(
                "Update inputs.cbpf.af_packet.tunning.ring_blocks_enabled from {:?} to {:?}.",
                tunning.ring_blocks_enabled, new_tunning.ring_blocks_enabled
            );
            tunning.ring_blocks_enabled = new_tunning.ring_blocks_enabled;
            restart_agent = !first_run;
        }
        if tunning.packet_fanout_count != new_tunning.packet_fanout_count {
            info!(
                "Update inputs.cbpf.af_packet.tunning.packet_fanout_count from {:?} to {:?}.",
                tunning.packet_fanout_count, new_tunning.packet_fanout_count
            );
            tunning.packet_fanout_count = new_tunning.packet_fanout_count;
            restart_agent = !first_run;
        }
        if tunning.packet_fanout_mode != new_tunning.packet_fanout_mode {
            info!(
                "Update inputs.cbpf.af_packet.tunning.packet_fanout_mode from {:?} to {:?}.",
                tunning.packet_fanout_mode, new_tunning.packet_fanout_mode
            );
            tunning.packet_fanout_mode = new_tunning.packet_fanout_mode;
            restart_agent = !first_run;
        }
        if tunning.promisc != new_tunning.promisc {
            info!(
                "Update inputs.cbpf.af_packet.tunning.interface_promisc_enabled from {:?} to {:?}.",
                tunning.promisc, new_tunning.promisc
            );
            tunning.promisc = new_tunning.promisc;
            restart_agent = !first_run;
        }
        if tunning.ring_blocks != new_tunning.ring_blocks {
            info!(
                "Update inputs.cbpf.af_packet.tunning.ring_blocks from {:?} to {:?}.",
                tunning.ring_blocks, new_tunning.ring_blocks
            );
            tunning.ring_blocks = new_tunning.ring_blocks;
            restart_agent = !first_run;
        }
        if tunning.socket_version != new_tunning.socket_version {
            info!(
                "Update inputs.cbpf.af_packet.tunning.socket_version from {:?} to {:?}.",
                tunning.socket_version, new_tunning.socket_version
            );
            tunning.socket_version = new_tunning.socket_version;
            restart_dispatcher = !cfg!(target_os = "windows");
        }

        let packet_fanout_count = config.inputs.cbpf.af_packet.tunning.packet_fanout_count;
        let common = &mut config.inputs.cbpf.common;
        let new_common = &mut new_config.user_config.inputs.cbpf.common;
        if common.capture_mode != new_common.capture_mode {
            info!(
                "Update inputs.cbpf.common.capture_mode from {:?} to {:?}.",
                common.capture_mode, new_common.capture_mode
            );
            common.capture_mode = new_common.capture_mode;
            candidate_config.capture_mode = new_common.capture_mode;
            if let Some(c) = components.as_mut() {
                if packet_fanout_count > 1 {
                    info!("capture_mode changes and fanout is enabled, deepflow-agent restart...");
                    crate::utils::notify_exit(public::consts::NORMAL_EXIT_WITH_RESTART);
                    return vec![];
                } else {
                    info!("capture_mode changes, dispatcher restart...");
                    c.clear_dispatcher_components();
                }
            }
            restart_agent = !first_run;
        }
        if candidate_config.capture_mode != PacketCaptureType::Analyzer
            && !running_in_container()
            && !is_kernel_available_for_cgroups()
        // In the environment where cgroups is not supported, we need to check free memory
        {
            // Check and send out exceptions in time
            if let Err(e) = free_memory_check(new_config.environment.max_memory, exception_handler)
            {
                warn!("{}", e);
            }
        }

        let physical_mirror = &mut config.inputs.cbpf.physical_mirror;
        let new_physical_mirror = &mut new_config.user_config.inputs.cbpf.physical_mirror;
        if physical_mirror.packet_dedup_disabled != new_physical_mirror.packet_dedup_disabled {
            info!(
                "Update inputs.cbpf.physical_mirror.packet_dedup_disabled from {:?} to {:?}.",
                physical_mirror.packet_dedup_disabled, new_physical_mirror.packet_dedup_disabled
            );
            physical_mirror.packet_dedup_disabled = new_physical_mirror.packet_dedup_disabled;
            restart_agent = !first_run;
        }
        if physical_mirror.private_cloud_gateway_traffic
            != new_physical_mirror.private_cloud_gateway_traffic
        {
            info!("Update inputs.cbpf.physical_mirror.private_cloud_gateway_traffic from {:?} to {:?}.",
                physical_mirror.private_cloud_gateway_traffic, new_physical_mirror.private_cloud_gateway_traffic);
            physical_mirror.private_cloud_gateway_traffic =
                new_physical_mirror.private_cloud_gateway_traffic;
            restart_agent = !first_run;
        }
        if physical_mirror.default_capture_network_type
            != new_physical_mirror.default_capture_network_type
        {
            info!("Update inputs.cbpf.physical_mirror.default_capture_network_type from {:?} to {:?}.",
                physical_mirror.default_capture_network_type, new_physical_mirror.default_capture_network_type);
            physical_mirror.default_capture_network_type =
                new_physical_mirror.default_capture_network_type;
            restart_agent = !first_run;
        }

        let preprocess = &mut config.inputs.cbpf.preprocess;
        let new_preprocess = &mut new_config.user_config.inputs.cbpf.preprocess;
        if preprocess.packet_segmentation_reassembly
            != new_preprocess.packet_segmentation_reassembly
        {
            info!(
                "Update inputs.cbpf.preprocess.packet_segmentation_reassembly from {:?} to {:?}.",
                preprocess.packet_segmentation_reassembly,
                new_preprocess.packet_segmentation_reassembly
            );
            preprocess.packet_segmentation_reassembly =
                new_preprocess.packet_segmentation_reassembly.clone();
            restart_agent = !first_run;
        }
        if preprocess.tunnel_decap_protocols != new_preprocess.tunnel_decap_protocols {
            info!(
                "Update inputs.cbpf.preprocess.tunnel_decap_protocols from {:?} to {:?}.",
                preprocess.tunnel_decap_protocols, new_preprocess.tunnel_decap_protocols
            );
            preprocess.tunnel_decap_protocols = new_preprocess.tunnel_decap_protocols.clone();
        }
        if preprocess.tunnel_trim_protocols != new_preprocess.tunnel_trim_protocols {
            info!(
                "Update inputs.cbpf.preprocess.tunnel_trim_protocols from {:?} to {:?}.",
                preprocess.tunnel_trim_protocols, new_preprocess.tunnel_trim_protocols
            );
            preprocess.tunnel_trim_protocols = new_preprocess.tunnel_trim_protocols.clone();
            restart_agent = !first_run;
        }

        let special_network = &mut config.inputs.cbpf.special_network;
        let new_special_network = &mut new_config.user_config.inputs.cbpf.special_network;
        if special_network.dpdk.source != new_special_network.dpdk.source {
            info!(
                "Update inputs.cbpf.special_network.dpdk.source from {:?} to {:?}.",
                special_network.dpdk.source, new_special_network.dpdk.source
            );
            special_network.dpdk.source = new_special_network.dpdk.source;
            restart_agent = !first_run;
        }
        if special_network.libpcap.enabled != new_special_network.libpcap.enabled {
            info!(
                "Update inputs.cbpf.special_network.libpcap.enabled from {:?} to {:?}.",
                special_network.libpcap.enabled, new_special_network.libpcap.enabled
            );
            special_network.libpcap.enabled = new_special_network.libpcap.enabled;
            restart_agent = !first_run;
        }

        let physical_switch = &mut special_network.physical_switch;
        let new_physical_switch = &mut new_special_network.physical_switch;
        if physical_switch.netflow_ports != new_physical_switch.netflow_ports {
            info!("Update inputs.cbpf.special_network.physical_switch.netflow_ports  from {:?} to {:?}.",
                physical_switch.netflow_ports , new_physical_switch.netflow_ports );
            physical_switch.netflow_ports = new_physical_switch.netflow_ports.clone();
            restart_agent = !first_run;
        }
        if physical_switch.sflow_ports != new_physical_switch.sflow_ports {
            info!("Update inputs.cbpf.special_network.physical_switch.sflow_ports  from {:?} to {:?}.",
                physical_switch.sflow_ports , new_physical_switch.sflow_ports );
            physical_switch.sflow_ports = new_physical_switch.sflow_ports.clone();
            restart_agent = !first_run;
        }
        if special_network.vhost_user.vhost_socket_path
            != new_special_network.vhost_user.vhost_socket_path
        {
            info!("Update inputs.cbpf.special_network.vhost_user.vhost_socket_path from {:?} to {:?}.",
                special_network.vhost_user.vhost_socket_path, new_special_network.vhost_user.vhost_socket_path);
            special_network.vhost_user.vhost_socket_path =
                new_special_network.vhost_user.vhost_socket_path.clone();
            restart_agent = !first_run;
        }

        let tunning = &mut config.inputs.cbpf.tunning;
        let new_tunning = &mut new_config.user_config.inputs.cbpf.tunning;
        if tunning.dispatcher_queue_enabled != new_tunning.dispatcher_queue_enabled {
            info!(
                "Update inputs.cbpf.tunning.dispatcher_queue_enabled from {:?} to {:?}.",
                tunning.dispatcher_queue_enabled, new_tunning.dispatcher_queue_enabled
            );
            tunning.dispatcher_queue_enabled = new_tunning.dispatcher_queue_enabled;
            restart_agent = !first_run;
        }
        if tunning.max_capture_packet_size != new_tunning.max_capture_packet_size {
            info!(
                "Update inputs.cbpf.tunning.max_capture_packet_size from {:?} to {:?}.",
                tunning.max_capture_packet_size, new_tunning.max_capture_packet_size
            );
            tunning.max_capture_packet_size = new_tunning.max_capture_packet_size;
        }
        if tunning.max_capture_pps != new_tunning.max_capture_pps {
            info!(
                "Update inputs.cbpf.tunning.max_capture_pps from {:?} to {:?}.",
                tunning.max_capture_pps, new_tunning.max_capture_pps
            );
            tunning.max_capture_pps = new_tunning.max_capture_pps;
            callbacks.push(Self::leaky_bucket_callback);
        }
        if tunning.raw_packet_buffer_block_size != new_tunning.raw_packet_buffer_block_size {
            info!(
                "Update inputs.cbpf.tunning.raw_packet_buffer_block_size from {:?} to {:?}.",
                tunning.raw_packet_buffer_block_size, new_tunning.raw_packet_buffer_block_size
            );
            tunning.raw_packet_buffer_block_size = new_tunning.raw_packet_buffer_block_size;
            restart_agent = !first_run;
        }
        if tunning.raw_packet_queue_size != new_tunning.raw_packet_queue_size {
            info!(
                "Update inputs.cbpf.tunning.raw_packet_queue_size from {:?} to {:?}.",
                tunning.raw_packet_queue_size, new_tunning.raw_packet_queue_size
            );
            tunning.raw_packet_queue_size = new_tunning.raw_packet_queue_size;
            restart_agent = !first_run;
        }

        let ebpf = &mut config.inputs.ebpf;
        let new_ebpf = &mut new_config.user_config.inputs.ebpf;
        if ebpf.disabled != new_ebpf.disabled {
            info!(
                "Update inputs.ebpf.disabled from {:?} to {:?}.",
                ebpf.disabled, new_ebpf.disabled
            );
            ebpf.disabled = new_ebpf.disabled;
            restart_agent = !first_run;
        }

        let io_event = &mut ebpf.file.io_event;
        let new_io_event = &mut new_ebpf.file.io_event;
        if io_event.collect_mode != new_io_event.collect_mode {
            info!(
                "Update inputs.ebpf.file.io_event.collect_mode from {:?} to {:?}.",
                io_event.collect_mode, new_io_event.collect_mode
            );
            io_event.collect_mode = new_io_event.collect_mode;
            restart_agent = !first_run;
        }
        if io_event.minimal_duration != new_io_event.minimal_duration {
            info!(
                "Update inputs.ebpf.file.io_event.minimal_duration from {:?} to {:?}.",
                io_event.minimal_duration, new_io_event.minimal_duration
            );
            io_event.minimal_duration = new_io_event.minimal_duration;
            restart_agent = !first_run;
        }
        if ebpf.java_symbol_file_refresh_defer_interval
            != new_ebpf.java_symbol_file_refresh_defer_interval
        {
            info!(
                "Update inputs.ebpf.java_symbol_file_refresh_defer_interval from {:?} to {:?}.",
                ebpf.java_symbol_file_refresh_defer_interval,
                new_ebpf.java_symbol_file_refresh_defer_interval
            );
            ebpf.java_symbol_file_refresh_defer_interval =
                new_ebpf.java_symbol_file_refresh_defer_interval;
            restart_agent = !first_run;
        }

        let on_cpu = &mut ebpf.profile.on_cpu;
        let new_on_cpu = &mut new_ebpf.profile.on_cpu;
        if on_cpu.aggregate_by_cpu != new_on_cpu.aggregate_by_cpu {
            info!(
                "Update inputs.ebpf.profile.on_cpu.aggregate_by_cpu from {:?} to {:?}.",
                on_cpu.aggregate_by_cpu, new_on_cpu.aggregate_by_cpu
            );
            on_cpu.aggregate_by_cpu = new_on_cpu.aggregate_by_cpu;
            restart_agent = !first_run;
        }
        if on_cpu.disabled != new_on_cpu.disabled {
            info!(
                "Update inputs.ebpf.profile.on_cpu.disabled from {:?} to {:?}.",
                on_cpu.disabled, new_on_cpu.disabled
            );
            on_cpu.disabled = new_on_cpu.disabled;
            restart_agent = !first_run;
        }
        if on_cpu.sampling_frequency != new_on_cpu.sampling_frequency {
            info!(
                "Update inputs.ebpf.profile.on_cpu.sampling_frequency from {:?} to {:?}.",
                on_cpu.sampling_frequency, new_on_cpu.sampling_frequency
            );
            on_cpu.sampling_frequency = new_on_cpu.sampling_frequency;
            restart_agent = !first_run;
        }

        let memory = &mut ebpf.profile.memory;
        let new_memory = &mut new_ebpf.profile.memory;
        if memory.disabled != new_memory.disabled {
            info!(
                "Update inputs.ebpf.profile.memory.disabled from {:?} to {:?}.",
                memory.disabled, new_memory.disabled
            );
            memory.disabled = new_memory.disabled;
            restart_agent = !first_run;
        }
        if memory.report_interval != new_memory.report_interval {
            info!(
                "Update inputs.ebpf.profile.memory.report_interval from {:?} to {:?}.",
                memory.report_interval, new_memory.report_interval
            );
            memory.report_interval = new_memory.report_interval;
            restart_agent = !first_run;
        }

        let off_cpu = &mut ebpf.profile.off_cpu;
        let new_off_cpu = &mut new_ebpf.profile.off_cpu;
        if off_cpu.aggregate_by_cpu != new_off_cpu.aggregate_by_cpu {
            info!(
                "Update inputs.ebpf.profile.off_cpu.aggregate_by_cpu from {:?} to {:?}.",
                off_cpu.aggregate_by_cpu, new_off_cpu.aggregate_by_cpu
            );
            off_cpu.aggregate_by_cpu = new_off_cpu.aggregate_by_cpu;
            restart_agent = !first_run;
        }
        if off_cpu.disabled != new_off_cpu.disabled {
            info!(
                "Update inputs.ebpf.profile.off_cpu.disabled from {:?} to {:?}.",
                off_cpu.disabled, new_off_cpu.disabled
            );
            off_cpu.disabled = new_off_cpu.disabled;
            restart_agent = !first_run;
        }
        if off_cpu.min_blocking_time != new_off_cpu.min_blocking_time {
            info!(
                "Update inputs.ebpf.profile.off_cpu.min_blocking_time from {:?} to {:?}.",
                off_cpu.min_blocking_time, new_off_cpu.min_blocking_time
            );
            off_cpu.min_blocking_time = new_off_cpu.min_blocking_time;
            restart_agent = !first_run;
        }

        if ebpf.profile.preprocess.stack_compression
            != new_ebpf.profile.preprocess.stack_compression
        {
            info!(
                "Update inputs.ebpf.profile.preprocess.stack_compression from {:?} to {:?}.",
                ebpf.profile.preprocess.stack_compression,
                new_ebpf.profile.preprocess.stack_compression
            );
            ebpf.profile.preprocess.stack_compression =
                new_ebpf.profile.preprocess.stack_compression;
            restart_agent = !first_run;
        }

        let unwinding = &mut ebpf.profile.unwinding;
        let new_unwinding = &mut new_ebpf.profile.unwinding;
        if unwinding.dwarf_disabled != new_unwinding.dwarf_disabled {
            info!(
                "Update inputs.ebpf.profile.unwinding.dwarf_disabled from {:?} to {:?}.",
                unwinding.dwarf_disabled, new_unwinding.dwarf_disabled
            );
            unwinding.dwarf_disabled = new_unwinding.dwarf_disabled;
            restart_agent = !first_run;
        }
        if unwinding.dwarf_process_map_size != new_unwinding.dwarf_process_map_size {
            info!(
                "Update inputs.ebpf.profile.unwinding.dwarf_process_map_size from {:?} to {:?}.",
                unwinding.dwarf_process_map_size, new_unwinding.dwarf_process_map_size
            );
            unwinding.dwarf_process_map_size = new_unwinding.dwarf_process_map_size;
            restart_agent = !first_run;
        }
        if unwinding.dwarf_regex != new_unwinding.dwarf_regex {
            info!(
                "Update inputs.ebpf.profile.unwinding.dwarf_regex from {:?} to {:?}.",
                unwinding.dwarf_regex, new_unwinding.dwarf_regex
            );
            unwinding.dwarf_regex = new_unwinding.dwarf_regex.clone();
            restart_agent = !first_run;
        }
        if unwinding.dwarf_shard_map_size != new_unwinding.dwarf_shard_map_size {
            info!(
                "Update inputs.ebpf.profile.unwinding.dwarf_shard_map_size from {:?} to {:?}.",
                unwinding.dwarf_shard_map_size, new_unwinding.dwarf_shard_map_size
            );
            unwinding.dwarf_shard_map_size = new_unwinding.dwarf_shard_map_size;
            restart_agent = !first_run;
        }

        let kprobe = &mut ebpf.socket.kprobe;
        let new_kprobe = &mut new_ebpf.socket.kprobe;
        if kprobe.blacklist.ports != new_kprobe.blacklist.ports {
            info!(
                "Update inputs.ebpf.socket.kprobe.blacklist.ports from {:?} to {:?}.",
                kprobe.blacklist.ports, new_kprobe.blacklist.ports
            );
            kprobe.blacklist.ports = new_kprobe.blacklist.ports.clone();
            restart_agent = !first_run;
        }
        if kprobe.whitelist.ports != new_kprobe.whitelist.ports {
            info!(
                "Update inputs.ebpf.socket.kprobe.whitelist.ports from {:?} to {:?}.",
                kprobe.whitelist.ports, new_kprobe.whitelist.ports
            );
            kprobe.whitelist.ports = new_kprobe.whitelist.ports.clone();
            restart_agent = !first_run;
        }

        let uprobe = &mut ebpf.socket.uprobe;
        let new_uprobe = &mut new_ebpf.socket.uprobe;
        if uprobe.tls.enabled != new_uprobe.tls.enabled {
            info!(
                "Update inputs.ebpf.socket.uprobe.tls.enabled from {:?} to {:?}.",
                uprobe.tls.enabled, new_uprobe.tls.enabled
            );
            uprobe.tls.enabled = new_uprobe.tls.enabled;
            restart_agent = !first_run;
        }
        let golang_uprobe = &mut uprobe.golang;
        let new_golang_uprobe = &mut new_uprobe.golang;
        if golang_uprobe.enabled != new_golang_uprobe.enabled {
            info!(
                "Update inputs.ebpf.socket.uprobe.golang.enabled from {:?} to {:?}.",
                golang_uprobe.enabled, new_golang_uprobe.enabled
            );
            golang_uprobe.enabled = new_golang_uprobe.enabled;
            restart_agent = !first_run;
        }
        if golang_uprobe.tracing_timeout != new_golang_uprobe.tracing_timeout {
            info!(
                "Update inputs.ebpf.socket.uprobe.golang.tracing_timeout from {:?} to {:?}.",
                golang_uprobe.tracing_timeout, new_golang_uprobe.tracing_timeout
            );
            golang_uprobe.tracing_timeout = new_golang_uprobe.tracing_timeout;
            restart_agent = !first_run;
        }

        let dpdk_uprobe = &mut uprobe.dpdk;
        let new_dpdk_uprobe = &mut new_uprobe.dpdk;
        if dpdk_uprobe.command != new_dpdk_uprobe.command {
            info!(
                "Update inputs.ebpf.socket.uprobe.dpdk.command from {:?} to {:?}.",
                dpdk_uprobe.command, new_dpdk_uprobe.command
            );
            dpdk_uprobe.command = new_dpdk_uprobe.command.clone();
            restart_agent = !first_run;
        }
        if dpdk_uprobe.rx_hooks != new_dpdk_uprobe.rx_hooks {
            info!(
                "Update inputs.ebpf.socket.uprobe.dpdk.rx_hooks from {:?} to {:?}.",
                dpdk_uprobe.rx_hooks, new_dpdk_uprobe.rx_hooks
            );
            dpdk_uprobe.rx_hooks = new_dpdk_uprobe.rx_hooks.clone();
            restart_agent = !first_run;
        }
        if dpdk_uprobe.tx_hooks != new_dpdk_uprobe.tx_hooks {
            info!(
                "Update inputs.ebpf.socket.uprobe.dpdk.tx_hooks from {:?} to {:?}.",
                dpdk_uprobe.tx_hooks, new_dpdk_uprobe.tx_hooks
            );
            dpdk_uprobe.tx_hooks = new_dpdk_uprobe.tx_hooks.clone();
            restart_agent = !first_run;
        }

        let preprocess = &mut ebpf.socket.preprocess;
        let new_preprocess = &mut new_ebpf.socket.preprocess;
        if preprocess.out_of_order_reassembly_cache_size
            != new_preprocess.out_of_order_reassembly_cache_size
        {
            info!("Update inputs.ebpf.socket.preprocess.out_of_order_reassembly_cache_size from {:?} to {:?}.",
                preprocess.out_of_order_reassembly_cache_size, new_preprocess.out_of_order_reassembly_cache_size);
            preprocess.out_of_order_reassembly_cache_size =
                new_preprocess.out_of_order_reassembly_cache_size;
            restart_agent = !first_run;
        }
        if preprocess.out_of_order_reassembly_protocols
            != new_preprocess.out_of_order_reassembly_protocols
        {
            info!("Update inputs.ebpf.socket.preprocess.out_of_order_reassembly_protocols from {:?} to {:?}.",
                preprocess.out_of_order_reassembly_protocols, new_preprocess.out_of_order_reassembly_protocols);
            preprocess.out_of_order_reassembly_protocols =
                new_preprocess.out_of_order_reassembly_protocols.clone();
            restart_agent = !first_run;
        }
        if preprocess.segmentation_reassembly_protocols
            != new_preprocess.segmentation_reassembly_protocols
        {
            info!("Update inputs.ebpf.socket.preprocess.segmentation_reassembly_protocols from {:?} to {:?}.",
                preprocess.segmentation_reassembly_protocols, new_preprocess.segmentation_reassembly_protocols);
            preprocess.segmentation_reassembly_protocols =
                new_preprocess.segmentation_reassembly_protocols.clone();
            restart_agent = !first_run;
        }

        let tunning = &mut ebpf.socket.tunning;
        let new_tunning = &mut new_ebpf.socket.tunning;
        if tunning.map_prealloc_disabled != new_tunning.map_prealloc_disabled {
            info!(
                "Update inputs.ebpf.socket.tunning.map_prealloc_disabled from {:?} to {:?}.",
                tunning.map_prealloc_disabled, new_tunning.map_prealloc_disabled
            );
            tunning.map_prealloc_disabled = new_tunning.map_prealloc_disabled;
            restart_agent = !first_run;
        }
        if tunning.syscall_trace_id_disabled != new_tunning.syscall_trace_id_disabled {
            info!(
                "Update inputs.ebpf.socket.tunning.syscall_trace_id_disabled from {:?} to {:?}.",
                tunning.syscall_trace_id_disabled, new_tunning.syscall_trace_id_disabled
            );
            tunning.syscall_trace_id_disabled = new_tunning.syscall_trace_id_disabled;
            restart_agent = !first_run;
        }
        if tunning.max_capture_rate != new_tunning.max_capture_rate {
            info!(
                "Update inputs.ebpf.socket.tunning.max_capture_rate from {:?} to {:?}.",
                tunning.max_capture_rate, new_tunning.max_capture_rate
            );
            tunning.max_capture_rate = new_tunning.max_capture_rate;
            restart_agent = !first_run;
        }

        let tunning = &mut ebpf.tunning;
        let new_tunning = &mut new_ebpf.tunning;
        if tunning.collector_queue_size != new_tunning.collector_queue_size {
            info!(
                "Update inputs.ebpf.tunning.collector_queue_size from {:?} to {:?}.",
                tunning.collector_queue_size, new_tunning.collector_queue_size
            );
            tunning.collector_queue_size = new_tunning.collector_queue_size;
            restart_agent = !first_run;
        }
        if tunning.kernel_ring_size != new_tunning.kernel_ring_size {
            info!(
                "Update inputs.ebpf.tunning.kernel_ring_size from {:?} to {:?}.",
                tunning.kernel_ring_size, new_tunning.kernel_ring_size
            );
            tunning.kernel_ring_size = new_tunning.kernel_ring_size;
            restart_agent = !first_run;
        }
        if tunning.max_socket_entries != new_tunning.max_socket_entries {
            info!(
                "Update inputs.ebpf.tunning.max_socket_entries from {:?} to {:?}.",
                tunning.max_socket_entries, new_tunning.max_socket_entries
            );
            tunning.max_socket_entries = new_tunning.max_socket_entries;
            restart_agent = !first_run;
        }
        if tunning.max_trace_entries != new_tunning.max_trace_entries {
            info!(
                "Update inputs.ebpf.tunning.max_trace_entries from {:?} to {:?}.",
                tunning.max_trace_entries, new_tunning.max_trace_entries
            );
            tunning.max_trace_entries = new_tunning.max_trace_entries;
            restart_agent = !first_run;
        }
        if tunning.perf_pages_count != new_tunning.perf_pages_count {
            info!(
                "Update inputs.ebpf.tunning.perf_pages_count from {:?} to {:?}.",
                tunning.perf_pages_count, new_tunning.perf_pages_count
            );
            tunning.perf_pages_count = new_tunning.perf_pages_count;
            restart_agent = !first_run;
        }
        if tunning.socket_map_reclaim_threshold != new_tunning.socket_map_reclaim_threshold {
            info!(
                "Update inputs.ebpf.tunning.socket_map_reclaim_threshold from {:?} to {:?}.",
                tunning.socket_map_reclaim_threshold, new_tunning.socket_map_reclaim_threshold
            );
            tunning.socket_map_reclaim_threshold = new_tunning.socket_map_reclaim_threshold;
            restart_agent = !first_run;
        }
        if tunning.userspace_worker_threads != new_tunning.userspace_worker_threads {
            info!(
                "Update inputs.ebpf.tunning.userspace_worker_threads from {:?} to {:?}.",
                tunning.userspace_worker_threads, new_tunning.userspace_worker_threads
            );
            tunning.userspace_worker_threads = new_tunning.userspace_worker_threads;
            restart_agent = !first_run;
        }

        let integration = &mut config.inputs.integration;
        let new_integration = &mut new_config.user_config.inputs.integration;
        if integration.enabled != new_integration.enabled {
            info!(
                "Update inputs.integration.enabled from {:?} to {:?}.",
                integration.enabled, new_integration.enabled
            );
            integration.enabled = new_integration.enabled;
            restart_agent = !first_run;
        }
        if integration.compression != new_integration.compression {
            info!(
                "Update inputs.integration.compression from {:?} to {:?}.",
                integration.compression, new_integration.compression
            );
            integration.compression = new_integration.compression.clone();
            restart_agent = !first_run;
        }
        if integration.feature_control != new_integration.feature_control {
            info!(
                "Update inputs.integration.feature_control from {:?} to {:?}.",
                integration.feature_control, new_integration.feature_control
            );
            integration.feature_control = new_integration.feature_control;
            restart_agent = !first_run;
        }
        if integration.listen_port != new_integration.listen_port {
            info!(
                "Update inputs.integration.listen_port from {:?} to {:?}.",
                integration.listen_port, new_integration.listen_port
            );
            integration.listen_port = new_integration.listen_port;
            restart_agent = !first_run;
        }
        if integration.prometheus_extra_labels != new_integration.prometheus_extra_labels {
            info!(
                "Update inputs.integration.prometheus_extra_labels from {:?} to {:?}.",
                integration.prometheus_extra_labels, new_integration.prometheus_extra_labels
            );
            integration.prometheus_extra_labels = new_integration.prometheus_extra_labels.clone();
            restart_agent = !first_run;
        }

        let resources = &mut config.inputs.resources;
        let new_resources = &mut new_config.user_config.inputs.resources;
        if resources.push_interval != new_resources.push_interval {
            info!(
                "Update inputs.resources.push_interval from {:?} to {:?}.",
                resources.push_interval, new_resources.push_interval
            );
            resources.push_interval = new_resources.push_interval;
        }

        let kubernetes = &mut resources.kubernetes;
        let new_kubernetes = &mut new_resources.kubernetes;
        if kubernetes.api_list_max_interval != new_kubernetes.api_list_max_interval {
            info!(
                "Update inputs.resources.kubernetes.api_list_max_interval from {:?} to {:?}.",
                kubernetes.api_list_max_interval, new_kubernetes.api_list_max_interval
            );
            kubernetes.api_list_max_interval = new_kubernetes.api_list_max_interval;
            restart_agent = !first_run;
        }
        if kubernetes.api_list_page_size != new_kubernetes.api_list_page_size {
            info!(
                "Update inputs.resources.kubernetes.api_list_page_size from {:?} to {:?}.",
                kubernetes.api_list_page_size, new_kubernetes.api_list_page_size
            );
            kubernetes.api_list_page_size = new_kubernetes.api_list_page_size;
            restart_agent = !first_run;
        }
        if kubernetes.api_resources != new_kubernetes.api_resources {
            info!(
                "Update inputs.resources.kubernetes.api_resources from {:?} to {:?}.",
                kubernetes.api_resources, new_kubernetes.api_resources
            );
            kubernetes.api_resources = new_kubernetes.api_resources.clone();
            restart_agent = !first_run;
        }
        if kubernetes.ingress_flavour != new_kubernetes.ingress_flavour {
            info!(
                "Update inputs.resources.kubernetes.ingress_flavour from {:?} to {:?}.",
                kubernetes.ingress_flavour, new_kubernetes.ingress_flavour
            );
            kubernetes.ingress_flavour = new_kubernetes.ingress_flavour.clone();
            restart_agent = !first_run;
        }
        if kubernetes.kubernetes_namespace != new_kubernetes.kubernetes_namespace {
            info!(
                "Update inputs.resources.kubernetes.kubernetes_namespace from {:?} to {:?}.",
                kubernetes.kubernetes_namespace, new_kubernetes.kubernetes_namespace
            );
            kubernetes.kubernetes_namespace = new_kubernetes.kubernetes_namespace.clone();
            restart_agent = !first_run;
        }
        if kubernetes.pod_mac_collection_method != new_kubernetes.pod_mac_collection_method {
            info!(
                "Update inputs.resources.kubernetes.pod_mac_collection_method from {:?} to {:?}.",
                kubernetes.pod_mac_collection_method, new_kubernetes.pod_mac_collection_method
            );
            kubernetes.pod_mac_collection_method = new_kubernetes.pod_mac_collection_method;
            restart_agent = !first_run;
        }

        let private_cloud = &mut resources.private_cloud;
        let new_private_cloud = &mut new_resources.private_cloud;
        if private_cloud.hypervisor_resource_enabled
            != new_private_cloud.hypervisor_resource_enabled
        {
            info!("Update inputs.resources.private_cloud.hypervisor_resource_enabled from {:?} to {:?}.",
                private_cloud.hypervisor_resource_enabled, new_private_cloud.hypervisor_resource_enabled);
            private_cloud.hypervisor_resource_enabled =
                new_private_cloud.hypervisor_resource_enabled;
            restart_agent = !first_run;
        }
        if private_cloud.vm_mac_mapping_script != new_private_cloud.vm_mac_mapping_script {
            info!(
                "Update inputs.resources.private_cloud.vm_mac_mapping_script from {:?} to {:?}.",
                private_cloud.vm_mac_mapping_script, new_private_cloud.vm_mac_mapping_script
            );
            private_cloud.vm_mac_mapping_script = new_private_cloud.vm_mac_mapping_script.clone();
            restart_agent = !first_run;
        }
        if private_cloud.vm_mac_source != new_private_cloud.vm_mac_source {
            info!(
                "Update inputs.resources.private_cloud.vm_mac_source from {:?} to {:?}.",
                private_cloud.vm_mac_source, new_private_cloud.vm_mac_source
            );
            private_cloud.vm_mac_source = new_private_cloud.vm_mac_source;
        }
        if private_cloud.vm_xml_directory != new_private_cloud.vm_xml_directory {
            info!(
                "Update inputs.resources.private_cloud.vm_xml_directory from {:?} to {:?}.",
                private_cloud.vm_xml_directory, new_private_cloud.vm_xml_directory
            );
            private_cloud.vm_xml_directory = new_private_cloud.vm_xml_directory.clone();
        }

        let proc = &mut config.inputs.proc;
        let new_proc = &mut new_config.user_config.inputs.proc;
        if proc.enabled != new_proc.enabled {
            info!(
                "Update inputs.proc.enabled from {:?} to {:?}.",
                proc.enabled, new_proc.enabled
            );
            proc.enabled = new_proc.enabled;
        }
        if proc.min_lifetime != new_proc.min_lifetime {
            info!(
                "Update inputs.proc.min_lifetime from {:?} to {:?}.",
                proc.min_lifetime, new_proc.min_lifetime
            );
            proc.min_lifetime = new_proc.min_lifetime;
            restart_agent = !first_run;
        }
        let mut process_matcher_update = false;
        if proc.proc_dir_path != new_proc.proc_dir_path {
            info!(
                "Update inputs.proc.proc_dir_path from {:?} to {:?}.",
                proc.proc_dir_path, new_proc.proc_dir_path
            );
            proc.proc_dir_path = new_proc.proc_dir_path.clone();
            process_matcher_update = true;
            restart_agent = !first_run;
        }
        if proc.process_matcher != new_proc.process_matcher {
            info!(
                "Update inputs.proc.process_matcher from {:?} to {:?}.",
                proc.process_matcher, new_proc.process_matcher
            );
            proc.process_matcher = new_proc.process_matcher.clone();
            process_matcher_update = true;
            restart_agent = !first_run;
        }
        if proc.symbol_table != new_proc.symbol_table {
            info!(
                "Update inputs.proc.symbol_table from {:?} to {:?}.",
                proc.symbol_table, new_proc.symbol_table
            );
            proc.symbol_table = new_proc.symbol_table;
            restart_agent = !first_run;
        }
        if proc.socket_info_sync_interval != new_proc.socket_info_sync_interval {
            info!(
                "Update inputs.proc.socket_info_sync_interval from {:?} to {:?}.",
                proc.socket_info_sync_interval, new_proc.socket_info_sync_interval
            );
            proc.socket_info_sync_interval = new_proc.socket_info_sync_interval;
            restart_agent = !first_run;
        }

        let tag = &mut proc.tag_extraction;
        let new_tag = &mut new_proc.tag_extraction;
        if tag.exec_username != new_tag.exec_username {
            info!(
                "Update inputs.proc.tag_extraction.exec_username from {:?} to {:?}.",
                tag.exec_username, new_tag.exec_username
            );
            tag.exec_username = new_tag.exec_username.clone();
            process_matcher_update = true;
            restart_agent = !first_run;
        }
        if tag.script_command != new_tag.script_command {
            info!(
                "Update inputs.proc.tag_extraction.script_command from {:?} to {:?}.",
                tag.script_command, new_tag.script_command
            );
            tag.script_command = new_tag.script_command.clone();
            process_matcher_update = true;
            restart_agent = !first_run;
        }

        if process_matcher_update {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            if let Some(c) = components.as_ref() {
                c.process_listener.on_config_change(
                    &new_config.user_config.inputs.proc.process_matcher,
                    new_config.user_config.inputs.proc.proc_dir_path.clone(),
                    new_config
                        .user_config
                        .inputs
                        .proc
                        .tag_extraction
                        .exec_username
                        .clone(),
                    new_config
                        .user_config
                        .inputs
                        .proc
                        .tag_extraction
                        .script_command
                        .clone(),
                );
            }
        }

        // global
        let alerts = &mut config.global.alerts;
        let new_alerts = &mut new_config.user_config.global.alerts;
        if alerts.check_core_file_disabled != new_alerts.check_core_file_disabled {
            info!(
                "Update global.alerts.check_core_file_disabled from {:?} to {:?}.",
                alerts.check_core_file_disabled, new_alerts.check_core_file_disabled
            );
            alerts.check_core_file_disabled = new_alerts.check_core_file_disabled;
            restart_agent = !first_run;
        }
        if alerts.process_threshold != new_alerts.process_threshold {
            info!(
                "Update global.alerts.process_threshold from {:?} to {:?}.",
                alerts.process_threshold, new_alerts.process_threshold
            );
            alerts.process_threshold = new_alerts.process_threshold;
        }
        if alerts.thread_threshold != new_alerts.thread_threshold {
            info!(
                "Update global.alerts.thread_threshold from {:?} to {:?}.",
                alerts.thread_threshold, new_alerts.thread_threshold
            );
            alerts.thread_threshold = new_alerts.thread_threshold;
        }

        let circuit_breakers = &mut config.global.circuit_breakers;
        let new_circuit_breakers = &mut new_config.user_config.global.circuit_breakers;
        let relative_sys_load = &mut circuit_breakers.relative_sys_load;
        let new_relative_sys_load = &mut new_circuit_breakers.relative_sys_load;
        if relative_sys_load.recovery_threshold != new_relative_sys_load.recovery_threshold {
            info!("Update global.circuit_breakers.relative_sys_load.recovery_threshold from {:?} to {:?}.",
                relative_sys_load.recovery_threshold, new_relative_sys_load.recovery_threshold);
            relative_sys_load.recovery_threshold = new_relative_sys_load.recovery_threshold;
        }
        if relative_sys_load.metric != new_relative_sys_load.metric {
            info!(
                "Update global.circuit_breakers.relative_sys_load.metric from {:?} to {:?}.",
                relative_sys_load.metric, new_relative_sys_load.metric
            );
            relative_sys_load.metric = new_relative_sys_load.metric;
        }
        if relative_sys_load.trigger_threshold != new_relative_sys_load.trigger_threshold {
            info!("Update global.circuit_breakers.relative_sys_load.trigger_threshold from {:?} to {:?}.",
                relative_sys_load.trigger_threshold, new_relative_sys_load.trigger_threshold);
            relative_sys_load.trigger_threshold = new_relative_sys_load.trigger_threshold;
        }
        let sys_memory_percentage = &mut circuit_breakers.sys_memory_percentage;
        let new_sys_memory_percentage = &mut new_circuit_breakers.sys_memory_percentage;
        if sys_memory_percentage.trigger_threshold != new_sys_memory_percentage.trigger_threshold {
            info!("Update global.circuit_breakers.sys_memory_percentage.trigger_threshold from {:?} to {:?}.",
                sys_memory_percentage.trigger_threshold, new_sys_memory_percentage.trigger_threshold);
            sys_memory_percentage.trigger_threshold = new_sys_memory_percentage.trigger_threshold;
        }
        if sys_memory_percentage.metric != new_sys_memory_percentage.metric {
            info!(
                "Update global.circuit_breakers.sys_memory_percentage.metric from {:?} to {:?}.",
                sys_memory_percentage.metric, new_sys_memory_percentage.metric
            );
            sys_memory_percentage.metric = new_sys_memory_percentage.metric;
        }
        let tx_throughput = &mut circuit_breakers.tx_throughput;
        let new_tx_throughput = &mut new_circuit_breakers.tx_throughput;
        if tx_throughput.trigger_threshold != new_tx_throughput.trigger_threshold {
            info!(
                "Update global.circuit_breakers.tx_throughput.trigger_threshold from {:?} to {:?}.",
                tx_throughput.trigger_threshold, new_tx_throughput.trigger_threshold
            );
            tx_throughput.trigger_threshold = new_tx_throughput.trigger_threshold;
            if let Some(components) = &components {
                components
                    .npb_bandwidth_watcher
                    .set_nic_rate(new_tx_throughput.trigger_threshold);
            }
        }
        if tx_throughput.throughput_monitoring_interval
            != new_tx_throughput.throughput_monitoring_interval
        {
            info!("Update global.circuit_breakers.tx_throughput.throughput_monitoring_interval from {:?} to {:?}.",
                tx_throughput.throughput_monitoring_interval, new_tx_throughput.throughput_monitoring_interval);
            tx_throughput.throughput_monitoring_interval =
                new_tx_throughput.throughput_monitoring_interval;

            if let Some(components) = &components {
                components
                    .npb_bandwidth_watcher
                    .set_interval(tx_throughput.throughput_monitoring_interval.as_secs());
            }
        }

        let common = &mut config.global.common;
        let new_common = &mut new_config.user_config.global.common;
        if common.kubernetes_api_enabled != new_common.kubernetes_api_enabled {
            info!(
                "Update global.common.kubernetes_api_enabled from {:?} to {:?}.",
                common.enabled, new_common.enabled
            );
            common.kubernetes_api_enabled = new_common.kubernetes_api_enabled;
        }
        if common.enabled != new_common.enabled {
            callbacks.push(if new_common.enabled {
                info!(
                    "Update global.common.enabled from {:?} to {:?}, dispatcher start... ",
                    common.enabled, new_common.enabled
                );
                Self::start_dispatcher
            } else {
                info!(
                    "Update global.common.enabled from {:?} to {:?}, dispatcher stop...",
                    common.enabled, new_common.enabled
                );
                Self::stop_dispatcher
            });
            common.enabled = new_common.enabled;
        }
        if common.region_id != new_common.region_id {
            info!(
                "Update global.common.region_id from {:?} to {:?}.",
                common.region_id, new_common.region_id
            );
            common.region_id = new_common.region_id;
        }
        if common.pod_cluster_id != new_common.pod_cluster_id {
            info!(
                "Update global.common.region_id from {:?} to {:?}.",
                common.pod_cluster_id, new_common.pod_cluster_id
            );
            common.pod_cluster_id = new_common.pod_cluster_id;
        }
        if common.vpc_id != new_common.vpc_id {
            info!(
                "Update global.common.vpc_id from {:?} to {:?}.",
                common.vpc_id, new_common.vpc_id
            );
            common.vpc_id = new_common.vpc_id;
        }
        if common.agent_id != new_common.agent_id {
            info!(
                "Update global.common.agent_id from {:?} to {:?}.",
                common.agent_id, new_common.agent_id
            );
            common.agent_id = new_common.agent_id;
        }
        if common.team_id != new_common.team_id {
            info!(
                "Update global.common.team_id from {:?} to {:?}.",
                common.team_id, new_common.team_id
            );
            common.team_id = new_common.team_id;
        }
        if common.organize_id != new_common.organize_id {
            info!(
                "Update global.common.organize_id from {:?} to {:?}.",
                common.organize_id, new_common.organize_id
            );
            common.organize_id = new_common.organize_id;
        }
        if common.agent_type != new_common.agent_type {
            info!(
                "Update global.common.agent_type from {:?} to {:?}.",
                common.agent_type, new_common.agent_type
            );
            common.agent_type = new_common.agent_type;
        }
        if common.secret_key != new_common.secret_key {
            info!(
                "Update global.common.secret_key from {:?} to {:?}.",
                common.secret_key, new_common.secret_key
            );
            common.secret_key = new_common.secret_key.clone();
        }

        let communication = &mut config.global.communication;
        let new_communication = &mut new_config.user_config.global.communication;
        if communication.request_via_nat_ip != new_communication.request_via_nat_ip {
            info!(
                "Update global.communication.request_via_nat_ip from {:?} to {:?}.",
                communication.request_via_nat_ip, new_communication.request_via_nat_ip
            );
            communication.request_via_nat_ip = new_communication.request_via_nat_ip;
        }
        if communication.grpc_buffer_size != new_communication.grpc_buffer_size {
            info!(
                "Update global.communication.grpc_buffer_size from {:?} to {:?}.",
                communication.grpc_buffer_size, new_communication.grpc_buffer_size
            );
            communication.grpc_buffer_size = new_communication.grpc_buffer_size;
            restart_agent = !first_run;
        }
        if communication.ingester_ip != new_communication.ingester_ip {
            info!(
                "Update global.communication.ingester_ip from {:?} to {:?}.",
                communication.ingester_ip, new_communication.ingester_ip
            );
            communication.ingester_ip = new_communication.ingester_ip.clone();
        }
        if communication.ingester_port != new_communication.ingester_port {
            info!(
                "Update global.communication.ingester_port from {:?} to {:?}.",
                communication.ingester_port, new_communication.ingester_port
            );
            communication.ingester_port = new_communication.ingester_port;
        }
        if communication.max_escape_duration != new_communication.max_escape_duration {
            info!(
                "Update global.communication.max_escape_duration from {:?} to {:?}.",
                communication.max_escape_duration, new_communication.max_escape_duration
            );
            communication.max_escape_duration = new_communication.max_escape_duration;
        }
        if communication.proactive_request_interval != new_communication.proactive_request_interval
        {
            info!(
                "Update global.communication.proactive_request_interval from {:?} to {:?}.",
                communication.proactive_request_interval,
                new_communication.proactive_request_interval
            );
            communication.proactive_request_interval = new_communication.proactive_request_interval;
        }
        if communication.proxy_controller_ip != new_communication.proxy_controller_ip {
            info!(
                "Update global.communication.proxy_controller_ip from {:?} to {:?}.",
                communication.proxy_controller_ip, new_communication.proxy_controller_ip
            );
            communication.proxy_controller_ip = new_communication.proxy_controller_ip.clone();
        }
        if communication.proxy_controller_port != new_communication.proxy_controller_port {
            info!(
                "Update global.communication.proxy_controller_port from {:?} to {:?}.",
                communication.proxy_controller_port, new_communication.proxy_controller_port
            );
            communication.proxy_controller_port = new_communication.proxy_controller_port;
        }

        let limits = &mut config.global.limits;
        let new_limits = &mut new_config.user_config.global.limits;
        if limits.local_log_retention != new_limits.local_log_retention {
            info!(
                "Update global.limits.local_log_retention from {:?} to {:?}.",
                limits.local_log_retention, new_limits.local_log_retention
            );
            if Self::set_log_retention(logger_handle, &new_limits.local_log_retention, &log_file) {
                limits.local_log_retention = new_limits.local_log_retention;
            } else {
                new_limits.local_log_retention = limits.local_log_retention;
            }
        }
        if limits.max_local_log_file_size != new_limits.max_local_log_file_size {
            info!(
                "Update global.limits.max_local_log_file_size from {:?} to {:?}.",
                limits.max_local_log_file_size, new_limits.max_local_log_file_size
            );
            limits.max_local_log_file_size = new_limits.max_local_log_file_size;
        }
        if limits.max_log_backhaul_rate != new_limits.max_log_backhaul_rate {
            info!(
                "Update global.limits.max_log_backhaul_rate from {:?} to {:?}.",
                limits.max_log_backhaul_rate, new_limits.max_log_backhaul_rate
            );
            limits.max_log_backhaul_rate = new_limits.max_log_backhaul_rate;
        }
        if limits.max_memory != new_limits.max_memory {
            info!(
                "Update global.limits.max_memory from {:?} to {:?}.",
                limits.max_memory, new_limits.max_memory
            );
            limits.max_memory = new_limits.max_memory;
        }
        if limits.max_millicpus != new_limits.max_millicpus {
            info!(
                "Update global.limits.max_millicpus from {:?} to {:?}.",
                limits.max_millicpus, new_limits.max_millicpus
            );
            limits.max_millicpus = new_limits.max_millicpus;
        }

        let ntp = &mut config.global.ntp;
        let new_ntp = &mut new_config.user_config.global.ntp;
        if ntp.enabled != new_ntp.enabled {
            info!(
                "Update global.ntp.enabled from {:?} to {:?}.",
                ntp.enabled, new_ntp.enabled
            );
            ntp.enabled = new_ntp.enabled;
        }
        if ntp.max_drift != new_ntp.max_drift {
            info!(
                "Update global.ntp.max_drift from {:?} to {:?}.",
                ntp.max_drift, new_ntp.max_drift
            );
            ntp.max_drift = new_ntp.max_drift;
            restart_agent = !first_run;
        }
        if ntp.min_drift != new_ntp.min_drift {
            info!(
                "Update global.ntp.min_drift from {:?} to {:?}.",
                ntp.min_drift, new_ntp.min_drift
            );
            ntp.min_drift = new_ntp.min_drift;
            restart_agent = !first_run;
        }

        let self_monitoring = &mut config.global.self_monitoring;
        let new_self_monitoring = &mut new_config.user_config.global.self_monitoring;
        let debug = &mut self_monitoring.debug;
        let new_debug = &mut new_self_monitoring.debug;
        if debug.enabled != new_debug.enabled {
            info!(
                "Update global.self_monitoring.debug.enabled from {:?} to {:?}.",
                debug.enabled, new_debug.enabled
            );
            debug.enabled = debug.enabled;
        }
        if debug.debug_metrics_enabled != new_debug.debug_metrics_enabled {
            info!(
                "Update global.self_monitoring.debug.debug_metrics_enabled from {:?} to {:?}.",
                debug.debug_metrics_enabled, new_debug.debug_metrics_enabled
            );
            debug.debug_metrics_enabled = debug.debug_metrics_enabled;
            restart_agent = !first_run;
        }
        if debug.local_udp_port != new_debug.local_udp_port {
            info!(
                "Update global.self_monitoring.debug.local_udp_port from {:?} to {:?}.",
                debug.local_udp_port, new_debug.local_udp_port
            );
            debug.local_udp_port = debug.local_udp_port;
            restart_agent = !first_run;
        }

        if self_monitoring.hostname != new_self_monitoring.hostname {
            info!(
                "Update global.self_monitoring.hostname from {:?} to {:?}.",
                self_monitoring.hostname, new_self_monitoring.hostname
            );
            if new_self_monitoring.hostname.is_empty() {
                info!(
                    "Hostname is empty, keep the last hostname {}",
                    self_monitoring.hostname
                );
                new_self_monitoring.hostname = self_monitoring.hostname.clone();
            } else {
                self_monitoring.hostname = new_self_monitoring.hostname.clone();
            }
        }
        if self_monitoring.interval != new_self_monitoring.interval {
            info!(
                "Update global.self_monitoring.interval from {:?} to {:?}.",
                self_monitoring.interval, new_self_monitoring.interval
            );
            self_monitoring.interval = new_self_monitoring.interval;
        }

        let log = &mut self_monitoring.log;
        let new_log = &mut new_self_monitoring.log;
        if log.log_backhaul_enabled != new_log.log_backhaul_enabled {
            info!(
                "Update global.self_monitoring.log.log_backhaul_enabled from {:?} to {:?}.",
                log.log_backhaul_enabled, new_log.log_backhaul_enabled
            );
            log.log_backhaul_enabled = new_log.log_backhaul_enabled;
        }
        if log.log_file != new_log.log_file {
            info!(
                "Update global.self_monitoring.log.log_file from {:?} to {:?}.",
                log.log_file, new_log.log_file
            );
            log.log_file = new_log.log_file.clone();
            restart_agent = !first_run;
        }
        if log.log_level != new_log.log_level {
            info!(
                "Update global.self_monitoring.log.log_level from {:?} to {:?}.",
                log.log_level, new_log.log_level
            );
            if Self::set_log_level(logger_handle, &new_log.log_level) {
                log.log_level = new_log.log_level;
            } else {
                new_log.log_level = log.log_level;
            }
        }
        if self_monitoring.profile.enabled != new_self_monitoring.profile.enabled {
            info!(
                "Update global.self_monitoring.profile.enabled from {:?} to {:?}.",
                self_monitoring.profile.enabled, new_self_monitoring.profile.enabled
            );
            self_monitoring.profile.enabled = new_self_monitoring.profile.enabled;
            restart_agent = !first_run;
        }

        let standalone_mode = &mut config.global.standalone_mode;
        let new_standalone_mode = &mut new_config.user_config.global.standalone_mode;
        if standalone_mode.data_file_dir != new_standalone_mode.data_file_dir {
            info!(
                "Update global.standalone_mode.data_file_dir from {:?} to {:?}.",
                standalone_mode.data_file_dir, new_standalone_mode.data_file_dir
            );
            standalone_mode.data_file_dir = new_standalone_mode.data_file_dir.clone();
            restart_agent = !first_run;
        }
        if standalone_mode.max_data_file_size != new_standalone_mode.max_data_file_size {
            info!(
                "Update global.standalone_mode.max_data_file_size from {:?} to {:?}.",
                standalone_mode.max_data_file_size, new_standalone_mode.max_data_file_size
            );
            standalone_mode.max_data_file_size = new_standalone_mode.max_data_file_size;
            restart_agent = !first_run;
        }

        let tunning = &mut config.global.tunning;
        let new_tunning = &mut new_config.user_config.global.tunning;
        if tunning.idle_memory_trimming != new_tunning.idle_memory_trimming {
            info!(
                "Update global.tunning.idle_memory_trimming from {:?} to {:?}.",
                tunning.idle_memory_trimming, new_tunning.idle_memory_trimming
            );
            tunning.idle_memory_trimming = new_tunning.idle_memory_trimming;
            restart_agent = !first_run;
        }
        if tunning.cpu_affinity != new_tunning.cpu_affinity {
            info!(
                "Update global.tunning.cpu_affinity from {:?} to {:?}.",
                tunning.cpu_affinity, new_tunning.cpu_affinity
            );
            tunning.cpu_affinity = new_tunning.cpu_affinity.clone();
            #[cfg(any(target_os = "linux", target_os = "android"))]
            {
                Self::set_cpu_affinity(&tunning.cpu_affinity, &mut cpu_set);
                new_config.dispatcher.cpu_set = cpu_set;
            }
            restart_agent = !first_run;
        }
        if tunning.process_scheduling_priority != new_tunning.process_scheduling_priority {
            info!(
                "Update global.tunning.process_scheduling_priority from {:?} to {:?}.",
                tunning.process_scheduling_priority, new_tunning.process_scheduling_priority
            );
            tunning.process_scheduling_priority = new_tunning.process_scheduling_priority;
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::set_process_scheduling_priority(tunning.process_scheduling_priority);
            restart_agent = !first_run;
        }
        if tunning.resource_monitoring_interval != new_tunning.resource_monitoring_interval {
            info!(
                "Update global.tunning.resource_monitoring_interval from {:?} to {:?}.",
                tunning.resource_monitoring_interval, new_tunning.resource_monitoring_interval
            );
            tunning.resource_monitoring_interval = new_tunning.resource_monitoring_interval;
            restart_agent = !first_run;
        }

        // dev
        let dev = &mut config.dev;
        let new_dev = &mut new_config.user_config.dev;
        if dev.feature_flags != new_dev.feature_flags {
            info!(
                "Update dev.feature_flags from {:?} to {:?}.",
                dev.feature_flags, new_dev.feature_flags
            );
            dev.feature_flags = new_dev.feature_flags.clone();
            restart_agent = !first_run;
        }

        // output
        let outputs = &mut config.outputs;
        let new_outputs = &mut new_config.user_config.outputs;
        let socket = &mut outputs.socket;
        let new_socket = &mut new_outputs.socket;
        if socket.multiple_sockets_to_ingester != new_socket.multiple_sockets_to_ingester {
            info!(
                "Update outputs.socket.multiple_sockets_to_ingester from {:?} to {:?}.",
                socket.multiple_sockets_to_ingester, new_socket.multiple_sockets_to_ingester
            );
            socket.multiple_sockets_to_ingester = new_socket.multiple_sockets_to_ingester;
        }
        if socket.raw_udp_qos_bypass != new_socket.raw_udp_qos_bypass {
            info!(
                "Update outputs.socket.raw_udp_qos_bypass from {:?} to {:?}.",
                socket.raw_udp_qos_bypass, new_socket.raw_udp_qos_bypass
            );
            socket.raw_udp_qos_bypass = new_socket.raw_udp_qos_bypass;
            restart_agent = !first_run;
        }
        if socket.data_socket_type != new_socket.data_socket_type {
            info!(
                "Update outputs.socket.data_socket_type from {:?} to {:?}.",
                socket.data_socket_type, new_socket.data_socket_type
            );
            socket.data_socket_type = new_socket.data_socket_type;
        }
        if socket.npb_socket_type != new_socket.npb_socket_type {
            info!(
                "Update outputs.socket.npb_socket_type from {:?} to {:?}.",
                socket.npb_socket_type, new_socket.npb_socket_type
            );
            socket.npb_socket_type = new_socket.npb_socket_type;
        }

        let flow_log = &mut outputs.flow_log;
        let new_flow_log = &mut new_outputs.flow_log;
        let filters = &mut flow_log.filters;
        let new_filters = &mut new_flow_log.filters;
        if filters.l4_capture_network_types != new_filters.l4_capture_network_types {
            info!(
                "Update outputs.flow_log.filters.l4_capture_network_types from {:?} to {:?}.",
                filters.l4_capture_network_types, new_filters.l4_capture_network_types
            );
            filters.l4_capture_network_types = new_filters.l4_capture_network_types.clone();
        }
        if filters.l4_ignored_observation_points != new_filters.l4_ignored_observation_points {
            info!(
                "Update outputs.flow_log.filters.l4_ignored_observation_points from {:?} to {:?}.",
                filters.l4_ignored_observation_points, new_filters.l4_ignored_observation_points
            );
            filters.l4_ignored_observation_points =
                new_filters.l4_ignored_observation_points.clone();
        }
        if filters.l7_capture_network_types != new_filters.l7_capture_network_types {
            info!(
                "Update outputs.flow_log.filters.l7_capture_network_types from {:?} to {:?}.",
                filters.l7_capture_network_types, new_filters.l7_capture_network_types
            );
            filters.l7_capture_network_types = new_filters.l7_capture_network_types.clone();
        }
        if filters.l7_ignored_observation_points != new_filters.l7_ignored_observation_points {
            info!(
                "Update outputs.flow_log.filters.l7_ignored_observation_points from {:?} to {:?}.",
                filters.l7_ignored_observation_points, new_filters.l7_ignored_observation_points
            );
            filters.l7_ignored_observation_points =
                new_filters.l7_ignored_observation_points.clone();
        }

        let throttles = &mut flow_log.throttles;
        let new_throttles = &mut new_flow_log.throttles;
        if throttles.l4_throttle != new_throttles.l4_throttle {
            info!(
                "Update outputs.flow_log.throttles.l4_throttle from {:?} to {:?}.",
                throttles.l4_throttle, new_throttles.l4_throttle
            );
            throttles.l4_throttle = new_throttles.l4_throttle;
        }
        if throttles.l7_throttle != new_throttles.l7_throttle {
            info!(
                "Update outputs.flow_log.throttles.l7_throttle from {:?} to {:?}.",
                throttles.l7_throttle, new_throttles.l7_throttle
            );
            throttles.l7_throttle = new_throttles.l7_throttle;
        }

        let tunning = &mut flow_log.tunning;
        let new_tunning = &mut new_flow_log.tunning;
        if tunning.collector_queue_size != new_tunning.collector_queue_size {
            info!(
                "Update outputs.flow_log.tunning.collector_queue_size from {:?} to {:?}.",
                tunning.collector_queue_size, new_tunning.collector_queue_size
            );
            tunning.collector_queue_size = new_tunning.collector_queue_size;
            restart_agent = !first_run;
        }

        let flow_metrics = &mut outputs.flow_metrics;
        let new_flow_metrics = &mut new_outputs.flow_metrics;
        if flow_metrics.enabled != new_flow_metrics.enabled {
            info!(
                "Update outputs.flow_metrics.enabled from {:?} to {:?}.",
                flow_metrics.enabled, new_flow_metrics.enabled
            );
            flow_metrics.enabled = new_flow_metrics.enabled;
        }
        let filters = &mut outputs.flow_metrics.filters;
        let new_filters = &mut new_outputs.flow_metrics.filters;
        if filters.apm_metrics != new_filters.apm_metrics {
            info!(
                "Update outputs.flow_metrics.filters.apm_metrics from {:?} to {:?}.",
                filters.apm_metrics, new_filters.apm_metrics
            );
            filters.apm_metrics = new_filters.apm_metrics;
        }
        if filters.inactive_ip_aggregation != new_filters.inactive_ip_aggregation {
            info!(
                "Update outputs.flow_metrics.filters.inactive_ip_aggregation from {:?} to {:?}.",
                filters.inactive_ip_aggregation, new_filters.inactive_ip_aggregation
            );
            filters.inactive_ip_aggregation = new_filters.inactive_ip_aggregation;
            restart_dispatcher = true;
        }
        if filters.inactive_server_port_aggregation != new_filters.inactive_server_port_aggregation
        {
            info!("Update outputs.flow_metrics.filters.inactive_server_port_aggregation from {:?} to {:?}.",
                filters.inactive_server_port_aggregation, new_filters.inactive_server_port_aggregation);
            filters.inactive_server_port_aggregation = new_filters.inactive_server_port_aggregation;
        }
        if filters.npm_metrics != new_filters.npm_metrics {
            info!(
                "Update outputs.flow_metrics.filters.npm_metrics from {:?} to {:?}.",
                filters.npm_metrics, new_filters.npm_metrics
            );
            filters.npm_metrics = new_filters.npm_metrics;
        }
        if filters.npm_metrics_concurrent != new_filters.npm_metrics_concurrent {
            info!(
                "Update outputs.flow_metrics.filters.npm_metrics_concurrent from {:?} to {:?}.",
                filters.npm_metrics_concurrent, new_filters.npm_metrics_concurrent
            );
            filters.npm_metrics_concurrent = new_filters.npm_metrics_concurrent;
            restart_dispatcher = true;
        }
        if filters.second_metrics != new_filters.second_metrics {
            info!(
                "Update outputs.flow_metrics.filters.second_metrics from {:?} to {:?}.",
                filters.second_metrics, new_filters.second_metrics
            );
            filters.second_metrics = new_filters.second_metrics;
        }
        let tunning = &mut outputs.flow_metrics.tunning;
        let new_tunning = &mut new_outputs.flow_metrics.tunning;
        if tunning.sender_queue_size != new_tunning.sender_queue_size {
            info!(
                "Update outputs.flow_metrics.tunning.sender_queue_size from {:?} to {:?}.",
                tunning.sender_queue_size, new_tunning.sender_queue_size
            );
            tunning.sender_queue_size = new_tunning.sender_queue_size;
            restart_agent = !first_run;
        }

        let npb = &mut outputs.npb;
        let new_npb = &mut new_outputs.npb;
        if npb.overlay_vlan_header_trimming != new_npb.overlay_vlan_header_trimming {
            info!(
                "Update outputs.npb.overlay_vlan_header_trimming from {:?} to {:?}.",
                npb.overlay_vlan_header_trimming, new_npb.overlay_vlan_header_trimming
            );
            npb.overlay_vlan_header_trimming = new_npb.overlay_vlan_header_trimming;
            restart_agent = !first_run;
        }
        if npb.traffic_global_dedup != new_npb.traffic_global_dedup {
            info!(
                "Update outputs.npb.traffic_global_dedup from {:?} to {:?}.",
                npb.traffic_global_dedup, new_npb.traffic_global_dedup
            );
            npb.traffic_global_dedup = new_npb.traffic_global_dedup;
        }
        if npb.custom_vxlan_flags != new_npb.custom_vxlan_flags {
            info!(
                "Update outputs.npb.custom_vxlan_flags from {:?} to {:?}.",
                npb.custom_vxlan_flags, new_npb.custom_vxlan_flags
            );
            npb.custom_vxlan_flags = new_npb.custom_vxlan_flags;
            restart_agent = !first_run;
        }
        if npb.extra_vlan_header != new_npb.extra_vlan_header {
            info!(
                "Update outputs.npb.extra_vlan_header from {:?} to {:?}.",
                npb.extra_vlan_header, new_npb.extra_vlan_header
            );
            npb.extra_vlan_header = new_npb.extra_vlan_header;
        }
        if npb.max_mtu != new_npb.max_mtu {
            info!(
                "Update outputs.npb.max_mtu from {:?} to {:?}.",
                npb.max_mtu, new_npb.max_mtu
            );
            npb.max_mtu = new_npb.max_mtu;
        }
        if npb.max_tx_throughput != new_npb.max_tx_throughput {
            info!(
                "Update outputs.npb.max_tx_throughput from {:?} to {:?}.",
                npb.max_tx_throughput, new_npb.max_tx_throughput
            );
            npb.max_tx_throughput = new_npb.max_tx_throughput;
            if let Some(components) = &components {
                components
                    .npb_bandwidth_watcher
                    .set_npb_rate(new_config.sender.npb_bps_threshold);
            }
        }
        if npb.raw_udp_vlan_tag != new_npb.raw_udp_vlan_tag {
            info!(
                "Update outputs.npb.raw_udp_vlan_tag from {:?} to {:?}.",
                npb.raw_udp_vlan_tag, new_npb.raw_udp_vlan_tag
            );
            npb.raw_udp_vlan_tag = new_npb.raw_udp_vlan_tag;
        }
        if npb.target_port != new_npb.target_port {
            info!(
                "Update outputs.npb.target_port from {:?} to {:?}.",
                npb.target_port, new_npb.target_port
            );
            npb.target_port = new_npb.target_port;
            restart_agent = !first_run;
        }
        if outputs.compression != new_outputs.compression {
            info!(
                "Update outputs.compression from {:?} to {:?}.",
                outputs.compression, new_outputs.compression
            );
            outputs.compression = new_outputs.compression.clone();
            restart_agent = !first_run;
        }

        // plugins
        let plugins = &mut config.plugins;
        let new_plugins = &mut new_config.user_config.plugins;
        if plugins.so_plugins != new_plugins.so_plugins {
            info!(
                "Update plugins.so_plugins from {:?} to {:?}.",
                plugins.so_plugins, new_plugins.so_plugins
            );
            plugins.so_plugins = new_plugins.so_plugins.clone();
        }
        if plugins.update_time != new_plugins.update_time {
            info!(
                "Update plugins.update_time from {:?} to {:?}.",
                plugins.update_time, new_plugins.update_time
            );
            plugins.update_time = new_plugins.update_time;
        }
        if plugins.wasm_plugins != new_plugins.wasm_plugins {
            info!(
                "Update plugins.wasm_plugins from {:?} to {:?}.",
                plugins.wasm_plugins, new_plugins.wasm_plugins
            );
            plugins.wasm_plugins = new_plugins.wasm_plugins.clone();
        }

        // processors
        let processors = &mut config.processors;
        let new_processors = &mut new_config.user_config.processors;
        let packet = &mut processors.packet;
        let new_packet = &mut new_processors.packet;
        let pcap = &mut packet.pcap_stream;
        let new_pcap = &mut new_packet.pcap_stream;
        if pcap.buffer_size_per_flow != new_pcap.buffer_size_per_flow {
            info!(
                "Update processors.packet.pcap_stream.buffer_size_per_flow from {:?} to {:?}.",
                pcap.buffer_size_per_flow, new_pcap.buffer_size_per_flow
            );
            pcap.buffer_size_per_flow = new_pcap.buffer_size_per_flow;
            restart_agent = !first_run;
        }
        if pcap.flush_interval != new_pcap.flush_interval {
            info!(
                "Update processors.packet.pcap_stream.flush_interval from {:?} to {:?}.",
                pcap.flush_interval, new_pcap.flush_interval
            );
            pcap.flush_interval = new_pcap.flush_interval;
            restart_agent = !first_run;
        }
        if pcap.receiver_queue_size != new_pcap.receiver_queue_size {
            info!(
                "Update processors.packet.pcap_stream.receiver_queue_size from {:?} to {:?}.",
                pcap.receiver_queue_size, new_pcap.receiver_queue_size
            );
            pcap.receiver_queue_size = new_pcap.receiver_queue_size;
            restart_agent = !first_run;
        }
        if pcap.total_buffer_size != new_pcap.total_buffer_size {
            info!(
                "Update processors.packet.pcap_stream.total_buffer_size from {:?} to {:?}.",
                pcap.total_buffer_size, new_pcap.total_buffer_size
            );
            pcap.total_buffer_size = new_pcap.total_buffer_size;
            restart_agent = !first_run;
        }

        let policy = &mut packet.policy;
        let new_policy = &mut new_packet.policy;
        if policy.fast_path_disabled != new_policy.fast_path_disabled {
            info!(
                "Update processors.packet.policy.fast_path_disabled from {:?} to {:?}.",
                policy.fast_path_disabled, new_policy.fast_path_disabled
            );
            policy.fast_path_disabled = new_policy.fast_path_disabled;
            restart_agent = !first_run;
        }
        if policy.fast_path_map_size != new_policy.fast_path_map_size {
            info!(
                "Update processors.packet.policy.fast_path_map_size from {:?} to {:?}.",
                policy.fast_path_map_size, new_policy.fast_path_map_size
            );
            policy.fast_path_map_size = new_policy.fast_path_map_size;
            restart_agent = !first_run;
        }
        if policy.forward_table_capacity != new_policy.forward_table_capacity {
            info!(
                "Update processors.packet.policy.forward_table_capacity from {:?} to {:?}.",
                policy.forward_table_capacity, new_policy.forward_table_capacity
            );
            policy.forward_table_capacity = new_policy.forward_table_capacity;
            restart_agent = !first_run;
        }
        if policy.max_first_path_level != new_policy.max_first_path_level {
            info!(
                "Update processors.packet.policy.max_first_path_level from {:?} to {:?}.",
                policy.max_first_path_level, new_policy.max_first_path_level
            );
            policy.max_first_path_level = new_policy.max_first_path_level;
            restart_agent = !first_run;
        }

        let tcp_header = &mut packet.tcp_header;
        let new_tcp_header = &mut new_packet.tcp_header;
        if tcp_header.block_size != new_tcp_header.block_size {
            info!(
                "Update processors.packet.tcp_header.block_size from {:?} to {:?}.",
                tcp_header.block_size, new_tcp_header.block_size
            );
            tcp_header.block_size = new_tcp_header.block_size;
            restart_agent = !first_run;
        }
        if tcp_header.header_fields_flag != new_tcp_header.header_fields_flag {
            info!(
                "Update processors.packet.tcp_header.header_fields_flag from {:?} to {:?}.",
                tcp_header.header_fields_flag, new_tcp_header.header_fields_flag
            );
            tcp_header.header_fields_flag = new_tcp_header.header_fields_flag;
            restart_agent = !first_run;
        }
        if tcp_header.sender_queue_size != new_tcp_header.sender_queue_size {
            info!(
                "Update processors.packet.tcp_header.sender_queue_size from {:?} to {:?}.",
                tcp_header.sender_queue_size, new_tcp_header.sender_queue_size
            );
            tcp_header.sender_queue_size = new_tcp_header.sender_queue_size;
            restart_agent = !first_run;
        }

        let toa = &mut packet.toa;
        let new_toa = &mut new_packet.toa;
        if toa.cache_size != new_toa.cache_size {
            info!(
                "Update processors.packet.toa.cache_size from {:?} to {:?}.",
                toa.cache_size, new_toa.cache_size
            );
            toa.cache_size = new_toa.cache_size;
            restart_agent = !first_run;
        }
        if toa.sender_queue_size != new_toa.sender_queue_size {
            info!(
                "Update processors.packet.toa.sender_queue_size from {:?} to {:?}.",
                toa.sender_queue_size, new_toa.sender_queue_size
            );
            toa.sender_queue_size = new_toa.sender_queue_size;
            restart_agent = !first_run;
        }

        let flow_log = &mut processors.flow_log;
        let new_flow_log = &mut new_processors.flow_log;
        let conntrack = &mut flow_log.conntrack;
        let new_conntrack = &mut new_flow_log.conntrack;
        if conntrack.flow_flush_interval != new_conntrack.flow_flush_interval {
            info!(
                "Update processors.flow_log.conntrack.flow_flush_interval from {:?} to {:?}.",
                conntrack.flow_flush_interval, new_conntrack.flow_flush_interval
            );
            conntrack.flow_flush_interval = new_conntrack.flow_flush_interval;
            restart_agent = !first_run;
        }
        let flow_generation = &mut conntrack.flow_generation;
        let new_flow_generation = &mut new_conntrack.flow_generation;
        if flow_generation.cloud_traffic_ignore_mac != new_flow_generation.cloud_traffic_ignore_mac
        {
            info!("Update processors.flow_log.conntrack.flow_generation.cloud_traffic_ignore_mac from {:?} to {:?}.",
                flow_generation.cloud_traffic_ignore_mac, new_flow_generation.cloud_traffic_ignore_mac);
            flow_generation.cloud_traffic_ignore_mac = new_flow_generation.cloud_traffic_ignore_mac;
            restart_agent = !first_run;
        }
        if flow_generation.idc_traffic_ignore_vlan != new_flow_generation.idc_traffic_ignore_vlan {
            info!("Update processors.flow_log.conntrack.flow_generation.idc_traffic_ignore_vlan from {:?} to {:?}.",
                flow_generation.idc_traffic_ignore_vlan, new_flow_generation.idc_traffic_ignore_vlan);
            flow_generation.idc_traffic_ignore_vlan = new_flow_generation.idc_traffic_ignore_vlan;
            restart_agent = !first_run;
        }
        if flow_generation.ignore_l2_end != new_flow_generation.ignore_l2_end {
            info!("Update processors.flow_log.conntrack.flow_generation.ignore_l2_end from {:?} to {:?}.",
                flow_generation.ignore_l2_end, new_flow_generation.ignore_l2_end);
            flow_generation.ignore_l2_end = new_flow_generation.ignore_l2_end;
            restart_agent = !first_run;
        }
        if flow_generation.server_ports != new_flow_generation.server_ports {
            info!("Update processors.flow_log.conntrack.flow_generation.server_ports from {:?} to {:?}.",
                flow_generation.server_ports, new_flow_generation.server_ports);
            flow_generation.server_ports = new_flow_generation.server_ports.clone();
            restart_agent = !first_run;
        }

        let timeouts = &mut conntrack.timeouts;
        let new_timeouts = &mut new_conntrack.timeouts;
        if timeouts.closing_rst != new_timeouts.closing_rst {
            info!(
                "Update processors.flow_log.conntrack.timeouts.closing_rst from {:?} to {:?}.",
                timeouts.closing_rst, new_timeouts.closing_rst
            );
            timeouts.closing_rst = new_timeouts.closing_rst;
            restart_agent = !first_run;
        }
        if timeouts.established != new_timeouts.established {
            info!(
                "Update processors.flow_log.conntrack.timeouts.established from {:?} to {:?}.",
                timeouts.established, new_timeouts.established
            );
            timeouts.established = new_timeouts.established;
            restart_agent = !first_run;
        }
        if timeouts.opening_rst != new_timeouts.opening_rst {
            info!(
                "Update processors.flow_log.conntrack.timeouts.opening_rst from {:?} to {:?}.",
                timeouts.opening_rst, new_timeouts.opening_rst
            );
            timeouts.opening_rst = new_timeouts.opening_rst;
            restart_agent = !first_run;
        }
        if timeouts.others != new_timeouts.others {
            info!(
                "Update processors.flow_log.conntrack.timeouts.others from {:?} to {:?}.",
                timeouts.others, new_timeouts.others
            );
            timeouts.others = new_timeouts.others;
            restart_agent = !first_run;
        }

        let time_window = &mut flow_log.time_window;
        let new_time_window = &mut new_flow_log.time_window;
        if time_window.extra_tolerable_flow_delay != new_time_window.extra_tolerable_flow_delay {
            info!("Update processors.flow_log.time_window.extra_tolerable_flow_delay from {:?} to {:?}.",
                time_window.extra_tolerable_flow_delay, new_time_window.extra_tolerable_flow_delay);
            time_window.extra_tolerable_flow_delay = new_time_window.extra_tolerable_flow_delay;
            restart_agent = !first_run;
        }
        if time_window.max_tolerable_packet_delay != new_time_window.max_tolerable_packet_delay {
            info!("Update processors.flow_log.time_window.max_tolerable_packet_delay from {:?} to {:?}.",
                time_window.max_tolerable_packet_delay, new_time_window.max_tolerable_packet_delay);
            time_window.max_tolerable_packet_delay = new_time_window.max_tolerable_packet_delay;
            restart_agent = !first_run;
        }

        let tunning = &mut flow_log.tunning;
        let new_tunning = &mut new_flow_log.tunning;
        if tunning.concurrent_flow_limit != new_tunning.concurrent_flow_limit {
            info!(
                "Update processors.flow_log.tunning.concurrent_flow_limit from {:?} to {:?}.",
                tunning.concurrent_flow_limit, new_tunning.concurrent_flow_limit
            );
            tunning.concurrent_flow_limit = new_tunning.concurrent_flow_limit;
            restart_agent = !first_run;
        }
        if tunning.flow_aggregator_queue_size != new_tunning.flow_aggregator_queue_size {
            info!(
                "Update processors.flow_log.tunning.flow_aggregator_queue_size from {:?} to {:?}.",
                tunning.flow_aggregator_queue_size, new_tunning.flow_aggregator_queue_size
            );
            tunning.flow_aggregator_queue_size = new_tunning.flow_aggregator_queue_size;
            restart_agent = !first_run;
        }
        if tunning.flow_generator_queue_size != new_tunning.flow_generator_queue_size {
            info!(
                "Update processors.flow_log.tunning.flow_generator_queue_size from {:?} to {:?}.",
                tunning.flow_generator_queue_size, new_tunning.flow_generator_queue_size
            );
            tunning.flow_generator_queue_size = new_tunning.flow_generator_queue_size;
            restart_agent = !first_run;
        }
        if tunning.flow_map_hash_slots != new_tunning.flow_map_hash_slots {
            info!(
                "Update processors.flow_log.tunning.flow_map_hash_slots from {:?} to {:?}.",
                tunning.flow_map_hash_slots, new_tunning.flow_map_hash_slots
            );
            tunning.flow_map_hash_slots = new_tunning.flow_map_hash_slots;
            restart_agent = !first_run;
        }
        if tunning.max_batched_buffer_size != new_tunning.max_batched_buffer_size {
            info!(
                "Update processors.flow_log.tunning.max_batched_buffer_size from {:?} to {:?}.",
                tunning.max_batched_buffer_size, new_tunning.max_batched_buffer_size
            );
            tunning.max_batched_buffer_size = new_tunning.max_batched_buffer_size;
            restart_agent = !first_run;
        }
        if tunning.memory_pool_size != new_tunning.memory_pool_size {
            info!(
                "Update processors.flow_log.tunning.memory_pool_size from {:?} to {:?}.",
                tunning.memory_pool_size, new_tunning.memory_pool_size
            );
            tunning.memory_pool_size = new_tunning.memory_pool_size;
            restart_agent = !first_run;
        }
        if tunning.quadruple_generator_queue_size != new_tunning.quadruple_generator_queue_size {
            info!("Update processors.flow_log.tunning.quadruple_generator_queue_size from {:?} to {:?}.",
                tunning.quadruple_generator_queue_size, new_tunning.quadruple_generator_queue_size);
            tunning.quadruple_generator_queue_size = new_tunning.quadruple_generator_queue_size;
            restart_agent = !first_run;
        }

        let request_log = &mut processors.request_log;
        let new_request_log = &mut new_processors.request_log;
        let app = &mut request_log.application_protocol_inference;
        let new_app = &mut new_request_log.application_protocol_inference;
        if app.enabled_protocols != new_app.enabled_protocols {
            info!("Update processors.request_log.application_protocol_inference.enabled_protocols from {:?} to {:?}.",
                app.enabled_protocols, new_app.enabled_protocols);
            app.enabled_protocols = new_app.enabled_protocols.clone();
            restart_agent = !first_run;
        }
        if app.inference_max_retries != new_app.inference_max_retries {
            info!("Update processors.request_log.application_protocol_inference.inference_max_retries from {:?} to {:?}.",
                app.inference_max_retries, new_app.inference_max_retries);
            app.inference_max_retries = new_app.inference_max_retries;
            restart_agent = !first_run;
        }
        if app.inference_result_ttl != new_app.inference_result_ttl {
            info!("Update processors.request_log.application_protocol_inference.inference_result_ttl from {:?} to {:?}.",
                app.inference_result_ttl, new_app.inference_result_ttl);
            app.inference_result_ttl = new_app.inference_result_ttl;
            restart_agent = !first_run;
        }
        if app.protocol_special_config != new_app.protocol_special_config {
            info!("Update processors.request_log.application_protocol_inference.protocol_special_config from {:?} to {:?}.",
                app.protocol_special_config, new_app.protocol_special_config);
            app.protocol_special_config = new_app.protocol_special_config;
            restart_agent = !first_run;
        }
        let filters = &mut request_log.filters;
        let new_filters = &mut new_request_log.filters;
        if filters.port_number_prefilters != new_filters.port_number_prefilters {
            info!(
                "Update processors.request_log.filters.port_number_prefilters from {:?} to {:?}.",
                filters.port_number_prefilters, new_filters.port_number_prefilters
            );
            filters.port_number_prefilters = new_filters.port_number_prefilters.clone();
            restart_agent = !first_run;
        }
        if filters.tag_filters != new_filters.tag_filters {
            info!(
                "Update processors.request_log.filters.tag_filters from {:?} to {:?}.",
                filters.tag_filters, new_filters.tag_filters
            );
            filters.tag_filters = new_filters.tag_filters.clone();
            restart_agent = !first_run;
        }
        if filters.unconcerned_dns_nxdomain_response_suffixes
            != new_filters.unconcerned_dns_nxdomain_response_suffixes
        {
            info!("Update processors.request_log.filters.unconcerned_dns_nxdomain_response_suffixes from {:?} to {:?}.",
                filters.unconcerned_dns_nxdomain_response_suffixes, new_filters.unconcerned_dns_nxdomain_response_suffixes);
            filters.unconcerned_dns_nxdomain_response_suffixes = new_filters
                .unconcerned_dns_nxdomain_response_suffixes
                .clone();
            restart_agent = !first_run;
        }
        let tag_extraction = &mut request_log.tag_extraction;
        let new_tag_extraction = &mut new_request_log.tag_extraction;
        if tag_extraction.custom_fields != new_tag_extraction.custom_fields {
            info!(
                "Update processors.request_log.tag_extraction.custom_fields from {:?} to {:?}.",
                tag_extraction.custom_fields, new_tag_extraction.custom_fields
            );
            tag_extraction.custom_fields = new_tag_extraction.custom_fields.clone();
            restart_agent = !first_run;
        }
        if tag_extraction.http_endpoint != new_tag_extraction.http_endpoint {
            info!(
                "Update processors.request_log.tag_extraction.http_endpoint from {:?} to {:?}.",
                tag_extraction.http_endpoint, new_tag_extraction.http_endpoint
            );
            tag_extraction.http_endpoint = new_tag_extraction.http_endpoint.clone();
            restart_agent = !first_run;
        }
        if tag_extraction.obfuscate_protocols != new_tag_extraction.obfuscate_protocols {
            info!("Update processors.request_log.tag_extraction.obfuscate_protocols from {:?} to {:?}.",
                tag_extraction.obfuscate_protocols, new_tag_extraction.obfuscate_protocols);
            tag_extraction.obfuscate_protocols = new_tag_extraction.obfuscate_protocols.clone();
            restart_agent = !first_run;
        }
        if tag_extraction.tracing_tag != new_tag_extraction.tracing_tag {
            info!(
                "Update processors.request_log.tag_extraction.tracing_tag from {:?} to {:?}.",
                tag_extraction.tracing_tag, new_tag_extraction.tracing_tag
            );
            tag_extraction.tracing_tag = new_tag_extraction.tracing_tag.clone();
        }

        let tunning = &mut request_log.tunning;
        let new_tunning = &mut new_request_log.tunning;
        if tunning.consistent_timestamp_in_l7_metrics
            != new_tunning.consistent_timestamp_in_l7_metrics
        {
            info!("Update processors.request_log.tunning.consistent_timestamp_in_l7_metrics from {:?} to {:?}.",
                tunning.consistent_timestamp_in_l7_metrics, new_tunning.consistent_timestamp_in_l7_metrics);
            tunning.consistent_timestamp_in_l7_metrics =
                new_tunning.consistent_timestamp_in_l7_metrics;
            restart_agent = !first_run;
        }
        if tunning.payload_truncation != new_tunning.payload_truncation {
            info!(
                "Update processors.request_log.tunning.payload_truncation from {:?} to {:?}.",
                tunning.payload_truncation, new_tunning.payload_truncation
            );
            tunning.payload_truncation = new_tunning.payload_truncation;
        }
        if tunning.session_aggregate_slot_capacity != new_tunning.session_aggregate_slot_capacity {
            info!("Update processors.request_log.tunning.session_aggregate_slot_capacity from {:?} to {:?}.",
                tunning.session_aggregate_slot_capacity, new_tunning.session_aggregate_slot_capacity);
            tunning.session_aggregate_slot_capacity = new_tunning.session_aggregate_slot_capacity;
            restart_agent = !first_run;
        }

        let vector = &mut config.inputs.vector;
        let new_vector = &mut new_config.user_config.inputs.vector;
        if vector.enabled != new_vector.enabled || vector.config != new_vector.config {
            info!(
                "vector inputs.vector from {:#?} to {:#?}",
                vector, new_vector
            );
            if components.is_some() {
                callbacks.push(Self::set_vector);
            }
            vector.enabled = new_vector.enabled;
            vector.config = new_vector.config.clone();
        }

        candidate_config.enabled = new_config.enabled;
        candidate_config.capture_mode = new_config.capture_mode;
        if candidate_config.dispatcher != new_config.dispatcher {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            {
                candidate_config.dispatcher.cpu_set = new_config.dispatcher.cpu_set;
            }

            if candidate_config.dispatcher.max_memory != new_config.dispatcher.max_memory
                || candidate_config
                    .user_config
                    .get_af_packet_blocks(new_config.capture_mode, new_config.dispatcher.max_memory)
                    != new_config.user_config.get_af_packet_blocks(
                        new_config.capture_mode,
                        new_config.dispatcher.max_memory,
                    )
                || candidate_config
                    .user_config
                    .get_fast_path_map_size(new_config.dispatcher.max_memory)
                    != new_config
                        .user_config
                        .get_fast_path_map_size(candidate_config.dispatcher.max_memory)
                || candidate_config.get_channel_size(new_config.dispatcher.max_memory)
                    != candidate_config.get_channel_size(candidate_config.dispatcher.max_memory)
                || candidate_config.get_flow_capacity(new_config.dispatcher.max_memory)
                    != candidate_config.get_flow_capacity(candidate_config.dispatcher.max_memory)
            {
                restart_dispatcher = true;
                info!("max_memory change, restart dispatcher");
            }

            debug!(
                "dispatcher config change from {:#?} to {:#?}",
                candidate_config.dispatcher, new_config.dispatcher
            );
            candidate_config.dispatcher = new_config.dispatcher.clone();
        }

        if candidate_config.stats != new_config.stats {
            debug!(
                "stats config change from {:#?} to {:#?}",
                candidate_config.stats, new_config.stats
            );
            candidate_config.stats = new_config.stats.clone();
            callbacks.push(Self::set_stats);
        }

        if candidate_config.debug != new_config.debug {
            debug!(
                "debug config change from {:#?} to {:#?}",
                candidate_config.debug, new_config.debug
            );
            candidate_config.debug = new_config.debug.clone();
            callbacks.push(Self::set_debug);
        }

        if candidate_config.diagnose != new_config.diagnose {
            //TODO diagnose stuff
            debug!(
                "diagnose config change from {:#?} to {:#?}",
                candidate_config.diagnose, new_config.diagnose
            );
            candidate_config.diagnose = new_config.diagnose.clone();
        }

        if candidate_config.environment.max_memory != new_config.environment.max_memory {
            if let Some(ref components) = components {
                components
                    .policy_setter
                    .set_memory_limit(new_config.environment.max_memory);
            }
        }

        if candidate_config.capture_mode != PacketCaptureType::Analyzer {
            if candidate_config.environment.max_memory != new_config.environment.max_memory {
                info!(
                    "memory limit set to {}",
                    ByteSize::b(new_config.environment.max_memory).to_string_as(true)
                );
                candidate_config.environment.max_memory = new_config.environment.max_memory;
            }

            if candidate_config.environment.max_millicpus != new_config.environment.max_millicpus {
                info!(
                    "cpu limit set to {}",
                    new_config.environment.max_millicpus as f64 / 1000.0
                );
                candidate_config.environment.max_millicpus = new_config.environment.max_millicpus;
            }
            #[cfg(target_os = "linux")]
            if running_in_container() {
                if self.container_cpu_limit != candidate_config.environment.max_millicpus
                    || self.container_mem_limit != candidate_config.environment.max_memory
                {
                    info!("current container cpu limit: {}, memory limit: {}bytes, set cpu limit {} and memory limit {}bytes", self.container_cpu_limit as f64 / 1000.0, self.container_mem_limit, candidate_config.environment.max_millicpus as f64 / 1000.0, candidate_config.environment.max_memory);
                    if let Err(e) = runtime.block_on(set_container_resource_limit(
                        candidate_config.environment.max_millicpus,
                        candidate_config.environment.max_memory,
                    )) {
                        warn!("set container resources limit failed: {:?}", e);
                    };
                }
            }
        } else {
            let mut system = sysinfo::System::new();
            system.refresh_memory();
            let max_memory = system.total_memory();
            system.refresh_cpu();
            let max_cpus = 1.max(system.cpus().len()) as u32;
            let max_millicpus = max_cpus * 1000;

            new_config.environment.max_memory = max_memory;
            new_config.environment.max_millicpus = max_millicpus;

            if candidate_config.environment.max_memory != new_config.environment.max_memory {
                info!("memory set ulimit when capture_mode=analyzer");
            }

            if candidate_config.environment.max_millicpus != new_config.environment.max_millicpus {
                info!("cpu set ulimit when capture_mode=analyzer");
            }
        }

        if candidate_config.flow != new_config.flow {
            if candidate_config.flow.collector_enabled != new_config.flow.collector_enabled {
                restart_dispatcher = true;
            }
            debug!(
                "flow_generator config change from {:#?} to {:#?}",
                candidate_config.flow, new_config.flow
            );
            if candidate_config.flow.plugins.digest != new_config.flow.plugins.digest {
                debug!(
                    "plugins changed, pulling {} plugins from server",
                    new_config.flow.plugins.names.len()
                );
                new_config
                    .flow
                    .plugins
                    .fill_plugin_prog_from_server(runtime, session, agent_id);
            }
            candidate_config.flow = new_config.flow.clone();
        }

        if candidate_config.collector != new_config.collector {
            debug!(
                "collector config change from {:#?} to {:#?}",
                candidate_config.collector, new_config.collector
            );
            restart_dispatcher = candidate_config.collector.agent_id
                != new_config.collector.agent_id
                && new_config.collector.enabled;
            candidate_config.collector = new_config.collector.clone();
        }

        if candidate_config.platform != new_config.platform {
            #[cfg(target_os = "linux")]
            let old_cfg = &candidate_config.platform;
            #[cfg(target_os = "linux")]
            let new_cfg = &new_config.platform;

            // restart api watcher if it keeps running and config changes
            #[cfg(target_os = "linux")]
            let restart_api_watcher = old_cfg.kubernetes_api_enabled
                && new_cfg.kubernetes_api_enabled
                && (old_cfg.kubernetes_api_list_limit != new_cfg.kubernetes_api_list_limit
                    || old_cfg.kubernetes_api_list_interval
                        != new_cfg.kubernetes_api_list_interval
                    || old_cfg.kubernetes_resources != new_cfg.kubernetes_resources
                    || old_cfg.max_memory != new_cfg.max_memory);
            #[cfg(target_os = "linux")]
            if restart_api_watcher {
                api_watcher.stop();
            }

            debug!(
                "platform config change from {:#?} to {:#?}",
                candidate_config.platform, new_config.platform
            );
            candidate_config.platform = new_config.platform.clone();

            #[cfg(target_os = "linux")]
            if static_config.agent_mode == RunningMode::Managed {
                callbacks.push(Self::set_platform);
            }
        }

        if candidate_config.sender != new_config.sender {
            if candidate_config.sender.collector_socket_type
                != new_config.sender.collector_socket_type
            {
                if candidate_config.sender.enabled != new_config.sender.enabled {
                    restart_dispatcher = true;
                }
            }

            if candidate_config.sender.npb_socket_type != new_config.sender.npb_socket_type {
                if candidate_config.capture_mode != PacketCaptureType::Analyzer {
                    restart_dispatcher = true;
                }
            }

            if candidate_config.sender.npb_dedup_enabled != new_config.sender.npb_dedup_enabled {
                if candidate_config.capture_mode != PacketCaptureType::Analyzer {
                    restart_dispatcher = true;
                }
            }

            debug!(
                "sender config change from {:#?} to {:#?}",
                candidate_config.sender, new_config.sender
            );
            candidate_config.sender = new_config.sender.clone();
        }

        if candidate_config.handler != new_config.handler {
            if candidate_config.handler.npb_dedup_enabled != new_config.handler.npb_dedup_enabled {
                if candidate_config.capture_mode != PacketCaptureType::Analyzer {
                    restart_dispatcher = true;
                }
            }
            debug!(
                "handler config change from {:#?} to {:#?}",
                candidate_config.handler, new_config.handler
            );
            candidate_config.handler = new_config.handler.clone();
        }

        if candidate_config.log_parser != new_config.log_parser {
            debug!(
                "log_parser config change from {:#?} to {:#?}",
                candidate_config.log_parser, new_config.log_parser
            );
            candidate_config.log_parser = new_config.log_parser.clone();
        }

        if candidate_config.synchronizer != new_config.synchronizer {
            debug!(
                "synchronizer config change from {:#?} to {:#?}",
                candidate_config.synchronizer, new_config.synchronizer
            );
            candidate_config.synchronizer = new_config.synchronizer.clone();
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        if candidate_config.ebpf != new_config.ebpf {
            if candidate_config.capture_mode != PacketCaptureType::Analyzer {
                debug!(
                    "ebpf config change from {:#?} to {:#?}",
                    candidate_config.ebpf, new_config.ebpf
                );
                callbacks.push(Self::set_ebpf);
            }
            candidate_config.ebpf = new_config.ebpf.clone();
        }

        if candidate_config.agent_type != new_config.agent_type {
            debug!(
                "agent_type change from {:?} to {:?}",
                candidate_config.agent_type, new_config.agent_type
            );
            candidate_config.agent_type = new_config.agent_type;
        }

        if candidate_config.metric_server != new_config.metric_server {
            if candidate_config.metric_server.enabled != new_config.metric_server.enabled {
                if let Some(c) = components.as_mut() {
                    if new_config.metric_server.enabled {
                        c.metrics_server_component.start();
                    } else {
                        c.metrics_server_component.stop();
                    }
                }
            }

            debug!(
                "integration collector config change from {:#?} to {:#?}",
                candidate_config.metric_server, new_config.metric_server
            );
            candidate_config.metric_server = new_config.metric_server.clone();
            callbacks.push(Self::set_metric_server);
        }

        if candidate_config.npb != new_config.npb {
            debug!(
                "npb config change from {:#?} to {:#?}",
                candidate_config.npb, new_config.npb
            );
            candidate_config.npb = new_config.npb.clone();
            restart_dispatcher = true;
            if components.is_some() {
                callbacks.push(Self::set_npb);
            }
        }

        candidate_config.environment = new_config.environment.clone();
        candidate_config.log = new_config.log.clone();
        candidate_config.port_config = new_config.port_config.clone();
        candidate_config.pcap = new_config.pcap.clone();

        if new_config != *candidate_config {
            error!("Some configurations have not been updated, please check the code.");
            error!(
                "Configurations from {:#?} to {:#?}",
                candidate_config, new_config
            );
            crate::utils::notify_exit(public::consts::NORMAL_EXIT_WITH_RESTART);
            return vec![];
        }

        // avoid first config changed to restart dispatcher
        let restart_dispatcher =
            components.is_some() && restart_dispatcher && candidate_config.dispatcher.enabled;
        if restart_dispatcher
            && new_config
                .user_config
                .inputs
                .cbpf
                .special_network
                .dpdk
                .source
                == DpdkSource::Ebpf
        {
            restart_agent = !first_run;
        }

        if restart_agent {
            warn!("Change configuration and restart agent...");
            crate::utils::notify_exit(public::consts::NORMAL_EXIT_WITH_RESTART);
            return vec![];
        }

        if restart_dispatcher {
            warn!("Change configuration and restart dispatcher...");
            callbacks.push(Self::set_restart_dispatcher);
        }

        // deploy updated config
        self.current_config
            .store(Arc::new(candidate_config.clone()));
        exception_handler.clear(agent::Exception::InvalidConfiguration);

        callbacks
    }
}

impl ModuleConfig {
    fn get_channel_size(&self, mem_size: u64) -> usize {
        if self.capture_mode == PacketCaptureType::Analyzer {
            return 1 << 14;
        }

        min(max((mem_size / MB / 128 * 32000) as usize, 32000), 1 << 14)
    }

    fn get_flow_capacity(&self, mem_size: u64) -> usize {
        if self.capture_mode == PacketCaptureType::Analyzer {
            return self
                .user_config
                .processors
                .flow_log
                .tunning
                .concurrent_flow_limit as usize;
        }

        min((mem_size / MB / 128 * 65536) as usize, 1 << 30)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_trie() {
        let trie = HttpEndpointTrie::new();
        assert!(trie.root.children.is_empty());
        assert_eq!(trie.root.keep_segments, None);
    }

    #[test]
    fn test_insert_trie_node() {
        let mut trie = HttpEndpointTrie::new();
        let rule1 = HttpEndpointMatchRule {
            url_prefix: "/a".to_string(),
            keep_segments: 1,
        };
        let rule2 = HttpEndpointMatchRule {
            url_prefix: "/a/b/c/d".to_string(),
            keep_segments: 3,
        };
        let rule3 = HttpEndpointMatchRule {
            url_prefix: "/d/e/f".to_string(),
            keep_segments: 3,
        };
        trie.insert(&rule1);
        assert_eq!(trie.root.children.len(), 1);
        trie.insert(&rule2);
        assert_eq!(trie.root.children.len(), 1);
        trie.insert(&rule3);
        assert_eq!(trie.root.children.get(&'/').unwrap().children.len(), 2);
    }

    #[test]
    fn test_find_matching_rule() {
        let mut trie = HttpEndpointTrie::new();
        let rule1 = HttpEndpointMatchRule {
            url_prefix: "/a/b/c".to_string(),
            keep_segments: 1,
        };
        let rule2 = HttpEndpointMatchRule {
            url_prefix: "/a/b/c/d".to_string(),
            keep_segments: 3,
        };
        let rule3 = HttpEndpointMatchRule {
            url_prefix: "/d/e/f".to_string(),
            keep_segments: 3,
        };
        let rule4 = HttpEndpointMatchRule {
            url_prefix: "".to_string(),
            keep_segments: 5,
        };
        assert_eq!(trie.find_matching_rule("/x/y/z"), 2); // no rlues, 2 is the default keep_segments
        trie.insert(&rule1);
        trie.insert(&rule2);
        trie.insert(&rule3);
        assert_eq!(trie.find_matching_rule("/a/b/c"), 1);
        assert_eq!(trie.find_matching_rule("/d/e/f"), 3);
        assert_eq!(trie.find_matching_rule("/a/b/c/d"), 3);
        assert_eq!(trie.find_matching_rule("/x/y/z"), 0); // there is no matching rule
        trie.insert(&rule4);
        assert_eq!(trie.find_matching_rule("/x/y/z"), 5); // the keep_segments for any rule that matches "" is 5
    }

    #[test]
    fn trace_type_id_parse() {
        let testcases = vec![
            (
                TraceType::Uber,
                "trace_id:xxx:span_id:xxx",
                Some("trace_id"),
                Some("span_id"),
            ),
            (
                TraceType::Sw3,
                "span|id|x|x|x|x|x|trace_id|x",
                Some("trace_id"),
                Some("span-id"),
            ),
            (
                TraceType::Sw6,
                "1-dHJhY2VfaWQ=-c3Bhbg==-id-x-x-x-x-",
                Some("trace_id"),
                Some("span-id"),
            ),
            (
                TraceType::Sw8,
                "1-dHJhY2VfaWQ=-c3Bhbg==-id-x-x-x-x-",
                Some("trace_id"),
                Some("span-id"),
            ),
            (
                TraceType::TraceParent,
                "00-trace_id-span_id-01",
                Some("trace_id"),
                Some("span_id"),
            ),
        ];
        for (tt, value, tid, sid) in testcases {
            assert_eq!(tt.decode_trace_id(value).as_ref().map(|s| s.as_ref()), tid);
            assert_eq!(tt.decode_span_id(value).as_ref().map(|s| s.as_ref()), sid);
        }
    }
}
