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
use log::{info, warn, Level};
#[cfg(any(target_os = "linux", target_os = "android"))]
use nix::{
    sched::{sched_setaffinity, CpuSet},
    unistd::Pid,
};
use sysinfo::SystemExt;
#[cfg(any(target_os = "linux", target_os = "android"))]
use sysinfo::{CpuRefreshKind, RefreshKind, System};
use tokio::runtime::Runtime;

use super::config::{ExtraLogFields, FlowGeneratorConfig, L7LogBlacklist, OracleParseConfig};
#[cfg(any(target_os = "linux", target_os = "android"))]
use super::{
    config::EbpfYamlConfig, OsProcRegexp, OS_PROC_REGEXP_MATCH_ACTION_ACCEPT,
    OS_PROC_REGEXP_MATCH_TYPE_PROC_NAME,
};
use super::{
    config::{
        Config, HttpEndpointExtraction, KubernetesResourceConfig, MatchRule, PcapConfig,
        PortConfig, YamlConfig,
    },
    ConfigError, KubernetesPollerType, RuntimeConfig,
};
use crate::flow_generator::protocol_logs::decode_new_rpc_trace_context_with_type;
use crate::rpc::Session;
use crate::{
    common::{decapsulate::TunnelTypeBitmap, enums::TapType, l7_protocol_log::L7ProtocolBitmap},
    dispatcher::recv_engine,
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
    platform::ProcRegRewrite,
    utils::environment::{
        get_container_resource_limits, get_ctrl_ip_and_mac, is_tt_workload,
        set_container_resource_limit,
    },
};
#[cfg(target_os = "linux")]
use crate::{
    platform::{kubernetes::Poller, ApiWatcher},
    utils::environment::is_tt_pod,
};

use public::bitmap::Bitmap;
use public::l7_protocol::L7Protocol;
use public::proto::{
    common::TridentType,
    trident::{self, CaptureSocketType, Exception, IfMacSource, SocketType, TapMode},
};

use crate::{trident::AgentId, utils::cgroups::is_kernel_available_for_cgroups};
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

pub type PcapAccess = Access<PcapConfig>;

pub type DebugAccess = Access<DebugConfig>;

pub type SynchronizerAccess = Access<SynchronizerConfig>;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub type EbpfAccess = Access<EbpfConfig>;

pub type MetricServerAccess = Access<MetricServerConfig>;

pub type PortAccess = Access<PortConfig>;

#[derive(Clone, PartialEq, Eq)]
pub struct CollectorConfig {
    pub enabled: bool,
    pub inactive_server_port_enabled: bool,
    pub inactive_ip_enabled: bool,
    pub vtap_flow_1s_enabled: bool,
    pub l4_log_collect_nps_threshold: u64,
    pub l4_log_store_tap_types: [bool; 256],
    pub l4_log_ignore_tap_sides: [bool; TapSide::MAX as usize + 1],
    pub l7_metrics_enabled: bool,
    pub trident_type: TridentType,
    pub vtap_id: u16,
    pub cloud_gateway_traffic: bool,
    pub packet_delay: Duration,
}

impl fmt::Debug for CollectorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectorConfig")
            .field("enabled", &self.enabled)
            .field(
                "inactive_server_port_enabled",
                &self.inactive_server_port_enabled,
            )
            .field("inactive_ip_enabled", &self.inactive_ip_enabled)
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
            .field("trident_type", &self.trident_type)
            .field("vtap_id", &self.vtap_id)
            .field("cloud_gateway_traffic", &self.cloud_gateway_traffic)
            .field("packet_delay", &self.packet_delay)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EnvironmentConfig {
    pub max_memory: u64,
    pub max_millicpus: u32,
    pub process_threshold: u32,
    pub thread_threshold: u32,
    pub sys_free_memory_limit: u32,
    pub log_file_size: u32,
    pub tap_mode: TapMode,
    pub guard_interval: Duration,
    pub max_sockets: usize,
    pub max_sockets_tolerate_interval: Duration,
    pub system_load_circuit_breaker_threshold: f32,
    pub system_load_circuit_breaker_recover: f32,
    pub system_load_circuit_breaker_metric: trident::SystemLoadMetric,
    pub page_cache_reclaim_percentage: u8,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SenderConfig {
    pub mtu: u32,
    pub dest_ip: String,
    pub vtap_id: u16,
    pub team_id: u32,
    pub organize_id: u32,
    pub dest_port: u16,
    pub npb_port: u16,
    pub vxlan_flags: u8,
    pub npb_enable_qos_bypass: bool,
    pub npb_vlan: u16,
    pub npb_vlan_mode: trident::VlanMode,
    pub npb_dedup_enabled: bool,
    pub npb_bps_threshold: u64,
    pub npb_socket_type: trident::SocketType,
    pub collector_socket_type: trident::SocketType,
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
    pub vlan_mode: trident::VlanMode,
    pub socket_type: trident::SocketType,
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
    pub os_proc_regex: Vec<ProcRegRewrite>,
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
    pub vtap_id: u16,
    pub enabled: bool,
    pub trident_type: TridentType,
    pub epc_id: u32,
    pub kubernetes_api_enabled: bool,
    pub kubernetes_api_list_limit: u32,
    pub kubernetes_api_list_interval: Duration,
    pub kubernetes_resources: Vec<KubernetesResourceConfig>,
    pub max_memory: u64,
    pub namespace: Option<String>,
    pub thread_threshold: u32,
    pub tap_mode: TapMode,
    pub os_proc_scan_conf: OsProcScanConfig,
    pub agent_enabled: bool,
    #[cfg(target_os = "linux")]
    pub extra_netns_regex: String,
}

#[derive(Clone, PartialEq, Debug, Eq)]
pub struct HandlerConfig {
    pub npb_dedup_enabled: bool,
    pub trident_type: TridentType,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DispatcherConfig {
    pub global_pps_threshold: u64,
    pub capture_packet_size: u32,
    pub l7_log_packet_size: u32,
    pub tunnel_type_bitmap: TunnelTypeBitmap,
    pub tunnel_type_trim_bitmap: TunnelTypeBitmap,
    pub trident_type: TridentType,
    pub vtap_id: u16,
    pub capture_socket_type: CaptureSocketType,
    #[cfg(target_os = "linux")]
    pub extra_netns_regex: String,
    pub tap_interface_regex: String,
    pub if_mac_source: IfMacSource,
    pub analyzer_ip: String,
    pub analyzer_port: u16,
    pub proxy_controller_ip: String,
    pub proxy_controller_port: u16,
    pub capture_bpf: String,
    pub max_memory: u64,
    pub af_packet_blocks: usize,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub af_packet_version: OptTpacketVersion,
    pub tap_mode: TapMode,
    pub region_id: u32,
    pub pod_cluster_id: u32,
    pub enabled: bool,
    pub npb_dedup_enabled: bool,
    pub dpdk_enabled: bool,
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
    pub names: Vec<(String, trident::PluginType)>,
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
                match session.get_plugin(name, *ptype, agent_id).await {
                    Ok(prog) => match ptype {
                        trident::PluginType::Wasm => self.wasm_plugins.push((name.clone(), prog)),
                        #[cfg(any(target_os = "linux", target_os = "android"))]
                        trident::PluginType::So => self.so_plugins.push((name.clone(), prog)),
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

#[derive(Clone, PartialEq, Eq)]
pub struct FlowConfig {
    pub vtap_id: u16,
    pub trident_type: TridentType,
    pub tap_mode: TapMode,
    pub cloud_gateway_traffic: bool,
    pub collector_enabled: bool,
    pub l7_log_tap_types: [bool; 256],

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

    pub oracle_parse_conf: OracleParseConfig,

    pub obfuscate_enabled_protocols: L7ProtocolBitmap,
    pub server_ports: Vec<u16>,
    pub consistent_timestamp_in_l7_metrics: bool,
}

impl From<&RuntimeConfig> for FlowConfig {
    fn from(conf: &RuntimeConfig) -> Self {
        let flow_config = &conf.yaml_config.flow;
        FlowConfig {
            vtap_id: conf.vtap_id as u16,
            trident_type: conf.trident_type,
            tap_mode: conf.tap_mode,
            cloud_gateway_traffic: conf.yaml_config.cloud_gateway_traffic,
            collector_enabled: conf.collector_enabled,
            l7_log_tap_types: {
                let mut tap_types = [false; 256];
                for &t in conf.l7_log_store_tap_types.iter() {
                    if (t as u16) >= u16::from(TapType::Max) {
                        warn!("invalid tap type: {}", t);
                    } else {
                        tap_types[t as usize] = true;
                    }
                }
                tap_types
            },
            capacity: flow_config.capacity,
            hash_slots: flow_config.hash_slots,
            packet_delay: conf.yaml_config.packet_delay,
            flush_interval: flow_config.flush_interval,
            flow_timeout: FlowTimeout::from(TcpTimeout {
                established: flow_config.established_timeout.into(),
                closing_rst: flow_config.closing_rst_timeout.into(),
                others: flow_config.others_timeout.into(),
                opening_rst: flow_config.opening_rst_timeout.into(),
            }),
            ignore_tor_mac: flow_config.ignore_tor_mac,
            ignore_l2_end: flow_config.ignore_l2_end,
            ignore_idc_vlan: flow_config.ignore_idc_vlan,
            memory_pool_size: flow_config.memory_pool_size,
            l7_metrics_enabled: conf.l7_metrics_enabled,
            app_proto_log_enabled: conf.app_proto_log_enabled,
            l4_performance_enabled: conf.l4_performance_enabled,
            l7_log_packet_size: conf.l7_log_packet_size,
            l7_protocol_inference_max_fail_count: conf
                .yaml_config
                .l7_protocol_inference_max_fail_count,
            l7_protocol_inference_ttl: conf.yaml_config.l7_protocol_inference_ttl,
            packet_sequence_flag: conf.yaml_config.packet_sequence_flag, // Enterprise Edition Feature: packet-sequence
            packet_sequence_block_size: conf.yaml_config.packet_sequence_block_size, // Enterprise Edition Feature: packet-sequence
            l7_protocol_enabled_bitmap: L7ProtocolBitmap::from(
                &conf.yaml_config.l7_protocol_enabled,
            ),
            l7_protocol_parse_port_bitmap: Arc::new(
                (&conf.yaml_config).get_protocol_port_parse_bitmap(),
            ),
            plugins: PluginConfig {
                last_updated: conf
                    .plugins
                    .as_ref()
                    .and_then(|p| p.update_time)
                    .unwrap_or_default(),
                digest: {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    if let Some(plugins) = &conf.plugins {
                        plugins.update_time.hash(&mut hasher);
                        for plugin in plugins.wasm_plugins.iter() {
                            plugin.hash(&mut hasher);
                            trident::PluginType::Wasm.hash(&mut hasher);
                        }
                        for plugin in plugins.so_plugins.iter() {
                            plugin.hash(&mut hasher);
                            trident::PluginType::So.hash(&mut hasher);
                        }
                    }
                    hasher.finish()
                },
                names: {
                    let mut plugins = vec![];
                    if let Some(p) = &conf.plugins {
                        plugins.extend(
                            p.wasm_plugins
                                .iter()
                                .map(|p| (p.clone(), trident::PluginType::Wasm)),
                        );
                        plugins.extend(
                            p.so_plugins
                                .iter()
                                .map(|p| (p.clone(), trident::PluginType::So)),
                        );
                    }
                    plugins
                },
                wasm_plugins: vec![],
                so_plugins: vec![],
            },
            rrt_tcp_timeout: conf.yaml_config.rrt_tcp_timeout.as_micros() as usize,
            rrt_udp_timeout: conf.yaml_config.rrt_udp_timeout.as_micros() as usize,
            batched_buffer_size_limit: conf.yaml_config.batched_buffer_size_limit,
            oracle_parse_conf: conf.yaml_config.oracle_parse_config,
            obfuscate_enabled_protocols: L7ProtocolBitmap::from(
                &conf
                    .yaml_config
                    .l7_protocol_advanced_features
                    .obfuscate_enabled_protocols,
            ),
            server_ports: conf.yaml_config.server_ports.clone(),
            consistent_timestamp_in_l7_metrics: conf.yaml_config.consistent_timestamp_in_l7_metrics,
        }
    }
}

impl fmt::Debug for FlowConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlowConfig")
            .field("vtap_id", &self.vtap_id)
            .field("trident_type", &self.trident_type)
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
            .finish()
    }
}

impl FlowConfig {
    pub fn flow_capacity(&self) -> u32 {
        let default_capacity = FlowGeneratorConfig::default().capacity;
        match self.tap_mode {
            TapMode::Analyzer if self.capacity <= default_capacity => u32::MAX,
            _ => self.capacity,
        }
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

    pub fn insert(&mut self, rule: &MatchRule) {
        let mut node = &mut self.root;
        for ch in rule.prefix.chars() {
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

impl From<&HttpEndpointExtraction> for HttpEndpointTrie {
    fn from(v: &HttpEndpointExtraction) -> Self {
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

    pub fn new(blacklists: &Vec<L7LogBlacklist>) -> Option<BlacklistTrie> {
        if blacklists.is_empty() {
            return None;
        }

        let mut b = BlacklistTrie::default();
        for i in blacklists.iter() {
            b.insert(i);
        }
        Some(b)
    }

    pub fn insert(&mut self, rule: &L7LogBlacklist) {
        let mut node = match rule.field_name.to_ascii_lowercase().as_str() {
            Self::ENDPOINT => &mut self.endpoint,
            Self::REQUEST_TYPE => &mut self.request_type,
            Self::REQUEST_DOMAIN => &mut self.request_domain,
            Self::REQUEST_RESOURCE => &mut self.request_resource,
            _ => {
                warn!("Unsupported field_name: {}, only supports endpoint, request_type, request_domain, request_resource.", rule.field_name.as_str());
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
    pub l7_log_blacklist: HashMap<String, Vec<L7LogBlacklist>>,
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
    pub vtap_id: u16,
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
    pub vtap_id: u16,
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
    pub ebpf: EbpfYamlConfig,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
impl fmt::Debug for EbpfConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EbpfConfig")
            .field("collector_enabled", &self.collector_enabled)
            .field("l7_metrics_enabled", &self.l7_metrics_enabled)
            .field("vtap_id", &self.vtap_id)
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
        return self.l7_log_tap_types[u16::from(TapType::Any) as usize]
            || self.l7_log_tap_types[u16::from(TapType::Cloud) as usize];
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
    pub proxy_client: String,
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
        mut proxy_client: String,
        x_request_id: Vec<String>,
        trace_types: Vec<TraceType>,
        span_types: Vec<TraceType>,
        mut extra_log_fields: ExtraLogFields,
    ) -> Self {
        proxy_client.make_ascii_lowercase();

        let mut expected_headers_set = get_expected_headers();
        expected_headers_set.insert(proxy_client.as_bytes().to_vec());
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
            proxy_client,
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
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModuleConfig {
    pub enabled: bool,
    pub tap_mode: TapMode,
    pub yaml_config: YamlConfig,
    pub collector: CollectorConfig,
    pub environment: EnvironmentConfig,
    pub platform: PlatformConfig,
    pub dispatcher: DispatcherConfig,
    pub flow: FlowConfig,
    pub log_parser: LogParserConfig,
    pub pcap: PcapConfig,
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
    pub trident_type: TridentType,
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
            RuntimeConfig::default(),
        ))
        .unwrap()
    }
}

impl TryFrom<(Config, RuntimeConfig)> for ModuleConfig {
    type Error = ConfigError;

    fn try_from(conf: (Config, RuntimeConfig)) -> Result<Self, Self::Error> {
        let (static_config, conf) = conf;
        let controller_ip = static_config.controller_ips[0].parse::<IpAddr>().unwrap();
        let dest_ip = if conf.analyzer_ip.len() > 0 {
            conf.analyzer_ip.clone()
        } else {
            match controller_ip {
                IpAddr::V4(_) => Ipv4Addr::UNSPECIFIED.to_string(),
                IpAddr::V6(_) => Ipv6Addr::UNSPECIFIED.to_string(),
            }
        };
        let proxy_controller_ip = if conf.proxy_controller_ip.len() > 0 {
            conf.proxy_controller_ip.clone()
        } else {
            static_config.controller_ips[0].clone()
        };

        let config = ModuleConfig {
            enabled: conf.enabled,
            yaml_config: conf.yaml_config.clone(),
            tap_mode: conf.tap_mode,
            diagnose: DiagnoseConfig {
                enabled: conf.enabled,
                libvirt_xml_path: conf.libvirt_xml_path.parse().unwrap_or_default(),
            },
            environment: EnvironmentConfig {
                max_memory: conf.max_memory,
                max_millicpus: conf.max_millicpus,
                process_threshold: conf.process_threshold,
                thread_threshold: conf.thread_threshold,
                sys_free_memory_limit: conf.sys_free_memory_limit,
                log_file_size: conf.log_file_size,
                tap_mode: conf.tap_mode,
                guard_interval: conf.yaml_config.guard_interval,
                max_sockets: conf.yaml_config.max_sockets,
                max_sockets_tolerate_interval: conf.yaml_config.max_sockets_tolerate_interval,
                system_load_circuit_breaker_threshold: conf.system_load_circuit_breaker_threshold,
                system_load_circuit_breaker_recover: conf.system_load_circuit_breaker_recover,
                system_load_circuit_breaker_metric: conf.system_load_circuit_breaker_metric,
                page_cache_reclaim_percentage: conf.yaml_config.page_cache_reclaim_percentage,
            },
            synchronizer: SynchronizerConfig {
                sync_interval: Duration::from_secs(conf.sync_interval),
                output_vlan: conf.output_vlan,
                ntp_enabled: conf.ntp_enabled,
                max_escape: Duration::from_secs(conf.max_escape),
            },
            stats: StatsConfig {
                interval: Duration::from_secs(conf.stats_interval),
                host: conf.host.clone(),
                analyzer_ip: dest_ip.clone(),
                analyzer_port: conf.analyzer_port,
            },
            dispatcher: DispatcherConfig {
                global_pps_threshold: conf.global_pps_threshold,
                capture_packet_size: conf.capture_packet_size,
                dpdk_enabled: conf.yaml_config.dpdk_enabled,
                dispatcher_queue: conf.yaml_config.dispatcher_queue,
                l7_log_packet_size: conf.l7_log_packet_size,
                tunnel_type_bitmap: TunnelTypeBitmap::new(&conf.decap_types),
                tunnel_type_trim_bitmap: TunnelTypeBitmap::from_strings(
                    &conf.yaml_config.trim_tunnel_types,
                ),
                trident_type: conf.trident_type,
                vtap_id: conf.vtap_id as u16,
                capture_socket_type: conf.capture_socket_type,
                #[cfg(target_os = "linux")]
                extra_netns_regex: conf.extra_netns_regex.to_string(),
                tap_interface_regex: conf.tap_interface_regex.to_string(),
                if_mac_source: conf.if_mac_source,
                analyzer_ip: dest_ip.clone(),
                analyzer_port: conf.analyzer_port,
                proxy_controller_ip,
                proxy_controller_port: conf.proxy_controller_port,
                capture_bpf: conf.capture_bpf.to_string(),
                max_memory: conf.max_memory,
                af_packet_blocks: conf
                    .yaml_config
                    .get_af_packet_blocks(conf.tap_mode, conf.max_memory),
                #[cfg(any(target_os = "linux", target_os = "android"))]
                af_packet_version: conf.capture_socket_type.into(),
                tap_mode: conf.tap_mode,
                region_id: conf.region_id,
                pod_cluster_id: conf.pod_cluster_id,
                enabled: conf.enabled,
                npb_dedup_enabled: conf.npb_dedup_enabled,
                bond_group: if conf.yaml_config.tap_interface_bond_groups.is_empty() {
                    vec![]
                } else {
                    conf.yaml_config.tap_interface_bond_groups[0]
                        .tap_interfaces
                        .clone()
                },
                #[cfg(any(target_os = "linux", target_os = "android"))]
                cpu_set: CpuSet::new(),
            },
            sender: SenderConfig {
                mtu: conf.mtu,
                dest_ip: dest_ip.clone(),
                vtap_id: conf.vtap_id as u16,
                team_id: conf.team_id,
                organize_id: conf.organize_id,
                dest_port: conf.analyzer_port,
                npb_port: conf.yaml_config.npb_port,
                vxlan_flags: conf.yaml_config.vxlan_flags,
                npb_enable_qos_bypass: conf.yaml_config.enable_qos_bypass,
                npb_vlan: conf.output_vlan,
                npb_vlan_mode: conf.npb_vlan_mode,
                npb_dedup_enabled: conf.npb_dedup_enabled,
                npb_bps_threshold: conf.npb_bps_threshold,
                npb_socket_type: conf.npb_socket_type,
                server_tx_bandwidth_threshold: conf.server_tx_bandwidth_threshold,
                bandwidth_probe_interval: conf.bandwidth_probe_interval,
                collector_socket_type: conf.collector_socket_type,
                standalone_data_file_size: conf.yaml_config.standalone_data_file_size,
                standalone_data_file_dir: conf.yaml_config.standalone_data_file_dir.clone(),
                enabled: conf.collector_enabled,
            },
            npb: NpbConfig {
                mtu: conf.mtu,
                underlay_is_ipv6: controller_ip.is_ipv6(),
                npb_port: conf.yaml_config.npb_port,
                vxlan_flags: conf.yaml_config.vxlan_flags,
                ignore_overlay_vlan: conf.yaml_config.ignore_overlay_vlan,
                enable_qos_bypass: conf.yaml_config.enable_qos_bypass,
                output_vlan: conf.output_vlan,
                vlan_mode: conf.npb_vlan_mode,
                dedup_enabled: conf.npb_dedup_enabled,
                socket_type: conf.npb_socket_type,
                queue_size: conf.yaml_config.collector_sender_queue_size,
            },
            collector: CollectorConfig {
                enabled: conf.collector_enabled,
                inactive_server_port_enabled: conf.inactive_server_port_enabled,
                inactive_ip_enabled: conf.inactive_ip_enabled,
                vtap_flow_1s_enabled: conf.vtap_flow_1s_enabled,
                l4_log_collect_nps_threshold: conf.l4_log_collect_nps_threshold,
                l7_metrics_enabled: conf.l7_metrics_enabled,
                trident_type: conf.trident_type,
                vtap_id: conf.vtap_id as u16,
                l4_log_store_tap_types: {
                    let mut tap_types = [false; 256];
                    for &t in conf.l4_log_store_tap_types.iter() {
                        if (t as u16) >= u16::from(TapType::Max) {
                            warn!("invalid tap type: {}", t);
                        } else {
                            tap_types[t as usize] = true;
                        }
                    }
                    tap_types
                },
                l4_log_ignore_tap_sides: {
                    let mut tap_sides = [false; TapSide::MAX as usize + 1];
                    for t in conf.l4_log_ignore_tap_sides.iter() {
                        // TapSide values will be in range [0, TapSide::MAX]
                        tap_sides[*t as usize] = true;
                    }
                    tap_sides
                },
                cloud_gateway_traffic: conf.yaml_config.cloud_gateway_traffic,
                packet_delay: conf.yaml_config.packet_delay,
            },
            handler: HandlerConfig {
                npb_dedup_enabled: conf.npb_dedup_enabled,
                trident_type: conf.trident_type,
            },
            pcap: conf.yaml_config.pcap.clone(),
            platform: PlatformConfig {
                sync_interval: Duration::from_secs(conf.platform_sync_interval),
                kubernetes_cluster_id: static_config.kubernetes_cluster_id.clone(),
                libvirt_xml_path: conf.libvirt_xml_path.parse().unwrap_or_default(),
                kubernetes_poller_type: conf.yaml_config.kubernetes_poller_type,
                vtap_id: conf.vtap_id as u16,
                enabled: conf.platform_enabled,
                trident_type: conf.trident_type,
                epc_id: conf.epc_id,
                kubernetes_api_enabled: conf.kubernetes_api_enabled,
                kubernetes_api_list_limit: conf.yaml_config.kubernetes_api_list_limit,
                kubernetes_api_list_interval: conf.yaml_config.kubernetes_api_list_interval,
                kubernetes_resources: conf.yaml_config.kubernetes_resources.clone(),
                max_memory: conf.max_memory,
                namespace: if conf.yaml_config.kubernetes_namespace.is_empty() {
                    None
                } else {
                    Some(conf.yaml_config.kubernetes_namespace.clone())
                },
                thread_threshold: conf.thread_threshold,
                tap_mode: conf.tap_mode,
                #[cfg(any(target_os = "linux", target_os = "android"))]
                os_proc_scan_conf: OsProcScanConfig {
                    os_proc_root: conf.yaml_config.os_proc_root.clone(),
                    os_proc_socket_sync_interval: conf.yaml_config.os_proc_socket_sync_interval,
                    os_proc_socket_min_lifetime: conf.yaml_config.os_proc_socket_min_lifetime,
                    os_proc_regex: {
                        let mut v = vec![];
                        for i in &conf.yaml_config.os_proc_regex {
                            if let Ok(r) = ProcRegRewrite::try_from(i) {
                                v.push(r);
                            }
                        }

                        // append the .* at the end for accept the proc whic not match any regexp
                        v.push(
                            ProcRegRewrite::try_from(&OsProcRegexp {
                                match_regex: ".*".into(),
                                match_type: OS_PROC_REGEXP_MATCH_TYPE_PROC_NAME.into(),
                                rewrite_name: "".into(),
                                action: OS_PROC_REGEXP_MATCH_ACTION_ACCEPT.into(),
                            })
                            .unwrap(),
                        );
                        v
                    },
                    os_app_tag_exec_user: conf.yaml_config.os_app_tag_exec_user.clone(),
                    os_app_tag_exec: conf.yaml_config.os_app_tag_exec.clone(),
                    os_proc_sync_enabled: conf.yaml_config.os_proc_sync_enabled,
                    os_proc_sync_tagged_only: conf.yaml_config.os_proc_sync_tagged_only,
                },
                #[cfg(target_os = "windows")]
                os_proc_scan_conf: OsProcScanConfig {},
                agent_enabled: conf.enabled,
                #[cfg(target_os = "linux")]
                extra_netns_regex: conf.extra_netns_regex.to_string(),
            },
            flow: (&conf).into(),
            log_parser: LogParserConfig {
                l7_log_collect_nps_threshold: conf.l7_log_collect_nps_threshold,
                l7_log_session_aggr_timeout: conf.yaml_config.l7_log_session_aggr_timeout,
                l7_log_session_slot_capacity: conf.yaml_config.l7_log_session_slot_capacity,
                l7_log_dynamic: L7LogDynamicConfig::new(
                    conf.http_log_proxy_client.to_string().to_ascii_lowercase(),
                    conf.http_log_x_request_id
                        .split(',')
                        .map(|x| x.to_lowercase())
                        .collect(),
                    conf.http_log_trace_id
                        .split(',')
                        .map(|item| TraceType::from(item))
                        .collect(),
                    conf.http_log_span_id
                        .split(',')
                        .map(|item| TraceType::from(item))
                        .collect(),
                    conf.yaml_config
                        .l7_protocol_advanced_features
                        .extra_log_fields
                        .clone(),
                ),
                l7_log_ignore_tap_sides: {
                    let mut tap_sides = [false; TapSide::MAX as usize + 1];
                    for t in conf.l7_log_ignore_tap_sides.iter() {
                        // TapSide values will be in range [0, TapSide::MAX]
                        tap_sides[*t as usize] = true;
                    }
                    tap_sides
                },
                http_endpoint_disabled: conf
                    .yaml_config
                    .l7_protocol_advanced_features
                    .http_endpoint_extraction
                    .disabled,
                http_endpoint_trie: HttpEndpointTrie::from(
                    &conf
                        .yaml_config
                        .l7_protocol_advanced_features
                        .http_endpoint_extraction,
                ),
                obfuscate_enabled_protocols: L7ProtocolBitmap::from(
                    &conf
                        .yaml_config
                        .l7_protocol_advanced_features
                        .obfuscate_enabled_protocols,
                ),
                l7_log_blacklist: conf.yaml_config.l7_log_blacklist.clone(),
                l7_log_blacklist_trie: {
                    let mut blacklist_trie = HashMap::new();
                    for (k, v) in conf.yaml_config.l7_log_blacklist.iter() {
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
                    .yaml_config
                    .l7_protocol_advanced_features
                    .unconcerned_dns_nxdomain_response_suffixes
                    .clone(),
                unconcerned_dns_nxdomain_trie: DnsNxdomainTrie::from(
                    &conf
                        .yaml_config
                        .l7_protocol_advanced_features
                        .unconcerned_dns_nxdomain_response_suffixes,
                ),
            },
            debug: DebugConfig {
                vtap_id: conf.vtap_id as u16,
                enabled: conf.debug_enabled,
                controller_ips: static_config
                    .controller_ips
                    .iter()
                    .map(|c| c.parse::<IpAddr>().unwrap())
                    .collect(),
                listen_port: conf.yaml_config.debug_listen_port,
                controller_port: static_config.controller_port,
                agent_mode: static_config.agent_mode,
            },
            log: LogConfig {
                log_level: conf.log_level,
                log_threshold: conf.log_threshold,
                log_retention: conf.log_retention,
                rsyslog_enabled: {
                    if dest_ip == Ipv4Addr::UNSPECIFIED.to_string()
                        || dest_ip == Ipv6Addr::UNSPECIFIED.to_string()
                    {
                        info!("analyzer_ip not set, remote log disabled");
                        false
                    } else {
                        conf.rsyslog_enabled
                    }
                },
                host: conf.host.clone(),
            },
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf: EbpfConfig {
                collector_enabled: conf.collector_enabled,
                l7_metrics_enabled: conf.l7_metrics_enabled,
                vtap_id: conf.vtap_id as u16,
                epc_id: conf.epc_id,
                l7_log_session_timeout: conf.yaml_config.l7_log_session_aggr_timeout,
                l7_log_packet_size: CAP_LEN_MAX.min(conf.l7_log_packet_size as usize),
                l7_log_tap_types: {
                    let mut tap_types = [false; 256];
                    for &t in conf.l7_log_store_tap_types.iter() {
                        if (t as u16) >= u16::from(TapType::Max) {
                            warn!("invalid tap type: {}", t);
                        } else {
                            tap_types[t as usize] = true;
                        }
                    }
                    tap_types
                },
                l7_protocol_inference_max_fail_count: conf
                    .yaml_config
                    .l7_protocol_inference_max_fail_count,
                l7_protocol_inference_ttl: conf.yaml_config.l7_protocol_inference_ttl,
                ctrl_mac: if is_tt_workload(conf.trident_type) {
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
                    &conf.yaml_config.l7_protocol_enabled,
                ),
                l7_protocol_parse_port_bitmap: Arc::new(
                    (&conf.yaml_config).get_protocol_port_parse_bitmap(),
                ),
                l7_protocol_ports: conf.yaml_config.get_protocol_port(),
                queue_size: conf.yaml_config.ebpf_collector_queue_size,
                ebpf: conf.yaml_config.ebpf.clone(),
            },
            metric_server: MetricServerConfig {
                enabled: conf.external_agent_http_proxy_enabled,
                port: conf.external_agent_http_proxy_port as u16,
                compressed: conf.yaml_config.external_agent_http_proxy_compressed,
            },
            trident_type: conf.trident_type,
            port_config: PortConfig {
                analyzer_port: conf.analyzer_port,
                proxy_controller_port: conf.proxy_controller_port,
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
            ModuleConfig::try_from((config.clone(), RuntimeConfig::default())).unwrap();
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
        Map::new(self.current_config.clone(), |config| -> &PcapConfig {
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

    pub fn on_config(
        &mut self,
        new_config: RuntimeConfig,
        exception_handler: &ExceptionHandler,
        mut components: Option<&mut AgentComponents>,
        #[cfg(target_os = "linux")] api_watcher: &Arc<ApiWatcher>,
        runtime: &Runtime,
        session: &Session,
        agent_id: &AgentId,
    ) -> Vec<fn(&ConfigHandler, &mut AgentComponents)> {
        let candidate_config = &mut self.candidate_config;
        let static_config = &self.static_config;
        let yaml_config = &mut candidate_config.yaml_config;
        let mut new_config: ModuleConfig = (static_config.clone(), new_config).try_into().unwrap();
        let mut callbacks: Vec<fn(&ConfigHandler, &mut AgentComponents)> = vec![];
        let mut restart_dispatcher = false;

        if candidate_config.tap_mode != new_config.tap_mode {
            info!("tap_mode set to {:?}", new_config.tap_mode);
            candidate_config.tap_mode = new_config.tap_mode;
            if let Some(c) = components.as_mut() {
                if yaml_config.local_dispatcher_count > 1 {
                    info!("tap_mode changes and fanout is enabled, deepflow-agent restart...");
                    crate::utils::notify_exit(public::consts::NORMAL_EXIT_WITH_RESTART);
                    return vec![];
                } else {
                    info!("tap_mode changes, dispatcher restart...");
                    c.clear_dispatcher_components();
                }
            }
        }

        if candidate_config.tap_mode != TapMode::Analyzer
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

        if yaml_config.analyzer_dedup_disabled != new_config.yaml_config.analyzer_dedup_disabled {
            yaml_config.analyzer_dedup_disabled = new_config.yaml_config.analyzer_dedup_disabled;
            info!(
                "analyzer_dedup_disabled set to {:?}",
                yaml_config.analyzer_dedup_disabled
            );
        }

        if yaml_config.mirror_traffic_pcp != new_config.yaml_config.mirror_traffic_pcp {
            yaml_config.mirror_traffic_pcp = new_config.yaml_config.mirror_traffic_pcp;
            info!(
                "mirror_traffic_pcp set to {:?}",
                yaml_config.mirror_traffic_pcp
            );
        }

        if yaml_config.prometheus_extra_config != new_config.yaml_config.prometheus_extra_config {
            info!(
                "prometheus_extra_config set to {:?}",
                new_config.yaml_config.prometheus_extra_config
            );
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        if yaml_config.process_scheduling_priority
            != new_config.yaml_config.process_scheduling_priority
        {
            info!(
                "Process scheduling priority set to {}.",
                new_config.yaml_config.process_scheduling_priority
            );
            let pid = std::process::id();
            unsafe {
                if libc::setpriority(
                    libc::PRIO_PROCESS,
                    pid,
                    new_config.yaml_config.process_scheduling_priority as libc::c_int,
                ) != 0
                {
                    warn!(
                        "Process scheduling priority set {} to pid {} error.",
                        new_config.yaml_config.process_scheduling_priority, pid
                    );
                }
            }
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        if yaml_config.cpu_affinity != new_config.yaml_config.cpu_affinity || components.is_none() {
            info!(
                "CPU Affinity set to {}.",
                new_config.yaml_config.cpu_affinity
            );
            let mut cpu_set = CpuSet::new();
            let splits = new_config.yaml_config.cpu_affinity.split(',');
            let mut invalid_config = false;
            let system = System::new_with_specifics(
                RefreshKind::new().with_cpu(CpuRefreshKind::everything()),
            );
            let cpu_count = system.cpus().len() as usize;
            if new_config.yaml_config.cpu_affinity.len() > 0 {
                for id in splits.into_iter() {
                    match id.parse::<usize>() {
                        Ok(id) if id < cpu_count => {
                            if let Err(e) = cpu_set.set(id) {
                                warn!(
                                    "Invalid CPU Affinity config {}, error: {:?}",
                                    new_config.yaml_config.cpu_affinity, e
                                );
                                invalid_config = true;
                            }
                        }
                        _ => {
                            invalid_config = true;
                            break;
                        }
                    };
                }
            } else {
                for i in 0..cpu_count {
                    let _ = cpu_set.set(i);
                }
            }

            if invalid_config {
                warn!(
                    "Invalid CPU Affinity config {}.",
                    new_config.yaml_config.cpu_affinity
                );
            } else {
                let pid = std::process::id() as i32;
                if let Err(e) = sched_setaffinity(Pid::from_raw(pid), &cpu_set) {
                    warn!("CPU Affinity({:?}) bind error: {:?}.", &cpu_set, e);
                }
                new_config.dispatcher.cpu_set = cpu_set;
            }
        }

        if yaml_config.external_profile_integration_disabled
            != new_config.yaml_config.external_profile_integration_disabled
        {
            info!(
                "external_profile_integration_disabled set to {}",
                new_config.yaml_config.external_profile_integration_disabled
            );
        }
        if yaml_config.external_trace_integration_disabled
            != new_config.yaml_config.external_trace_integration_disabled
        {
            info!(
                "external_trace_integration_disabled set to {}",
                new_config.yaml_config.external_trace_integration_disabled
            );
        }
        if yaml_config.external_metric_integration_disabled
            != new_config.yaml_config.external_metric_integration_disabled
        {
            info!(
                "external_metric_integration_disabled set to {}",
                new_config.yaml_config.external_metric_integration_disabled
            );
        }
        if yaml_config.external_log_integration_disabled
            != new_config.yaml_config.external_log_integration_disabled
        {
            info!(
                "external_log_integration_disabled set to {}",
                new_config.yaml_config.external_log_integration_disabled
            );
        }

        if *yaml_config != new_config.yaml_config {
            *yaml_config = new_config.yaml_config;
        }

        if candidate_config.dispatcher != new_config.dispatcher {
            #[cfg(target_os = "linux")]
            if candidate_config.dispatcher.extra_netns_regex
                != new_config.dispatcher.extra_netns_regex
            {
                info!(
                    "extra_netns_regex set to: {:?}",
                    new_config.dispatcher.extra_netns_regex
                );
                if let Some(c) = components.as_ref() {
                    let old_regex = if candidate_config.dispatcher.extra_netns_regex != "" {
                        regex::Regex::new(&candidate_config.dispatcher.extra_netns_regex).ok()
                    } else {
                        None
                    };

                    let regex = new_config.dispatcher.extra_netns_regex.as_ref();
                    let regex = if regex != "" {
                        match regex::Regex::new(regex) {
                            Ok(re) => {
                                info!("platform monitoring extra netns: /{}/", regex);
                                Some(re)
                            }
                            Err(_) => {
                                warn!("platform monitoring no extra netns because regex /{}/ is invalid", regex);
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
                        return vec![];
                    }

                    c.kubernetes_poller.set_netns_regex(regex);
                }
            }

            if candidate_config.dispatcher.if_mac_source != new_config.dispatcher.if_mac_source {
                if candidate_config.tap_mode != TapMode::Local {
                    info!(
                        "if_mac_source set to {:?}",
                        new_config.dispatcher.if_mac_source
                    );
                }
            }

            #[cfg(target_os = "windows")]
            if candidate_config.tap_mode == TapMode::Local
                && candidate_config.dispatcher.tap_interface_regex
                    != new_config.dispatcher.tap_interface_regex
            {
                fn switch_recv_engine(handler: &ConfigHandler, comp: &mut AgentComponents) {
                    for d in comp.dispatcher_components.iter() {
                        if let Err(e) = d
                            .dispatcher
                            .switch_recv_engine(&handler.candidate_config.dispatcher)
                        {
                            log::error!(
                                "switch RecvEngine error: {}, deepflow-agent restart...",
                                e
                            );
                            crate::utils::notify_exit(-1);
                            return;
                        }
                    }
                }
                callbacks.push(switch_recv_engine);
            }

            if candidate_config.dispatcher.capture_packet_size
                != new_config.dispatcher.capture_packet_size
            {
                candidate_config.dispatcher.capture_packet_size =
                    new_config.dispatcher.capture_packet_size;
                if !components.is_none() {
                    info!("Capture packet size update, deepflow-agent restart...");
                    crate::utils::notify_exit(1);
                    return vec![];
                }
            }

            if candidate_config.dispatcher.capture_socket_type
                != new_config.dispatcher.capture_socket_type
            {
                restart_dispatcher = !cfg!(target_os = "windows");
            }

            if candidate_config.dispatcher.enabled != new_config.dispatcher.enabled {
                if new_config.dispatcher.enabled {
                    info!(
                        "enabled set to {}, dispatcher start...",
                        new_config.dispatcher.enabled
                    );
                    fn start_dispatcher(handler: &ConfigHandler, components: &mut AgentComponents) {
                        match handler.candidate_config.tap_mode {
                            TapMode::Analyzer => {
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
                    callbacks.push(start_dispatcher);
                } else {
                    info!(
                        "enabled set to {}, dispatcher stop...",
                        new_config.dispatcher.enabled
                    );
                    fn stop_dispatcher(_: &ConfigHandler, components: &mut AgentComponents) {
                        for d in components.dispatcher_components.iter_mut() {
                            d.stop();
                        }
                    }
                    callbacks.push(stop_dispatcher);
                }
            }

            if candidate_config.dispatcher.max_memory != new_config.dispatcher.max_memory {
                if yaml_config
                    .get_af_packet_blocks(new_config.tap_mode, new_config.dispatcher.max_memory)
                    != yaml_config.get_af_packet_blocks(
                        candidate_config.tap_mode,
                        candidate_config.dispatcher.max_memory,
                    )
                    || yaml_config.get_fast_path_map_size(new_config.dispatcher.max_memory)
                        != yaml_config
                            .get_fast_path_map_size(candidate_config.dispatcher.max_memory)
                    || candidate_config.get_channel_size(new_config.dispatcher.max_memory)
                        != candidate_config.get_channel_size(candidate_config.dispatcher.max_memory)
                    || candidate_config.get_flow_capacity(new_config.dispatcher.max_memory)
                        != candidate_config
                            .get_flow_capacity(candidate_config.dispatcher.max_memory)
                {
                    restart_dispatcher = true;
                    info!("max_memory change, restart dispatcher");
                }
            }

            if candidate_config.dispatcher.global_pps_threshold
                != new_config.dispatcher.global_pps_threshold
            {
                candidate_config.dispatcher.global_pps_threshold =
                    new_config.dispatcher.global_pps_threshold;

                fn leaky_bucket_callback(
                    handler: &ConfigHandler,
                    components: &mut AgentComponents,
                ) {
                    match handler.candidate_config.tap_mode {
                        TapMode::Analyzer => {
                            components.rx_leaky_bucket.set_rate(None);
                            info!("dispatcher.global pps set ulimit when tap_mode=analyzer");
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
                callbacks.push(leaky_bucket_callback);
            }

            info!(
                "dispatcher config change from {:#?} to {:#?}",
                candidate_config.dispatcher, new_config.dispatcher
            );
            candidate_config.dispatcher = new_config.dispatcher;
        }

        if candidate_config.log != new_config.log {
            if new_config.log.host == "" {
                new_config.log.host = candidate_config.log.host.clone();
            }
            if candidate_config.log.rsyslog_enabled != new_config.log.rsyslog_enabled {
                if new_config.log.rsyslog_enabled {
                    info!("Enable rsyslog");
                } else {
                    info!("Disable rsyslog");
                }
            }
            if candidate_config.log.log_level != new_config.log.log_level {
                match self.logger_handle.as_mut() {
                    Some(h) => match h
                        .parse_and_push_temp_spec(new_config.log.log_level.as_str().to_lowercase())
                    {
                        Ok(_) => {
                            candidate_config.log.log_level = new_config.log.log_level;
                            info!("log level set to {}", new_config.log.log_level);
                        }
                        Err(e) => warn!("failed to set log_level: {}", e),
                    },
                    None => warn!("logger_handle not set"),
                }
            }
            if candidate_config.log.host != new_config.log.host {
                info!(
                    "remote log hostname {} -> {}",
                    candidate_config.log.host, new_config.log.host
                )
            }
            if candidate_config.log.log_threshold != new_config.log.log_threshold {
                info!(
                    "remote log threshold {} -> {}",
                    candidate_config.log.log_threshold, new_config.log.log_threshold
                )
            }
            if candidate_config.log.log_retention != new_config.log.log_retention {
                match self.logger_handle.as_mut() {
                    Some(h) => match h.flw_config() {
                        Err(FlexiLoggerError::NoFileLogger) => {
                            info!("no file logger, skipped log_retention change")
                        }
                        _ => match h.reset_flw(
                            &FileLogWriter::builder(
                                FileSpec::try_from(&static_config.log_file).unwrap(),
                            )
                            .rotate(
                                Criterion::Age(Age::Day),
                                Naming::Timestamps,
                                Cleanup::KeepLogFiles(new_config.log.log_retention as usize),
                            )
                            .create_symlink(&static_config.log_file)
                            .append(),
                        ) {
                            Ok(_) => {
                                info!("log_retention set to {}", new_config.log.log_retention);
                            }
                            Err(e) => {
                                warn!("failed to set log_retention: {}", e);
                            }
                        },
                    },
                    None => warn!("logger_handle not set"),
                }
            }
            candidate_config.log = new_config.log;
        }

        if candidate_config.stats != new_config.stats {
            info!(
                "stats config change from {:#?} to {:#?}",
                candidate_config.stats, new_config.stats
            );
            candidate_config.stats = new_config.stats;

            fn stats_callback(handler: &ConfigHandler, components: &mut AgentComponents) {
                let c = &components.stats_collector;
                c.set_hostname(handler.candidate_config.stats.host.clone());
                c.set_min_interval(handler.candidate_config.stats.interval);
            }
            callbacks.push(stats_callback);
        }

        if candidate_config.debug != new_config.debug {
            info!(
                "debug config change from {:#?} to {:#?}",
                candidate_config.debug, new_config.debug
            );
            candidate_config.debug = new_config.debug;

            fn debug_callback(handler: &ConfigHandler, components: &mut AgentComponents) {
                if handler.candidate_config.debug.enabled {
                    components.debugger.start();
                } else {
                    components.debugger.stop();
                }
            }

            callbacks.push(debug_callback);
        }

        if candidate_config.diagnose != new_config.diagnose {
            //TODO diagnose stuff
            info!(
                "diagnose config change from {:#?} to {:#?}",
                candidate_config.diagnose, new_config.diagnose
            );
            candidate_config.diagnose = new_config.diagnose;
        }

        if candidate_config.environment.max_memory != new_config.environment.max_memory {
            if let Some(ref components) = components {
                components
                    .policy_setter
                    .set_memory_limit(new_config.environment.max_memory);
            }
        }

        if candidate_config.tap_mode != TapMode::Analyzer {
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

            if candidate_config.environment.max_memory != max_memory {
                info!("memory set ulimit when tap_mode=analyzer");
                candidate_config.environment.max_memory = max_memory;
            }

            if candidate_config.environment.max_millicpus != max_millicpus {
                info!("cpu set ulimit when tap_mode=analyzer");
                candidate_config.environment.max_millicpus = max_millicpus;
            }
        }

        if candidate_config.environment.max_sockets != new_config.environment.max_sockets {
            info!("max_sockets set to {}", new_config.environment.max_sockets);
            candidate_config.environment.max_sockets = new_config.environment.max_sockets;
        }

        if candidate_config.environment.max_sockets_tolerate_interval
            != new_config.environment.max_sockets_tolerate_interval
        {
            info!(
                "max_sockets_tolerate_interval set to {:?}",
                new_config.environment.max_sockets_tolerate_interval
            );
            candidate_config.environment.max_sockets_tolerate_interval =
                new_config.environment.max_sockets_tolerate_interval;
        }

        if candidate_config.environment.sys_free_memory_limit
            != new_config.environment.sys_free_memory_limit
        {
            info!(
                "sys_free_memory_limit set to {}",
                new_config.environment.sys_free_memory_limit
            );
            candidate_config.environment.sys_free_memory_limit =
                new_config.environment.sys_free_memory_limit;
        }

        if candidate_config.environment.process_threshold
            != new_config.environment.process_threshold
        {
            info!(
                "process_threshold set to {}",
                new_config.environment.process_threshold
            );
            candidate_config.environment.process_threshold =
                new_config.environment.process_threshold;
        }

        if candidate_config.environment.thread_threshold != new_config.environment.thread_threshold
        {
            info!(
                "thread_threshold set to {}",
                new_config.environment.thread_threshold
            );
            candidate_config.environment.thread_threshold = new_config.environment.thread_threshold;
        }

        if candidate_config.environment.log_file_size != new_config.environment.log_file_size {
            info!(
                "log_file_size set to {}",
                new_config.environment.log_file_size
            );
            candidate_config.environment.log_file_size = new_config.environment.log_file_size;
        }

        if candidate_config
            .environment
            .system_load_circuit_breaker_metric
            != new_config.environment.system_load_circuit_breaker_metric
        {
            info!(
                "system_load_circuit_breaker_metric set to {:?}",
                new_config.environment.system_load_circuit_breaker_metric
            );
            candidate_config
                .environment
                .system_load_circuit_breaker_metric =
                new_config.environment.system_load_circuit_breaker_metric;
        }

        if candidate_config
            .environment
            .system_load_circuit_breaker_recover
            != new_config.environment.system_load_circuit_breaker_recover
        {
            info!(
                "system_load_circuit_breaker_recover set to {:?}",
                new_config.environment.system_load_circuit_breaker_recover
            );
            candidate_config
                .environment
                .system_load_circuit_breaker_recover =
                new_config.environment.system_load_circuit_breaker_recover;
        }

        if candidate_config
            .environment
            .system_load_circuit_breaker_threshold
            != new_config.environment.system_load_circuit_breaker_threshold
        {
            info!(
                "system_load_circuit_breaker_threshold set to {}",
                new_config.environment.system_load_circuit_breaker_threshold
            );
            candidate_config
                .environment
                .system_load_circuit_breaker_threshold =
                new_config.environment.system_load_circuit_breaker_threshold;
        }
        if candidate_config.environment.page_cache_reclaim_percentage
            != new_config.environment.page_cache_reclaim_percentage
        {
            info!(
                "Update page_cache_reclaim_percentage from {:?} to {:?}.",
                candidate_config.environment.page_cache_reclaim_percentage,
                new_config.environment.page_cache_reclaim_percentage
            );
            candidate_config.environment.page_cache_reclaim_percentage =
                new_config.environment.page_cache_reclaim_percentage;
        }

        if candidate_config.flow != new_config.flow {
            if candidate_config.flow.collector_enabled != new_config.flow.collector_enabled {
                restart_dispatcher = true;
            }
            info!(
                "flow_generator config change from {:#?} to {:#?}",
                candidate_config.flow, new_config.flow
            );
            if candidate_config.flow.plugins.digest != new_config.flow.plugins.digest {
                info!(
                    "plugins changed, pulling {} plugins from server",
                    new_config.flow.plugins.names.len()
                );
                new_config
                    .flow
                    .plugins
                    .fill_plugin_prog_from_server(runtime, session, agent_id);
            }
            candidate_config.flow = new_config.flow;
        }

        if candidate_config.collector != new_config.collector {
            if candidate_config.collector.l4_log_store_tap_types
                != new_config.collector.l4_log_store_tap_types
            {
                info!(
                    "collector config l4_log_store_tap_types change from {:?} to {:?}",
                    candidate_config
                        .collector
                        .l4_log_store_tap_types
                        .iter()
                        .enumerate()
                        .filter(|&(_, b)| *b)
                        .collect::<Vec<_>>(),
                    new_config
                        .collector
                        .l4_log_store_tap_types
                        .iter()
                        .enumerate()
                        .filter(|&(_, b)| *b)
                        .collect::<Vec<_>>()
                );
            }
            if candidate_config.collector.l4_log_ignore_tap_sides
                != new_config.collector.l4_log_ignore_tap_sides
            {
                info!(
                    "collector config l4_log_store_tap_types change from {:?} to {:?}",
                    candidate_config
                        .collector
                        .l4_log_ignore_tap_sides
                        .iter()
                        .enumerate()
                        .filter_map(|(i, b)| if *b {
                            TapSide::try_from(i as u8).ok()
                        } else {
                            None
                        })
                        .collect::<Vec<_>>(),
                    new_config
                        .collector
                        .l4_log_ignore_tap_sides
                        .iter()
                        .enumerate()
                        .filter_map(|(i, b)| if *b {
                            TapSide::try_from(i as u8).ok()
                        } else {
                            None
                        })
                        .collect::<Vec<_>>(),
                );
            }

            if candidate_config.collector.vtap_id != new_config.collector.vtap_id {
                if new_config.collector.enabled {
                    restart_dispatcher = true;
                }
            }

            if candidate_config.collector.l7_metrics_enabled
                != new_config.collector.l7_metrics_enabled
            {
                info!(
                    "quadruple generator update l7_metrics_enabled to {}",
                    new_config.collector.l7_metrics_enabled
                );
                candidate_config.collector.l7_metrics_enabled =
                    new_config.collector.l7_metrics_enabled;
            }
            if candidate_config.collector.vtap_flow_1s_enabled
                != new_config.collector.vtap_flow_1s_enabled
            {
                info!(
                    "quadruple generator update vtap_flow_1s_enabled to {}",
                    new_config.collector.vtap_flow_1s_enabled
                );
                candidate_config.collector.vtap_flow_1s_enabled =
                    new_config.collector.vtap_flow_1s_enabled;
            }
            if candidate_config.collector.enabled != new_config.collector.enabled {
                info!(
                    "quadruple generator update collector_enabled to {}",
                    new_config.collector.enabled
                );
                candidate_config.collector.enabled = new_config.collector.enabled;
            }

            info!(
                "collector config change from {:#?} to {:#?}",
                candidate_config.collector, new_config.collector
            );
            candidate_config.collector = new_config.collector;
        }

        if candidate_config.platform != new_config.platform {
            let old_cfg = &candidate_config.platform;
            let new_cfg = &new_config.platform;

            if old_cfg.enabled != new_cfg.enabled {
                info!("Platform enabled set to {}", new_cfg.enabled);
            }
            if old_cfg.kubernetes_api_list_limit != new_cfg.kubernetes_api_list_limit {
                info!(
                    "Kubernetes API list limit set to {}",
                    new_cfg.kubernetes_api_list_limit
                );
            }
            if old_cfg.kubernetes_api_list_interval != new_cfg.kubernetes_api_list_interval {
                info!(
                    "Kubernetes API list interval set to {:?}",
                    new_cfg.kubernetes_api_list_interval
                );
            }
            if old_cfg.kubernetes_resources != new_cfg.kubernetes_resources {
                info!(
                    "Kubernetes resources set to {:?}",
                    new_cfg.kubernetes_resources
                );
            }
            if old_cfg.kubernetes_api_enabled != new_cfg.kubernetes_api_enabled {
                info!(
                    "Kubernetes API enabled set to {}",
                    new_cfg.kubernetes_api_enabled
                );
            }

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

            info!(
                "platform config change from {:#?} to {:#?}",
                candidate_config.platform, new_config.platform
            );
            candidate_config.platform = new_config.platform;

            #[cfg(target_os = "linux")]
            if static_config.agent_mode == RunningMode::Managed {
                fn platform_callback(handler: &ConfigHandler, components: &mut AgentComponents) {
                    let conf = &handler.candidate_config.platform;

                    if conf.agent_enabled
                        && (conf.tap_mode == TapMode::Local || is_tt_pod(conf.trident_type))
                    {
                        if is_tt_pod(conf.trident_type) {
                            components.kubernetes_poller.start();
                        } else {
                            components.kubernetes_poller.stop();
                        }
                    }
                }
                callbacks.push(platform_callback);
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
                if candidate_config.tap_mode != TapMode::Analyzer {
                    restart_dispatcher = true;
                }
            }

            if candidate_config.sender.npb_dedup_enabled != new_config.sender.npb_dedup_enabled {
                if candidate_config.tap_mode != TapMode::Analyzer {
                    restart_dispatcher = true;
                }
            }

            if let Some(components) = &components {
                if candidate_config.sender.bandwidth_probe_interval
                    != new_config.sender.bandwidth_probe_interval
                {
                    info!(
                        "Npb tx interface bandwidth probe interval set to {}s.",
                        new_config.sender.bandwidth_probe_interval.as_secs()
                    );
                    components
                        .npb_bandwidth_watcher
                        .set_interval(new_config.sender.bandwidth_probe_interval.as_secs());
                }
                if candidate_config.sender.server_tx_bandwidth_threshold
                    != new_config.sender.server_tx_bandwidth_threshold
                {
                    info!(
                        "Npb tx interface bandwidth threshold set to {}.",
                        new_config.sender.server_tx_bandwidth_threshold
                    );
                    components
                        .npb_bandwidth_watcher
                        .set_nic_rate(new_config.sender.server_tx_bandwidth_threshold);
                }
                if candidate_config.sender.npb_bps_threshold != new_config.sender.npb_bps_threshold
                {
                    info!(
                        "Npb bps threshold set to {}.",
                        new_config.sender.npb_bps_threshold
                    );
                    components
                        .npb_bandwidth_watcher
                        .set_npb_rate(new_config.sender.npb_bps_threshold);
                }
            }

            info!(
                "sender config change from {:#?} to {:#?}",
                candidate_config.sender, new_config.sender
            );
            candidate_config.sender = new_config.sender;
        }

        if candidate_config.handler != new_config.handler {
            if candidate_config.handler.npb_dedup_enabled != new_config.handler.npb_dedup_enabled {
                if candidate_config.tap_mode != TapMode::Analyzer {
                    restart_dispatcher = true;
                }
            }
            info!(
                "handler config change from {:#?} to {:#?}",
                candidate_config.handler, new_config.handler
            );
            candidate_config.handler = new_config.handler;
        }

        if candidate_config.log_parser != new_config.log_parser {
            info!(
                "log_parser config change from {:#?} to {:#?}",
                candidate_config.log_parser, new_config.log_parser
            );

            if candidate_config.log_parser.l7_log_dynamic != new_config.log_parser.l7_log_dynamic {
                info!(
                    "l7 log dynamic config change from {:#?} to {:#?}",
                    candidate_config.log_parser.l7_log_dynamic,
                    new_config.log_parser.l7_log_dynamic
                );
            }
            if candidate_config.log_parser.l7_log_collect_nps_threshold
                != new_config.log_parser.l7_log_collect_nps_threshold
            {
                info!(
                    "l7 log collect nps threshold set to {}",
                    new_config.log_parser.l7_log_collect_nps_threshold
                );
            }
            if candidate_config.log_parser.l7_log_ignore_tap_sides
                != new_config.log_parser.l7_log_ignore_tap_sides
            {
                info!(
                    "l7 log config l7_log_store_tap_types change from {:?} to {:?}",
                    candidate_config
                        .log_parser
                        .l7_log_ignore_tap_sides
                        .iter()
                        .enumerate()
                        .filter_map(|(i, b)| if *b {
                            TapSide::try_from(i as u8).ok()
                        } else {
                            None
                        })
                        .collect::<Vec<_>>(),
                    new_config
                        .log_parser
                        .l7_log_ignore_tap_sides
                        .iter()
                        .enumerate()
                        .filter_map(|(i, b)| if *b {
                            TapSide::try_from(i as u8).ok()
                        } else {
                            None
                        })
                        .collect::<Vec<_>>(),
                );
            }

            candidate_config.log_parser = new_config.log_parser;
        }

        if candidate_config.synchronizer != new_config.synchronizer {
            info!(
                "synchronizer config change from {:#?} to {:#?}",
                candidate_config.synchronizer, new_config.synchronizer
            );
            candidate_config.synchronizer = new_config.synchronizer;
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        if candidate_config.ebpf != new_config.ebpf
            && candidate_config.tap_mode != TapMode::Analyzer
        {
            info!(
                "ebpf config change from {:#?} to {:#?}",
                candidate_config.ebpf, new_config.ebpf
            );
            candidate_config.ebpf = new_config.ebpf;

            fn ebpf_callback(handler: &ConfigHandler, components: &mut AgentComponents) {
                if let Some(d) = components.ebpf_dispatcher_component.as_mut() {
                    d.ebpf_collector
                        .on_config_change(&handler.candidate_config.ebpf);
                }
            }
            callbacks.push(ebpf_callback);
        }

        if candidate_config.trident_type != new_config.trident_type {
            info!(
                "trident_type change from {:?} to {:?}",
                candidate_config.trident_type, new_config.trident_type
            );
            candidate_config.trident_type = new_config.trident_type;
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

            // 当端口更新后，在enabled情况下需要重启服务器重新监听
            if candidate_config.metric_server.port != new_config.metric_server.port {
                if let Some(c) = components.as_mut() {
                    c.metrics_server_component
                        .external_metrics_server
                        .set_port(new_config.metric_server.port);
                }
            }
            if candidate_config.metric_server.compressed != new_config.metric_server.compressed {
                fn metric_server_callback(
                    handler: &ConfigHandler,
                    components: &mut AgentComponents,
                ) {
                    components
                        .metrics_server_component
                        .external_metrics_server
                        .enable_compressed(handler.candidate_config.metric_server.compressed);
                }
                callbacks.push(metric_server_callback);
            }
            info!(
                "integration collector config change from {:#?} to {:#?}",
                candidate_config.metric_server, new_config.metric_server
            );
            candidate_config.metric_server = new_config.metric_server;
        }

        if candidate_config.npb != new_config.npb {
            fn dispatcher_callback(handler: &ConfigHandler, components: &mut AgentComponents) {
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
                    handler.candidate_config.npb.socket_type == SocketType::RawUdp,
                );
            }
            if components.is_some() {
                callbacks.push(dispatcher_callback);
            }
            info!(
                "npb config change from {:#?} to {:#?}",
                candidate_config.npb, new_config.npb
            );
            candidate_config.npb = new_config.npb;
            restart_dispatcher = true;
        }

        // avoid first config changed to restart dispatcher
        if components.is_some() && restart_dispatcher && candidate_config.dispatcher.enabled {
            fn dispatcher_callback(handler: &ConfigHandler, components: &mut AgentComponents) {
                info!("Configuration changes and dispatcher restart...");
                for d in components.dispatcher_components.iter_mut() {
                    d.stop();
                }
                if handler.candidate_config.tap_mode != TapMode::Analyzer
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
            callbacks.push(dispatcher_callback);
        }

        // deploy updated config
        self.current_config
            .store(Arc::new(candidate_config.clone()));
        exception_handler.clear(Exception::InvalidConfiguration);

        callbacks
    }
}

impl ModuleConfig {
    fn get_channel_size(&self, mem_size: u64) -> usize {
        if self.tap_mode == TapMode::Analyzer {
            return 1 << 14;
        }

        min(max((mem_size / MB / 128 * 32000) as usize, 32000), 1 << 14)
    }

    fn get_flow_capacity(&self, mem_size: u64) -> usize {
        if self.tap_mode == TapMode::Analyzer {
            return self.yaml_config.flow.capacity as usize;
        }

        min((mem_size / MB / 128 * 65536) as usize, 1 << 30)
    }
}

impl YamlConfig {
    pub fn get_fast_path_map_size(&self, mem_size: u64) -> usize {
        if self.fast_path_map_size > 0 {
            return self.fast_path_map_size;
        }

        min(max((mem_size / MB / 128 * 32000) as usize, 32000), 1 << 20)
    }

    fn get_af_packet_blocks(&self, tap_mode: TapMode, mem_size: u64) -> usize {
        if tap_mode == TapMode::Analyzer || self.af_packet_blocks_enabled {
            self.af_packet_blocks.max(8)
        } else {
            (mem_size as usize / recv_engine::DEFAULT_BLOCK_SIZE / 16).min(128)
        }
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
        let rule1 = MatchRule {
            prefix: "/a".to_string(),
            keep_segments: 1,
        };
        let rule2 = MatchRule {
            prefix: "/a/b/c/d".to_string(),
            keep_segments: 3,
        };
        let rule3 = MatchRule {
            prefix: "/d/e/f".to_string(),
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
        let rule1 = MatchRule {
            prefix: "/a/b/c".to_string(),
            keep_segments: 1,
        };
        let rule2 = MatchRule {
            prefix: "/a/b/c/d".to_string(),
            keep_segments: 3,
        };
        let rule3 = MatchRule {
            prefix: "/d/e/f".to_string(),
            keep_segments: 3,
        };
        let rule4 = MatchRule {
            prefix: "".to_string(),
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
