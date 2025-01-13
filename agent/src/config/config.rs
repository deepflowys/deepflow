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

use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::Path;
use std::time::Duration;

use log::{debug, error, info, warn};
use md5::{Digest, Md5};
use regex::Regex;
use serde::{
    de::{self, Unexpected},
    Deserialize, Deserializer,
};
use thiserror::Error;
use tokio::runtime::Runtime;

use crate::common::l7_protocol_log::L7ProtocolParser;
use crate::flow_generator::{DnsLog, OracleLog, TlsLog};
use crate::{
    common::{
        decapsulate::TunnelType,
        enums::TapType,
        l7_protocol_log::{get_all_protocol, L7ProtocolParserInterface},
        DEFAULT_LOG_FILE, L7_PROTOCOL_INFERENCE_MAX_FAIL_COUNT, L7_PROTOCOL_INFERENCE_TTL,
    },
    flow_generator::protocol_logs::SLOT_WIDTH,
    metric::document::TapSide,
    rpc::Session,
    trident::RunningMode,
};
use public::{
    bitmap::Bitmap,
    consts::NPB_DEFAULT_PORT,
    proto::{
        common,
        trident::{self, KubernetesClusterIdRequest, TapMode},
    },
    utils::bitmap::parse_u16_range_list_to_bitmap,
};

pub const K8S_CA_CRT_PATH: &str = "/run/secrets/kubernetes.io/serviceaccount/ca.crt";
const MINUTE: Duration = Duration::from_secs(60);
const DEFAULT_STANDALONE_CONFIG: &str = "/etc/deepflow-agent-standalone.yaml";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("controller-ips is empty")]
    ControllerIpsEmpty,
    #[error("controller-ips invalid")]
    ControllerIpsInvalid,
    #[error("runtime config invalid: {0}")]
    RuntimeConfigInvalid(String),
    #[error("yaml config invalid: {0}")]
    YamlConfigInvalid(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum AgentIdType {
    #[default]
    IpMac,
    Ip,
}

impl<'de> Deserialize<'de> for AgentIdType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match String::deserialize(deserializer)?.as_str() {
            "ip-and-mac" | "ip_and_mac" => Ok(Self::IpMac),
            "ip" => Ok(Self::Ip),
            other => Err(de::Error::invalid_value(
                Unexpected::Str(other),
                &"ip|ip-and-mac|ip_and_mac",
            )),
        }
    }
}

impl From<AgentIdType> for trident::AgentIdentifier {
    fn from(t: AgentIdType) -> Self {
        match t {
            AgentIdType::IpMac => trident::AgentIdentifier::IpAndMac,
            AgentIdType::Ip => trident::AgentIdentifier::Ip,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, rename_all = "kebab-case")]
pub struct Config {
    pub controller_ips: Vec<String>,
    pub controller_port: u16,
    pub controller_tls_port: u16,
    pub controller_cert_file_prefix: String,
    pub log_file: String,
    pub kubernetes_cluster_id: String,
    pub kubernetes_cluster_name: Option<String>,
    pub vtap_group_id_request: String,
    pub controller_domain_name: Vec<String>,
    #[serde(skip)]
    pub agent_mode: RunningMode,
    pub override_os_hostname: Option<String>,
    pub async_worker_thread_number: u16,
    pub agent_unique_identifier: AgentIdType,
    #[cfg(target_os = "linux")]
    pub pid_file: String,
    pub team_id: String,
    pub cgroups_disabled: bool,
}

impl Config {
    pub fn load_from_file<T: AsRef<Path>>(path: T) -> Result<Self, ConfigError> {
        let contents =
            fs::read_to_string(path).map_err(|e| ConfigError::YamlConfigInvalid(e.to_string()))?;
        Self::load(&contents)
    }

    pub fn load<C: AsRef<str>>(contents: C) -> Result<Self, ConfigError> {
        let contents = contents.as_ref();
        if contents.len() == 0 {
            // parsing empty string leads to EOF error
            Ok(Self::default())
        } else {
            let mut cfg: Self = serde_yaml::from_str(contents)
                .map_err(|e| ConfigError::YamlConfigInvalid(e.to_string()))?;

            for i in 0..cfg.controller_ips.len() {
                if cfg.controller_ips[i].parse::<IpAddr>().is_err() {
                    let ip = resolve_domain(&cfg.controller_ips[i]);
                    if ip.is_none() {
                        return Err(ConfigError::ControllerIpsInvalid);
                    }

                    cfg.controller_domain_name
                        .push(cfg.controller_ips[i].clone());
                    cfg.controller_ips[i] = ip.unwrap();
                }
            }

            // convert relative path to absolute
            if Path::new(&cfg.log_file).is_relative() {
                let Ok(mut pb) = env::current_dir() else {
                    return Err(ConfigError::YamlConfigInvalid("get cwd failed".to_owned()));
                };
                pb.push(&cfg.log_file);
                match pb.to_str() {
                    Some(s) => cfg.log_file = s.to_owned(),
                    None => {
                        return Err(ConfigError::YamlConfigInvalid(format!(
                            "invalid log path {}",
                            cfg.log_file
                        )))
                    }
                }
            }

            Ok(cfg)
        }
    }

    pub async fn async_get_k8s_cluster_id(session: &Session, config: &Config) -> Option<String> {
        let ca_md5 = match fs::read_to_string(K8S_CA_CRT_PATH) {
            Ok(c) => Some(
                Md5::digest(c.as_bytes())
                    .into_iter()
                    .fold(String::new(), |s, c| s + &format!("{:02x}", c)),
            ),
            Err(e) => {
                info!(
                    "get kubernetes_cluster_id error: failed to read {} error: {}, this shows that agent is not running in K8s.",
                    K8S_CA_CRT_PATH, e
                );
                return None;
            }
        };

        loop {
            let request = KubernetesClusterIdRequest {
                ca_md5: ca_md5.clone(),
                kubernetes_cluster_name: config.kubernetes_cluster_name.clone(),
                team_id: Some(config.team_id.clone()),
            };

            match session
                .grpc_get_kubernetes_cluster_id_with_statsd(request)
                .await
            {
                Ok(response) => {
                    let cluster_id_response = response.into_inner();
                    if !cluster_id_response.error_msg().is_empty() {
                        error!(
                            "get_kubernetes_cluster_id grpc call from server error: {}",
                            cluster_id_response.error_msg()
                        );
                        tokio::time::sleep(MINUTE).await;
                        continue;
                    }
                    match cluster_id_response.cluster_id {
                        Some(id) => {
                            if id.is_empty() {
                                error!("call get_kubernetes_cluster_id return cluster_id is empty string");
                                tokio::time::sleep(MINUTE).await;
                                continue;
                            }
                            info!("set kubernetes_cluster_id to {}", id);
                            // FIXME: The channel in the session will become invalid after success here, so reset the session.
                            // ==============================================================================================
                            // FIXME: 这里获取成功后 Session 中的 Channel 会失效，所以在这里重置 Session
                            session.reset();
                            return Some(id);
                        }
                        None => {
                            error!("call get_kubernetes_cluster_id return response is none")
                        }
                    }
                }
                Err(e) => error!("get_kubernetes_cluster_id grpc call error: {}", e),
            }
            tokio::time::sleep(MINUTE).await;
        }
    }

    // 目的是为了k8s采集器configmap中不配置k8s-cluster-id也能实现注册。
    // 如果agent在容器中运行且ConfigMap中kubernetes-cluster-id为空,
    // 调用GetKubernetesClusterID RPC，获取cluster-id, 如果RPC调用失败，sleep 1分钟后再次调用，直到成功
    // ======================================================================================================
    // The purpose is to enable registration without configuring k8s-cluster-id in the k8s collector configmap.
    // If agent is running in container and the kubernetes-cluster-id in the
    // ConfigMap is empty, Call GetKubernetesClusterID RPC to get the cluster-id, if the RPC call fails, call it again
    // after 1 minute of sleep until it succeeds
    pub fn get_k8s_cluster_id(
        runtime: &Runtime,
        session: &Session,
        config: &Config,
    ) -> Option<String> {
        runtime.block_on(Self::async_get_k8s_cluster_id(session, config))
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            controller_ips: vec![],
            controller_port: 30035,
            controller_tls_port: 30135,
            controller_cert_file_prefix: "".into(),
            log_file: DEFAULT_LOG_FILE.into(),
            kubernetes_cluster_id: "".into(),
            kubernetes_cluster_name: Default::default(),
            vtap_group_id_request: "".into(),
            controller_domain_name: vec![],
            agent_mode: Default::default(),
            override_os_hostname: None,
            async_worker_thread_number: 16,
            agent_unique_identifier: Default::default(),
            #[cfg(target_os = "linux")]
            pid_file: Default::default(),
            team_id: "".into(),
            cgroups_disabled: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct UprobeProcRegExp {
    pub golang_symbol: String,
    pub golang: String,
    pub openssl: String,
}

impl Default for UprobeProcRegExp {
    fn default() -> Self {
        Self {
            golang_symbol: String::new(),
            golang: String::new(),
            openssl: String::new(),
        }
    }
}

pub const OS_PROC_REGEXP_MATCH_TYPE_CMD: &'static str = "cmdline";
pub const OS_PROC_REGEXP_MATCH_TYPE_PROC_NAME: &'static str = "process_name";
pub const OS_PROC_REGEXP_MATCH_TYPE_PARENT_PROC_NAME: &'static str = "parent_process_name";
pub const OS_PROC_REGEXP_MATCH_TYPE_TAG: &'static str = "tag";

pub const OS_PROC_REGEXP_MATCH_ACTION_ACCEPT: &'static str = "accept";
pub const OS_PROC_REGEXP_MATCH_ACTION_DROP: &'static str = "drop";
// use for proc scan match and replace
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(default, rename_all = "kebab-case")]
pub struct OsProcRegexp {
    pub match_regex: String,
    pub match_type: String, // one of cmdline or process_name
    pub rewrite_name: String,
    pub action: String, // one of accept or drop
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(default, rename_all = "kebab-case")]
pub struct EbpfKprobePortlist {
    pub port_list: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct OnCpuProfile {
    pub disabled: bool,
    pub frequency: u16,
    pub cpu: u16,
    pub regex: String,
}

impl Default for OnCpuProfile {
    fn default() -> Self {
        OnCpuProfile {
            disabled: false,
            frequency: 99,
            cpu: 0,
            regex: "^deepflow-.*".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct OffCpuProfile {
    pub disabled: bool,
    pub regex: String,
    pub cpu: u16,
    #[serde(rename = "minblock", with = "humantime_serde")]
    pub min_block: Duration,
}

impl Default for OffCpuProfile {
    fn default() -> Self {
        OffCpuProfile {
            disabled: false,
            regex: "^deepflow-.*".to_string(),
            cpu: 0,
            min_block: Duration::from_micros(50),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct EbpfYamlConfig {
    pub disabled: bool,
    pub log_file: String,
    pub global_ebpf_pps_threshold: u64,
    pub kprobe_whitelist: EbpfKprobePortlist,
    pub kprobe_blacklist: EbpfKprobePortlist,
    #[serde(rename = "uprobe-process-name-regexs")]
    pub uprobe_proc_regexp: UprobeProcRegExp,
    pub thread_num: usize,
    pub perf_pages_count: usize,
    pub ring_size: usize,
    pub max_socket_entries: usize,
    pub max_trace_entries: usize,
    pub socket_map_max_reclaim: usize,
    pub go_tracing_timeout: usize,
    pub io_event_collect_mode: usize,
    #[serde(with = "humantime_serde")]
    pub io_event_minimal_duration: Duration,
    #[serde(with = "humantime_serde")]
    pub java_symbol_file_refresh_defer_interval: Duration,
    pub on_cpu_profile: OnCpuProfile,
    pub off_cpu_profile: OffCpuProfile,
    pub syscall_out_of_order_cache_size: usize,
    pub syscall_out_of_order_reassembly: Vec<String>,
    pub syscall_segmentation_reassembly: Vec<String>,
}

impl Default for EbpfYamlConfig {
    fn default() -> Self {
        EbpfYamlConfig {
            disabled: false,
            log_file: String::new(),
            global_ebpf_pps_threshold: 0,
            thread_num: 1,
            perf_pages_count: 128,
            ring_size: 65536,
            max_socket_entries: 131072,
            max_trace_entries: 131072,
            socket_map_max_reclaim: 120000,
            kprobe_whitelist: EbpfKprobePortlist::default(),
            kprobe_blacklist: EbpfKprobePortlist::default(),
            uprobe_proc_regexp: UprobeProcRegExp::default(),
            go_tracing_timeout: 120,
            io_event_collect_mode: 1,
            io_event_minimal_duration: Duration::from_millis(1),
            java_symbol_file_refresh_defer_interval: Duration::from_secs(60),
            on_cpu_profile: OnCpuProfile::default(),
            off_cpu_profile: OffCpuProfile::default(),
            syscall_out_of_order_reassembly: vec![],
            syscall_segmentation_reassembly: vec![],
            syscall_out_of_order_cache_size: 16,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct PrometheusExtraConfig {
    pub enabled: bool,
    pub labels: Vec<String>,
    pub labels_limit: u32,
    pub values_limit: u32,
}

impl Default for PrometheusExtraConfig {
    fn default() -> Self {
        PrometheusExtraConfig {
            enabled: false,
            labels: vec![],
            labels_limit: 1024,
            values_limit: 4096,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct KubernetesResourceConfig {
    pub name: String,
    pub group: String,
    pub version: String,
    pub disabled: bool,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct MatchRule {
    pub prefix: String,
    pub keep_segments: usize,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct HttpEndpointExtraction {
    pub disabled: bool,
    pub match_rules: Vec<MatchRule>,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct ExtraLogFieldsInfo {
    pub field_name: String,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct ExtraLogFields {
    pub http: Vec<ExtraLogFieldsInfo>,
    pub http2: Vec<ExtraLogFieldsInfo>,
}

impl ExtraLogFields {
    pub fn deduplicate(&mut self) {
        fn deduplicate_fields(fields: &mut Vec<ExtraLogFieldsInfo>) {
            fields
                .iter_mut()
                .for_each(|f| f.field_name.make_ascii_lowercase());
            fields.sort_by(|a, b| a.field_name.cmp(&b.field_name));
            fields.dedup_by(|a, b| a.field_name == b.field_name);
        }

        deduplicate_fields(&mut self.http);
        deduplicate_fields(&mut self.http2);
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct L7ProtocolAdvancedFeatures {
    pub http_endpoint_extraction: HttpEndpointExtraction,
    pub obfuscate_enabled_protocols: Vec<String>,
    pub extra_log_fields: ExtraLogFields,
    pub unconcerned_dns_nxdomain_response_suffixes: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(default, rename_all = "kebab-case")]
pub struct L7LogBlacklist {
    pub field_name: String,
    pub operator: String,
    pub value: String,
}

#[derive(Clone, Copy, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct OracleParseConfig {
    pub is_be: bool,
    pub int_compress: bool,
    pub resp_0x04_extra_byte: bool,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct BondGroup {
    pub tap_interfaces: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct YamlConfig {
    #[serde(with = "LevelDef")]
    pub log_level: log::Level,
    pub profiler: bool,
    #[serde(alias = "afpacket-blocks-enabled")]
    pub af_packet_blocks_enabled: bool,
    #[serde(alias = "afpacket-blocks")]
    pub af_packet_blocks: usize,
    pub enable_debug_stats: bool,
    pub analyzer_dedup_disabled: bool,
    pub default_tap_type: u32,
    pub debug_listen_port: u16,
    pub enable_qos_bypass: bool,
    pub fast_path_map_size: usize,
    pub first_path_level: u32,
    pub local_dispatcher_count: usize,
    pub packet_fanout_mode: u32,
    pub src_interfaces: Vec<String>,
    pub tap_interface_bond_groups: Vec<BondGroup>,
    pub mirror_traffic_pcp: u16,
    pub vtap_group_id_request: String,
    pub pcap: PcapConfig,
    pub flow: FlowGeneratorConfig,
    pub flow_queue_size: usize,
    pub quadruple_queue_size: usize,
    pub analyzer_queue_size: usize,
    pub analyzer_raw_packet_block_size: usize,
    pub batched_buffer_size_limit: usize,
    pub dpdk_enabled: bool,
    pub dispatcher_queue: bool,
    pub libpcap_enabled: bool,
    pub vhost_socket_path: String,
    pub xflow_collector: XflowGeneratorConfig,
    pub vxlan_flags: u8,
    pub ignore_overlay_vlan: bool,
    pub collector_sender_queue_size: usize,
    pub collector_sender_queue_count: usize,
    pub toa_sender_queue_size: usize,
    pub toa_lru_cache_size: usize,
    pub flow_sender_queue_size: usize,
    pub flow_sender_queue_count: usize,
    #[serde(rename = "second-flow-extra-delay-second", with = "humantime_serde")]
    pub second_flow_extra_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub packet_delay: Duration,
    pub triple: TripleMapConfig,
    pub kubernetes_poller_type: KubernetesPollerType,
    pub trim_tunnel_types: Vec<String>,
    pub analyzer_ip: String,
    pub grpc_buffer_size: usize,
    #[serde(with = "humantime_serde")]
    pub l7_log_session_aggr_timeout: Duration,
    pub l7_log_session_slot_capacity: usize,
    pub tap_mac_script: String,
    pub cloud_gateway_traffic: bool,
    pub kubernetes_namespace: String,
    pub kubernetes_api_list_limit: u32,
    #[serde(with = "humantime_serde")]
    pub kubernetes_api_list_interval: Duration,
    pub kubernetes_resources: Vec<KubernetesResourceConfig>,
    pub external_metrics_sender_queue_size: usize,
    pub ebpf_collector_queue_size: usize,
    pub l7_protocol_inference_max_fail_count: usize,
    pub l7_protocol_inference_ttl: usize,
    pub packet_sequence_block_size: usize, // Enterprise Edition Feature: packet-sequence
    pub packet_sequence_queue_size: usize, // Enterprise Edition Feature: packet-sequence
    pub packet_sequence_queue_count: usize, // Enterprise Edition Feature: packet-sequence
    pub packet_sequence_flag: u8,          // Enterprise Edition Feature: packet-sequence
    pub feature_flags: Vec<String>,
    pub l7_protocol_enabled: Vec<String>,
    pub ebpf: EbpfYamlConfig,
    pub external_agent_http_proxy_compressed: bool,
    pub standalone_data_file_size: u32,
    pub standalone_data_file_dir: String,
    pub log_file: String,
    #[serde(rename = "l7-protocol-ports")]
    // hashmap<protocolName, portRange>
    pub l7_protocol_ports: HashMap<String, String>,
    pub l7_log_blacklist: HashMap<String, Vec<L7LogBlacklist>>,
    pub npb_port: u16,
    // process and socket scan config
    pub os_proc_root: String,
    pub os_proc_socket_sync_interval: u32, // for sec
    pub os_proc_socket_min_lifetime: u32,  // for sec
    pub os_proc_regex: Vec<OsProcRegexp>,
    pub os_app_tag_exec_user: String,
    pub os_app_tag_exec: Vec<String>,
    // whether to sync os socket and proc info.
    // only make sense when process_info_enabled() == true
    pub os_proc_sync_enabled: bool,
    // sync os socket and proc info only when the process has been tagged.
    pub os_proc_sync_tagged_only: bool,
    #[serde(with = "humantime_serde")]
    pub guard_interval: Duration,
    pub max_sockets: usize,
    #[serde(with = "humantime_serde")]
    pub max_sockets_tolerate_interval: Duration,
    pub check_core_file_disabled: bool,
    pub memory_trim_disabled: bool,
    pub page_cache_reclaim_percentage: u8,
    pub forward_capacity: usize,
    pub fast_path_disabled: bool,
    // rrt timeout must gt aggr SLOT_WIDTH
    #[serde(with = "humantime_serde")]
    pub rrt_tcp_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub rrt_udp_timeout: Duration,
    pub prometheus_extra_config: PrometheusExtraConfig,
    pub process_scheduling_priority: i8,
    pub cpu_affinity: String,
    pub external_profile_integration_disabled: bool,
    pub external_trace_integration_disabled: bool,
    pub external_metric_integration_disabled: bool,
    pub external_log_integration_disabled: bool,
    #[serde(with = "humantime_serde")]
    pub ntp_max_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub ntp_min_interval: Duration,
    pub l7_protocol_advanced_features: L7ProtocolAdvancedFeatures,
    pub oracle_parse_config: OracleParseConfig,
    pub server_ports: Vec<u16>,
    pub consistent_timestamp_in_l7_metrics: bool,
}

impl YamlConfig {
    const DEFAULT_DNS_PORTS: &'static str = "53,5353";
    const DEFAULT_TLS_PORTS: &'static str = "443,6443";
    const DEFAULT_ORACLE_PORTS: &'static str = "1521";
    const PACKET_FANOUT_MODE_MAX: u32 = 7;

    pub fn load_from_file<T: AsRef<Path>>(path: T, tap_mode: TapMode) -> Result<Self, io::Error> {
        let contents = fs::read_to_string(path)?;
        Self::load(&contents, tap_mode)
    }

    pub fn load<C: AsRef<str>>(contents: C, tap_mode: TapMode) -> Result<Self, io::Error> {
        let contents = contents.as_ref();
        let mut c = if contents.len() == 0 {
            // parsing empty string leads to EOF error
            Self::default()
        } else {
            serde_yaml::from_str(contents)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        };

        if c.pcap.queue_size == 0 {
            c.pcap.queue_size = 1 << 16;
        }
        if c.pcap.flow_buffer_size <= 0 {
            c.pcap.flow_buffer_size = 1 << 20;
        }
        if c.pcap.buffer_size <= 0 {
            c.pcap.buffer_size = 1 << 23;
        }
        if c.pcap.flush_interval < MINUTE {
            c.pcap.flush_interval = MINUTE;
        }

        if c.flow.flush_interval < Duration::from_secs(1)
            || c.flow.flush_interval > Duration::from_secs(10)
        {
            c.flow.flush_interval = Duration::from_secs(1);
        }
        if c.flow_queue_size == 0 {
            c.flow_queue_size = 1 << 16;
        }
        if c.quadruple_queue_size == 0 {
            c.quadruple_queue_size = 1 << 18;
        }
        if c.analyzer_queue_size == 0 {
            c.analyzer_queue_size = 1 << 17;
        }
        if c.analyzer_raw_packet_block_size < 65536 {
            c.analyzer_raw_packet_block_size = 65536;
        }
        if c.batched_buffer_size_limit < 1024 {
            c.batched_buffer_size_limit = 1024;
        }
        if c.collector_sender_queue_size == 0 {
            c.collector_sender_queue_size = if tap_mode == trident::TapMode::Analyzer {
                8 << 20
            } else {
                1 << 16
            };
        }
        if c.flow_sender_queue_size == 0 {
            c.flow_sender_queue_size = if tap_mode == trident::TapMode::Analyzer {
                8 << 20
            } else {
                1 << 16
            };
        }
        if c.packet_delay < Duration::from_secs(1) || c.packet_delay > Duration::from_secs(10) {
            c.packet_delay = Duration::from_secs(1);
        }
        if c.first_path_level < 1 || c.first_path_level > 16 {
            c.first_path_level = 8;
        }

        // L7Log Session timeout must more than or equal 10s to keep window
        if c.l7_log_session_aggr_timeout.as_secs() < 10 {
            c.l7_log_session_aggr_timeout = Duration::from_secs(10);
        }

        if c.l7_log_session_slot_capacity < 1024 {
            c.l7_log_session_slot_capacity = 1024;
        }

        if c.external_metrics_sender_queue_size == 0 {
            c.external_metrics_sender_queue_size = 1 << 12;
        }

        if c.ebpf_collector_queue_size < 4096 {
            c.ebpf_collector_queue_size = 1 << 16;
        }

        if c.l7_protocol_inference_max_fail_count == 0 {
            c.l7_protocol_inference_max_fail_count = L7_PROTOCOL_INFERENCE_MAX_FAIL_COUNT;
        }

        if c.l7_protocol_inference_ttl == 0 {
            c.l7_protocol_inference_ttl = L7_PROTOCOL_INFERENCE_TTL;
        }

        // Enterprise Edition Feature: packet-sequence
        if c.packet_sequence_block_size <= 0 || c.packet_sequence_block_size >= 1024 {
            c.packet_sequence_block_size = 256;
        }

        // Enterprise Edition Feature: packet-sequence
        if c.packet_sequence_queue_size == 0 {
            if tap_mode == trident::TapMode::Analyzer {
                c.packet_sequence_queue_size = 8 << 20;
            } else {
                c.packet_sequence_queue_size = 1 << 16;
            }
        }

        // Enterprise Edition Feature: packet-sequence
        if c.packet_sequence_queue_count == 0 {
            c.packet_sequence_queue_count = 1;
        }

        if c.vxlan_flags == 0x08 || c.vxlan_flags == 0 {
            c.vxlan_flags = 0xff;
        }
        c.vxlan_flags |= 0x08;

        if c.standalone_data_file_size == 0 {
            c.standalone_data_file_size = 200;
        }

        if c.standalone_data_file_dir.len() == 0 {
            c.standalone_data_file_dir = Path::new(DEFAULT_LOG_FILE)
                .parent()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
        }
        if c.npb_port == 0 {
            c.npb_port = NPB_DEFAULT_PORT;
        }
        if c.ebpf.thread_num == 0 {
            c.ebpf.thread_num = 1;
        }
        if c.ebpf.perf_pages_count < 32 || c.ebpf.perf_pages_count > 8192 {
            c.ebpf.perf_pages_count = 128
        }
        if c.ebpf.ring_size < 8192 || c.ebpf.ring_size > 131072 {
            c.ebpf.ring_size = 65536;
        }
        if c.ebpf.max_socket_entries < 100000 || c.ebpf.max_socket_entries > 2000000 {
            c.ebpf.max_socket_entries = 131072;
        }
        if c.ebpf.socket_map_max_reclaim < 100000 || c.ebpf.socket_map_max_reclaim > 2000000 {
            c.ebpf.socket_map_max_reclaim = 120000;
        }
        if c.ebpf.max_trace_entries < 100000 || c.ebpf.max_trace_entries > 2000000 {
            c.ebpf.max_trace_entries = 131072;
        }
        if c.ebpf.java_symbol_file_refresh_defer_interval < Duration::from_secs(5)
            || c.ebpf.java_symbol_file_refresh_defer_interval > Duration::from_secs(3600)
        {
            c.ebpf.java_symbol_file_refresh_defer_interval = Duration::from_secs(60)
        }
        c.ebpf.off_cpu_profile.min_block = c
            .ebpf
            .off_cpu_profile
            .min_block
            .clamp(Duration::from_micros(0), Duration::from_micros(3600000000));
        if !(8..=1024).contains(&c.ebpf.syscall_out_of_order_cache_size) {
            c.ebpf.syscall_out_of_order_cache_size = 16;
        }

        if c.guard_interval < Duration::from_secs(1) || c.guard_interval > Duration::from_secs(3600)
        {
            c.guard_interval = Duration::from_secs(10);
        }

        c.max_sockets = c.max_sockets.clamp(16, 4096);
        c.max_sockets_tolerate_interval = c
            .max_sockets_tolerate_interval
            .min(Duration::from_secs(3600));

        if c.kubernetes_api_list_limit < 10 {
            c.kubernetes_api_list_limit = 10;
        }

        if c.kubernetes_api_list_interval < Duration::from_secs(600) {
            c.kubernetes_api_list_interval = Duration::from_secs(600);
        }

        if c.forward_capacity < 1 << 14 {
            c.forward_capacity = 1 << 14;
        }

        if let Err(e) = c.validate() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, e.to_string()));
        }
        c.rrt_tcp_timeout = c
            .rrt_tcp_timeout
            .max(Duration::from_secs(SLOT_WIDTH))
            .min(Duration::from_secs(3600));

        c.rrt_udp_timeout = c
            .rrt_udp_timeout
            .max(Duration::from_secs(SLOT_WIDTH))
            .min(Duration::from_secs(300));

        if c.prometheus_extra_config.labels_limit < 1024
            || c.prometheus_extra_config.labels_limit > 1024 * 1024
        {
            c.prometheus_extra_config.labels_limit = 1024;
        }

        if c.prometheus_extra_config.values_limit < 4096
            || c.prometheus_extra_config.values_limit > 4096 * 1024
        {
            c.prometheus_extra_config.values_limit = 4096;
        }

        let mut valid_labels = vec![];
        let re = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        for label in c.prometheus_extra_config.labels {
            if re.is_match(&label) {
                valid_labels.push(label);
            } else {
                debug!("invalid prometheus_extra_config label: {:?}", label);
            }
        }
        c.prometheus_extra_config.labels = valid_labels;

        if c.process_scheduling_priority < -20 || c.process_scheduling_priority > 19 {
            c.process_scheduling_priority = 0;
        }

        if c.local_dispatcher_count == 0 {
            c.local_dispatcher_count = 1;
        }

        if c.packet_fanout_mode > Self::PACKET_FANOUT_MODE_MAX {
            c.packet_fanout_mode = 0;
        }

        Ok(c)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }

    pub fn get_protocol_port(&self) -> HashMap<String, String> {
        let mut new = self.l7_protocol_ports.clone();

        let dns_str = L7ProtocolParser::DNS(DnsLog::default()).as_str();
        // dns default only parse 53,5353 port. when l7_protocol_ports config without DNS, need to reserve the dns default config.
        if !self.l7_protocol_ports.contains_key(dns_str) {
            new.insert(dns_str.to_string(), Self::DEFAULT_DNS_PORTS.to_string());
        }
        let tls_str = L7ProtocolParser::TLS(TlsLog::default()).as_str();
        // tls default only parse 443,6443 port. when l7_protocol_ports config without TLS, need to reserve the tls default config.
        if !self.l7_protocol_ports.contains_key(tls_str) {
            new.insert(tls_str.to_string(), Self::DEFAULT_TLS_PORTS.to_string());
        }
        let oracle_str = L7ProtocolParser::Oracle(OracleLog::default()).as_str();
        // oracle default only parse 1521 port. when l7_protocol_ports config without ORACLE, need to reserve the oracle default config.
        if !self.l7_protocol_ports.contains_key(oracle_str) {
            new.insert(
                oracle_str.to_string(),
                Self::DEFAULT_ORACLE_PORTS.to_string(),
            );
        }

        new
    }

    pub fn get_protocol_port_parse_bitmap(&self) -> Vec<(String, Bitmap)> {
        /*
            parse all protocol port range
            format example:

                l7-protocol-ports:
                    "HTTP": "80,8080,1000-2000"
                ...
        */
        let l7_protocol_ports = self.get_protocol_port();
        let mut port_bitmap = Vec::new();
        for (protocol_name, port_range) in l7_protocol_ports.iter() {
            port_bitmap.push((
                protocol_name.clone(),
                parse_u16_range_list_to_bitmap(port_range, false).unwrap(),
            ));
        }
        port_bitmap.sort_unstable_by_key(|p| p.0.clone());
        port_bitmap
    }
}

impl Default for YamlConfig {
    fn default() -> Self {
        Self {
            log_level: log::Level::Info,
            profiler: false,
            af_packet_blocks_enabled: false,
            af_packet_blocks: 128,
            enable_debug_stats: false,
            analyzer_dedup_disabled: false,
            default_tap_type: 3,
            debug_listen_port: 0,
            enable_qos_bypass: false,
            fast_path_map_size: 0,
            first_path_level: 0,
            src_interfaces: vec![],
            tap_interface_bond_groups: vec![],
            mirror_traffic_pcp: 0,
            vtap_group_id_request: "".into(),
            pcap: Default::default(),
            flow: Default::default(),
            flow_queue_size: 65536,
            quadruple_queue_size: 262144,
            analyzer_queue_size: 131072,
            analyzer_raw_packet_block_size: 65536,
            batched_buffer_size_limit: 131072,
            dpdk_enabled: false,
            dispatcher_queue: false,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            libpcap_enabled: false,
            #[cfg(target_os = "windows")]
            libpcap_enabled: true,
            vhost_socket_path: "".into(),
            xflow_collector: Default::default(),
            vxlan_flags: 0xff,
            ignore_overlay_vlan: false,
            // default size changes according to tap_mode
            collector_sender_queue_size: 1 << 16,
            collector_sender_queue_count: 1,
            toa_sender_queue_size: 1 << 16,
            toa_lru_cache_size: 1 << 16,
            // default size changes according to tap_mode
            flow_sender_queue_size: 1 << 16,
            flow_sender_queue_count: 1,
            second_flow_extra_delay: Duration::from_secs(0),
            packet_delay: Duration::from_secs(1),
            triple: Default::default(),
            kubernetes_poller_type: KubernetesPollerType::Adaptive,
            trim_tunnel_types: vec![],
            analyzer_ip: "".into(),
            grpc_buffer_size: 5,
            l7_log_session_aggr_timeout: Duration::from_secs(120),
            l7_log_session_slot_capacity: 1024,
            tap_mac_script: "".into(),
            cloud_gateway_traffic: false,
            kubernetes_namespace: "".into(),
            kubernetes_api_list_limit: 1000,
            kubernetes_api_list_interval: Duration::from_secs(600),
            kubernetes_resources: vec![],
            external_metrics_sender_queue_size: 1 << 12,
            l7_protocol_inference_max_fail_count: L7_PROTOCOL_INFERENCE_MAX_FAIL_COUNT,
            l7_protocol_inference_ttl: L7_PROTOCOL_INFERENCE_TTL,
            packet_sequence_block_size: 256, // Enterprise Edition Feature: packet-sequence
            packet_sequence_queue_size: 1 << 16, // Enterprise Edition Feature: packet-sequence
            packet_sequence_queue_count: 1,  // Enterprise Edition Feature: packet-sequence
            packet_sequence_flag: 0,         // Enterprise Edition Feature: packet-sequence
            feature_flags: vec![],
            l7_protocol_enabled: {
                let mut protos = vec![];
                for i in get_all_protocol() {
                    if i.parse_default() {
                        protos.push(i.as_str().to_owned());
                    }
                }
                protos
            },
            external_agent_http_proxy_compressed: false,
            standalone_data_file_size: 200,
            standalone_data_file_dir: Path::new(DEFAULT_LOG_FILE)
                .parent()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),

            log_file: DEFAULT_LOG_FILE.into(),
            l7_protocol_ports: HashMap::from([
                (String::from("DNS"), String::from(Self::DEFAULT_DNS_PORTS)),
                (String::from("TLS"), String::from(Self::DEFAULT_TLS_PORTS)),
            ]),
            l7_log_blacklist: HashMap::new(),
            ebpf: EbpfYamlConfig::default(),
            npb_port: NPB_DEFAULT_PORT,
            os_proc_root: "/proc".into(),
            os_proc_socket_sync_interval: 10,
            os_proc_socket_min_lifetime: 3,
            os_proc_regex: vec![OsProcRegexp {
                match_regex: ".*".into(),
                match_type: OS_PROC_REGEXP_MATCH_TYPE_PROC_NAME.into(),
                rewrite_name: "".into(),
                action: OS_PROC_REGEXP_MATCH_ACTION_ACCEPT.into(),
            }],
            os_app_tag_exec_user: "deepflow".to_string(),
            os_app_tag_exec: vec![],
            os_proc_sync_enabled: false,
            os_proc_sync_tagged_only: false,
            guard_interval: Duration::from_secs(10),
            max_sockets: 1024,
            max_sockets_tolerate_interval: Duration::from_secs(60),
            check_core_file_disabled: false,
            memory_trim_disabled: false,
            page_cache_reclaim_percentage: 100,
            fast_path_disabled: false,
            forward_capacity: 1 << 14,
            rrt_tcp_timeout: Duration::from_secs(1800),
            rrt_udp_timeout: Duration::from_secs(150),
            prometheus_extra_config: PrometheusExtraConfig::default(),
            process_scheduling_priority: 0,
            cpu_affinity: "".to_string(),
            external_profile_integration_disabled: false,
            external_trace_integration_disabled: false,
            external_metric_integration_disabled: false,
            external_log_integration_disabled: false,
            ntp_max_interval: Duration::from_secs(300),
            ntp_min_interval: Duration::from_secs(10),
            l7_protocol_advanced_features: L7ProtocolAdvancedFeatures::default(),
            local_dispatcher_count: 1,
            packet_fanout_mode: 0,
            oracle_parse_config: OracleParseConfig {
                is_be: true,
                int_compress: true,
                resp_0x04_extra_byte: false,
            },
            ebpf_collector_queue_size: 65535,
            server_ports: vec![],
            consistent_timestamp_in_l7_metrics: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(remote = "log::Level", rename_all = "kebab-case")]
enum LevelDef {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(remote = "trident::TapMode")]
enum TapModeDef {
    #[serde(rename = "0")]
    Local,
    #[serde(rename = "1")]
    Mirror,
    #[serde(rename = "2")]
    Analyzer,
    #[serde(rename = "3")]
    Decap,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct PortConfig {
    pub analyzer_port: u16,
    pub proxy_controller_port: u16,
}

impl Default for PortConfig {
    fn default() -> Self {
        let config = trident::Config {
            ..Default::default()
        };
        PortConfig {
            analyzer_port: config.analyzer_port.unwrap() as u16,
            proxy_controller_port: config.proxy_controller_port.unwrap() as u16,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct PcapConfig {
    pub queue_size: u32,
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    pub buffer_size: u64,
    pub flow_buffer_size: u32,
}

impl Default for PcapConfig {
    fn default() -> Self {
        PcapConfig {
            queue_size: 65536,
            flush_interval: Duration::from_secs(60),
            buffer_size: 96 << 10,      // 96K
            flow_buffer_size: 64 << 10, // 64K
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct FlowGeneratorConfig {
    // tcp timeout config
    #[serde(with = "humantime_serde")]
    pub established_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub closing_rst_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub others_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub opening_rst_timeout: Duration,

    #[serde(rename = "flow-slots-size")]
    pub hash_slots: u32,
    #[serde(rename = "flow-count-limit")]
    pub capacity: u32,
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    #[serde(rename = "flow-aggr-queue-size")]
    pub aggr_queue_size: u32,
    pub memory_pool_size: usize,

    pub ignore_tor_mac: bool,
    pub ignore_l2_end: bool,
    pub ignore_idc_vlan: bool,
}

impl Default for FlowGeneratorConfig {
    fn default() -> Self {
        FlowGeneratorConfig {
            established_timeout: Duration::from_secs(300),
            closing_rst_timeout: Duration::from_secs(35),
            others_timeout: Duration::from_secs(5),
            opening_rst_timeout: Duration::from_secs(1),

            hash_slots: 131072,
            capacity: 65535,
            flush_interval: Duration::from_secs(1),
            aggr_queue_size: 65535,
            memory_pool_size: 65536,

            ignore_tor_mac: false,
            ignore_l2_end: false,
            ignore_idc_vlan: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct XflowGeneratorConfig {
    pub sflow_ports: Vec<String>,
    pub netflow_ports: Vec<String>,
}

impl Default for XflowGeneratorConfig {
    fn default() -> Self {
        XflowGeneratorConfig {
            sflow_ports: vec!["6343".into()],
            netflow_ports: vec!["2055".into()],
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct TripleMapConfig {
    #[serde(rename = "flow-slots-size")]
    pub hash_slots: u32,
    pub capacity: u32,
}

impl Default for TripleMapConfig {
    fn default() -> Self {
        TripleMapConfig {
            hash_slots: 65536,
            capacity: 1048576,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum KubernetesPollerType {
    Adaptive,
    Active,
    Passive,
}

#[derive(Debug, Deserialize)]
#[serde(default = "RuntimeConfig::standalone_default")]
pub struct RuntimeConfig {
    pub vtap_group_id: String,
    #[serde(skip)]
    pub enabled: bool,
    pub max_millicpus: u32,
    pub max_memory: u64,
    pub sync_interval: u64,          // unit(second)
    pub platform_sync_interval: u64, // unit(second)
    pub stats_interval: u64,         // unit(second)
    #[serde(rename = "max_collect_pps")]
    pub global_pps_threshold: u64,
    #[cfg(target_os = "linux")]
    #[serde(skip)]
    pub extra_netns_regex: String,
    pub tap_interface_regex: String,
    #[serde(skip)]
    pub host: String,
    #[serde(deserialize_with = "bool_from_int")]
    pub rsyslog_enabled: bool,
    #[serde(skip)]
    pub output_vlan: u16,
    pub mtu: u32,
    #[serde(rename = "max_npb_bps")]
    pub npb_bps_threshold: u64,
    #[serde(deserialize_with = "bool_from_int")]
    pub collector_enabled: bool,
    pub l4_log_store_tap_types: Vec<u8>,
    #[serde(skip)]
    pub app_proto_log_enabled: bool,
    pub l7_log_store_tap_types: Vec<u8>,
    #[serde(deserialize_with = "tap_side_vec_de")]
    pub l4_log_ignore_tap_sides: Vec<TapSide>,
    #[serde(deserialize_with = "tap_side_vec_de")]
    pub l7_log_ignore_tap_sides: Vec<TapSide>,
    #[serde(deserialize_with = "bool_from_int")]
    pub platform_enabled: bool,
    #[serde(skip)]
    pub system_load_circuit_breaker_threshold: f32,
    #[serde(skip)]
    pub system_load_circuit_breaker_recover: f32,
    #[serde(skip)]
    pub system_load_circuit_breaker_metric: trident::SystemLoadMetric,
    #[serde(skip)]
    pub server_tx_bandwidth_threshold: u64,
    #[serde(skip)]
    pub bandwidth_probe_interval: Duration,
    #[serde(deserialize_with = "to_vlan_mode")]
    pub npb_vlan_mode: trident::VlanMode,
    #[serde(skip)]
    pub npb_dedup_enabled: bool,
    #[serde(deserialize_with = "to_if_mac_source")]
    pub if_mac_source: trident::IfMacSource,
    #[serde(deserialize_with = "bool_from_int")]
    pub vtap_flow_1s_enabled: bool,
    #[serde(skip)]
    pub debug_enabled: bool,
    pub log_threshold: u32,
    #[serde(deserialize_with = "to_log_level")]
    pub log_level: log::Level,
    pub analyzer_ip: String,
    pub analyzer_port: u16,
    #[serde(rename = "max_escape_seconds")]
    pub max_escape: u64,
    #[serde(skip)]
    pub proxy_controller_ip: String,
    pub proxy_controller_port: u16,
    #[serde(skip)]
    pub epc_id: u32,
    #[serde(skip)]
    pub vtap_id: u16,
    #[serde(skip)]
    pub team_id: u32,
    #[serde(skip)]
    pub organize_id: u32,
    #[serde(deserialize_with = "to_socket_type")]
    pub collector_socket_type: trident::SocketType,
    #[serde(deserialize_with = "to_socket_type")]
    pub npb_socket_type: trident::SocketType,
    #[serde(skip)]
    pub trident_type: common::TridentType,
    pub capture_packet_size: u32,
    #[serde(deserialize_with = "bool_from_int")]
    pub inactive_server_port_enabled: bool,
    #[serde(deserialize_with = "bool_from_int")]
    pub inactive_ip_enabled: bool,
    #[serde(rename = "vm_xml_path")]
    pub libvirt_xml_path: String,
    pub l7_log_packet_size: u32,
    pub l4_log_collect_nps_threshold: u64,
    pub l7_log_collect_nps_threshold: u64,
    #[serde(deserialize_with = "bool_from_int")]
    pub l7_metrics_enabled: bool,
    #[serde(deserialize_with = "to_tunnel_types")]
    pub decap_types: Vec<TunnelType>,
    pub http_log_proxy_client: String,
    pub http_log_trace_id: String,
    pub http_log_span_id: String,
    pub http_log_x_request_id: String,
    #[serde(skip)]
    pub region_id: u32,
    #[serde(skip)]
    pub pod_cluster_id: u32,
    pub log_retention: u32,
    #[serde(deserialize_with = "to_capture_socket_type")]
    pub capture_socket_type: trident::CaptureSocketType,
    pub process_threshold: u32,
    pub thread_threshold: u32,
    pub capture_bpf: String,
    #[serde(deserialize_with = "bool_from_int")]
    pub l4_performance_enabled: bool,
    #[serde(skip)]
    pub kubernetes_api_enabled: bool,
    #[serde(deserialize_with = "bool_from_int")]
    pub ntp_enabled: bool,
    pub sys_free_memory_limit: u32,
    pub log_file_size: u32,
    #[serde(deserialize_with = "bool_from_int")]
    pub external_agent_http_proxy_enabled: bool,
    pub external_agent_http_proxy_port: u16,
    #[serde(deserialize_with = "to_tap_mode")]
    pub tap_mode: TapMode,
    #[serde(skip)]
    pub plugins: Option<trident::PluginConfig>,
    // TODO: expand and remove
    #[serde(rename = "static_config")]
    pub yaml_config: YamlConfig,
}

impl RuntimeConfig {
    pub fn load_from_file<T: AsRef<Path>>(path: T) -> Result<Self, io::Error> {
        let contents = fs::read_to_string(path)?;
        let mut c = if contents.len() == 0 {
            // parsing empty string leads to EOF error
            Self::standalone_default()
        } else {
            serde_yaml::from_str(contents.as_str())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        };

        // reset below switch in standalone mode
        c.app_proto_log_enabled = !c.l7_log_store_tap_types.is_empty();
        c.ntp_enabled = false;
        // c.collector_socket_type = trident::SocketType::File;
        c.max_memory <<= 20;
        c.server_tx_bandwidth_threshold <<= 20;
        c.modify_decap_types();
        c.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(c)
    }

    fn standalone_default() -> Self {
        Self {
            vtap_group_id: Default::default(),
            enabled: true,
            max_millicpus: 1000,
            max_memory: 768,
            sync_interval: 60,
            platform_sync_interval: 10,
            stats_interval: 10,
            global_pps_threshold: 2000000,
            #[cfg(target_os = "linux")]
            extra_netns_regex: Default::default(),
            tap_interface_regex: "^(tap.*|cali.*|veth.*|eth.*|en[ospx].*|lxc.*|lo|[0-9a-f]+_h)$"
                .into(),
            host: Default::default(),
            rsyslog_enabled: false,
            output_vlan: 0,
            mtu: 1500,
            npb_bps_threshold: 1000,
            collector_enabled: true,
            l4_log_store_tap_types: vec![0],
            platform_enabled: false,
            system_load_circuit_breaker_threshold: 1.0,
            system_load_circuit_breaker_recover: 0.9,
            system_load_circuit_breaker_metric: trident::SystemLoadMetric::Load15,
            server_tx_bandwidth_threshold: 1,
            bandwidth_probe_interval: Duration::from_secs(60),
            npb_vlan_mode: trident::VlanMode::None,
            npb_dedup_enabled: false,
            if_mac_source: trident::IfMacSource::IfMac,
            vtap_flow_1s_enabled: true,
            debug_enabled: true,
            log_threshold: 300,
            log_level: log::Level::Info,
            analyzer_ip: "127.0.0.1".into(),
            analyzer_port: 30033,
            max_escape: 3600,
            proxy_controller_ip: "127.0.0.1".into(),
            proxy_controller_port: 30035,
            epc_id: 3302,
            vtap_id: 3302,
            team_id: 0,
            organize_id: 0,
            collector_socket_type: trident::SocketType::File,
            npb_socket_type: trident::SocketType::RawUdp,
            trident_type: common::TridentType::TtProcess,
            capture_packet_size: 65535,
            inactive_server_port_enabled: true,
            inactive_ip_enabled: true,
            libvirt_xml_path: "/etc/libvirt/qemu/".into(),
            l7_log_packet_size: 1024,
            l4_log_collect_nps_threshold: 10000,
            l7_log_collect_nps_threshold: 10000,
            l7_metrics_enabled: true,
            app_proto_log_enabled: true,
            l7_log_store_tap_types: vec![0],
            l4_log_ignore_tap_sides: vec![],
            l7_log_ignore_tap_sides: vec![],
            decap_types: Default::default(),
            http_log_proxy_client: "X-Forwarded-For".into(),
            http_log_trace_id: "traceparent, sw8".into(),
            http_log_span_id: "traceparent, sw8".into(),
            http_log_x_request_id: "X-Request-ID".into(),
            region_id: 3302,
            pod_cluster_id: 0,
            log_retention: 300,
            capture_socket_type: trident::CaptureSocketType::Auto,
            process_threshold: 10,
            thread_threshold: 500,
            capture_bpf: Default::default(),
            l4_performance_enabled: true,
            kubernetes_api_enabled: false,
            ntp_enabled: false,
            sys_free_memory_limit: 0,
            log_file_size: 1000,
            external_agent_http_proxy_enabled: false,
            external_agent_http_proxy_port: 38086,
            tap_mode: TapMode::Local,
            yaml_config: YamlConfig::load("", TapMode::Local).unwrap(), // Default configuration that needs to be corrected to be available
            plugins: Default::default(),
        }
    }

    fn modify_decap_types(&mut self) {
        for tunnel_type in &self.yaml_config.trim_tunnel_types {
            self.decap_types
                .push(TunnelType::from(tunnel_type.as_str()));
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.sync_interval < 1 || self.sync_interval > 60 * 60 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "sync-interval {:?} not in [1s, 1h]",
                Duration::from_secs(self.sync_interval)
            )));
        }
        if self.platform_sync_interval < 10 || self.platform_sync_interval > 60 * 60 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "platform-sync-interval {:?} not in [10s, 1h]",
                Duration::from_secs(self.platform_sync_interval)
            )));
        }
        if self.stats_interval < 1 || self.stats_interval > 60 * 60 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "stats-interval {:?} not in [1s, 1h]",
                Duration::from_secs(self.stats_interval)
            )));
        }

        // 虽然RFC 791里最低MTU是68，但是此时compressor会崩溃，
        // 所以MTU最低限定到200以确保deepflow-agent能够成功运行
        if self.mtu < 200 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "MTU({}) specified smaller than 200",
                self.mtu
            )));
        }

        if self.output_vlan > 4095 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "output-vlan({}) out of range (0-4095)",
                self.output_vlan
            )));
        }

        if self.analyzer_port == 0 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "analyzer-port({}) invalid",
                self.analyzer_port
            )));
        }
        #[cfg(target_os = "linux")]
        if regex::Regex::new(&self.extra_netns_regex).is_err() {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "malformed extra-netns-regex({})",
                self.extra_netns_regex
            )));
        }

        if regex::Regex::new(&self.tap_interface_regex).is_err() {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "malformed tap-interface-regex({})",
                self.tap_interface_regex
            )));
        }

        if self.max_escape < 600 || self.max_escape > 30 * 24 * 60 * 60 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "max-escape-seconds {:?} not in [600s, 30d]",
                self.max_escape
            )));
        }

        if self.proxy_controller_port == 0 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "proxy-controller-port({}) invalid",
                self.proxy_controller_port
            )));
        }

        if self.capture_packet_size > 65535 || self.capture_packet_size < 128 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "capture packet size {} not in [128, 65535]",
                self.capture_packet_size
            )));
        }

        if self.collector_socket_type == trident::SocketType::RawUdp {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "invalid collector_socket_type {:?}",
                self.collector_socket_type
            )));
        }

        Ok(())
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        trident::Config::default().try_into().unwrap()
    }
}

impl TryFrom<trident::Config> for RuntimeConfig {
    type Error = io::Error;

    fn try_from(conf: trident::Config) -> Result<Self, io::Error> {
        let mut rc = Self {
            vtap_group_id: Default::default(),
            enabled: conf.enabled(),
            max_millicpus: {
                // Compatible with max_cpus and max_millicpus, take the smaller value in milli-cores.
                let max_cpus = conf.max_cpus() * 1000;
                let max_millicpus = conf.max_millicpus.unwrap_or(max_cpus); // conf.max_millicpus may be None, handle the case where conf.max_millicpus is None
                if max_cpus != 0 && max_millicpus != 0 {
                    max_cpus.min(max_millicpus)
                } else {
                    max_cpus | max_millicpus
                }
            },
            max_memory: (conf.max_memory() as u64) << 20,
            sync_interval: conf.sync_interval() as u64,
            platform_sync_interval: conf.platform_sync_interval() as u64,
            stats_interval: conf.stats_interval() as u64,
            global_pps_threshold: conf.global_pps_threshold(),
            #[cfg(target_os = "linux")]
            extra_netns_regex: conf.extra_netns_regex().to_owned(),
            tap_interface_regex: conf.tap_interface_regex().to_owned(),
            host: conf.host().to_owned(),
            rsyslog_enabled: conf.rsyslog_enabled(),
            output_vlan: (conf.output_vlan() & 0xFFFFFFFF) as u16,
            mtu: conf.mtu(),
            npb_bps_threshold: conf.npb_bps_threshold(),
            collector_enabled: conf.collector_enabled(),
            l4_log_store_tap_types: conf
                .l4_log_tap_types
                .iter()
                .filter_map(|&i| {
                    if i >= u16::from(TapType::Max) as u32 {
                        warn!("invalid tap type: {}", i);
                        None
                    } else {
                        Some(i as u8)
                    }
                })
                .collect(),
            platform_enabled: conf.platform_enabled(),
            system_load_circuit_breaker_threshold: conf.system_load_circuit_breaker_threshold(),
            system_load_circuit_breaker_recover: conf.system_load_circuit_breaker_recover(),
            system_load_circuit_breaker_metric: conf.system_load_circuit_breaker_metric(),
            server_tx_bandwidth_threshold: conf.server_tx_bandwidth_threshold(),
            bandwidth_probe_interval: Duration::from_secs(conf.bandwidth_probe_interval()),
            npb_vlan_mode: conf.npb_vlan_mode(),
            npb_dedup_enabled: conf.npb_dedup_enabled(),
            if_mac_source: conf.if_mac_source(),
            vtap_flow_1s_enabled: conf.vtap_flow_1s_enabled(),
            debug_enabled: conf.debug_enabled(),
            log_threshold: conf.log_threshold(),
            log_level: match conf.log_level().to_lowercase().as_str() {
                "error" => log::Level::Error,
                "warn" | "warning" => log::Level::Warn,
                "info" => log::Level::Info,
                "debug" => log::Level::Debug,
                "trace" => log::Level::Trace,
                _ => log::Level::Info,
            },
            analyzer_ip: conf.analyzer_ip().to_owned(),
            analyzer_port: conf.analyzer_port() as u16,
            max_escape: conf.max_escape_seconds() as u64,
            proxy_controller_ip: conf.proxy_controller_ip().to_owned(),
            proxy_controller_port: conf.proxy_controller_port() as u16,
            epc_id: conf.epc_id(),
            vtap_id: (conf.vtap_id() & 0xFFFFFFFF) as u16,
            team_id: conf.team_id(),
            organize_id: conf.organize_id(),
            collector_socket_type: conf.collector_socket_type(),
            npb_socket_type: conf.npb_socket_type(),
            trident_type: conf.trident_type(),
            capture_packet_size: conf.capture_packet_size(),
            inactive_server_port_enabled: conf.inactive_server_port_enabled(),
            inactive_ip_enabled: conf.inactive_ip_enabled(),
            libvirt_xml_path: conf.libvirt_xml_path().to_owned(),
            l7_log_packet_size: conf.l7_log_packet_size(),
            l4_log_collect_nps_threshold: conf.l4_log_collect_nps_threshold(),
            l7_log_collect_nps_threshold: conf.l7_log_collect_nps_threshold(),
            l7_metrics_enabled: conf.l7_metrics_enabled(),
            app_proto_log_enabled: !conf.l7_log_store_tap_types.is_empty(),
            l7_log_store_tap_types: conf
                .l7_log_store_tap_types
                .iter()
                .filter_map(|&i| {
                    if i >= u16::from(TapType::Max) as u32 {
                        warn!("invalid tap type: {}", i);
                        None
                    } else {
                        Some(i as u8)
                    }
                })
                .collect(),
            l4_log_ignore_tap_sides: conf
                .l4_log_ignore_tap_sides
                .iter()
                .filter_map(|s| match TapSide::try_from(*s as u8) {
                    Ok(side) => Some(side),
                    Err(_) => {
                        warn!("invalid tap side: {}", s);
                        None
                    }
                })
                .collect(),
            l7_log_ignore_tap_sides: conf
                .l7_log_ignore_tap_sides
                .iter()
                .filter_map(|s| match TapSide::try_from(*s as u8) {
                    Ok(side) => Some(side),
                    Err(_) => {
                        warn!("invalid tap side: {}", s);
                        None
                    }
                })
                .collect(),
            decap_types: conf
                .decap_type
                .iter()
                .filter_map(|&t| match TunnelType::try_from(t as u8) {
                    Ok(t) => Some(t),
                    Err(_) => {
                        warn!("invalid tunnel type: {}", t);
                        None
                    }
                })
                .collect(),
            http_log_proxy_client: conf.http_log_proxy_client().to_owned(),
            http_log_trace_id: conf.http_log_trace_id().to_owned(),
            http_log_span_id: conf.http_log_span_id().to_owned(),
            http_log_x_request_id: conf.http_log_x_request_id().to_owned(),
            region_id: conf.region_id(),
            pod_cluster_id: conf.pod_cluster_id(),
            log_retention: conf.log_retention(),
            capture_socket_type: conf.capture_socket_type(),
            process_threshold: conf.process_threshold(),
            thread_threshold: conf.thread_threshold(),
            capture_bpf: conf.capture_bpf().to_owned(),
            l4_performance_enabled: conf.l4_performance_enabled(),
            kubernetes_api_enabled: conf.kubernetes_api_enabled(),
            ntp_enabled: conf.ntp_enabled(),
            sys_free_memory_limit: conf.sys_free_memory_limit(),
            log_file_size: conf.log_file_size(),
            external_agent_http_proxy_enabled: conf.external_agent_http_proxy_enabled(),
            external_agent_http_proxy_port: conf.external_agent_http_proxy_port() as u16,
            tap_mode: conf.tap_mode(),
            yaml_config: YamlConfig::load(conf.local_config(), conf.tap_mode())?,
            plugins: conf.plugins,
        };
        rc.modify_decap_types();
        rc.validate()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        Ok(rc)
    }
}

fn to_capture_socket_type<'de, D>(deserializer: D) -> Result<trident::CaptureSocketType, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(trident::CaptureSocketType::Auto),
        1 => Ok(trident::CaptureSocketType::AfPacketV1),
        2 => Ok(trident::CaptureSocketType::AfPacketV2),
        3 => Ok(trident::CaptureSocketType::AfPacketV3),
        o => Err(de::Error::invalid_value(
            Unexpected::Unsigned(o as u64),
            &"0|1|2|3",
        )),
    }
}

fn to_tunnel_types<'de, D>(deserializer: D) -> Result<Vec<TunnelType>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<u8>::deserialize(deserializer)?
        .into_iter()
        .map(|t| {
            TunnelType::try_from(t).map_err(|_| {
                de::Error::invalid_value(
                    Unexpected::Unsigned(t as u64),
                    &"None|Vxlan|Ipip|TencentGre|Geneve|ErspanOrTeb",
                )
            })
        })
        .collect()
}

fn to_socket_type<'de, D>(deserializer: D) -> Result<trident::SocketType, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.as_str() {
        "FILE" => Ok(trident::SocketType::File),
        "TCP" => Ok(trident::SocketType::Tcp),
        "UDP" => Ok(trident::SocketType::Udp),
        "RAW_UDP" => Ok(trident::SocketType::RawUdp),
        "" => Ok(trident::SocketType::File),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"FILE|TCP|UDP|RAW_UDP",
        )),
    }
}

fn to_log_level<'de, D>(deserializer: D) -> Result<log::Level, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.to_lowercase().as_str() {
        "error" => Ok(log::Level::Error),
        "warn" | "warning" => Ok(log::Level::Warn),
        "info" => Ok(log::Level::Info),
        "debug" => Ok(log::Level::Debug),
        "trace" => Ok(log::Level::Trace),
        "" => Ok(log::Level::Info),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"trace|debug|info|warn|error",
        )),
    }
}

fn to_if_mac_source<'de, D>(deserializer: D) -> Result<trident::IfMacSource, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(trident::IfMacSource::IfMac),
        1 => Ok(trident::IfMacSource::IfName),
        2 => Ok(trident::IfMacSource::IfLibvirtXml),
        other => Err(de::Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"0|1|2",
        )),
    }
}

fn to_vlan_mode<'de, D>(deserializer: D) -> Result<trident::VlanMode, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(trident::VlanMode::None),
        1 => Ok(trident::VlanMode::Qinq),
        2 => Ok(trident::VlanMode::Vlan),
        other => Err(de::Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"0|1|2",
        )),
    }
}

fn to_tap_mode<'de, D>(deserializer: D) -> Result<TapMode, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(TapMode::Local),
        1 => Ok(TapMode::Mirror),
        2 => Ok(TapMode::Analyzer),
        other => Err(de::Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"0|1|2",
        )),
    }
}

fn bool_from_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(de::Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"0|1",
        )),
    }
}

fn tap_side_vec_de<'de, D>(deserializer: D) -> Result<Vec<TapSide>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<u8>::deserialize(deserializer)?
        .into_iter()
        .map(|t| TapSide::try_from(t).map_err(de::Error::custom))
        .collect()
}

// resolve domain name (without port) to ip address
fn resolve_domain(addr: &str) -> Option<String> {
    match format!("{}:1", addr).to_socket_addrs() {
        Ok(mut addr) => match addr.next() {
            Some(addr) => Some(addr.ip().to_string()),
            None => None,
        },
        Err(e) => {
            eprintln!("{:?}", e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_yaml_file() {
        // TODO: improve test cases
        let c = Config::load_from_file("config/deepflow-agent.yaml")
            .expect("failed loading config file");
        assert_eq!(c.controller_ips.len(), 1);
        assert_eq!(&c.controller_ips[0], "127.0.0.1");
    }
}
