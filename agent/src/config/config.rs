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

use std::cmp::{max, min};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::Path;
use std::time::Duration;

#[cfg(any(target_os = "linux", target_os = "android"))]
use envmnt::{ExpandOptions, ExpansionType};
use log::{error, info};
use md5::{Digest, Md5};
#[cfg(any(target_os = "linux", target_os = "android"))]
use procfs::process::Process;
use regex::Regex;
use serde::{
    de::{self, Unexpected},
    Deserialize, Deserializer,
};
use thiserror::Error;
use tokio::runtime::Runtime;

use crate::common::l7_protocol_log::L7ProtocolParser;
use crate::dispatcher::recv_engine::DEFAULT_BLOCK_SIZE;
use crate::flow_generator::{DnsLog, MemcachedLog};
#[cfg(any(target_os = "linux", target_os = "android"))]
use crate::platform::{get_container_id, OsAppTag, ProcessData};
use crate::{
    common::DEFAULT_LOG_FILE, metric::document::TapSide, rpc::Session, trident::RunningMode,
};

use public::{bitmap::Bitmap, proto::agent, utils::bitmap::parse_u16_range_list_to_bitmap};

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

impl From<AgentIdType> for agent::AgentIdentifier {
    fn from(t: AgentIdType) -> Self {
        match t {
            AgentIdType::IpMac => agent::AgentIdentifier::IpAndMac,
            AgentIdType::Ip => agent::AgentIdentifier::Ip,
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
            let request = agent::KubernetesClusterIdRequest {
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
#[serde(default)]
pub struct TagExtraction {
    pub script_command: Vec<String>,
    pub exec_username: String,
}

impl Default for TagExtraction {
    fn default() -> Self {
        Self {
            script_command: vec![],
            exec_username: "deepflow".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub enum ProcessMatchType {
    Cmd,
    ProcessName,
    ParentProcessName,
    Tag,
    CmdWithArgs,
}

impl From<&str> for ProcessMatchType {
    fn from(value: &str) -> Self {
        match value {
            OS_PROC_REGEXP_MATCH_TYPE_CMD => Self::Cmd,
            OS_PROC_REGEXP_MATCH_TYPE_PARENT_PROC_NAME => Self::ParentProcessName,
            OS_PROC_REGEXP_MATCH_TYPE_TAG => Self::Tag,
            OS_PROC_REGEXP_MATCH_TYPE_CMD_WITH_ARGS => Self::CmdWithArgs,
            _ => Self::ProcessName,
        }
    }
}

fn to_process_match_type<'de, D>(deserializer: D) -> Result<ProcessMatchType, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.as_str().into())
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ProcessMatcher {
    #[serde(deserialize_with = "to_match_regex")]
    pub match_regex: Regex,
    #[serde(deserialize_with = "to_process_match_type")]
    pub match_type: ProcessMatchType,
    pub match_languages: Vec<String>,
    pub match_usernames: Vec<String>,
    pub only_in_container: bool,
    pub only_with_tag: bool,
    pub ignore: bool,
    pub rewrite_name: String,
    pub enabled_features: Vec<String>,
}

impl Eq for ProcessMatcher {}

impl PartialEq for ProcessMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.match_regex.as_str() == other.match_regex.as_str()
            && self.match_type == other.match_type
            && self.match_languages == other.match_languages
            && self.match_usernames == other.match_usernames
            && self.only_in_container == other.only_in_container
            && self.only_with_tag == other.only_with_tag
            && self.ignore == other.ignore
            && self.rewrite_name == other.rewrite_name
            && self.enabled_features == other.enabled_features
    }
}

fn to_match_regex<'de, D>(deserializer: D) -> Result<Regex, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    if let Ok(regex) = Regex::new(raw.as_str()) {
        Ok(regex)
    } else {
        Err(de::Error::invalid_value(
            Unexpected::Str(raw.as_str()),
            &"See: https://regexr.com/",
        ))
    }
}

impl Default for ProcessMatcher {
    fn default() -> Self {
        Self {
            match_regex: Regex::new("").unwrap(),
            match_type: "".into(),
            match_languages: vec![],
            match_usernames: vec![],
            only_in_container: true,
            only_with_tag: false,
            ignore: false,
            rewrite_name: "".to_string(),
            enabled_features: vec![],
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
impl ProcessMatcher {
    // TODO: match_languages
    pub fn get_process_data(
        &self,
        process: &Process,
        tag_map: &HashMap<u64, OsAppTag>,
    ) -> Option<ProcessData> {
        if self.only_in_container && get_container_id(process).is_none() {
            return None;
        }
        if self.only_with_tag && !tag_map.contains_key(&(process.pid as u64)) {
            return None;
        }

        let env_rewrite = |r: String| {
            envmnt::expand(
                r.as_str(),
                Some(ExpandOptions {
                    expansion_type: Some(ExpansionType::Windows),
                    default_to_empty: true,
                }),
            )
        };
        let Ok(mut process_data) = ProcessData::try_from(process) else {
            return None;
        };
        let mut match_replace_fn = |reg: &Regex, ignored: bool, s: &String, replace: &String| {
            if reg.is_match(s.as_str()) {
                if !ignored && !replace.is_empty() {
                    process_data.name = reg.replace_all(s.as_str(), replace).to_string();
                }
                true
            } else {
                false
            }
        };

        let replace = env_rewrite(self.rewrite_name.clone());

        match self.match_type {
            ProcessMatchType::Cmd => {
                if match_replace_fn(&self.match_regex, self.ignore, &process_data.cmd, &replace) {
                    Some(process_data)
                } else {
                    None
                }
            }
            ProcessMatchType::CmdWithArgs => {
                if match_replace_fn(
                    &self.match_regex,
                    self.ignore,
                    &process_data.cmd_with_args.join(" "),
                    &replace,
                ) {
                    Some(process_data)
                } else {
                    None
                }
            }
            ProcessMatchType::ProcessName => {
                if match_replace_fn(
                    &self.match_regex,
                    self.ignore,
                    &process_data.process_name,
                    &replace,
                ) {
                    Some(process_data)
                } else {
                    None
                }
            }
            ProcessMatchType::ParentProcessName => {
                fn match_parent(proc: &ProcessData, reg: &Regex) -> Option<ProcessData> {
                    const MAX_DEPTH: usize = 10;
                    let mut ppid = proc.ppid;
                    let mut pid = proc.pid;
                    for _ in 0..MAX_DEPTH {
                        if ppid == 0 {
                            return None;
                        }

                        let Ok(parent) = Process::new(ppid as i32) else {
                            return None;
                        };

                        let Ok(parent_data) = ProcessData::try_from(&parent) else {
                            error!("pid {} have no parent proc with ppid: {}", pid, ppid);
                            return None;
                        };

                        if reg.is_match(&parent_data.process_name.as_str()) {
                            return Some(parent_data);
                        }
                        ppid = parent_data.ppid;
                        pid = parent_data.pid;
                    }

                    None
                }

                match_parent(&process_data, &self.match_regex)
            }
            ProcessMatchType::Tag => {
                if let Some(tag) = tag_map.get(&process_data.pid) {
                    let mut found = None;
                    for tag_kv in tag.tags.iter() {
                        let composed = format!("{}:{}", &tag_kv.key, &tag_kv.value);
                        if self.match_regex.is_match(&composed.as_str()) {
                            found = Some(process_data);
                            break;
                        }
                    }
                    found
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct GolangSpecific {
    pub enabled: bool,
}

impl Default for GolangSpecific {
    fn default() -> Self {
        Self { enabled: false }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Java {
    #[serde(with = "humantime_serde")]
    pub refresh_defer_duration: Duration,
    #[serde(deserialize_with = "deser_usize_with_mega_unit")]
    pub max_symbol_file_size: usize,
}

impl Default for Java {
    fn default() -> Self {
        Self {
            refresh_defer_duration: Duration::from_secs(60),
            max_symbol_file_size: 10 << 20,
        }
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SymbolTable {
    pub golang_specific: GolangSpecific,
    pub java: Java,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Proc {
    pub enabled: bool,
    pub proc_dir_path: String,
    #[serde(with = "humantime_serde")]
    pub socket_info_sync_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub min_lifetime: Duration,
    pub tag_extraction: TagExtraction,
    pub process_matcher: Vec<ProcessMatcher>,
    pub symbol_table: SymbolTable,
}

impl Default for Proc {
    fn default() -> Self {
        Self {
            enabled: false,
            proc_dir_path: "/proc".to_string(),
            socket_info_sync_interval: Duration::from_secs(0),
            min_lifetime: Duration::from_secs(3),
            tag_extraction: TagExtraction::default(),
            process_matcher: vec![ProcessMatcher {
                match_regex: Regex::new("deepflow-.*").unwrap(),
                only_in_container: false,
                enabled_features: vec![
                    "ebpf.profile.on_cpu".to_string(),
                    "ebpf.profile.off_cpu".to_string(),
                    "proc.gprocess_info".to_string(),
                ],
                ..Default::default()
            }],
            symbol_table: SymbolTable::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(remote = "agent::PacketCaptureType")]
enum PacketCaptureTypeDef {
    #[serde(rename = "0")]
    Local,
    #[serde(rename = "1")]
    Mirror,
    #[serde(rename = "2")]
    Analyzer,
    #[serde(rename = "3")]
    Decap,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Common {
    #[serde(with = "PacketCaptureTypeDef")]
    pub capture_mode: agent::PacketCaptureType,
}

impl Default for Common {
    fn default() -> Self {
        Self {
            capture_mode: agent::PacketCaptureType::Local,
        }
    }
}

fn to_capture_socket_type<'de, D>(deserializer: D) -> Result<agent::CaptureSocketType, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(agent::CaptureSocketType::Auto),
        2 => Ok(agent::CaptureSocketType::AfPacketV2),
        3 => Ok(agent::CaptureSocketType::AfPacketV3),
        o => Err(de::Error::invalid_value(
            Unexpected::Unsigned(o as u64),
            &"0|2|3",
        )),
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AfPacketTunning {
    #[serde(deserialize_with = "to_capture_socket_type")]
    pub socket_version: agent::CaptureSocketType,
    pub ring_blocks_enabled: bool,
    pub ring_blocks: usize,
    pub packet_fanout_count: usize,
    pub packet_fanout_mode: u32,
    #[serde(rename = "interface_promisc_enabled")]
    pub promisc: bool,
}

impl Default for AfPacketTunning {
    fn default() -> Self {
        Self {
            socket_version: agent::CaptureSocketType::Auto,
            ring_blocks_enabled: false,
            ring_blocks: 128,
            packet_fanout_count: 1,
            packet_fanout_mode: 0,
            promisc: false,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BondInterface {
    pub slave_interfaces: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AfPacket {
    pub interface_regex: String,
    pub bond_interfaces: Vec<BondInterface>,
    pub extra_netns_regex: String,
    pub extra_bpf_filter: String,
    pub src_interfaces: Vec<String>,
    pub vlan_pcp_in_physical_mirror_traffic: u16,
    pub bpf_filter_disabled: bool,
    pub skip_npb_bpf: bool,
    pub tunning: AfPacketTunning,
}

impl Default for AfPacket {
    fn default() -> Self {
        Self {
            interface_regex: "^(tap.*|cali.*|veth.*|eth.*|en[osipx].*|lxc.*|lo|[0-9a-f]+_h)$"
                .to_string(),
            bond_interfaces: vec![],
            extra_netns_regex: "".to_string(),
            extra_bpf_filter: "".to_string(),
            vlan_pcp_in_physical_mirror_traffic: 0,
            bpf_filter_disabled: false,
            skip_npb_bpf: false,
            tunning: AfPacketTunning::default(),
            src_interfaces: vec![],
        }
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, PartialEq, Eq)]
pub enum DpdkSource {
    #[default]
    None,
    Ebpf,
    PDump,
}

fn to_dpdk_source<'de, D>(deserializer: D) -> Result<DpdkSource, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.to_uppercase().as_str() {
        "NONE" => Ok(DpdkSource::None),
        "EBPF" => Ok(DpdkSource::Ebpf),
        "PDUMP" => Ok(DpdkSource::PDump),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"None|eBPF|pDump",
        )),
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Dpdk {
    #[serde(deserialize_with = "to_dpdk_source")]
    pub source: DpdkSource,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Libpcap {
    pub enabled: bool,
}

impl Default for Libpcap {
    fn default() -> Self {
        Self { enabled: false }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct VhostUser {
    pub vhost_socket_path: String,
}

impl Default for VhostUser {
    fn default() -> Self {
        Self {
            vhost_socket_path: "".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PhysicalSwitch {
    pub sflow_ports: Vec<u16>,
    pub netflow_ports: Vec<u16>,
}

impl Default for PhysicalSwitch {
    fn default() -> Self {
        Self {
            sflow_ports: vec![],
            netflow_ports: vec![],
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SpecialNetwork {
    pub dpdk: Dpdk,
    pub libpcap: Libpcap,
    pub vhost_user: VhostUser,
    pub physical_switch: PhysicalSwitch,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct CbpfTunning {
    pub dispatcher_queue_enabled: bool,
    pub max_capture_packet_size: u32,
    pub raw_packet_buffer_block_size: usize,
    pub raw_packet_queue_size: usize,
    pub max_capture_pps: u64,
}

impl Default for CbpfTunning {
    fn default() -> Self {
        Self {
            dispatcher_queue_enabled: false,
            max_capture_packet_size: 65535,
            raw_packet_buffer_block_size: 65536,
            raw_packet_queue_size: 131072,
            max_capture_pps: 1048576,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PreProcess {
    pub tunnel_decap_protocols: Vec<u8>,
    pub tunnel_trim_protocols: Vec<String>,
    pub packet_segmentation_reassembly: Vec<u16>,
}

impl Default for PreProcess {
    fn default() -> Self {
        Self {
            tunnel_decap_protocols: vec![1, 2],
            tunnel_trim_protocols: vec![],
            packet_segmentation_reassembly: vec![],
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PhysicalMirror {
    pub default_capture_network_type: u16,
    pub packet_dedup_disabled: bool,
    pub private_cloud_gateway_traffic: bool,
}

impl Default for PhysicalMirror {
    fn default() -> Self {
        Self {
            default_capture_network_type: 3,
            packet_dedup_disabled: false,
            private_cloud_gateway_traffic: false,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Cbpf {
    pub common: Common,
    pub af_packet: AfPacket,
    pub special_network: SpecialNetwork,
    pub tunning: CbpfTunning,
    pub preprocess: PreProcess,
    pub physical_mirror: PhysicalMirror,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfSocketUprobeTls {
    pub enabled: bool,
}

impl Default for EbpfSocketUprobeTls {
    fn default() -> Self {
        Self { enabled: false }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfSocketUprobeGolang {
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub tracing_timeout: Duration,
}

impl Default for EbpfSocketUprobeGolang {
    fn default() -> Self {
        Self {
            enabled: false,
            tracing_timeout: Duration::from_secs(120),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfSocketUprobeDpdk {
    pub command: String,
    pub rx_hooks: Vec<String>,
    pub tx_hooks: Vec<String>,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfSocketUprobe {
    pub golang: EbpfSocketUprobeGolang,
    pub tls: EbpfSocketUprobeTls,
    pub dpdk: EbpfSocketUprobeDpdk,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfSocketKprobePorts {
    pub ports: String,
}

impl Default for EbpfSocketKprobePorts {
    fn default() -> Self {
        Self {
            ports: "".to_string(),
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfSocketKprobe {
    pub blacklist: EbpfSocketKprobePorts,
    pub whitelist: EbpfSocketKprobePorts,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfSocketTunning {
    pub max_capture_rate: u64,
    pub syscall_trace_id_disabled: bool,
    pub map_prealloc_disabled: bool,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfSocket {
    pub uprobe: EbpfSocketUprobe,
    pub kprobe: EbpfSocketKprobe,
    pub tunning: EbpfSocketTunning,
    pub preprocess: EbpfSocketPreprocess,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfFileIoEvent {
    pub collect_mode: usize,
    #[serde(with = "humantime_serde")]
    pub minimal_duration: Duration,
}

impl Default for EbpfFileIoEvent {
    fn default() -> Self {
        Self {
            collect_mode: 1,
            minimal_duration: Duration::from_millis(1),
        }
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfFile {
    pub io_event: EbpfFileIoEvent,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfProfileOnCpu {
    pub disabled: bool,
    pub sampling_frequency: i32,
    pub aggregate_by_cpu: bool,
}

impl Default for EbpfProfileOnCpu {
    fn default() -> Self {
        Self {
            disabled: false,
            sampling_frequency: 99,
            aggregate_by_cpu: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfProfileOffCpu {
    pub disabled: bool,
    #[serde(with = "humantime_serde")]
    pub min_blocking_time: Duration,
    pub aggregate_by_cpu: bool,
}

impl Default for EbpfProfileOffCpu {
    fn default() -> Self {
        Self {
            disabled: true,
            min_blocking_time: Duration::from_micros(50),
            aggregate_by_cpu: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfProfileMemory {
    pub disabled: bool,
    #[serde(with = "humantime_serde")]
    pub report_interval: Duration,
    pub allocated_addresses_lru_len: u32,
}

impl Default for EbpfProfileMemory {
    fn default() -> Self {
        Self {
            disabled: true,
            report_interval: Duration::from_secs(10),
            allocated_addresses_lru_len: 131072,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfProfilePreprocess {
    pub stack_compression: bool,
}

impl Default for EbpfProfilePreprocess {
    fn default() -> Self {
        Self {
            stack_compression: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Unwinding {
    pub dwarf_disabled: bool,
    pub dwarf_regex: String,
    pub dwarf_process_map_size: u32,
    pub dwarf_shard_map_size: u32,
}

impl Default for Unwinding {
    fn default() -> Self {
        Self {
            dwarf_disabled: true,
            dwarf_regex: Default::default(),
            dwarf_process_map_size: 1024,
            dwarf_shard_map_size: 128,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfProfile {
    pub on_cpu: EbpfProfileOnCpu,
    pub off_cpu: EbpfProfileOffCpu,
    pub memory: EbpfProfileMemory,
    pub unwinding: Unwinding,
    pub preprocess: EbpfProfilePreprocess,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfTunning {
    pub collector_queue_size: usize,
    pub userspace_worker_threads: i32,
    pub perf_pages_count: u32,
    pub kernel_ring_size: u32,
    pub max_socket_entries: u32,
    pub socket_map_reclaim_threshold: u32,
    pub max_trace_entries: u32,
}

impl Default for EbpfTunning {
    fn default() -> Self {
        Self {
            collector_queue_size: 65535,
            userspace_worker_threads: 1,
            perf_pages_count: 128,
            kernel_ring_size: 65536,
            max_socket_entries: 131072,
            socket_map_reclaim_threshold: 120000,
            max_trace_entries: 131072,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EbpfSocketPreprocess {
    pub out_of_order_reassembly_cache_size: usize,
    pub out_of_order_reassembly_protocols: Vec<String>,
    pub segmentation_reassembly_protocols: Vec<String>,
}

impl Default for EbpfSocketPreprocess {
    fn default() -> Self {
        Self {
            out_of_order_reassembly_cache_size: 16,
            out_of_order_reassembly_protocols: vec![],
            segmentation_reassembly_protocols: vec![],
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Ebpf {
    pub disabled: bool,
    pub socket: EbpfSocket,
    pub file: EbpfFile,
    pub profile: EbpfProfile,
    pub tunning: EbpfTunning,
    #[serde(skip)]
    pub java_symbol_file_refresh_defer_interval: i32,
}

impl Default for Ebpf {
    fn default() -> Self {
        Self {
            disabled: false,
            socket: EbpfSocket::default(),
            file: EbpfFile::default(),
            profile: EbpfProfile::default(),
            tunning: EbpfTunning::default(),
            java_symbol_file_refresh_defer_interval: 60,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PrivateCloud {
    pub hypervisor_resource_enabled: bool,
    #[serde(deserialize_with = "to_if_mac_source")]
    pub vm_mac_source: agent::IfMacSource,
    pub vm_xml_directory: String,
    pub vm_mac_mapping_script: String,
}

impl Default for PrivateCloud {
    fn default() -> Self {
        Self {
            hypervisor_resource_enabled: false,
            vm_mac_source: agent::IfMacSource::IfMac,
            vm_xml_directory: "/etc/libvirt/qemu/".to_string(),
            vm_mac_mapping_script: "".to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ApiResources {
    pub name: String,
    pub disabled: bool,
    pub group: String,
    pub version: String,
    pub field_selector: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum KubernetesPollerType {
    Adaptive,
    Active,
    Passive,
}

fn to_kubernetes_poller_type<'de, D>(deserializer: D) -> Result<KubernetesPollerType, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.to_uppercase().as_str() {
        "ADAPTIVE" => Ok(KubernetesPollerType::Adaptive),
        "ACTIVE" => Ok(KubernetesPollerType::Active),
        "PASSIVE" => Ok(KubernetesPollerType::Passive),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"Adaptive|Active|Passive",
        )),
    }
}

impl KubernetesPollerType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Adaptive => "adaptive",
            Self::Active => "active",
            Self::Passive => "passive",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Kubernetes {
    pub kubernetes_namespace: String,
    pub api_resources: Vec<ApiResources>,
    pub api_list_page_size: u32,
    #[serde(with = "humantime_serde")]
    pub api_list_max_interval: Duration,
    pub ingress_flavour: String,
    #[serde(deserialize_with = "to_kubernetes_poller_type")]
    pub pod_mac_collection_method: KubernetesPollerType,
}

impl Default for Kubernetes {
    fn default() -> Self {
        Self {
            kubernetes_namespace: "".to_string(),
            api_resources: vec![
                ApiResources {
                    name: "namespaces".to_string(),
                    ..Default::default()
                },
                ApiResources {
                    name: "nodes".to_string(),
                    ..Default::default()
                },
                ApiResources {
                    name: "pods".to_string(),
                    ..Default::default()
                },
                ApiResources {
                    name: "replicationcontrollers".to_string(),
                    ..Default::default()
                },
                ApiResources {
                    name: "services".to_string(),
                    ..Default::default()
                },
                ApiResources {
                    name: "daemonsets".to_string(),
                    ..Default::default()
                },
                ApiResources {
                    name: "deployments".to_string(),
                    ..Default::default()
                },
                ApiResources {
                    name: "replicasets".to_string(),
                    ..Default::default()
                },
                ApiResources {
                    name: "statefulsets".to_string(),
                    ..Default::default()
                },
                ApiResources {
                    name: "ingresses".to_string(),
                    ..Default::default()
                },
            ],
            api_list_page_size: 1000,
            api_list_max_interval: Duration::from_secs(600),
            ingress_flavour: "kubernetes".to_string(),
            pod_mac_collection_method: KubernetesPollerType::Adaptive,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Resources {
    #[serde(with = "humantime_serde")]
    pub push_interval: Duration,
    pub private_cloud: PrivateCloud,
    pub kubernetes: Kubernetes,
}

impl Default for Resources {
    fn default() -> Self {
        Self {
            push_interval: Duration::from_secs(10),
            private_cloud: PrivateCloud::default(),
            kubernetes: Kubernetes::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PrometheusExtraLabels {
    pub enabled: bool,
    pub extra_labels: Vec<String>,
    pub label_length: usize,
    pub value_length: usize,
}

impl Default for PrometheusExtraLabels {
    fn default() -> Self {
        Self {
            enabled: false,
            extra_labels: vec![],
            label_length: 1024,
            value_length: 4096,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FeatureControl {
    pub profile_integration_disabled: bool,
    pub trace_integration_disabled: bool,
    pub metric_integration_disabled: bool,
    pub log_integration_disabled: bool,
}

impl Default for FeatureControl {
    fn default() -> Self {
        Self {
            profile_integration_disabled: false,
            trace_integration_disabled: false,
            metric_integration_disabled: false,
            log_integration_disabled: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Compression {
    pub trace: bool,
    pub profile: bool,
}

impl Default for Compression {
    fn default() -> Self {
        Self {
            trace: true,
            profile: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Integration {
    pub enabled: bool,
    pub listen_port: u16,
    pub compression: Compression,
    pub prometheus_extra_labels: PrometheusExtraLabels,
    pub feature_control: FeatureControl,
}

impl Default for Integration {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_port: 38086,
            compression: Compression::default(),
            prometheus_extra_labels: PrometheusExtraLabels::default(),
            feature_control: FeatureControl::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Vector {
    pub enabled: bool,
    pub config: serde_yaml::Value,
}

impl Default for Vector {
    fn default() -> Self {
        Self {
            enabled: false,
            config: serde_yaml::Value::Null,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Inputs {
    pub proc: Proc,
    pub cbpf: Cbpf,
    pub ebpf: Ebpf,
    pub resources: Resources,
    pub integration: Integration,
    pub vector: Vector,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Policy {
    pub fast_path_map_size: usize,
    pub fast_path_disabled: bool,
    pub forward_table_capacity: usize,
    pub max_first_path_level: usize,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            fast_path_map_size: 0,
            fast_path_disabled: false,
            forward_table_capacity: 16384,
            max_first_path_level: 8,
        }
    }
}

fn parse_maybe_binary_u8<'de, D>(deserializer: D) -> Result<u8, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if let Ok(n) = s.parse::<u8>() {
        return Ok(n);
    }

    if s.starts_with("0b") {
        let mut n = 0;
        for c in s[2..].chars() {
            if matches!(c, '0' | '1') {
                n <<= 1;
                n += if c == '1' { 1 } else { 0 };
            }
        }
        return Ok(n);
    }

    return Err(de::Error::invalid_value(
        Unexpected::Str(&s),
        &"0b[0-1]{8}|[0-9]+",
    ));
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TcpHeader {
    pub block_size: usize,
    pub sender_queue_size: usize,
    #[serde(deserialize_with = "parse_maybe_binary_u8")]
    pub header_fields_flag: u8,
}

impl Default for TcpHeader {
    fn default() -> Self {
        Self {
            block_size: 256,
            sender_queue_size: 65536,
            header_fields_flag: 0b0000_0000,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PcapStream {
    pub receiver_queue_size: usize,
    pub buffer_size_per_flow: u32,
    pub total_buffer_size: u64,
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
}

impl Default for PcapStream {
    fn default() -> Self {
        Self {
            receiver_queue_size: 65536,
            buffer_size_per_flow: 65536,
            total_buffer_size: 88304,
            flush_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Toa {
    pub sender_queue_size: usize,
    pub cache_size: usize,
}

impl Default for Toa {
    fn default() -> Self {
        Self {
            sender_queue_size: 65536,
            cache_size: 65536,
        }
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Packet {
    pub policy: Policy,
    pub tcp_header: TcpHeader,
    pub pcap_stream: PcapStream,
    pub toa: Toa,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct OracleConfig {
    pub is_be: bool,
    pub int_compressed: bool,
    pub resp_0x04_extra_byte: bool,
}

impl Default for OracleConfig {
    fn default() -> Self {
        Self {
            is_be: true,
            int_compressed: true,
            resp_0x04_extra_byte: false,
        }
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ProtocolSpecialConfig {
    pub oracle: OracleConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ApplicationProtocolInference {
    pub inference_max_retries: usize,
    #[serde(with = "humantime_serde")]
    pub inference_result_ttl: Duration,
    pub enabled_protocols: Vec<String>,
    pub protocol_special_config: ProtocolSpecialConfig,
}

impl Default for ApplicationProtocolInference {
    fn default() -> Self {
        Self {
            inference_max_retries: 5,
            inference_result_ttl: Duration::from_secs(60),
            enabled_protocols: vec![
                "HTTP".to_string(),
                "HTTP2".to_string(),
                "MySQL".to_string(),
                "Redis".to_string(),
                "Kafka".to_string(),
                "DNS".to_string(),
                "TLS".to_string(),
                "Ping".to_string(),
            ],
            protocol_special_config: ProtocolSpecialConfig::default(),
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TagFilterOperator {
    pub name: String,
    pub operator: String,
    pub value: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Filters {
    pub port_number_prefilters: HashMap<String, String>,
    pub tag_filters: HashMap<String, Vec<TagFilterOperator>>,
    pub unconcerned_dns_nxdomain_response_suffixes: Vec<String>,
}

impl Default for Filters {
    fn default() -> Self {
        Self {
            port_number_prefilters: HashMap::from([
                ("HTTP".to_string(), "1-65535".to_string()),
                ("HTTP2".to_string(), "1-65535".to_string()),
                ("Dubbo".to_string(), "1-65535".to_string()),
                ("SofaRPC".to_string(), "1-65535".to_string()),
                ("FastCGI".to_string(), "1-65535".to_string()),
                ("bRPC".to_string(), "1-65535".to_string()),
                ("Tars".to_string(), "1-65535".to_string()),
                ("SomeIP".to_string(), "1-65535".to_string()),
                ("MySQL".to_string(), "1-65535".to_string()),
                ("PostgreSQL".to_string(), "1-65535".to_string()),
                ("Oracle".to_string(), "1521".to_string()),
                ("Redis".to_string(), "1-65535".to_string()),
                ("MongoDB".to_string(), "1-65535".to_string()),
                ("Memcached".to_string(), "11211".to_string()),
                ("Kafka".to_string(), "1-65535".to_string()),
                ("MQTT".to_string(), "1-65535".to_string()),
                ("AMQP".to_string(), "1-65535".to_string()),
                ("OpenWire".to_string(), "1-65535".to_string()),
                ("NATS".to_string(), "1-65535".to_string()),
                ("Pulsar".to_string(), "1-65535".to_string()),
                ("ZMTP".to_string(), "1-65535".to_string()),
                ("RocketMQ".to_string(), "1-65535".to_string()),
                ("DNS".to_string(), "53,5353".to_string()),
                ("TLS".to_string(), "443,6443".to_string()),
                ("PING".to_string(), "1-65535".to_string()),
                ("Custom".to_string(), "1-65535".to_string()),
            ]),
            tag_filters: HashMap::from([
                ("HTTP".to_string(), vec![]),
                ("HTTP2".to_string(), vec![]),
                ("Dubbo".to_string(), vec![]),
                ("gRPC".to_string(), vec![]),
                ("SOFARPC".to_string(), vec![]),
                ("FastCGI".to_string(), vec![]),
                ("bRPC".to_string(), vec![]),
                ("Tars".to_string(), vec![]),
                ("SomeIP".to_string(), vec![]),
                ("MySQL".to_string(), vec![]),
                ("PostgreSQL".to_string(), vec![]),
                ("Oracle".to_string(), vec![]),
                ("Redis".to_string(), vec![]),
                ("MongoDB".to_string(), vec![]),
                ("Memcached".to_string(), vec![]),
                ("Kafka".to_string(), vec![]),
                ("MQTT".to_string(), vec![]),
                ("AMQP".to_string(), vec![]),
                ("OpenWire".to_string(), vec![]),
                ("NATS".to_string(), vec![]),
                ("Pulsar".to_string(), vec![]),
                ("ZMTP".to_string(), vec![]),
                ("RocketMQ".to_string(), vec![]),
                ("DNS".to_string(), vec![]),
                ("TLS".to_string(), vec![]),
                ("PING".to_string(), vec![]),
            ]),
            unconcerned_dns_nxdomain_response_suffixes: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Timeouts {
    #[serde(with = "humantime_serde")]
    pub tcp_request_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub udp_request_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub session_aggregate_window_duration: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            tcp_request_timeout: Duration::from_secs(1800),
            udp_request_timeout: Duration::from_secs(150),
            session_aggregate_window_duration: Duration::from_secs(120),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TracingTag {
    pub http_real_client: Vec<String>,
    pub x_request_id: Vec<String>,
    pub apm_trace_id: Vec<String>,
    pub apm_span_id: Vec<String>,
}

impl Default for TracingTag {
    fn default() -> Self {
        Self {
            http_real_client: vec!["X_Forwarded_For".to_string()],
            x_request_id: vec!["X_Request_ID".to_string()],
            apm_trace_id: vec!["traceparent".to_string(), "sw8".to_string()],
            apm_span_id: vec!["traceparent".to_string(), "sw8".to_string()],
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct HttpEndpointMatchRule {
    pub url_prefix: String,
    pub keep_segments: usize,
}

impl Default for HttpEndpointMatchRule {
    fn default() -> Self {
        Self {
            url_prefix: "".to_string(),
            keep_segments: 2,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct HttpEndpoint {
    pub extraction_disabled: bool,
    pub match_rules: Vec<HttpEndpointMatchRule>,
}

impl Default for HttpEndpoint {
    fn default() -> Self {
        Self {
            extraction_disabled: false,
            match_rules: vec![HttpEndpointMatchRule::default()],
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct CustomFields {
    pub field_name: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RequestLogTagExtraction {
    pub tracing_tag: TracingTag,
    pub http_endpoint: HttpEndpoint,
    pub custom_fields: HashMap<String, Vec<CustomFields>>,
    pub obfuscate_protocols: Vec<String>,
}

impl Default for RequestLogTagExtraction {
    fn default() -> Self {
        Self {
            tracing_tag: TracingTag::default(),
            http_endpoint: HttpEndpoint::default(),
            custom_fields: HashMap::from([
                ("HTTP".to_string(), vec![]),
                ("HTTP2".to_string(), vec![]),
            ]),
            obfuscate_protocols: vec!["Redis".to_string()],
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RequestLogTunning {
    pub payload_truncation: u32,
    pub session_aggregate_slot_capacity: usize,
    pub consistent_timestamp_in_l7_metrics: bool,
}

impl Default for RequestLogTunning {
    fn default() -> Self {
        Self {
            payload_truncation: 1024,
            session_aggregate_slot_capacity: 1024,
            consistent_timestamp_in_l7_metrics: false,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RequestLog {
    pub application_protocol_inference: ApplicationProtocolInference,
    pub filters: Filters,
    pub timeouts: Timeouts,
    pub tag_extraction: RequestLogTagExtraction,
    pub tunning: RequestLogTunning,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TimeWindow {
    #[serde(with = "humantime_serde")]
    pub max_tolerable_packet_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub extra_tolerable_flow_delay: Duration,
}

impl Default for TimeWindow {
    fn default() -> Self {
        Self {
            max_tolerable_packet_delay: Duration::from_secs(1),
            extra_tolerable_flow_delay: Duration::ZERO,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FlowGeneration {
    pub server_ports: Vec<u16>,
    pub cloud_traffic_ignore_mac: bool,
    pub ignore_l2_end: bool,
    pub idc_traffic_ignore_vlan: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConntrackTimeouts {
    #[serde(with = "humantime_serde")]
    pub established: Duration,
    #[serde(with = "humantime_serde")]
    pub closing_rst: Duration,
    #[serde(with = "humantime_serde")]
    pub opening_rst: Duration,
    #[serde(with = "humantime_serde")]
    pub others: Duration,
}

impl Default for ConntrackTimeouts {
    fn default() -> Self {
        Self {
            established: Duration::from_secs(300),
            closing_rst: Duration::from_secs(35),
            opening_rst: Duration::from_secs(1),
            others: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Conntrack {
    #[serde(with = "humantime_serde")]
    pub flow_flush_interval: Duration,
    pub flow_generation: FlowGeneration,
    pub timeouts: ConntrackTimeouts,
}

impl Default for Conntrack {
    fn default() -> Self {
        Self {
            flow_flush_interval: Duration::from_secs(1),
            flow_generation: FlowGeneration::default(),
            timeouts: ConntrackTimeouts::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ProcessorsFlowLogTunning {
    pub flow_map_hash_slots: u32,
    pub concurrent_flow_limit: u32,
    pub memory_pool_size: usize,
    pub max_batched_buffer_size: usize,
    pub flow_aggregator_queue_size: usize,
    pub flow_generator_queue_size: usize,
    pub quadruple_generator_queue_size: usize,
}

impl Default for ProcessorsFlowLogTunning {
    fn default() -> Self {
        Self {
            flow_map_hash_slots: 131072,
            concurrent_flow_limit: 65535,
            memory_pool_size: 65536,
            max_batched_buffer_size: 131072,
            flow_aggregator_queue_size: 65535,
            flow_generator_queue_size: 65536,
            quadruple_generator_queue_size: 262144,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ProcessorsFlowLog {
    pub time_window: TimeWindow,
    pub conntrack: Conntrack,
    pub tunning: ProcessorsFlowLogTunning,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Processors {
    pub packet: Packet,
    pub request_log: RequestLog,
    pub flow_log: ProcessorsFlowLog,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Limits {
    pub max_millicpus: u32,
    #[serde(deserialize_with = "deser_u64_with_mega_unit")]
    pub max_memory: u64,
    pub max_log_backhaul_rate: u32,
    #[serde(deserialize_with = "deser_u32_with_mega_unit")]
    pub max_local_log_file_size: u32,
    #[serde(with = "humantime_serde")]
    pub local_log_retention: Duration,
    pub max_sockets: usize,
    #[serde(with = "humantime_serde")]
    pub max_sockets_tolerate_interval: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_millicpus: 1000,
            max_memory: 768 << 20,
            max_log_backhaul_rate: 300,
            max_local_log_file_size: 1000 << 20,
            local_log_retention: Duration::from_secs(300 * 24 * 3600),
            max_sockets: 1024,
            max_sockets_tolerate_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Alerts {
    pub thread_threshold: u32,
    pub process_threshold: u32,
    pub check_core_file_disabled: bool,
}

impl Default for Alerts {
    fn default() -> Self {
        Self {
            thread_threshold: 500,
            process_threshold: 10,
            check_core_file_disabled: false,
        }
    }
}

fn to_sys_memory_metric<'de, D>(deserializer: D) -> Result<agent::SysMemoryMetric, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.as_str() {
        "free" => Ok(agent::SysMemoryMetric::Free),
        "available" => Ok(agent::SysMemoryMetric::Available),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"[free|available]",
        )),
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SysMemoryPercentage {
    pub trigger_threshold: u32,
    #[serde(deserialize_with = "to_sys_memory_metric")]
    pub metric: agent::SysMemoryMetric,
}

fn to_system_load_metric<'de, D>(deserializer: D) -> Result<agent::SystemLoadMetric, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.as_str() {
        "load1" => Ok(agent::SystemLoadMetric::Load1),
        "load5" => Ok(agent::SystemLoadMetric::Load5),
        "load15" => Ok(agent::SystemLoadMetric::Load15),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"[load1|load5|load15]",
        )),
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialOrd)]
#[serde(default)]
pub struct RelativeSysLoad {
    pub trigger_threshold: f32,
    pub recovery_threshold: f32,
    #[serde(deserialize_with = "to_system_load_metric")]
    pub metric: agent::SystemLoadMetric,
}

impl PartialEq for RelativeSysLoad {
    fn eq(&self, other: &Self) -> bool {
        self.trigger_threshold == other.trigger_threshold
            || self.recovery_threshold == other.recovery_threshold
            || self.metric == other.metric
    }
}
impl Eq for RelativeSysLoad {}

impl Default for RelativeSysLoad {
    fn default() -> Self {
        RelativeSysLoad {
            trigger_threshold: 1.0,
            recovery_threshold: 0.9,
            metric: agent::SystemLoadMetric::Load15,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TxThroughput {
    #[serde(deserialize_with = "deser_u64_with_mega_unit")]
    pub trigger_threshold: u64,
    #[serde(with = "humantime_serde")]
    pub throughput_monitoring_interval: Duration,
}

impl Default for TxThroughput {
    fn default() -> Self {
        Self {
            trigger_threshold: 0,
            throughput_monitoring_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct CircuitBreakers {
    pub sys_memory_percentage: SysMemoryPercentage,
    pub relative_sys_load: RelativeSysLoad,
    pub tx_throughput: TxThroughput,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Tunning {
    pub cpu_affinity: Vec<usize>,
    pub process_scheduling_priority: usize,
    pub idle_memory_trimming: bool,
    pub page_cache_reclaim_percentage: u8,
    #[serde(with = "humantime_serde")]
    pub resource_monitoring_interval: Duration,
}

impl Default for Tunning {
    fn default() -> Self {
        Self {
            cpu_affinity: vec![],
            process_scheduling_priority: 0,
            idle_memory_trimming: false,
            page_cache_reclaim_percentage: 100,
            resource_monitoring_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Ntp {
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub max_drift: Duration,
    #[serde(with = "humantime_serde")]
    pub min_drift: Duration,
}

impl Default for Ntp {
    fn default() -> Self {
        Self {
            enabled: false,
            max_drift: Duration::from_secs(300),
            min_drift: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Communication {
    #[serde(with = "humantime_serde")]
    pub proactive_request_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub max_escape_duration: Duration,
    pub ingester_ip: String,
    pub ingester_port: u16,
    #[serde(deserialize_with = "deser_usize_with_mega_unit")]
    pub grpc_buffer_size: usize,
    pub request_via_nat_ip: bool,
    pub proxy_controller_ip: String,
    pub proxy_controller_port: u16,
}

impl Default for Communication {
    fn default() -> Self {
        Self {
            proactive_request_interval: Duration::from_secs(60),
            max_escape_duration: Duration::from_secs(3600),
            proxy_controller_ip: "127.0.0.1".to_string(),
            proxy_controller_port: 30035,
            ingester_ip: "".to_string(),
            ingester_port: 30033,
            grpc_buffer_size: 5 << 20,
            request_via_nat_ip: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Log {
    #[serde(deserialize_with = "to_log_level")]
    pub log_level: log::Level,
    pub log_file: String,
    pub log_backhaul_enabled: bool,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            log_level: log::Level::Info,
            log_file: "/var/log/deepflow-agent/deepflow-agent.log".to_string(),
            log_backhaul_enabled: true,
        }
    }
}

#[derive(Clone, Copy, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Profile {
    pub enabled: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Debug {
    pub enabled: bool,
    pub local_udp_port: u16,
    pub debug_metrics_enabled: bool,
}

impl Default for Debug {
    fn default() -> Self {
        Self {
            local_udp_port: 0,
            debug_metrics_enabled: false,
            enabled: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SelfMonitoring {
    pub log: Log,
    pub profile: Profile,
    pub debug: Debug,
    #[serde(skip)]
    pub hostname: String,
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
}

impl Default for SelfMonitoring {
    fn default() -> Self {
        Self {
            log: Log::default(),
            profile: Profile::default(),
            debug: Debug::default(),
            hostname: "".to_string(),
            interval: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StandaloneMode {
    #[serde(deserialize_with = "deser_u32_with_mega_unit")]
    pub max_data_file_size: u32,
    pub data_file_dir: String,
}

impl Default for StandaloneMode {
    fn default() -> Self {
        Self {
            max_data_file_size: 200 << 20,
            data_file_dir: "/var/log/deepflow-agent/".to_string(),
        }
    }
}

fn to_agent_type<'de, D>(deserializer: D) -> Result<agent::AgentType, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(agent::AgentType::TtUnknown),
        1 => Ok(agent::AgentType::TtProcess),
        2 => Ok(agent::AgentType::TtVm),
        3 => Ok(agent::AgentType::TtPublicCloud),
        5 => Ok(agent::AgentType::TtPhysicalMachine),
        6 => Ok(agent::AgentType::TtDedicatedPhysicalMachine),
        7 => Ok(agent::AgentType::TtHostPod),
        8 => Ok(agent::AgentType::TtVmPod),
        9 => Ok(agent::AgentType::TtTunnelDecapsulation),
        10 => Ok(agent::AgentType::TtHyperVCompute),
        11 => Ok(agent::AgentType::TtHyperVNetwork),
        12 => Ok(agent::AgentType::TtK8sSidecar),
        other => Err(de::Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"[0-12]",
        )),
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct GlobalCommon {
    pub kubernetes_api_enabled: bool,
    pub enabled: bool,
    pub region_id: u32,
    pub pod_cluster_id: u32,
    pub vpc_id: u32,
    pub agent_id: u32,
    pub team_id: u32,
    pub organize_id: u32,
    #[serde(deserialize_with = "to_agent_type")]
    pub agent_type: agent::AgentType,
    pub secret_key: String,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Global {
    pub limits: Limits,
    pub alerts: Alerts,
    pub circuit_breakers: CircuitBreakers,
    pub tunning: Tunning,
    pub ntp: Ntp,
    pub communication: Communication,
    pub self_monitoring: SelfMonitoring,
    pub standalone_mode: StandaloneMode,
    #[serde(skip)]
    pub common: GlobalCommon,
}

fn to_agent_socket_type<'de, D>(deserializer: D) -> Result<agent::SocketType, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.as_str() {
        "FILE" => Ok(agent::SocketType::File),
        "TCP" => Ok(agent::SocketType::Tcp),
        "UDP" => Ok(agent::SocketType::Udp),
        "RAW_UDP" => Ok(agent::SocketType::RawUdp),
        "ZMQ" => Ok(agent::SocketType::Zmq),
        "" => Ok(agent::SocketType::File),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"FILE|TCP|UDP|RAW_UDP|ZMQ",
        )),
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Socket {
    #[serde(deserialize_with = "to_agent_socket_type")]
    pub data_socket_type: agent::SocketType,
    #[serde(deserialize_with = "to_agent_socket_type")]
    pub npb_socket_type: agent::SocketType,
    pub raw_udp_qos_bypass: bool,
    pub multiple_sockets_to_ingester: bool,
}

impl Default for Socket {
    fn default() -> Self {
        Self {
            data_socket_type: agent::SocketType::Tcp,
            npb_socket_type: agent::SocketType::RawUdp,
            raw_udp_qos_bypass: false,
            multiple_sockets_to_ingester: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FlowLogFilters {
    pub l4_capture_network_types: Vec<i16>,
    pub l7_capture_network_types: Vec<i16>,
    pub l4_ignored_observation_points: Vec<u16>,
    pub l7_ignored_observation_points: Vec<u16>,
}

impl Default for FlowLogFilters {
    fn default() -> Self {
        Self {
            l4_capture_network_types: vec![0],
            l7_capture_network_types: vec![0],
            l4_ignored_observation_points: vec![],
            l7_ignored_observation_points: vec![],
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Throttles {
    pub l4_throttle: u64,
    pub l7_throttle: u64,
}

impl Default for Throttles {
    fn default() -> Self {
        Self {
            l4_throttle: 10000,
            l7_throttle: 10000,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct OutputsFlowLogTunning {
    pub collector_queue_size: usize,
}

impl Default for OutputsFlowLogTunning {
    fn default() -> Self {
        Self {
            collector_queue_size: 65536,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct OutputsFlowLog {
    pub filters: FlowLogFilters,
    pub throttles: Throttles,
    pub tunning: OutputsFlowLogTunning,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FlowMetricsFilters {
    pub inactive_server_port_aggregation: bool,
    pub inactive_ip_aggregation: bool,
    pub npm_metrics: bool,
    pub npm_metrics_concurrent: bool,
    pub apm_metrics: bool,
    pub second_metrics: bool,
}

impl Default for FlowMetricsFilters {
    fn default() -> Self {
        Self {
            inactive_server_port_aggregation: false,
            inactive_ip_aggregation: false,
            npm_metrics: true,
            npm_metrics_concurrent: true,
            apm_metrics: true,
            second_metrics: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FlowMetricsTunning {
    pub sender_queue_size: usize,
}

impl Default for FlowMetricsTunning {
    fn default() -> Self {
        Self {
            sender_queue_size: 65536,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FlowMetrics {
    pub enabled: bool,
    pub filters: FlowMetricsFilters,
    pub tunning: FlowMetricsTunning,
}

impl Default for FlowMetrics {
    fn default() -> Self {
        Self {
            enabled: true,
            filters: FlowMetricsFilters::default(),
            tunning: FlowMetricsTunning::default(),
        }
    }
}

fn to_vlan_mode<'de, D>(deserializer: D) -> Result<agent::VlanMode, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(agent::VlanMode::None),
        1 => Ok(agent::VlanMode::Qinq),
        2 => Ok(agent::VlanMode::Vlan),
        other => Err(de::Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"0|1|2",
        )),
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Npb {
    pub max_mtu: u32,
    pub raw_udp_vlan_tag: u16,
    #[serde(deserialize_with = "to_vlan_mode")]
    pub extra_vlan_header: agent::VlanMode,
    pub traffic_global_dedup: bool,
    pub target_port: u16,
    #[serde(deserialize_with = "parse_maybe_binary_u8")]
    pub custom_vxlan_flags: u8,
    pub overlay_vlan_header_trimming: bool,
    #[serde(deserialize_with = "deser_u64_with_mega_unit")]
    pub max_tx_throughput: u64,
}

impl Default for Npb {
    fn default() -> Self {
        Self {
            max_mtu: 1500,
            raw_udp_vlan_tag: 0,
            extra_vlan_header: agent::VlanMode::None,
            traffic_global_dedup: true,
            target_port: 4789,
            custom_vxlan_flags: 0b1111_1111,
            overlay_vlan_header_trimming: false,
            max_tx_throughput: 1000 << 20,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct OutputCompression {
    pub application_log: bool,
    pub pcap: bool,
}

impl Default for OutputCompression {
    fn default() -> Self {
        Self {
            application_log: true,
            pcap: true,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Outputs {
    pub socket: Socket,
    pub flow_log: OutputsFlowLog,
    pub flow_metrics: FlowMetrics,
    pub npb: Npb,
    pub compression: OutputCompression,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Plugins {
    #[serde(with = "humantime_serde")]
    pub update_time: Duration,
    pub wasm_plugins: Vec<String>,
    pub so_plugins: Vec<String>,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Dev {
    pub feature_flags: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct UserConfig {
    pub global: Global,
    pub inputs: Inputs,
    pub outputs: Outputs,
    pub processors: Processors,
    pub plugins: Plugins,
    pub dev: Dev,
}

const MB: u64 = 1048576;

impl UserConfig {
    const DEFAULT_DNS_PORTS: &'static str = "53,5353";
    const DEFAULT_TLS_PORTS: &'static str = "443,6443";
    const DEFAULT_ORACLE_PORTS: &'static str = "1521";
    const DEFAULT_MEMCACHED_PORTS: &'static str = "11211";
    const PACKET_FANOUT_MODE_MAX: u32 = 7;

    pub fn get_fast_path_map_size(&self, mem_size: u64) -> usize {
        if self.processors.packet.policy.fast_path_map_size > 0 {
            return self.processors.packet.policy.fast_path_map_size;
        }

        min(max((mem_size / MB / 128 * 32000) as usize, 32000), 1 << 20)
    }

    pub fn get_af_packet_blocks(
        &self,
        capture_mode: agent::PacketCaptureType,
        mem_size: u64,
    ) -> usize {
        if capture_mode == agent::PacketCaptureType::Analyzer
            || self.inputs.cbpf.af_packet.tunning.ring_blocks_enabled
        {
            self.inputs.cbpf.af_packet.tunning.ring_blocks.max(8)
        } else {
            (mem_size as usize / DEFAULT_BLOCK_SIZE / 16).min(128)
        }
    }

    pub fn get_protocol_port(&self) -> HashMap<String, String> {
        let mut new = self
            .processors
            .request_log
            .filters
            .port_number_prefilters
            .clone();

        let dns_str = L7ProtocolParser::DNS(DnsLog::default()).as_str();
        // dns default only parse 53,5353 port. when l7_protocol_ports config without DNS, need to reserve the dns default config.
        if !self
            .processors
            .request_log
            .filters
            .port_number_prefilters
            .contains_key(dns_str)
        {
            new.insert(dns_str.to_string(), Self::DEFAULT_DNS_PORTS.to_string());
        }
        #[cfg(feature = "enterprise")]
        {
            let tls_str =
                L7ProtocolParser::TLS(crate::flow_generator::protocol_logs::tls::TlsLog::default())
                    .as_str();
            // tls default only parse 443,6443 port. when l7_protocol_ports config without TLS, need to reserve the tls default config.
            if !self
                .processors
                .request_log
                .filters
                .port_number_prefilters
                .contains_key(tls_str)
            {
                new.insert(tls_str.to_string(), Self::DEFAULT_TLS_PORTS.to_string());
            }
            let oracle_str = L7ProtocolParser::Oracle(
                crate::flow_generator::protocol_logs::sql::OracleLog::default(),
            )
            .as_str();
            // oracle default only parse 1521 port. when l7_protocol_ports config without ORACLE, need to reserve the oracle default config.
            if !self
                .processors
                .request_log
                .filters
                .port_number_prefilters
                .contains_key(oracle_str)
            {
                new.insert(
                    oracle_str.to_string(),
                    Self::DEFAULT_ORACLE_PORTS.to_string(),
                );
            }
        }
        let memcached_str = L7ProtocolParser::Memcached(MemcachedLog::default()).as_str();
        // memcached default only parse 11211 port. when l7_protocol_ports config without MEMCACHED, need to reserve the memcached default config.
        if !self
            .processors
            .request_log
            .filters
            .port_number_prefilters
            .contains_key(memcached_str)
        {
            new.insert(
                memcached_str.to_string(),
                Self::DEFAULT_MEMCACHED_PORTS.to_string(),
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

    pub fn load_from_file<T: AsRef<Path>>(path: T) -> Result<Self, io::Error> {
        let contents = fs::read_to_string(path)?;
        let mut c = if contents.len() == 0 {
            Self::standalone_default()
        } else {
            serde_yaml::from_str(contents.as_str())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        };

        c.set_standalone();
        c.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(c)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.global.communication.proactive_request_interval < Duration::from_secs(1)
            || self.global.communication.proactive_request_interval > Duration::from_secs(60 * 60)
        {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "proactive_request_interval {:?} not in [1s, 1h]",
                self.global.communication.proactive_request_interval
            )));
        }
        if self.inputs.resources.push_interval < Duration::from_secs(10)
            || self.inputs.resources.push_interval > Duration::from_secs(60 * 60)
        {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "push_interval {:?} not in [10s, 1h]",
                self.inputs.resources.push_interval
            )));
        }
        if self.global.self_monitoring.interval < Duration::from_secs(1)
            || self.global.self_monitoring.interval > Duration::from_secs(60 * 60)
        {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "interval {:?} not in [1s, 1h]",
                self.global.self_monitoring.interval
            )));
        }

        // 虽然RFC 791里最低MTU是68，但是此时compressor会崩溃，
        // 所以MTU最低限定到200以确保deepflow-agent能够成功运行
        if self.outputs.npb.max_mtu < 200 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "MTU({}) specified smaller than 200",
                self.outputs.npb.max_mtu
            )));
        }

        if self.outputs.npb.raw_udp_vlan_tag > 4095 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "raw_udp_vlan_tag({}) out of range (0-4095)",
                self.outputs.npb.raw_udp_vlan_tag
            )));
        }

        if self.global.communication.ingester_port == 0 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "ingester_port({}) invalid",
                self.global.communication.ingester_port
            )));
        }
        #[cfg(target_os = "linux")]
        if regex::Regex::new(&self.inputs.cbpf.af_packet.extra_netns_regex).is_err() {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "malformed extra_netns_regex({})",
                self.inputs.cbpf.af_packet.extra_netns_regex
            )));
        }

        if !self.inputs.cbpf.af_packet.interface_regex.is_empty()
            && regex::Regex::new(&self.inputs.cbpf.af_packet.interface_regex).is_err()
        {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "malformed interface_regex({})",
                self.inputs.cbpf.af_packet.interface_regex
            )));
        }

        if self.global.communication.max_escape_duration < Duration::from_secs(600)
            || self.global.communication.max_escape_duration
                > Duration::from_secs(30 * 24 * 60 * 60)
        {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "max_escape_duration {:?} not in [600s, 30d]",
                self.global.communication.max_escape_duration
            )));
        }

        if self.global.communication.proxy_controller_port == 0 {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "proxy_controller_port({}) invalid",
                self.global.communication.proxy_controller_port
            )));
        }

        if !(128..=65535).contains(&self.inputs.cbpf.tunning.max_capture_packet_size) {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "max_capture_packet_size {} not in [128, 65535]",
                self.inputs.cbpf.tunning.max_capture_packet_size
            )));
        }

        if self.outputs.socket.data_socket_type == agent::SocketType::RawUdp {
            return Err(ConfigError::RuntimeConfigInvalid(format!(
                "invalid data_socket_type {:?}",
                self.outputs.socket.data_socket_type
            )));
        }

        Ok(())
    }

    fn set_standalone(&mut self) {
        self.global.common.enabled = true;
        self.global.communication.ingester_ip = "127.0.0.1".to_string();
        self.global.communication.ingester_port = 30033;
        self.global.communication.proxy_controller_ip = "127.0.0.1".to_string();
        self.global.communication.proxy_controller_port = 30035;
        self.global.ntp.enabled = false;
        self.inputs.cbpf.af_packet.interface_regex = "".to_string();
        self.outputs.flow_metrics.filters.apm_metrics = true;
        self.outputs.flow_metrics.filters.npm_metrics = true;
        self.outputs.flow_metrics.filters.npm_metrics_concurrent = true;
        self.outputs.socket.data_socket_type = agent::SocketType::File;
        self.outputs.flow_log.filters.l4_capture_network_types = vec![3];
        self.outputs.flow_log.filters.l7_capture_network_types = vec![3];
    }

    pub fn standalone_default() -> Self {
        let mut config = Self::default();
        config.set_standalone();

        config
    }

    pub fn set_dynamic_config(&mut self, dynamic_config: &agent::DynamicConfig) {
        self.global.common.kubernetes_api_enabled = dynamic_config.kubernetes_api_enabled();
        self.global.common.enabled = dynamic_config.enabled();
        self.global.common.region_id = dynamic_config.region_id();
        self.global.common.pod_cluster_id = dynamic_config.pod_cluster_id();
        self.global.common.vpc_id = dynamic_config.vpc_id();
        self.global.common.agent_id = dynamic_config.agent_id();
        self.global.common.team_id = dynamic_config.team_id();
        self.global.common.organize_id = dynamic_config.organize_id();
        self.global.common.agent_type = dynamic_config.agent_type();
        self.global.common.secret_key = dynamic_config.secret_key().to_string();
        self.global.self_monitoring.hostname = dynamic_config.hostname().to_string();
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
pub const OS_PROC_REGEXP_MATCH_TYPE_CMD_WITH_ARGS: &'static str = "cmdline_with_args";

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

impl From<&EbpfSocketKprobePorts> for EbpfKprobePortlist {
    fn from(p: &EbpfSocketKprobePorts) -> Self {
        Self {
            port_list: p.ports.clone(),
        }
    }
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
pub struct MemoryProfile {
    pub disabled: bool,
    pub regex: String,
    #[serde(with = "humantime_serde")]
    pub report_interval: Duration,
}

impl Default for MemoryProfile {
    fn default() -> Self {
        MemoryProfile {
            disabled: true,
            regex: "^java".to_string(),
            report_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct Preprocess {
    pub stack_compression: bool,
}

impl Default for Preprocess {
    fn default() -> Self {
        Preprocess {
            stack_compression: true,
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
    pub field_selector: String,
}

impl From<&ApiResources> for KubernetesResourceConfig {
    fn from(r: &ApiResources) -> Self {
        Self {
            name: r.name.clone(),
            group: r.group.clone(),
            version: r.version.clone(),
            disabled: r.disabled.clone(),
            field_selector: r.field_selector.clone(),
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct MatchRule {
    pub prefix: String,
    pub keep_segments: usize,
}

impl From<&HttpEndpointMatchRule> for MatchRule {
    fn from(r: &HttpEndpointMatchRule) -> Self {
        Self {
            prefix: r.url_prefix.clone(),
            keep_segments: r.keep_segments,
        }
    }
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

impl From<&CustomFields> for ExtraLogFieldsInfo {
    fn from(c: &CustomFields) -> Self {
        Self {
            field_name: c.field_name.clone(),
        }
    }
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

fn default_obfuscate_enabled_protocols() -> Vec<String> {
    vec!["Redis".to_string()]
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct L7ProtocolAdvancedFeatures {
    pub http_endpoint_extraction: HttpEndpointExtraction,
    #[serde(default = "default_obfuscate_enabled_protocols")]
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

impl From<&TagFilterOperator> for L7LogBlacklist {
    fn from(o: &TagFilterOperator) -> Self {
        Self {
            field_name: o.name.to_string(),
            operator: o.operator.to_string(),
            value: o.value.to_string(),
        }
    }
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
#[serde(default)]
pub struct PortConfig {
    pub analyzer_port: u16,
    pub proxy_controller_port: u16,
}

impl Default for PortConfig {
    fn default() -> Self {
        let config = UserConfig::default();
        PortConfig {
            analyzer_port: config.global.communication.ingester_port,
            proxy_controller_port: config.global.communication.proxy_controller_port,
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

fn to_if_mac_source<'de, D>(deserializer: D) -> Result<agent::IfMacSource, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(agent::IfMacSource::IfMac),
        1 => Ok(agent::IfMacSource::IfName),
        2 => Ok(agent::IfMacSource::IfLibvirtXml),
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

fn deser_u32_with_mega_unit<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(u32::deserialize(deserializer)? << 20)
}

fn deser_u64_with_mega_unit<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(u64::deserialize(deserializer)? << 20)
}

fn deser_usize_with_mega_unit<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(usize::deserialize(deserializer)? << 20)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    #[test]
    fn read_yaml_file() {
        // TODO: improve test cases
        let c = Config::load_from_file("config/deepflow-agent.yaml")
            .expect("failed loading config file");
        assert_eq!(c.controller_ips.len(), 1);
        assert_eq!(&c.controller_ips[0], "127.0.0.1");
    }

    #[test]
    fn parse_log_level() {
        let yaml = r#"
log_level: Info
log_file: "/var/log/deepflow-agent/test.log"
log_backhaul_enabled: true
"#;
        let log: Log = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(log.log_level, log::Level::Info);
        assert_eq!(log.log_file, "/var/log/deepflow-agent/test.log");
        assert!(log.log_backhaul_enabled);

        let yaml_with_warn = r#"
log_level: WARN
log_file: "/tmp/agent.log"
log_backhaul_enabled: false
"#;
        let log: Log = serde_yaml::from_str(yaml_with_warn).unwrap();
        assert_eq!(log.log_level, log::Level::Warn);
        assert_eq!(log.log_file, "/tmp/agent.log");
        assert!(!log.log_backhaul_enabled);
    }

    #[test]
    fn parse_user_config() {
        let template_path = "../server/agent_config/template.yaml";
        let yaml_content = fs::read_to_string(template_path).expect("Failed to read template.yaml");

        let _: UserConfig = serde_yaml::from_str(&yaml_content)
            .expect("Failed to parse template.yaml to UserConfig");
    }

    #[test]
    fn check_defaults() {
        let template_path = "../server/agent_config/template.yaml";
        let yaml_content = fs::read_to_string(template_path).expect("Failed to read template.yaml");

        let parsed_config: UserConfig = serde_yaml::from_str(&yaml_content)
            .expect("Failed to parse template.yaml to UserConfig");

        assert_eq!(parsed_config, UserConfig::default());
    }

    #[test]
    fn parse_tcp_header() {
        let yaml = r#"
block_size: 512
sender_queue_size: 131072
header_fields_flag: "0b1010_1010"
"#;
        let tcp_header: TcpHeader = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(tcp_header.block_size, 512);
        assert_eq!(tcp_header.sender_queue_size, 131072);
        assert_eq!(tcp_header.header_fields_flag, 0b1010_1010);

        // Test with decimal input for header_fields_flag
        let yaml = r#"
block_size: 256
sender_queue_size: 65536
header_fields_flag: "170"
"#;
        let tcp_header: TcpHeader = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(tcp_header.header_fields_flag, 170); // 170 decimal == 0b1010_1010

        // Test with invalid input
        let yaml_invalid = r#"
block_size: 256
sender_queue_size: 65536
header_fields_flag: "invalid"
"#;
        let result: Result<TcpHeader, _> = serde_yaml::from_str(yaml_invalid);
        assert!(result.is_err());
    }

    #[test]
    fn parse_proc_config() {
        let yaml = r#"
process_matcher:
- match_regex: python[2|3].* (.*)\.py
  match_type: cmdline
  match_languages: []
  match_usernames: []
  only_in_container: true
  only_with_tag: false
  ignore: false
  enabled_features: [ebpf.socket.uprobe.golang, ebpf.profile.on_cpu]
"#;
        let _proc: Proc = serde_yaml::from_str(yaml).unwrap();

        let default_matcher_yaml = r#"
enabled: true
"#;
        let proc: Proc = serde_yaml::from_str(default_matcher_yaml).unwrap();
        assert_eq!(
            proc.process_matcher.get(0),
            Some(&ProcessMatcher::default())
        );
    }
}
