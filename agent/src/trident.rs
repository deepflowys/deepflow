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

use std::env;
use std::fmt;
use std::fs;
use std::io::Write;
use std::mem;
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, Condvar, Mutex, RwLock, Weak,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow, Result};
use arc_swap::access::Access;
use dns_lookup::lookup_host;
use flate2::{
    write::{GzEncoder, ZlibEncoder},
    Compression,
};
use flexi_logger::{
    colored_opt_format, writers::LogWriter, Age, Cleanup, Criterion, FileSpec, Logger, Naming,
};
use integration_vector::vector_component::VectorComponent;
use log::{debug, info, warn};
use num_enum::{FromPrimitive, IntoPrimitive};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::broadcast;
use zstd::Encoder as ZstdEncoder;

use crate::{
    collector::{
        flow_aggr::FlowAggrThread, quadruple_generator::QuadrupleGeneratorThread, CollectorThread,
        MetricsType,
    },
    collector::{
        l7_quadruple_generator::L7QuadrupleGeneratorThread, Collector, L7Collector,
        L7CollectorThread,
    },
    common::{
        enums::CaptureNetworkType,
        flow::L7Stats,
        proc_event::BoxedProcEvents,
        tagged_flow::{BoxedTaggedFlow, TaggedFlow},
        tap_types::CaptureNetworkTyper,
        FeatureFlags, DEFAULT_LOG_RETENTION, DEFAULT_TRIDENT_CONF_FILE, FREE_SPACE_REQUIREMENT,
    },
    config::PcapStream,
    config::{
        config,
        handler::{ConfigHandler, DispatcherConfig, ModuleConfig},
        Config, ConfigError, DpdkSource, UserConfig,
    },
    debug::{ConstructDebugCtx, Debugger},
    dispatcher::{
        self, recv_engine::bpf, BpfOptions, Dispatcher, DispatcherBuilder, DispatcherListener,
    },
    exception::ExceptionHandler,
    flow_generator::{
        protocol_logs::BoxAppProtoLogsData, protocol_logs::SessionAggregator, PacketSequenceParser,
        TIME_UNIT,
    },
    handler::{NpbBuilder, PacketHandlerBuilder},
    integration_collector::{
        ApplicationLog, BoxedPrometheusExtra, Datadog, MetricServer, OpenTelemetry,
        OpenTelemetryCompressed, Profile, TelegrafMetric,
    },
    metric::document::BoxedDocument,
    monitor::Monitor,
    platform::synchronizer::Synchronizer as PlatformSynchronizer,
    policy::{Policy, PolicyGetter, PolicySetter},
    rpc::{Session, Synchronizer, DEFAULT_TIMEOUT},
    sender::{
        npb_sender::NpbArpTable,
        uniform_sender::{Connection, UniformSenderThread},
    },
    utils::{
        cgroups::{is_kernel_available_for_cgroups, Cgroups},
        command::get_hostname,
        environment::{
            check, controller_ip_check, free_memory_check, free_space_checker, get_ctrl_ip_and_mac,
            get_env, kernel_check, running_in_container, running_in_k8s, tap_interface_check,
            trident_process_check,
        },
        guard::Guard,
        logger::{LogLevelWriter, LogWriterAdapter, RemoteLogWriter},
        npb_bandwidth_watcher::NpbBandwidthWatcher,
        stats::{self, Countable, QueueStats, RefCountable},
    },
};
#[cfg(any(target_os = "linux", target_os = "android"))]
use crate::{
    ebpf_dispatcher::EbpfCollector,
    platform::SocketSynchronizer,
    utils::{environment::core_file_check, lru::Lru, process::ProcessListener},
};
#[cfg(target_os = "linux")]
use crate::{
    platform::{
        kubernetes::{GenericPoller, Poller, SidecarPoller},
        ApiWatcher, LibvirtXmlExtractor,
    },
    utils::environment::{IN_CONTAINER, K8S_WATCH_POLICY},
};

use integration_skywalking::SkyWalkingExtra;
use packet_sequence_block::BoxedPacketSequenceBlock;
use pcap_assembler::{BoxedPcapBatch, PcapAssembler};

use public::{
    buffer::BatchedBox,
    debug::QueueDebugger,
    packet::MiniPacket,
    proto::agent::{self, Exception, PacketCaptureType, SocketType},
    queue::{self, DebugSender},
    utils::net::{get_route_src_ip, Link, MacAddr},
    LeakyBucket,
};
#[cfg(target_os = "linux")]
use public::{netns, packet, queue::Receiver};

const MINUTE: Duration = Duration::from_secs(60);
const COMMON_DELAY: u64 = 5; // Potential delay from other processing steps in flow_map
const QG_PROCESS_MAX_DELAY: u64 = 5; // FIXME: Potential delay from processing steps in qg, it is an estimated value and is not accurate; the data processing capability of the quadruple_generator should be optimized.

#[derive(Debug, Default)]
pub struct ChangedConfig {
    pub user_config: UserConfig,
    pub blacklist: Vec<u64>,
    pub vm_mac_addrs: Vec<MacAddr>,
    pub gateway_vmac_addrs: Vec<MacAddr>,
    pub tap_types: Vec<agent::CaptureNetworkType>,
}

#[derive(Clone, Default, Copy, PartialEq, Eq, Debug)]
pub enum RunningMode {
    #[default]
    Managed,
    Standalone,
}

#[derive(Copy, Clone, Debug)]
struct InnerState {
    enabled: bool,
    melted_down: bool,
}

impl Default for InnerState {
    fn default() -> Self {
        Self {
            enabled: false,
            melted_down: true,
        }
    }
}

impl From<InnerState> for State {
    fn from(state: InnerState) -> Self {
        if state.enabled && !state.melted_down {
            State::Running
        } else {
            State::Disabled
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum State {
    Running,
    Terminated,
    Disabled,
}

#[derive(Default)]
pub struct AgentState {
    // terminated is outside of Mutex because during termination, state will be locked in main thread,
    // and the main thread will try to stop other threads, in which may lock and update agent state,
    // causing a deadlock. Checking terminated state before locking inner state will avoid this deadlock.
    terminated: AtomicBool,
    state: Mutex<(InnerState, Option<ChangedConfig>)>,
    notifier: Condvar,
}

impl AgentState {
    pub fn enable(&self) {
        if self.terminated.load(Ordering::Relaxed) {
            return;
        }
        let mut sg = self.state.lock().unwrap();
        let old_state: State = sg.0.into();
        sg.0.enabled = true;
        let new_state: State = sg.0.into();
        if old_state != new_state {
            info!("Agent state changed from {old_state:?} to {new_state:?} (enabled: {} melted_down: {})", sg.0.enabled, sg.0.melted_down);
            self.notifier.notify_one();
        }
    }

    pub fn disable(&self) {
        if self.terminated.load(Ordering::Relaxed) {
            return;
        }
        let mut sg = self.state.lock().unwrap();
        let old_state: State = sg.0.into();
        sg.0.enabled = false;
        let new_state: State = sg.0.into();
        if old_state != new_state {
            info!("Agent state changed from {old_state:?} to {new_state:?} (enabled: {} melted_down: {})", sg.0.enabled, sg.0.melted_down);
            self.notifier.notify_one();
        }
    }

    pub fn melt_down(&self) {
        if self.terminated.load(Ordering::Relaxed) {
            return;
        }
        let mut sg = self.state.lock().unwrap();
        let old_state: State = sg.0.into();
        sg.0.melted_down = true;
        let new_state: State = sg.0.into();
        if old_state != new_state {
            info!("Agent state changed from {old_state:?} to {new_state:?} (enabled: {} melted_down: {})", sg.0.enabled, sg.0.melted_down);
            self.notifier.notify_one();
        }
    }

    pub fn recover(&self) {
        if self.terminated.load(Ordering::Relaxed) {
            return;
        }
        let mut sg = self.state.lock().unwrap();
        let old_state: State = sg.0.into();
        sg.0.melted_down = false;
        let new_state: State = sg.0.into();
        if old_state != new_state {
            info!("Agent state changed from {old_state:?} to {new_state:?} (enabled: {} melted_down: {})", sg.0.enabled, sg.0.melted_down);
            self.notifier.notify_one();
        }
    }

    pub fn terminate(&self) {
        if !self.terminated.swap(true, Ordering::Relaxed) {
            // log only the first time
            info!("Agent state changed to {:?}", State::Terminated);
        }
        self.notifier.notify_one();
    }

    pub fn update_config(&self, config: ChangedConfig) {
        if self.terminated.load(Ordering::Relaxed) {
            return;
        }
        let mut sg = self.state.lock().unwrap();
        sg.0.enabled = config.user_config.global.common.enabled;
        sg.1.replace(config);
        self.notifier.notify_one();
    }
}

pub struct VersionInfo {
    pub name: &'static str,
    pub branch: &'static str,
    pub commit_id: &'static str,
    pub rev_count: &'static str,
    pub compiler: &'static str,
    pub compile_time: &'static str,

    pub revision: &'static str,
}

impl VersionInfo {
    pub fn brief_tag(&self) -> String {
        format!(
            "{}|{}|{}",
            match self.name {
                "deepflow-agent-ce" => "CE",
                "deepflow-agent-ee" => "EE",
                _ => panic!("{:?} unknown deepflow-agent edition", &self.name),
            },
            self.branch,
            self.commit_id
        )
    }
}

impl fmt::Display for VersionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}
Name: {}
Branch: {}
CommitId: {}
RevCount: {}
Compiler: {}
CompileTime: {}",
            self.rev_count,
            self.commit_id,
            match self.name {
                "deepflow-agent-ce" => "deepflow-agent community edition",
                "deepflow-agent-ee" => "deepflow-agent enterprise edition",
                _ => panic!("{:?} unknown deepflow-agent edition", &self.name),
            },
            self.branch,
            self.commit_id,
            self.rev_count,
            self.compiler,
            self.compile_time
        )
    }
}

#[derive(Clone, Debug)]
pub struct AgentId {
    pub ip: IpAddr,
    pub mac: MacAddr,
    pub team_id: String,
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.team_id.is_empty() {
            write!(f, "{}/{}", self.ip, self.mac)
        } else {
            write!(f, "{}/{}/{}", self.ip, self.mac, self.team_id)
        }
    }
}

impl From<&AgentId> for agent::AgentId {
    fn from(id: &AgentId) -> Self {
        Self {
            ip: Some(id.ip.to_string()),
            mac: Some(id.mac.to_string()),
            team_id: Some(id.team_id.clone()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, FromPrimitive, IntoPrimitive, num_enum::Default)]
#[repr(u8)]
pub enum SenderEncoder {
    #[num_enum(default)]
    Raw = 0,

    Zlib = 1,
    Gzip = 2,
    Zstd = 3,
}

impl SenderEncoder {
    pub fn encode(&self, encode_buffer: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let result = match self {
            Self::Raw => None,
            Self::Zlib => {
                let mut encoder = ZlibEncoder::new(
                    Vec::with_capacity(encode_buffer.len()),
                    Compression::default(),
                );
                encoder.write_all(&encode_buffer)?;
                Some(encoder.finish()?)
            }
            Self::Gzip => {
                let mut encoder = GzEncoder::new(
                    Vec::with_capacity(encode_buffer.len()),
                    Compression::default(),
                );
                encoder.write_all(&encode_buffer)?;
                Some(encoder.finish()?)
            }
            Self::Zstd => {
                let mut encoder = ZstdEncoder::new(Vec::with_capacity(encode_buffer.len()), 0)?;
                encoder.write_all(&encode_buffer)?;
                Some(encoder.finish()?)
            }
        };
        Ok(result)
    }
}

pub struct Trident {
    state: Arc<AgentState>,
    handle: Option<JoinHandle<()>>,
    #[cfg(target_os = "linux")]
    pid_file: Option<crate::utils::pid_file::PidFile>,
}

impl Trident {
    pub fn start<P: AsRef<Path>>(
        config_path: P,
        version_info: &'static VersionInfo,
        agent_mode: RunningMode,
        sidecar_mode: bool,
        cgroups_disabled: bool,
    ) -> Result<Trident> {
        let config = match agent_mode {
            RunningMode::Managed => {
                match Config::load_from_file(config_path.as_ref()) {
                    Ok(conf) => conf,
                    Err(e) => {
                        if let ConfigError::YamlConfigInvalid(_) = e {
                            // try to load config file from trident.yaml to support upgrading from trident
                            if let Ok(conf) = Config::load_from_file(DEFAULT_TRIDENT_CONF_FILE) {
                                conf
                            } else {
                                // return the original error instead of loading trident conf
                                return Err(e.into());
                            }
                        } else {
                            return Err(e.into());
                        }
                    }
                }
            }
            RunningMode::Standalone => {
                let rc = UserConfig::load_from_file(config_path.as_ref())?;
                let mut conf = Config::default();
                conf.controller_ips = vec!["127.0.0.1".into()];
                conf.log_file = rc.global.self_monitoring.log.log_file;
                conf.agent_mode = agent_mode;
                conf
            }
        };
        #[cfg(target_os = "linux")]
        let pid_file = if !config.pid_file.is_empty() {
            match crate::utils::pid_file::PidFile::open(&config.pid_file) {
                Ok(file) => Some(file),
                Err(e) => return Err(anyhow!("Create pid file {} failed: {}", config.pid_file, e)),
            }
        } else {
            None
        };

        let controller_ip: IpAddr = config.controller_ips[0].parse()?;
        let (ctrl_ip, ctrl_mac) = match get_ctrl_ip_and_mac(&controller_ip) {
            Ok(tuple) => tuple,
            Err(e) => return Err(anyhow!("get ctrl ip and mac failed: {}", e)),
        };
        let mut config_handler = ConfigHandler::new(config, ctrl_ip, ctrl_mac);

        let config = &config_handler.static_config;
        let cgroups_disabled = cgroups_disabled || config.cgroups_disabled;
        let hostname = match config.override_os_hostname.as_ref() {
            Some(name) => name.to_owned(),
            None => get_hostname().unwrap_or("Unknown".to_string()),
        };

        let ntp_diff = Arc::new(AtomicI64::new(0));
        let stats_collector = Arc::new(stats::Collector::new(&hostname, ntp_diff.clone()));
        let exception_handler = ExceptionHandler::default();

        let log_stats_shared_connection = Arc::new(Mutex::new(Connection::new()));
        let mut stats_sender = UniformSenderThread::new(
            "stats",
            stats_collector.get_receiver(),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            Some(log_stats_shared_connection.clone()),
            SenderEncoder::Raw,
        );
        stats_sender.start();

        let base_name = Path::new(&env::args().next().unwrap())
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();

        let (log_level_writer, log_level_counter) = LogLevelWriter::new();
        let logger = Logger::try_with_env_or_str("info")
            .unwrap()
            .format(colored_opt_format);
        // check log folder permission
        let base_path = Path::new(&config.log_file).parent().unwrap();
        let write_to_file = if base_path.exists() {
            base_path
                .metadata()
                .ok()
                .map(|meta| !meta.permissions().readonly())
                .unwrap_or(false)
        } else {
            fs::create_dir_all(base_path).is_ok()
        };
        let mut logger_writers: Vec<Box<dyn LogWriter>> = vec![Box::new(log_level_writer)];
        if matches!(config.agent_mode, RunningMode::Managed) {
            let remote_log_writer = RemoteLogWriter::new(
                base_name,
                hostname.clone(),
                config_handler.log(),
                config_handler.sender(),
                stats_collector.clone(),
                exception_handler.clone(),
                ntp_diff.clone(),
                log_stats_shared_connection,
            );
            logger_writers.push(Box::new(remote_log_writer));
        }
        let logger = if write_to_file {
            logger
                .log_to_file_and_writer(
                    FileSpec::try_from(&config.log_file)?,
                    Box::new(LogWriterAdapter::new(logger_writers)),
                )
                .rotate(
                    Criterion::AgeOrSize(
                        Age::Day,
                        (config::Limits::default().local_log_current_file_size_mb as u64) << 20,
                    ),
                    Naming::Timestamps,
                    Cleanup::KeepLogFiles(DEFAULT_LOG_RETENTION as usize),
                )
                .create_symlink(&config.log_file)
                .append()
        } else {
            eprintln!(
                "Log file path '{}' access denied, logs will not be written to file",
                &config.log_file
            );
            logger.log_to_writer(Box::new(LogWriterAdapter::new(logger_writers)))
        };

        #[cfg(any(target_os = "linux", target_os = "android"))]
        let logger = if nix::unistd::getppid().as_raw() != 1 {
            logger.duplicate_to_stderr(flexi_logger::Duplicate::All)
        } else {
            logger
        };
        let logger_handle = logger.start()?;
        config_handler.set_logger_handle(logger_handle);

        let config = &config_handler.static_config;
        // Use controller ip to replace analyzer ip before obtaining configuration
        if matches!(config.agent_mode, RunningMode::Managed) {
            stats_collector.start();
        }

        stats_collector.register_countable(
            &stats::NoTagModule("log_counter"),
            stats::Countable::Owned(Box::new(log_level_counter)),
        );

        info!("static_config {:#?}", config);
        let state = Arc::new(AgentState::default());
        let state_thread = state.clone();
        let config_path = match agent_mode {
            RunningMode::Managed => None,
            RunningMode::Standalone => Some(config_path.as_ref().to_path_buf()),
        };
        let handle = Some(thread::spawn(move || {
            if let Err(e) = Self::run(
                state_thread,
                ctrl_ip,
                ctrl_mac,
                config_handler,
                version_info,
                stats_collector,
                exception_handler,
                config_path,
                sidecar_mode,
                cgroups_disabled,
                ntp_diff,
            ) {
                warn!(
                    "Launching deepflow-agent failed: {}, deepflow-agent restart...",
                    e
                );
                crate::utils::notify_exit(1);
            }
        }));

        Ok(Trident {
            state,
            handle,
            #[cfg(target_os = "linux")]
            pid_file,
        })
    }

    fn run(
        state: Arc<AgentState>,
        ctrl_ip: IpAddr,
        ctrl_mac: MacAddr,
        mut config_handler: ConfigHandler,
        version_info: &'static VersionInfo,
        stats_collector: Arc<stats::Collector>,
        exception_handler: ExceptionHandler,
        config_path: Option<PathBuf>,
        sidecar_mode: bool,
        cgroups_disabled: bool,
        ntp_diff: Arc<AtomicI64>,
    ) -> Result<()> {
        info!("==================== Launching DeepFlow-Agent ====================");
        info!("Brief tag: {}", version_info.brief_tag());
        info!("Environment variables: {:?}", get_env());

        if running_in_container() {
            info!(
                "use K8S_NODE_IP_FOR_DEEPFLOW env ip as destination_ip({})",
                ctrl_ip
            );
        }

        #[cfg(target_os = "linux")]
        let agent_id = if sidecar_mode {
            AgentId {
                ip: ctrl_ip.clone(),
                mac: ctrl_mac,
                team_id: config_handler.static_config.team_id.clone(),
            }
        } else {
            // use host ip/mac as agent id if not in sidecar mode
            if let Err(e) = netns::open_named_and_setns(&netns::NsFile::Root) {
                return Err(anyhow!("agent must have CAP_SYS_ADMIN to run without 'hostNetwork: true'. setns error: {}", e));
            }
            let controller_ip: IpAddr = config_handler.static_config.controller_ips[0].parse()?;
            let (ip, mac) = match get_ctrl_ip_and_mac(&controller_ip) {
                Ok(tuple) => tuple,
                Err(e) => return Err(anyhow!("get ctrl ip and mac failed with error: {}", e)),
            };
            if let Err(e) = netns::reset_netns() {
                return Err(anyhow!("reset netns error: {}", e));
            };
            AgentId {
                ip,
                mac,
                team_id: config_handler.static_config.team_id.clone(),
            }
        };
        #[cfg(any(target_os = "windows", target_os = "android"))]
        let agent_id = AgentId {
            ip: ctrl_ip.clone(),
            mac: ctrl_mac,
            team_id: config_handler.static_config.team_id.clone(),
        };

        info!(
            "agent {} running in {:?} mode, ctrl_ip {} ctrl_mac {}",
            agent_id, config_handler.static_config.agent_mode, ctrl_ip, ctrl_mac
        );

        let session = Arc::new(Session::new(
            config_handler.static_config.controller_port,
            config_handler.static_config.controller_tls_port,
            DEFAULT_TIMEOUT,
            config_handler
                .static_config
                .controller_cert_file_prefix
                .clone(),
            config_handler.static_config.controller_ips.clone(),
            exception_handler.clone(),
            &stats_collector,
        ));

        let runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(
                    config_handler
                        .static_config
                        .async_worker_thread_number
                        .into(),
                )
                .enable_all()
                .build()
                .unwrap(),
        );

        if matches!(
            config_handler.static_config.agent_mode,
            RunningMode::Managed
        ) && running_in_k8s()
            && config_handler
                .static_config
                .kubernetes_cluster_id
                .is_empty()
        {
            config_handler.static_config.kubernetes_cluster_id =
                Config::get_k8s_cluster_id(&runtime, &session, &config_handler.static_config)
                    .unwrap_or_default();
        }

        let (agent_id_tx, _) = broadcast::channel::<AgentId>(1);
        let agent_id_tx = Arc::new(agent_id_tx);

        let synchronizer = Arc::new(Synchronizer::new(
            runtime.clone(),
            session.clone(),
            state.clone(),
            version_info,
            agent_id,
            config_handler.static_config.controller_ips[0].clone(),
            config_handler.static_config.vtap_group_id_request.clone(),
            config_handler.static_config.kubernetes_cluster_id.clone(),
            config_handler.static_config.kubernetes_cluster_name.clone(),
            config_handler.static_config.override_os_hostname.clone(),
            config_handler.static_config.agent_unique_identifier,
            exception_handler.clone(),
            config_handler.static_config.agent_mode,
            config_path,
            agent_id_tx.clone(),
            ntp_diff,
        ));
        stats_collector.register_countable(
            &stats::NoTagModule("ntp"),
            stats::Countable::Owned(Box::new(synchronizer.ntp_counter())),
        );
        synchronizer.start();

        if matches!(
            config_handler.static_config.agent_mode,
            RunningMode::Managed
        ) {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let remote_executor = crate::rpc::Executor::new(
                synchronizer.agent_id.clone(),
                session.clone(),
                runtime.clone(),
                exception_handler.clone(),
            );
            #[cfg(any(target_os = "linux", target_os = "android"))]
            remote_executor.start();
        }

        let mut domain_name_listener = DomainNameListener::new(
            stats_collector.clone(),
            session.clone(),
            config_handler.static_config.controller_domain_name.clone(),
            config_handler.static_config.controller_ips.clone(),
            config_handler.static_config.team_id.clone(),
            sidecar_mode,
            agent_id_tx,
        );
        domain_name_listener.start();

        let mut cgroup_mount_path = "".to_string();
        let mut is_cgroup_v2 = false;
        let mut cgroups_controller = None;
        if running_in_container() {
            info!("don't initialize cgroups controller, because agent is running in container");
        } else if !is_kernel_available_for_cgroups() {
            // fixme: Linux after kernel version 2.6.24 can use cgroups
            info!("don't initialize cgroups controller, because kernel version < 3 or agent is in Windows");
        } else if cgroups_disabled {
            info!("don't initialize cgroups controller, disable cgroups, deepflow-agent will default to checking the CPU and memory resource usage in a loop every 10 seconds to prevent resource usage from exceeding limits");
        } else {
            match Cgroups::new(process::id() as u64, config_handler.environment()) {
                Ok(cg_controller) => {
                    cg_controller.start();
                    cgroup_mount_path = cg_controller.get_mount_path();
                    is_cgroup_v2 = cg_controller.is_v2();
                    cgroups_controller = Some(cg_controller);
                }
                Err(e) => {
                    warn!("initialize cgroups controller failed: {}, resource utilization will be checked regularly to prevent resource usage from exceeding the limit.", e);
                    exception_handler.set(Exception::CgroupsConfigError);
                }
            }
        }

        let log_dir = Path::new(config_handler.static_config.log_file.as_str());
        let log_dir = log_dir.parent().unwrap().to_str().unwrap();
        let guard = match Guard::new(
            config_handler.environment(),
            state.clone(),
            log_dir.to_string(),
            exception_handler.clone(),
            cgroup_mount_path,
            is_cgroup_v2,
            config_handler
                .candidate_config
                .user_config
                .global
                .tunning
                .idle_memory_trimming,
            cgroups_disabled,
        ) {
            Ok(g) => g,
            Err(e) => {
                warn!("guard create failed");
                return Err(anyhow!(e));
            }
        };

        let monitor = Monitor::new(
            stats_collector.clone(),
            log_dir.to_string(),
            config_handler.environment(),
        )?;
        monitor.start();

        #[cfg(target_os = "linux")]
        let (libvirt_xml_extractor, platform_synchronizer, sidecar_poller, api_watcher) = {
            let ext = Arc::new(LibvirtXmlExtractor::new());
            let syn = Arc::new(PlatformSynchronizer::new(
                runtime.clone(),
                config_handler.platform(),
                config_handler.static_config.override_os_hostname.clone(),
                synchronizer.agent_id.clone(),
                session.clone(),
                ext.clone(),
                exception_handler.clone(),
            ));
            ext.start();
            let poller = if sidecar_mode {
                let p = match SidecarPoller::new(
                    config_handler.static_config.controller_ips[0].parse()?,
                ) {
                    Ok(p) => p,
                    Err(e) => return Err(anyhow!(e)),
                };
                let p: Arc<GenericPoller> = Arc::new(p.into());
                syn.set_kubernetes_poller(p.clone());
                Some(p)
            } else {
                None
            };
            let watcher = Arc::new(ApiWatcher::new(
                runtime.clone(),
                config_handler.platform(),
                synchronizer.agent_id.clone(),
                session.clone(),
                exception_handler.clone(),
                stats_collector.clone(),
            ));
            (ext, syn, poller, watcher)
        };
        #[cfg(any(target_os = "windows", target_os = "android"))]
        let platform_synchronizer = Arc::new(PlatformSynchronizer::new(
            runtime.clone(),
            config_handler.platform(),
            config_handler.static_config.override_os_hostname.clone(),
            synchronizer.agent_id.clone(),
            session.clone(),
            exception_handler.clone(),
        ));
        if matches!(
            config_handler.static_config.agent_mode,
            RunningMode::Managed
        ) {
            platform_synchronizer.start();
        }

        let mut state_guard = state.state.lock().unwrap();
        let mut components: Option<Components> = None;
        let mut first_run = true;
        let mut config_initialized = false;

        loop {
            if state.terminated.load(Ordering::Relaxed) {
                if let Some(mut c) = components {
                    c.stop();
                    guard.stop();
                    monitor.stop();
                    domain_name_listener.stop();
                    platform_synchronizer.stop();
                    #[cfg(target_os = "linux")]
                    {
                        api_watcher.stop();
                        libvirt_xml_extractor.stop();
                    }
                    if let Some(cg_controller) = cgroups_controller {
                        if let Err(e) = cg_controller.stop() {
                            info!("stop cgroups controller failed, {:?}", e);
                        }
                    }
                }
                return Ok(());
            }

            match State::from(state_guard.0) {
                State::Running if state_guard.1.is_none() => {
                    state_guard = state.notifier.wait(state_guard).unwrap();
                    #[cfg(target_os = "linux")]
                    if config_handler
                        .candidate_config
                        .platform
                        .kubernetes_api_enabled
                    {
                        api_watcher.start();
                    } else {
                        api_watcher.stop();
                    }
                    if let Some(ref mut c) = components {
                        c.start();
                    }
                    continue;
                }
                State::Disabled => {
                    if let Some(ref mut c) = components {
                        c.stop();
                    }
                    if let Some(cfg) = state_guard.1.take() {
                        let agent_id = synchronizer.agent_id.read().clone();
                        let callbacks = config_handler.on_config(
                            cfg.user_config,
                            &exception_handler,
                            None,
                            #[cfg(target_os = "linux")]
                            &api_watcher,
                            &runtime,
                            &session,
                            &agent_id,
                            first_run,
                        );
                        first_run = false;

                        #[cfg(target_os = "linux")]
                        if config_handler
                            .candidate_config
                            .platform
                            .kubernetes_api_enabled
                        {
                            api_watcher.start();
                        } else {
                            api_watcher.stop();
                        }

                        if let Some(Components::Agent(c)) = components.as_mut() {
                            for callback in callbacks {
                                callback(&config_handler, c);
                            }

                            for d in c.dispatcher_components.iter_mut() {
                                d.dispatcher_listener
                                    .on_config_change(&config_handler.candidate_config.dispatcher);
                            }
                        } else {
                            stats_collector
                                .set_hostname(config_handler.candidate_config.stats.host.clone());
                            stats_collector
                                .set_min_interval(config_handler.candidate_config.stats.interval);
                        }

                        if !config_initialized {
                            // start guard on receiving first config to ensure
                            // the meltdown thresholds are set by the config
                            guard.start();
                            config_initialized = true;
                        }
                    }
                    state_guard = state.notifier.wait(state_guard).unwrap();
                    continue;
                }
                _ => (),
            }

            let ChangedConfig {
                user_config,
                blacklist,
                vm_mac_addrs,
                gateway_vmac_addrs,
                tap_types,
            } = state_guard.1.take().unwrap();
            mem::drop(state_guard);

            // TODO At present, all changes in user_config will not cause the agent to restart,
            // hot update needs to be implemented and this judgment should be removed
            // if let Some(old_user_config) = current_user_config {
            //     if old_user_config != user_config {
            //         if let Some(mut c) = components.take() {
            //             c.stop();
            //         }
            //         // EbpfCollector does not support recreation because it calls bpf_tracer_init, which can only be called once in a process
            //         // Work around this problem by exiting and restart trident
            //         let info = "user_config updated, deepflow-agent restart...";
            //         warn!("{}", info);
            //         thread::sleep(Duration::from_secs(1));
            //         return Err(anyhow!(info));
            //     }
            // }
            // current_user_config = Some(user_config.clone());
            let agent_id = synchronizer.agent_id.read().clone();
            match components.as_mut() {
                None => {
                    let callbacks = config_handler.on_config(
                        user_config,
                        &exception_handler,
                        None,
                        #[cfg(target_os = "linux")]
                        &api_watcher,
                        &runtime,
                        &session,
                        &agent_id,
                        first_run,
                    );
                    first_run = false;

                    #[cfg(target_os = "linux")]
                    if config_handler
                        .candidate_config
                        .platform
                        .kubernetes_api_enabled
                    {
                        api_watcher.start();
                    } else {
                        api_watcher.stop();
                    }

                    let mut comp = Components::new(
                        &version_info,
                        &config_handler,
                        stats_collector.clone(),
                        &session,
                        &synchronizer,
                        exception_handler.clone(),
                        #[cfg(target_os = "linux")]
                        libvirt_xml_extractor.clone(),
                        platform_synchronizer.clone(),
                        #[cfg(target_os = "linux")]
                        sidecar_poller.clone(),
                        #[cfg(target_os = "linux")]
                        api_watcher.clone(),
                        vm_mac_addrs,
                        gateway_vmac_addrs,
                        config_handler.static_config.agent_mode,
                        runtime.clone(),
                    )?;

                    comp.start();

                    if let Components::Agent(components) = &mut comp {
                        if config_handler.candidate_config.dispatcher.capture_mode
                            == PacketCaptureType::Analyzer
                        {
                            parse_tap_type(components, tap_types);
                        }

                        for callback in callbacks {
                            callback(&config_handler, components);
                        }
                    }

                    components.replace(comp);
                }
                Some(Components::Agent(components)) => {
                    let callbacks: Vec<fn(&ConfigHandler, &mut AgentComponents)> = config_handler
                        .on_config(
                            user_config,
                            &exception_handler,
                            Some(components),
                            #[cfg(target_os = "linux")]
                            &api_watcher,
                            &runtime,
                            &session,
                            &agent_id,
                            first_run,
                        );
                    first_run = false;

                    #[cfg(target_os = "linux")]
                    if config_handler
                        .candidate_config
                        .platform
                        .kubernetes_api_enabled
                    {
                        api_watcher.start();
                    } else {
                        api_watcher.stop();
                    }

                    components.config = config_handler.candidate_config.clone();
                    components.start();

                    component_on_config_change(
                        &config_handler,
                        components,
                        blacklist,
                        vm_mac_addrs,
                        gateway_vmac_addrs,
                        tap_types,
                        &synchronizer,
                        #[cfg(target_os = "linux")]
                        libvirt_xml_extractor.clone(),
                    );
                    for callback in callbacks {
                        callback(&config_handler, components);
                    }

                    for d in components.dispatcher_components.iter_mut() {
                        d.dispatcher_listener
                            .on_config_change(&config_handler.candidate_config.dispatcher);
                    }
                }
                _ => {
                    config_handler.on_config(
                        user_config,
                        &exception_handler,
                        None,
                        #[cfg(target_os = "linux")]
                        &api_watcher,
                        &runtime,
                        &session,
                        &agent_id,
                        first_run,
                    );
                    first_run = false;

                    #[cfg(target_os = "linux")]
                    if config_handler
                        .candidate_config
                        .platform
                        .kubernetes_api_enabled
                    {
                        api_watcher.start();
                    } else {
                        api_watcher.stop();
                    }
                }
            }

            if !config_initialized {
                // start guard on receiving first config to ensure
                // the meltdown thresholds are set by the config
                guard.start();
                config_initialized = true;
            }

            state_guard = state.state.lock().unwrap();
        }
    }

    pub fn stop(&mut self) {
        info!("Gracefully stopping");
        self.state.terminate();
        self.handle.take().unwrap().join().unwrap();
        info!("Gracefully stopped");
    }
}

fn get_listener_links(
    conf: &DispatcherConfig,
    #[cfg(target_os = "linux")] netns: &netns::NsFile,
) -> Vec<Link> {
    if conf.tap_interface_regex.is_empty() {
        info!("tap-interface-regex is empty, skip packet dispatcher");
        return vec![];
    }
    #[cfg(target_os = "linux")]
    match netns::links_by_name_regex_in_netns(&conf.tap_interface_regex, netns) {
        Err(e) => {
            warn!("get interfaces by name regex in {:?} failed: {}", netns, e);
            vec![]
        }
        Ok(links) => {
            if links.is_empty() {
                warn!(
                    "tap-interface-regex({}) do not match any interface in {:?}",
                    conf.tap_interface_regex, netns,
                );
            }
            debug!("tap interfaces in namespace {:?}: {:?}", netns, links);
            links
        }
    }

    #[cfg(any(target_os = "windows", target_os = "android"))]
    match public::utils::net::links_by_name_regex(&conf.tap_interface_regex) {
        Err(e) => {
            warn!("get interfaces by name regex failed: {}", e);
            vec![]
        }
        Ok(links) => {
            if links.is_empty() {
                warn!(
                    "tap-interface-regex({}) do not match any interface, in local mode",
                    conf.tap_interface_regex
                );
            }
            debug!("tap interfaces: {:?}", links);
            links
        }
    }
}

fn component_on_config_change(
    config_handler: &ConfigHandler,
    components: &mut AgentComponents,
    blacklist: Vec<u64>,
    vm_mac_addrs: Vec<MacAddr>,
    gateway_vmac_addrs: Vec<MacAddr>,
    tap_types: Vec<agent::CaptureNetworkType>,
    synchronizer: &Arc<Synchronizer>,
    #[cfg(target_os = "linux")] libvirt_xml_extractor: Arc<LibvirtXmlExtractor>,
) {
    let conf = &config_handler.candidate_config.dispatcher;
    match conf.capture_mode {
        PacketCaptureType::Local => {
            let if_mac_source = conf.if_mac_source;
            components.dispatcher_components.retain_mut(|d| {
                let links = get_listener_links(
                    conf,
                    #[cfg(target_os = "linux")]
                    d.dispatcher_listener.netns(),
                );
                if links.is_empty() {
                    info!("No interfaces found, stopping dispatcher {}", d.id);
                    d.stop();
                    return false;
                }
                d.dispatcher_listener.on_tap_interface_change(
                    &links,
                    if_mac_source,
                    conf.agent_type,
                    &blacklist,
                );
                d.dispatcher_listener
                    .on_vm_change(&vm_mac_addrs, &gateway_vmac_addrs);
                true
            });

            if components.dispatcher_components.is_empty() {
                let links = get_listener_links(
                    conf,
                    #[cfg(target_os = "linux")]
                    &netns::NsFile::Root,
                );
                if links.is_empty() {
                    return;
                }
                match build_dispatchers(
                    components.last_dispatcher_component_id + 1,
                    links,
                    components.stats_collector.clone(),
                    config_handler,
                    components.debugger.clone_queue(),
                    components.is_ce_version,
                    synchronizer,
                    components.npb_bps_limit.clone(),
                    components.npb_arp_table.clone(),
                    components.rx_leaky_bucket.clone(),
                    components.policy_getter,
                    components.exception_handler.clone(),
                    components.bpf_options.clone(),
                    components.packet_sequence_uniform_output.clone(),
                    components.proto_log_sender.clone(),
                    components.pcap_batch_sender.clone(),
                    components.tap_typer.clone(),
                    vm_mac_addrs.clone(),
                    gateway_vmac_addrs.clone(),
                    components.toa_info_sender.clone(),
                    components.l4_flow_aggr_sender.clone(),
                    components.metrics_sender.clone(),
                    #[cfg(target_os = "linux")]
                    netns::NsFile::Root,
                    #[cfg(target_os = "linux")]
                    components.kubernetes_poller.clone(),
                    #[cfg(target_os = "linux")]
                    libvirt_xml_extractor.clone(),
                    #[cfg(target_os = "linux")]
                    None,
                    #[cfg(target_os = "linux")]
                    false,
                ) {
                    Ok(mut d) => {
                        d.start();
                        components.dispatcher_components.push(d);
                        components.last_dispatcher_component_id += 1;
                    }
                    Err(e) => {
                        warn!(
                            "build dispatcher_component failed: {}, deepflow-agent restart...",
                            e
                        );
                        crate::utils::notify_exit(1);
                    }
                }
            }
        }
        PacketCaptureType::Mirror | PacketCaptureType::Analyzer => {
            for d in components.dispatcher_components.iter() {
                let links = get_listener_links(
                    conf,
                    #[cfg(target_os = "linux")]
                    &netns::NsFile::Root,
                );
                d.dispatcher_listener.on_tap_interface_change(
                    &links,
                    conf.if_mac_source,
                    conf.agent_type,
                    &blacklist,
                );
                d.dispatcher_listener
                    .on_vm_change(&vm_mac_addrs, &gateway_vmac_addrs);
            }
            if conf.capture_mode == PacketCaptureType::Analyzer {
                parse_tap_type(components, tap_types);
            }

            #[cfg(target_os = "linux")]
            if conf.capture_mode != PacketCaptureType::Local
                && (!config_handler
                    .candidate_config
                    .user_config
                    .inputs
                    .cbpf
                    .special_network
                    .vhost_user
                    .vhost_socket_path
                    .is_empty()
                    || conf.dpdk_source != DpdkSource::None)
            {
                return;
            }

            // Obtain the currently configured network interfaces
            let mut current_interfaces = get_listener_links(
                conf,
                #[cfg(target_os = "linux")]
                &netns::NsFile::Root,
            );
            current_interfaces.sort();

            if current_interfaces == components.tap_interfaces {
                return;
            }
            info!("Regular matching interface changes, dispatcher restart...");

            // By comparing current_interfaces and components.tap_interfaces, we can determine which
            // dispatcher_components should be closed and which dispatcher_components should be built
            let interfaces_to_build: Vec<_> = current_interfaces
                .iter()
                .filter(|i| !components.tap_interfaces.contains(i))
                .cloned()
                .collect();

            components.dispatcher_components.retain_mut(|d| {
                let retain = current_interfaces.contains(&d.src_link);
                if !retain {
                    d.stop();
                }
                retain
            });

            let mut id = components.last_dispatcher_component_id;
            components
                .policy_setter
                .reset_queue_size(id + interfaces_to_build.len() + 1);
            let debugger_queue = components.debugger.clone_queue();
            for i in interfaces_to_build {
                id += 1;
                match build_dispatchers(
                    id,
                    vec![i],
                    components.stats_collector.clone(),
                    config_handler,
                    debugger_queue.clone(),
                    components.is_ce_version,
                    synchronizer,
                    components.npb_bps_limit.clone(),
                    components.npb_arp_table.clone(),
                    components.rx_leaky_bucket.clone(),
                    components.policy_getter,
                    components.exception_handler.clone(),
                    components.bpf_options.clone(),
                    components.packet_sequence_uniform_output.clone(),
                    components.proto_log_sender.clone(),
                    components.pcap_batch_sender.clone(),
                    components.tap_typer.clone(),
                    vm_mac_addrs.clone(),
                    gateway_vmac_addrs.clone(),
                    components.toa_info_sender.clone(),
                    components.l4_flow_aggr_sender.clone(),
                    components.metrics_sender.clone(),
                    #[cfg(target_os = "linux")]
                    netns::NsFile::Root,
                    #[cfg(target_os = "linux")]
                    components.kubernetes_poller.clone(),
                    #[cfg(target_os = "linux")]
                    libvirt_xml_extractor.clone(),
                    #[cfg(target_os = "linux")]
                    None,
                    #[cfg(target_os = "linux")]
                    false,
                ) {
                    Ok(mut d) => {
                        d.start();
                        components.dispatcher_components.push(d);
                    }
                    Err(e) => {
                        warn!(
                            "build dispatcher_component failed: {}, deepflow-agent restart...",
                            e
                        );
                        crate::utils::notify_exit(1);
                    }
                }
            }
            components.last_dispatcher_component_id = id;
            components.tap_interfaces = current_interfaces;
        }

        _ => {}
    }
}

fn parse_tap_type(components: &mut AgentComponents, tap_types: Vec<agent::CaptureNetworkType>) {
    let mut updated = false;
    if components.cur_tap_types.len() != tap_types.len() {
        updated = true;
    } else {
        for i in 0..tap_types.len() {
            if components.cur_tap_types[i] != tap_types[i] {
                updated = true;
                break;
            }
        }
    }
    if updated {
        components.tap_typer.on_tap_types_change(tap_types.clone());
        components.cur_tap_types.clear();
        components.cur_tap_types.clone_from(&tap_types);
    }
}

pub struct DomainNameListener {
    stats_collector: Arc<stats::Collector>,
    session: Arc<Session>,
    ips: Vec<String>,
    domain_names: Vec<String>,
    team_id: String,

    sidecar_mode: bool,

    thread_handler: Option<JoinHandle<()>>,
    stopped: Arc<AtomicBool>,
    agent_id_tx: Arc<broadcast::Sender<AgentId>>,
}

impl DomainNameListener {
    const INTERVAL: Duration = Duration::from_secs(5);

    fn new(
        stats_collector: Arc<stats::Collector>,
        session: Arc<Session>,
        domain_names: Vec<String>,
        ips: Vec<String>,
        team_id: String,
        sidecar_mode: bool,
        agent_id_tx: Arc<broadcast::Sender<AgentId>>,
    ) -> DomainNameListener {
        Self {
            stats_collector,
            session,
            domain_names,
            ips,
            team_id,
            sidecar_mode,
            thread_handler: None,
            stopped: Arc::new(AtomicBool::new(false)),
            agent_id_tx,
        }
    }

    fn start(&mut self) {
        if self.thread_handler.is_some() {
            return;
        }
        self.stopped.store(false, Ordering::Relaxed);
        self.run();
    }

    fn stop(&mut self) {
        if self.thread_handler.is_none() {
            return;
        }
        self.stopped.store(true, Ordering::Relaxed);
        if let Some(handler) = self.thread_handler.take() {
            let _ = handler.join();
        }
    }

    fn run(&mut self) {
        if self.domain_names.len() == 0 {
            return;
        }

        let mut ips = self.ips.clone();
        let domain_names = self.domain_names.clone();
        let team_id = self.team_id.clone();
        let stopped = self.stopped.clone();
        let agent_id_tx = self.agent_id_tx.clone();
        let session = self.session.clone();

        #[cfg(target_os = "linux")]
        let sidecar_mode = self.sidecar_mode;

        info!(
            "Resolve controller domain name {} {}",
            domain_names[0], ips[0]
        );

        self.thread_handler = Some(
            thread::Builder::new()
                .name("domain-name-listener".to_owned())
                .spawn(move || {
                    while !stopped.swap(false, Ordering::Relaxed) {
                        thread::sleep(Self::INTERVAL);

                        let mut changed = false;
                        for i in 0..domain_names.len() {
                            let current = lookup_host(domain_names[i].as_str());
                            if current.is_err() {
                                continue;
                            }
                            let current = current.unwrap();

                            changed = current.iter().find(|&&x| x.to_string() == ips[i]).is_none();
                            if changed {
                                info!(
                                    "Domain name {} ip {} change to {}",
                                    domain_names[i], ips[i], current[0]
                                );
                                ips[i] = current[0].to_string();
                            }
                        }

                        if changed {
                            let (ctrl_ip, ctrl_mac) = match get_ctrl_ip_and_mac(&ips[0].parse().unwrap()) {
                                Ok(tuple) => tuple,
                                Err(e) => {
                                    warn!("get ctrl ip and mac failed with error: {}, deepflow-agent restart...", e);
                                    crate::utils::notify_exit(1);
                                    continue;
                                }
                            };
                            info!(
                                "use K8S_NODE_IP_FOR_DEEPFLOW env ip as destination_ip({})",
                                ctrl_ip
                            );
                            #[cfg(target_os = "linux")]
                            let agent_id = if sidecar_mode {
                                AgentId { ip: ctrl_ip.clone(), mac: ctrl_mac, team_id: team_id.clone() }
                            } else {
                                // use host ip/mac as agent id if not in sidecar mode
                                if let Err(e) = netns::open_named_and_setns(&netns::NsFile::Root) {
                                    warn!("agent must have CAP_SYS_ADMIN to run without 'hostNetwork: true'.");
                                    warn!("setns error: {}, deepflow-agent restart...", e);
                                    crate::utils::notify_exit(1);
                                    continue;
                                }
                                let (ip, mac) = match get_ctrl_ip_and_mac(&ips[0].parse().unwrap()) {
                                    Ok(tuple) => tuple,
                                    Err(e) => {
                                        warn!("get ctrl ip and mac failed with error: {}, deepflow-agent restart...", e);
                                        crate::utils::notify_exit(1);
                                        continue;
                                    }
                                };
                                if let Err(e) = netns::reset_netns() {
                                    warn!("reset setns error: {}, deepflow-agent restart...", e);
                                    crate::utils::notify_exit(1);
                                    continue;
                                }
                                AgentId { ip, mac, team_id: team_id.clone() }
                            };
                            #[cfg(any(target_os = "windows", target_os = "android"))]
                            let agent_id = AgentId { ip: ctrl_ip.clone(), mac: ctrl_mac, team_id: team_id.clone() };

                            session.reset_server_ip(ips.clone());
                            let _ = agent_id_tx.send(agent_id);
                        }
                    }
                })
                .unwrap(),
        );
    }
}

pub enum Components {
    Agent(AgentComponents),
    #[cfg(target_os = "linux")]
    Watcher(WatcherComponents),
    Other,
}

#[cfg(target_os = "linux")]
pub struct WatcherComponents {
    pub running: AtomicBool,
    capture_mode: PacketCaptureType,
    agent_mode: RunningMode,
    runtime: Arc<Runtime>,
}

#[cfg(target_os = "linux")]
impl WatcherComponents {
    fn new(
        config_handler: &ConfigHandler,
        agent_mode: RunningMode,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let candidate_config = &config_handler.candidate_config;
        info!("This agent will only watch K8s resource because IN_CONTAINER={} and K8S_WATCH_POLICY={}", env::var(IN_CONTAINER).unwrap_or_default(), env::var(K8S_WATCH_POLICY).unwrap_or_default());
        Ok(WatcherComponents {
            running: AtomicBool::new(false),
            capture_mode: candidate_config.capture_mode,
            agent_mode,
            runtime,
        })
    }

    fn start(&mut self) {
        if self.running.swap(true, Ordering::Relaxed) {
            return;
        }
        info!("Started watcher components.");
    }

    fn stop(&mut self) {
        if !self.running.swap(false, Ordering::Relaxed) {
            return;
        }
        info!("Stopped watcher components.")
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub struct EbpfDispatcherComponent {
    pub ebpf_collector: Box<EbpfCollector>,
    pub session_aggregator: SessionAggregator,
    pub l7_collector: L7CollectorThread,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
impl EbpfDispatcherComponent {
    pub fn start(&mut self) {
        self.session_aggregator.start();
        self.l7_collector.start();
        self.ebpf_collector.start();
    }

    pub fn stop(&mut self) {
        self.session_aggregator.stop();
        self.l7_collector.stop();
        self.ebpf_collector.notify_stop();
    }
}

pub struct MetricsServerComponent {
    pub external_metrics_server: MetricServer,
    pub l7_collector: L7CollectorThread,
}

impl MetricsServerComponent {
    pub fn start(&mut self) {
        self.external_metrics_server.start();
        self.l7_collector.start();
    }

    pub fn stop(&mut self) {
        self.external_metrics_server.stop();
        self.l7_collector.stop();
    }
}

pub struct DispatcherComponent {
    pub id: usize,
    pub dispatcher: Dispatcher,
    pub dispatcher_listener: DispatcherListener,
    pub session_aggregator: SessionAggregator,
    pub collector: CollectorThread,
    pub l7_collector: L7CollectorThread,
    pub packet_sequence_parser: PacketSequenceParser,
    pub pcap_assembler: PcapAssembler,
    pub handler_builders: Arc<RwLock<Vec<PacketHandlerBuilder>>>,
    pub src_link: Link, // The original src_interface
}

impl DispatcherComponent {
    pub fn start(&mut self) {
        self.dispatcher.start();
        self.session_aggregator.start();
        self.collector.start();
        self.l7_collector.start();
        self.packet_sequence_parser.start();
        self.pcap_assembler.start();
        self.handler_builders
            .write()
            .unwrap()
            .iter_mut()
            .for_each(|y| {
                y.start();
            });
    }
    pub fn stop(&mut self) {
        self.dispatcher.stop();
        self.session_aggregator.stop();
        self.collector.stop();
        self.l7_collector.stop();
        self.packet_sequence_parser.stop();
        self.pcap_assembler.stop();
        self.handler_builders
            .write()
            .unwrap()
            .iter_mut()
            .for_each(|y| {
                y.stop();
            });
    }
}

pub struct AgentComponents {
    pub config: ModuleConfig,
    pub rx_leaky_bucket: Arc<LeakyBucket>,
    pub tap_typer: Arc<CaptureNetworkTyper>,
    pub cur_tap_types: Vec<agent::CaptureNetworkType>,
    pub dispatcher_components: Vec<DispatcherComponent>,
    pub l4_flow_uniform_sender: UniformSenderThread<BoxedTaggedFlow>,
    pub metrics_uniform_sender: UniformSenderThread<BoxedDocument>,
    pub l7_flow_uniform_sender: UniformSenderThread<BoxAppProtoLogsData>,
    pub platform_synchronizer: Arc<PlatformSynchronizer>,
    #[cfg(target_os = "linux")]
    pub kubernetes_poller: Arc<GenericPoller>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub socket_synchronizer: SocketSynchronizer,
    pub debugger: Debugger,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub ebpf_dispatcher_component: Option<EbpfDispatcherComponent>,
    pub running: AtomicBool,
    pub stats_collector: Arc<stats::Collector>,
    pub metrics_server_component: MetricsServerComponent,
    pub otel_uniform_sender: UniformSenderThread<OpenTelemetry>,
    pub prometheus_uniform_sender: UniformSenderThread<BoxedPrometheusExtra>,
    pub telegraf_uniform_sender: UniformSenderThread<TelegrafMetric>,
    pub profile_uniform_sender: UniformSenderThread<Profile>,
    pub packet_sequence_uniform_output: DebugSender<BoxedPacketSequenceBlock>, // Enterprise Edition Feature: packet-sequence
    pub packet_sequence_uniform_sender: UniformSenderThread<BoxedPacketSequenceBlock>, // Enterprise Edition Feature: packet-sequence
    pub proc_event_uniform_sender: UniformSenderThread<BoxedProcEvents>,
    pub application_log_uniform_sender: UniformSenderThread<ApplicationLog>,
    pub skywalking_uniform_sender: UniformSenderThread<SkyWalkingExtra>,
    pub datadog_uniform_sender: UniformSenderThread<Datadog>,
    pub exception_handler: ExceptionHandler,
    pub proto_log_sender: DebugSender<BoxAppProtoLogsData>,
    pub pcap_batch_sender: DebugSender<BoxedPcapBatch>,
    pub toa_info_sender: DebugSender<Box<(SocketAddr, SocketAddr)>>,
    pub l4_flow_aggr_sender: DebugSender<BoxedTaggedFlow>,
    pub metrics_sender: DebugSender<BoxedDocument>,
    pub npb_bps_limit: Arc<LeakyBucket>,
    pub compressed_otel_uniform_sender: UniformSenderThread<OpenTelemetryCompressed>,
    pub pcap_batch_uniform_sender: UniformSenderThread<BoxedPcapBatch>,
    pub policy_setter: PolicySetter,
    pub policy_getter: PolicyGetter,
    pub npb_bandwidth_watcher: Box<Arc<NpbBandwidthWatcher>>,
    pub npb_arp_table: Arc<NpbArpTable>,
    pub vector_component: VectorComponent,
    pub is_ce_version: bool, // Determine whether the current version is a ce version, CE-AGENT always set pcap-assembler disabled
    pub tap_interfaces: Vec<Link>,
    pub bpf_options: Arc<Mutex<BpfOptions>>,
    pub last_dispatcher_component_id: usize,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub process_listener: Arc<ProcessListener>,
    max_memory: u64,
    capture_mode: PacketCaptureType,
    agent_mode: RunningMode,

    runtime: Arc<Runtime>,
}

impl AgentComponents {
    fn get_flowgen_tolerable_delay(config: &UserConfig) -> u64 {
        // FIXME: The flow_generator and dispatcher should be decoupled, and a delay function should be provided for this purpose.
        // The components of quadruple_generator's Delay are as follows:
        //   - Inherent delay in flow statistics data in flow_map: second_flow_extra_delay + packet_delay
        //   - Additional delay in inject_flush_ticker in flow_map: TIME_UNIT
        //   - Delay in flushing the output queue in flow_map: flow.flush_interval
        //   - Potential delay from other processing steps in flow_map: COMMON_DELAY 5 seconds
        //   - The delay caused by the time window being pushed ahead in flow_map: flow.flush_interval
        config
            .processors
            .flow_log
            .time_window
            .max_tolerable_packet_delay
            .as_secs()
            + TIME_UNIT.as_secs()
            + config
                .processors
                .flow_log
                .conntrack
                .flow_flush_interval
                .as_secs()
            + COMMON_DELAY
            + config
                .processors
                .flow_log
                .time_window
                .extra_tolerable_flow_delay
                .as_secs()
            + config
                .processors
                .flow_log
                .conntrack
                .flow_flush_interval
                .as_secs() // The flow_map may send data to qg ahead of time due to the output_buffer exceeding its limit. This can result in the time_window of qg being advanced prematurely, with the maximum advancement time being the flush_interval.
    }
    fn new_collector(
        id: usize,
        stats_collector: Arc<stats::Collector>,
        flow_receiver: queue::Receiver<Arc<BatchedBox<TaggedFlow>>>,
        toa_info_sender: DebugSender<Box<(SocketAddr, SocketAddr)>>,
        l4_flow_aggr_sender: Option<DebugSender<BoxedTaggedFlow>>,
        metrics_sender: DebugSender<BoxedDocument>,
        metrics_type: MetricsType,
        config_handler: &ConfigHandler,
        queue_debugger: &QueueDebugger,
        synchronizer: &Arc<Synchronizer>,
        agent_mode: RunningMode,
    ) -> CollectorThread {
        let config = &config_handler.candidate_config.user_config;

        let flowgen_tolerable_delay = Self::get_flowgen_tolerable_delay(config);
        // minute QG window is also pushed forward by flow stat time,
        // therefore its delay should be 60 + second delay (including extra flow delay)
        let minute_quadruple_tolerable_delay = 60 + flowgen_tolerable_delay;

        let mut l4_flow_aggr_outer = None;
        let mut l4_log_sender_outer = None;
        if l4_flow_aggr_sender.is_some() {
            let (l4_log_sender, l4_log_receiver, counter) = queue::bounded_with_debug(
                config
                    .processors
                    .flow_log
                    .tunning
                    .flow_aggregator_queue_size,
                "2-second-flow-to-minute-aggrer",
                queue_debugger,
            );
            l4_log_sender_outer = Some(l4_log_sender);
            stats_collector.register_countable(
                &QueueStats {
                    id,
                    module: "2-second-flow-to-minute-aggrer",
                },
                Countable::Owned(Box::new(counter)),
            );
            let (l4_flow_aggr, flow_aggr_counter) = FlowAggrThread::new(
                id,                                   // id
                l4_log_receiver,                      // input
                l4_flow_aggr_sender.unwrap().clone(), // output
                config_handler.collector(),
                Duration::from_secs(flowgen_tolerable_delay),
                synchronizer.ntp_diff(),
            );
            l4_flow_aggr_outer = Some(l4_flow_aggr);
            stats_collector.register_countable(
                &stats::SingleTagModule("flow_aggr", "index", id),
                Countable::Ref(Arc::downgrade(&flow_aggr_counter) as Weak<dyn RefCountable>),
            );
        }

        let (second_sender, second_receiver, counter) = queue::bounded_with_debug(
            config
                .processors
                .flow_log
                .tunning
                .quadruple_generator_queue_size,
            "2-flow-with-meter-to-second-collector",
            queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                id,
                module: "2-flow-with-meter-to-second-collector",
            },
            Countable::Owned(Box::new(counter)),
        );
        let (minute_sender, minute_receiver, counter) = queue::bounded_with_debug(
            config
                .processors
                .flow_log
                .tunning
                .quadruple_generator_queue_size,
            "2-flow-with-meter-to-minute-collector",
            queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                id,
                module: "2-flow-with-meter-to-minute-collector",
            },
            Countable::Owned(Box::new(counter)),
        );

        let quadruple_generator = QuadrupleGeneratorThread::new(
            id,
            flow_receiver,
            second_sender,
            minute_sender,
            toa_info_sender,
            l4_log_sender_outer,
            (config.processors.flow_log.tunning.flow_map_hash_slots as usize) << 3, // connection_lru_capacity
            metrics_type,
            flowgen_tolerable_delay,
            minute_quadruple_tolerable_delay,
            1 << 18, // possible_host_size
            config_handler.collector(),
            synchronizer.ntp_diff(),
            stats_collector.clone(),
        );

        let (mut second_collector, mut minute_collector) = (None, None);
        if metrics_type.contains(MetricsType::SECOND) {
            second_collector = Some(Collector::new(
                id as u32,
                second_receiver,
                metrics_sender.clone(),
                MetricsType::SECOND,
                flowgen_tolerable_delay + QG_PROCESS_MAX_DELAY,
                &stats_collector,
                config_handler.collector(),
                synchronizer.ntp_diff(),
                agent_mode,
            ));
        }
        if metrics_type.contains(MetricsType::MINUTE) {
            minute_collector = Some(Collector::new(
                id as u32,
                minute_receiver,
                metrics_sender,
                MetricsType::MINUTE,
                minute_quadruple_tolerable_delay + QG_PROCESS_MAX_DELAY,
                &stats_collector,
                config_handler.collector(),
                synchronizer.ntp_diff(),
                agent_mode,
            ));
        }

        CollectorThread::new(
            quadruple_generator,
            l4_flow_aggr_outer,
            second_collector,
            minute_collector,
        )
    }

    fn new_l7_collector(
        id: usize,
        stats_collector: Arc<stats::Collector>,
        l7_stats_receiver: queue::Receiver<BatchedBox<L7Stats>>,
        metrics_sender: DebugSender<BoxedDocument>,
        metrics_type: MetricsType,
        config_handler: &ConfigHandler,
        queue_debugger: &QueueDebugger,
        synchronizer: &Arc<Synchronizer>,
        agent_mode: RunningMode,
    ) -> L7CollectorThread {
        let user_config = &config_handler.candidate_config.user_config;

        let (l7_second_sender, l7_second_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .quadruple_generator_queue_size,
            "2-flow-with-meter-to-l7-second-collector",
            queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                id,
                module: "2-flow-with-meter-to-l7-second-collector",
            },
            Countable::Owned(Box::new(counter)),
        );
        let (l7_minute_sender, l7_minute_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .quadruple_generator_queue_size,
            "2-flow-with-meter-to-l7-minute-collector",
            queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                id,
                module: "2-flow-with-meter-to-l7-minute-collector",
            },
            Countable::Owned(Box::new(counter)),
        );

        let second_quadruple_tolerable_delay = Self::get_flowgen_tolerable_delay(user_config);
        // minute QG window is also pushed forward by flow stat time,
        // therefore its delay should be 60 + second delay (including extra flow delay)
        let minute_quadruple_tolerable_delay = 60 + second_quadruple_tolerable_delay;

        let quadruple_generator = L7QuadrupleGeneratorThread::new(
            id,
            l7_stats_receiver,
            l7_second_sender,
            l7_minute_sender,
            metrics_type,
            second_quadruple_tolerable_delay,
            minute_quadruple_tolerable_delay,
            1 << 18, // possible_host_size
            config_handler.collector(),
            synchronizer.ntp_diff(),
            stats_collector.clone(),
        );

        let (mut second_collector, mut minute_collector) = (None, None);
        if metrics_type.contains(MetricsType::SECOND) {
            second_collector = Some(L7Collector::new(
                id as u32,
                l7_second_receiver,
                metrics_sender.clone(),
                MetricsType::SECOND,
                second_quadruple_tolerable_delay + QG_PROCESS_MAX_DELAY,
                &stats_collector,
                config_handler.collector(),
                synchronizer.ntp_diff(),
                agent_mode,
            ));
        }
        if metrics_type.contains(MetricsType::MINUTE) {
            minute_collector = Some(L7Collector::new(
                id as u32,
                l7_minute_receiver,
                metrics_sender,
                MetricsType::MINUTE,
                minute_quadruple_tolerable_delay + QG_PROCESS_MAX_DELAY,
                &stats_collector,
                config_handler.collector(),
                synchronizer.ntp_diff(),
                agent_mode,
            ));
        }

        L7CollectorThread::new(quadruple_generator, second_collector, minute_collector)
    }

    fn new(
        version_info: &VersionInfo,
        config_handler: &ConfigHandler,
        stats_collector: Arc<stats::Collector>,
        session: &Arc<Session>,
        synchronizer: &Arc<Synchronizer>,
        exception_handler: ExceptionHandler,
        #[cfg(target_os = "linux")] libvirt_xml_extractor: Arc<LibvirtXmlExtractor>,
        platform_synchronizer: Arc<PlatformSynchronizer>,
        #[cfg(target_os = "linux")] sidecar_poller: Option<Arc<GenericPoller>>,
        #[cfg(target_os = "linux")] api_watcher: Arc<ApiWatcher>,
        vm_mac_addrs: Vec<MacAddr>,
        gateway_vmac_addrs: Vec<MacAddr>,
        agent_mode: RunningMode,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let static_config = &config_handler.static_config;
        let candidate_config = &config_handler.candidate_config;
        let user_config = &candidate_config.user_config;
        let ctrl_ip = config_handler.ctrl_ip;
        let max_memory = config_handler.candidate_config.environment.max_memory;
        let process_threshold = config_handler
            .candidate_config
            .environment
            .process_threshold;
        let feature_flags = FeatureFlags::from(&user_config.dev.feature_flags);

        if !user_config.inputs.cbpf.af_packet.src_interfaces.is_empty()
            && user_config.inputs.cbpf.special_network.dpdk.source == DpdkSource::None
        {
            warn!("src_interfaces is not empty, but this has already been deprecated, instead, the tap_interface_regex should be set");
        }

        info!("Start check process...");
        trident_process_check(process_threshold);
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if !user_config.global.alerts.check_core_file_disabled {
            info!("Start check core file...");
            core_file_check();
        }
        info!("Start check controller ip...");
        controller_ip_check(&static_config.controller_ips);
        info!("Start check free space...");
        check(free_space_checker(
            &static_config.log_file,
            FREE_SPACE_REQUIREMENT,
            exception_handler.clone(),
        ));

        #[cfg(target_os = "linux")]
        let mut interfaces_and_ns: Vec<(Vec<Link>, netns::NsFile)> = vec![];
        #[cfg(any(target_os = "windows", target_os = "android"))]
        let mut interfaces_and_ns: Vec<Vec<Link>> = vec![];

        #[cfg(target_os = "linux")]
        if candidate_config.dispatcher.extra_netns_regex != "" {
            if candidate_config.capture_mode == PacketCaptureType::Local {
                let re = regex::Regex::new(&candidate_config.dispatcher.extra_netns_regex).unwrap();
                let mut nss = netns::find_ns_files_by_regex(&re);
                nss.sort_unstable();
                for ns in nss.into_iter() {
                    let links = get_listener_links(&candidate_config.dispatcher, &ns);
                    if !links.is_empty() {
                        interfaces_and_ns.push((links, ns));
                    }
                }
            } else {
                log::error!("When the PacketCaptureType is not Local, it does not support extra_netns_regex, other modes only support interfaces under the root network namespace");
            }
        }

        #[cfg(target_os = "linux")]
        let mut packet_fanout_count = if candidate_config.dispatcher.extra_netns_regex == "" {
            user_config
                .inputs
                .cbpf
                .af_packet
                .tunning
                .packet_fanout_count
        } else {
            1
        };
        #[cfg(any(target_os = "windows", target_os = "android"))]
        let packet_fanout_count = 1;

        let links = get_listener_links(
            &candidate_config.dispatcher,
            #[cfg(target_os = "linux")]
            &netns::NsFile::Root,
        );
        if interfaces_and_ns.is_empty() && !links.is_empty() {
            if packet_fanout_count > 1 || candidate_config.capture_mode == PacketCaptureType::Local
            {
                for _ in 0..packet_fanout_count {
                    #[cfg(target_os = "linux")]
                    interfaces_and_ns.push((links.clone(), netns::NsFile::Root));
                    #[cfg(any(target_os = "windows", target_os = "android"))]
                    interfaces_and_ns.push(links.clone());
                }
            } else {
                for l in links {
                    #[cfg(target_os = "linux")]
                    interfaces_and_ns.push((vec![l], netns::NsFile::Root));
                    #[cfg(any(target_os = "windows", target_os = "android"))]
                    interfaces_and_ns.push(vec![l]);
                }
            }
        }
        #[cfg(target_os = "linux")]
        if candidate_config.capture_mode != PacketCaptureType::Local
            && (!user_config
                .inputs
                .cbpf
                .special_network
                .vhost_user
                .vhost_socket_path
                .is_empty()
                || candidate_config.dispatcher.dpdk_source != DpdkSource::None)
        {
            packet_fanout_count = 1;
            interfaces_and_ns = vec![(vec![], netns::NsFile::Root)];
        }

        match candidate_config.capture_mode {
            PacketCaptureType::Analyzer => {
                info!("Start check kernel...");
                kernel_check();
                if candidate_config
                    .user_config
                    .inputs
                    .cbpf
                    .special_network
                    .dpdk
                    .source
                    == DpdkSource::None
                {
                    info!("Start check tap interface...");
                    #[cfg(target_os = "linux")]
                    let tap_interfaces: Vec<_> = interfaces_and_ns
                        .iter()
                        .filter_map(|i| i.0.get(0).map(|l| l.name.clone()))
                        .collect();
                    #[cfg(any(target_os = "windows", target_os = "android"))]
                    let tap_interfaces: Vec<_> = interfaces_and_ns
                        .iter()
                        .filter_map(|i| i.get(0).map(|l| l.name.clone()))
                        .collect();

                    tap_interface_check(&tap_interfaces);
                }
            }
            _ => {
                // NPF服务检查
                // TODO: npf (only on windows)
                if candidate_config.capture_mode == PacketCaptureType::Mirror {
                    info!("Start check kernel...");
                    kernel_check();
                }
            }
        }

        info!("Agent run with feature-flags: {:?}.", feature_flags);
        // Currently, only loca-mode + ebpf collector is supported, and ebpf collector is not
        // applicable to fastpath, so the number of queues is 1
        // =================================================================================
        // 目前仅支持local-mode + ebpf-collector，ebpf-collector不适用fastpath, 所以队列数为1
        let (policy_setter, policy_getter) = Policy::new(
            1.max(
                if candidate_config.capture_mode != PacketCaptureType::Local {
                    interfaces_and_ns.len()
                } else {
                    1
                },
            ),
            user_config.processors.packet.policy.max_first_path_level,
            user_config.get_fast_path_map_size(candidate_config.dispatcher.max_memory),
            user_config.processors.packet.policy.forward_table_capacity,
            user_config.processors.packet.policy.fast_path_disabled,
            candidate_config.capture_mode == PacketCaptureType::Analyzer,
        );
        synchronizer.add_flow_acl_listener(Box::new(policy_setter));
        policy_setter.set_memory_limit(max_memory);

        // TODO: collector enabled
        // TODO: packet handler builders

        #[cfg(target_os = "linux")]
        // sidecar poller is created before agent start to provide pod interface info for server
        let kubernetes_poller = sidecar_poller.unwrap_or_else(|| {
            let poller = Arc::new(GenericPoller::new(
                config_handler.platform(),
                config_handler
                    .candidate_config
                    .dispatcher
                    .extra_netns_regex
                    .clone(),
            ));
            platform_synchronizer.set_kubernetes_poller(poller.clone());
            poller
        });

        let context = ConstructDebugCtx {
            runtime: runtime.clone(),
            #[cfg(target_os = "linux")]
            api_watcher: api_watcher.clone(),
            #[cfg(target_os = "linux")]
            poller: kubernetes_poller.clone(),
            session: session.clone(),
            static_config: synchronizer.static_config.clone(),
            agent_id: synchronizer.agent_id.clone(),
            status: synchronizer.status.clone(),
            config: config_handler.debug(),
            policy_setter,
        };
        let debugger = Debugger::new(context);
        let queue_debugger = debugger.clone_queue();
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let process_listener = Arc::new(ProcessListener::new(
            &candidate_config.user_config.inputs.proc.process_matcher,
            candidate_config
                .user_config
                .inputs
                .proc
                .proc_dir_path
                .clone(),
            candidate_config
                .user_config
                .inputs
                .proc
                .tag_extraction
                .exec_username
                .clone(),
            candidate_config
                .user_config
                .inputs
                .proc
                .tag_extraction
                .script_command
                .clone(),
        ));
        #[cfg(any(target_os = "linux", target_os = "android"))]
        platform_synchronizer.set_process_listener(&process_listener);

        #[cfg(any(target_os = "linux", target_os = "android"))]
        let (toa_sender, toa_recv, _) = queue::bounded_with_debug(
            user_config.processors.packet.toa.sender_queue_size,
            "1-socket-sync-toa-info-queue",
            &queue_debugger,
        );
        #[cfg(target_os = "windows")]
        let (toa_sender, _, _) = queue::bounded_with_debug(
            user_config.processors.packet.toa.sender_queue_size,
            "1-socket-sync-toa-info-queue",
            &queue_debugger,
        );
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let socket_synchronizer = SocketSynchronizer::new(
            runtime.clone(),
            config_handler.platform(),
            synchronizer.agent_id.clone(),
            Arc::new(Mutex::new(policy_getter)),
            policy_setter,
            session.clone(),
            toa_recv,
            Arc::new(Mutex::new(Lru::with_capacity(
                user_config.processors.packet.toa.cache_size >> 5,
                user_config.processors.packet.toa.cache_size,
            ))),
            process_listener.clone(),
        );

        let rx_leaky_bucket = Arc::new(LeakyBucket::new(match candidate_config.capture_mode {
            PacketCaptureType::Analyzer => None,
            _ => Some(
                config_handler
                    .candidate_config
                    .dispatcher
                    .global_pps_threshold,
            ),
        }));

        let tap_typer = Arc::new(CaptureNetworkTyper::new());

        // TODO: collector enabled
        let mut dispatcher_components = vec![];

        // Sender/Collector
        info!(
            "static analyzer ip: '{}' actual analyzer ip '{}'",
            user_config.global.communication.ingester_ip, candidate_config.sender.dest_ip
        );
        let l4_flow_aggr_queue_name = "3-flowlog-to-collector-sender";
        let (l4_flow_aggr_sender, l4_flow_aggr_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .flow_generator_queue_size,
            l4_flow_aggr_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: l4_flow_aggr_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let l4_flow_uniform_sender = UniformSenderThread::new(
            l4_flow_aggr_queue_name,
            Arc::new(l4_flow_aggr_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            SenderEncoder::Raw,
        );

        let metrics_queue_name = "3-doc-to-collector-sender";
        let (metrics_sender, metrics_receiver, counter) = queue::bounded_with_debug(
            user_config.outputs.flow_metrics.tunning.sender_queue_size,
            metrics_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: metrics_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let metrics_uniform_sender = UniformSenderThread::new(
            metrics_queue_name,
            Arc::new(metrics_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            SenderEncoder::Raw,
        );

        let proto_log_queue_name = "2-protolog-to-collector-sender";
        let (proto_log_sender, proto_log_receiver, counter) = queue::bounded_with_debug(
            user_config.outputs.flow_log.tunning.collector_queue_size,
            proto_log_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: proto_log_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let l7_flow_uniform_sender = UniformSenderThread::new(
            proto_log_queue_name,
            Arc::new(proto_log_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            SenderEncoder::Raw,
        );

        let analyzer_ip = if candidate_config
            .dispatcher
            .analyzer_ip
            .parse::<IpAddr>()
            .is_ok()
        {
            candidate_config
                .dispatcher
                .analyzer_ip
                .parse::<IpAddr>()
                .unwrap()
        } else {
            let ips = lookup_host(&candidate_config.dispatcher.analyzer_ip)?;
            ips[0]
        };

        // Dispatcher
        let source_ip = match get_route_src_ip(&analyzer_ip) {
            Ok(ip) => ip,
            Err(e) => {
                warn!("get route to '{}' failed: {:?}", &analyzer_ip, e);
                if ctrl_ip.is_ipv6() {
                    Ipv6Addr::UNSPECIFIED.into()
                } else {
                    Ipv4Addr::UNSPECIFIED.into()
                }
            }
        };

        let npb_bps_limit = Arc::new(LeakyBucket::new(Some(
            config_handler.candidate_config.sender.npb_bps_threshold,
        )));
        let npb_arp_table = Arc::new(NpbArpTable::new(
            config_handler.candidate_config.npb.socket_type == SocketType::RawUdp,
            exception_handler.clone(),
        ));

        let pcap_batch_queue = "2-pcap-batch-to-sender";
        let (pcap_batch_sender, pcap_batch_receiver, pcap_batch_counter) =
            queue::bounded_with_debug(
                user_config
                    .processors
                    .packet
                    .pcap_stream
                    .receiver_queue_size,
                pcap_batch_queue,
                &queue_debugger,
            );
        stats_collector.register_countable(
            &QueueStats {
                module: pcap_batch_queue,
                ..Default::default()
            },
            Countable::Owned(Box::new(pcap_batch_counter)),
        );

        let pcap_packet_shared_connection = Arc::new(Mutex::new(Connection::new()));

        let pcap_batch_uniform_sender = UniformSenderThread::new(
            pcap_batch_queue,
            Arc::new(pcap_batch_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            Some(pcap_packet_shared_connection.clone()),
            SenderEncoder::Raw,
        );
        // Enterprise Edition Feature: packet-sequence
        let packet_sequence_queue_name = "2-packet-sequence-block-to-sender";
        let (packet_sequence_uniform_output, packet_sequence_uniform_input, counter) =
            queue::bounded_with_debug(
                user_config.processors.packet.tcp_header.sender_queue_size,
                packet_sequence_queue_name,
                &queue_debugger,
            );

        stats_collector.register_countable(
            &QueueStats {
                module: packet_sequence_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );

        let packet_sequence_uniform_sender = UniformSenderThread::new(
            packet_sequence_queue_name,
            Arc::new(packet_sequence_uniform_input),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            Some(pcap_packet_shared_connection),
            SenderEncoder::Raw,
        );

        let bpf_builder = bpf::Builder {
            is_ipv6: ctrl_ip.is_ipv6(),
            vxlan_flags: user_config.outputs.npb.custom_vxlan_flags,
            npb_port: user_config.outputs.npb.target_port,
            controller_port: static_config.controller_port,
            controller_tls_port: static_config.controller_tls_port,
            proxy_controller_port: candidate_config.dispatcher.proxy_controller_port,
            analyzer_source_ip: source_ip,
            analyzer_port: candidate_config.dispatcher.analyzer_port,
        };
        let bpf_syntax_str = bpf_builder.build_pcap_syntax_to_str();
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let bpf_syntax = bpf_builder.build_pcap_syntax();

        let bpf_options = Arc::new(Mutex::new(BpfOptions {
            capture_bpf: candidate_config.dispatcher.capture_bpf.clone(),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            bpf_syntax,
            bpf_syntax_str,
        }));

        #[cfg(any(target_os = "linux", target_os = "android"))]
        let queue_size = config_handler.ebpf().load().queue_size;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let queue_name = "0-ebpf-dpdk-to-dispatcher";
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let (dpdk_ebpf_sender, dpdk_ebpf_receiver, counter) =
            queue::bounded_with_debug(queue_size, queue_name, &queue_debugger);
        #[cfg(any(target_os = "linux", target_os = "android"))]
        stats_collector.register_countable(
            &stats::QueueStats {
                id: 0,
                module: queue_name,
            },
            Countable::Owned(Box::new(counter)),
        );
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let mut dpdk_ebpf_receiver = Some(dpdk_ebpf_receiver);

        let mut tap_interfaces = vec![];
        for (i, entry) in interfaces_and_ns.into_iter().enumerate() {
            #[cfg(target_os = "linux")]
            let links = entry.0;
            #[cfg(any(target_os = "windows", target_os = "android"))]
            let links = entry;
            tap_interfaces.extend(links.clone());
            #[cfg(target_os = "linux")]
            let netns = entry.1;
            let dispatcher_component = build_dispatchers(
                i,
                links,
                stats_collector.clone(),
                config_handler,
                queue_debugger.clone(),
                version_info.name != env!("AGENT_NAME"),
                synchronizer,
                npb_bps_limit.clone(),
                npb_arp_table.clone(),
                rx_leaky_bucket.clone(),
                policy_getter,
                exception_handler.clone(),
                bpf_options.clone(),
                packet_sequence_uniform_output.clone(),
                proto_log_sender.clone(),
                pcap_batch_sender.clone(),
                tap_typer.clone(),
                vm_mac_addrs.clone(),
                gateway_vmac_addrs.clone(),
                toa_sender.clone(),
                l4_flow_aggr_sender.clone(),
                metrics_sender.clone(),
                #[cfg(target_os = "linux")]
                netns,
                #[cfg(target_os = "linux")]
                kubernetes_poller.clone(),
                #[cfg(target_os = "linux")]
                libvirt_xml_extractor.clone(),
                #[cfg(target_os = "linux")]
                dpdk_ebpf_receiver.take(),
                #[cfg(target_os = "linux")]
                {
                    packet_fanout_count > 1
                },
            )?;
            dispatcher_components.push(dispatcher_component);
        }
        tap_interfaces.sort();
        let proc_event_queue_name = "1-proc-event-to-sender";
        #[allow(unused)]
        let (proc_event_sender, proc_event_receiver, counter) = queue::bounded_with_debug(
            user_config.inputs.ebpf.tunning.collector_queue_size,
            proc_event_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: proc_event_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let proc_event_uniform_sender = UniformSenderThread::new(
            proc_event_queue_name,
            Arc::new(proc_event_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            SenderEncoder::Raw,
        );

        let profile_queue_name = "1-profile-to-sender";
        let (profile_sender, profile_receiver, counter) = queue::bounded_with_debug(
            user_config.inputs.ebpf.tunning.collector_queue_size,
            profile_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: profile_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let profile_uniform_sender = UniformSenderThread::new(
            profile_queue_name,
            Arc::new(profile_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            // profiler compress is a special one, it requires compressed and directly write into db
            // so we compress profile data inside and not compress secondly
            SenderEncoder::Raw,
        );
        let application_log_queue_name = "1-application-log-to-sender";
        let (application_log_sender, application_log_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .flow_aggregator_queue_size,
            application_log_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: application_log_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let application_log_uniform_sender = UniformSenderThread::new(
            application_log_queue_name,
            Arc::new(application_log_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            if candidate_config.metric_server.application_log_compressed {
                SenderEncoder::Zlib
            } else {
                SenderEncoder::Raw
            },
        );

        let skywalking_queue_name = "1-skywalking-to-sender";
        let (skywalking_sender, skywalking_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .flow_aggregator_queue_size,
            skywalking_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: skywalking_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let skywalking_uniform_sender = UniformSenderThread::new(
            skywalking_queue_name,
            Arc::new(skywalking_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            if candidate_config.metric_server.compressed {
                SenderEncoder::Zlib
            } else {
                SenderEncoder::Raw
            },
        );

        let datadog_queue_name = "1-datadog-to-sender";
        let (datadog_sender, datadog_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .flow_aggregator_queue_size,
            datadog_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: datadog_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let datadog_uniform_sender = UniformSenderThread::new(
            datadog_queue_name,
            Arc::new(datadog_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            if candidate_config.metric_server.compressed {
                SenderEncoder::Zlib
            } else {
                SenderEncoder::Raw
            },
        );

        let ebpf_dispatcher_id = dispatcher_components.len();
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let mut ebpf_dispatcher_component = None;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if !config_handler.ebpf().load().ebpf.disabled
            && (candidate_config.capture_mode != PacketCaptureType::Analyzer
                || candidate_config
                    .user_config
                    .inputs
                    .cbpf
                    .special_network
                    .dpdk
                    .source
                    == DpdkSource::Ebpf)
        {
            let (l7_stats_sender, l7_stats_receiver, counter) = queue::bounded_with_debug(
                user_config
                    .processors
                    .flow_log
                    .tunning
                    .flow_generator_queue_size,
                "1-l7-stats-to-quadruple-generator",
                &queue_debugger,
            );
            stats_collector.register_countable(
                &QueueStats {
                    id: ebpf_dispatcher_id,
                    module: "1-l7-stats-to-quadruple-generator",
                },
                Countable::Owned(Box::new(counter)),
            );
            let (log_sender, log_receiver, counter) = queue::bounded_with_debug(
                user_config
                    .processors
                    .flow_log
                    .tunning
                    .flow_generator_queue_size,
                "1-tagged-flow-to-app-protocol-logs",
                &queue_debugger,
            );
            stats_collector.register_countable(
                &QueueStats {
                    id: ebpf_dispatcher_id,
                    module: "1-tagged-flow-to-app-protocol-logs",
                },
                Countable::Owned(Box::new(counter)),
            );
            let (session_aggregator, counter) = SessionAggregator::new(
                log_receiver,
                proto_log_sender.clone(),
                ebpf_dispatcher_id as u32,
                config_handler.log_parser(),
                synchronizer.ntp_diff(),
            );
            stats_collector.register_countable(
                &stats::SingleTagModule("l7_session_aggr", "index", ebpf_dispatcher_id),
                Countable::Ref(Arc::downgrade(&counter) as Weak<dyn RefCountable>),
            );
            let l7_collector = Self::new_l7_collector(
                ebpf_dispatcher_id,
                stats_collector.clone(),
                l7_stats_receiver,
                metrics_sender.clone(),
                MetricsType::SECOND | MetricsType::MINUTE,
                config_handler,
                &queue_debugger,
                &synchronizer,
                agent_mode,
            );
            match EbpfCollector::new(
                ebpf_dispatcher_id,
                synchronizer.ntp_diff(),
                config_handler.ebpf(),
                config_handler.log_parser(),
                config_handler.flow(),
                config_handler.collector(),
                policy_getter,
                dpdk_ebpf_sender,
                log_sender,
                l7_stats_sender,
                proc_event_sender,
                profile_sender.clone(),
                &queue_debugger,
                stats_collector.clone(),
                exception_handler.clone(),
                &process_listener,
            ) {
                Ok(ebpf_collector) => {
                    synchronizer
                        .add_flow_acl_listener(Box::new(ebpf_collector.get_sync_dispatcher()));
                    stats_collector.register_countable(
                        &stats::NoTagModule("ebpf-collector"),
                        Countable::Owned(Box::new(ebpf_collector.get_sync_counter())),
                    );
                    ebpf_dispatcher_component = Some(EbpfDispatcherComponent {
                        ebpf_collector,
                        session_aggregator,
                        l7_collector,
                    });
                }
                Err(e) => {
                    log::error!("ebpf collector error: {:?}", e);
                }
            };
        }

        let otel_queue_name = "1-otel-to-sender";
        let (otel_sender, otel_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .flow_aggregator_queue_size,
            otel_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: otel_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let otel_uniform_sender = UniformSenderThread::new(
            otel_queue_name,
            Arc::new(otel_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            if candidate_config.metric_server.compressed {
                SenderEncoder::Zlib
            } else {
                SenderEncoder::Raw
            },
        );

        let otel_dispatcher_id = ebpf_dispatcher_id + 1;

        let (l7_stats_sender, l7_stats_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .flow_generator_queue_size,
            "1-l7-stats-to-quadruple-generator",
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                id: otel_dispatcher_id,
                module: "1-l7-stats-to-quadruple-generator",
            },
            Countable::Owned(Box::new(counter)),
        );
        let l7_collector = Self::new_l7_collector(
            otel_dispatcher_id,
            stats_collector.clone(),
            l7_stats_receiver,
            metrics_sender.clone(),
            MetricsType::SECOND | MetricsType::MINUTE,
            config_handler,
            &queue_debugger,
            &synchronizer,
            agent_mode,
        );

        let prometheus_queue_name = "1-prometheus-to-sender";
        let (prometheus_sender, prometheus_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .flow_aggregator_queue_size,
            prometheus_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: prometheus_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );

        let prometheus_telegraf_shared_connection = Arc::new(Mutex::new(Connection::new()));
        let prometheus_uniform_sender = UniformSenderThread::new(
            prometheus_queue_name,
            Arc::new(prometheus_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            Some(prometheus_telegraf_shared_connection.clone()),
            SenderEncoder::Raw,
        );

        let telegraf_queue_name = "1-telegraf-to-sender";
        let (telegraf_sender, telegraf_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .flow_aggregator_queue_size,
            telegraf_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: telegraf_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let telegraf_uniform_sender = UniformSenderThread::new(
            telegraf_queue_name,
            Arc::new(telegraf_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            Some(prometheus_telegraf_shared_connection),
            SenderEncoder::Raw,
        );

        let compressed_otel_queue_name = "1-compressed-otel-to-sender";
        let (compressed_otel_sender, compressed_otel_receiver, counter) = queue::bounded_with_debug(
            user_config
                .processors
                .flow_log
                .tunning
                .flow_aggregator_queue_size,
            compressed_otel_queue_name,
            &queue_debugger,
        );
        stats_collector.register_countable(
            &QueueStats {
                module: compressed_otel_queue_name,
                ..Default::default()
            },
            Countable::Owned(Box::new(counter)),
        );
        let compressed_otel_uniform_sender = UniformSenderThread::new(
            compressed_otel_queue_name,
            Arc::new(compressed_otel_receiver),
            config_handler.sender(),
            stats_collector.clone(),
            exception_handler.clone(),
            None,
            SenderEncoder::Raw,
        );

        let (external_metrics_server, external_metrics_counter) = MetricServer::new(
            runtime.clone(),
            otel_sender,
            compressed_otel_sender,
            l7_stats_sender,
            prometheus_sender,
            telegraf_sender,
            profile_sender,
            application_log_sender,
            skywalking_sender,
            datadog_sender,
            candidate_config.metric_server.port,
            exception_handler.clone(),
            candidate_config.metric_server.compressed,
            candidate_config.metric_server.profile_compressed,
            candidate_config.platform.epc_id,
            policy_getter,
            synchronizer.ntp_diff(),
            user_config
                .inputs
                .integration
                .prometheus_extra_labels
                .clone(),
            candidate_config.log_parser.clone(),
            user_config
                .inputs
                .integration
                .feature_control
                .profile_integration_disabled,
            user_config
                .inputs
                .integration
                .feature_control
                .trace_integration_disabled,
            user_config
                .inputs
                .integration
                .feature_control
                .metric_integration_disabled,
            user_config
                .inputs
                .integration
                .feature_control
                .log_integration_disabled,
        );

        stats_collector.register_countable(
            &stats::NoTagModule("integration_collector"),
            Countable::Owned(Box::new(external_metrics_counter)),
        );

        let sender_config = config_handler.sender().load();
        let (npb_bandwidth_watcher, npb_bandwidth_watcher_counter) = NpbBandwidthWatcher::new(
            sender_config.bandwidth_probe_interval.as_secs(),
            sender_config.npb_bps_threshold,
            sender_config.server_tx_bandwidth_threshold,
            npb_bps_limit.clone(),
            exception_handler.clone(),
        );
        synchronizer.add_flow_acl_listener(npb_bandwidth_watcher.clone());
        stats_collector.register_countable(
            &stats::NoTagModule("npb_bandwidth_watcher"),
            Countable::Ref(Arc::downgrade(&npb_bandwidth_watcher_counter) as Weak<dyn RefCountable>),
        );
        let vector_component = VectorComponent::new(
            user_config.inputs.vector.enabled,
            user_config.inputs.vector.config.clone(),
            runtime.clone(),
        );

        Ok(AgentComponents {
            config: candidate_config.clone(),
            rx_leaky_bucket,
            tap_typer,
            cur_tap_types: vec![],
            l4_flow_uniform_sender,
            metrics_uniform_sender,
            l7_flow_uniform_sender,
            platform_synchronizer,
            #[cfg(target_os = "linux")]
            kubernetes_poller,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            socket_synchronizer,
            debugger,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            ebpf_dispatcher_component,
            stats_collector,
            running: AtomicBool::new(false),
            metrics_server_component: MetricsServerComponent {
                external_metrics_server,
                l7_collector,
            },
            exception_handler,
            max_memory,
            otel_uniform_sender,
            prometheus_uniform_sender,
            telegraf_uniform_sender,
            profile_uniform_sender,
            proc_event_uniform_sender,
            application_log_uniform_sender,
            skywalking_uniform_sender,
            datadog_uniform_sender,
            capture_mode: candidate_config.capture_mode,
            packet_sequence_uniform_output, // Enterprise Edition Feature: packet-sequence
            packet_sequence_uniform_sender, // Enterprise Edition Feature: packet-sequence
            npb_bps_limit,
            compressed_otel_uniform_sender,
            pcap_batch_uniform_sender,
            proto_log_sender,
            pcap_batch_sender,
            toa_info_sender: toa_sender,
            l4_flow_aggr_sender,
            metrics_sender,
            agent_mode,
            policy_setter,
            policy_getter,
            npb_bandwidth_watcher,
            npb_arp_table,
            vector_component,
            runtime,
            dispatcher_components,
            is_ce_version: version_info.name != env!("AGENT_NAME"),
            tap_interfaces,
            last_dispatcher_component_id: otel_dispatcher_id,
            bpf_options,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            process_listener,
        })
    }

    pub fn clear_dispatcher_components(&mut self) {
        self.dispatcher_components.iter_mut().for_each(|d| d.stop());
        self.dispatcher_components.clear();
        self.tap_interfaces.clear();
    }

    fn start(&mut self) {
        if self.running.swap(true, Ordering::Relaxed) {
            return;
        }
        info!("Starting agent components.");
        self.stats_collector.start();

        #[cfg(any(target_os = "linux", target_os = "android"))]
        self.socket_synchronizer.start();
        #[cfg(target_os = "linux")]
        if crate::utils::environment::is_tt_pod(self.config.agent_type) {
            self.kubernetes_poller.start();
        }
        self.debugger.start();
        self.metrics_uniform_sender.start();
        self.l7_flow_uniform_sender.start();
        self.l4_flow_uniform_sender.start();

        // Enterprise Edition Feature: packet-sequence
        self.packet_sequence_uniform_sender.start();

        // When capture_mode is Analyzer mode and agent is not running in container and agent
        // in the environment where cgroup is not supported, we need to check free memory
        if self.capture_mode != PacketCaptureType::Analyzer
            && !running_in_container()
            && !is_kernel_available_for_cgroups()
        {
            match free_memory_check(self.max_memory, &self.exception_handler) {
                Ok(()) => {
                    for d in self.dispatcher_components.iter_mut() {
                        d.start();
                    }
                }
                Err(e) => {
                    warn!("{}", e);
                }
            }
        } else {
            for d in self.dispatcher_components.iter_mut() {
                d.start();
            }
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(ebpf_dispatcher_component) = self.ebpf_dispatcher_component.as_mut() {
            ebpf_dispatcher_component.start();
        }
        if matches!(self.agent_mode, RunningMode::Managed) {
            self.otel_uniform_sender.start();
            self.compressed_otel_uniform_sender.start();
            self.prometheus_uniform_sender.start();
            self.telegraf_uniform_sender.start();
            self.profile_uniform_sender.start();
            self.proc_event_uniform_sender.start();
            self.application_log_uniform_sender.start();
            self.skywalking_uniform_sender.start();
            self.datadog_uniform_sender.start();
            if self.config.metric_server.enabled {
                self.metrics_server_component.start();
            }
            self.pcap_batch_uniform_sender.start();
        }

        self.npb_bandwidth_watcher.start();
        self.npb_arp_table.start();
        self.vector_component.start();
        #[cfg(any(target_os = "linux", target_os = "android"))]
        self.process_listener.start();
        info!("Started agent components.");
    }

    fn stop(&mut self) {
        if !self.running.swap(false, Ordering::Relaxed) {
            return;
        }

        let mut join_handles = vec![];

        self.policy_setter.reset_queue_size(0);
        for d in self.dispatcher_components.iter_mut() {
            d.stop();
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        self.socket_synchronizer.stop();
        #[cfg(target_os = "linux")]
        self.kubernetes_poller.stop();

        if let Some(h) = self.l4_flow_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.metrics_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.l7_flow_uniform_sender.notify_stop() {
            join_handles.push(h);
        }

        self.debugger.stop();

        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(d) = self.ebpf_dispatcher_component.as_mut() {
            d.stop();
        }

        self.metrics_server_component.stop();
        if let Some(h) = self.otel_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.compressed_otel_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.prometheus_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.telegraf_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.profile_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.proc_event_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.pcap_batch_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.application_log_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.skywalking_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.datadog_uniform_sender.notify_stop() {
            join_handles.push(h);
        }
        // Enterprise Edition Feature: packet-sequence
        if let Some(h) = self.packet_sequence_uniform_sender.notify_stop() {
            join_handles.push(h);
        }

        if let Some(h) = self.npb_bandwidth_watcher.notify_stop() {
            join_handles.push(h);
        }

        if let Some(h) = self.npb_arp_table.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.stats_collector.notify_stop() {
            join_handles.push(h);
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(h) = self.process_listener.notify_stop() {
            join_handles.push(h);
        }
        if let Some(h) = self.vector_component.notify_stop() {
            join_handles.push(h);
        }

        for handle in join_handles {
            if !handle.is_finished() {
                info!(
                    "wait for {} to fully stop",
                    handle.thread().name().unwrap_or("unnamed thread")
                );
            }
            let _ = handle.join();
        }

        info!("Stopped agent components.")
    }
}

impl Components {
    fn start(&mut self) {
        match self {
            Self::Agent(a) => a.start(),
            #[cfg(target_os = "linux")]
            Self::Watcher(w) => w.start(),
            _ => {}
        }
    }

    fn new(
        version_info: &VersionInfo,
        config_handler: &ConfigHandler,
        stats_collector: Arc<stats::Collector>,
        session: &Arc<Session>,
        synchronizer: &Arc<Synchronizer>,
        exception_handler: ExceptionHandler,
        #[cfg(target_os = "linux")] libvirt_xml_extractor: Arc<LibvirtXmlExtractor>,
        platform_synchronizer: Arc<PlatformSynchronizer>,
        #[cfg(target_os = "linux")] sidecar_poller: Option<Arc<GenericPoller>>,
        #[cfg(target_os = "linux")] api_watcher: Arc<ApiWatcher>,
        vm_mac_addrs: Vec<MacAddr>,
        gateway_vmac_addrs: Vec<MacAddr>,
        agent_mode: RunningMode,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        #[cfg(target_os = "linux")]
        if crate::utils::environment::running_in_only_watch_k8s_mode() {
            let components = WatcherComponents::new(config_handler, agent_mode, runtime)?;
            return Ok(Components::Watcher(components));
        }
        let components = AgentComponents::new(
            version_info,
            config_handler,
            stats_collector,
            session,
            synchronizer,
            exception_handler,
            #[cfg(target_os = "linux")]
            libvirt_xml_extractor,
            platform_synchronizer,
            #[cfg(target_os = "linux")]
            sidecar_poller,
            #[cfg(target_os = "linux")]
            api_watcher,
            vm_mac_addrs,
            gateway_vmac_addrs,
            agent_mode,
            runtime,
        )?;
        return Ok(Components::Agent(components));
    }

    fn stop(&mut self) {
        match self {
            Self::Agent(a) => a.stop(),
            #[cfg(target_os = "linux")]
            Self::Watcher(w) => w.stop(),
            _ => {}
        }
    }
}

fn build_pcap_assembler(
    enabled: bool,
    config: &PcapStream,
    stats_collector: &stats::Collector,
    pcap_batch_sender: DebugSender<BoxedPcapBatch>,
    queue_debugger: &QueueDebugger,
    ntp_diff: Arc<AtomicI64>,
    id: usize,
) -> (PcapAssembler, DebugSender<MiniPacket>) {
    let mini_packet_queue = "1-mini-meta-packet-to-pcap-handler";
    let (mini_packet_sender, mini_packet_receiver, mini_packet_counter) = queue::bounded_with_debug(
        config.receiver_queue_size,
        mini_packet_queue,
        &queue_debugger,
    );
    let pcap_assembler = PcapAssembler::new(
        id as u32,
        enabled,
        config.total_buffer_size,
        config.buffer_size_per_flow,
        config.flush_interval,
        pcap_batch_sender,
        mini_packet_receiver,
        ntp_diff,
    );
    stats_collector.register_countable(
        &stats::SingleTagModule("pcap_assembler", "id", id),
        Countable::Ref(Arc::downgrade(&pcap_assembler.counter) as Weak<dyn RefCountable>),
    );
    stats_collector.register_countable(
        &QueueStats {
            id,
            module: mini_packet_queue,
        },
        Countable::Owned(Box::new(mini_packet_counter)),
    );
    (pcap_assembler, mini_packet_sender)
}

fn build_dispatchers(
    id: usize,
    links: Vec<Link>,
    stats_collector: Arc<stats::Collector>,
    config_handler: &ConfigHandler,
    queue_debugger: Arc<QueueDebugger>,
    is_ce_version: bool,
    synchronizer: &Arc<Synchronizer>,
    npb_bps_limit: Arc<LeakyBucket>,
    npb_arp_table: Arc<NpbArpTable>,
    rx_leaky_bucket: Arc<LeakyBucket>,
    policy_getter: PolicyGetter,
    exception_handler: ExceptionHandler,
    bpf_options: Arc<Mutex<BpfOptions>>,
    packet_sequence_uniform_output: DebugSender<BoxedPacketSequenceBlock>,
    proto_log_sender: DebugSender<BoxAppProtoLogsData>,
    pcap_batch_sender: DebugSender<BoxedPcapBatch>,
    tap_typer: Arc<CaptureNetworkTyper>,
    vm_mac_addrs: Vec<MacAddr>,
    gateway_vmac_addrs: Vec<MacAddr>,
    toa_info_sender: DebugSender<Box<(SocketAddr, SocketAddr)>>,
    l4_flow_aggr_sender: DebugSender<BoxedTaggedFlow>,
    metrics_sender: DebugSender<BoxedDocument>,
    #[cfg(target_os = "linux")] netns: netns::NsFile,
    #[cfg(target_os = "linux")] kubernetes_poller: Arc<GenericPoller>,
    #[cfg(target_os = "linux")] libvirt_xml_extractor: Arc<LibvirtXmlExtractor>,
    #[cfg(target_os = "linux")] dpdk_ebpf_receiver: Option<Receiver<Box<packet::Packet<'static>>>>,
    #[cfg(target_os = "linux")] fanout_enabled: bool,
) -> Result<DispatcherComponent> {
    let candidate_config = &config_handler.candidate_config;
    let user_config = &candidate_config.user_config;
    let dispatcher_config = &candidate_config.dispatcher;
    let static_config = &config_handler.static_config;
    let agent_mode = static_config.agent_mode;
    let ctrl_ip = config_handler.ctrl_ip;
    let ctrl_mac = config_handler.ctrl_mac;
    let src_link = links.get(0).map(|l| l.to_owned()).unwrap_or_default();

    let (flow_sender, flow_receiver, counter) = queue::bounded_with_debug(
        user_config
            .processors
            .flow_log
            .tunning
            .flow_generator_queue_size,
        "1-tagged-flow-to-quadruple-generator",
        &queue_debugger,
    );
    stats_collector.register_countable(
        &QueueStats {
            id,
            module: "1-tagged-flow-to-quadruple-generator",
        },
        Countable::Owned(Box::new(counter)),
    );

    let (l7_stats_sender, l7_stats_receiver, counter) = queue::bounded_with_debug(
        user_config
            .processors
            .flow_log
            .tunning
            .flow_generator_queue_size,
        "1-l7-stats-to-quadruple-generator",
        &queue_debugger,
    );
    stats_collector.register_countable(
        &QueueStats {
            id,
            module: "1-l7-stats-to-quadruple-generator",
        },
        Countable::Owned(Box::new(counter)),
    );

    // create and start app proto logs
    let (log_sender, log_receiver, counter) = queue::bounded_with_debug(
        user_config
            .processors
            .flow_log
            .tunning
            .flow_generator_queue_size,
        "1-tagged-flow-to-app-protocol-logs",
        &queue_debugger,
    );
    stats_collector.register_countable(
        &QueueStats {
            id,
            module: "1-tagged-flow-to-app-protocol-logs",
        },
        Countable::Owned(Box::new(counter)),
    );

    let (session_aggr, counter) = SessionAggregator::new(
        log_receiver,
        proto_log_sender.clone(),
        id as u32,
        config_handler.log_parser(),
        synchronizer.ntp_diff(),
    );
    stats_collector.register_countable(
        &stats::SingleTagModule("l7_session_aggr", "index", id),
        Countable::Ref(Arc::downgrade(&counter) as Weak<dyn RefCountable>),
    );

    // Enterprise Edition Feature: packet-sequence
    // create and start packet sequence
    let (packet_sequence_sender, packet_sequence_receiver, counter) = queue::bounded_with_debug(
        user_config.processors.packet.tcp_header.sender_queue_size,
        "1-packet-sequence-block-to-parser",
        &queue_debugger,
    );
    stats_collector.register_countable(
        &QueueStats {
            id,
            module: "1-packet-sequence-block-to-parser",
        },
        Countable::Owned(Box::new(counter)),
    );

    let packet_sequence_parser = PacketSequenceParser::new(
        packet_sequence_receiver,
        packet_sequence_uniform_output,
        id as u32,
    );
    let (pcap_assembler, mini_packet_sender) = build_pcap_assembler(
        is_ce_version,
        &user_config.processors.packet.pcap_stream,
        &stats_collector,
        pcap_batch_sender.clone(),
        &queue_debugger,
        synchronizer.ntp_diff(),
        id,
    );

    let handler_builders = Arc::new(RwLock::new(vec![
        PacketHandlerBuilder::Pcap(mini_packet_sender),
        PacketHandlerBuilder::Npb(NpbBuilder::new(
            id,
            &candidate_config.npb,
            &queue_debugger,
            npb_bps_limit.clone(),
            npb_arp_table.clone(),
            stats_collector.clone(),
        )),
    ]));

    let pcap_interfaces = if candidate_config.capture_mode != PacketCaptureType::Local
        && candidate_config
            .user_config
            .inputs
            .cbpf
            .special_network
            .dpdk
            .source
            != DpdkSource::None
    {
        vec![]
    } else {
        links.clone()
    };

    let dispatcher_builder = DispatcherBuilder::new()
        .id(id)
        .pause(agent_mode == RunningMode::Managed)
        .handler_builders(handler_builders.clone())
        .ctrl_mac(ctrl_mac)
        .leaky_bucket(rx_leaky_bucket.clone())
        .options(Arc::new(Mutex::new(dispatcher::Options {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            af_packet_version: dispatcher_config.af_packet_version,
            packet_blocks: dispatcher_config.af_packet_blocks,
            capture_mode: candidate_config.capture_mode,
            tap_mac_script: user_config
                .inputs
                .resources
                .private_cloud
                .vm_mac_mapping_script
                .clone(),
            is_ipv6: ctrl_ip.is_ipv6(),
            npb_port: user_config.outputs.npb.target_port,
            vxlan_flags: user_config.outputs.npb.custom_vxlan_flags,
            controller_port: static_config.controller_port,
            controller_tls_port: static_config.controller_tls_port,
            libpcap_enabled: user_config.inputs.cbpf.special_network.libpcap.enabled,
            snap_len: dispatcher_config.capture_packet_size as usize,
            dpdk_source: dispatcher_config.dpdk_source,
            dispatcher_queue: dispatcher_config.dispatcher_queue,
            packet_fanout_mode: user_config.inputs.cbpf.af_packet.tunning.packet_fanout_mode,
            vhost_socket_path: user_config
                .inputs
                .cbpf
                .special_network
                .vhost_user
                .vhost_socket_path
                .clone(),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            cpu_set: dispatcher_config.cpu_set,
            #[cfg(target_os = "linux")]
            dpdk_ebpf_receiver,
            #[cfg(target_os = "linux")]
            fanout_enabled,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            promisc: user_config.inputs.cbpf.af_packet.tunning.promisc,
            ..Default::default()
        })))
        .bpf_options(bpf_options)
        .default_tap_type(
            (user_config
                .inputs
                .cbpf
                .physical_mirror
                .default_capture_network_type)
                .try_into()
                .unwrap_or(CaptureNetworkType::Cloud),
        )
        .mirror_traffic_pcp(
            user_config
                .inputs
                .cbpf
                .af_packet
                .vlan_pcp_in_physical_mirror_traffic,
        )
        .tap_typer(tap_typer.clone())
        .analyzer_dedup_disabled(user_config.inputs.cbpf.tunning.dispatcher_queue_enabled)
        .flow_output_queue(flow_sender.clone())
        .l7_stats_output_queue(l7_stats_sender.clone())
        .log_output_queue(log_sender.clone())
        .packet_sequence_output_queue(packet_sequence_sender) // Enterprise Edition Feature: packet-sequence
        .stats_collector(stats_collector.clone())
        .flow_map_config(config_handler.flow())
        .log_parse_config(config_handler.log_parser())
        .collector_config(config_handler.collector())
        .policy_getter(policy_getter)
        .exception_handler(exception_handler.clone())
        .ntp_diff(synchronizer.ntp_diff())
        .src_interface(
            if candidate_config.capture_mode != PacketCaptureType::Local {
                #[cfg(target_os = "linux")]
                if !fanout_enabled {
                    src_link.name.clone()
                } else {
                    "".into()
                }
                #[cfg(target_os = "windows")]
                "".into()
            } else {
                "".into()
            },
        )
        .agent_type(dispatcher_config.agent_type)
        .queue_debugger(queue_debugger.clone())
        .analyzer_queue_size(user_config.inputs.cbpf.tunning.raw_packet_queue_size)
        .pcap_interfaces(pcap_interfaces.clone())
        .tunnel_type_trim_bitmap(dispatcher_config.tunnel_type_trim_bitmap)
        .bond_group(dispatcher_config.bond_group.clone())
        .analyzer_raw_packet_block_size(
            user_config.inputs.cbpf.tunning.raw_packet_buffer_block_size,
        );
    #[cfg(target_os = "linux")]
    let dispatcher_builder = dispatcher_builder
        .netns(netns)
        .libvirt_xml_extractor(libvirt_xml_extractor.clone())
        .platform_poller(kubernetes_poller.clone());
    let dispatcher = match dispatcher_builder.build() {
        Ok(d) => d,
        Err(e) => {
            warn!(
                "dispatcher creation failed: {}, deepflow-agent restart...",
                e
            );
            thread::sleep(Duration::from_secs(1));
            return Err(e.into());
        }
    };
    let mut dispatcher_listener = dispatcher.listener();
    dispatcher_listener.on_config_change(dispatcher_config);
    dispatcher_listener.on_tap_interface_change(
        &links,
        dispatcher_config.if_mac_source,
        dispatcher_config.agent_type,
        &vec![],
    );
    dispatcher_listener.on_vm_change(&vm_mac_addrs, &gateway_vmac_addrs);
    synchronizer.add_flow_acl_listener(Box::new(dispatcher_listener.clone()));

    // create and start collector
    let collector = AgentComponents::new_collector(
        id,
        stats_collector.clone(),
        flow_receiver,
        toa_info_sender.clone(),
        Some(l4_flow_aggr_sender.clone()),
        metrics_sender.clone(),
        MetricsType::SECOND | MetricsType::MINUTE,
        config_handler,
        &queue_debugger,
        &synchronizer,
        agent_mode,
    );

    let l7_collector = AgentComponents::new_l7_collector(
        id,
        stats_collector.clone(),
        l7_stats_receiver,
        metrics_sender.clone(),
        MetricsType::SECOND | MetricsType::MINUTE,
        config_handler,
        &queue_debugger,
        &synchronizer,
        agent_mode,
    );
    Ok(DispatcherComponent {
        id,
        dispatcher,
        dispatcher_listener,
        session_aggregator: session_aggr,
        collector,
        l7_collector,
        packet_sequence_parser,
        pcap_assembler,
        handler_builders,
        src_link,
    })
}
