use std::collections::HashMap;
use std::convert::TryInto;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{self, Arc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::{debug, info, warn};
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use prost::Message;
use sysinfo::{System, SystemExt};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::time;

use crate::common::policy::{Cidr, IpGroupData, PeerConnection};
use crate::common::{FlowAclListener, PlatformData as VInterface};
use crate::config::RuntimeConfig;
use crate::dispatcher::DispatcherListener;
use crate::policy::PolicySetter;
use crate::proto::trident::{self as tp, TapMode};
use crate::rpc::session::Session;
use crate::trident::{self, TridentState};
use crate::utils::{
    self,
    environment::{is_tt_pod, is_tt_process},
    net::{is_unicast_link_local, links_by_name_regex, MacAddr},
    stats::{Countable, Counter},
};

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(10);
const RPC_RETRY_INTERVAL: Duration = Duration::from_secs(60);

pub struct StaticConfig {
    pub revision: String,
    pub boot_time: SystemTime,

    pub tap_mode: tp::TapMode,
    pub vtap_group_id_request: String,
    pub kubernetes_cluster_id: String,
    pub ctrl_mac: String,
    pub ctrl_ip: String,

    pub env: RuntimeEnvironment,
}

impl Default for StaticConfig {
    fn default() -> Self {
        Self {
            revision: Default::default(),
            boot_time: SystemTime::now(),
            tap_mode: Default::default(),
            vtap_group_id_request: Default::default(),
            kubernetes_cluster_id: Default::default(),
            ctrl_ip: Default::default(),
            ctrl_mac: Default::default(),
            env: Default::default(),
        }
    }
}

#[derive(Default)]
pub struct Config {
    proxy_ip: String,
}

#[derive(Default)]
pub struct Status {
    pub hostname: String,

    pub time_diff: i64,

    pub config_accepted: bool,
    pub synced: bool,
    pub new_revision: Option<String>,

    // GRPC数据
    pub version_platform_data: u64,
    pub version_acls: u64,
    pub version_groups: u64,

    pub interfaces: Vec<Arc<VInterface>>,
    pub peers: Vec<Arc<PeerConnection>>,
    pub cidrs: Vec<Arc<Cidr>>,
    pub ip_groups: Vec<Arc<IpGroupData>>,
}

impl Status {
    fn update_platform_data(
        &mut self,
        version: u64,
        interfaces: Vec<Arc<VInterface>>,
        peers: Vec<Arc<PeerConnection>>,
        cidrs: Vec<Arc<Cidr>>,
    ) {
        info!(
            "Update PlatformData version {} to {}.",
            self.version_platform_data, version
        );

        self.version_platform_data = version;
        self.interfaces = interfaces;
        self.cidrs = cidrs;
        self.peers = peers;
    }

    fn update_ip_groups(&mut self, version: u64, ip_groups: Vec<Arc<IpGroupData>>) {
        info!(
            "Update IpGroups version {} to {}.",
            self.version_groups, version
        );

        self.version_groups = version;
        self.ip_groups = ip_groups;
    }

    fn update_flow_acl(&mut self, version: u64) {
        info!(
            "Update FlowAcls version {} to {}.",
            self.version_acls, version
        );

        self.version_acls = version;

        // TODO: add acl
    }

    fn get_platform_data(&mut self, resp: &tp::SyncResponse) -> bool {
        let current_version = self.version_platform_data;
        let version = resp.version_platform_data.unwrap();
        debug!(
            "get grpc PlatformData version: {} vs current version: {}.",
            version, current_version
        );
        if version == 0 {
            debug!("platform data in preparation.");
            return false;
        }
        if version == current_version {
            debug!("platform data same version.");
            return false;
        }

        if let Some(platform_compressed) = &resp.platform_data {
            let platform = tp::PlatformData::decode(platform_compressed.as_slice());
            if platform.is_ok() {
                let platform = platform.unwrap();
                let mut interfaces = Vec::new();
                let mut peers = Vec::new();
                let mut cidrs = Vec::new();
                for item in &platform.interfaces {
                    let result = VInterface::try_from(item);
                    if result.is_ok() {
                        interfaces.push(Arc::new(result.unwrap()));
                    } else {
                        warn!("{:?}: {}", item, result.unwrap_err());
                    }
                }
                for item in &platform.peer_connections {
                    peers.push(Arc::new(PeerConnection::from(item)));
                }
                for item in &platform.cidrs {
                    let result = Cidr::try_from(item);
                    if result.is_ok() {
                        cidrs.push(Arc::new(result.unwrap()));
                    } else {
                        warn!("{:?}: {}", item, result.unwrap_err());
                    }
                }

                self.update_platform_data(version, interfaces, peers, cidrs);
                return true;
            }
        }
        return false;
    }

    fn modify_platform(
        &mut self,
        tap_mode: TapMode,
        macs: &Vec<MacAddr>,
        config: &RuntimeConfig,
    ) -> Vec<VInterface> {
        let mut black_list = Vec::new();
        if tap_mode == TapMode::Analyzer {
            return black_list;
        }
        let mut local_mac_map = HashMap::new();
        for mac in macs {
            let _ = local_mac_map.insert(u64::from(*mac), true);
        }
        let region_id = config.region_id;
        let pod_cluster_id = config.pod_cluster_id;
        let mut vinterfaces = Vec::new();
        for i in &self.interfaces {
            let mut viface = (*(i.clone())).clone();
            if !is_tt_pod(config.trident_type) {
                viface.skip_mac = viface.region_id != region_id;
            } else {
                let mut is_tap_interface = viface.pod_cluster_id == pod_cluster_id;
                is_tap_interface = is_tap_interface
                    || (viface.region_id == region_id
                        && viface.device_type != (tp::DeviceType::Pod as u8));
                viface.skip_mac = !is_tap_interface;
            }

            // vm为k8s node场景，k8s node流量由k8s内的采集器来采集，不在这里采集避免采集重复的流量
            if viface.skip_tap_interface
                && is_tt_process(config.trident_type)
                && viface.region_id == region_id
            {
                black_list.push(viface.clone());
            }
            if let Some(v) = local_mac_map.get(&viface.mac) {
                viface.is_local = *v;
            }
            vinterfaces.push(Arc::new(viface));
        }

        self.interfaces = vinterfaces;
        // TODO：bridge fdb
        return black_list;
    }

    fn get_flow_acls(&mut self, resp: &tp::SyncResponse) -> bool {
        let version = resp.version_platform_data.unwrap();
        debug!(
            "get grpc FlowAcls version: {} vs current version: {}.",
            version, self.version_acls
        );
        if version == 0 {
            debug!("FlowAcls data in preparation.");
            return false;
        }
        if version == self.version_acls {
            debug!("FlowAcls data same version.");
            return false;
        }

        if let Some(acls_commpressed) = &resp.flow_acls {
            let acls = tp::FlowAcls::decode(acls_commpressed.as_slice());
            if acls.is_ok() {
                // TODO: acl parse
                self.update_flow_acl(version);
                return true;
            }
        }
        return false;
    }

    fn get_ip_groups(&mut self, resp: &tp::SyncResponse) -> bool {
        let version = resp.version_groups.unwrap();
        debug!(
            "get grpc Groups version: {} vs current version: {}.",
            version, self.version_groups
        );
        if version == 0 {
            debug!("Groups data in preparation.");
            return false;
        }
        if self.version_groups == version {
            debug!("Groups data same version.");
            return false;
        }

        if let Some(groups_compressed) = &resp.groups {
            let groups = tp::Groups::decode(groups_compressed.as_slice());
            if groups.is_ok() {
                let groups = groups.unwrap();
                let mut ip_groups = Vec::new();
                for item in &groups.groups {
                    let result = IpGroupData::try_from(item);
                    if result.is_ok() {
                        ip_groups.push(Arc::new(result.unwrap()));
                    } else {
                        warn!("{:?}: {}", item, result.unwrap_err());
                    }
                }
                self.update_ip_groups(version, ip_groups);
                return true;
            }
        }
        return false;
    }

    fn trigger_flow_acl(&self, listener: &mut Box<dyn FlowAclListener>) {
        listener.flow_acl_change(&self.ip_groups, &self.interfaces, &self.peers, &self.cidrs);
    }
}

pub struct Synchronizer {
    pub static_config: Arc<StaticConfig>,
    config: Arc<RwLock<Config>>,
    pub status: Arc<RwLock<Status>>,

    trident_state: TridentState,

    sync_interval: Duration,

    session: Arc<Session>,
    dispatcher_listener: Arc<Mutex<Option<DispatcherListener>>>,
    // 策略模块和NPB带宽检测会用到
    flow_acl_listener: Arc<sync::Mutex<Vec<Box<dyn FlowAclListener>>>>,

    running: Arc<AtomicBool>,

    // threads
    rt: Runtime,
    threads: Mutex<Vec<JoinHandle<()>>>,

    max_memory: Arc<AtomicU64>,
}

impl Synchronizer {
    pub fn new(
        session: Arc<Session>,
        trident_state: TridentState,
        revision: String,
        ctrl_ip: String,
        ctrl_mac: String,
        vtap_group_id_request: String,
        kubernetes_cluster_id: String,
        policy_setter: PolicySetter,
    ) -> Synchronizer {
        Synchronizer {
            static_config: Arc::new(StaticConfig {
                revision,
                boot_time: SystemTime::now(),
                tap_mode: tp::TapMode::Local,
                vtap_group_id_request,
                kubernetes_cluster_id,
                ctrl_mac,
                ctrl_ip,
                env: RuntimeEnvironment::new(),
            }),
            trident_state,
            config: Default::default(),
            status: Default::default(),
            sync_interval: DEFAULT_SYNC_INTERVAL,
            session,
            dispatcher_listener: Default::default(),
            running: Arc::new(AtomicBool::new(false)),
            rt: Runtime::new().unwrap(),
            threads: Default::default(),
            flow_acl_listener: Arc::new(sync::Mutex::new(vec![Box::new(policy_setter)])),

            max_memory: Default::default(),
        }
    }

    pub fn add_flow_acl_listener(&mut self, module: Box<dyn FlowAclListener>) {
        let mut listeners = self.flow_acl_listener.lock().unwrap();
        for item in listeners.iter() {
            if item.id() == module.id() {
                return;
            }
        }
        listeners.push(module);
    }

    pub fn max_memory(&self) -> Arc<AtomicU64> {
        self.max_memory.clone()
    }

    pub fn generate_sync_request(
        static_config: &Arc<StaticConfig>,
        status: &Arc<RwLock<Status>>,
    ) -> tp::SyncRequest {
        let status = status.read();

        let boot_time = static_config
            .boot_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let boot_time = (boot_time as i64 + status.time_diff) / 1_000_000_000;

        fn is_excluded_ip_addr(ip_addr: IpAddr) -> bool {
            if ip_addr.is_loopback() || ip_addr.is_unspecified() || ip_addr.is_multicast() {
                return true;
            }
            match ip_addr {
                IpAddr::V4(addr) => addr.is_link_local(),
                // Ipv6Addr::is_unicast_link_local()是实验API无法使用
                IpAddr::V6(addr) => is_unicast_link_local(&addr),
            }
        }

        tp::SyncRequest {
            boot_time: Some(boot_time as u32),
            config_accepted: Some(status.config_accepted),
            version_platform_data: Some(status.version_platform_data),
            version_acls: Some(status.version_acls),
            version_groups: Some(status.version_groups),
            state: Some(tp::State::Running.into()),
            revision: Some(static_config.revision.clone()),
            exception: None, // TBD
            process_name: Some("trident".into()),
            ctrl_mac: Some(static_config.ctrl_mac.clone()),
            ctrl_ip: Some(static_config.ctrl_ip.clone()),
            tap_mode: Some(static_config.tap_mode.into()),
            host: Some(status.hostname.clone()),
            host_ips: utils::net::addr_list().map_or(vec![], |xs| {
                xs.into_iter()
                    .filter_map(|x| {
                        if is_excluded_ip_addr(x.ip_addr) {
                            None
                        } else {
                            Some(x.ip_addr.to_string())
                        }
                    })
                    .collect()
            }),
            cpu_num: Some(static_config.env.cpu_num),
            memory_size: Some(static_config.env.memory_size),
            arch: Some(static_config.env.arch.clone()),
            os: Some(static_config.env.os.clone()),
            kernel_version: Some(static_config.env.kernel_version.clone()),
            vtap_group_id_request: Some(static_config.vtap_group_id_request.clone()),
            kubernetes_cluster_id: Some(static_config.kubernetes_cluster_id.clone()),

            // not interested
            communication_vtaps: vec![],
            tsdb_report_info: None,
            // FIXME @xiangwang 后续增加配置文件处理业务逻辑
            local_config_file: None,
        }
    }

    pub fn clone_session(&self) -> Arc<Session> {
        self.session.clone()
    }

    fn parse_upgrade(
        resp: &tp::SyncResponse,
        static_config: &Arc<StaticConfig>,
        status: &Arc<RwLock<Status>>,
    ) {
        match &resp.revision {
            Some(revision) if revision != &static_config.revision => {
                if let Some(url) = &resp.self_update_url {
                    if url.trim().to_lowercase() != "grpc" {
                        warn!("error upgrade method, onlly support grpc: {}", url);
                        return;
                    }
                    info!(
                        "trigger upgrade as revision update from {} to {}",
                        &static_config.revision, revision
                    );
                    status.write().new_revision = Some(revision.clone());
                }
            }
            _ => (),
        }
    }

    fn on_config_change(_config: &RuntimeConfig) {}

    fn trigger_dispatcher_listener(
        listener: &DispatcherListener,
        _: &tp::SyncResponse,
        tap_mode: tp::TapMode,
        rt_config: &RuntimeConfig,
        _: &Vec<MacAddr>,
        blacklist: &Vec<VInterface>,
    ) {
        if tap_mode == tp::TapMode::Local {
            let if_mac_source = rt_config.if_mac_source;
            match links_by_name_regex(&rt_config.tap_interface_regex) {
                Err(e) => warn!("get interfaces by name regex failed: {}", e),
                Ok(links) if links.is_empty() => warn!(
                    "tap-interface-regex({}) do not match any interface, in local mode",
                    rt_config.tap_interface_regex
                ),
                Ok(links) => listener.on_tap_interface_change(
                    &links,
                    if_mac_source,
                    rt_config.trident_type,
                    blacklist,
                ),
            }
        } else {
            todo!()
        }
    }

    fn parse_segment(
        tap_mode: tp::TapMode,
        resp: &tp::SyncResponse,
    ) -> (Vec<tp::Segment>, Vec<MacAddr>) {
        let segments = if tap_mode == tp::TapMode::Analyzer {
            resp.remote_segments.clone()
        } else {
            resp.local_segments.clone()
        };

        if segments.len() == 0 && tap_mode != tp::TapMode::Local {
            warn!("Segment is empty, in {:?} mode.", tap_mode);
        }
        let mut macs = Vec::new();
        for segment in &segments {
            for mac_str in &segment.mac {
                let mac = MacAddr::from_str(mac_str.as_str());
                if mac.is_err() {
                    warn!(
                        "Malformed VM mac {}, response rejected: {}",
                        mac_str,
                        mac.unwrap_err()
                    );
                    continue;
                }
                macs.push(mac.unwrap());
            }
        }
        return (segments, macs);
    }

    fn on_response(
        remote: &str,
        mut resp: tp::SyncResponse,
        trident_state: &TridentState,
        static_config: &Arc<StaticConfig>,
        status: &Arc<RwLock<Status>>,
        dispatcher_listener: &Arc<Mutex<Option<DispatcherListener>>>,
        _flow_acl_listener: &Arc<sync::Mutex<Vec<Box<dyn FlowAclListener>>>>,
        max_memory: &Arc<AtomicU64>,
    ) {
        // TODO: reset escape timer
        // TODO: 把cleaner UpdatePcapDataRetention挪到别的地方
        Self::parse_upgrade(&resp, static_config, status);

        match resp.status() {
            tp::Status::Failed => warn!(
                "trisolaris (ip: {}) responded with {:?}",
                remote,
                tp::Status::Failed
            ),
            tp::Status::Heartbeat => return,
            _ => (),
        }

        let config = resp.config.take();
        if config.is_none() {
            warn!("invalid response from {} without config", remote);
            return;
        }
        let new_config = config.clone();
        let runtime_config = config.unwrap().try_into();
        if let Err(e) = runtime_config {
            warn!(
                "invalid response from {} with invalid config: {}",
                remote, e
            );
            return;
        }
        let runtime_config = runtime_config.unwrap();
        Self::on_config_change(&runtime_config);

        max_memory.store(runtime_config.max_memory, Ordering::Relaxed);

        let mut dispatcher_listener = dispatcher_listener.lock();
        if let Some(listener) = (*dispatcher_listener).as_mut() {
            listener.on_config_change(&runtime_config);
        }

        let mut _blacklist = vec![];
        let (_segments, macs) = Self::parse_segment(static_config.tap_mode, &resp);

        /* 这段代码在容器环境下会死锁
        let mut status = status.write();
        let updated_platform = status.get_platform_data(&resp);
        if updated_platform {
            blacklist = status.modify_platform(static_config.tap_mode, &macs, &runtime_config);
        }
        let mut updated = status.get_ip_groups(&resp) || updated_platform;
        updated = status.get_flow_acls(&resp) || updated;
        if updated {
            // 更新策略相关
            let last = SystemTime::now();
            info!("Grpc version ip-groups: {}, interfaces, peer-connections and cidrs: {}, flow-acls: {}",
            status.version_groups, status.version_platform_data, status.version_acls);
            for listener in flow_acl_listener.lock().unwrap().iter_mut() {
                status.trigger_flow_acl(listener);
            }
            let now = SystemTime::now();
            info!("Grpc finish update cost {:?} on {} listener, {} ip-groups, {} interfaces, {} peer-connections, {} cidrs, {} flow-acls",
                now.duration_since(last).unwrap_or(Duration::from_secs(0)),
                flow_acl_listener.lock().unwrap().len(),
                status.ip_groups.len(),
                status.interfaces.len(),
                status.peers.len(),
                status.cidrs.len(),
                0,
            );
        }
         */

        // TODO: bridge forward
        // TODO: check trisolaris
        // TODO: segments
        // TODO: modify platform
        if let Some(listener) = &*dispatcher_listener {
            Self::trigger_dispatcher_listener(
                &listener,
                &resp,
                static_config.tap_mode,
                &runtime_config,
                &macs,
                &_blacklist,
            );
        }

        let (trident_state, cvar) = &**trident_state;
        *trident_state.lock().unwrap() =
            trident::State::ConfigChanged((runtime_config, new_config.unwrap()));
        cvar.notify_one();
    }

    fn run_triggered_session(&self) {
        let session = self.session.clone();
        let trident_state = self.trident_state.clone();
        let static_config = self.static_config.clone();
        let status = self.status.clone();
        let running = self.running.clone();
        let dispatcher_listener = self.dispatcher_listener.clone();
        let max_memory = self.max_memory.clone();
        let flow_acl_listener = self.flow_acl_listener.clone();
        self.threads.lock().push(self.rt.spawn(async move {
            while running.load(Ordering::SeqCst) {
                session.update_current_server().await;
                let client = session.get_client();
                if client.is_none() {
                    info!("rpc trigger not running, client not connected");
                    time::sleep(Duration::new(1, 0)).await;
                    continue;
                }
                let mut client = tp::synchronizer_client::SynchronizerClient::new(client.unwrap());
                let version = session.get_version();
                let response = client
                    .push(Synchronizer::generate_sync_request(&static_config, &status))
                    .await;
                if let Err(m) = response {
                    warn!("rpc error {:?}", m);
                    time::sleep(RPC_RETRY_INTERVAL).await;
                    continue;
                }
                let mut stream = response.unwrap().into_inner();
                while running.load(Ordering::SeqCst) {
                    let message = stream.message().await;
                    if session.get_version() != version {
                        info!("grpc server changed");
                        break;
                    }
                    if let Err(m) = message {
                        warn!("rpc error {:?}", m);
                        break;
                    }
                    let message = message.unwrap();
                    if message.is_none() {
                        debug!("end of stream");
                        break;
                    }
                    let message = message.unwrap();

                    debug!("received realtime policy successfully");
                    if !status.read().synced {
                        // 如果没有同步过（trident重启），trisolaris下发的数据仅有版本号，此时应由trident主动请求
                        continue;
                    }
                    Self::on_response(
                        &session.get_current_server(),
                        message,
                        &trident_state,
                        &static_config,
                        &status,
                        &dispatcher_listener,
                        &flow_acl_listener,
                        &max_memory,
                    );
                }
            }
        }));
    }

    fn run(&self) {
        let session = self.session.clone();
        let trident_state = self.trident_state.clone();
        let static_config = self.static_config.clone();
        let _config = self.config.clone();
        let status = self.status.clone();
        let sync_interval = self.sync_interval;
        let running = self.running.clone();
        let dispatcher_listener = self.dispatcher_listener.clone();
        let flow_acl_listener = self.flow_acl_listener.clone();
        let max_memory = self.max_memory.clone();
        self.threads.lock().push(self.rt.spawn(async move {
            let mut client = None;
            let version = session.get_version();
            while running.load(Ordering::SeqCst) {
                match hostname::get() {
                    Ok(hostname) => {
                        if let Ok(s) = hostname.into_string() {
                            let r = status.upgradable_read();
                            if s.ne(&r.hostname) {
                                info!("hostname changed from \"{}\" to \"{}\"", r.hostname, s);
                                RwLockUpgradableReadGuard::upgrade(r).hostname = s;
                            }
                        }
                    }
                    Err(e) => warn!("refresh hostname failed: {}", e),
                }
                if session.get_request_failed() {
                    let status = status.read();
                    info!(
                        "TapMode: {:?}, CtrlMac: {}, CtrlIp: {}, Hostname: {}",
                        static_config.tap_mode,
                        static_config.ctrl_ip,
                        static_config.ctrl_mac,
                        status.hostname,
                    )
                }

                let changed = session.update_current_server().await;
                let request = Synchronizer::generate_sync_request(&static_config, &status);
                debug!("grpc sync request: {:?}", request);

                if client.is_none() || version != session.get_version() {
                    let inner_client = session.get_client();
                    if inner_client.is_none() {
                        info!("grpc sync client not connected");
                        time::sleep(Duration::new(1, 0)).await;
                        continue;
                    }
                    client = Some(tp::synchronizer_client::SynchronizerClient::new(
                        inner_client.unwrap(),
                    ));
                }
                let now = Instant::now();
                let response = client.as_mut().unwrap().sync(request).await;
                if let Err(m) = response {
                    warn!(
                        "grpc sync error, server {} unavailable, status-code {}, message: \"{}\"",
                        session.get_current_server(),
                        m.code(),
                        m.message()
                    );
                    session.set_request_failed(true);
                    time::sleep(RPC_RETRY_INTERVAL).await;
                    continue;
                }

                debug!("grpc sync took {:?}", now.elapsed());
                session.set_request_failed(false);

                if changed {
                    info!(
                        "grpc sync new rpc server {} available",
                        session.get_current_server()
                    );
                }

                Self::on_response(
                    &session.get_current_server(),
                    response.unwrap().into_inner(),
                    &trident_state,
                    &static_config,
                    &status,
                    &dispatcher_listener,
                    &flow_acl_listener,
                    &max_memory,
                );
                if status.read().new_revision.is_some() {
                    // TODO: upgrade
                }

                time::sleep(sync_interval).await;
            }
        }));
    }

    pub fn start(&self) {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }
        self.run_triggered_session();
        self.run();
    }

    pub fn stop(&self) {
        if !self.running.swap(false, Ordering::SeqCst) {
            return;
        }
        self.rt.block_on(async move {
            for t in self.threads.lock().drain(..) {
                let _ = t.await;
            }
        });
    }
}

impl Countable for Synchronizer {
    fn get_counters(&self) -> Vec<Counter> {
        vec![]
    }

    fn closed(&self) -> bool {
        !self.running.load(Ordering::SeqCst)
    }
}

pub struct SynchronizerBuilder {
    port: Option<u16>,
    tls_port: Option<u16>,
    timeout: Duration,
    controller_cert_file_prefix: String,
    vtap_group_id_request: String,

    ctrl_ip: String,
    ctrl_mac: String,
    controller_ips: Vec<String>,
    analyzer_ip: String,
}

impl SynchronizerBuilder {
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }
}

#[derive(Debug, Default)]
pub struct RuntimeEnvironment {
    pub cpu_num: u32,
    pub memory_size: u64,

    pub arch: String,
    pub os: String,

    pub kernel_version: String,
}

impl RuntimeEnvironment {
    fn new() -> RuntimeEnvironment {
        let mut sys = System::new();
        sys.refresh_memory();
        RuntimeEnvironment {
            cpu_num: sys.physical_core_count().unwrap_or_default() as u32,
            // 这里乘1000，因为库作者思路清奇换算成了10基底的KB
            memory_size: sys.total_memory() * 1000,
            arch: std::env::consts::ARCH.into(),
            os: format!(
                "{} {}",
                sys.name().unwrap_or_default(),
                sys.os_version().unwrap_or_default()
            ),
            kernel_version: sys
                .kernel_version()
                .unwrap_or_default()
                .split('-')
                .next()
                .unwrap_or_default()
                .into(),
        }
    }
}
