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
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, UdpSocket},
    time::Duration,
};
#[cfg(target_os = "linux")]
use std::{fmt, io::Write};

use anyhow::{anyhow, Result};
use bincode::{config, decode_from_std_read};
use clap::{ArgEnum, Parser, Subcommand};
#[cfg(target_os = "linux")]
use flate2::write::ZlibDecoder;

use deepflow_agent::debug::{
    Beacon, Client, Message, Module, PolicyMessage, RpcMessage, DEBUG_QUEUE_IDLE_TIMEOUT,
    DEEPFLOW_AGENT_BEACON,
};
#[cfg(target_os = "linux")]
use deepflow_agent::debug::{EbpfMessage, PlatformMessage};
use public::{consts::DEFAULT_CONTROLLER_PORT, debug::QueueMessage};

const ERR_PORT_MSG: &str = "error: The following required arguments were not provided:
    \t--port <PORT> required arguments were not provided";

#[derive(Parser)]
#[clap(name = "deepflow-agent-ctl")]
struct Cmd {
    #[clap(subcommand)]
    command: ControllerCmd,
    /// remote deepflow-agent listening port
    #[clap(short, long, parse(try_from_str))]
    port: Option<u16>,
    /// remote deepflow-agent host ip
    ///
    /// ipv6 format is 'fe80::5054:ff:fe95:c839', ipv4 format is '127.0.0.1'
    #[clap(short, long, parse(try_from_str), default_value_t=IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))]
    address: IpAddr,
}

#[derive(Subcommand)]
enum ControllerCmd {
    /// get information about the rpc synchronizer
    Rpc(RpcCmd),
    #[cfg(target_os = "linux")]
    /// get information about the k8s platform
    Platform(PlatformCmd),
    /// monitor various queues of the selected deepflow-agent
    Queue(QueueCmd),
    /// get information about the policy
    Policy(PolicyCmd),
    #[cfg(target_os = "linux")]
    /// get information about the ebpf
    Ebpf(EbpfCmd),
    /// get information about the deepflow-agent
    List,
}

#[derive(Parser)]
struct QueueCmd {
    /// monitor module
    ///
    /// eg: monitor 1-tagged-flow-to-quadruple-generator queue with 60s
    ///
    /// deepflow-agent-ctl queue --on 1-tagged-flow-to-quadruple-generator --duration 60
    #[clap(long, requires = "monitor")]
    on: Option<String>,
    /// monitoring duration in seconds
    #[clap(long, group = "monitor")]
    duration: Option<u64>,
    /// turn off monitor
    ///
    /// eg: turn off 1-tagged-flow-to-quadruple-generator queue monitor
    ///
    /// deepflow-agent-ctl queue --off 1-tagged-flow-to-quadruple-generator queue
    #[clap(long)]
    off: Option<String>,
    /// show queue list
    ///
    /// eg: deepflow-agent-ctl queue --show
    #[clap(long)]
    show: bool,
    /// turn off all queue
    ///
    /// eg: deepflow-agent-ctl queue --clear
    #[clap(long)]
    clear: bool,
}

#[cfg(target_os = "linux")]
#[derive(Parser)]
struct PlatformCmd {
    /// get resources with k8s api
    ///
    /// eg: deepflow-agent-ctl platform --k8s_get node
    #[clap(short, long, arg_enum)]
    k8s_get: Option<Resource>,
    /// show k8s container mac to global interface index mappings
    ///
    /// eg: deepflow-agent-ctl platform --mac_mappings
    #[clap(short, long)]
    mac_mappings: bool,
}

#[derive(Debug, Parser)]
struct PolicyCmd {
    #[clap(subcommand)]
    subcmd: PolicySubCmd,
}

#[derive(Subcommand, Debug)]
enum PolicySubCmd {
    Monitor,
    Show,
    Analyzing(AnalyzingArgs),
}

#[derive(Debug, Parser)]
struct AnalyzingArgs {
    /// Set policy id
    ///
    /// eg: deepflow-agent-ctl policy analyzing --id 10
    #[clap(long, parse(try_from_str))]
    id: Option<u32>,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Parser)]
struct EbpfCmd {
    #[clap(subcommand)]
    subcmd: EbpfSubCmd,
}

#[cfg(target_os = "linux")]
#[derive(Subcommand, Debug)]
enum EbpfSubCmd {
    /// monitor datadump
    Datadump(EbpfArgs),
    /// monitor cpdbg
    Cpdbg(EbpfArgs),
}

#[cfg(target_os = "linux")]
#[derive(Debug, Parser)]
struct EbpfArgs {
    /// Set datadump pid
    ///
    /// eg: deepflow-agent-ctl ebpf datadump --pid 10001
    #[clap(long, parse(try_from_str), default_value_t = 0)]
    pid: u32,
    /// Set datadump name
    ///
    /// eg: deepflow-agent-ctl ebpf datadump --name nginx
    #[clap(long, parse(try_from_str), default_value = "")]
    name: String,
    /// Set datadump app protocol
    ///
    /// App Protocol: All(0), Other(1),
    ///   HTTP1(20), HTTP2(21), Dubbo(40), SofaRPC(43),
    ///   MySQL(60), PostGreSQL(61), Oracle(62),
    ///   Redis(80), MongoDB(81), Memcached(82),
    ///   Kafka(100), MQTT(101), RocketMQ(107), DNS(120), TLS(121),
    ///
    /// eg: deepflow-agent-ctl ebpf datadump --proto 20
    #[clap(long, parse(try_from_str), default_value_t = 0)]
    proto: u8,
    /// Set datadump/cpdbg duration
    ///
    /// eg: deepflow-agent-ctl ebpf datadump --duration 10
    #[clap(long, parse(try_from_str), default_value_t = 30)]
    duration: u16,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, ArgEnum, Debug)]
enum Resource {
    Version,
    No,
    Node,
    Nodes,
    Ns,
    Namespace,
    Namespaces,
    Ing,
    Ingress,
    Ingresses,
    Svc,
    Service,
    Services,
    Deploy,
    Deployment,
    Deployments,
    Po,
    Pod,
    Pods,
    St,
    Statefulset,
    Statefulsets,
    Ds,
    Daemonset,
    Daemonsets,
    Rc,
    Replicationcontroller,
    Replicationcontrollers,
    Rs,
    Replicaset,
    Replicasets,
}

#[cfg(target_os = "linux")]
impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Resource::No | Resource::Node | Resource::Nodes => write!(f, "nodes"),
            Resource::Ns | Resource::Namespace | Resource::Namespaces => write!(f, "namespaces"),
            Resource::Svc | Resource::Service | Resource::Services => write!(f, "services"),
            Resource::Deploy | Resource::Deployment | Resource::Deployments => {
                write!(f, "deployments")
            }
            Resource::Po | Resource::Pod | Resource::Pods => write!(f, "pods"),
            Resource::St | Resource::Statefulset | Resource::Statefulsets => {
                write!(f, "statefulsets")
            }
            Resource::Ds | Resource::Daemonset | Resource::Daemonsets => write!(f, "daemonsets"),
            Resource::Rc | Resource::Replicationcontroller | Resource::Replicationcontrollers => {
                write!(f, "replicationcontrollers")
            }
            Resource::Rs | Resource::Replicaset | Resource::Replicasets => {
                write!(f, "replicasets")
            }
            Resource::Ing | Resource::Ingress | Resource::Ingresses => write!(f, "ingresses"),
            Resource::Version => write!(f, "version"),
        }
    }
}

#[derive(Parser)]
struct RpcCmd {
    /// Get data from RPC
    ///
    /// eg: get rpc config data
    /// deepflow-agent-ctl rpc --get config
    ///
    #[clap(long, arg_enum)]
    get: RpcData,
}

#[derive(Clone, Copy, ArgEnum, Debug)]
enum RpcData {
    Config,
    Platform,
    CaptureNetworkTypes,
    Cidr,
    Groups,
    Acls,
    Segments,
    Version,
}

struct Controller {
    cmd: Option<Cmd>,
    addr: IpAddr,
    port: Option<u16>,
}

impl Controller {
    pub fn new() -> Self {
        let cmd = Cmd::parse();
        Self {
            addr: cmd.address,
            port: cmd.port,
            cmd: Some(cmd),
        }
    }

    fn dispatch(&mut self) -> Result<()> {
        match self.cmd.take().unwrap().command {
            #[cfg(target_os = "linux")]
            ControllerCmd::Platform(c) => self.platform(c),
            ControllerCmd::Rpc(c) => self.rpc(c),
            ControllerCmd::List => self.list(),
            ControllerCmd::Queue(c) => self.queue(c),
            ControllerCmd::Policy(c) => self.policy(c),
            #[cfg(target_os = "linux")]
            ControllerCmd::Ebpf(c) => self.ebpf(c),
        }
    }

    fn new_client(&self) -> Result<Client> {
        let addr = match self.addr {
            IpAddr::V4(a) => IpAddr::V4(a),
            IpAddr::V6(a) => {
                if let Some(v4) = a.to_ipv4() {
                    IpAddr::V4(v4)
                } else {
                    IpAddr::V6(a)
                }
            }
        };

        let client = Client::new(
            (
                addr,
                self.port.expect("need input a port to connect debugger"),
            )
                .into(),
        )?;
        Ok(client)
    }

    /*
    $ deepflow-agent-ctl list
    deepflow-agent-ctl listening udp port 30035 to find deepflow-agent

    -----------------------------------------------------------------------------------------------------
    VTAP ID        HOSTNAME                     IP                                            PORT
    -----------------------------------------------------------------------------------------------------
    1              ubuntu                       ::ffff:127.0.0.1                              42700
    */
    fn list(&self) -> Result<()> {
        let beacon_port = if let Some(port) = self.port {
            port
        } else {
            DEFAULT_CONTROLLER_PORT
        };

        let server = UdpSocket::bind((Ipv6Addr::UNSPECIFIED, beacon_port))?;
        let mut vtap_map = HashSet::new();

        println!(
            "deepflow-agent-ctl listening udp port {} to find deepflow-agent\n",
            beacon_port
        );
        println!("{:-<100}", "");
        println!(
            "{:<14} {:<28} {:45} {}",
            "VTAP ID", "HOSTNAME", "IP", "PORT"
        );
        println!("{:-<100}", "");
        loop {
            let mut buf = [0u8; 1024];
            match server.recv_from(&mut buf) {
                Ok((n, a)) => {
                    if n == 0 {
                        continue;
                    }

                    // 过滤trident的beacon包
                    let length = DEEPFLOW_AGENT_BEACON.as_bytes().len();
                    if buf
                        .get(..length)
                        .filter(|&s| s == DEEPFLOW_AGENT_BEACON.as_bytes())
                        .is_none()
                    {
                        continue;
                    }

                    let beacon: Beacon =
                        decode_from_std_read(&mut &buf[length..n], config::standard())?;
                    if !vtap_map.contains(&beacon.agent_id) {
                        println!(
                            "{:<14} {:<28} {:<45} {}",
                            beacon.agent_id,
                            beacon.hostname,
                            a.ip(),
                            a.port()
                        );
                        vtap_map.insert(beacon.agent_id);
                    }
                }
                Err(e) => return Err(anyhow!("{}", e)),
            };
        }
    }

    fn rpc(&self, c: RpcCmd) -> Result<()> {
        if self.port.is_none() {
            return Err(anyhow!(ERR_PORT_MSG));
        }
        let mut client = self.new_client()?;

        let payload = match c.get {
            RpcData::Acls => RpcMessage::Acls(None),
            RpcData::Config => RpcMessage::Config(None),
            RpcData::Platform => RpcMessage::PlatformData(None),
            RpcData::CaptureNetworkTypes => RpcMessage::CaptureNetworkTypes(None),
            RpcData::Cidr => RpcMessage::Cidr(None),
            RpcData::Groups => RpcMessage::Groups(None),
            RpcData::Segments => RpcMessage::Segments(None),
            RpcData::Version => RpcMessage::Version(None),
        };

        let msg = Message {
            module: Module::Rpc,
            msg: payload,
        };
        client.send_to(msg)?;

        loop {
            let Ok(resp) = client.recv::<RpcMessage>() else {
                continue;
            };
            match resp {
                RpcMessage::Acls(v)
                | RpcMessage::PlatformData(v)
                | RpcMessage::CaptureNetworkTypes(v)
                | RpcMessage::Cidr(v)
                | RpcMessage::Groups(v)
                | RpcMessage::Segments(v) => match v {
                    Some(v) => println!("{}", v),
                    None => return Err(anyhow!(format!("{:?} data is empty", c.get))),
                },
                RpcMessage::Config(s) | RpcMessage::Version(s) => match s {
                    Some(s) => println!("{}", s),
                    None => return Err(anyhow!(format!("{:?} is empty", c.get))),
                },
                RpcMessage::Fin => return Ok(()),
                RpcMessage::Err(e) => return Err(anyhow!(e)),
            }
        }
    }

    fn queue(&self, c: QueueCmd) -> Result<()> {
        if self.port.is_none() {
            return Err(anyhow!(ERR_PORT_MSG));
        }
        if c.on.is_some() && c.off.is_some() {
            return Err(anyhow!("error: --on and --off cannot set at the same time"));
        }

        let mut client = self.new_client()?;
        if c.show {
            let msg = Message {
                module: Module::Queue,
                msg: QueueMessage::Names(None),
            };
            client.send_to(msg)?;

            println!("available queues: ");

            loop {
                let Ok(res) = client.recv::<QueueMessage>() else {
                    continue;
                };
                match res {
                    QueueMessage::Names(e) => match e {
                        Some(e) => {
                            for (i, (s, e)) in e.into_iter().enumerate() {
                                println!(
                                    "{:<5} {:<45} {}",
                                    i,
                                    s,
                                    if e { "enabled" } else { "disabled" }
                                );
                            }
                        }
                        None => return Err(anyhow!("cannot get queue names")),
                    },
                    QueueMessage::Fin => return Ok(()),
                    QueueMessage::Err(e) => return Err(anyhow!(e)),
                    _ => unreachable!(),
                }
            }
        }

        if c.clear {
            let msg = Message {
                module: Module::Queue,
                msg: QueueMessage::Clear,
            };
            client.send_to(msg)?;

            let Ok(res) = client.recv::<QueueMessage>() else {
                return Ok(());
            };
            match res {
                QueueMessage::Fin => {
                    println!("turn off all queues successful");
                    return Ok(());
                }
                QueueMessage::Err(e) => return Err(anyhow!(e)),
                _ => unreachable!(),
            }
        }

        if let Some(s) = c.off {
            let msg = Message {
                module: Module::Queue,
                msg: QueueMessage::Off(s.clone()),
            };
            client.send_to(msg)?;
            let Ok(res) = client.recv::<QueueMessage>() else {
                return Ok(());
            };
            match res {
                QueueMessage::Fin => {
                    println!("turn off queue={} successful", s);
                    return Ok(());
                }
                QueueMessage::Err(e) => return Err(anyhow!(e)),
                _ => unreachable!(),
            }
        }

        if let Some((s, d)) = c.on.zip(c.duration) {
            if d == 0 {
                return Err(anyhow!("zero duration isn't allowed"));
            }

            let dur = Duration::from_secs(d);

            let msg = Message {
                module: Module::Queue,
                msg: QueueMessage::On((s, dur)),
            };
            client.send_to(msg)?;

            let Ok(res) = client.recv::<QueueMessage>() else {
                return Ok(());
            };
            if let QueueMessage::Err(e) = res {
                return Err(anyhow!(e));
            }
            println!("loading queue item...");
            let mut seq = 0;
            loop {
                let Ok(res) = client.recv::<QueueMessage>() else {
                    continue;
                };
                match res {
                    QueueMessage::Send(e) => {
                        println!("MSG-{} {}", seq, e);
                        seq += 1;
                    }
                    QueueMessage::Continue => {
                        println!("nothing received for {:?}", DEBUG_QUEUE_IDLE_TIMEOUT);
                        continue;
                    }
                    QueueMessage::Fin => return Ok(()),
                    QueueMessage::Err(e) => return Err(anyhow!(e)),
                    _ => unreachable!(),
                }
            }
        }

        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn decode_entry(decoder: &mut ZlibDecoder<Vec<u8>>, entry: &[u8]) -> Result<String> {
        decoder.write_all(entry)?;
        let b = decoder.reset(vec![])?;
        let result = String::from_utf8(b)?;
        Ok(result)
    }

    #[cfg(target_os = "linux")]
    fn platform(&self, c: PlatformCmd) -> Result<()> {
        if self.port.is_none() {
            return Err(anyhow!(ERR_PORT_MSG));
        }
        let mut client = self.new_client()?;
        if c.mac_mappings {
            let msg = Message {
                module: Module::Platform,
                msg: PlatformMessage::MacMappings(None),
            };
            client.send_to(msg)?;
            println!("Interface Index \t MAC address");

            loop {
                let Ok(res) = client.recv::<PlatformMessage>() else {
                    continue;
                };
                match res {
                    PlatformMessage::MacMappings(e) => {
                        match e {
                            /*
                            $ deepflow-agent-ctl -p 42700 platform --mac-mappings
                            Interface Index          MAC address
                            12                       01:02:03:04:05:06
                            13                       01:02:03:04:05:06
                            14                       01:02:03:04:05:06
                            */
                            Some(e) => {
                                for (idx, m) in e {
                                    println!("{:<15} \t {}", idx, m);
                                }
                            }
                            None => return Err(anyhow!("mac mappings is empty")),
                        }
                    }
                    PlatformMessage::Fin => return Ok(()),
                    _ => unreachable!(),
                }
            }
        }

        if let Some(r) = c.k8s_get {
            if let Resource::Version = r {
                let msg = Message {
                    module: Module::Platform,
                    msg: PlatformMessage::Version(None),
                };
                client.send_to(msg)?;
                loop {
                    let Ok(res) = client.recv::<PlatformMessage>() else {
                        continue;
                    };
                    match res {
                        PlatformMessage::Version(v) => {
                            /*
                            $ deepflow-agent-ctl -p 54911 platform --k8s-get version
                            k8s-api-watcher-version xxx
                            */
                            match v {
                                Some(v) => println!("{}", v),
                                None => return Err(anyhow!("server version is empty")),
                            }
                        }
                        PlatformMessage::Fin => return Ok(()),
                        _ => unreachable!(),
                    }
                }
            }

            let msg = Message {
                module: Module::Platform,
                msg: PlatformMessage::Watcher(r.to_string().into_bytes()),
            };
            client.send_to(msg)?;
            let mut decoder = ZlibDecoder::new(vec![]);
            loop {
                let Ok(res) = client.recv::<PlatformMessage>() else {
                    continue;
                };
                match res {
                    PlatformMessage::Watcher(v) => {
                        /*
                        $ deepflow-agent-ctl -p 54911 platform --k8s-get node
                        nodes entries...
                        */
                        match Self::decode_entry(&mut decoder, v.as_slice()) {
                            Ok(v) => println!("{}", v),
                            Err(e) => eprintln!("{}", e),
                        }
                    }
                    PlatformMessage::NotFound => return Err(anyhow!("no data")),
                    PlatformMessage::Fin => return Ok(()),
                    _ => unreachable!(),
                }
            }
        }
        Ok(())
    }

    fn policy(&self, c: PolicyCmd) -> Result<()> {
        if self.port.is_none() {
            return Err(anyhow!(ERR_PORT_MSG));
        }

        let mut client = self.new_client()?;
        match c.subcmd {
            PolicySubCmd::Monitor => {
                client.send_to(Message {
                    module: Module::Policy,
                    msg: PolicyMessage::On,
                })?;

                loop {
                    let Ok(res) = client.recv::<PolicyMessage>() else {
                        continue;
                    };
                    match res {
                        PolicyMessage::Context(c) => println!("{}", c),
                        PolicyMessage::Done => return Ok(()),
                        PolicyMessage::Err(e) => {
                            println!("{}", e);
                            return Ok(());
                        }
                        _ => unreachable!(),
                    }
                }
            }
            PolicySubCmd::Show => {
                client.send_to(Message {
                    module: Module::Policy,
                    msg: PolicyMessage::Show,
                })?;

                let mut count = 1;
                loop {
                    let Ok(res) = client.recv::<PolicyMessage>() else {
                        continue;
                    };
                    match res {
                        PolicyMessage::Title(t) => {
                            println!("{}", t);
                            continue;
                        }
                        PolicyMessage::Context(c) => println!("\t{}: {}", count, c),
                        PolicyMessage::Done => return Ok(()),
                        PolicyMessage::Err(e) => {
                            println!("{}", e);
                            return Ok(());
                        }
                        _ => unreachable!(),
                    }
                    count += 1;
                }
            }
            PolicySubCmd::Analyzing(args) => {
                client.send_to(Message {
                    module: Module::Policy,
                    msg: PolicyMessage::Analyzing(args.id.unwrap_or_default()),
                })?;

                let Ok(res) = client.recv::<PolicyMessage>() else {
                    return Ok(());
                };
                match res {
                    PolicyMessage::Context(c) => println!("{}", c),
                    _ => unreachable!(),
                }
                Ok(())
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn ebpf(&self, c: EbpfCmd) -> Result<()> {
        if self.port.is_none() {
            return Err(anyhow!(ERR_PORT_MSG));
        }

        let mut client = self.new_client()?;
        match c.subcmd {
            EbpfSubCmd::Cpdbg(arg) => {
                client.send_to(Message {
                    module: Module::Ebpf,
                    msg: EbpfMessage::Cpdbg(arg.duration),
                })?;
            }
            EbpfSubCmd::Datadump(arg) => {
                client.send_to(Message {
                    module: Module::Ebpf,
                    msg: EbpfMessage::DataDump((arg.pid, arg.name, arg.proto, arg.duration)),
                })?;
            }
        }

        loop {
            let Ok(res) = client.recv::<EbpfMessage>() else {
                continue;
            };
            match res {
                EbpfMessage::Context((seq, c)) => {
                    println!("SEQ {}: {}", seq, String::from_utf8_lossy(&c))
                }
                EbpfMessage::Done => return Ok(()),
                EbpfMessage::Error(e) => {
                    println!("{}", e);
                    return Ok(());
                }
                _ => unreachable!(),
            }
        }
    }
}

fn main() {
    let mut controller = Controller::new();
    if let Err(e) = controller.dispatch() {
        eprintln!("{}", e);
    }
}
