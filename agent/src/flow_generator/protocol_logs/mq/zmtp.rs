use crate::{
    common::{
        flow::{L7PerfStats, L7Protocol, PacketDirection},
        l7_protocol_info::{L7ProtocolInfo, L7ProtocolInfoInterface},
        l7_protocol_log::{L7ParseResult, L7ProtocolParserInterface, ParseParam},
        meta_packet::EbpfFlags,
    },
    config::handler::LogParserConfig,
    flow_generator::{
        error::{Error, Result},
        protocol_logs::{
            pb_adapter::{ExtendedInfo, KeyVal, L7ProtocolSendLog, L7Request, L7Response},
            set_captured_byte, AppProtoHead, L7ResponseStatus, LogMessageType,
        },
    },
    plugin::wasm::{
        wasm_plugin::{zmtp_message, ZmtpMessage},
        WasmData,
    },
};
use serde::Serialize;
use std::fmt;

#[derive(Serialize, Clone, Debug)]
enum Mechanism {
    NULL,
    PLAIN,
    CURVE,
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
enum SocketType {
    REQ,
    REP,
    DEALER,
    ROUTER,
    PUB,
    SUB,
    XPUB,
    XSUB,
    PUSH,
    PULL,
    PAIR,
}

impl SocketType {
    fn from_bytes(s: &[u8]) -> Option<SocketType> {
        match s {
            b"REQ" => Some(SocketType::REQ),
            b"REP" => Some(SocketType::REP),
            b"DEALER" => Some(SocketType::DEALER),
            b"ROUTER" => Some(SocketType::ROUTER),
            b"PUB" => Some(SocketType::PUB),
            b"SUB" => Some(SocketType::SUB),
            b"XPUB" => Some(SocketType::XPUB),
            b"XSUB" => Some(SocketType::XSUB),
            b"PUSH" => Some(SocketType::PUSH),
            b"PULL" => Some(SocketType::PULL),
            b"PAIR" => Some(SocketType::PAIR),
            _ => None,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq)]
enum FrameType {
    Greeting,
    Command,
    Message,
    Unknown,
}

impl fmt::Display for FrameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameType::Greeting => write!(f, "Greeting"),
            FrameType::Command => write!(f, "Command"),
            FrameType::Message => write!(f, "Message"),
            FrameType::Unknown => write!(f, "Unknown"),
        }
    }
}

impl Default for FrameType {
    fn default() -> Self {
        FrameType::Unknown
    }
}

impl FrameType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FrameType::Greeting => "Greeting",
            FrameType::Command => "Command",
            FrameType::Message => "Message",
            FrameType::Unknown => "Unknown",
        }
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ZmtpInfo {
    msg_type: LogMessageType,
    req_msg_size: Option<u64>,
    res_msg_size: Option<u64>,
    is_tls: bool,
    rtt: u64,
    status: L7ResponseStatus,
    err_msg: Option<String>,
    subscription: Option<String>,

    major_version: Option<u8>,
    minor_version: Option<u8>,
    more_frames: Option<bool>,
    socket_type: Option<SocketType>,
    frame_type: FrameType,
    mechanism: Option<Mechanism>,
    command_name: Option<String>,
    payload: Vec<u8>,

    captured_request_byte: u32,
    captured_response_byte: u32,

    #[serde(skip)]
    attributes: Vec<KeyVal>,
    #[serde(skip)]
    l7_protocol_str: Option<String>,

    #[serde(skip)]
    is_on_blacklist: bool,
}

impl ZmtpInfo {
    fn get_version(&self) -> String {
        match (self.major_version, self.minor_version) {
            (Some(major), Some(minor)) => format!("{}.{}", major, minor),
            (Some(major), None) => format!("{}", major),
            _ => "".to_string(),
        }
    }
    fn merge(&mut self, res: &mut Self) {
        if self.res_msg_size.is_none() {
            self.res_msg_size = res.res_msg_size.take();
        }
        if self.status == L7ResponseStatus::Ok {
            self.status = res.status;
            self.err_msg = res.err_msg.take();
        }
        self.captured_response_byte = res.captured_response_byte;
        if res.is_on_blacklist {
            self.is_on_blacklist = res.is_on_blacklist;
        }
    }
    fn wasm_hook(&mut self, param: &ParseParam, payload: &[u8]) {
        let mut vm_ref = param.wasm_vm.borrow_mut();
        let Some(vm) = vm_ref.as_mut() else {
            return;
        };
        let wasm_data = WasmData::from_request(
            L7Protocol::ZMTP,
            ZmtpMessage {
                payload: self.payload.drain(..).collect(),
                subscription: self
                    .subscription
                    .clone()
                    .map(|s| zmtp_message::Subscription::MatchPattern(s)),
            },
        );
        if let Some(custom) = vm.on_custom_message(payload, param, wasm_data) {
            if !custom.attributes.is_empty() {
                self.attributes.extend(custom.attributes);
            }
            if custom.proto_str.len() > 0 {
                self.l7_protocol_str = Some(custom.proto_str);
            }
        }
    }

    fn set_is_on_blacklist(&mut self, config: &LogParserConfig) {
        if let Some(t) = config.l7_log_blacklist_trie.get(&L7Protocol::ZMTP) {
            self.is_on_blacklist = t.request_type.is_on_blacklist(self.frame_type.as_str())
                || self
                    .subscription
                    .as_ref()
                    .map(|p| {
                        t.request_domain.is_on_blacklist(p) || t.request_resource.is_on_blacklist(p)
                    })
                    .unwrap_or_default();
        }
    }
}

impl From<ZmtpInfo> for L7ProtocolSendLog {
    fn from(f: ZmtpInfo) -> Self {
        let flags = if f.is_tls {
            EbpfFlags::TLS.bits()
        } else {
            EbpfFlags::NONE.bits()
        };
        L7ProtocolSendLog {
            req_len: f.req_msg_size.map(|x| x as u32),
            resp_len: f.res_msg_size.map(|x| x as u32),
            captured_request_byte: f.captured_request_byte,
            captured_response_byte: f.captured_response_byte,
            row_effect: 0,
            req: L7Request {
                req_type: f.frame_type.to_string(),
                domain: f.subscription.clone().unwrap_or_default(),
                resource: f.subscription.clone().unwrap_or_default(),
                ..Default::default()
            },
            resp: L7Response {
                status: f.status,
                exception: f.err_msg.clone().unwrap_or_default(),
                ..Default::default()
            },
            version: Some(f.get_version()),
            flags,
            ext_info: Some(ExtendedInfo {
                attributes: {
                    if f.attributes.is_empty() {
                        None
                    } else {
                        Some(f.attributes)
                    }
                },
                protocol_str: f.l7_protocol_str,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

impl L7ProtocolInfoInterface for ZmtpInfo {
    fn session_id(&self) -> Option<u32> {
        None
    }
    fn merge_log(&mut self, other: &mut L7ProtocolInfo) -> Result<()> {
        if let L7ProtocolInfo::ZmtpInfo(other) = other {
            self.merge(other);
        }
        Ok(())
    }
    fn app_proto_head(&self) -> Option<AppProtoHead> {
        Some(AppProtoHead {
            proto: L7Protocol::ZMTP,
            msg_type: self.msg_type,
            rrt: self.rtt,
        })
    }
    fn is_tls(&self) -> bool {
        self.is_tls
    }
    fn get_request_domain(&self) -> String {
        self.subscription.clone().unwrap_or_default()
    }
    fn is_on_blacklist(&self) -> bool {
        self.is_on_blacklist
    }
}

#[derive(Default)]
pub struct ZmtpLog {
    major_version: Option<u8>,
    minor_version: Option<u8>,
    client_socket_type: Option<SocketType>,
    server_socket_type: Option<SocketType>,
    mechanism: Option<Mechanism>,

    perf_stats: Option<L7PerfStats>,
    last_is_on_blacklist: bool,
}

fn parse_byte(payload: &[u8]) -> Option<(&[u8], u8)> {
    if payload.is_empty() {
        return None;
    }
    Some((&payload[1..], payload[0]))
}
fn parse_bytes(payload: &[u8], length: usize) -> Option<(&[u8], &[u8])> {
    if payload.len() < length {
        return None;
    }
    Some((&payload[length..], &payload[..length]))
}
fn parse_integer(payload: &[u8]) -> Option<(&[u8], u32)> {
    if payload.len() < 4 {
        return None;
    }
    let mut value = 0;
    for &byte in payload.iter().take(4) {
        value = (value << 8) | byte as u32;
    }
    Some((&payload[4..], value))
}
fn parse_long(payload: &[u8]) -> Option<(&[u8], u64)> {
    if payload.len() < 8 {
        return None;
    }
    let mut value = 0;
    for &byte in payload.iter().take(8) {
        value = (value << 8) | byte as u64;
    }
    Some((&payload[8..], value))
}

fn check_major_version(major_version: u8) -> Result<()> {
    if major_version != 3 {
        return Err(Error::ZmtpLogParseFailed);
    }
    Ok(())
}
fn check_minor_version(major_version: u8, minor_version: u8) -> Result<()> {
    // 3.0, 3.1
    if major_version == 3 && minor_version <= 1 {
        return Ok(());
    }
    Err(Error::ZmtpLogParseFailed)
}

impl ZmtpLog {
    fn parse_greeting<'a>(payload: &'a [u8], info: &mut ZmtpInfo) -> Result<&'a [u8]> {
        info.frame_type = FrameType::Greeting;
        let payload = if let Some(0xff) = payload.get(0) {
            // full greeting header
            // signature: 0xff 8OCTET 0x7f
            let (payload, signature) = parse_bytes(payload, 10).ok_or(Error::ZmtpLogParseFailed)?;
            if signature[9] != 0x7f {
                return Err(Error::ZmtpLogParseFailed);
            }
            // major version
            let (payload, major_version) = parse_byte(payload).ok_or(Error::ZmtpLogParseEOF)?;
            check_major_version(major_version)?;
            info.major_version = Some(major_version);
            // minor version
            let (payload, minor_version) = parse_byte(payload).ok_or(Error::ZmtpLogParseEOF)?;
            check_minor_version(major_version, minor_version)?;
            info.minor_version = Some(minor_version);
            payload
        } else if payload.len() == 53 {
            // partial greeting header
            // minor version
            let (payload, minor_version) = parse_byte(payload).ok_or(Error::ZmtpLogParseFailed)?;
            check_minor_version(3, minor_version)?;
            info.major_version = Some(3);
            info.minor_version = Some(minor_version);
            payload
        } else if payload.len() == 54 {
            // partial greeting header
            // major version
            let (payload, major_version) = parse_byte(payload).ok_or(Error::ZmtpLogParseFailed)?;
            check_major_version(major_version)?;
            info.major_version = Some(major_version);
            // minor version
            let (payload, minor_version) = parse_byte(payload).ok_or(Error::ZmtpLogParseFailed)?;
            check_minor_version(major_version, minor_version)?;
            info.minor_version = Some(minor_version);
            payload
        } else {
            return Err(Error::ZmtpLogParseFailed);
        };
        // mechanism
        let (payload, mechanism) = parse_bytes(payload, 20).ok_or(Error::ZmtpLogParseFailed)?;
        let mechanism = match &mechanism[..5] {
            b"NULL\0" => Mechanism::NULL,
            b"PLAIN" => Mechanism::PLAIN,
            b"CURVE" => Mechanism::CURVE,
            _ => return Err(Error::ZmtpLogParseFailed),
        };
        info.mechanism = Some(mechanism);
        // as-server
        let (payload, as_server) = parse_byte(payload).ok_or(Error::ZmtpLogParseFailed)?;
        if as_server > 1 {
            return Err(Error::ZmtpLogParseFailed);
        }
        // filler
        let (payload, filler) = parse_bytes(payload, 31).ok_or(Error::ZmtpLogParseFailed)?;
        if filler.iter().any(|&x| x != 0) {
            return Err(Error::ZmtpLogParseFailed);
        }
        Ok(payload)
    }
    fn parse_command<'a>(
        payload: &'a [u8],
        info: &mut ZmtpInfo,
        mechanism: Option<Mechanism>,
    ) -> Result<&'a [u8]> {
        info.frame_type = FrameType::Command;
        // command size
        let (payload, size_type) = parse_byte(payload).ok_or(Error::ZmtpLogParseFailed)?;
        info.more_frames = Some(false);
        let (payload, size) = match size_type {
            0x04 => {
                // short-size
                let (payload, size) = parse_byte(payload).ok_or(Error::ZmtpLogParseFailed)?;
                (payload, size as u64)
            }
            0x06 => {
                // long-size
                let (payload, size) = parse_long(payload).ok_or(Error::ZmtpLogParseFailed)?;
                // size are unlikely to surpass 2^31
                if size < u8::MAX as u64 || size > i32::MAX as u64 {
                    return Err(Error::ZmtpLogParseFailed);
                }
                (payload, size as u64)
            }
            _ => return Err(Error::ZmtpLogParseFailed),
        };
        info.req_msg_size = Some(size);
        // command body
        let (payload, length) = parse_byte(payload).ok_or(Error::ZmtpLogParseFailed)?;
        // Due to a libzmq bug, "\x05ERROR" is treated as "\x5e" "RROR",
        // so we process it as an exceptional case.
        let payload =
            if length == 0x5e && payload.get(0..4) == Some(b"RROR").map(|arr| arr as &[u8]) {
                info.command_name = Some("ERROR".to_string());
                &payload[4..]
            } else {
                // Currently, the shortest command names are "PING", "PONG",
                // and "JOIN" with the length of 4.
                if length < 4 {
                    return Err(Error::ZmtpLogParseFailed);
                }
                let (payload, command_name) =
                    parse_bytes(payload, length as usize).ok_or(Error::ZmtpLogParseFailed)?;
                // only allow uppercase ASCII characters
                if !command_name.iter().all(|&x| x.is_ascii_uppercase()) {
                    return Err(Error::ZmtpLogParseFailed);
                }
                info.command_name = Some(String::from_utf8_lossy(command_name).to_string());
                payload
            };
        if info.command_name.as_ref().unwrap() == "ERROR" {
            info.status = L7ResponseStatus::ClientError;
            // error message
            let (payload, length) = parse_byte(payload).ok_or(Error::ZmtpLogParseEOF)?;
            let (payload, err_msg) =
                parse_bytes(payload, length as usize).ok_or(Error::ZmtpLogParseEOF)?;
            info.err_msg = Some(String::from_utf8_lossy(err_msg).to_string());
            return Ok(payload);
        }
        let remaining = size
            .checked_sub(1 + length as u64)
            .ok_or(Error::ZmtpLogParseFailed)?;
        // command data
        let (payload, data) = match parse_bytes(payload, remaining as usize) {
            Some((payload, data)) => (payload, data),
            None => (payload[0..0].as_ref(), payload),
        };
        // parse socket-type only for NULL and PLAIN mechanism
        // due to command data is encrypted for CURVE mechanism
        match info.command_name.as_ref().unwrap().as_str() {
            "READY" | "INITIATE" => {
                match mechanism {
                    Some(Mechanism::NULL) | Some(Mechanism::PLAIN) | None => {
                        // socket type
                        if let Some(data) = data
                            .windows(11)
                            .position(|w| w == b"Socket-Type")
                            .map(|i| &data[i + 11..])
                        {
                            if let Some((data, length)) = parse_integer(data) {
                                if let Some((_, socket_type)) = parse_bytes(data, length as usize) {
                                    let socket_type = SocketType::from_bytes(socket_type)
                                        .ok_or(Error::ZmtpLogParseFailed)?;
                                    info.socket_type = Some(socket_type);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            "SUBSCRIBE" => match mechanism {
                Some(Mechanism::NULL) | Some(Mechanism::PLAIN) | None => {
                    info.subscription = Some(String::from_utf8_lossy(data).to_string());
                }
                _ => {}
            },
            _ => {}
        }
        if data.len() < remaining as usize {
            Err(Error::ZmtpLogParseEOF)
        } else {
            Ok(payload)
        }
    }
    fn parse_message<'a>(payload: &'a [u8], info: &mut ZmtpInfo) -> Result<&'a [u8]> {
        info.frame_type = FrameType::Message;
        // message size
        let (payload, size_type) = parse_byte(payload).ok_or(Error::ZmtpLogParseFailed)?;
        info.more_frames = match size_type {
            0x00 | 0x02 => Some(false),
            0x01 | 0x03 => Some(true),
            _ => return Err(Error::ZmtpLogParseFailed),
        };
        let (payload, size) = match size_type {
            0x00 | 0x01 => {
                // short-size
                let (payload, size) = parse_byte(payload).ok_or(Error::ZmtpLogParseFailed)?;
                (payload, size as u64)
            }
            0x02 | 0x03 => {
                // long-size
                let (payload, size) = parse_long(payload).ok_or(Error::ZmtpLogParseFailed)?;
                // size are unlikely to surpass 2^31
                if size < u8::MAX as u64 || size > i32::MAX as u64 {
                    return Err(Error::ZmtpLogParseFailed);
                }
                (payload, size as u64)
            }
            _ => return Err(Error::ZmtpLogParseFailed),
        };
        info.req_msg_size = Some(size);
        // message body
        let (payload, bytes) = parse_bytes(payload, size as usize).ok_or(Error::ZmtpLogParseEOF)?;
        info.payload = bytes.to_vec();
        Ok(payload)
    }
    fn try_parse<'a>(&mut self, payload: &'a [u8], info: &mut ZmtpInfo) -> Result<&'a [u8]> {
        *info = ZmtpInfo::default();
        match Self::parse_greeting(payload, info) {
            Ok(payload) => {
                return Ok(payload);
            }
            Err(Error::ZmtpLogParseEOF) => return Err(Error::ZmtpLogParseEOF),
            _ => *info = ZmtpInfo::default(),
        }
        match Self::parse_command(payload, info, self.mechanism.clone()) {
            Ok(payload) => return Ok(payload),
            Err(Error::ZmtpLogParseEOF) => return Err(Error::ZmtpLogParseEOF),
            _ => *info = ZmtpInfo::default(),
        }
        // message lacks uniqueness, so we do not allow EOF
        match Self::parse_message(payload, info) {
            Ok(payload) => Ok(payload),
            _ => Err(Error::ZmtpLogParseFailed),
        }
    }
    fn check_protocol(payload: &[u8], param: &ParseParam) -> bool {
        if !param.ebpf_type.is_raw_protocol() {
            return false;
        }
        let mut parser = ZmtpLog::default();
        parser
            .parse(payload, param, true)
            .map(|infos| !infos.is_empty())
            .unwrap_or(false)
    }
    fn parse(
        &mut self,
        mut payload: &[u8],
        param: &ParseParam,
        strict_check: bool,
    ) -> Result<Vec<L7ProtocolInfo>> {
        if param.is_tls() {
            return Err(Error::ZmtpLogParseFailed);
        }
        let mut info_list = vec![];
        while !payload.is_empty() {
            let mut info = ZmtpInfo::default();
            payload = match self.try_parse(payload, &mut info) {
                Ok(p) => p,
                Err(Error::ZmtpLogParseEOF) => {
                    // always allow malformed greeting
                    if strict_check && info.frame_type != FrameType::Greeting {
                        return Err(Error::ZmtpLogParseFailed);
                    }
                    &payload[0..0]
                }
                Err(_) => return Err(Error::ZmtpLogParseFailed),
            };
            if param.direction == PacketDirection::ServerToClient {
                info.res_msg_size = info.req_msg_size.take();
                if info.status == L7ResponseStatus::ClientError {
                    info.status = L7ResponseStatus::ServerError;
                }
            }
            match info.frame_type {
                FrameType::Greeting | FrameType::Command => {
                    info.msg_type = LogMessageType::Session;
                    if info.socket_type.is_some() {
                        if param.direction == PacketDirection::ClientToServer {
                            self.client_socket_type = info.socket_type.clone();
                        } else {
                            self.server_socket_type = info.socket_type.clone();
                        }
                    }
                    info_list.push(L7ProtocolInfo::ZmtpInfo(info));
                }
                FrameType::Message => {
                    if strict_check {
                        continue;
                    }
                    if self.client_socket_type == Some(SocketType::REQ)
                        || self.client_socket_type == Some(SocketType::REP)
                        || self.server_socket_type == Some(SocketType::REQ)
                        || self.server_socket_type == Some(SocketType::REP)
                    {
                        if param.direction == PacketDirection::ClientToServer {
                            info.msg_type = LogMessageType::Request;
                        } else {
                            info.msg_type = LogMessageType::Response;
                        }
                    } else {
                        info.msg_type = LogMessageType::Session;
                    }
                    // discard delimiter
                    if info.more_frames != Some(true) || info.req_msg_size != Some(0) {
                        info_list.push(L7ProtocolInfo::ZmtpInfo(info));
                    }
                }
                _ => return Err(Error::ZmtpLogParseFailed),
            }
        }
        Ok(info_list)
    }
}

impl L7ProtocolParserInterface for ZmtpLog {
    fn check_payload(&mut self, payload: &[u8], param: &ParseParam) -> bool {
        Self::check_protocol(payload, param)
    }
    fn parse_payload(&mut self, payload: &[u8], param: &ParseParam) -> Result<L7ParseResult> {
        if self.perf_stats.is_none() && param.parse_perf {
            self.perf_stats = Some(L7PerfStats::default())
        };
        let mut info_list = self.parse(payload, param, false)?;

        info_list.iter_mut().for_each(|info| {
            let info = match info {
                L7ProtocolInfo::ZmtpInfo(info) => info,
                _ => return,
            };
            set_captured_byte!(info, param);
            info.wasm_hook(param, payload);
            if let Some(config) = param.parse_config {
                info.set_is_on_blacklist(config);
            }
            if !info.is_on_blacklist && !self.last_is_on_blacklist {
                match param.direction {
                    PacketDirection::ClientToServer => {
                        self.perf_stats.as_mut().map(|p| p.inc_req());
                    }
                    PacketDirection::ServerToClient => {
                        self.perf_stats.as_mut().map(|p| p.inc_resp());
                    }
                }
                match info.status {
                    L7ResponseStatus::ClientError => {
                        self.perf_stats
                            .as_mut()
                            .map(|p: &mut L7PerfStats| p.inc_req_err());
                    }
                    L7ResponseStatus::ServerError => {
                        self.perf_stats
                            .as_mut()
                            .map(|p: &mut L7PerfStats| p.inc_resp_err());
                    }
                    _ => {}
                }
                info.cal_rrt(param).map(|rtt| {
                    info.rtt = rtt;
                    self.perf_stats.as_mut().map(|p| p.update_rrt(rtt));
                });
            }
            self.last_is_on_blacklist = info.is_on_blacklist;
        });

        if !param.parse_log {
            Ok(L7ParseResult::None)
        } else if info_list.len() == 1 {
            Ok(L7ParseResult::Single(info_list.pop().unwrap()))
        } else if info_list.len() > 1 {
            Ok(L7ParseResult::Multi(info_list))
        } else {
            Ok(L7ParseResult::None)
        }
    }
    fn protocol(&self) -> L7Protocol {
        L7Protocol::ZMTP
    }
    fn perf_stats(&mut self) -> Option<L7PerfStats> {
        self.perf_stats.take()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::path::Path;
    use std::{fs, rc::Rc};

    use super::*;

    use crate::common::l7_protocol_log::L7PerfCache;
    use crate::flow_generator::L7_RRT_CACHE_CAPACITY;
    use crate::{
        common::{flow::PacketDirection, MetaPacket},
        utils::test::Capture,
    };

    const FILE_DIR: &str = "resources/test/flow_generator/zmtp";

    fn run(name: &str) -> String {
        let capture = Capture::load_pcap(Path::new(FILE_DIR).join(name), Some(1024));
        let log_cache = Rc::new(RefCell::new(L7PerfCache::new(L7_RRT_CACHE_CAPACITY)));
        let mut packets = capture.as_meta_packets();
        if packets.is_empty() {
            return "".to_string();
        }

        let mut zmtp = ZmtpLog::default();
        let mut output: String = String::new();
        let first_dst_port = packets[0].lookup_key.dst_port;
        for packet in packets.iter_mut() {
            packet.lookup_key.direction = if packet.lookup_key.dst_port == first_dst_port {
                PacketDirection::ClientToServer
            } else {
                PacketDirection::ServerToClient
            };
            let payload = match packet.get_l4_payload() {
                Some(p) => p,
                None => continue,
            };
            let param = &mut ParseParam::new(
                packet as &MetaPacket,
                log_cache.clone(),
                Default::default(),
                #[cfg(any(target_os = "linux", target_os = "android"))]
                Default::default(),
                true,
                true,
            );
            param.set_captured_byte(payload.len());

            let is_zmtp = ZmtpLog::check_protocol(payload, param);
            match zmtp.parse(payload, param, false) {
                Ok(info_list) => {
                    for info in info_list {
                        let info = match info {
                            L7ProtocolInfo::ZmtpInfo(info) => info,
                            _ => unreachable!(),
                        };
                        output.push_str(&format!("{:?} is_zmtp: {}\n", info, is_zmtp));
                    }
                }
                _ => output.push_str("parse failed\n"),
            }
        }
        output
    }

    #[test]
    fn check() {
        let files = vec![
            ("zmtp_null.pcap", "zmtp_null.result"),
            ("zmtp_plain.pcap", "zmtp_plain.result"),
            ("zmtp_subscribe_all.pcap", "zmtp_subscribe_all.result"),
            ("zmtp_subscribe_one.pcap", "zmtp_subscribe_one.result"),
            ("zmtp_subscribe_two.pcap", "zmtp_subscribe_two.result"),
            ("zmtp_error.pcap", "zmtp_error.result"),
        ];

        for item in files.iter() {
            let expected = fs::read_to_string(&Path::new(FILE_DIR).join(item.1)).unwrap();
            let output = run(item.0);

            if output != expected {
                let output_path = Path::new("actual.txt");
                fs::write(&output_path, &output).unwrap();
                assert!(
                    output == expected,
                    "output different from expected {}, written to {:?}",
                    item.1,
                    output_path
                );
            }
        }
    }
}
