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

pub mod consts;
pub(crate) mod dns;
pub(crate) mod fastcgi;
pub(crate) mod http;
pub(crate) mod mq;
mod parser;
pub mod pb_adapter;
pub(crate) mod ping;
pub(crate) mod plugin;
pub(crate) mod rpc;
pub(crate) mod sql;
pub use self::http::{check_http_method, parse_v1_headers, HttpInfo, HttpLog};
use self::pb_adapter::L7ProtocolSendLog;

pub use dns::{DnsInfo, DnsLog};
pub use mq::{
    AmqpInfo, AmqpLog, KafkaInfo, KafkaLog, MqttInfo, MqttLog, NatsInfo, NatsLog, OpenWireInfo,
    OpenWireLog, PulsarInfo, PulsarLog, RocketmqInfo, RocketmqLog, ZmtpInfo, ZmtpLog,
};
use num_enum::TryFromPrimitive;
pub use parser::{AppProto, MetaAppProto, PseudoAppProto, SessionAggregator};
pub use ping::{PingInfo, PingLog};
pub use rpc::{
    decode_new_rpc_trace_context_with_type, BrpcInfo, BrpcLog, DubboInfo, DubboLog, SofaRpcInfo,
    SofaRpcLog, TarsInfo, TarsLog, SOFA_NEW_RPC_TRACE_CTX_KEY,
};
pub use sql::{
    MemcachedInfo, MemcachedLog, MongoDBInfo, MongoDBLog, MysqlInfo, MysqlLog, PostgreInfo,
    PostgresqlLog, RedisInfo, RedisLog,
};

cfg_if::cfg_if! {
    if #[cfg(feature = "enterprise")] {
        pub(crate) mod tls;

        pub use sql::{OracleInfo, OracleLog};
        pub use rpc::{SomeIpInfo, SomeIpLog};
        pub use tls::{TlsInfo, TlsLog};
    }
}

#[cfg(test)]
pub use self::plugin::wasm::{get_wasm_parser, WasmLog};

use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str,
};

use base64::{prelude::BASE64_STANDARD, Engine};
use prost::Message;
use serde::{Serialize, Serializer};

use crate::{
    common::{
        ebpf::EbpfType,
        enums::{CaptureNetworkType, IpProtocol},
        flow::{L7Protocol, PacketDirection, SignalSource},
        tap_port::TapPort,
        Timestamp,
    },
    metric::document::TapSide,
};
use public::proto::flow_log;
use public::sender::{SendMessageType, Sendable};
use public::utils::net::MacAddr;

const NANOS_PER_MICRO: u64 = 1000;

#[derive(Serialize, Debug, PartialEq, Copy, Clone, Eq, TryFromPrimitive)]
#[repr(u8)]
pub enum L7ResponseStatus {
    Ok = 0,
    NotExist = 2,
    ServerError = 3,
    ClientError = 4,
    Timeout = 5,
}

impl Default for L7ResponseStatus {
    fn default() -> Self {
        L7ResponseStatus::Ok
    }
}

#[derive(Serialize, Debug, PartialEq, Eq, Clone, Copy, TryFromPrimitive)]
#[repr(u8)]
pub enum LogMessageType {
    Request,
    Response,
    Session,
    Other,
    Max,
}

impl Default for LogMessageType {
    fn default() -> Self {
        LogMessageType::Other
    }
}

impl From<PacketDirection> for LogMessageType {
    fn from(d: PacketDirection) -> LogMessageType {
        match d {
            PacketDirection::ClientToServer => LogMessageType::Request,
            PacketDirection::ServerToClient => LogMessageType::Response,
        }
    }
}

// 应用层协议原始数据类型
#[derive(Debug, PartialEq, Copy, Clone, Serialize)]
#[repr(u8)]
pub enum L7ProtoRawDataType {
    // 标准协议类型, 从af_packet, ebpf 的 tracepoint 或者 部分 uprobe(read/write等获取原始数据的hook点) 上报的数据都属于这个类型
    RawProtocol,
    // ebpf hook 在 go readHeader/writeHeader 获取http2原始未压缩的 header
    GoHttp2Uprobe,
}

impl Default for L7ProtoRawDataType {
    fn default() -> Self {
        return Self::RawProtocol;
    }
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct AppProtoHead {
    #[serde(rename = "l7_protocol")]
    pub proto: L7Protocol,
    pub msg_type: LogMessageType, // HTTP，DNS: request/response
    #[serde(rename = "response_duration")]
    pub rrt: u64, // HTTP，DNS时延: response-request
}

impl From<AppProtoHead> for flow_log::AppProtoHead {
    fn from(f: AppProtoHead) -> Self {
        flow_log::AppProtoHead {
            proto: f.proto as u32,
            msg_type: f.msg_type as u32,
            rrt: f.rrt * NANOS_PER_MICRO,
            ..Default::default()
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct AppProtoLogsBaseInfo {
    #[serde(serialize_with = "timestamp_to_micros")]
    pub start_time: Timestamp,
    #[serde(serialize_with = "timestamp_to_micros")]
    pub end_time: Timestamp,
    pub flow_id: u64,
    #[serde(serialize_with = "to_string_format")]
    pub tap_port: TapPort,
    pub signal_source: SignalSource,
    pub agent_id: u16,
    pub tap_type: CaptureNetworkType,
    pub tap_side: TapSide,
    pub biz_type: u8,
    #[serde(flatten)]
    pub head: AppProtoHead,

    /* L2 */
    #[serde(
        skip_serializing_if = "value_is_default",
        serialize_with = "to_string_format"
    )]
    pub mac_src: MacAddr,
    #[serde(
        skip_serializing_if = "value_is_default",
        serialize_with = "to_string_format"
    )]
    pub mac_dst: MacAddr,
    /* L3 ipv4 or ipv6 */
    pub ip_src: IpAddr,
    pub ip_dst: IpAddr,
    /* L3EpcID */
    pub l3_epc_id_src: i32,
    pub l3_epc_id_dst: i32,
    /* L4 */
    pub port_src: u16,
    pub port_dst: u16,
    /* First L7 TCP Seq */
    pub req_tcp_seq: u32,
    pub resp_tcp_seq: u32,

    /* GPID */
    pub gpid_0: u32,
    pub gpid_1: u32,

    /* EBPF Info */
    pub ebpf_type: EbpfType,
    #[serde(skip_serializing_if = "value_is_default")]
    pub process_id_0: u32,
    #[serde(skip_serializing_if = "value_is_default")]
    pub process_id_1: u32,
    #[serde(skip_serializing_if = "value_is_default")]
    pub process_kname_0: String,
    #[serde(skip_serializing_if = "value_is_default")]
    pub process_kname_1: String,
    #[serde(skip_serializing_if = "value_is_default")]
    pub syscall_trace_id_request: u64,
    #[serde(skip_serializing_if = "value_is_default")]
    pub syscall_trace_id_response: u64,
    #[serde(rename = "syscall_thread_0", skip_serializing_if = "value_is_default")]
    pub syscall_trace_id_thread_0: u32,
    #[serde(rename = "syscall_thread_1", skip_serializing_if = "value_is_default")]
    pub syscall_trace_id_thread_1: u32,
    #[serde(skip_serializing_if = "value_is_default")]
    pub syscall_coroutine_0: u64,
    #[serde(skip_serializing_if = "value_is_default")]
    pub syscall_coroutine_1: u64,
    #[serde(skip_serializing_if = "value_is_default")]
    pub syscall_cap_seq_0: u32,
    #[serde(skip_serializing_if = "value_is_default")]
    pub syscall_cap_seq_1: u32,

    pub protocol: IpProtocol,
    #[serde(skip)]
    pub is_vip_interface_src: bool,
    #[serde(skip)]
    pub is_vip_interface_dst: bool,
    #[serde(skip)]
    pub pod_id_0: u32,
    #[serde(skip)]
    pub pod_id_1: u32,
}

pub fn timestamp_to_micros<S>(d: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u64(d.as_micros() as u64)
}

pub fn to_string_format<D, S>(d: &D, serializer: S) -> Result<S::Ok, S::Error>
where
    D: fmt::Display,
    S: Serializer,
{
    serializer.serialize_str(&d.to_string())
}

pub fn value_is_default<T>(t: &T) -> bool
where
    T: Default + std::cmp::PartialEq,
{
    t == &T::default()
}

pub fn value_is_negative<T>(t: &T) -> bool
where
    T: Default + std::cmp::PartialEq + std::cmp::PartialOrd,
{
    t < &T::default()
}

impl From<AppProtoLogsBaseInfo> for flow_log::AppProtoLogsBaseInfo {
    fn from(f: AppProtoLogsBaseInfo) -> Self {
        let (ip4_src, ip4_dst, ip6_src, ip6_dst) = match (f.ip_src, f.ip_dst) {
            (IpAddr::V4(ip4), IpAddr::V4(ip4_1)) => {
                (ip4, ip4_1, Ipv6Addr::UNSPECIFIED, Ipv6Addr::UNSPECIFIED)
            }
            (IpAddr::V6(ip6), IpAddr::V6(ip6_1)) => {
                (Ipv4Addr::UNSPECIFIED, Ipv4Addr::UNSPECIFIED, ip6, ip6_1)
            }
            _ => panic!("{:?} ip_src,ip_dst type mismatch", &f),
        };
        flow_log::AppProtoLogsBaseInfo {
            start_time: f.start_time.as_nanos() as u64,
            end_time: f.end_time.as_nanos() as u64,
            flow_id: f.flow_id,
            tap_port: f.tap_port.0,
            vtap_id: f.agent_id as u32,
            tap_type: u16::from(f.tap_type) as u32,
            is_ipv6: f.ip_src.is_ipv6() as u32,
            tap_side: f.tap_side as u32,
            head: Some(f.head.into()),
            mac_src: f.mac_src.into(),
            mac_dst: f.mac_dst.into(),
            ip_src: u32::from_be_bytes(ip4_src.octets()),
            ip_dst: u32::from_be_bytes(ip4_dst.octets()),
            ip6_src: ip6_src.octets().to_vec(),
            ip6_dst: ip6_dst.octets().to_vec(),
            l3_epc_id_src: f.l3_epc_id_src,
            l3_epc_id_dst: f.l3_epc_id_dst,
            port_src: f.port_src as u32,
            port_dst: f.port_dst as u32,
            protocol: u8::from(f.protocol) as u32,
            is_vip_interface_src: f.is_vip_interface_src as u32,
            is_vip_interface_dst: f.is_vip_interface_dst as u32,
            req_tcp_seq: f.req_tcp_seq,
            resp_tcp_seq: f.resp_tcp_seq,
            process_id_0: f.process_id_0,
            process_id_1: f.process_id_1,
            process_kname_0: f.process_kname_0,
            process_kname_1: f.process_kname_1,
            syscall_trace_id_request: f.syscall_trace_id_request,
            syscall_trace_id_response: f.syscall_trace_id_response,
            syscall_trace_id_thread_0: f.syscall_trace_id_thread_0,
            syscall_trace_id_thread_1: f.syscall_trace_id_thread_1,
            syscall_cap_seq_0: f.syscall_cap_seq_0,
            syscall_cap_seq_1: f.syscall_cap_seq_1,
            syscall_coroutine_0: f.syscall_coroutine_0,
            syscall_coroutine_1: f.syscall_coroutine_1,
            gpid_0: f.gpid_0,
            gpid_1: f.gpid_1,
            pod_id_0: f.pod_id_0,
            pod_id_1: f.pod_id_1,
            biz_type: f.biz_type as u32,
        }
    }
}

impl AppProtoLogsBaseInfo {
    // 请求调用回应来合并
    fn merge(&mut self, log: &mut AppProtoLogsBaseInfo) {
        // adjust protocol when change, now only use for http2 change to grpc.
        if self.head.proto != log.head.proto {
            self.head.proto = log.head.proto;
        }
        if log.process_id_0 > 0 {
            self.process_id_0 = log.process_id_0;
            std::mem::swap(&mut self.process_kname_0, &mut log.process_kname_0);
        }
        if log.process_id_1 > 0 {
            self.process_id_1 = log.process_id_1;
            std::mem::swap(&mut self.process_kname_1, &mut log.process_kname_1);
        }
        if log.syscall_coroutine_0 > 0 {
            self.syscall_coroutine_0 = log.syscall_coroutine_0;
        }
        if log.syscall_coroutine_1 > 0 {
            self.syscall_coroutine_1 = log.syscall_coroutine_1;
        }
        if log.syscall_trace_id_thread_0 > 0 {
            self.syscall_trace_id_thread_0 = log.syscall_trace_id_thread_0;
        }
        if log.syscall_trace_id_thread_1 > 0 {
            self.syscall_trace_id_thread_1 = log.syscall_trace_id_thread_1;
        }
        if log.syscall_cap_seq_0 > 0 {
            self.syscall_cap_seq_0 = log.syscall_cap_seq_0;
        }
        if log.syscall_cap_seq_1 > 0 {
            self.syscall_cap_seq_1 = log.syscall_cap_seq_1;
        }

        if log.syscall_trace_id_request > 0 {
            self.syscall_trace_id_request = log.syscall_trace_id_request;
        }
        if log.syscall_trace_id_response > 0 {
            self.syscall_trace_id_response = log.syscall_trace_id_response;
        }

        self.start_time = log.start_time.min(self.start_time);
        self.end_time = log.end_time.max(self.end_time);

        if self.req_tcp_seq == 0 {
            self.req_tcp_seq = log.req_tcp_seq;
        }
        if self.resp_tcp_seq == 0 {
            self.resp_tcp_seq = log.resp_tcp_seq;
        }

        // go http2 uprobe  may merge multi times, if not req and resp merge can not set to session
        if self.head.msg_type != log.head.msg_type {
            self.head.msg_type = LogMessageType::Session;
        }

        self.head.rrt = if self.end_time > self.start_time {
            (self.end_time.as_micros() - self.start_time.as_micros()) as u64
        } else {
            0
        }
    }
}

#[derive(Debug)]
pub struct BoxAppProtoLogsData(pub Box<MetaAppProto>);

impl Sendable for BoxAppProtoLogsData {
    fn encode(self, buf: &mut Vec<u8>) -> Result<usize, prost::EncodeError> {
        let mut pb_proto_logs_data = flow_log::AppProtoLogsData {
            base: Some(self.0.base_info.into()),
            direction_score: self.0.direction_score as u32,
            ..Default::default()
        };

        let log: L7ProtocolSendLog = self.0.l7_info.into();
        log.fill_app_proto_log(&mut pb_proto_logs_data);
        pb_proto_logs_data
            .encode(buf)
            .map(|_| pb_proto_logs_data.encoded_len())
    }

    fn file_name(&self) -> &str {
        "l7_flow_log"
    }

    fn message_type(&self) -> SendMessageType {
        SendMessageType::ProtocolLog
    }

    fn to_kv_string(&self, kv_string: &mut String) {
        let json = serde_json::to_string(&(*self.0)).unwrap();
        kv_string.push_str(&json);
        kv_string.push('\n');
    }
}

impl fmt::Display for AppProtoLogsBaseInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Timestamp: {:?} Vtap_id: {} Flow_id: {} CaptureNetworkType: {} TapPort: {} TapSide: {:?}\n \
                \t{}_{}_{} -> {}_{}_{} Proto: {:?} Seq: {} -> {} VIP: {} -> {} EPC: {} -> {}\n \
                \tProcess: {}:{} -> {}:{} Trace-id: {} -> {} Thread: {} -> {} cap_seq: {} -> {}\n \
                \tL7Protocol: {:?} MsgType: {:?} Rrt: {}",
            self.start_time,
            self.agent_id,
            self.flow_id,
            self.tap_type,
            self.tap_port,
            self.tap_side,
            self.mac_src,
            self.ip_src,
            self.port_src,
            self.mac_dst,
            self.ip_dst,
            self.port_dst,
            self.protocol,
            self.req_tcp_seq,
            self.resp_tcp_seq,
            self.is_vip_interface_src,
            self.is_vip_interface_dst,
            self.l3_epc_id_src,
            self.l3_epc_id_dst,
            self.process_kname_0,
            self.process_id_0,
            self.process_kname_1,
            self.process_id_1,
            self.syscall_trace_id_request,
            self.syscall_trace_id_response,
            self.syscall_trace_id_thread_0,
            self.syscall_trace_id_thread_1,
            self.syscall_cap_seq_0,
            self.syscall_cap_seq_1,
            self.head.proto,
            self.head.msg_type,
            self.head.rrt
        )
    }
}

fn decode_base64_to_string(value: &str) -> String {
    let bytes = match BASE64_STANDARD.decode(value) {
        Ok(v) => v,
        Err(_) => return value.to_string(),
    };
    match str::from_utf8(&bytes) {
        Ok(s) => s.to_string(),
        Err(_) => value.to_string(),
    }
}

macro_rules! swap_if {
    ($this:expr, $field:ident, is_none, $other:expr) => {
        if $this.$field.is_none() {
            std::mem::swap(&mut $this.$field, &mut $other.$field);
        }
    };
    ($this:expr, $field:ident, is_empty, $other:expr) => {
        if $this.$field.is_empty() {
            std::mem::swap(&mut $this.$field, &mut $other.$field);
        }
    };
}

macro_rules! set_captured_byte {
    ($this:expr, $param:expr) => {
        match $this.msg_type {
            LogMessageType::Request => $this.captured_request_byte = $param.captured_byte as u32,
            LogMessageType::Response => $this.captured_response_byte = $param.captured_byte as u32,
            _ => {
                match LogMessageType::from($param.direction) {
                    LogMessageType::Request => {
                        $this.captured_request_byte = $param.captured_byte as u32
                    }
                    LogMessageType::Response => {
                        $this.captured_response_byte = $param.captured_byte as u32
                    }
                    _ => unimplemented!(),
                };
            }
        }
    };
}

pub(crate) use set_captured_byte;
pub(crate) use swap_if;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_l7_response_status_as_uint() {
        assert_eq!(L7ResponseStatus::Ok as u32, 0);
        assert_eq!(L7ResponseStatus::NotExist as u32, 2);
        assert_eq!(L7ResponseStatus::ServerError as u32, 3);
        assert_eq!(L7ResponseStatus::ClientError as u32, 4);
    }
}
