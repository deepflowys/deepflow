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

use std::cell::RefCell;
use std::fmt::Debug;
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use enum_dispatch::enum_dispatch;
use log::debug;
use lru::LruCache;

use super::ebpf::EbpfType;
use super::flow::{L7PerfStats, PacketDirection};
use super::l7_protocol_info::L7ProtocolInfo;
use super::MetaPacket;

use crate::common::meta_packet::{IcmpData, ProtocolData};
use crate::config::handler::LogParserConfig;
use crate::config::OracleConfig;
use crate::flow_generator::flow_map::FlowMapCounter;
use crate::flow_generator::protocol_logs::fastcgi::FastCGILog;
use crate::flow_generator::protocol_logs::plugin::custom_wrap::CustomWrapLog;
use crate::flow_generator::protocol_logs::plugin::get_custom_log_parser;
use crate::flow_generator::protocol_logs::sql::ObfuscateCache;
use crate::flow_generator::protocol_logs::{
    AmqpLog, BrpcLog, DnsLog, DubboLog, HttpLog, KafkaLog, MemcachedLog, MongoDBLog, MqttLog,
    MysqlLog, NatsLog, OpenWireLog, PingLog, PostgresqlLog, PulsarLog, RedisLog, RocketmqLog,
    SofaRpcLog, TarsLog, ZmtpLog,
};

use crate::flow_generator::{LogMessageType, Result};
#[cfg(any(target_os = "linux", target_os = "android"))]
use crate::plugin::c_ffi::SoPluginFunc;
use crate::plugin::wasm::WasmVm;

use public::enums::IpProtocol;
use public::l7_protocol::{CustomProtocol, L7Protocol, L7ProtocolChecker, L7ProtocolEnum};

/*
 所有协议都需要实现L7ProtocolLogInterface这个接口.
 其中，check_payload 用于MetaPacket判断应用层协议，parse_payload 用于解析具体协议.
 更具体就是遍历ALL_PROTOCOL的协议，用check判断协议，再用parse解析整个payload，得到L7ProtocolInfo.
 最后发送到server之前，调用into() 转成通用结构L7ProtocolSendLog.

 all protocol need implement L7ProtocolLogInterface trait.
 check_payload use to determine what protocol the payload is.
 parse_payload use to parse whole payload.
 more specifically, traversal all protocol from get_all_protocol,check the payload and then parse it,
 get the L7ProtocolInfo enum, finally convert to L7ProtocolSendLog struct and send to server.

 the parser flow:

    check_payload -> parse_payload -> reset --
                        /|\                  |
                         |                   |
                         |_____next packet___|
*/

macro_rules! count {
    () => (0);
    ($x:tt $($xs: tt)* ) => (1usize + count!($($xs)*));
}

macro_rules! impl_protocol_parser {
    (pub enum $name:ident { $($proto:ident($log_type:ty)),* $(,)? }) => {
        #[enum_dispatch(L7ProtocolParserInterface)]
        pub enum $name {
            Custom(CustomWrapLog),
            Http(HttpLog),
            $($proto($log_type)),*
        }

        impl L7ProtocolParser {
            pub fn as_str(&self) -> &'static str {
                match self {
                    Self::Http(p) => {
                        match p.protocol() {
                            L7Protocol::Http1 => return "HTTP",
                            L7Protocol::Http2 => return "HTTP2",
                            _ => unreachable!()
                        }
                    },
                    Self::Custom(_) => return "Custom",
                    $(
                        Self::$proto(_) => stringify!($proto),
                    )*
                }
            }
        }

        impl TryFrom<&str> for L7ProtocolParser {
            type Error = String;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                match value {
                    "HTTP" => Ok(Self::Http(HttpLog::new_v1())),
                    "HTTP2" => Ok(Self::Http(HttpLog::new_v2(false))),
                    "Custom"=>Ok(Self::Custom(Default::default())),
                    $(
                        stringify!($proto) => Ok(Self::$proto(Default::default())),
                    )*
                    _ => Err(String::from(format!("unknown protocol {}",value))),
                }
            }
        }

        pub fn get_parser(p: L7ProtocolEnum) -> Option<L7ProtocolParser> {
            match p {
                L7ProtocolEnum::L7Protocol(p) => match p {
                    L7Protocol::Http1 => Some(L7ProtocolParser::Http(HttpLog::new_v1())),
                    L7Protocol::Http2 => Some(L7ProtocolParser::Http(HttpLog::new_v2(false))),
                    L7Protocol::Grpc => Some(L7ProtocolParser::Http(HttpLog::new_v2(true))),

                    // in check_payload, need to get the default Custom by L7Protocol.
                    // due to Custom not in macro, need to define explicit
                    L7Protocol::Custom => Some(L7ProtocolParser::Custom(CustomWrapLog::default())),
                    $(
                        L7Protocol::$proto => Some(L7ProtocolParser::$proto(Default::default())),
                    )+
                    _ => None,
                },
                L7ProtocolEnum::Custom(p) => Some(get_custom_log_parser(p)),
            }
        }

        pub fn get_all_protocol() -> [L7ProtocolParser; 3 + count!($($proto)*)] {
            [
                L7ProtocolParser::Custom(Default::default()),
                L7ProtocolParser::Http(HttpLog::new_v1()),
                L7ProtocolParser::Http(HttpLog::new_v2(false)),
                $(
                    L7ProtocolParser::$proto(Default::default()),
                )+
            ]
        }
    }
}

// 内部实现的协议
// log的具体结构和实现在 src/flow_generator/protocol_logs/** 下
// 注意枚举名大小写，因为会用于字符串解析
// 大结构体（128B以上）注意加Box，减少内存使用
// =========================================================
// the inner implement protocol source code in src/flow_generator/protocol_logs/**
// enum name will be used to parse strings so case matters
// large structs (>128B) should be boxed to reduce memory consumption
//
cfg_if::cfg_if! {
    if #[cfg(not(feature = "enterprise"))] {
        impl_protocol_parser! {
            pub enum L7ProtocolParser {
                // http have two version but one parser, can not place in macro param.
                // custom must in first so can not place in macro
                DNS(DnsLog),
                SofaRPC(SofaRpcLog),
                MySQL(MysqlLog),
                Kafka(KafkaLog),
                Redis(RedisLog),
                MongoDB(MongoDBLog),
                Memcached(MemcachedLog),
                PostgreSQL(PostgresqlLog),
                Dubbo(DubboLog),
                FastCGI(FastCGILog),
                Brpc(BrpcLog),
                Tars(TarsLog),
                MQTT(MqttLog),
                AMQP(AmqpLog),
                NATS(NatsLog),
                Pulsar(PulsarLog),
                ZMTP(ZmtpLog),
                RocketMQ(RocketmqLog),
                OpenWire(OpenWireLog),
                Ping(PingLog),
                // add protocol below
            }
        }
    } else {
        impl_protocol_parser! {
            pub enum L7ProtocolParser {
                // http have two version but one parser, can not place in macro param.
                // custom must in first so can not place in macro
                DNS(DnsLog),
                SofaRPC(SofaRpcLog),
                MySQL(MysqlLog),
                Kafka(KafkaLog),
                Redis(RedisLog),
                MongoDB(MongoDBLog),
                Memcached(MemcachedLog),
                PostgreSQL(PostgresqlLog),
                Dubbo(DubboLog),
                FastCGI(FastCGILog),
                Brpc(BrpcLog),
                Tars(TarsLog),
                Oracle(crate::flow_generator::protocol_logs::sql::OracleLog),
                MQTT(MqttLog),
                AMQP(AmqpLog),
                NATS(NatsLog),
                Pulsar(PulsarLog),
                ZMTP(ZmtpLog),
                RocketMQ(RocketmqLog),
                OpenWire(OpenWireLog),
                TLS(crate::flow_generator::protocol_logs::tls::TlsLog),
                SomeIp(crate::flow_generator::protocol_logs::rpc::SomeIpLog),
                Ping(PingLog),
                // add protocol below
            }
        }
    }
}

pub enum L7ParseResult {
    Single(L7ProtocolInfo),
    Multi(Vec<L7ProtocolInfo>),
    None,
}

impl L7ParseResult {
    pub fn is_none(&self) -> bool {
        match self {
            L7ParseResult::None => true,
            _ => false,
        }
    }

    pub fn unwrap_single(self) -> L7ProtocolInfo {
        match self {
            L7ParseResult::Single(s) => s,
            L7ParseResult::Multi(_) => panic!("parse result is mutli but unwrap single"),
            L7ParseResult::None => panic!("parse result is none but unwrap single"),
        }
    }

    pub fn unwrap_multi(self) -> Vec<L7ProtocolInfo> {
        match self {
            L7ParseResult::Multi(m) => m,
            L7ParseResult::Single(_) => panic!("parse result is single but unwrap multi"),
            L7ParseResult::None => panic!("parse result is none but unwrap multi"),
        }
    }
}

#[enum_dispatch]
pub trait L7ProtocolParserInterface {
    fn check_payload(&mut self, payload: &[u8], param: &ParseParam) -> bool;
    // 协议解析
    fn parse_payload(&mut self, payload: &[u8], param: &ParseParam) -> Result<L7ParseResult>;
    // 返回协议号和协议名称，由于的bitmap使用u128，所以协议号不能超过128.
    // 其中 crates/public/src/l7_protocol.rs 里面的 pub const L7_PROTOCOL_xxx 是已实现的协议号.
    // ===========================================================================================
    // return protocol number and protocol string. because of bitmap use u128, so the max protocol number can not exceed 128
    // crates/public/src/l7_protocol.rs, pub const L7_PROTOCOL_xxx is the implemented protocol.
    fn protocol(&self) -> L7Protocol;

    // return inner proto of Custom, only when L7Protocol is Custom will not None
    fn custom_protocol(&self) -> Option<CustomProtocol> {
        None
    }

    // this func must call after log success check_payload, otherwise maybe panic
    fn l7_protocol_enum(&self) -> L7ProtocolEnum {
        let proto = self.protocol();
        match proto {
            L7Protocol::Custom => L7ProtocolEnum::Custom(self.custom_protocol().unwrap()),
            _ => L7ProtocolEnum::L7Protocol(proto),
        }
    }
    // l4是tcp时是否解析，用于快速过滤协议
    // ==============================
    // whether l4 is parsed when tcp, use for quickly protocol filter
    fn parsable_on_tcp(&self) -> bool {
        true
    }
    // l4是udp是是否解析，用于快速过滤协议
    // ==============================
    // whether l4 is parsed when udp, use for quickly protocol filter
    fn parsable_on_udp(&self) -> bool {
        true
    }

    // l4即不是udp也不是tcp，用于快速过滤协议
    // ==============================
    // L4 is neither UDP nor TCP and is used to quickly filter protocols
    fn parsable_on_other(&self) -> bool {
        false
    }

    // is parse default? use for config init.
    fn parse_default(&self) -> bool {
        true
    }

    fn reset(&mut self) {}

    // return perf data
    fn perf_stats(&mut self) -> Option<L7PerfStats>;

    fn set_obfuscate_cache(&mut self, _: Option<ObfuscateCache>) {}
}

#[derive(Clone)]
pub struct EbpfParam<'a> {
    pub is_tls: bool,
    // 目前仅 http2 uprobe 有意义
    // ==========================
    // now only http2 uprobe uses
    pub is_req_end: bool,
    pub is_resp_end: bool,
    pub process_kname: &'a str,
}

pub struct LogCache {
    pub msg_type: LogMessageType,
    pub time: u64,
    // req_end, resp_end, merged
    // set merged to true when req and resp merge once
    pub multi_merge_info: Option<(bool, bool, bool)>,
}

pub struct L7PerfCache {
    // lru cache previous rrt
    pub rrt_cache: LruCache<u128, LogCache>,
    // LruCache<flow_id, (in_cache_req, count)>
    pub timeout_cache: LruCache<u64, (usize, usize)>,
    // time in microseconds
    pub last_log_time: u64,
}

impl L7PerfCache {
    // 60 seconds
    const LOG_INTERVAL: u64 = 60_000_000;

    pub fn new(cap: usize) -> Self {
        L7PerfCache {
            rrt_cache: LruCache::new(cap.try_into().unwrap()),
            timeout_cache: LruCache::new(cap.try_into().unwrap()),
            last_log_time: 0,
        }
    }

    pub fn put(&mut self, key: u128, value: LogCache) -> Option<LogCache> {
        let now = value.time;
        if self.rrt_cache.len() >= usize::from(self.rrt_cache.cap())
            && self.last_log_time + Self::LOG_INTERVAL < now
        {
            self.last_log_time = now;
            debug!("The capacity({}) of the rrt table will be exceeded. please adjust the configuration", self.rrt_cache.cap());
        }
        self.rrt_cache.put(key, value)
    }

    pub fn pop_timeout_count(&mut self, flow_id: &u64, flow_end: bool) -> usize {
        let (in_cache, t) = self.timeout_cache.pop(flow_id).unwrap_or((0, 0));
        if flow_end {
            in_cache + t
        } else {
            self.timeout_cache.put(*flow_id, (in_cache, 0));
            t
        }
    }
}

pub struct ParseParam<'a> {
    // l3/l4 info
    pub l4_protocol: IpProtocol,
    pub ip_src: IpAddr,
    pub ip_dst: IpAddr,
    pub port_src: u16,
    pub port_dst: u16,
    pub flow_id: u64,
    pub icmp_data: Option<&'a IcmpData>,

    // parse info
    pub direction: PacketDirection,
    pub ebpf_type: EbpfType,
    // ebpf_type 不为 EBPF_TYPE_NONE 会有值
    // ===================================
    // not None when payload from ebpf
    pub ebpf_param: Option<EbpfParam<'a>>,
    // calculate from cap_seq, req and correspond resp may have same packet seq, non ebpf always 0
    pub packet_start_seq: u64,
    pub packet_end_seq: u64,
    pub time: u64, // micro second
    pub parse_perf: bool,
    pub parse_log: bool,

    pub parse_config: Option<&'a LogParserConfig>,

    pub l7_perf_cache: Rc<RefCell<L7PerfCache>>,

    // plugins
    pub wasm_vm: Rc<RefCell<Option<WasmVm>>>,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub so_func: Rc<RefCell<Option<Vec<SoPluginFunc>>>>,

    pub stats_counter: Option<Arc<FlowMapCounter>>,

    // rrt cal timeout
    pub rrt_timeout: usize, // micro second

    // the config of `l7_log_packet_size`, must set in parse_payload and check_payload
    pub buf_size: u16,
    pub captured_byte: u16,

    pub oracle_parse_conf: OracleConfig,
}

impl<'a> ParseParam<'a> {
    pub fn is_from_ebpf(&self) -> bool {
        self.ebpf_type != EbpfType::None
    }

    pub fn new(
        packet: &'a MetaPacket<'a>,
        cache: Rc<RefCell<L7PerfCache>>,
        wasm_vm: Rc<RefCell<Option<WasmVm>>>,
        #[cfg(any(target_os = "linux", target_os = "android"))] so_func: Rc<
            RefCell<Option<Vec<SoPluginFunc>>>,
        >,
        parse_perf: bool,
        parse_log: bool,
    ) -> Self {
        let mut param = Self {
            l4_protocol: packet.lookup_key.proto,
            ip_src: packet.lookup_key.src_ip,
            ip_dst: packet.lookup_key.dst_ip,
            port_src: packet.lookup_key.src_port,
            port_dst: packet.lookup_key.dst_port,
            icmp_data: if let ProtocolData::IcmpData(icmp_data) = &packet.protocol_data {
                Some(icmp_data)
            } else {
                None
            },
            flow_id: packet.flow_id,

            direction: packet.lookup_key.direction,
            ebpf_type: packet.ebpf_type,
            packet_start_seq: packet.cap_start_seq,
            packet_end_seq: packet.cap_end_seq,
            ebpf_param: None,
            time: packet.lookup_key.timestamp.as_micros() as u64,
            parse_perf,
            parse_log,
            parse_config: None,

            l7_perf_cache: cache,

            wasm_vm,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            so_func,

            stats_counter: None,

            // the timeout will overwrite by set_rrt_timeout(), 10s set in here only use for test.
            rrt_timeout: Duration::from_secs(10).as_micros() as usize,

            buf_size: 0,
            captured_byte: 0,

            oracle_parse_conf: OracleConfig::default(),
        };
        if packet.ebpf_type != EbpfType::None {
            param.ebpf_param = Some(EbpfParam {
                is_tls: packet.is_tls(),
                is_req_end: packet.is_request_end,
                is_resp_end: packet.is_response_end,
                #[cfg(any(target_os = "linux", target_os = "android"))]
                process_kname: std::str::from_utf8(&packet.process_kname[..]).unwrap_or(""),
                #[cfg(target_os = "windows")]
                process_kname: "",
            });
        }

        param
    }
}

impl<'a> ParseParam<'a> {
    pub fn is_tls(&self) -> bool {
        if let Some(ebpf_param) = self.ebpf_param.as_ref() {
            return ebpf_param.is_tls;
        }
        false
    }

    pub fn set_counter(&mut self, stat: Arc<FlowMapCounter>) {
        self.stats_counter = Some(stat);
    }

    pub fn set_buf_size(&mut self, buf_size: usize) {
        self.buf_size = buf_size as u16;
    }

    pub fn set_captured_byte(&mut self, captured_byte: usize) {
        self.captured_byte = captured_byte as u16;
    }

    pub fn set_rrt_timeout(&mut self, t: usize) {
        self.rrt_timeout = t;
    }

    pub fn set_log_parse_config(&mut self, conf: &'a LogParserConfig) {
        self.parse_config = Some(conf);
    }

    pub fn set_oracle_conf(&mut self, conf: OracleConfig) {
        self.oracle_parse_conf = conf;
    }
}

/*
    param:
        protocol: the protocol which should check

        l7_enabled: the protocol from static config indicate which protocol should check or skip

    it will merge the bitmap from config and l4 protocol filter.

    return the protocol bitmap indicate which protocol should check and parse.
*/
pub fn get_parse_bitmap(protocol: IpProtocol, l7_enabled: L7ProtocolBitmap) -> L7ProtocolBitmap {
    let mut bitmap = L7ProtocolBitmap(0);
    for i in get_all_protocol().iter() {
        if l7_enabled.is_enabled(i.protocol()) {
            match protocol {
                IpProtocol::TCP if i.parsable_on_tcp() => {
                    bitmap.set_enabled(i.protocol());
                }
                IpProtocol::UDP if i.parsable_on_udp() => {
                    bitmap.set_enabled(i.protocol());
                }
                _ => {}
            }
        }
    }

    bitmap
}

/*
    protocol is u128 bitmap indicate which protocol should check or skip.
    when bit set 0 should skip the protocol check.
    so the protocol number can not exceed 127.
*/
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct L7ProtocolBitmap(u128);

impl L7ProtocolBitmap {
    pub fn set_enabled(&mut self, p: L7Protocol) {
        self.0 |= 1 << (p as u128);
    }

    pub fn set_disabled(&mut self, p: L7Protocol) {
        self.0 &= !(1 << (p as u128));
    }
}

impl L7ProtocolChecker for L7ProtocolBitmap {
    fn is_disabled(&self, p: L7Protocol) -> bool {
        self.0 & (1 << (p as u128)) == 0
    }

    fn is_enabled(&self, p: L7Protocol) -> bool {
        !self.is_disabled(p)
    }
}

impl From<&[String]> for L7ProtocolBitmap {
    fn from(vs: &[String]) -> Self {
        let mut bitmap = L7ProtocolBitmap(0);
        for v in vs.iter() {
            if let Ok(p) = L7ProtocolParser::try_from(v.as_str()) {
                bitmap.set_enabled(p.protocol());
            }
        }
        bitmap
    }
}

impl Debug for L7ProtocolBitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut p = vec![];
        for i in get_all_protocol() {
            if self.is_enabled(i.protocol()) {
                p.push(i.protocol());
            }
        }
        f.write_str(format!("{:#?}", p).as_str())
    }
}
