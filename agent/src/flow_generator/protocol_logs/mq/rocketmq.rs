/*
 * Copyright (c) 2025 Yunshan Networks
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

use serde::{Deserialize, Serialize};

use crate::{
    common::{
        enums::IpProtocol,
        flow::{L7PerfStats, L7Protocol, PacketDirection},
        l7_protocol_info::{L7ProtocolInfo, L7ProtocolInfoInterface},
        l7_protocol_log::{L7ParseResult, L7ProtocolParserInterface, ParseParam},
        meta_packet::EbpfFlags,
    },
    config::handler::LogParserConfig,
    flow_generator::{
        error::{Error, Result},
        protocol_logs::{
            pb_adapter::{ExtendedInfo, L7ProtocolSendLog, L7Request, L7Response},
            set_captured_byte, swap_if, value_is_default, value_is_negative, AppProtoHead,
            L7ResponseStatus, LogMessageType,
        },
    },
    utils::bytes,
};

#[derive(Serialize, Debug, Default, Clone)]
pub struct RocketmqInfo {
    msg_type: LogMessageType,
    #[serde(skip)]
    is_tls: bool,

    #[serde(skip_serializing_if = "value_is_default")]
    pub x_request_id: u32,
    #[serde(skip_serializing_if = "value_is_default")]
    pub trace_id: String,
    #[serde(skip_serializing_if = "value_is_default")]
    pub span_id: String,

    // request
    #[serde(rename = "request_length", skip_serializing_if = "value_is_negative")]
    pub req_msg_size: Option<u32>,
    #[serde(skip_serializing_if = "value_is_default")]
    pub version: String,
    #[serde(rename = "request_type")]
    pub req_code: i32,
    #[serde(skip)]
    pub req_code_name: String,
    #[serde(rename = "request_domain", skip_serializing_if = "value_is_default")]
    pub ext_group: String,
    #[serde(rename = "request_resource", skip_serializing_if = "value_is_default")]
    pub ext_topic: String,
    #[serde(rename = "request_id", skip_serializing_if = "value_is_default")]
    pub opaque: u32,
    #[serde(skip)]
    pub ext_queue_id: String,
    #[serde(skip)]
    pub req_body: String,

    // response
    #[serde(rename = "response_length", skip_serializing_if = "value_is_negative")]
    pub resp_msg_size: Option<u32>,
    #[serde(rename = "response_status")]
    pub status: L7ResponseStatus,
    #[serde(rename = "response_code", skip_serializing_if = "value_is_default")]
    pub resp_code: i32,
    #[serde(skip)]
    pub resp_code_name: String,
    #[serde(
        rename = "response_exception",
        skip_serializing_if = "value_is_default"
    )]
    pub remark: String,
    #[serde(rename = "response_result", skip_serializing_if = "value_is_default")]
    pub resp_body: String,

    captured_request_byte: u32,
    captured_response_byte: u32,

    rrt: u64,

    #[serde(skip)]
    is_on_blacklist: bool,
    #[serde(skip)]
    endpoint: Option<String>,
}

impl L7ProtocolInfoInterface for RocketmqInfo {
    fn session_id(&self) -> Option<u32> {
        Some(self.x_request_id)
    }

    fn merge_log(&mut self, other: &mut L7ProtocolInfo) -> Result<()> {
        if let L7ProtocolInfo::RocketmqInfo(rocketmq) = other {
            self.merge(rocketmq);
        }
        Ok(())
    }

    fn app_proto_head(&self) -> Option<AppProtoHead> {
        Some(AppProtoHead {
            proto: L7Protocol::RocketMQ,
            msg_type: self.msg_type,
            rrt: self.rrt,
        })
    }

    fn is_tls(&self) -> bool {
        self.is_tls
    }

    fn get_endpoint(&self) -> Option<String> {
        if self.ext_topic.is_empty() || self.ext_queue_id.is_empty() {
            None
        } else {
            Some(format!("{}-{}", self.ext_topic, self.ext_queue_id))
        }
    }

    fn get_request_resource_length(&self) -> usize {
        self.ext_topic.len()
    }

    fn is_on_blacklist(&self) -> bool {
        self.is_on_blacklist
    }
}

impl RocketmqInfo {
    pub fn merge(&mut self, other: &mut Self) {
        swap_if!(self, resp_msg_size, is_none, other);
        if self.status == L7ResponseStatus::default() {
            self.status = other.status;
        }
        if self.resp_code_name.is_empty() {
            std::mem::swap(&mut self.resp_code_name, &mut other.resp_code_name);
            self.resp_code = other.resp_code;
        }
        swap_if!(self, remark, is_empty, other);
        swap_if!(self, resp_body, is_empty, other);
        self.captured_response_byte = other.captured_response_byte;
        swap_if!(self, endpoint, is_none, other);
        if other.is_on_blacklist {
            self.is_on_blacklist = other.is_on_blacklist;
        }
    }

    fn set_is_on_blacklist(&mut self, config: &LogParserConfig) {
        if let Some(t) = config.l7_log_blacklist_trie.get(&L7Protocol::RocketMQ) {
            self.is_on_blacklist = t.request_type.is_on_blacklist(&self.req_code_name)
                || t.request_domain.is_on_blacklist(&self.ext_group)
                || t.request_resource.is_on_blacklist(&self.ext_topic)
                || self
                    .endpoint
                    .as_ref()
                    .map(|p| t.endpoint.is_on_blacklist(p))
                    .unwrap_or_default();
        }
    }
}

impl From<RocketmqInfo> for L7ProtocolSendLog {
    fn from(f: RocketmqInfo) -> Self {
        let flags = if f.is_tls {
            EbpfFlags::TLS.bits()
        } else {
            EbpfFlags::NONE.bits()
        };
        L7ProtocolSendLog {
            captured_request_byte: f.captured_request_byte,
            captured_response_byte: f.captured_response_byte,
            version: Some(f.version),
            req_len: f.req_msg_size,
            resp_len: f.resp_msg_size,
            req: L7Request {
                req_type: f.req_code_name,
                domain: f.ext_group,
                resource: f.ext_topic,
                endpoint: f.endpoint.unwrap_or_default(),
                ..Default::default()
            },
            resp: L7Response {
                status: f.status,
                code: Some(f.resp_code),
                exception: f.remark,
                result: f.resp_body,
                ..Default::default()
            },
            ext_info: Some(ExtendedInfo {
                request_id: Some(f.opaque),
                x_request_id_0: Some(f.x_request_id.to_string()),
                ..Default::default()
            }),
            flags,
            ..Default::default()
        }
    }
}

#[derive(Default)]
pub struct RocketmqLog {
    perf_stats: Option<L7PerfStats>,
    last_is_on_blacklist: bool,
}

impl L7ProtocolParserInterface for RocketmqLog {
    fn check_payload(&mut self, payload: &[u8], param: &ParseParam) -> bool {
        if !param.ebpf_type.is_raw_protocol() {
            return false;
        }
        self.check(payload, param.l4_protocol)
    }

    fn parse_payload(&mut self, payload: &[u8], param: &ParseParam) -> Result<L7ParseResult> {
        if self.perf_stats.is_none() && param.parse_perf {
            self.perf_stats = Some(L7PerfStats::default())
        }
        let mut info = RocketmqInfo::default();
        self.parse(payload, param.l4_protocol, param.direction, &mut info)?;
        info.is_tls = param.is_tls();
        set_captured_byte!(info, param);
        if let Some(config) = param.parse_config {
            info.set_is_on_blacklist(config);
        }
        if !info.is_on_blacklist && !self.last_is_on_blacklist {
            match info.msg_type {
                LogMessageType::Request => {
                    self.perf_stats.as_mut().map(|p| p.inc_req());
                }
                LogMessageType::Response => {
                    self.perf_stats.as_mut().map(|p| p.inc_resp());
                    if info.resp_code != 0 {
                        match info.status {
                            L7ResponseStatus::ClientError => {
                                self.perf_stats.as_mut().map(|p| p.inc_req_err());
                            }
                            _ => {
                                self.perf_stats.as_mut().map(|p| p.inc_resp_err());
                            }
                        }
                    }
                }
                _ => {}
            }
            info.cal_rrt(param).map(|rrt| {
                info.rrt = rrt;
                self.perf_stats.as_mut().map(|p| p.update_rrt(rrt));
            });
        }
        self.last_is_on_blacklist = info.is_on_blacklist;
        if param.parse_log {
            Ok(L7ParseResult::Single(L7ProtocolInfo::RocketmqInfo(info)))
        } else {
            Ok(L7ParseResult::None)
        }
    }

    fn protocol(&self) -> L7Protocol {
        L7Protocol::RocketMQ
    }

    fn parsable_on_udp(&self) -> bool {
        false
    }

    fn perf_stats(&mut self) -> Option<L7PerfStats> {
        self.perf_stats.take()
    }
}

impl RocketmqLog {
    fn check(&mut self, payload: &[u8], protocol: IpProtocol) -> bool {
        if protocol != IpProtocol::TCP {
            return false;
        }
        let mut header = RocketmqHeader::default();
        if header.decode(payload) < 0 {
            return false;
        }
        true
    }

    fn parse(
        &mut self,
        payload: &[u8],
        protocol: IpProtocol,
        _direction: PacketDirection,
        info: &mut RocketmqInfo,
    ) -> Result<()> {
        if protocol != IpProtocol::TCP {
            return Err(Error::InvalidIpProtocol);
        }
        let mut header = RocketmqHeader::default();
        let header_offset = header.decode(payload);
        if header_offset < 0 {
            return Err(Error::RocketmqLogParseFailed);
        }
        let mut body = RocketmqBody::default();
        let body_offset = body.decode(&payload[(header_offset as usize)..], &header);
        if body_offset < 0 {
            return Err(Error::RocketmqLogParseFailed);
        }
        let header_data_ext_fields = match &header.header_data.ext_fields {
            Some(ext_fields) => ext_fields.clone(),
            _ => RocketmqHeaderExtFields::default(),
        };
        info.version = String::from(header.get_version_str());
        info.opaque = header.header_data.opaque as u32;
        info.ext_topic = match &header_data_ext_fields.topic {
            Some(topic) => topic.clone(),
            _ => String::new(),
        };
        info.ext_queue_id = match &header_data_ext_fields.queue_id {
            Some(queue_id) => queue_id.clone(),
            _ => String::new(),
        };
        if header.is_request() {
            info.msg_type = LogMessageType::Request;
            info.req_msg_size = Some(header.length as u32);
            info.req_code = header.header_data.code;
            info.req_code_name = String::from(header.get_request_code_str());
            match &info.req_code_name[..] {
                "SEND_BATCH_MESSAGE" | "SEND_MESSAGE" | "SEND_MESSAGE_V2" => {
                    info.ext_group = match &header_data_ext_fields.producer_group {
                        Some(producer_group) => producer_group.clone(),
                        _ => String::new(),
                    };
                }
                // TODO: there are some different but necessary keys corresponding to request code
                _ => {
                    info.ext_group = match &header_data_ext_fields.consumer_group {
                        Some(consumer_group) => consumer_group.clone(),
                        _ => String::new(),
                    };
                }
            }
            info.req_body = body.body_data.body;
            //  handle oneway requests expecting no response in particular
            if header.is_oneway_request() {
                info.msg_type = LogMessageType::Session;
                info.status = L7ResponseStatus::Ok;
                info.resp_code = 0;
                info.resp_code_name = String::from("ONEWAY_REQUEST");
            }
        } else {
            info.msg_type = LogMessageType::Response;
            info.resp_msg_size = Some(header.length as u32);
            info.resp_code = header.header_data.code;
            let (resp_code_name, status) = header.get_response_code_str_and_status();
            info.resp_code_name = String::from(resp_code_name);
            info.status = status;
            info.remark = match &header.header_data.remark {
                Some(remark) => remark.clone(),
                _ => String::new(),
            };
            info.resp_body = body.body_data.body;
        }
        info.x_request_id = info.opaque;
        info.endpoint = info.get_endpoint();
        Ok(())
    }
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocketmqSerializeType {
    Json,
    RocketMQ,
}

impl Default for RocketmqSerializeType {
    fn default() -> Self {
        RocketmqSerializeType::Json
    }
}

impl RocketmqSerializeType {
    fn from_int(i: i32) -> Option<RocketmqSerializeType> {
        match i {
            0 => Some(RocketmqSerializeType::Json),
            1 => Some(RocketmqSerializeType::RocketMQ),
            _ => None,
        }
    }
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct RocketmqHeader {
    length: i32,
    header_length: i32,
    serialize_type: Option<RocketmqSerializeType>,
    header_data: RocketmqHeaderData,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct RocketmqHeaderData {
    code: i32,
    #[serde(rename = "extFields", skip_serializing_if = "Option::is_none")]
    ext_fields: Option<RocketmqHeaderExtFields>,
    flag: i32,
    language: String,
    opaque: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    remark: Option<String>,
    #[serde(rename = "serializeTypeCurrentRPC")]
    serialize_type_current_rpc: String,
    version: i32,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct RocketmqHeaderExtFields {
    #[serde(rename = "consumerGroup", skip_serializing_if = "Option::is_none")]
    consumer_group: Option<String>,
    #[serde(
        rename = "producerGroup",
        alias = "a",
        skip_serializing_if = "Option::is_none"
    )]
    producer_group: Option<String>,
    #[serde(rename = "topic", alias = "b", skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(
        rename = "queueId",
        alias = "e",
        skip_serializing_if = "Option::is_none"
    )]
    queue_id: Option<String>,
    // TODO: add necessary keys according to request code
}

impl RocketmqHeader {
    fn decode(&mut self, payload: &[u8]) -> isize {
        // length(4B) + origin_header_length(4B)
        if payload.len() < 8 {
            return -1;
        }
        self.length = bytes::read_i32_be(payload);
        // origin_header_length(4B) and maximum packet length(65535B)
        if self.length < 4 || self.length > 65535 {
            return -1;
        // loose the length check due to possible packet segmentation
        } else if self.length > payload.len() as i32 - 4 {
            self.length = payload.len() as i32 - 4;
        }
        let origin_header_length = bytes::read_i32_be(&payload[4..8]);
        self.header_length = origin_header_length & 0xFFFFFF;
        if self.header_length < 0 || self.header_length > self.length - 4 {
            return -1;
        }
        self.serialize_type = RocketmqSerializeType::from_int((origin_header_length >> 24) & 0xFF);
        let header_data = &payload[8..(self.header_length as usize + 8)];
        match self.serialize_type {
            Some(RocketmqSerializeType::Json) => {
                // there must be the following characters
                // {"code":0,"flag":1,"language":"","opaque":1,"serializeTypeCurrentRPC":"JSON","version":0}
                // in header data at least, total: 89B
                if self.header_length >= 89 && self.decode_for_json_type(header_data) > 0 {
                    return self.header_length as isize + 8;
                }
            }
            Some(RocketmqSerializeType::RocketMQ) => {
                // there must be code(2B), language(1B), version(2B), opaque(4B) and flag(4B)
                // in header data at least, total: 2 + 1 + 2 + 4 + 4 = 13B
                if self.header_length >= 13 && self.decode_for_rocketmq_type(header_data) > 0 {
                    return self.header_length as isize + 8;
                }
            }
            _ => (),
        }
        -1
    }

    fn decode_for_json_type(&mut self, data: &[u8]) -> isize {
        if let Ok(header_data) = serde_json::from_slice::<RocketmqHeaderData>(data) {
            self.header_data = header_data.clone();
            return data.len() as isize;
        }
        -1
    }

    fn decode_for_rocketmq_type(&mut self, data: &[u8]) -> isize {
        self.header_data.serialize_type_current_rpc = String::from("ROCKETMQ");
        self.header_data.code = bytes::read_i16_be(data) as i32;
        self.header_data.language = String::from(Self::get_language(data[2]));
        self.header_data.version = bytes::read_i16_be(&data[3..5]) as i32;
        self.header_data.opaque = bytes::read_i32_be(&data[5..9]);
        self.header_data.flag = bytes::read_i32_be(&data[9..13]);
        let remark_length = self.decode_remark(&data[13..]);
        if remark_length < 0 {
            return -1;
        }
        let ext_fields_length = self.decode_ext_fields(&data[(remark_length as usize + 13)..]);
        if ext_fields_length < 0 {
            return -1;
        }
        data.len() as isize
    }

    fn get_language(language_code: u8) -> &'static str {
        let language = [
            "JAVA", "CPP", "DOTNET", "PYTHON", "DELPHI", "ERLANG", "RUBY", "OTHER", "HTTP", "GO",
            "PHP", "OMS", "RUST",
        ];
        match language_code {
            0..=12 => language[language_code as usize],
            _ => "",
        }
    }

    fn decode_remark(&mut self, data: &[u8]) -> isize {
        let length = bytes::read_i32_be(data);
        if length < 0 || length > data.len() as i32 - 4 {
            return -1;
        }
        if length == 0 {
            return 4;
        }
        let remark = String::from_utf8_lossy(&data[4..(length as usize + 4)]).into_owned();
        self.header_data.remark = Some(remark);
        length as isize + 4
    }

    fn decode_ext_fields(&mut self, data: &[u8]) -> isize {
        let length = bytes::read_i32_be(data);
        if length < 0 || length > data.len() as i32 - 4 {
            return -1;
        } else if length == 0 {
            return 0;
        }
        let mut ext_fields = RocketmqHeaderExtFields::default();
        let mut offset: usize = 4;
        let end_offset = length as usize + 4;
        while offset < end_offset {
            let key_length = bytes::read_u16_be(&data[offset..(offset + 2)]);
            offset += 2;
            if key_length > (end_offset - offset) as u16 {
                return -1;
            }
            let key = &data[offset..(offset + key_length as usize)];
            offset += key_length as usize;
            let value_length = bytes::read_i32_be(&data[offset..(offset + 4)]);
            offset += 4;
            if value_length < 0 || value_length > (end_offset - offset) as i32 {
                return -1;
            }
            let value = &data[offset..(offset + value_length as usize)];
            offset += value_length as usize;
            let mut flags = 0b0000;
            match key {
                b"consumerGroup" => {
                    ext_fields.consumer_group = Some(String::from_utf8_lossy(value).into_owned());
                    flags |= 0b0001;
                }
                b"producerGroup" | b"a" => {
                    ext_fields.producer_group = Some(String::from_utf8_lossy(value).into_owned());
                    flags |= 0b0001;
                }
                b"topic" | b"b" => {
                    ext_fields.topic = Some(String::from_utf8_lossy(value).into_owned());
                    flags |= 0b0010;
                }
                b"queueId" | b"e" => {
                    ext_fields.queue_id = Some(String::from_utf8_lossy(value).into_owned());
                    flags |= 0b0100;
                }
                _ => (),
            }
            if flags == 0b0111 {
                break;
            }
        }
        self.header_data.ext_fields = Some(ext_fields);
        length as isize + 4
    }

    fn is_request(&self) -> bool {
        self.header_data.flag & 0x1 == 0
    }

    fn is_oneway_request(&self) -> bool {
        (self.header_data.flag >> 1) & 0x1 == 1
    }

    fn is_remark_found(&self) -> bool {
        let remark = match &self.header_data.remark {
            Some(value) => value,
            _ => "",
        };
        remark == "FOUND"
    }

    pub fn get_version_str(&self) -> &'static str {
        let version_str = [
            "V3_0_0_SNAPSHOT",
            "V3_0_0_ALPHA1",
            "V3_0_0_BETA1",
            "V3_0_0_BETA2",
            "V3_0_0_BETA3",
            "V3_0_0_BETA4",
            "V3_0_0_BETA5",
            "V3_0_0_BETA6_SNAPSHOT",
            "V3_0_0_BETA6",
            "V3_0_0_BETA7_SNAPSHOT",
            "V3_0_0_BETA7",
            "V3_0_0_BETA8_SNAPSHOT",
            "V3_0_0_BETA8",
            "V3_0_0_BETA9_SNAPSHOT",
            "V3_0_0_BETA9",
            "V3_0_0_FINAL",
            "V3_0_1_SNAPSHOT",
            "V3_0_1",
            "V3_0_2_SNAPSHOT",
            "V3_0_2",
            "V3_0_3_SNAPSHOT",
            "V3_0_3",
            "V3_0_4_SNAPSHOT",
            "V3_0_4",
            "V3_0_5_SNAPSHOT",
            "V3_0_5",
            "V3_0_6_SNAPSHOT",
            "V3_0_6",
            "V3_0_7_SNAPSHOT",
            "V3_0_7",
            "V3_0_8_SNAPSHOT",
            "V3_0_8",
            "V3_0_9_SNAPSHOT",
            "V3_0_9",
            "V3_0_10_SNAPSHOT",
            "V3_0_10",
            "V3_0_11_SNAPSHOT",
            "V3_0_11",
            "V3_0_12_SNAPSHOT",
            "V3_0_12",
            "V3_0_13_SNAPSHOT",
            "V3_0_13",
            "V3_0_14_SNAPSHOT",
            "V3_0_14",
            "V3_0_15_SNAPSHOT",
            "V3_0_15",
            "V3_1_0_SNAPSHOT",
            "V3_1_0",
            "V3_1_1_SNAPSHOT",
            "V3_1_1",
            "V3_1_2_SNAPSHOT",
            "V3_1_2",
            "V3_1_3_SNAPSHOT",
            "V3_1_3",
            "V3_1_4_SNAPSHOT",
            "V3_1_4",
            "V3_1_5_SNAPSHOT",
            "V3_1_5",
            "V3_1_6_SNAPSHOT",
            "V3_1_6",
            "V3_1_7_SNAPSHOT",
            "V3_1_7",
            "V3_1_8_SNAPSHOT",
            "V3_1_8",
            "V3_1_9_SNAPSHOT",
            "V3_1_9",
            "V3_2_0_SNAPSHOT",
            "V3_2_0",
            "V3_2_1_SNAPSHOT",
            "V3_2_1",
            "V3_2_2_SNAPSHOT",
            "V3_2_2",
            "V3_2_3_SNAPSHOT",
            "V3_2_3",
            "V3_2_4_SNAPSHOT",
            "V3_2_4",
            "V3_2_5_SNAPSHOT",
            "V3_2_5",
            "V3_2_6_SNAPSHOT",
            "V3_2_6",
            "V3_2_7_SNAPSHOT",
            "V3_2_7",
            "V3_2_8_SNAPSHOT",
            "V3_2_8",
            "V3_2_9_SNAPSHOT",
            "V3_2_9",
            "V3_3_1_SNAPSHOT",
            "V3_3_1",
            "V3_3_2_SNAPSHOT",
            "V3_3_2",
            "V3_3_3_SNAPSHOT",
            "V3_3_3",
            "V3_3_4_SNAPSHOT",
            "V3_3_4",
            "V3_3_5_SNAPSHOT",
            "V3_3_5",
            "V3_3_6_SNAPSHOT",
            "V3_3_6",
            "V3_3_7_SNAPSHOT",
            "V3_3_7",
            // 100
            "V3_3_8_SNAPSHOT",
            "V3_3_8",
            "V3_3_9_SNAPSHOT",
            "V3_3_9",
            "V3_4_1_SNAPSHOT",
            "V3_4_1",
            "V3_4_2_SNAPSHOT",
            "V3_4_2",
            "V3_4_3_SNAPSHOT",
            "V3_4_3",
            "V3_4_4_SNAPSHOT",
            "V3_4_4",
            "V3_4_5_SNAPSHOT",
            "V3_4_5",
            "V3_4_6_SNAPSHOT",
            "V3_4_6",
            "V3_4_7_SNAPSHOT",
            "V3_4_7",
            "V3_4_8_SNAPSHOT",
            "V3_4_8",
            "V3_4_9_SNAPSHOT",
            "V3_4_9",
            "V3_5_1_SNAPSHOT",
            "V3_5_1",
            "V3_5_2_SNAPSHOT",
            "V3_5_2",
            "V3_5_3_SNAPSHOT",
            "V3_5_3",
            "V3_5_4_SNAPSHOT",
            "V3_5_4",
            "V3_5_5_SNAPSHOT",
            "V3_5_5",
            "V3_5_6_SNAPSHOT",
            "V3_5_6",
            "V3_5_7_SNAPSHOT",
            "V3_5_7",
            "V3_5_8_SNAPSHOT",
            "V3_5_8",
            "V3_5_9_SNAPSHOT",
            "V3_5_9",
            "V3_6_1_SNAPSHOT",
            "V3_6_1",
            "V3_6_2_SNAPSHOT",
            "V3_6_2",
            "V3_6_3_SNAPSHOT",
            "V3_6_3",
            "V3_6_4_SNAPSHOT",
            "V3_6_4",
            "V3_6_5_SNAPSHOT",
            "V3_6_5",
            "V3_6_6_SNAPSHOT",
            "V3_6_6",
            "V3_6_7_SNAPSHOT",
            "V3_6_7",
            "V3_6_8_SNAPSHOT",
            "V3_6_8",
            "V3_6_9_SNAPSHOT",
            "V3_6_9",
            "V3_7_1_SNAPSHOT",
            "V3_7_1",
            "V3_7_2_SNAPSHOT",
            "V3_7_2",
            "V3_7_3_SNAPSHOT",
            "V3_7_3",
            "V3_7_4_SNAPSHOT",
            "V3_7_4",
            "V3_7_5_SNAPSHOT",
            "V3_7_5",
            "V3_7_6_SNAPSHOT",
            "V3_7_6",
            "V3_7_7_SNAPSHOT",
            "V3_7_7",
            "V3_7_8_SNAPSHOT",
            "V3_7_8",
            "V3_7_9_SNAPSHOT",
            "V3_7_9",
            "V3_8_1_SNAPSHOT",
            "V3_8_1",
            "V3_8_2_SNAPSHOT",
            "V3_8_2",
            "V3_8_3_SNAPSHOT",
            "V3_8_3",
            "V3_8_4_SNAPSHOT",
            "V3_8_4",
            "V3_8_5_SNAPSHOT",
            "V3_8_5",
            "V3_8_6_SNAPSHOT",
            "V3_8_6",
            "V3_8_7_SNAPSHOT",
            "V3_8_7",
            "V3_8_8_SNAPSHOT",
            "V3_8_8",
            "V3_8_9_SNAPSHOT",
            "V3_8_9",
            "V3_9_1_SNAPSHOT",
            "V3_9_1",
            "V3_9_2_SNAPSHOT",
            "V3_9_2",
            "V3_9_3_SNAPSHOT",
            "V3_9_3",
            // 200
            "V3_9_4_SNAPSHOT",
            "V3_9_4",
            "V3_9_5_SNAPSHOT",
            "V3_9_5",
            "V3_9_6_SNAPSHOT",
            "V3_9_6",
            "V3_9_7_SNAPSHOT",
            "V3_9_7",
            "V3_9_8_SNAPSHOT",
            "V3_9_8",
            "V3_9_9_SNAPSHOT",
            "V3_9_9",
            "V4_0_0_SNAPSHOT",
            "V4_0_0",
            "V4_0_1_SNAPSHOT",
            "V4_0_1",
            "V4_0_2_SNAPSHOT",
            "V4_0_2",
            "V4_0_3_SNAPSHOT",
            "V4_0_3",
            "V4_0_4_SNAPSHOT",
            "V4_0_4",
            "V4_0_5_SNAPSHOT",
            "V4_0_5",
            "V4_0_6_SNAPSHOT",
            "V4_0_6",
            "V4_0_7_SNAPSHOT",
            "V4_0_7",
            "V4_0_8_SNAPSHOT",
            "V4_0_8",
            "V4_0_9_SNAPSHOT",
            "V4_0_9",
            "V4_1_0_SNAPSHOT",
            "V4_1_0",
            "V4_1_1_SNAPSHOT",
            "V4_1_1",
            "V4_1_2_SNAPSHOT",
            "V4_1_2",
            "V4_1_3_SNAPSHOT",
            "V4_1_3",
            "V4_1_4_SNAPSHOT",
            "V4_1_4",
            "V4_1_5_SNAPSHOT",
            "V4_1_5",
            "V4_1_6_SNAPSHOT",
            "V4_1_6",
            "V4_1_7_SNAPSHOT",
            "V4_1_7",
            "V4_1_8_SNAPSHOT",
            "V4_1_8",
            "V4_1_9_SNAPSHOT",
            "V4_1_9",
            "V4_2_0_SNAPSHOT",
            "V4_2_0",
            "V4_2_1_SNAPSHOT",
            "V4_2_1",
            "V4_2_2_SNAPSHOT",
            "V4_2_2",
            "V4_2_3_SNAPSHOT",
            "V4_2_3",
            "V4_2_4_SNAPSHOT",
            "V4_2_4",
            "V4_2_5_SNAPSHOT",
            "V4_2_5",
            "V4_2_6_SNAPSHOT",
            "V4_2_6",
            "V4_2_7_SNAPSHOT",
            "V4_2_7",
            "V4_2_8_SNAPSHOT",
            "V4_2_8",
            "V4_2_9_SNAPSHOT",
            "V4_2_9",
            "V4_3_0_SNAPSHOT",
            "V4_3_0",
            "V4_3_1_SNAPSHOT",
            "V4_3_1",
            "V4_3_2_SNAPSHOT",
            "V4_3_2",
            "V4_3_3_SNAPSHOT",
            "V4_3_3",
            "V4_3_4_SNAPSHOT",
            "V4_3_4",
            "V4_3_5_SNAPSHOT",
            "V4_3_5",
            "V4_3_6_SNAPSHOT",
            "V4_3_6",
            "V4_3_7_SNAPSHOT",
            "V4_3_7",
            "V4_3_8_SNAPSHOT",
            "V4_3_8",
            "V4_3_9_SNAPSHOT",
            "V4_3_9",
            "V4_4_0_SNAPSHOT",
            "V4_4_0",
            "V4_4_1_SNAPSHOT",
            "V4_4_1",
            "V4_4_2_SNAPSHOT",
            "V4_4_2",
            "V4_4_3_SNAPSHOT",
            "V4_4_3",
            // 300
            "V4_4_4_SNAPSHOT",
            "V4_4_4",
            "V4_4_5_SNAPSHOT",
            "V4_4_5",
            "V4_4_6_SNAPSHOT",
            "V4_4_6",
            "V4_4_7_SNAPSHOT",
            "V4_4_7",
            "V4_4_8_SNAPSHOT",
            "V4_4_8",
            "V4_4_9_SNAPSHOT",
            "V4_4_9",
            "V4_5_0_SNAPSHOT",
            "V4_5_0",
            "V4_5_1_SNAPSHOT",
            "V4_5_1",
            "V4_5_2_SNAPSHOT",
            "V4_5_2",
            "V4_5_3_SNAPSHOT",
            "V4_5_3",
            "V4_5_4_SNAPSHOT",
            "V4_5_4",
            "V4_5_5_SNAPSHOT",
            "V4_5_5",
            "V4_5_6_SNAPSHOT",
            "V4_5_6",
            "V4_5_7_SNAPSHOT",
            "V4_5_7",
            "V4_5_8_SNAPSHOT",
            "V4_5_8",
            "V4_5_9_SNAPSHOT",
            "V4_5_9",
            "V4_6_0_SNAPSHOT",
            "V4_6_0",
            "V4_6_1_SNAPSHOT",
            "V4_6_1",
            "V4_6_2_SNAPSHOT",
            "V4_6_2",
            "V4_6_3_SNAPSHOT",
            "V4_6_3",
            "V4_6_4_SNAPSHOT",
            "V4_6_4",
            "V4_6_5_SNAPSHOT",
            "V4_6_5",
            "V4_6_6_SNAPSHOT",
            "V4_6_6",
            "V4_6_7_SNAPSHOT",
            "V4_6_7",
            "V4_6_8_SNAPSHOT",
            "V4_6_8",
            "V4_6_9_SNAPSHOT",
            "V4_6_9",
            "V4_7_0_SNAPSHOT",
            "V4_7_0",
            "V4_7_1_SNAPSHOT",
            "V4_7_1",
            "V4_7_2_SNAPSHOT",
            "V4_7_2",
            "V4_7_3_SNAPSHOT",
            "V4_7_3",
            "V4_7_4_SNAPSHOT",
            "V4_7_4",
            "V4_7_5_SNAPSHOT",
            "V4_7_5",
            "V4_7_6_SNAPSHOT",
            "V4_7_6",
            "V4_7_7_SNAPSHOT",
            "V4_7_7",
            "V4_7_8_SNAPSHOT",
            "V4_7_8",
            "V4_7_9_SNAPSHOT",
            "V4_7_9",
            "V4_8_0_SNAPSHOT",
            "V4_8_0",
            "V4_8_1_SNAPSHOT",
            "V4_8_1",
            "V4_8_2_SNAPSHOT",
            "V4_8_2",
            "V4_8_3_SNAPSHOT",
            "V4_8_3",
            "V4_8_4_SNAPSHOT",
            "V4_8_4",
            "V4_8_5_SNAPSHOT",
            "V4_8_5",
            "V4_8_6_SNAPSHOT",
            "V4_8_6",
            "V4_8_7_SNAPSHOT",
            "V4_8_7",
            "V4_8_8_SNAPSHOT",
            "V4_8_8",
            "V4_8_9_SNAPSHOT",
            "V4_8_9",
            "V4_9_0_SNAPSHOT",
            "V4_9_0",
            "V4_9_1_SNAPSHOT",
            "V4_9_1",
            "V4_9_2_SNAPSHOT",
            "V4_9_2",
            "V4_9_3_SNAPSHOT",
            "V4_9_3",
            // 400
            "V4_9_4_SNAPSHOT",
            "V4_9_4",
            "V4_9_5_SNAPSHOT",
            "V4_9_5",
            "V4_9_6_SNAPSHOT",
            "V4_9_6",
            "V4_9_7_SNAPSHOT",
            "V4_9_7",
            "V4_9_8_SNAPSHOT",
            "V4_9_8",
            "V4_9_9_SNAPSHOT",
            "V4_9_9",
            "V5_0_0_SNAPSHOT",
            "V5_0_0",
            "V5_0_1_SNAPSHOT",
            "V5_0_1",
            "V5_0_2_SNAPSHOT",
            "V5_0_2",
            "V5_0_3_SNAPSHOT",
            "V5_0_3",
            "V5_0_4_SNAPSHOT",
            "V5_0_4",
            "V5_0_5_SNAPSHOT",
            "V5_0_5",
            "V5_0_6_SNAPSHOT",
            "V5_0_6",
            "V5_0_7_SNAPSHOT",
            "V5_0_7",
            "V5_0_8_SNAPSHOT",
            "V5_0_8",
            "V5_0_9_SNAPSHOT",
            "V5_0_9",
            "V5_1_0_SNAPSHOT",
            "V5_1_0",
            "V5_1_1_SNAPSHOT",
            "V5_1_1",
            "V5_1_2_SNAPSHOT",
            "V5_1_2",
            "V5_1_3_SNAPSHOT",
            "V5_1_3",
            "V5_1_4_SNAPSHOT",
            "V5_1_4",
            "V5_1_5_SNAPSHOT",
            "V5_1_5",
            "V5_1_6_SNAPSHOT",
            "V5_1_6",
            "V5_1_7_SNAPSHOT",
            "V5_1_7",
            "V5_1_8_SNAPSHOT",
            "V5_1_8",
            "V5_1_9_SNAPSHOT",
            "V5_1_9",
            "V5_2_0_SNAPSHOT",
            "V5_2_0",
            "V5_2_1_SNAPSHOT",
            "V5_2_1",
            "V5_2_2_SNAPSHOT",
            "V5_2_2",
            "V5_2_3_SNAPSHOT",
            "V5_2_3",
            "V5_2_4_SNAPSHOT",
            "V5_2_4",
            "V5_2_5_SNAPSHOT",
            "V5_2_5",
            "V5_2_6_SNAPSHOT",
            "V5_2_6",
            "V5_2_7_SNAPSHOT",
            "V5_2_7",
            "V5_2_8_SNAPSHOT",
            "V5_2_8",
            "V5_2_9_SNAPSHOT",
            "V5_2_9",
            "V5_3_0_SNAPSHOT",
            "V5_3_0",
            "V5_3_1_SNAPSHOT",
            "V5_3_1",
            "V5_3_2_SNAPSHOT",
            "V5_3_2",
            "V5_3_3_SNAPSHOT",
            "V5_3_3",
            "V5_3_4_SNAPSHOT",
            "V5_3_4",
            "V5_3_5_SNAPSHOT",
            "V5_3_5",
            "V5_3_6_SNAPSHOT",
            "V5_3_6",
            "V5_3_7_SNAPSHOT",
            "V5_3_7",
            "V5_3_8_SNAPSHOT",
            "V5_3_8",
            "V5_3_9_SNAPSHOT",
            "V5_3_9",
            "V5_4_0_SNAPSHOT",
            "V5_4_0",
            "V5_4_1_SNAPSHOT",
            "V5_4_1",
            "V5_4_2_SNAPSHOT",
            "V5_4_2",
            "V5_4_3_SNAPSHOT",
            "V5_4_3",
            // 500
            "V5_4_4_SNAPSHOT",
            "V5_4_4",
            "V5_4_5_SNAPSHOT",
            "V5_4_5",
            "V5_4_6_SNAPSHOT",
            "V5_4_6",
            "V5_4_7_SNAPSHOT",
            "V5_4_7",
            "V5_4_8_SNAPSHOT",
            "V5_4_8",
            "V5_4_9_SNAPSHOT",
            "V5_4_9",
            "V5_5_0_SNAPSHOT",
            "V5_5_0",
            "V5_5_1_SNAPSHOT",
            "V5_5_1",
            "V5_5_2_SNAPSHOT",
            "V5_5_2",
            "V5_5_3_SNAPSHOT",
            "V5_5_3",
            "V5_5_4_SNAPSHOT",
            "V5_5_4",
            "V5_5_5_SNAPSHOT",
            "V5_5_5",
            "V5_5_6_SNAPSHOT",
            "V5_5_6",
            "V5_5_7_SNAPSHOT",
            "V5_5_7",
            "V5_5_8_SNAPSHOT",
            "V5_5_8",
            "V5_5_9_SNAPSHOT",
            "V5_5_9",
            "V5_6_0_SNAPSHOT",
            "V5_6_0",
            "V5_6_1_SNAPSHOT",
            "V5_6_1",
            "V5_6_2_SNAPSHOT",
            "V5_6_2",
            "V5_6_3_SNAPSHOT",
            "V5_6_3",
            "V5_6_4_SNAPSHOT",
            "V5_6_4",
            "V5_6_5_SNAPSHOT",
            "V5_6_5",
            "V5_6_6_SNAPSHOT",
            "V5_6_6",
            "V5_6_7_SNAPSHOT",
            "V5_6_7",
            "V5_6_8_SNAPSHOT",
            "V5_6_8",
            "V5_6_9_SNAPSHOT",
            "V5_6_9",
            "V5_7_0_SNAPSHOT",
            "V5_7_0",
            "V5_7_1_SNAPSHOT",
            "V5_7_1",
            "V5_7_2_SNAPSHOT",
            "V5_7_2",
            "V5_7_3_SNAPSHOT",
            "V5_7_3",
            "V5_7_4_SNAPSHOT",
            "V5_7_4",
            "V5_7_5_SNAPSHOT",
            "V5_7_5",
            "V5_7_6_SNAPSHOT",
            "V5_7_6",
            "V5_7_7_SNAPSHOT",
            "V5_7_7",
            "V5_7_8_SNAPSHOT",
            "V5_7_8",
            "V5_7_9_SNAPSHOT",
            "V5_7_9",
            "V5_8_0_SNAPSHOT",
            "V5_8_0",
            "V5_8_1_SNAPSHOT",
            "V5_8_1",
            "V5_8_2_SNAPSHOT",
            "V5_8_2",
            "V5_8_3_SNAPSHOT",
            "V5_8_3",
            "V5_8_4_SNAPSHOT",
            "V5_8_4",
            "V5_8_5_SNAPSHOT",
            "V5_8_5",
            "V5_8_6_SNAPSHOT",
            "V5_8_6",
            "V5_8_7_SNAPSHOT",
            "V5_8_7",
            "V5_8_8_SNAPSHOT",
            "V5_8_8",
            "V5_8_9_SNAPSHOT",
            "V5_8_9",
            "V5_9_0_SNAPSHOT",
            "V5_9_0",
            "V5_9_1_SNAPSHOT",
            "V5_9_1",
            "V5_9_2_SNAPSHOT",
            "V5_9_2",
            "V5_9_3_SNAPSHOT",
            "V5_9_3",
            // 600
            "V5_9_4_SNAPSHOT",
            "V5_9_4",
            "V5_9_5_SNAPSHOT",
            "V5_9_5",
            "V5_9_6_SNAPSHOT",
            "V5_9_6",
            "V5_9_7_SNAPSHOT",
            "V5_9_7",
            "V5_9_8_SNAPSHOT",
            "V5_9_8",
            "V5_9_9_SNAPSHOT",
            "V5_9_9",
        ];
        match self.header_data.version {
            0..=611 => version_str[self.header_data.version as usize],
            _ => "",
        }
    }

    pub fn get_request_code_str(&self) -> &'static str {
        if !self.is_request() {
            return "";
        }
        // compatible for versions between v4.4.0 and v5.3.1
        match self.header_data.code {
            10 => "SEND_MESSAGE",
            11 => "PULL_MESSAGE",
            12 => "QUERY_MESSAGE",
            13 => "QUERY_BROKER_OFFSET",
            14 => "QUERY_CONSUMER_OFFSET",
            15 => "UPDATE_CONSUMER_OFFSET",
            17 => "UPDATE_AND_CREATE_TOPIC",
            18 => "UPDATE_AND_CREATE_TOPIC_LIST",
            21 => "GET_ALL_TOPIC_CONFIG",
            22 => "GET_TOPIC_CONFIG_LIST",
            23 => "GET_TOPIC_NAME_LIST",
            25 => "UPDATE_BROKER_CONFIG",
            26 => "GET_BROKER_CONFIG",
            27 => "TRIGGER_DELETE_FILES",
            28 => "GET_BROKER_RUNTIME_INFO",
            29 => "SEARCH_OFFSET_BY_TIMESTAMP",
            30 => "GET_MAX_OFFSET",
            31 => "GET_MIN_OFFSET",
            32 => "GET_EARLIEST_MSG_STORETIME",
            33 => "VIEW_MESSAGE_BY_ID",
            34 => "HEART_BEAT",
            35 => "UNREGISTER_CLIENT",
            36 => "CONSUMER_SEND_MSG_BACK",
            37 => "END_TRANSACTION",
            38 => "GET_CONSUMER_LIST_BY_GROUP",
            39 => "CHECK_TRANSACTION_STATE",
            40 => "NOTIFY_CONSUMER_IDS_CHANGED",
            41 => "LOCK_BATCH_MQ",
            42 => "UNLOCK_BATCH_MQ",
            43 => "GET_ALL_CONSUMER_OFFSET",
            45 => "GET_ALL_DELAY_OFFSET",
            46 => "CHECK_CLIENT_CONFIG",
            47 => "GET_CLIENT_CONFIG",
            50 => "UPDATE_AND_CREATE_ACL_CONFIG",
            51 => "DELETE_ACL_CONFIG",
            52 => "GET_BROKER_CLUSTER_ACL_INFO",
            53 => "UPDATE_GLOBAL_WHITE_ADDRS_CONFIG",
            54 => "GET_BROKER_CLUSTER_ACL_CONFIG",
            60 => "GET_TIMER_CHECK_POINT",
            61 => "GET_TIMER_METRICS",
            200050 => "POP_MESSAGE",
            200051 => "ACK_MESSAGE",
            200151 => "BATCH_ACK_MESSAGE",
            200052 => "PEEK_MESSAGE",
            200053 => "CHANGE_MESSAGE_INVISIBLETIME",
            200054 => "NOTIFICATION",
            200055 => "POLLING_INFO",
            100 => "PUT_KV_CONFIG",
            101 => "GET_KV_CONFIG",
            102 => "DELETE_KV_CONFIG",
            103 => "REGISTER_BROKER",
            104 => "UNREGISTER_BROKER",
            105 => "GET_ROUTEINFO_BY_TOPIC",
            106 => "GET_BROKER_CLUSTER_INFO",
            200 => "UPDATE_AND_CREATE_SUBSCRIPTIONGROUP",
            201 => "GET_ALL_SUBSCRIPTIONGROUP_CONFIG",
            202 => "GET_TOPIC_STATS_INFO",
            203 => "GET_CONSUMER_CONNECTION_LIST",
            204 => "GET_PRODUCER_CONNECTION_LIST",
            205 => "WIPE_WRITE_PERM_OF_BROKER",
            206 => "GET_ALL_TOPIC_LIST_FROM_NAMESERVER",
            207 => "DELETE_SUBSCRIPTIONGROUP",
            208 => "GET_CONSUME_STATS",
            209 => "SUSPEND_CONSUMER",
            210 => "RESUME_CONSUMER",
            211 => "RESET_CONSUMER_OFFSET_IN_CONSUMER",
            212 => "RESET_CONSUMER_OFFSET_IN_BROKER",
            213 => "ADJUST_CONSUMER_THREAD_POOL",
            214 => "WHO_CONSUME_THE_MESSAGE",
            215 => "DELETE_TOPIC_IN_BROKER",
            216 => "DELETE_TOPIC_IN_NAMESRV",
            217 => "REGISTER_TOPIC_IN_NAMESRV",
            219 => "GET_KVLIST_BY_NAMESPACE",
            220 => "RESET_CONSUMER_CLIENT_OFFSET",
            221 => "GET_CONSUMER_STATUS_FROM_CLIENT",
            222 => "INVOKE_BROKER_TO_RESET_OFFSET",
            223 => "INVOKE_BROKER_TO_GET_CONSUMER_STATUS",
            300 => "QUERY_TOPIC_CONSUME_BY_WHO",
            224 => "GET_TOPICS_BY_CLUSTER",
            225 => "UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST",
            343 => "QUERY_TOPICS_BY_CONSUMER",
            345 => "QUERY_SUBSCRIPTION_BY_CONSUMER",
            301 => "REGISTER_FILTER_SERVER",
            302 => "REGISTER_MESSAGE_FILTER_CLASS",
            303 => "QUERY_CONSUME_TIME_SPAN",
            304 => "GET_SYSTEM_TOPIC_LIST_FROM_NS",
            305 => "GET_SYSTEM_TOPIC_LIST_FROM_BROKER",
            306 => "CLEAN_EXPIRED_CONSUMEQUEUE",
            307 => "GET_CONSUMER_RUNNING_INFO",
            308 => "QUERY_CORRECTION_OFFSET",
            309 => "CONSUME_MESSAGE_DIRECTLY",
            310 => "SEND_MESSAGE_V2",
            311 => "GET_UNIT_TOPIC_LIST",
            312 => "GET_HAS_UNIT_SUB_TOPIC_LIST",
            313 => "GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST",
            314 => "CLONE_GROUP_OFFSET",
            315 => "VIEW_BROKER_STATS_DATA",
            316 => "CLEAN_UNUSED_TOPIC",
            317 => "GET_BROKER_CONSUME_STATS",
            318 => "UPDATE_NAMESRV_CONFIG",
            319 => "GET_NAMESRV_CONFIG",
            320 => "SEND_BATCH_MESSAGE",
            321 => "QUERY_CONSUME_QUEUE",
            322 => "QUERY_DATA_VERSION",
            323 => "RESUME_CHECK_HALF_MESSAGE",
            324 => "SEND_REPLY_MESSAGE",
            325 => "SEND_REPLY_MESSAGE_V2",
            326 => "PUSH_REPLY_MESSAGE_TO_CLIENT",
            327 => "ADD_WRITE_PERM_OF_BROKER",
            351 => "GET_TOPIC_CONFIG",
            352 => "GET_SUBSCRIPTIONGROUP_CONFIG",
            353 => "UPDATE_AND_GET_GROUP_FORBIDDEN",
            354 => "CHECK_ROCKSDB_CQ_WRITE_PROGRESS",
            361 => "LITE_PULL_MESSAGE",
            400 => "QUERY_ASSIGNMENT",
            401 => "SET_MESSAGE_REQUEST_MODE",
            402 => "GET_ALL_MESSAGE_REQUEST_MODE",
            513 => "UPDATE_AND_CREATE_STATIC_TOPIC",
            901 => "GET_BROKER_MEMBER_GROUP",
            902 => "ADD_BROKER",
            903 => "REMOVE_BROKER",
            904 => "BROKER_HEARTBEAT",
            905 => "NOTIFY_MIN_BROKER_ID_CHANGE",
            906 => "EXCHANGE_BROKER_HA_INFO",
            907 => "GET_BROKER_HA_STATUS",
            908 => "RESET_MASTER_FLUSH_OFFSET",
            328 => "GET_ALL_PRODUCER_INFO",
            329 => "DELETE_EXPIRED_COMMITLOG",
            1001 => "CONTROLLER_ALTER_SYNC_STATE_SET",
            1002 => "CONTROLLER_ELECT_MASTER",
            1003 => "CONTROLLER_REGISTER_BROKER",
            1004 => "CONTROLLER_GET_REPLICA_INFO",
            1005 => "CONTROLLER_GET_METADATA_INFO",
            1006 => "CONTROLLER_GET_SYNC_STATE_DATA",
            1007 => "GET_BROKER_EPOCH_CACHE",
            1008 => "NOTIFY_BROKER_ROLE_CHANGED",
            1009 => "UPDATE_CONTROLLER_CONFIG",
            1010 => "GET_CONTROLLER_CONFIG",
            1011 => "CLEAN_BROKER_DATA",
            1012 => "CONTROLLER_GET_NEXT_BROKER_ID",
            1013 => "CONTROLLER_APPLY_BROKER_ID",
            1014 => "BROKER_CLOSE_CHANNEL_REQUEST",
            1015 => "CHECK_NOT_ACTIVE_BROKER_REQUEST",
            1016 => "GET_BROKER_LIVE_INFO_REQUEST",
            1017 => "GET_SYNC_STATE_DATA_REQUEST",
            1018 => "RAFT_BROKER_HEART_BEAT_EVENT_REQUEST",
            2001 => "UPDATE_COLD_DATA_FLOW_CTR_CONFIG",
            2002 => "REMOVE_COLD_DATA_FLOW_CTR_CONFIG",
            2003 => "GET_COLD_DATA_FLOW_CTR_INFO",
            2004 => "SET_COMMITLOG_READ_MODE",
            3001 => "AUTH_CREATE_USER",
            3002 => "AUTH_UPDATE_USER",
            3003 => "AUTH_DELETE_USER",
            3004 => "AUTH_GET_USER",
            3005 => "AUTH_LIST_USER",
            3006 => "AUTH_CREATE_ACL",
            3007 => "AUTH_UPDATE_ACL",
            3008 => "AUTH_DELETE_ACL",
            3009 => "AUTH_GET_ACL",
            3010 => "AUTH_LIST_ACL",
            _ => "",
        }
    }

    pub fn get_response_code_str_and_status(&self) -> (&'static str, L7ResponseStatus) {
        if self.is_request() {
            return ("", L7ResponseStatus::NotExist);
        }
        // compatible for versions between v4.4.0 and v5.3.1
        match self.header_data.code {
            0 => ("SUCCESS", L7ResponseStatus::Ok),
            1 => ("SYSTEM_ERROR", L7ResponseStatus::ServerError),
            2 => ("SYSTEM_BUSY", L7ResponseStatus::ServerError),
            3 => ("REQUEST_CODE_NOT_SUPPORTED", L7ResponseStatus::ClientError),
            4 => ("TRANSACTION_FAILED", L7ResponseStatus::ServerError),
            10 => ("FLUSH_DISK_TIMEOUT", L7ResponseStatus::ServerError),
            11 => ("SLAVE_NOT_AVAILABLE", L7ResponseStatus::ServerError),
            12 => ("FLUSH_SLAVE_TIMEOUT", L7ResponseStatus::ServerError),
            13 => ("MESSAGE_ILLEGAL", L7ResponseStatus::ClientError),
            14 => ("SERVICE_NOT_AVAILABLE", L7ResponseStatus::ServerError),
            15 => ("VERSION_NOT_SUPPORTED", L7ResponseStatus::ClientError),
            16 => ("NO_PERMISSION", L7ResponseStatus::ClientError),
            17 => ("TOPIC_NOT_EXIST", L7ResponseStatus::ClientError),
            18 => ("TOPIC_EXIST_ALREADY", L7ResponseStatus::ClientError),
            19 => ("PULL_NOT_FOUND", L7ResponseStatus::ClientError),
            20 => ("PULL_RETRY_IMMEDIATELY", L7ResponseStatus::ClientError),
            21 => ("PULL_OFFSET_MOVED", L7ResponseStatus::ClientError),
            22 => ("QUERY_NOT_FOUND", L7ResponseStatus::ClientError),
            23 => ("SUBSCRIPTION_PARSE_FAILED", L7ResponseStatus::ClientError),
            24 => ("SUBSCRIPTION_NOT_EXIST", L7ResponseStatus::ClientError),
            25 => ("SUBSCRIPTION_NOT_LATEST", L7ResponseStatus::ClientError),
            26 => (
                "SUBSCRIPTION_GROUP_NOT_EXIST",
                L7ResponseStatus::ClientError,
            ),
            27 => ("FILTER_DATA_NOT_EXIST", L7ResponseStatus::ClientError),
            28 => ("FILTER_DATA_NOT_LATEST", L7ResponseStatus::ClientError),
            200 => ("TRANSACTION_SHOULD_COMMIT", L7ResponseStatus::ClientError),
            201 => ("TRANSACTION_SHOULD_ROLLBACK", L7ResponseStatus::ClientError),
            202 => ("TRANSACTION_STATE_UNKNOW", L7ResponseStatus::ServerError),
            203 => (
                "TRANSACTION_STATE_GROUP_WRONG",
                L7ResponseStatus::ServerError,
            ),
            204 => ("NO_BUYER_ID", L7ResponseStatus::ClientError),
            205 => ("NOT_IN_CURRENT_UNIT", L7ResponseStatus::ClientError),
            206 => ("CONSUMER_NOT_ONLINE", L7ResponseStatus::ServerError),
            207 => ("CONSUME_MSG_TIMEOUT", L7ResponseStatus::ServerError),
            208 => ("NO_MESSAGE", L7ResponseStatus::ServerError),
            209 => (
                "UPDATE_AND_CREATE_ACL_CONFIG_FAILED",
                L7ResponseStatus::ServerError,
            ),
            210 => ("DELETE_ACL_CONFIG_FAILED", L7ResponseStatus::ServerError),
            211 => (
                "UPDATE_GLOBAL_WHITE_ADDRS_CONFIG_FAILED",
                L7ResponseStatus::ServerError,
            ),
            /* the following duplicated numbers are rocketmq codes' reason (added in v5.0.0):
             * 209 => ("POLLING_FULL", L7ResponseStatus::ServerError),
             * 210 => ("POLLING_TIMEOUT", L7ResponseStatus::ServerError),
             * 211 => ("BROKER_NOT_EXIST", L7ResponseStatus::ClientError),
             */
            212 => (
                "BROKER_DISPATCH_NOT_COMPLETE",
                L7ResponseStatus::ServerError,
            ),
            213 => ("BROADCAST_CONSUMPTION", L7ResponseStatus::ServerError),
            215 => ("FLOW_CONTROL", L7ResponseStatus::ServerError),
            501 => ("NOT_LEADER_FOR_QUEUE", L7ResponseStatus::ClientError),
            604 => ("ILLEGAL_OPERATION", L7ResponseStatus::ClientError),
            -1000 => ("RPC_UNKNOWN", L7ResponseStatus::ServerError),
            -1002 => ("RPC_ADDR_IS_NULL", L7ResponseStatus::ServerError),
            -1004 => ("RPC_SEND_TO_CHANNEL_FAILED", L7ResponseStatus::ServerError),
            -1006 => ("RPC_TIME_OUT", L7ResponseStatus::ServerError),
            1500 => ("GO_AWAY", L7ResponseStatus::ServerError),
            2000 => (
                "CONTROLLER_FENCED_MASTER_EPOCH",
                L7ResponseStatus::ServerError,
            ),
            2001 => (
                "CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH",
                L7ResponseStatus::ClientError,
            ),
            2002 => ("CONTROLLER_INVALID_MASTER", L7ResponseStatus::ServerError),
            2003 => ("CONTROLLER_INVALID_REPLICAS", L7ResponseStatus::ClientError),
            2004 => (
                "CONTROLLER_MASTER_NOT_AVAILABLE",
                L7ResponseStatus::ServerError,
            ),
            2005 => ("CONTROLLER_INVALID_REQUEST", L7ResponseStatus::ClientError),
            2006 => ("CONTROLLER_BROKER_NOT_ALIVE", L7ResponseStatus::ServerError),
            2007 => ("CONTROLLER_NOT_LEADER", L7ResponseStatus::ServerError),
            2008 => (
                "CONTROLLER_BROKER_METADATA_NOT_EXIST",
                L7ResponseStatus::ClientError,
            ),
            2009 => (
                "CONTROLLER_INVALID_CLEAN_BROKER_METADATA",
                L7ResponseStatus::ServerError,
            ),
            2010 => (
                "CONTROLLER_BROKER_NEED_TO_BE_REGISTERED",
                L7ResponseStatus::ClientError,
            ),
            2011 => (
                "CONTROLLER_MASTER_STILL_EXIST",
                L7ResponseStatus::ClientError,
            ),
            2012 => (
                "CONTROLLER_ELECT_MASTER_FAILED",
                L7ResponseStatus::ServerError,
            ),
            2013 => (
                "CONTROLLER_ALTER_SYNC_STATE_SET_FAILED",
                L7ResponseStatus::ClientError,
            ),
            2014 => (
                "CONTROLLER_BROKER_ID_INVALID",
                L7ResponseStatus::ClientError,
            ),
            2015 => (
                "CONTROLLER_JRAFT_INTERNAL_ERROR",
                L7ResponseStatus::ServerError,
            ),
            2016 => (
                "CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS",
                L7ResponseStatus::ClientError,
            ),
            3001 => ("USER_NOT_EXIST", L7ResponseStatus::ClientError),
            3002 => ("POLICY_NOT_EXIST", L7ResponseStatus::ClientError),
            _ => ("", L7ResponseStatus::NotExist),
        }
    }
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct RocketmqBody {
    body_length: i32,
    serialize_type: RocketmqSerializeType,
    body_data: RocketmqBodyData,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct RocketmqBodyData {
    body: String,
}

impl RocketmqBody {
    fn decode(&mut self, payload: &[u8], header: &RocketmqHeader) -> isize {
        self.body_length = header.length - 4 - header.header_length;
        if self.body_length > payload.len() as i32 {
            return -1;
        } else if self.body_length == 0 {
            return 0;
        }
        if !header.is_remark_found() {
            self.serialize_type = RocketmqSerializeType::Json;
            if self.decode_for_json_type(payload) > 0 {
                return self.body_length as isize;
            }
        } else {
            self.serialize_type = RocketmqSerializeType::RocketMQ;
            if self.decode_for_rocketmq_type(payload) > 0 {
                return self.body_length as isize;
            }
        }
        -1
    }

    // directly use the whole json string as body content
    fn decode_for_json_type(&mut self, data: &[u8]) -> isize {
        /*
         * Request Direction:
         *   clientID
         *   consumerDataSet
         *   producerDataSet
         *
         * Response Direction:
         *   brokerDatas
         *   queueDatas
         *   filterServerTable
         *   consumerIdList
         *   groupList
         *   topicList
         */
        self.body_data.body = String::from_utf8_lossy(data).into_owned();
        data.len() as isize
    }

    // only use the body string as body content
    fn decode_for_rocketmq_type(&mut self, data: &[u8]) -> isize {
        /*
         * Response Direction:
         *   totalSize(4B), magicCode(4B), bodyCRC(4B), queueId(4B), flag(4B),
         *   queueOffset(8B), physicOffset(8B), sysFlag(4B),
         *   bornTimeStamp(8B), bornHost(4B), port(4B),
         *   storeTimestamp(8B), storeHost(4B), storePort(4B),
         *   reconsumeTimes(4B), preparedTransactionOffset(8B),
         *   bodyStrLength(4B), bodyStr(bodyLength),
         *   topicLength(1B), topic(topicLength),
         *   propertiesStrLength(2B), propertiesStr(propertiesLength)
         */
        let mut offset: usize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 4 + 4 + 8 + 4 + 4 + 4 + 8;
        let length = bytes::read_i32_be(&data[offset..(offset + 4)]);
        offset += 4;
        if length < 0 || length > (data.len() - offset) as i32 {
            return -1;
        }
        self.body_data.body =
            String::from_utf8_lossy(&data[offset..(offset + length as usize)]).into_owned();
        data.len() as isize
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::rc::Rc;
    use std::{cell::RefCell, fs};

    use super::*;

    use crate::{
        common::{flow::PacketDirection, l7_protocol_log::L7PerfCache, MetaPacket},
        flow_generator::L7_RRT_CACHE_CAPACITY,
        utils::test::Capture,
    };

    const FILE_DIR: &str = "resources/test/flow_generator/rocketmq";

    fn run(name: &str) -> String {
        let capture = Capture::load_pcap(Path::new(FILE_DIR).join(name), None);
        let log_cache = Rc::new(RefCell::new(L7PerfCache::new(L7_RRT_CACHE_CAPACITY)));
        let mut packets = capture.as_meta_packets();
        if packets.is_empty() {
            return "".to_string();
        }

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

            let mut rocketmq = RocketmqLog::default();
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

            let is_rocketmq = rocketmq.check_payload(payload, param);
            let info = rocketmq.parse_payload(payload, param);
            if let Ok(info) = info {
                match info.unwrap_single() {
                    L7ProtocolInfo::RocketmqInfo(i) => {
                        output.push_str(&format!("{:?} is_rocketmq: {}\n", i, is_rocketmq));
                    }
                    _ => unreachable!(),
                }
            } else {
                output.push_str(&format!(
                    "{:?} is_rocketmq: {}\n",
                    RocketmqInfo::default(),
                    is_rocketmq
                ));
            }
        }
        output
    }

    #[test]
    fn check() {
        let files = vec![
            ("rocketmq-heartbeat.pcap", "rocketmq-heartbeat.result"),
            (
                "rocketmq-get-routeinfo-by-topic.pcap",
                "rocketmq-get-routeinfo-by-topic.result",
            ),
            (
                "rocketmq-get-consumer-list-by-group.pcap",
                "rocketmq-get-consumer-list-by-group.result",
            ),
            (
                "rocketmq-send-message-v2.pcap",
                "rocketmq-send-message-v2.result",
            ),
            ("rocketmq-pull-message.pcap", "rocketmq-pull-message.result"),
            (
                "rocketmq-update-consumer-offset.pcap",
                "rocketmq-update-consumer-offset.result",
            ),
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
