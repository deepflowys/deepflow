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

use pnet::packet::icmp::{IcmpType, IcmpTypes};
use serde::Serialize;

use crate::{
    common::{
        enums::IpProtocol,
        flow::{L7PerfStats, L7Protocol},
        l7_protocol_info::{L7ProtocolInfo, L7ProtocolInfoInterface},
        l7_protocol_log::{L7ParseResult, L7ProtocolParserInterface, ParseParam},
    },
    flow_generator::error::{Error, Result},
    flow_generator::protocol_logs::{
        pb_adapter::{ExtendedInfo, L7ProtocolSendLog, L7Request, L7Response},
        set_captured_byte, AppProtoHead, L7ResponseStatus, LogMessageType,
    },
};

#[derive(Serialize, Debug, Default, Clone)]
pub struct PingInfo {
    proto: L7Protocol,

    msg_type: LogMessageType,

    sequence: u16,
    id: u16,
    status: L7ResponseStatus,
    rrt: u64,

    captured_request_byte: u32,
    captured_response_byte: u32,
}

impl L7ProtocolInfoInterface for PingInfo {
    fn session_id(&self) -> Option<u32> {
        Some(((self.id as u32) << 16) | self.sequence as u32)
    }

    fn merge_log(&mut self, other: &mut L7ProtocolInfo) -> Result<()> {
        if let L7ProtocolInfo::PingInfo(other) = other {
            return self.merge(other);
        }
        Ok(())
    }

    fn app_proto_head(&self) -> Option<AppProtoHead> {
        Some(AppProtoHead {
            proto: self.proto,
            msg_type: self.msg_type,
            rrt: self.rrt,
        })
    }

    fn is_tls(&self) -> bool {
        false
    }
}

impl PingInfo {
    pub fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.msg_type = LogMessageType::Session;
        match other.msg_type {
            LogMessageType::Response => {
                self.rrt = other.rrt;
                self.captured_response_byte = other.captured_response_byte;
                self.status = L7ResponseStatus::Ok;
            }
            LogMessageType::Request => {
                self.captured_request_byte = other.captured_request_byte;
                self.status = L7ResponseStatus::Ok;
            }
            _ => {}
        }

        Ok(())
    }
}

impl From<PingInfo> for L7ProtocolSendLog {
    fn from(f: PingInfo) -> Self {
        L7ProtocolSendLog {
            captured_request_byte: f.captured_request_byte,
            captured_response_byte: f.captured_response_byte,
            req: L7Request {
                resource: f.id.to_string(),
                ..Default::default()
            },
            resp: L7Response {
                status: f.status,
                ..Default::default()
            },
            ext_info: Some(ExtendedInfo {
                request_id: Some(f.sequence as u32),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Default)]
pub struct PingLog {
    proto: L7Protocol,
    perf_stats: Option<L7PerfStats>,
}

impl L7ProtocolParserInterface for PingLog {
    fn check_payload(&mut self, _: &[u8], param: &ParseParam) -> bool {
        if param.l4_protocol != IpProtocol::ICMPV4 && param.l4_protocol != IpProtocol::ICMPV6 {
            return false;
        }

        let Some(icmp_data) = param.icmp_data else {
            return false;
        };

        icmp_data.icmp_type == IcmpTypes::EchoRequest.0
            || icmp_data.icmp_type == IcmpTypes::EchoReply.0
    }

    fn parse_payload(&mut self, _: &[u8], param: &ParseParam) -> Result<L7ParseResult> {
        let Some(icmp_data) = param.icmp_data else {
            return Err(Error::PingHeaderParseFailed);
        };

        if icmp_data.icmp_type != IcmpTypes::EchoRequest.0
            && icmp_data.icmp_type != IcmpTypes::EchoReply.0
        {
            return Err(Error::PingHeaderParseFailed);
        }

        match IcmpType::new(icmp_data.icmp_type) {
            IcmpTypes::EchoRequest => {
                let mut info = PingInfo {
                    msg_type: LogMessageType::Request,
                    proto: L7Protocol::Ping,
                    sequence: icmp_data.echo_id_seq as u16,
                    id: (icmp_data.echo_id_seq >> 16) as u16,
                    status: L7ResponseStatus::Timeout,
                    ..Default::default()
                };
                set_captured_byte!(info, param);
                self.perf_stats.as_mut().map(|p| p.inc_req());
                info.cal_rrt(param).map(|rrt| {
                    info.rrt = rrt;
                    self.perf_stats.as_mut().map(|p| p.update_rrt(rrt));
                });

                Ok(L7ParseResult::Single(L7ProtocolInfo::PingInfo(info)))
            }
            IcmpTypes::EchoReply => {
                let mut info = PingInfo {
                    msg_type: LogMessageType::Response,
                    proto: L7Protocol::Ping,
                    sequence: icmp_data.echo_id_seq as u16,
                    id: (icmp_data.echo_id_seq >> 16) as u16,
                    status: L7ResponseStatus::Timeout,
                    ..Default::default()
                };
                set_captured_byte!(info, param);
                self.perf_stats.as_mut().map(|p| p.inc_resp());
                info.cal_rrt(param).map(|rrt| {
                    info.rrt = rrt;
                    self.perf_stats.as_mut().map(|p| p.update_rrt(rrt));
                });

                Ok(L7ParseResult::Single(L7ProtocolInfo::PingInfo(info)))
            }
            _ => Err(Error::PingHeaderParseFailed),
        }
    }

    fn protocol(&self) -> L7Protocol {
        L7Protocol::Ping
    }

    fn perf_stats(&mut self) -> Option<L7PerfStats> {
        self.perf_stats.take()
    }

    fn parsable_on_other(&self) -> bool {
        true
    }
}
