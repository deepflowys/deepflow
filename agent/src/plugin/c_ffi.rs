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

use std::net::IpAddr;
use std::sync::{Arc, Weak};

use public::enums::IpProtocol;

use crate::flow_generator::protocol_logs::pb_adapter::KeyVal;
use crate::flow_generator::protocol_logs::LogMessageType;
use crate::plugin::PluginCounterInfo;
use crate::{common::l7_protocol_log::ParseParam, flow_generator::protocol_logs::L7ResponseStatus};

use super::{
    shared_obj::SoPluginCounter, CustomInfo, CustomInfoRequest, CustomInfoResp, CustomInfoTrace,
};
use public::counter::{Countable, RefCountable};

pub const INIT_FUNC_SYM: &'static str = "init";
pub const CHECK_PAYLOAD_FUNC_SYM: &'static str = "on_check_payload";
pub const PARSE_PAYLOAD_FUNC_SYM: &'static str = "on_parse_payload";

#[repr(C)]
pub struct ParseCtx {
    pub(super) ip_type: u8, // 4 or 6
    pub(super) ip_src: [u8; 16],
    pub(super) ip_dst: [u8; 16],
    pub(super) port_src: u16,
    pub(super) port_dst: u16,
    pub(super) l4_protocol: u8,
    // proto is return from on_check_payload, when on_check_payload, it set to 0, other wise will set to non zero value
    pub proto: u8,
    pub(super) ebpf_type: u8,
    pub(super) time: u64,
    pub(super) direction: u8,
    pub(super) process_kname: *const u8,
    // the config of `l7_log_packet_size`
    pub(super) buf_size: i32,
    pub(super) payload_size: i32,
    /*
        payload is from the payload: &[u8] in L7ProtocolParserInterface::check_payload() and L7ProtocolParserInterface::parse_payload(),
        it can not modify and drop.
    */
    pub(super) payload: *const u8,
}

impl From<(&ParseParam<'_>, &[u8])> for ParseCtx {
    fn from(v: (&ParseParam<'_>, &[u8])) -> Self {
        let (p, payload) = v;
        let mut ctx = Self {
            ip_type: 0,
            ip_src: [0u8; 16],
            ip_dst: [0u8; 16],

            port_src: p.port_src,
            port_dst: p.port_dst,
            l4_protocol: match p.l4_protocol {
                IpProtocol::TCP => 6,
                IpProtocol::UDP => 17,
                _ => unreachable!(),
            },
            proto: 0,
            ebpf_type: (p.ebpf_type as u8),
            time: p.time,
            direction: (p.direction as u8),
            process_kname: if let Some(e) = p.ebpf_param.as_ref() {
                e.process_kname.as_ptr()
            } else {
                "".as_ptr()
            },
            buf_size: p.buf_size as i32,
            payload_size: payload.len() as i32,
            payload: payload.as_ptr(),
        };

        match (p.ip_src, p.ip_dst) {
            (IpAddr::V4(src), IpAddr::V4(dst)) => {
                ctx.ip_type = 4;
                (&mut ctx.ip_src[..4]).copy_from_slice(&src.octets());
                (&mut ctx.ip_dst[..4]).copy_from_slice(&dst.octets());
            }
            (IpAddr::V6(src), IpAddr::V6(dst)) => {
                ctx.ip_type = 6;
                (&mut ctx.ip_src).copy_from_slice(&src.octets());
                (&mut ctx.ip_dst).copy_from_slice(&dst.octets());
            }
            _ => unreachable!(),
        };

        ctx
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Request {
    pub(super) req_type: [u8; 64],
    pub(super) domain: [u8; 128],
    pub(super) resource: [u8; 128],
    pub(super) endpoint: [u8; 128],
}

impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("req_type", &c_str_to_string(&self.req_type))
            .field("domain", &c_str_to_string(&self.domain))
            .field("resource", &c_str_to_string(&self.resource))
            .field("endpoint", &c_str_to_string(&self.endpoint))
            .finish()
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Response {
    pub(super) status: u8,
    pub(super) code: i32,
    pub(super) exception: [u8; 128],
    pub(super) result: [u8; 512],
}
impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("code", &self.code)
            .field("exception", &c_str_to_string(&self.exception))
            .field("result", &c_str_to_string(&self.result))
            .finish()
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TraceInfo {
    pub trace_id: [u8; 128],
    pub span_id: [u8; 128],
    pub parent_span_id: [u8; 128],
}

impl std::fmt::Debug for TraceInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceInfo")
            .field("trace_id", &c_str_to_string(&self.trace_id))
            .field("span_id", &c_str_to_string(&self.span_id))
            .field("parent_span_id", &c_str_to_string(&self.parent_span_id))
            .finish()
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union ReqRespUnion {
    pub(super) req: Request,
    pub(super) resp: Response,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParseInfo {
    pub(super) msg_type: u8,
    pub(super) req_len: i32,
    pub(super) resp_len: i32,
    pub(super) has_request_id: i8,
    pub(super) request_id: u32,
    pub(super) trace: TraceInfo,
    pub(super) req_resp: ReqRespUnion,
    pub(super) attr_len: u32,
    // format: repeated (${key bytes}\0${val bytes}\0)
    pub(super) attributes: [u8; 6144],
}

impl std::fmt::Debug for ParseInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg_type = self.msg_type.to_string();
        let t = LogMessageType::try_from(self.msg_type);
        if t.is_err() {
            return Ok(());
        }
        let t = t.unwrap();
        f.debug_struct("ParseInfo")
            .field("msg_type", &self.msg_type)
            .field("req_len", &self.req_len)
            .field("resp_len", &self.resp_len)
            .field("has_request_id", &self.has_request_id)
            .field("request_id", &self.request_id)
            .field("trace", &self.trace)
            .field(
                match t {
                    LogMessageType::Request => "req",
                    LogMessageType::Response => "resp",
                    _ => "unknown log msg type",
                },
                // union must use unsafe, correctness depends on plugin implementation
                unsafe {
                    match t {
                        LogMessageType::Request => &self.req_resp.req,
                        LogMessageType::Response => &self.req_resp.resp,
                        _ => &msg_type,
                    }
                },
            )
            .field("attr_len", &self.attr_len)
            .field("attributes", &read_attr(&self.attributes, self.attr_len))
            .finish()
    }
}

impl Default for ParseInfo {
    fn default() -> Self {
        Self {
            trace: TraceInfo {
                trace_id: [0; 128],
                span_id: [0; 128],
                parent_span_id: [0; 128],
            },
            req_resp: ReqRespUnion {
                req: Request {
                    req_type: [0; 64],
                    domain: [0; 128],
                    resource: [0; 128],
                    endpoint: [0; 128],
                },
            },
            attr_len: 0,
            attributes: [0; 6144],
            msg_type: 0,
            req_len: 0,
            resp_len: 0,
            has_request_id: 0,
            request_id: 0,
        }
    }
}

impl TryFrom<ParseInfo> for CustomInfo {
    type Error = String;

    fn try_from(v: ParseInfo) -> Result<Self, Self::Error> {
        let msg_type = LogMessageType::try_from(v.msg_type).map_err(|e| e.to_string())?;
        let (req, resp) = match msg_type {
            LogMessageType::Request => {
                // union must use unsafe, correctness depends on plugin implementation
                let req = unsafe { v.req_resp.req };
                (
                    CustomInfoRequest {
                        req_type: c_str_to_string(&req.req_type).unwrap_or_default(),
                        domain: c_str_to_string(&req.domain).unwrap_or_default(),
                        resource: c_str_to_string(&req.resource).unwrap_or_default(),
                        endpoint: c_str_to_string(&req.endpoint).unwrap_or_default(),
                    },
                    CustomInfoResp::default(),
                )
            }
            LogMessageType::Response => {
                // union must use unsafe, correctness depends on plugin implementation
                let resp = unsafe { v.req_resp.resp };
                (
                    CustomInfoRequest::default(),
                    CustomInfoResp {
                        status: match L7ResponseStatus::try_from(resp.status)
                            .map_err(|e| e.to_string())?
                        {
                            L7ResponseStatus::Ok => L7ResponseStatus::Ok,
                            L7ResponseStatus::NotExist => L7ResponseStatus::NotExist,
                            L7ResponseStatus::ServerError => L7ResponseStatus::ServerError,
                            L7ResponseStatus::ClientError => L7ResponseStatus::ClientError,
                            L7ResponseStatus::Timeout => L7ResponseStatus::Timeout,
                        },
                        code: Some(resp.code),
                        exception: c_str_to_string(&resp.exception).unwrap_or_default(),
                        result: c_str_to_string(&resp.result).unwrap_or_default(),
                    },
                )
            }
            _ => return Err(format!("msg type {} invalid", v.msg_type)),
        };

        Ok(Self {
            msg_type: match msg_type {
                LogMessageType::Request => LogMessageType::Request,
                LogMessageType::Response => LogMessageType::Response,
                _ => unreachable!(),
            },
            req_len: if v.req_len > 0 {
                Some(v.req_len as u32)
            } else {
                None
            },
            resp_len: if v.resp_len > 0 {
                Some(v.resp_len as u32)
            } else {
                None
            },
            request_id: if v.has_request_id > 0 {
                Some(v.request_id)
            } else {
                None
            },
            req,
            resp,
            trace: CustomInfoTrace {
                trace_id: c_str_to_string(&v.trace.trace_id),
                span_id: c_str_to_string(&v.trace.span_id),
                parent_span_id: c_str_to_string(&v.trace.parent_span_id),
            },
            attributes: read_attr(&v.attributes, v.attr_len),
            ..Default::default()
        })
    }
}

#[repr(C)]
pub struct CheckResult {
    pub proto: u8,
    pub proto_name: [u8; 16],
}

pub const ACTION_ERROR: u8 = 0;
pub const ACTION_CONTINUE: u8 = 1;
pub const ACTION_OK: u8 = 2;

#[repr(C)]
pub struct ParseResult {
    pub action: u8,
    pub len: i32,
}

pub type CheckPayloadCFunc = unsafe extern "C" fn(*const ParseCtx) -> CheckResult;

// due to C can not return variable length data, use the consistent length `info_max_len` as length of ParseInfo array
pub type ParsePayloadCFunc =
    unsafe extern "C" fn(*const ParseCtx, *mut ParseInfo, info_max_len: i32) -> ParseResult;

#[derive(Clone)]
pub struct SoPluginFunc {
    pub hash: String,
    pub name: String,
    pub check_payload_counter: Arc<SoPluginCounter>,
    pub parse_payload_counter: Arc<SoPluginCounter>,
    pub check_payload: CheckPayloadCFunc,
    // due to C can not return variable length data, use the consistent length `result_max_len` as length of ParseResult array
    // return < 0 indicate fail, >=0 assume success
    pub parse_payload: ParsePayloadCFunc,
}

impl PartialEq for SoPluginFunc {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for SoPluginFunc {}

impl SoPluginFunc {
    pub fn counters_in<'a>(&'a self, counters: &mut Vec<PluginCounterInfo<'a>>) {
        counters.push(PluginCounterInfo {
            plugin_name: self.name.as_str(),
            plugin_type: "so",
            function_name: CHECK_PAYLOAD_FUNC_SYM,
            counter: Countable::Ref(
                Arc::downgrade(&self.check_payload_counter) as Weak<dyn RefCountable>
            ),
        });
        counters.push(PluginCounterInfo {
            plugin_name: self.name.as_str(),
            plugin_type: "so",
            function_name: PARSE_PAYLOAD_FUNC_SYM,
            counter: Countable::Ref(
                Arc::downgrade(&self.parse_payload_counter) as Weak<dyn RefCountable>
            ),
        });
    }

    pub fn counters<'a>(&'a self) -> Vec<PluginCounterInfo<'a>> {
        let mut info = vec![];
        self.counters_in(&mut info);
        info
    }
}

pub fn c_str_to_string(s: &[u8]) -> Option<String> {
    s.iter()
        .position(|b| *b == 0)
        .map_or(Some(String::from_utf8_lossy(s).to_string()), |i| {
            if i == 0 {
                None
            } else {
                Some(String::from_utf8_lossy(&s[..i]).to_string())
            }
        })
}

fn read_attr(attr_bytes: &[u8], mut attr_len: u32) -> Vec<KeyVal> {
    let mut attr = vec![];
    let mut off = 0;
    while off < attr_bytes.len() && attr_len > 0 {
        let Some(key_idx) = (&attr_bytes[off..]).iter().position(|b| *b == 0) else {
            break;
        };
        let key = String::from_utf8_lossy(&attr_bytes[off..off + key_idx]).to_string();
        off += key_idx + 1;
        if off < attr_bytes.len() {
            let Some(val_idx) = &attr_bytes[off..].iter().position(|b| *b == 0) else {
                break;
            };
            let val = String::from_utf8_lossy(&attr_bytes[off..off + val_idx]).to_string();
            off += val_idx + 1;
            attr.push(KeyVal { key, val })
        }
        attr_len -= 1;
    }
    attr
}
