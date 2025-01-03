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

use std::{fmt, str};

use serde::{Serialize, Serializer};

use super::{
    super::{value_is_default, AppProtoHead, L7ResponseStatus, LogMessageType},
    ObfuscateCache,
};

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
            pb_adapter::{L7ProtocolSendLog, L7Request, L7Response},
            set_captured_byte,
        },
    },
};

const SEPARATOR_SIZE: usize = 2;

#[derive(Serialize, Debug, Default, Clone)]
pub struct RedisInfo {
    msg_type: LogMessageType,
    #[serde(skip)]
    is_tls: bool,

    #[serde(
        rename = "request_resource",
        skip_serializing_if = "value_is_default",
        serialize_with = "vec_u8_to_string"
    )]
    pub request: Vec<u8>, // 命令字段包括参数例如："set key value"
    #[serde(
        skip_serializing_if = "value_is_default",
        serialize_with = "vec_u8_to_string"
    )]
    pub request_type: Vec<u8>, // 命令类型不包括参数例如：命令为"set key value"，命令类型为："set"
    #[serde(
        rename = "response_result",
        skip_serializing_if = "value_is_default",
        serialize_with = "vec_u8_to_string"
    )]
    #[serde(skip)]
    pub status: Vec<u8>, // '+'
    #[serde(
        rename = "response_expection",
        skip_serializing_if = "value_is_default",
        serialize_with = "vec_u8_to_string"
    )]
    pub error: Vec<u8>, // '-'
    #[serde(rename = "response_status")]
    pub resp_status: L7ResponseStatus,

    captured_request_byte: u32,
    captured_response_byte: u32,

    rrt: u64,

    #[serde(skip)]
    is_on_blacklist: bool,
}

impl L7ProtocolInfoInterface for RedisInfo {
    fn session_id(&self) -> Option<u32> {
        None
    }

    fn merge_log(&mut self, other: &mut L7ProtocolInfo) -> Result<()> {
        if let L7ProtocolInfo::RedisInfo(other) = other {
            return self.merge(other);
        }
        Ok(())
    }

    fn app_proto_head(&self) -> Option<AppProtoHead> {
        Some(AppProtoHead {
            proto: L7Protocol::Redis,
            msg_type: self.msg_type,
            rrt: self.rrt,
        })
    }

    fn is_tls(&self) -> bool {
        self.is_tls
    }

    fn get_request_resource_length(&self) -> usize {
        self.request.len()
    }

    fn is_on_blacklist(&self) -> bool {
        self.is_on_blacklist
    }
}

pub fn vec_u8_to_string<S>(v: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&String::from_utf8_lossy(v))
}

impl RedisInfo {
    pub fn merge(&mut self, other: &mut Self) -> Result<()> {
        std::mem::swap(&mut self.status, &mut other.status);
        std::mem::swap(&mut self.error, &mut other.error);
        self.resp_status = other.resp_status;
        self.captured_response_byte = other.captured_response_byte;
        if other.is_on_blacklist {
            self.is_on_blacklist = other.is_on_blacklist;
        }
        Ok(())
    }

    fn set_is_on_blacklist(&mut self, config: &LogParserConfig) {
        if let Some(t) = config.l7_log_blacklist_trie.get(&L7Protocol::Redis) {
            self.is_on_blacklist = t
                .request_resource
                .is_on_blacklist(str::from_utf8(&self.request).unwrap_or_default())
                || t.request_type
                    .is_on_blacklist(str::from_utf8(&self.request_type).unwrap_or_default());
        }
    }
}

impl fmt::Display for RedisInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RedisInfo {{ request: {:?}, ",
            str::from_utf8(&self.request).unwrap_or_default()
        )?;
        write!(
            f,
            "request_type: {:?}, ",
            str::from_utf8(&self.request_type).unwrap_or_default()
        )?;
        write!(
            f,
            "status: {:?}, ",
            str::from_utf8(&self.status).unwrap_or_default()
        )?;
        write!(
            f,
            "error: {:?} }}",
            str::from_utf8(&self.error).unwrap_or_default()
        )
    }
}

impl From<RedisInfo> for L7ProtocolSendLog {
    fn from(f: RedisInfo) -> Self {
        let flags = if f.is_tls {
            EbpfFlags::TLS.bits()
        } else {
            EbpfFlags::NONE.bits()
        };
        let log = L7ProtocolSendLog {
            captured_request_byte: f.captured_request_byte,
            captured_response_byte: f.captured_response_byte,
            req: L7Request {
                req_type: String::from_utf8_lossy(f.request_type.as_slice()).to_string(),
                resource: String::from_utf8_lossy(f.request.as_slice()).to_string(),
                ..Default::default()
            },
            resp: L7Response {
                status: f.resp_status,
                exception: String::from_utf8_lossy(f.error.as_slice()).to_string(),
                ..Default::default()
            },
            flags,
            ..Default::default()
        };
        return log;
    }
}

#[derive(Default)]
pub struct RedisLog {
    has_request: bool,
    perf_stats: Option<L7PerfStats>,
    obfuscate: bool,
    last_is_on_blacklist: bool,
}

impl L7ProtocolParserInterface for RedisLog {
    fn check_payload(&mut self, payload: &[u8], param: &ParseParam) -> bool {
        if !param.ebpf_type.is_raw_protocol() {
            return false;
        }
        if param.l4_protocol != IpProtocol::TCP {
            return false;
        }

        CommandLine::new(payload).is_ok()
    }

    fn parse_payload(&mut self, payload: &[u8], param: &ParseParam) -> Result<L7ParseResult> {
        if self.perf_stats.is_none() && param.parse_perf {
            self.perf_stats = Some(L7PerfStats::default())
        };
        let mut info = RedisInfo::default();
        info.is_tls = param.is_tls();
        self.parse(payload, param.l4_protocol, param.direction, &mut info)?;
        set_captured_byte!(info, param);
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
                    if info.resp_status == L7ResponseStatus::ServerError {
                        self.perf_stats.as_mut().map(|p| p.inc_resp_err());
                    }
                }
            }
            info.cal_rrt(param).map(|rrt| {
                info.rrt = rrt;
                self.perf_stats.as_mut().map(|p| p.update_rrt(rrt));
            });
        }
        self.last_is_on_blacklist = info.is_on_blacklist;
        if param.parse_log {
            Ok(L7ParseResult::Single(L7ProtocolInfo::RedisInfo(info)))
        } else {
            Ok(L7ParseResult::None)
        }
    }

    fn protocol(&self) -> L7Protocol {
        L7Protocol::Redis
    }

    fn parsable_on_udp(&self) -> bool {
        false
    }

    fn perf_stats(&mut self) -> Option<L7PerfStats> {
        self.perf_stats.take()
    }

    fn set_obfuscate_cache(&mut self, obfuscate_cache: Option<ObfuscateCache>) {
        self.obfuscate = obfuscate_cache.is_some();
    }
}

impl RedisLog {
    fn reset(&mut self) {
        self.perf_stats = None;
    }

    fn fill_request(&mut self, request: CommandLine, info: &mut RedisInfo) {
        info.request_type = Vec::from(request.command());
        info.msg_type = LogMessageType::Request;
        info.request = request.stringify(self.obfuscate);
        self.has_request = true;
    }

    fn fill_response(&mut self, context: Vec<u8>, info: &mut RedisInfo) {
        info.msg_type = LogMessageType::Response;
        self.has_request = false;

        info.resp_status = L7ResponseStatus::Ok;

        if context.is_empty() {
            return;
        }
        match context[0] {
            b'+' => info.status = context,
            b'-' | b'!' => {
                info.error = context;
                info.resp_status = L7ResponseStatus::ServerError;
            }
            _ => {}
        }
    }

    fn parse(
        &mut self,
        payload: &[u8],
        proto: IpProtocol,
        direction: PacketDirection,
        info: &mut RedisInfo,
    ) -> Result<()> {
        if proto != IpProtocol::TCP {
            return Err(Error::InvalidIpProtocol);
        }
        if payload.is_empty() {
            return Err(Error::L7ProtocolUnknown);
        }

        match direction {
            // only parse the request with payload start with '*' which indicate is a command start, otherwise assume tcp fragment of request
            PacketDirection::ClientToServer if payload.get(0) == Some(&b'*') => {
                self.fill_request(CommandLine::new(payload)?, info)
            }
            PacketDirection::ServerToClient if self.has_request => {
                self.fill_response(stringifier::decode(payload, false)?, info)
            }
            _ => return Err(Error::L7ProtocolUnknown),
        };
        Ok(())
    }
}

mod stringifier {
    use super::*;

    pub const NULL_STR: &'static str = "NULL";

    // decode simple types that does not contain '\r' or '\n' but ends with "\r\n"
    fn decode_simple_type<'a, P>(
        output: Option<&mut Vec<u8>>,
        payload: &'a [u8],
        condition: P,
        limit: usize,
    ) -> Result<&'a [u8]>
    where
        P: Fn(usize, u8) -> bool,
    {
        let payload = &payload[1..];
        // find the first invalid character or '\r'
        let Some(end) = payload
            .iter()
            .enumerate()
            .position(|(i, &b)| !condition(i, b) || b == b'\r')
        else {
            return Err(Error::RedisLogParseFailed);
        };
        if end + 2 > payload.len() || &payload[end..end + 2] != b"\r\n" {
            return Err(Error::RedisLogParseFailed);
        }

        if let Some(output) = output {
            output.extend_from_slice(&payload[..end.min(limit)]);
            if end > 1 + limit {
                output.extend_from_slice(b"...");
            }
        }

        Ok(&payload[end + 2..])
    }

    fn validate_simple_type<P>(payload: &[u8], condition: P) -> Result<&[u8]>
    where
        P: Fn(usize, u8) -> bool,
    {
        decode_simple_type(None, payload, condition, 0)
    }

    // does not include type character
    pub fn read_length(payload: &[u8]) -> Result<(&[u8], isize)> {
        let Some(end) = payload
            .iter()
            .position(|&b| !(b == b'+' || b == b'-' || b.is_ascii_digit()) || b == b'\r')
        else {
            return Err(Error::RedisLogParseFailed);
        };
        if end + 2 > payload.len() || &payload[end..end + 2] != b"\r\n" {
            return Err(Error::RedisLogParseFailed);
        }

        // SAFTY: verified characters in [0, end) are ascii
        let s = unsafe { str::from_utf8_unchecked(&payload[..end]) };
        let Ok(length) = s.parse::<isize>() else {
            return Err(Error::RedisLogParseFailed);
        };

        Ok((&payload[end + 2..], length))
    }

    // decode TLV types
    fn decode_bulk_type<'a>(output: Option<&mut Vec<u8>>, payload: &'a [u8]) -> Result<&'a [u8]> {
        let (payload, length) = read_length(&payload[1..])?;

        // actually only -1 is valid
        if length < 0 {
            if let Some(output) = output {
                output.extend_from_slice(NULL_STR.as_bytes());
            }
            return Ok(payload);
        }

        let end = length as usize;
        if end + 2 > payload.len() || &payload[end..end + 2] != b"\r\n" {
            // for non-strict parse
            if let Some(output) = output {
                output.extend_from_slice(&payload[..end.min(payload.len())]);
            }
            return Err(Error::RedisLogParsePartial);
        }

        if let Some(output) = output {
            output.extend_from_slice(&payload[..end]);
        }

        Ok(&payload[end + 2..])
    }

    // decode TLV types
    fn validate_bulk_type(payload: &[u8]) -> Result<&[u8]> {
        decode_bulk_type(None, payload)
    }

    // decode arrays, sets and pushes
    fn validate_array_type(payload: &[u8]) -> Result<&[u8]> {
        let (mut payload, length) = read_length(&payload[1..])?;

        // actually only -1 is valid
        if length < 0 {
            return Ok(payload);
        }

        for _ in 0..length {
            match decode_resp_type(None, payload) {
                Ok(p) => payload = p,
                _ => return Err(Error::RedisLogParsePartial),
            };
        }

        Ok(payload)
    }

    fn decode_simple_string<'a>(
        mut output: Option<&mut Vec<u8>>,
        payload: &'a [u8],
    ) -> Result<&'a [u8]> {
        assert_eq!(payload[0], b'+');
        if let Some(ref mut output) = output {
            output.push(payload[0]);
        }
        decode_simple_type(output, payload, |_, c| c.is_ascii(), 32)
    }

    fn decode_simple_error<'a>(
        mut output: Option<&mut Vec<u8>>,
        payload: &'a [u8],
    ) -> Result<&'a [u8]> {
        assert_eq!(payload[0], b'-');
        if let Some(ref mut output) = output {
            output.push(payload[0]);
        }
        decode_simple_type(output, payload, |_, c| c.is_ascii(), 256)
    }

    fn validate_integer(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b':');
        validate_simple_type(payload, |i, c| {
            c.is_ascii_digit() || (i == 0 && (c == b'+' || c == b'-'))
        })
    }

    fn validate_bulk_string(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b'$');
        validate_bulk_type(payload)
    }

    fn validate_array(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b'*');
        validate_array_type(payload)
    }

    // _\r\n
    fn validate_null(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b'_');

        if payload.get(1..3) == Some(b"\r\n").map(|arr| arr as &[u8]) {
            Ok(&payload[3..])
        } else {
            Err(Error::RedisLogParseFailed)
        }
    }

    // #<t|f>\r\n
    fn validate_boolean(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b'#');

        match payload.get(1..4) {
            Some(b"t\r\n") | Some(b"f\r\n") => Ok(&payload[4..]),
            _ => Err(Error::RedisLogParseFailed),
        }
    }

    // ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
    // ,inf\r\n
    // ,-inf\r\n
    // ,nan\r\n
    fn validate_double(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b',');
        validate_simple_type(payload, |_, c| c.is_ascii())
    }

    // ([+|-]<number>\r\n
    fn validate_big_number(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b'(');
        validate_simple_type(payload, |i, c| {
            c.is_ascii_digit() || (i == 0 && (c == b'+' || c == b'-'))
        })
    }

    // !<length>\r\n<error>\r\n
    fn decode_bulk_error<'a>(
        mut output: Option<&mut Vec<u8>>,
        payload: &'a [u8],
    ) -> Result<&'a [u8]> {
        assert_eq!(payload[0], b'!');
        if let Some(ref mut output) = output {
            output.push(payload[0]);
        }
        decode_bulk_type(output, payload)
    }

    // =<length>\r\n<encoding>:<data>\r\n
    fn validate_verbatim_string(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b'=');
        validate_bulk_type(payload)
    }

    // %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
    fn validate_map(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b'%');

        let (mut payload, length) = read_length(&payload[1..])?;

        // actually only -1 is valid
        if length < 0 {
            return Ok(payload);
        }

        for _ in 0..length {
            match decode_resp_type(None, payload) {
                Ok(p) => payload = p,
                _ => return Err(Error::RedisLogParsePartial),
            };
            match decode_resp_type(None, payload) {
                Ok(p) => payload = p,
                _ => return Err(Error::RedisLogParsePartial),
            };
        }

        Ok(payload)
    }

    // ~<number-of-elements>\r\n<element-1>...<element-n>
    fn validate_set(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b'~');
        validate_array_type(payload)
    }

    // ><number-of-elements>\r\n<element-1>...<element-n>
    fn validate_push(payload: &[u8]) -> Result<&[u8]> {
        assert_eq!(payload[0], b'>');
        validate_array_type(payload)
    }

    fn decode_resp_type<'a>(output: Option<&mut Vec<u8>>, payload: &'a [u8]) -> Result<&'a [u8]> {
        if payload.is_empty() {
            // happens when compound RESP types are truncated between valid segments
            // for example: parsing b"*3\r\n+bbb\r\n" will call this function with empty payload
            return Err(Error::RedisLogParsePartial);
        }
        // decode '+', '-' and '!' RESP types used in fill_response
        // other types are only validated
        match payload[0] {
            b'+' => decode_simple_string(output, payload),
            b'-' => decode_simple_error(output, payload),
            b':' => validate_integer(payload),
            b'$' => validate_bulk_string(payload),
            b'*' => validate_array(payload),
            b'_' => validate_null(payload),
            b'#' => validate_boolean(payload),
            b',' => validate_double(payload),
            b'(' => validate_big_number(payload),
            b'!' => decode_bulk_error(output, payload),
            b'=' => validate_verbatim_string(payload),
            b'%' => validate_map(payload),
            b'~' => validate_set(payload),
            b'>' => validate_push(payload),
            _ => Err(Error::RedisLogParseFailed),
        }
    }

    pub fn decode(payload: &[u8], strict: bool) -> Result<Vec<u8>> {
        if payload.is_empty() {
            return Err(Error::RedisLogParseFailed);
        }
        let mut output = match payload[0] {
            b'+' | b'-' | b'!' => Vec::with_capacity(payload.len()),
            _ => Vec::new(),
        };
        match (strict, decode_resp_type(Some(&mut output), payload)) {
            (_, Err(Error::RedisLogParseFailed)) | (true, Err(Error::RedisLogParsePartial)) => {
                Err(Error::RedisLogParseFailed)
            }
            _ => Ok(output),
        }
    }
}

struct CommandLine<'a> {
    payload: &'a [u8],
    cmd_upper: String,
    length: usize,
}

impl<'a> CommandLine<'a> {
    const MAX_COMMAND_LENGTH: usize = 17;

    fn new(payload: &'a [u8]) -> Result<Self> {
        if payload.len() < "*0\r\n".len() || payload[0] != b'*' {
            return Err(Error::RedisLogParseFailed);
        }

        let (payload, length) = stringifier::read_length(&payload[1..])?;

        let mut cmd_upper = String::new();
        // read command
        let (mut payload_iter, command) = Self::decode_bulk_string(payload)?;
        if command.len() <= Self::MAX_COMMAND_LENGTH && command.is_ascii() {
            // SAFTY: checked ascii string
            unsafe {
                cmd_upper = str::from_utf8_unchecked(command).to_ascii_uppercase();
            }
        }

        // validate rest of the buffer
        for _ in 1..length {
            match Self::decode_bulk_string(payload_iter) {
                Ok((p, _)) => payload_iter = p,
                _ => return Err(Error::RedisLogParsePartial),
            };
        }

        Ok(Self {
            payload,
            cmd_upper,
            length: length as usize,
        })
    }

    fn decode_bulk_string(payload: &[u8]) -> Result<(&[u8], &[u8])> {
        if payload.len() < "$0\r\n".len() || payload[0] != b'$' {
            return Err(Error::RedisLogParseFailed);
        }

        let (payload, length) = stringifier::read_length(&payload[1..])?;

        // actually only -1 is valid
        if length < 0 {
            return Ok((payload, stringifier::NULL_STR.as_bytes()));
        }

        let end = length as usize;
        if end + 2 > payload.len() || &payload[end..end + 2] != b"\r\n" {
            return Err(Error::RedisLogParseFailed);
        }

        Ok((&payload[end + 2..], &payload[..end]))
    }

    fn iter(&self) -> CommandIterator<'a> {
        CommandIterator {
            payload: self.payload,
            index: 0,
            size: self.length,
        }
    }

    fn command(&self) -> &[u8] {
        // unwrap safe because checked in Self::new()
        Self::decode_bulk_string(self.payload).unwrap().1
    }

    fn stringify(&self, obfuscate: bool) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.payload.len());

        if !obfuscate || self.cmd_upper.is_empty() {
            self.iter().stringify_in(&mut output);
            return output;
        }

        let mut args = self.iter();
        output.extend_from_slice(args.next().unwrap());
        match self.cmd_upper.as_str() {
            "AUTH" => {
                // obfuscate everything
                // - AUTH password
                if args.next().is_some() {
                    output.extend_from_slice(b" ?");
                }
            }
            "HELLO" => {
                // obfuscate everything after 'AUTH' if there is one
                while let Some(arg) = args.next() {
                    output.push(b' ');
                    output.extend_from_slice(arg);
                    if arg.eq_ignore_ascii_case(b"AUTH") {
                        if args.next().is_some() {
                            output.extend_from_slice(b" ?");
                        }
                        break;
                    }
                }
            }
            "APPEND" | "GETSET" | "LPUSHX" | "GEORADIUSBYMEMBER" | "RPUSHX" | "SET" | "SETNX"
            | "SISMEMBER" | "ZRANK" | "ZREVRANK" | "ZSCORE" => {
                // obfuscate 2nd argument
                // - APPEND key value
                // - GETSET key value
                // - LPUSHX key value
                // - GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]
                // - RPUSHX key value
                // - SET key value [expiration EX seconds|PX milliseconds] [NX|XX]
                // - SETNX key value
                // - SISMEMBER key member
                // - ZRANK key member
                // - ZREVRANK key member
                // - ZSCORE key member
                args.obfuscate_nth_in(&mut output, 1);
            }
            "HSETNX" | "LREM" | "LSET" | "SETBIT" | "SETEX" | "PSETEX" | "SETRANGE" | "ZINCRBY"
            | "SMOVE" | "RESTORE" => {
                // obfuscate 3rd argument
                // - HSETNX key field value
                // - LREM key count value
                // - LSET key index value
                // - SETBIT key offset value
                // - SETEX key seconds value
                // - PSETEX key milliseconds value
                // - SETRANGE key offset value
                // - ZINCRBY key increment member
                // - SMOVE source destination member
                // - RESTORE key ttl serialized-value [REPLACE]
                args.obfuscate_nth_in(&mut output, 2);
            }
            "LINSERT" => {
                // obfuscate 4th argument
                // - LINSERT key BEFORE|AFTER pivot value
                args.obfuscate_nth_in(&mut output, 3);
            }
            "GEOHASH" | "GEOPOS" | "GEODIST" | "LPUSH" | "RPUSH" | "SREM" | "ZREM" | "SADD" => {
                // obfuscate everything after the first
                // - GEOHASH key member [member ...]
                // - GEOPOS key member [member ...]
                // - GEODIST key member1 member2 [unit]
                // - LPUSH key value [value ...]
                // - RPUSH key value [value ...]
                // - SREM key member [member ...]
                // - ZREM key member [member ...]
                // - SADD key member [member ...]
                if let Some(arg) = args.next() {
                    output.push(b' ');
                    output.extend_from_slice(arg);
                    if args.next().is_some() {
                        output.extend_from_slice(b" ?");
                    }
                }
            }
            "GEOADD" => {
                // obfuscate every 3rd argument after the first
                // - GEOADD key longitude latitude member [longitude latitude member ...]
                if let Some(arg) = args.next() {
                    output.push(b' ');
                    output.extend_from_slice(arg);
                    args.obfuscate_every_nth_in(&mut output, 3);
                }
            }
            "HSET" | "HMSET" => {
                // obfuscate every 2nd argument after the first
                // - HSET key field value [field value ...]
                // - HMSET key field value [field value ...]
                if let Some(arg) = args.next() {
                    output.push(b' ');
                    output.extend_from_slice(arg);
                    args.obfuscate_every_nth_in(&mut output, 2);
                }
            }
            "MSET" | "MSETNX" => {
                // obfuscate every 2nd argument
                // - MSET key value [key value ...]
                // - MSETNX key value [key value ...]
                args.obfuscate_every_nth_in(&mut output, 2);
            }
            "CONFIG" => {
                // obfuscate every 2nd argument after 'SET'
                // - CONFIG SET parameter value [parameter value ...]
                while let Some(arg) = args.next() {
                    output.push(b' ');
                    output.extend_from_slice(arg);
                    if arg.eq_ignore_ascii_case(b"SET") {
                        args.obfuscate_every_nth_in(&mut output, 2);
                        break;
                    }
                }
            }
            "BITFIELD" => {
                // obfuscate 3rd argument to 'SET'
                // - BITFIELD key [GET encoding offset | [OVERFLOW <WRAP | SAT | FAIL>]
                //       <SET encoding offset value | INCRBY encoding offset increment>
                //       [GET encoding offset | [OVERFLOW <WRAP | SAT | FAIL>]
                //       <SET encoding offset value | INCRBY encoding offset increment>
                //       ...]]
                let mut index_after_set = None;
                while let Some(arg) = args.next() {
                    output.push(b' ');
                    if let Some(i) = index_after_set.as_mut() {
                        *i += 1;
                        if *i == 3 {
                            output.push(b'?');
                            index_after_set = None;
                        } else {
                            output.extend_from_slice(arg);
                        }
                    } else {
                        output.extend_from_slice(arg);
                    }
                    if arg.eq_ignore_ascii_case(b"SET") {
                        index_after_set = Some(0);
                    }
                }
            }
            "ZADD" => {
                // obfuscate every 2nd argument after optional arguments
                // - ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]
                if let Some(arg) = args.next() {
                    // key
                    output.push(b' ');
                    output.extend_from_slice(arg);

                    // optional arguments
                    while let Some(arg) = args.next() {
                        output.push(b' ');
                        output.extend_from_slice(arg);
                        if arg.len() > 4 || !arg.is_ascii() {
                            break;
                        }
                        // SAFTY: checked ascii string
                        let arg_upper =
                            unsafe { str::from_utf8_unchecked(arg).to_ascii_uppercase() };
                        match arg_upper.as_str() {
                            "NX" | "XX" | "GT" | "LT" | "CH" | "INCR" => continue,
                            _ => break,
                        }
                    }
                    // consume next and write '?'
                    if args.next().is_some() {
                        output.extend_from_slice(b" ?");
                    }

                    // rest
                    args.obfuscate_every_nth_in(&mut output, 2);
                }
            }
            _ => {
                args.stringify_in(&mut output);
            }
        }

        output
    }
}

// redis command is defined as 'an array of bulk strings'
struct CommandIterator<'a> {
    payload: &'a [u8],
    index: usize,
    size: usize,
}

impl CommandIterator<'_> {
    fn stringify_in(self, output: &mut Vec<u8>) {
        for s in self {
            if !output.is_empty() && s.len() > 0 {
                output.push(b' ');
            }
            output.extend_from_slice(s);
        }
    }

    fn obfuscate_nth_in(self, output: &mut Vec<u8>, n: usize) {
        for (i, s) in self.enumerate() {
            if !output.is_empty() && s.len() > 0 {
                output.push(b' ');
            }
            if i == n {
                output.push(b'?');
            } else {
                output.extend_from_slice(s);
            }
        }
    }

    fn obfuscate_every_nth_in(self, output: &mut Vec<u8>, n: usize) {
        for (i, s) in self.enumerate() {
            if !output.is_empty() && s.len() > 0 {
                output.push(b' ');
            }
            if (i + 1) % n == 0 {
                output.push(b'?');
            } else {
                output.extend_from_slice(s);
            }
        }
    }
}

impl<'a> Iterator for CommandIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.size {
            return None;
        }

        // unwrap safe because checked in CommandLine::new()
        let (payload, s) = CommandLine::decode_bulk_string(self.payload).unwrap();
        self.payload = payload;
        self.index += 1;

        Some(s)
    }
}

// test log parse
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

    const FILE_DIR: &str = "resources/test/flow_generator/redis";

    fn run(name: &str) -> String {
        let pcap_file = Path::new(FILE_DIR).join(name);
        let log_cache = Rc::new(RefCell::new(L7PerfCache::new(L7_RRT_CACHE_CAPACITY)));
        let capture = Capture::load_pcap(pcap_file, None);
        let mut packets = capture.as_meta_packets();
        if packets.is_empty() {
            return "".to_string();
        }

        let mut output: String = String::new();
        let first_dst_port = packets[0].lookup_key.dst_port;
        let mut redis = RedisLog::default();
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

            let is_redis = match packet.lookup_key.direction {
                PacketDirection::ClientToServer => redis.check_payload(payload, param),
                PacketDirection::ServerToClient => stringifier::decode(payload, false).is_ok(),
            };

            let info = if let Ok(i) = redis.parse_payload(payload, param) {
                match i.unwrap_single() {
                    L7ProtocolInfo::RedisInfo(r) => r,
                    _ => unreachable!(),
                }
            } else {
                RedisInfo::default()
            };

            output.push_str(&format!("{} is_redis: {}\n", info, is_redis));
        }
        output
    }

    #[test]
    fn check() {
        let files = vec![
            ("redis.pcap", "redis.result"),
            ("redis-error.pcap", "redis-error.result"),
            ("redis-debug.pcap", "redis-debug.result"),
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

    #[test]
    fn test_decode() {
        let testcases = vec![
            (("*-1\r\n", true), Some("")),
            (
                ("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n", true),
                Some(""),
            ),
            (("$0\r\n\r\n", true), Some("")),
            (("$-1\r\n", true), Some("")),
            (("$9\r\n12345", false), Some("")),
            (("$9\r\n12345", true), None),
            (("-1\r\n", true), Some("-1")),
            // _\r\n
            (("_\r\n", true), Some("")),
            (("_\r", true), None),
            // #<t|f>\r\n
            (("#t\r\n", true), Some("")),
            (("#t\r", true), None),
            // ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
            // ,inf\r\n
            // ,-inf\r\n
            // ,nan\r\n
            ((",1.12\r\n", true), Some("")),
            // ([+|-]<number>\r\n
            (("(1112111211121112\r\n", true), Some("")),
            // !<length>\r\n<error>\r\n
            (("!9\r\nabcdefghi\r\n", true), Some("!abcdefghi")),
            // =<length>\r\n<encoding>:<data>\r\n
            (("=9\r\ntxt:abcde\r\n", true), Some("")),
            // %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
            (("%1\r\n+key\r\n:123\r\n", true), Some("")),
            // ~<number-of-elements>\r\n<element-1>...<element-n>
            // ><number-of-elements>\r\n<element-1>...<element-n>
            (("~2\r\n+key\r\n:123\r\n", true), Some("")),
        ];
        for (input, expected) in testcases.iter() {
            let output = stringifier::decode(&input.0.as_bytes(), input.1);
            assert_eq!(
                output.ok().as_ref().and_then(|vs| str::from_utf8(vs).ok()),
                *expected,
                "testcase input '{}' failed",
                str::from_utf8(input.0.as_bytes()).unwrap().escape_default()
            );
        }
    }

    #[test]
    fn truncated_compound_type() {
        assert!(stringifier::decode(b"%1\r\n+key\r\n", false).is_ok());
        assert!(stringifier::decode(b"%1\r\n+key\r\n", true).is_err());
        let s = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
        for i in 0..(s.len() - 1) {
            assert!(stringifier::decode(&s.as_bytes()[..i], true).is_err());
        }
    }

    #[test]
    fn check_perf() {
        let expected = vec![
            (
                "redis.pcap",
                L7PerfStats {
                    request_count: 10,
                    response_count: 10,
                    err_client_count: 0,
                    err_server_count: 0,
                    err_timeout: 0,
                    rrt_count: 10,
                    rrt_sum: 592,
                    rrt_max: 96,
                    ..Default::default()
                },
            ),
            (
                "redis-error.pcap",
                L7PerfStats {
                    request_count: 1,
                    response_count: 1,
                    err_client_count: 0,
                    err_server_count: 1,
                    err_timeout: 0,
                    rrt_count: 1,
                    rrt_sum: 73,
                    rrt_max: 73,
                    ..Default::default()
                },
            ),
            (
                "redis-debug.pcap",
                L7PerfStats {
                    request_count: 1,
                    response_count: 1,
                    err_client_count: 0,
                    err_server_count: 0,
                    err_timeout: 0,
                    rrt_count: 1,
                    rrt_sum: 1209,
                    rrt_max: 1209,
                    ..Default::default()
                },
            ),
        ];

        for item in expected.iter() {
            assert_eq!(item.1, run_perf(item.0), "parse pcap {} unexcepted", item.0);
        }
    }

    fn run_perf(pcap: &str) -> L7PerfStats {
        let rrt_cache = Rc::new(RefCell::new(L7PerfCache::new(100)));
        let mut redis = RedisLog::default();

        let capture = Capture::load_pcap(Path::new(FILE_DIR).join(pcap), None);
        let mut packets = capture.as_meta_packets();
        if packets.len() < 2 {
            unreachable!();
        }

        let first_dst_port = packets[0].lookup_key.dst_port;
        for packet in packets.iter_mut() {
            if packet.lookup_key.dst_port == first_dst_port {
                packet.lookup_key.direction = PacketDirection::ClientToServer;
            } else {
                packet.lookup_key.direction = PacketDirection::ServerToClient;
            }
            if packet.get_l4_payload().is_some() {
                let _ = redis.parse_payload(
                    packet.get_l4_payload().unwrap(),
                    &ParseParam::new(
                        &*packet,
                        rrt_cache.clone(),
                        Default::default(),
                        #[cfg(any(target_os = "linux", target_os = "android"))]
                        Default::default(),
                        true,
                        true,
                    ),
                );
            }
        }
        redis.perf_stats.unwrap()
    }

    fn encode_redis_command(command: &str) -> Vec<u8> {
        let n = command.split(" ").count();
        let mut output = Vec::from(format!("*{}\r\n", n));

        for arg in command.split(" ") {
            output.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }

        output
    }

    #[test]
    fn check_obfuscation() {
        let testcases = [
                ("GET key ", "GET key"),
                ("AUTH", "AUTH"),
                ("AUTH my-secret-password", "AUTH ?"),
                ("AUTH james my-secret-password", "AUTH ?"),
                ("HELLO 3 AUTH username passwd SETNAME cliname", "HELLO 3 AUTH ?"),
                ("APPEND key value", "APPEND key ?"),
                ("GETSET key value", "GETSET key ?"),
                ("LPUSHX key value", "LPUSHX key ?"),
                ("GEORADIUSBYMEMBER Sicily Agrigento 100 km", "GEORADIUSBYMEMBER Sicily ? 100 km"),
                ("RPUSHX key value", "RPUSHX key ?"),
                ("SET key value", "SET key ?"),
                ("SET anotherkey value EX 60", "SET anotherkey ? EX 60"),
                ("SETNX key value", "SETNX key ?"),
                ("SISMEMBER key member", "SISMEMBER key ?"),
                ("ZRANK key member", "ZRANK key ?"),
                ("ZREVRANK key member", "ZREVRANK key ?"),
                ("ZSCORE key member", "ZSCORE key ?"),
                ("BITFIELD key GET type offset SET type offset value INCRBY type", "BITFIELD key GET type offset SET type offset ? INCRBY type"),
                ("BITFIELD key SET type offset value INCRBY type", "BITFIELD key SET type offset ? INCRBY type"),
                ("BITFIELD key GET type offset INCRBY type", "BITFIELD key GET type offset INCRBY type"),
                ("BITFIELD key SET type offset", "BITFIELD key SET type offset"),
                ("CONFIG SET parameter value", "CONFIG SET parameter ?"),
                ("CONFIG foo bar baz", "CONFIG foo bar baz"),
                ("GEOADD key longitude latitude member longitude latitude member longitude latitude member", "GEOADD key longitude latitude ? longitude latitude ? longitude latitude ?"),
                ("GEOADD key longitude latitude member longitude latitude member", "GEOADD key longitude latitude ? longitude latitude ?"),
                ("GEOADD key longitude latitude member", "GEOADD key longitude latitude ?"),
                ("GEOADD key longitude latitude", "GEOADD key longitude latitude"),
                ("GEOADD key", "GEOADD key"),
                ("GEOHASH key", "GEOHASH key"),
                ("GEOPOS key", "GEOPOS key"),
                ("GEODIST key", "GEODIST key"),
                ("GEOHASH key member", "GEOHASH key ?"),
                ("GEOPOS key member", "GEOPOS key ?"),
                ("GEODIST key member", "GEODIST key ?"),
                ("GEOHASH key member member member", "GEOHASH key ?"),
                ("GEOPOS key member member", "GEOPOS key ?"),
                ("GEODIST key member member member", "GEODIST key ?"),
                ("SREM key member1 member2 member3", "SREM key ?"),
                ("ZREM key member1 member2 member3", "ZREM key ?"),
                ("SADD key member1 member2 member3", "SADD key ?"),
                ("GEODIST key member1 member2 m", "GEODIST key ?"),
                ("LPUSH key value1 value2 value3", "LPUSH key ?"),
                ("RPUSH key value1 value2 value3", "RPUSH key ?"),
                ("HSET key field value", "HSET key field ?"),
                ("HSETNX key field value", "HSETNX key field ?"),
                ("HSET key field value field1 value1 field2 value2", "HSET key field ? field1 ? field2 ?"),
                ("HSETNX key field value", "HSETNX key field ?"),
                ("LREM key count value", "LREM key count ?"),
                ("LSET key index value", "LSET key index ?"),
                ("SETBIT key offset value", "SETBIT key offset ?"),
                ("SETRANGE key offset value", "SETRANGE key offset ?"),
                ("SETEX key seconds value", "SETEX key seconds ?"),
                ("PSETEX key milliseconds value", "PSETEX key milliseconds ?"),
                ("ZINCRBY key increment member", "ZINCRBY key increment ?"),
                ("SMOVE source destination member", "SMOVE source destination ?"),
                ("RESTORE key ttl serialized-value [REPLACE]", "RESTORE key ttl ? [REPLACE]"),
                ("LINSERT key BEFORE pivot value", "LINSERT key BEFORE pivot ?"),
                ("LINSERT key AFTER pivot value", "LINSERT key AFTER pivot ?"),
                ("HMSET key field value field value", "HMSET key field ? field ?"),
                ("HMSET key field value", "HMSET key field ?"),
                ("HMSET key field", "HMSET key field"),
                ("MSET key value key value", "MSET key ? key ?"),
                ("MSET", "MSET"),
                ("MSET key value", "MSET key ?"),
                ("MSETNX key value key value", "MSETNX key ? key ?"),
                ("ZADD key score member score member", "ZADD key score ? score ?"),
                ("ZADD key NX score member score member", "ZADD key NX score ? score ?"),
                ("ZADD key NX CH score member score member", "ZADD key NX CH score ? score ?"),
                ("ZADD key NX CH INCR score member score member", "ZADD key NX CH INCR score ? score ?"),
                ("ZADD key XX INCR score member score member", "ZADD key XX INCR score ? score ?"),
                ("ZADD key XX INCR score member", "ZADD key XX INCR score ?"),
                ("ZADD key XX INCR score", "ZADD key XX INCR score"),
                ("CONFIG command SET k v", "CONFIG command SET k ?"),
                ("SET *😊®© ❤️", "SET *😊®© ?"),
                ("SET😊 ❤️*😊®© ❤️", "SET😊 ❤️*😊®© ❤️"),
                ("ZADD key 😊 member score 😊", "ZADD key 😊 ? score ?"),
            ];
        for (input, expected) in testcases.iter() {
            let redis_str = encode_redis_command(input);
            let cmdline = CommandLine::new(&redis_str).unwrap();
            let output = cmdline.stringify(true);
            assert_eq!(
                str::from_utf8(output.as_slice()).unwrap(),
                *expected,
                "testcase {} failed",
                input
            );
        }
    }
}
