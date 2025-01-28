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

use std::sync::atomic::Ordering;

use super::flow::PacketDirection;
use enum_dispatch::enum_dispatch;
use log::{debug, error, warn};
use serde::Serialize;

use crate::{
    common::l7_protocol_log::LogCache,
    flow_generator::{
        protocol_logs::{
            fastcgi::FastCGIInfo, pb_adapter::L7ProtocolSendLog, AmqpInfo, BrpcInfo, DnsInfo,
            DubboInfo, HttpInfo, KafkaInfo, MemcachedInfo, MongoDBInfo, MqttInfo, MysqlInfo,
            NatsInfo, OpenWireInfo, PingInfo, PostgreInfo, PulsarInfo, RedisInfo, RocketmqInfo,
            SofaRpcInfo, TarsInfo, ZmtpInfo,
        },
        AppProtoHead, LogMessageType, Result,
    },
    plugin::CustomInfo,
};

use super::{ebpf::EbpfType, l7_protocol_log::ParseParam};

macro_rules! all_protocol_info {
    ($($name:ident($info_struct:ty)),+$(,)?) => {

        #[derive(Serialize, Debug, Clone)]
        #[enum_dispatch]
        #[serde(untagged)]
        pub enum L7ProtocolInfo {
            $(
                $name($info_struct),
            )+
        }

        impl From<L7ProtocolInfo> for L7ProtocolSendLog{
            fn from(f:L7ProtocolInfo)->L7ProtocolSendLog{
                match f{
                    $(
                        L7ProtocolInfo::$name(info)=>info.into(),
                    )+
                }
            }
        }
    };
}

cfg_if::cfg_if! {
    if #[cfg(not(feature = "enterprise"))] {
        all_protocol_info!(
            DnsInfo(DnsInfo),
            HttpInfo(HttpInfo),
            MysqlInfo(MysqlInfo),
            RedisInfo(RedisInfo),
            MongoDBInfo(MongoDBInfo),
            MemcachedInfo(MemcachedInfo),
            DubboInfo(DubboInfo),
            FastCGIInfo(FastCGIInfo),
            BrpcInfo(BrpcInfo),
            TarsInfo(TarsInfo),
            KafkaInfo(KafkaInfo),
            MqttInfo(MqttInfo),
            AmqpInfo(AmqpInfo),
            NatsInfo(NatsInfo),
            PulsarInfo(PulsarInfo),
            ZmtpInfo(ZmtpInfo),
            RocketmqInfo(RocketmqInfo),
            PostgreInfo(PostgreInfo),
            OpenWireInfo(OpenWireInfo),
            SofaRpcInfo(SofaRpcInfo),
            PingInfo(PingInfo),
            CustomInfo(CustomInfo),
            // add new protocol info below
        );
    } else {
        all_protocol_info!(
            DnsInfo(DnsInfo),
            HttpInfo(HttpInfo),
            MysqlInfo(MysqlInfo),
            RedisInfo(RedisInfo),
            MongoDBInfo(MongoDBInfo),
            MemcachedInfo(MemcachedInfo),
            DubboInfo(DubboInfo),
            FastCGIInfo(FastCGIInfo),
            BrpcInfo(BrpcInfo),
            TarsInfo(TarsInfo),
            KafkaInfo(KafkaInfo),
            MqttInfo(MqttInfo),
            AmqpInfo(AmqpInfo),
            NatsInfo(NatsInfo),
            PulsarInfo(PulsarInfo),
            ZmtpInfo(ZmtpInfo),
            RocketmqInfo(RocketmqInfo),
            PostgreInfo(PostgreInfo),
            OpenWireInfo(OpenWireInfo),
            OracleInfo(crate::flow_generator::protocol_logs::sql::OracleInfo),
            SofaRpcInfo(SofaRpcInfo),
            TlsInfo(crate::flow_generator::protocol_logs::tls::TlsInfo),
            SomeIpInfo(crate::flow_generator::protocol_logs::rpc::SomeIpInfo),
            PingInfo(PingInfo),
            CustomInfo(CustomInfo),
            // add new protocol info below
        );
    }
}

#[enum_dispatch(L7ProtocolInfo)]
pub trait L7ProtocolInfoInterface: Into<L7ProtocolSendLog> {
    // 个别协议一个连接可能有子流，这里需要返回流标识，例如http2的stream id
    // ============================================================
    // Returns the stream ID, distinguishing substreams. such as http2 stream id, dns transaction id
    fn session_id(&self) -> Option<u32>;
    // 协议字段合并
    // 返回的错误暂时无视
    // =============================================================
    // merge request and response. now return err will have no effect.
    fn merge_log(&mut self, other: &mut L7ProtocolInfo) -> Result<()>;

    fn app_proto_head(&self) -> Option<AppProtoHead>;

    fn is_tls(&self) -> bool;

    fn get_endpoint(&self) -> Option<String> {
        None
    }

    fn get_biz_type(&self) -> u8 {
        0
    }

    fn skip_send(&self) -> bool {
        false
    }

    // 是否需要进一步合并，目前只有在ebpf有意义，内置协议也只有 EBPF_TYPE_GO_HTTP2_UPROBE 会用到.
    // 除非确实需要多次log合并，否则应该一律返回false
    // =================================================================================
    // should need merge more than once? only ebpf will need merge many times.
    // should always return false when non ebpf.
    fn need_merge(&self) -> bool {
        false
    }
    // 对于需要多次merge的情况下，判断流是否已经结束，只有在need_merge->true的情况下有用
    // 返回 req_end,resp_end
    // ========================================================================
    // when need merge more than once, use to determine if the stream has ended.
    fn is_req_resp_end(&self) -> (bool, bool) {
        (false, false)
    }

    fn cal_cache_key(&self, param: &ParseParam) -> u128 {
        /*
            if session id is some: flow id 64bit | 0 32bit | session id 32bit
            if session id is none: flow id 64bit | packet_seq 64bit
        */
        match self.session_id() {
            Some(sid) => ((param.flow_id as u128) << 64) | sid as u128,
            None => {
                ((param.flow_id as u128) << 64)
                    | (if param.ebpf_type != EbpfType::None {
                        // NOTE:
                        //   In the request-log session aggregation process, for eBPF data, we require that requests and
                        // responses have consecutive cap_seq to ensure the correctness of session aggregation. However,
                        // when SR (Segmentation-Reassembly) is enabled, we combine multiple eBPF socket event events
                        // before parsing the protocol. Therefore, in order to ensure that session aggregation can still
                        // be performed correctly, we need to retain the cap_seq of the last request and the cap_seq of
                        // the first response, so that the cap_seq of the request and response can still be consecutive.
                        if param.direction == PacketDirection::ClientToServer {
                            param.packet_end_seq + 1
                        } else {
                            param.packet_start_seq
                        }
                    } else {
                        0
                    }) as u128
            }
        }
    }

    /*
        calculate rrt
        if have previous log cache:
            if previous is req and current is resp and current time > previous time
                rrt = current time - previous time
            if previous is resp and current is req and current time < previous time, likely ebfp disorder
                rrt =  previous time - current time

            otherwise can not calculate rrt, cache current log rrt

        if have no previous log cache, cache the current log rrt
    */
    fn cal_rrt(&self, param: &ParseParam) -> Option<u64> {
        let mut perf_cache = param.l7_perf_cache.borrow_mut();
        let cache_key = self.cal_cache_key(param);
        let previous_log_info = perf_cache.rrt_cache.pop(&cache_key);

        let time = param.time;
        let msg_type: LogMessageType = param.direction.into();
        let timeout = param.rrt_timeout as u64;

        if time != 0 {
            let (in_cached_req, timeout_count) = perf_cache
                .timeout_cache
                .get_or_insert_mut(param.flow_id, || (0, 0));

            let Some(previous_log_info) = previous_log_info else {
                if msg_type == LogMessageType::Request {
                    *in_cached_req += 1;
                }
                perf_cache.put(
                    cache_key,
                    LogCache {
                        msg_type: param.direction.into(),
                        time: param.time,
                        multi_merge_info: None,
                    },
                );

                param.stats_counter.as_ref().map(|f| {
                    f.l7_perf_cache_len
                        .swap(perf_cache.rrt_cache.len() as u64, Ordering::Relaxed);
                    f.l7_timeout_cache_len
                        .swap(perf_cache.timeout_cache.len() as u64, Ordering::Relaxed)
                });
                return None;
            };

            if previous_log_info.msg_type == LogMessageType::Request {
                if *in_cached_req > 0 {
                    *in_cached_req -= 1;
                }
            }

            // if previous is req and current is resp, calculate the round trip time.
            if previous_log_info.msg_type == LogMessageType::Request
                && msg_type == LogMessageType::Response
                && time > previous_log_info.time
            {
                let rrt = time - previous_log_info.time;
                // timeout, save the latest
                if rrt > timeout {
                    *timeout_count += 1;
                    perf_cache.rrt_cache.put(
                        cache_key,
                        LogCache {
                            msg_type: param.direction.into(),
                            time: param.time,
                            multi_merge_info: None,
                        },
                    );
                    None
                } else {
                    Some(rrt)
                }

            // if previous is resp and current is req and previous time gt current time, likely ebpf disorder,
            // calculate the round trip time.
            } else if previous_log_info.msg_type == LogMessageType::Response
                && msg_type == LogMessageType::Request
                && previous_log_info.time > time
            {
                let rrt = previous_log_info.time - time;
                if rrt > timeout {
                    // disorder info rrt unlikely have large rrt gt timeout
                    warn!("l7 log info disorder with long time rrt {}", rrt);
                    // timeout, save latest
                    *timeout_count += 1;
                    perf_cache.rrt_cache.put(cache_key, previous_log_info);
                    None
                } else {
                    Some(rrt)
                }
            } else {
                debug!(
                    "can not calculate rrt, flow_id: {}, previous log type:{:?}, previous time: {}, current log type: {:?}, current time: {}",
                    param.flow_id, previous_log_info.msg_type, previous_log_info.time, msg_type, param.time,
                );

                // save the latest
                if previous_log_info.time > time {
                    if msg_type == LogMessageType::Request {
                        *timeout_count += 1;
                    }
                    if previous_log_info.msg_type == LogMessageType::Request {
                        *in_cached_req += 1;
                    }
                    perf_cache.rrt_cache.put(cache_key, previous_log_info);
                } else {
                    if previous_log_info.msg_type == LogMessageType::Request {
                        *timeout_count += 1;
                    }
                    if msg_type == LogMessageType::Request {
                        *in_cached_req += 1;
                    }
                    perf_cache.rrt_cache.put(
                        cache_key,
                        LogCache {
                            msg_type: param.direction.into(),
                            time: param.time,
                            multi_merge_info: None,
                        },
                    );
                }

                param.stats_counter.as_ref().map(|f| {
                    f.l7_perf_cache_len
                        .swap(perf_cache.rrt_cache.len() as u64, Ordering::Relaxed);
                    f.l7_timeout_cache_len
                        .swap(perf_cache.timeout_cache.len() as u64, Ordering::Relaxed);
                });
                None
            }
        } else {
            error!("flow_id: {}, packet time 0", param.flow_id);
            None
        }
    }

    // must have request id
    // the main different with cal_rrt() is need to push back to lru when session not end
    fn cal_rrt_for_multi_merge_log(&self, param: &ParseParam) -> Option<u64> {
        assert!(self.session_id().is_some());

        let mut perf_cache = param.l7_perf_cache.borrow_mut();
        let cache_key = self.cal_cache_key(param);
        let previous_log_info = perf_cache.rrt_cache.pop(&cache_key);

        let time = param.time;
        let msg_type: LogMessageType = param.direction.into();
        let timeout = param.rrt_timeout as u64;

        if time == 0 {
            error!("flow_id: {}, packet time 0", param.flow_id);
            return None;
        }

        let (in_cached_req, timeout_count) = perf_cache
            .timeout_cache
            .get_or_insert_mut(param.flow_id, || (0, 0));

        let (req_end, resp_end) = {
            let (req_end, resp_end) = self.is_req_resp_end();
            match param.direction {
                PacketDirection::ClientToServer => (req_end, false),
                PacketDirection::ServerToClient => (false, resp_end),
            }
        };

        let Some(mut previous_log_info) = previous_log_info else {
            if msg_type == LogMessageType::Request {
                *in_cached_req += 1;
            }
            perf_cache.put(
                cache_key,
                LogCache {
                    msg_type: param.direction.into(),
                    time: param.time,
                    multi_merge_info: Some((req_end, resp_end, false)),
                },
            );

            param.stats_counter.as_ref().map(|f| {
                f.l7_perf_cache_len
                    .swap(perf_cache.rrt_cache.len() as u64, Ordering::Relaxed);
                f.l7_timeout_cache_len
                    .swap(perf_cache.timeout_cache.len() as u64, Ordering::Relaxed)
            });
            return None;
        };

        let Some((cache_req_end, cache_resp_end, merged)) =
            previous_log_info.multi_merge_info.as_mut()
        else {
            error!(
                "{:?}:{} -> {:?}:{} flow_id: {} ebpf_type: {:?} rrt cal fail, multi_merge_info is none",
                param.ip_src,
                param.port_src,
                param.ip_dst,
                param.port_dst,
                param.flow_id,
                param.ebpf_type
            );
            return None;
        };

        if req_end {
            *cache_req_end = true;
        }
        if resp_end {
            *cache_resp_end = true;
        }

        let (put_back, merged) = (!(*cache_req_end && *cache_resp_end), *merged);

        if previous_log_info.msg_type != msg_type && !merged {
            previous_log_info.multi_merge_info.as_mut().unwrap().2 = true;
            if *in_cached_req > 0 {
                *in_cached_req -= 1;
            }
        }

        // if previous is req and current is resp, calculate the round trip time.
        if (previous_log_info.msg_type == LogMessageType::Request
            && msg_type == LogMessageType::Response
            && time > previous_log_info.time) ||
        // if previous is resp and current is req and previous time gt current time, likely ebpf disorder,
        // calculate the round trip time.
            (previous_log_info.msg_type == LogMessageType::Response
            && msg_type == LogMessageType::Request
            && previous_log_info.time > time)
        {
            let rrt = time.abs_diff(previous_log_info.time);

            // timeout
            let r = if rrt > timeout {
                if !merged {
                    *timeout_count += 1;
                }
                None
            } else {
                Some(rrt)
            };

            if put_back {
                perf_cache.rrt_cache.put(cache_key, previous_log_info);
            }
            r
        } else {
            if previous_log_info.msg_type != msg_type && !merged {
                *timeout_count += 1;
            }
            if put_back {
                perf_cache.rrt_cache.put(cache_key, previous_log_info);
            }
            None
        }
    }

    fn tcp_seq_offset(&self) -> u32 {
        return 0;
    }

    fn get_request_domain(&self) -> String {
        String::default()
    }

    fn get_request_resource_length(&self) -> usize {
        0
    }

    fn is_on_blacklist(&self) -> bool {
        false
    }
}

impl L7ProtocolInfo {
    pub fn is_session_end(&self) -> bool {
        let (req_end, resp_end) = self.is_req_resp_end();
        req_end && resp_end
    }
}
