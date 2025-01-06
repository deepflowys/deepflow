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

use std::fmt::{Debug, Formatter, Result as DebugResult};
use std::io;
use std::mem;
use std::net::Shutdown;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::process;

use libc::{
    c_int, c_uint, c_void, getsockopt, mmap, munmap, off_t, packet_mreq, poll, pollfd, setsockopt,
    size_t, sockaddr, sockaddr_ll, socket, socklen_t, write, AF_PACKET, ETH_P_ALL, MAP_LOCKED,
    MAP_NORESERVE, MAP_SHARED, PACKET_ADD_MEMBERSHIP, PACKET_MR_PROMISC, POLLERR, POLLIN,
    PROT_READ, PROT_WRITE, SOL_PACKET, SOL_SOCKET, SO_ATTACH_FILTER,
};
use log::{debug, info, warn};
use public::error::*;
use public::packet::Packet;
use socket2::Socket;

use super::{bpf, header, options};

use crate::utils::environment::is_kernel_available;
use crate::utils::stats;
use public::utils::net::{self, link_by_name};

const PACKET_VERSION: c_int = 10;
const PACKET_RX_RING: c_int = 5;
const PACKET_FANOUT: c_int = 18;
const PACKET_STATISTICS: c_int = 6;
const MILLI_SECONDS: u32 = 1000000;
const MIN_KERNEL_VERSION_SUPPORT_PACKET_FANOUT: &'static str = "3.1";

// https://www.ietf.org/archive/id/draft-gharris-opsawg-pcap-01.html
const LINKTYPE_ETHERNET: c_int = 1;

#[derive(Debug, Clone)]
#[repr(C)]
pub struct Stats {
    pub packets: i64,
    pub polls: i64,
}

#[derive(Clone, Debug, Copy)]
#[repr(C)]
pub struct TpacketStats {
    pub tp_packets: c_uint,
    pub tp_drops: c_uint,
}

#[derive(Clone, Debug)]
#[repr(C)]
pub struct TpacketStatsV3 {
    pub tp_packets: c_uint,
    pub tp_drops: c_uint,
    pub tp_freeze_q_cnt: c_uint,
}

pub struct Tpacket {
    _stats: Stats,

    raw_socket: Socket,
    ring: *mut u8,
    opts: options::Options,

    offset: u32,
    current: Option<Box<dyn header::Header>>,

    should_release_packet: bool,
    header_next_needed: bool,

    tp_version: options::OptTpacketVersion,

    v3: Option<*mut header::V3Wrapper>,
}

impl Debug for Tpacket {
    fn fmt(&self, f: &mut Formatter<'_>) -> DebugResult {
        f.write_fmt(format_args!(
            "Tpacket {{ socket: {:?}, ring: {:?}, opts: {:?}, offset: {:?}, tp_version: {:?} }}",
            self.raw_socket, self.ring, self.opts, self.offset, self.tp_version
        ))
    }
}

// it's safe because ring points to mmap'ed buffer
unsafe impl Send for Tpacket {}

impl Tpacket {
    fn bind(&self) -> af_packet::Result<()> {
        let mut if_index: i32 = 0;

        if self.opts.iface != "" {
            // 根据网卡名称获取网卡if_index
            let link = link_by_name(self.opts.iface.clone()).map_err(|e| match e {
                net::Error::LinkNotFound(s) => af_packet::Error::LinkError(s),
                net::Error::NetlinkError(ne) => af_packet::Error::LinkError(ne.to_string()),
                _ => unreachable!(),
            })?;
            if_index = link.if_index as i32;
        }
        unsafe {
            let mut sa: sockaddr_ll = std::mem::zeroed();
            sa.sll_family = AF_PACKET as u16;
            sa.sll_protocol = (ETH_P_ALL as u16).to_be();
            sa.sll_ifindex = if_index as i32;

            let res = libc::bind(
                self.raw_socket.as_raw_fd(),
                &sa as *const sockaddr_ll as *const sockaddr,
                std::mem::size_of::<sockaddr_ll>() as u32,
            );
            if res == -1 {
                return Err(io::Error::last_os_error().into());
            }
        }
        Ok(())
    }

    pub fn set_promisc(&self, if_index: &Vec<i32>) -> Result<()> {
        for i in if_index {
            let mreq = packet_mreq {
                mr_type: PACKET_MR_PROMISC as u16,
                mr_ifindex: *i,
                mr_alen: 0,
                mr_address: [0, 0, 0, 0, 0, 0, 0, 0],
            };
            let err = self.setsockopt(SOL_PACKET, PACKET_ADD_MEMBERSHIP, mreq);
            if err.is_err() {
                debug!("Ifindex {} set promisc error: {:?}", i, err);
            }
        }
        Ok(())
    }

    fn setsockopt<T>(&self, level: i32, name: i32, value: T) -> af_packet::Result<()> {
        unsafe {
            let value = &value as *const T as *const c_void;
            if setsockopt(
                self.raw_socket.as_raw_fd(),
                level,
                name,
                value,
                mem::size_of::<T>() as socklen_t,
            ) == -1
            {
                return Err(io::Error::last_os_error().into());
            }

            Ok(())
        }
    }

    fn set_version_internal(&mut self, tp_version: options::OptTpacketVersion) -> bool {
        // 设置af packet版本
        self.setsockopt(SOL_PACKET, PACKET_VERSION, tp_version as c_int)
            .is_ok()
    }

    fn set_version(&mut self) -> Result<()> {
        if (self.tp_version == options::OptTpacketVersion::TpacketVersionHighestavailablet
            || self.tp_version == options::OptTpacketVersion::TpacketVersion3)
            && self.set_version_internal(options::OptTpacketVersion::TpacketVersion3)
        {
            self.tp_version = options::OptTpacketVersion::TpacketVersion3;
            Ok(())
        } else if (self.tp_version == options::OptTpacketVersion::TpacketVersionHighestavailablet
            || self.tp_version == options::OptTpacketVersion::TpacketVersion2)
            && self.set_version_internal(options::OptTpacketVersion::TpacketVersion2)
        {
            self.tp_version = options::OptTpacketVersion::TpacketVersion2;
            Ok(())
        } else {
            Err(public::error::Error::AfPacketError(
                af_packet::Error::InvalidTpVersion(self.tp_version as isize),
            ))
        }
    }

    fn set_ring(&self) -> af_packet::Result<()> {
        if self.tp_version == options::OptTpacketVersion::TpacketVersion2 {
            let mut req: header::TpacketReq = Default::default();
            req.tp_block_nr = self.opts.num_blocks;
            req.tp_block_size = self.opts.block_size;
            req.tp_frame_nr = self.opts.frames_per_block() * self.opts.num_blocks;
            req.tp_frame_size = self.opts.frame_size;
            self.setsockopt(SOL_PACKET, PACKET_RX_RING, req)?;
            Ok(())
        } else if self.tp_version == options::OptTpacketVersion::TpacketVersion3 {
            let mut req: header::TpacketReq3 = Default::default();
            req.tp_block_size = self.opts.block_size;
            req.tp_block_nr = self.opts.num_blocks;
            req.tp_frame_size = self.opts.frame_size;
            req.tp_frame_nr = self.opts.frames_per_block() * self.opts.num_blocks;
            req.tp_retire_blk_tov = self.opts.block_timeout / MILLI_SECONDS;
            self.setsockopt(SOL_PACKET, PACKET_RX_RING, req)?;
            Ok(())
        } else {
            Err(af_packet::Error::InvalidTpVersion(self.tp_version as isize))
        }
    }

    fn set_fanout(&self) -> af_packet::Result<()> {
        // refer to https://man7.org/linux/man-pages/man7/packet.7.html
        if !is_kernel_available(MIN_KERNEL_VERSION_SUPPORT_PACKET_FANOUT) {
            info!("kernel version is lower than 3.1, skip the packet fanout setting");
            return Ok(());
        }
        let Some(packet_fanout_mode) = self.opts.packet_fanout_mode else {
            info!("Packet fanout disabled.");
            return Ok(());
        };
        // The first 16 bits encode the fanout group ID, and the second set of 16 bits encode the fanout mode and options.
        let fanout_group_id = process::id() & 0xffff;
        let fanout_arg: c_uint = fanout_group_id | (packet_fanout_mode << 16);
        self.setsockopt(SOL_PACKET, PACKET_FANOUT, fanout_arg)
    }

    fn mmap_ring(&mut self) -> af_packet::Result<()> {
        // 接收队列
        unsafe {
            let mut ret = mmap(
                std::ptr::null_mut(),
                (self.opts.block_size * self.opts.num_blocks) as size_t,
                (PROT_READ | PROT_WRITE) as c_int,
                (MAP_SHARED | MAP_LOCKED | MAP_NORESERVE) as c_int,
                self.raw_socket.as_raw_fd(),
                0 as off_t,
            ) as isize;
            if ret == -1 {
                warn!("Afpacket mmap error: {:?}, maybe env lack permission, retry without MAP_LOCKED and MAP_NORESERVE.", io::Error::last_os_error());
                ret = mmap(
                    std::ptr::null_mut(),
                    (self.opts.block_size * self.opts.num_blocks) as size_t,
                    (PROT_READ | PROT_WRITE) as c_int,
                    MAP_SHARED as c_int,
                    self.raw_socket.as_raw_fd(),
                    0 as off_t,
                ) as isize;
                if ret == -1 {
                    return Err(io::Error::last_os_error().into());
                }
            }
            self.ring = ret as *mut u8;
        }
        // bind rx ring
        Ok(())
    }

    fn get_packet_header(&mut self) -> Box<dyn header::Header> {
        match self.tp_version {
            options::OptTpacketVersion::TpacketVersion2 => {
                // AF_PACKET 2
                if self.offset >= self.opts.frames_per_block() * self.opts.num_blocks {
                    self.offset = 0
                }

                let position: *mut u8 =
                    (self.ring as usize + (self.opts.frame_size * self.offset) as usize) as *mut u8;
                return Box::from(header::Tpacket2Hdr::from(position));
            }
            options::OptTpacketVersion::TpacketVersion3 => {
                // AF_PACKET 3
                if self.offset >= self.opts.num_blocks {
                    self.offset = 0;
                }
                let position: *mut u8 = (self.ring as usize
                    + (self.opts.frame_size * self.offset * self.opts.frames_per_block()) as usize)
                    as *mut u8;
                return Box::from(header::V3Wrapper::from(position));
            }
            _ => {
                panic!("Unknown af_packet version {:?}.", &self.tp_version);
            }
        }
    }

    fn poll_for_first_packet(&mut self) -> bool {
        let timeout = self.opts.poll_timeout / MILLI_SECONDS as isize;
        if let Some(header) = self.current.as_ref() {
            while (header.get_status() & header::TP_STATUS_USER) == 0 {
                let mut poll_fd = pollfd {
                    fd: self.raw_socket.as_raw_fd(),
                    events: POLLIN | POLLERR,
                    revents: 0,
                };

                let n = unsafe { poll(&mut poll_fd, 1, timeout as i32) };
                if n == 0 {
                    return false;
                }

                if poll_fd.revents & POLLERR > 0 {
                    return false;
                }
            }
            self.should_release_packet = true;
            return true;
        }
        return false;
    }

    // The data referenced in the packet points to Shared memory. The life cycle
    // of the packet cannot exceed the next call to the read function.
    pub unsafe fn read(&mut self) -> Option<Packet> {
        if self.current.is_none()
            || !self.header_next_needed
            || !self.current.as_mut().unwrap().next()
        {
            if self.should_release_packet {
                if let Some(x) = self.current.as_mut() {
                    x.clear_status();
                }
                self.offset += 1;
                self.should_release_packet = false;
            }
            self.current = Some(self.get_packet_header());
            let ok = self.poll_for_first_packet();
            if !ok {
                self.header_next_needed = false;
                return None;
            }
            if let Some(x) = self.current.as_ref() {
                if x.get_length() == 0 {
                    // TODO: retry
                    return None;
                }
            }
        }
        if let Some(x) = self.current.as_ref() {
            let packet = Packet {
                timestamp: x.get_time(),
                if_index: x.get_iface_index(),
                data: x.get_data(),
                capture_length: x.get_length(),
                ..Default::default()
            };
            self.header_next_needed = true;
            return Some(packet);
        }
        return None;
    }

    pub fn write(&self, packet: &[u8]) -> isize {
        unsafe {
            write(
                self.raw_socket.as_raw_fd(),
                packet.as_ptr() as *const c_void,
                packet.len() as size_t,
            ) as isize
        }
    }

    pub fn set_bpf(&self, ins: Vec<bpf::RawInstruction>) -> af_packet::Result<()> {
        let prog = bpf::Prog::new(ins);
        self.setsockopt(SOL_SOCKET, SO_ATTACH_FILTER, prog)
    }

    pub fn get_counter_handle(&self) -> TpacketCounter {
        TpacketCounter {
            tp_version: self.tp_version,
            fd: self.raw_socket.as_raw_fd(),
        }
    }

    pub fn new(opts: options::Options) -> Result<Self> {
        opts.check()?;
        // 创建原始socket
        let raw_socket = unsafe {
            socket(
                AF_PACKET,
                opts.socket_type.to_i32(),
                (ETH_P_ALL as u16).to_be() as i32,
            )
        };
        if raw_socket == -1 {
            return Err(io::Error::last_os_error().into());
        }
        let raw_socket = unsafe { Socket::from_raw_fd(raw_socket) };
        let mut tpacket = Tpacket {
            _stats: Stats {
                packets: 0,
                polls: 0,
            },
            raw_socket,
            ring: std::ptr::null_mut(),
            opts: opts.clone(),
            offset: 0,
            current: Option::None,
            should_release_packet: false,
            header_next_needed: false,
            tp_version: opts.version,
            v3: Option::None,
        };
        tpacket.bind()?;
        tpacket.set_version()?;
        tpacket.set_ring()?;
        tpacket.mmap_ring()?;
        tpacket.set_fanout()?;
        tpacket.set_bpf(vec![bpf::BpfSyntax::RetConstant(bpf::RetConstant {
            val: 0,
        })
        .to_instruction()])?;
        Ok(tpacket)
    }
}

impl Drop for Tpacket {
    fn drop(&mut self) {
        if !self.ring.is_null() {
            unsafe {
                munmap(
                    self.ring as *mut c_void,
                    (self.opts.block_size * self.opts.num_blocks) as size_t,
                );
            }
            if let Err(_e) = self.raw_socket.shutdown(Shutdown::Both) {}
            self.ring = std::ptr::null_mut();
        }
    }
}

pub struct TpacketCounter {
    tp_version: options::OptTpacketVersion,
    fd: i32,
}

impl stats::RefCountable for TpacketCounter {
    fn get_counters(&self) -> Vec<stats::Counter> {
        if self.tp_version == options::OptTpacketVersion::TpacketVersion3 {
            let mut stats_v3 = TpacketStatsV3 {
                tp_packets: 0,
                tp_drops: 0,
                tp_freeze_q_cnt: 0,
            };
            let mut opt_len = std::mem::size_of_val(&stats_v3) as socklen_t;
            unsafe {
                let ret = getsockopt(
                    self.fd,
                    SOL_PACKET,
                    PACKET_STATISTICS,
                    &mut stats_v3 as *const TpacketStatsV3 as *mut c_void,
                    &mut opt_len as *mut u32,
                );
                if ret != 0 {
                    warn!("{:?}", io::Error::last_os_error());
                    return vec![];
                }
            }
            vec![
                (
                    "kernel_packets",
                    stats::CounterType::Counted,
                    stats::CounterValue::Unsigned(stats_v3.tp_packets as u64),
                ),
                (
                    "kernel_drops",
                    stats::CounterType::Counted,
                    stats::CounterValue::Unsigned(stats_v3.tp_drops as u64),
                ),
                (
                    "kernel_freezes",
                    stats::CounterType::Counted,
                    stats::CounterValue::Unsigned(stats_v3.tp_freeze_q_cnt as u64),
                ),
            ]
        } else if self.tp_version == options::OptTpacketVersion::TpacketVersion2 {
            let mut stats = TpacketStats {
                tp_packets: 0,
                tp_drops: 0,
            };
            let mut opt_len = std::mem::size_of_val(&stats) as socklen_t;
            unsafe {
                let ret = getsockopt(
                    self.fd,
                    SOL_PACKET,
                    PACKET_STATISTICS,
                    &mut stats as *const TpacketStats as *mut c_void,
                    &mut opt_len as *mut u32,
                );
                if ret != 0 {
                    warn!("{:?}", io::Error::last_os_error());
                    return vec![];
                }
            }
            vec![
                (
                    "kernel_packets",
                    stats::CounterType::Counted,
                    stats::CounterValue::Unsigned(stats.tp_packets as u64),
                ),
                (
                    "kernel_drops",
                    stats::CounterType::Counted,
                    stats::CounterValue::Unsigned(stats.tp_drops as u64),
                ),
            ]
        } else {
            warn!("invalid tp version");
            vec![]
        }
    }
}
