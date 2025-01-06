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

pub mod af_packet;
pub(crate) mod bpf;

use std::ffi::CStr;
use std::sync::{atomic::AtomicU64, Arc};
use std::time::Duration;

#[cfg(any(target_os = "linux", target_os = "android"))]
use af_packet::{options::Options, tpacket::Tpacket};
pub use public::error::{Error, Result};
use public::packet;

use crate::utils::stats;

#[cfg(target_os = "linux")]
pub use special_recv_engine::{Dpdk, VhostUser};
pub use special_recv_engine::{Libpcap, LibpcapCounter};

pub const DEFAULT_BLOCK_SIZE: usize = 1 << 20;
pub const FRAME_SIZE_MAX: usize = 1 << 16; // local and mirror
pub const FRAME_SIZE_MIN: usize = 1 << 11; // analyzer
pub const POLL_TIMEOUT: Duration = Duration::from_millis(100);

pub enum RecvEngine {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    AfPacket(Tpacket),
    #[cfg(target_os = "linux")]
    Dpdk(Dpdk),
    Libpcap(Option<Libpcap>),
    #[cfg(target_os = "linux")]
    VhostUser(VhostUser),
}

impl RecvEngine {
    const LIBPCAP_NONE: &'static str = "libpcap packet capture is none";

    pub fn init(&mut self) -> Result<()> {
        match self {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::AfPacket(_) => Ok(()),
            #[cfg(target_os = "linux")]
            Self::Dpdk(_) => Ok(()),
            Self::Libpcap(_) => Ok(()),
            #[cfg(target_os = "linux")]
            Self::VhostUser(_) => Ok(()),
        }
    }

    pub fn close(&mut self) {
        match self {
            Self::Libpcap(w) => {
                let _ = w.take();
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            _ => (),
        }
    }

    pub unsafe fn recv(&mut self) -> Result<packet::Packet> {
        match self {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::AfPacket(e) => match e.read() {
                Some(p) => Ok(p),
                None => Err(Error::Timeout),
            },
            #[cfg(target_os = "linux")]
            Self::Dpdk(d) => match d.read() {
                Ok(p) => Ok(p),
                _ => Err(Error::Timeout),
            },
            Self::Libpcap(w) => w
                .as_mut()
                .ok_or(Error::LibpcapError(Self::LIBPCAP_NONE.to_string()))
                .and_then(|e| e.read()),
            #[cfg(target_os = "linux")]
            Self::VhostUser(v) => match v.read() {
                Ok(p) => Ok(p),
                _ => Err(Error::Timeout),
            },
        }
    }

    #[allow(unused_variables)]
    pub fn set_bpf(&mut self, ins: Vec<af_packet::RawInstruction>, syntax: &CStr) -> Result<()> {
        match self {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::AfPacket(e) => e.set_bpf(ins).map_err(|e| e.into()),
            Self::Libpcap(w) => w
                .as_mut()
                .ok_or(Error::LibpcapError(Self::LIBPCAP_NONE.to_string()))
                .and_then(|e| e.set_bpf(syntax.to_str().unwrap())),
            #[cfg(target_os = "linux")]
            Self::Dpdk(_) => Ok(()),
            #[cfg(target_os = "linux")]
            Self::VhostUser(_) => Ok(()),
        }
    }

    pub fn set_promisc(&mut self, if_index: &Vec<i32>) -> Result<()> {
        match self {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::AfPacket(e) => e.set_promisc(if_index).map_err(|e| e.into()),
            _ => return Ok(()),
        }
    }

    pub fn get_counter_handle(&self) -> Arc<dyn stats::RefCountable> {
        match self {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::AfPacket(e) => Arc::new(e.get_counter_handle()),
            #[cfg(target_os = "linux")]
            Self::Dpdk(d) => d.get_counter_handle(),
            Self::Libpcap(w) => match w {
                Some(w) => w.get_counter_handle(),
                None => Arc::new(LibpcapCounter::default()),
            },
            #[cfg(target_os = "linux")]
            Self::VhostUser(v) => v.get_counter_handle(),
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
impl Default for RecvEngine {
    fn default() -> Self {
        Self::AfPacket(Tpacket::new(Options::default()).unwrap())
    }
}

#[cfg(target_os = "windows")]
impl Default for RecvEngine {
    fn default() -> Self {
        Self::Libpcap(None)
    }
}

#[derive(Default)]
pub(super) struct Counter {
    pub(super) retired: AtomicU64,
    pub(super) kernel_packets: AtomicU64,
    pub(super) kernel_drops: AtomicU64,
    pub(super) kernel_freezes: AtomicU64,

    pub(super) poll_error: AtomicU64,
    pub(super) intr_error: AtomicU64,
}
