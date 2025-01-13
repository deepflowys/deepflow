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

use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{fs, thread};

use super::Error;
use crate::config::handler::EnvironmentAccess;
use crate::utils::environment::is_kernel_available;

use arc_swap::access::Access;
use cgroups_rs::{
    cgroup_builder::CgroupBuilder,
    cpu::CpuController,
    hierarchies,
    memory::{MemController, Memory},
    Cgroup, CgroupPid, Controller, CpuResources, MemoryResources, Resources,
};
use log::{debug, info, trace, warn};
use public::consts::{DEFAULT_CPU_CFS_PERIOD_US, PROCESS_NAME};

pub struct Cgroups {
    config: EnvironmentAccess,
    thread: Mutex<Option<JoinHandle<()>>>,
    running: Arc<(Mutex<bool>, Condvar)>,
    cgroup: Cgroup,
    mount_path: String,
    is_v2: bool,
}

const CHECK_INTERVAL: Duration = Duration::from_secs(1);

fn cgroups_supported() -> bool {
    let Ok(fs) = fs::read_to_string("/proc/filesystems") else {
        return false;
    };
    fs.lines()
        .any(|line| line.to_lowercase().contains("cgroup"))
}

impl Cgroups {
    /// 创建cgroup hierarchy
    pub fn new(pid: u64, config: EnvironmentAccess) -> Result<Self, Error> {
        if !cgroups_supported() {
            return Err(Error::CgroupsNotSupported(
                "read /proc/filesystems failed or cgroups/cgroups2 not found.".to_string(),
            ));
        }
        let hier = hierarchies::auto();
        let is_v2 = hier.v2();
        let cg: Cgroup = CgroupBuilder::new(PROCESS_NAME).build(hier);
        let cpus: &CpuController = match cg.controller_of() {
            Some(controller) => controller,
            None => {
                return Err(Error::CpuControllerSetFailed(format!(
                    "maybe cgroups is not installed"
                )));
            }
        };
        let mem: &MemController = match cg.controller_of() {
            Some(controller) => controller,
            None => {
                return Err(Error::MemControllerSetFailed(format!(
                    "maybe cgroups is not installed"
                )));
            }
        };

        if !is_cgroup_procs_writable() {
            // In kernel versions before Linux 3.0, we use add_task method, write thread id to the tasks file
            if let Err(e) = cpus.add_task(&CgroupPid::from(pid)) {
                // fixme:All thread IDs belonging to this process need to be recorded to this file
                return Err(Error::CpuControllerSetFailed(e.to_string()));
            }
            if let Err(e) = mem.add_task(&CgroupPid::from(pid)) {
                return Err(Error::MemControllerSetFailed(e.to_string()));
            }
        } else {
            // In versions after Linux 3.0, we call the add_task_by_tgid method, which will
            // write the pid to the cgroup.procs file, so cgroups will automatically synchronize
            // the tasks file. Refer to: https://wudaijun.com/2018/10/linux-cgroup/
            if let Err(e) = cpus.add_task_by_tgid(&CgroupPid::from(pid)) {
                return Err(Error::CpuControllerSetFailed(e.to_string()));
            }
            if let Err(e) = mem.add_task_by_tgid(&CgroupPid::from(pid)) {
                return Err(Error::MemControllerSetFailed(e.to_string()));
            }
        }

        Ok(Cgroups {
            config,
            thread: Mutex::new(None),
            running: Arc::new((Mutex::new(false), Condvar::new())),
            cgroup: cg,
            mount_path: hierarchies::auto().root().to_str().unwrap().to_string(),
            is_v2,
        })
    }

    pub fn get_mount_path(&self) -> String {
        self.mount_path.clone()
    }

    pub fn is_v2(&self) -> bool {
        self.is_v2
    }

    pub fn start(&self) {
        {
            let (started, _) = &*self.running;
            let mut started = started.lock().unwrap();
            if *started {
                return;
            }
            *started = true;
        }

        let environment_config = self.config.clone();
        let running = self.running.clone();
        let mut last_millicpus = 0;
        let mut last_memory = 0;
        let cgroup = self.cgroup.clone();
        let thread = thread::Builder::new()
            .name("cgroups-controller".to_owned())
            .spawn(move || {
                loop {
                    let environment = environment_config.load();
                    let max_millicpus = environment.max_millicpus;
                    let max_memory = environment.max_memory;
                    if max_millicpus != last_millicpus || max_memory != last_memory {
                        if let Err(e) = Self::apply(cgroup.clone(), max_millicpus, max_memory) {
                            warn!(
                                "apply cgroups resource failed, {}, deepflow-agent restart...",
                                e
                            );
                            crate::utils::notify_exit(1);
                            break;
                        }
                    }
                    last_millicpus = max_millicpus;
                    last_memory = max_memory;

                    let (running, timer) = &*running;
                    let mut running = running.lock().unwrap();
                    if !*running {
                        break;
                    }
                    running = timer.wait_timeout(running, CHECK_INTERVAL).unwrap().0;
                    if !*running {
                        break;
                    }
                }
                info!("cgroups controller exited");
            })
            .unwrap();

        self.thread.lock().unwrap().replace(thread);
        info!("cgroups controller started");
    }

    /// 更改资源限制
    pub fn apply(cgroup: Cgroup, max_millicpus: u32, max_memory: u64) -> Result<(), Error> {
        let mut resources = Resources::default();
        let cpu_quota = max_millicpus * 100; // The unit of cpu_quota is 100_000 us. Convert max_millicpus to the unit of cpu_quota
        let cpu_resources = CpuResources {
            quota: Some(cpu_quota as i64),
            period: Some(DEFAULT_CPU_CFS_PERIOD_US as u64),
            ..Default::default()
        };
        resources.cpu = cpu_resources;

        let memory_resources = MemoryResources {
            memory_hard_limit: Some(max_memory as i64),
            ..Default::default()
        };
        resources.memory = memory_resources;
        if let Err(e) = cgroup.apply(&resources) {
            return Err(Error::ApplyResourcesFailed(e.to_string()));
        }
        Ok(())
    }

    /// 结束cgroup资源限制
    pub fn stop(&self) -> Result<(), Error> {
        let (stopped, timer) = &*self.running;
        {
            let mut stopped = stopped.lock().unwrap();
            if !*stopped {
                return Ok(());
            }
            *stopped = false;
        }
        timer.notify_one();

        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
        if let Err(e) = self.cgroup.delete() {
            return Err(Error::DeleteCgroupsFailed(e.to_string()));
        }
        info!("cgroups controller stopped");
        Ok(())
    }
}

pub fn is_kernel_available_for_cgroups() -> bool {
    const MIN_KERNEL_VERSION_SUPPORT_CGROUP: &str = "2.6.24"; // Support cgroups from Linux 2.6.24
    is_kernel_available(MIN_KERNEL_VERSION_SUPPORT_CGROUP)
}
pub fn is_cgroup_procs_writable() -> bool {
    // The cgroup.procs file can only be written after Linux 3.0. Refer to:
    // https://github.com/torvalds/linux/commit/74a1166dfe1135dcc168d35fa5261aa7e087011b
    const MIN_KERNEL_VERSION_CGROUP_PROCS: &str = "3";
    is_kernel_available(MIN_KERNEL_VERSION_CGROUP_PROCS)
}

const PID1_ROOT: &str = "/proc/1/root";

/*
 * The path of container memory cgroup from its own namespace is `/sys/fs/cgroup/memory`.
 * However, the path is mounted read-only, making it impossible to reclaim memory cache with:
 *
 *     `echo 0 > /sys/fs/cgroup/memory/memory.force_empty`
 *
 * The approach here is to take a little detour, visiting the actual memory cgroup path from global cgroup namespace.
 * The path would be:
 *
 *     /proc/1/root/{memory_cgroup_mount_point}/{cgroup_path}/memory.force_empty
 */
fn get_memory_cgroup_path() -> procfs::ProcResult<Option<PathBuf>> {
    let mut path = PathBuf::from(PID1_ROOT);
    let proc = procfs::process::Process::myself()?;

    let Some(mount_info) = proc
        .mountinfo()?
        .into_iter()
        .find(|m| m.fs_type == "cgroup" && m.super_options.contains_key("memory"))
    else {
        debug!("memory cgroup not found");
        return Ok(None);
    };
    trace!(
        "memory cgroup mount point: {}",
        mount_info.mount_point.display()
    );
    let mut mount_point = mount_info.mount_point.components();
    mount_point.next(); // skip "/"
    path.extend(mount_point);

    let Some(cg_info) = proc
        .cgroups()?
        .into_iter()
        .find(|cg| cg.controllers.iter().any(|c| c == "memory"))
    else {
        return Ok(None);
    };
    trace!("memory cgroup path: {}", cg_info.pathname);
    if cg_info.pathname == "/" {
        debug!("memory cgroup is mounted on root");
        return Ok(None);
    }
    let mut cg_path = Path::new(&cg_info.pathname).components();
    cg_path.next(); // skip "/"
    path.extend(cg_path);

    trace!("memory cgroup path: {}", path.display());
    Ok(Some(path))
}

fn cgroups_v1_check() -> bool {
    if !cgroups_supported() {
        debug!("cgroups not supported for this system");
        return false;
    }
    if hierarchies::is_cgroup2_unified_mode() {
        debug!("cgroups v2 is not supported");
        return false;
    }

    true
}

pub(crate) fn memory_info() -> Option<Memory> {
    if !cgroups_v1_check() {
        return None;
    }

    let mem_mount = match get_memory_cgroup_path() {
        Ok(Some(path)) => path,
        Ok(None) => {
            debug!("cgroups memory mount point not found or is invalid");
            return None;
        }
        Err(e) => {
            warn!("get memory path failed: {e}");
            return None;
        }
    };

    Some(MemController::new(mem_mount.clone(), false).memory_stat())
}

pub(crate) fn page_cache_reclaim_check(threshold: u8) -> bool {
    if threshold >= 100 {
        return false;
    }
    if !cgroups_v1_check() {
        return false;
    }

    let mem_mount = match get_memory_cgroup_path() {
        Ok(Some(path)) => path,
        Ok(None) => {
            debug!("cgroups memory mount point not found or is invalid");
            return false;
        }
        Err(e) => {
            warn!("get memory path failed: {e}");
            return false;
        }
    };
    let mc = MemController::new(mem_mount.clone(), false);
    let mut reclaim_path = mem_mount;
    reclaim_path.set_file_name("memory.force_empty");

    let m_stat = mc.memory_stat();
    let percentage = m_stat.stat.cache * 100 / m_stat.limit_in_bytes as u64;
    if percentage < threshold as u64 {
        debug!("cache / limit = {percentage}% < {threshold}%");
        return false;
    }

    debug!("cache before reclaim: {}", m_stat.stat.cache);
    if let Err(e) = fs::write(&reclaim_path, b"0") {
        warn!(
            "reclaim memory cache write to {} failed: {e}",
            reclaim_path.display()
        );
        return false;
    }
    debug!("cache after reclaim: {}", mc.memory_stat().stat.cache);
    true
}
