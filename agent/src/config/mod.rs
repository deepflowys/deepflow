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

pub(crate) mod config;
pub mod handler;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub use config::ApiResources;
pub use config::{
    AgentIdType, Config, ConfigError, DpdkSource, KubernetesPollerType, OracleConfig, PcapStream,
    ProcessMatcher, PrometheusExtraLabels, UserConfig, K8S_CA_CRT_PATH,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use handler::FlowAccess;
pub use handler::{DispatcherConfig, FlowConfig, ModuleConfig, NpbConfig};

#[cfg(test)]
pub use config::{ExtraLogFields, HttpEndpoint, HttpEndpointMatchRule};
#[cfg(test)]
pub use handler::HttpEndpointTrie;
