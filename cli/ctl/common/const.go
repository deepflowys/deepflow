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

package common

import "time"

const (
	SUCCESS = "SUCCESS"

	DEFAULT_ENCRYPTION_PASSWORD = "******"
)

var RESOURCE_TYPES = []string{
	"SubDomains", "Regions", "AZs", "Hosts", "VMs", "VPCs", "Networks", "Subnets", "VRouters",
	"RoutingTables", "DHCPPorts", "SecurityGroups", "SecurityGroupRules", "VMSecurityGroups",
	"NATGateways", "NATRules", "NATVMConnections", "LBs", "LBListeners", "LBTargetServers",
	"LBVMConnections", "PeerConnections", "CENs", "RedisInstances", "RDSInstances", "VInterfaces",
	"IPs", "FloatingIPs", "PodClusters", "PodNodes", "VMPodNodeConnections", "PodNamespaces",
	"PodGroups", "PodReplicaSets", "Pods", "PodServices", "PodServicePorts", "PodGroupPorts",
	"PodIngresses", "PodIngressRules", "PodIngressRuleBackends", "Processes", "PrometheusTargets", "VIPs",
}

//go:generate stringer -type=DomainType -trimprefix=DOMAIN_TYPE_ -linecomment
type DomainType int

const (
	// attention: following line comments are used by `stringer`
	DOMAIN_TYPE_UNKNOWN           DomainType = -1
	DOMAIN_TYPE_OPENSTACK         DomainType = 1  // openstack
	DOMAIN_TYPE_VSPHERE           DomainType = 2  // vsphere
	DOMAIN_TYPE_TENCENT           DomainType = 4  // tencent
	DOMAIN_TYPE_FILEREADER        DomainType = 5  // filereader
	DOMAIN_TYPE_AWS               DomainType = 6  // aws
	DOMAIN_TYPE_ALIYUN            DomainType = 9  // aliyun
	DOMAIN_TYPE_HUAWEI_PRIVATE    DomainType = 10 // huawei_private
	DOMAIN_TYPE_KUBERNETES        DomainType = 11 // kubernetes
	DOMAIN_TYPE_SIMULATION        DomainType = 12 // simulation
	DOMAIN_TYPE_HUAWEI            DomainType = 13 // huawei
	DOMAIN_TYPE_QINGCLOUD         DomainType = 14 // qingcloud
	DOMAIN_TYPE_QINGCLOUD_PRIVATE DomainType = 15 // qingcloud_private
	DOMAIN_TYPE_AZURE             DomainType = 18 // azure
	DOMAIN_TYPE_APSARA_STACK      DomainType = 19 // apsara_stack
	DOMAIN_TYPE_TENCENT_TCE       DomainType = 20 // tencent_tce
	DOMAIN_TYPE_AGENT_SYNC        DomainType = 23 // agent_sync
	DOMAIN_TYPE_MICROSOFT_ACS     DomainType = 24 // microsoft_acs
	DOMAIN_TYPE_BAIDU_BCE         DomainType = 25 // baidu_bce
	DOMAIN_TYPE_ESHORE            DomainType = 26 // eshore
	DOMAIN_TYPE_CLOUD_TOWER       DomainType = 27 // cloudtower
	DOMAIN_TYPE_NFVO              DomainType = 28 // nfvo
	DOMAIN_TYPE_SUGON             DomainType = 29 // sugon
	DOMAIN_TYPE_VOLCENGINE        DomainType = 30 // volcengine
	DOMAIN_TYPE_H3C               DomainType = 31 // h3c
	DOMAIN_TYPE_FUSIONCOMPUTE     DomainType = 32 // fusioncompute
)

var DomainTypes []DomainType = []DomainType{
	DOMAIN_TYPE_OPENSTACK,
	DOMAIN_TYPE_VSPHERE,
	DOMAIN_TYPE_TENCENT,
	DOMAIN_TYPE_FILEREADER,
	DOMAIN_TYPE_AWS,
	DOMAIN_TYPE_ALIYUN,
	DOMAIN_TYPE_HUAWEI_PRIVATE,
	DOMAIN_TYPE_KUBERNETES,
	DOMAIN_TYPE_SIMULATION,
	DOMAIN_TYPE_HUAWEI,
	DOMAIN_TYPE_QINGCLOUD,
	DOMAIN_TYPE_QINGCLOUD_PRIVATE,
	DOMAIN_TYPE_AZURE,
	DOMAIN_TYPE_APSARA_STACK,
	DOMAIN_TYPE_TENCENT_TCE,
	DOMAIN_TYPE_AGENT_SYNC,
	DOMAIN_TYPE_MICROSOFT_ACS,
	DOMAIN_TYPE_BAIDU_BCE,
	DOMAIN_TYPE_ESHORE,
	DOMAIN_TYPE_CLOUD_TOWER,
	DOMAIN_TYPE_NFVO,
	DOMAIN_TYPE_SUGON,
	DOMAIN_TYPE_VOLCENGINE,
	DOMAIN_TYPE_H3C,
	DOMAIN_TYPE_FUSIONCOMPUTE,
}

func GetDomainTypeByName(domainTypeName string) DomainType {
	for i := 0; i < len(DomainTypes); i++ {
		if DomainTypes[i].String() == domainTypeName {
			return DomainTypes[i]
		}
	}
	return DOMAIN_TYPE_UNKNOWN
}

//go:generate stringer -type=DomainEnabled -trimprefix=DOMAIN_ENABLED_ -linecomment
type DomainEnabled int

const (
	// attention: following line comments are used by `stringer`
	DOMAIN_ENABLED_DISABLE DomainEnabled = iota
	DOMAIN_ENABLED_ENABLE
)

//go:generate stringer -type=DomainState -trimprefix=DOMAIN_STATE_ -linecomment
type DomainState int

const (
	// attention: following line comments are used by `stringer`
	DOMAIN_STATE_NORMAL DomainState = iota + 1
	_                               // 2
	DOMAIN_STATE_EXCEPTION
	DOMAIN_STATE_WARN
)

//go:generate stringer -type=VtapState -trimprefix=VTAP_STATE_ -linecomment
type VtapState int

const (
	// attention: following line comments are used by `stringer`
	VTAP_STATE_NOT_CONNECTED VtapState = iota // LOST
	VTAP_STATE_NORMAL
	VTAP_STATE_DISABLE
	VTAP_STATE_PENDING
)

//go:generate stringer -type=VtapType -trimprefix=VTAP_TYPE_ -linecomment
type VtapType int

const (
	// attention: following line comments are used by `stringer`
	VTAP_TYPE_KVM VtapType = 1 + iota
	VTAP_TYPE_ESXI
	VTAP_TYPE_WORKLOAD_V // CHOST_VM
	_                    // 4
	VTAP_TYPE_WORKLOAD_P // CHOST_BM
	VTAP_TYPE_DEDICATED
	VTAP_TYPE_POD_HOST             // K8S_BM
	VTAP_TYPE_POD_VM               // K8S_VM
	VTAP_TYPE_TUNNEL_DECAPSULATION // TUN_DECAP
	VTAP_TYPE_HYPER_V
	_ // 11
	VTAP_TYPE_K8S_SIDECAR
)

//go:generate stringer -type=VtapException -trimprefix=VTAP_EXCEPTION_ -linecomment
type VtapException uint64

// need synchronized update with the server
const (
	// attention: following line comments are used by `stringer`
	VTAP_EXCEPTION_LICENSE_NOT_ENGOUTH     VtapException = 0x10000000
	VTAP_EXCEPTION_ALLOC_ANALYZER_FAILED   VtapException = 0x40000000
	VTAP_EXCEPTION_ALLOC_CONTROLLER_FAILED VtapException = 0x80000000
)

//go:generate stringer -type=VtapTapMode -trimprefix=VTAP_TAP_MODE_ -linecomment
type VtapTapMode int

const (
	// attention: following line comments are used by `stringer`
	VTAP_TAP_MODE_LOCAL     VtapTapMode = iota // local
	VTAP_TAP_MODE_MIRROR                       //mirror
	VTAP_TAP_MODE_DEDICATED                    // dedicated
)

var VtapTapModes []VtapTapMode = []VtapTapMode{
	VTAP_TAP_MODE_LOCAL,
	VTAP_TAP_MODE_MIRROR,
	VTAP_TAP_MODE_DEDICATED,
}

func GetVtapTapModeByName(tapModeName string) VtapTapMode {
	for i := 0; i < len(VtapTapModes); i++ {
		if VtapTapModes[i].String() == tapModeName {
			return VtapTapModes[i]
		}
	}
	return VTAP_TAP_MODE_LOCAL
}

//go:generate stringer -type=PluginType -trimprefix=PLUGIN_TYPE_ -linecomment
type PluginType int

const (
	// attention: following line comments are used by `stringer`
	PLUGIN_TYPE_WASM PluginType = 1 + iota
	PLUGIN_TYPE_SO
	PLUGIN_TYPE_LUA
)

type PluginUser int

//go:generate stringer -type=PluginUser -trimprefix=PLUGIN_USER_ -linecomment
const (
	// attention: following line comments are used by `stringer`
	PLUGIN_USER_AGENT PluginUser = 1 + iota
	PLUGIN_USER_SERVER
)

var (
	DefaultTimeout = time.Duration(time.Second * 30)
)
