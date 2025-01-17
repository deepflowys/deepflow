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

import (
	"time"
)

var GConfig *GlobalConfig

const GO_BIRTHDAY = "2006-01-02 15:04:05"
const K8S_CA_CRT_PATH = "/run/secrets/kubernetes.io/serviceaccount/ca.crt"

const (
	DEFAULT_ORG_ID    = 1
	DEFAULT_USER_TYPE = 1
	DEFAULT_USER_ID   = 1
	DEFAULT_TEAM_ID   = 1
	DEFAULT_APP_KEY   = "8c434f9a48bf1b7e729bde006e0409f8"
	ORG_ID_MAX        = 1024
)

const (
	REMOTE_API_TIMEOUT = 30
	INGESTER_API_PORT  = 30106
)

const (
	LOCALHOST                    = "127.0.0.1"
	MASTER_CONTROLLER_CHECK_PORT = 4040
)

const (
	HEALTH_CHECK_INTERVAL = 60 * time.Second
	HEALTH_CHECK_URL      = "http://%s:%d/v1/health/"
)

const (
	HOST_STATE_COMPLETE    = 2
	HOST_STATE_EXCEPTION   = 4
	HOST_STATE_MAINTENANCE = 5

	HOST_TYPE_VM  = 1
	HOST_TYPE_NSP = 3
	HOST_TYPE_DFI = 4

	HOST_HTYPE_ESXI    = 2
	HOST_HTYPE_KVM     = 3
	HOST_HTYPE_HYPER_V = 5
	HOST_HTYPE_GATEWAY = 6
)

const (
	HOST_VCPUS     = 8
	HOST_MEMORY_MB = 16384
)

const (
	HOST_TYPE_CONTROLLER = "controller"
	HOST_TYPE_ANALYZER   = "analyzer"
)

const (
	CONTROLLER_NODE_TYPE_MASTER = 1
	CONTROLLER_NODE_TYPE_SLAVE  = 2

	CONTROLLER_STATE_NORMAL    = 2
	CONTROLLER_STATE_EXCEPTION = 4
)

const (
	ARCH_X86 = 1
	ARCH_ARM = 2
)

const (
	OS_CENTOS  = 1
	OS_REDHAT  = 2
	OS_UBUNTU  = 3
	OS_SUSE    = 4
	OS_WINDOWS = 5
	OS_ANDROID = 6
)

const (
	VTAP_ENABLE_FALSE = 0
	VTAP_ENABLE_TRUE  = 1
)

const (
	VTAP_STATE_NOT_CONNECTED = iota
	VTAP_STATE_NORMAL
	VTAP_STATE_DISABLE
	VTAP_STATE_PENDING
)

var (
	VTapStateToChinese = map[int]string{
		VTAP_STATE_NOT_CONNECTED: "失联",
		VTAP_STATE_NORMAL:        "运行",
		VTAP_STATE_DISABLE:       "禁用",
		VTAP_STATE_PENDING:       "未注册",
	}
)

const (
	VTAP_STATE_NOT_CONNECTED_STR = "LOST"
	VTAP_STATE_NORMAL_STR        = "RUNNING"
	VTAP_STATE_DISABLE_STR       = "DISABLE"
	VTAP_STATE_PENDING_STR       = "PENDING"
)

const (
	VTAP_TYPE_KVM = 1 + iota
	VTAP_TYPE_ESXI
	VTAP_TYPE_WORKLOAD_V
	_ // 4
	VTAP_TYPE_WORKLOAD_P
	VTAP_TYPE_DEDICATED
	VTAP_TYPE_POD_HOST
	VTAP_TYPE_POD_VM
	VTAP_TYPE_TUNNEL_DECAPSULATION
	VTAP_TYPE_HYPER_V
	_ // 11
	VTAP_TYPE_K8S_SIDECAR
)

var VTapTypeName = map[int]string{
	VTAP_TYPE_KVM:                  "KVM",
	VTAP_TYPE_ESXI:                 "ESXI",
	VTAP_TYPE_WORKLOAD_V:           "CHOST_VM",
	VTAP_TYPE_WORKLOAD_P:           "CHOST_BM",
	VTAP_TYPE_DEDICATED:            "DEDICATED",
	VTAP_TYPE_POD_HOST:             "K8S_BM",
	VTAP_TYPE_POD_VM:               "K8S_VM",
	VTAP_TYPE_TUNNEL_DECAPSULATION: "TUN_DECAP",
	VTAP_TYPE_HYPER_V:              "HYPER_V",
	VTAP_TYPE_K8S_SIDECAR:          "K8S_SIDECAR",
}

var VTapTypeChinese = map[int]string{
	VTAP_TYPE_KVM:                  "KVM",
	VTAP_TYPE_ESXI:                 "ESXI",
	VTAP_TYPE_WORKLOAD_V:           "云服务器-V",
	VTAP_TYPE_WORKLOAD_P:           "云服务器-P",
	VTAP_TYPE_DEDICATED:            "专属服务器",
	VTAP_TYPE_POD_HOST:             "容器-P",
	VTAP_TYPE_POD_VM:               "容器-V",
	VTAP_TYPE_TUNNEL_DECAPSULATION: "隧道解封装",
	VTAP_TYPE_HYPER_V:              "Hyper-V",
	VTAP_TYPE_K8S_SIDECAR:          "K8s-Sidecar",
}

// need synchronized update with the cli
const (
	VTAP_EXCEPTION_LICENSE_NOT_ENGOUTH     = 0x10000000
	VTAP_EXCEPTION_ALLOC_ANALYZER_FAILED   = 0x40000000
	VTAP_EXCEPTION_ALLOC_CONTROLLER_FAILED = 0x80000000
)

var VTapExceptionChinese = map[int64]string{
	1 << 0:                                 "自检失败：日志所在磁盘剩余空间不足100MB",
	1 << 1:                                 "自检失败：可用内存不足",
	1 << 2:                                 "自检失败：Coredump文件过多",
	1 << 3:                                 "分发熔断",
	1 << 4:                                 "分发流量达到限速",
	1 << 5:                                 "到分发点的网关ARP无法找到",
	1 << 6:                                 "采集包速率达到限速",
	1 << 7:                                 "到数据节点的网关ARP无法找到",
	1 << 8:                                 "控制器下发的配置信息校验不通过",
	1 << 9:                                 "采集器线程数超限",
	1 << 10:                                "采集器进程数超限",
	1 << 11:                                "",
	1 << 12:                                "采集器编译生成的分发和PCAP策略数量超限",
	1 << 13:                                "空闲内存超限",
	1 << 14:                                "日志文件大小超限",
	1 << 15:                                "控制SOCKET错误",
	1 << 16:                                "数据SOCKET错误",
	1 << 17:                                "分发SOCKET错误",
	1 << 18:                                "集成SOCKET错误",
	1 << 19:                                "CGROUPS配置错误",
	VTAP_EXCEPTION_LICENSE_NOT_ENGOUTH:     "采集器授权个数不足",
	VTAP_EXCEPTION_ALLOC_ANALYZER_FAILED:   "分配数据节点失败",
	VTAP_EXCEPTION_ALLOC_CONTROLLER_FAILED: "分配控制器失败",
}

const VTAP_LICENSE_CHECK_INTERVAL = time.Minute

const (
	VTAP_LICENSE_TYPE_NONE = iota
	VTAP_LICENSE_TYPE_A
	VTAP_LICENSE_TYPE_B
	VTAP_LICENSE_TYPE_C
	VTAP_LICENSE_TYPE_DEDICATED
	VTAP_LICENSE_TYPE_MAX
)

const (
	VTAP_LICENSE_FUNCTION_NONE = iota
	VTAP_LICENSE_FUNCTION_TRAFFIC_DISTRIBUTION
	VTAP_LICENSE_FUNCTION_NETWORK_MONITORING
	VTAP_LICENSE_FUNCTION_CALL_MONITORING
	VTAP_LICENSE_FUNCTION_FUNCTION_MONITORING
	VTAP_LICENSE_FUNCTION_APPLICATION_MONITORING
	VTAP_LICENSE_FUNCTION_INDICATOR_MONITORING
	VTAP_LICENSE_FUNCTION_DATABASE_MONITORING
	VTAP_LICENSE_FUNCTION_LOG_MONITORING
	VTAP_LICENSE_FUNCTION_MAX
)

const VTAP_ALL_LICENSE_FUNCTIONS = "1,2,3,4,5,6,7,8"

var VTAP_TYPE_TO_DEVICE_TYPE = map[int]int{
	VTAP_TYPE_KVM:                  VIF_DEVICE_TYPE_HOST,
	VTAP_TYPE_ESXI:                 VIF_DEVICE_TYPE_HOST,
	VTAP_TYPE_WORKLOAD_V:           VIF_DEVICE_TYPE_VM,
	VTAP_TYPE_WORKLOAD_P:           VIF_DEVICE_TYPE_VM,
	VTAP_TYPE_DEDICATED:            0,
	VTAP_TYPE_POD_HOST:             VIF_DEVICE_TYPE_POD_NODE,
	VTAP_TYPE_POD_VM:               VIF_DEVICE_TYPE_POD_NODE,
	VTAP_TYPE_TUNNEL_DECAPSULATION: 0,
	VTAP_TYPE_HYPER_V:              VIF_DEVICE_TYPE_HOST,
	VTAP_TYPE_K8S_SIDECAR:          VIF_DEVICE_TYPE_POD,
}

const (
	DEFAULT_DOMAIN        = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	DEFAULT_REGION        = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	DEFAULT_AZ            = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	DEFAULT_VTAP_GROUP_ID = 1
	DEFAULT_DOMAIN_ICON   = -3
	DEFAULT_REGION_NAME   = "系统默认"
)

const (
	DOMAIN_ENABLED_FALSE = 0
	DOMAIN_ENABLED_TRUE  = 1

	DOMAIN_STATE_NORMAL = 1
)

const (
	ACL_STATE_ENABLE = 1
)

const (
	NPB_POLICY_FLOW_DROP       = 0
	NPB_POLICY_FLOW_DISTRIBUTE = 1
)

const (
	DEFAULT_ENCRYPTION_PASSWORD = "******"
	DEFAULT_ALL_MATCH_REGEX     = ".*"
	DEFAULT_NOT_MATCH_REGEX     = "^$"
	DEFAULT_PORT_NAME_REGEX     = "^(cni|flannel|vxlan.calico|tunl|en[ospx])"

	OPENSTACK         = 1
	VSPHERE           = 2
	NSP               = 3
	TENCENT           = 4
	FILEREADER        = 5
	AWS               = 6
	ZSTACK            = 8
	ALIYUN            = 9
	HUAWEI_PRIVATE    = 10
	KUBERNETES        = 11
	SIMULATION        = 12
	HUAWEI            = 13
	QINGCLOUD         = 14
	QINGCLOUD_PRIVATE = 15
	F5                = 16
	CMB_CMDB          = 17
	AZURE             = 18
	APSARA_STACK      = 19
	TENCENT_TCE       = 20
	KINGSOFT_PRIVATE  = 22
	AGENT_SYNC        = 23
	MICROSOFT_ACS     = 24
	BAIDU_BCE         = 25
	ESHORE            = 26
	CLOUD_TOWER       = 27
	NFVO              = 28
	SUGON             = 29
	VOLCENGINE        = 30
	H3C               = 31
	FUSIONCOMPUTE     = 32

	OPENSTACK_EN         = "openstack"
	VSPHERE_EN           = "vsphere"
	NSP_EN               = "nsp"
	TENCENT_EN           = "tencent"
	FILEREADER_EN        = "filereader"
	AWS_EN               = "aws"
	ZSTACK_EN            = "zstack"
	ALIYUN_EN            = "aliyun"
	HUAWEI_PRIVATE_EN    = "huawei_private"
	KUBERNETES_EN        = "kubernetes"
	SIMULATION_EN        = "simulation"
	HUAWEI_EN            = "huawei"
	QINGCLOUD_EN         = "qingcloud"
	QINGCLOUD_PRIVATE_EN = "qingcloud_private"
	F5_EN                = "f5"
	CMB_CMDB_EN          = "cmb_cmdb"
	AZURE_EN             = "azure"
	APSARA_STACK_EN      = "apsara_stack"
	TENCENT_TCE_EN       = "tencent_tce"
	ESHORE_EN            = "eshore"
	QINGCLOUD_K8S_EN     = "qingcloud_k8s"
	KINGSOFT_PRIVATE_EN  = "kingsoft_private"
	AGENT_SYNC_EN        = "genesis"
	MICROSOFT_ACS_EN     = "microsoft_acs"
	BAIDU_BCE_EN         = "baidu_bce"
	CLOUD_TOWER_EN       = "cloudtower"
	NFVO_EN              = "nfvo"
	SUGON_EN             = "sugon"
	VOLCENGINE_EN        = "volcengine"
	H3C_EN               = "h3c"
	FUSIONCOMPUTE_EN     = "fusioncompute"

	TENCENT_CH          = "腾讯云"
	ALIYUN_CH           = "阿里云"
	HUAWEI_CH           = "华为云"
	QINGCLOUD_CH        = "青云"
	KINGSOFT_PRIVATE_CH = "金山银河云"
	MICROSOFT_CH        = "微软云"
	BAIDU_BCE_CH        = "百度云"
	ESHORE_CH           = "亿迅云"
	NFVO_CH             = "华为NFVO+"
	SUGON_CH            = "曙光云"
	VOLCENGINE_CH       = "火山云"
	H3C_CH              = "华三云"

	OPENSTACK_CH     = "OpenStack"
	VSPHERE_CH       = "vSphere"
	NSP_CH           = "NSP"
	AWS_CH           = "AWS"
	ZSTACK_CH        = "ZStack"
	KUBERNETES_CH    = "Kubernetes"
	CLOUD_TOWER_CH   = "CloudTower"
	FUSIONCOMPUTE_CH = "FusionCompute"
)

var DomainTypeToIconID = map[int]int{
	KUBERNETES: 14,
}

var IconNameToDomainTypes = map[string][]int{
	OPENSTACK_CH:        {OPENSTACK},
	VSPHERE_CH:          {VSPHERE},
	NSP_CH:              {NSP},
	TENCENT_CH:          {TENCENT, TENCENT_TCE},
	AWS_CH:              {AWS},
	ZSTACK_CH:           {ZSTACK},
	ALIYUN_CH:           {ALIYUN, APSARA_STACK},
	KUBERNETES_CH:       {KUBERNETES},
	HUAWEI_CH:           {HUAWEI, HUAWEI_PRIVATE},
	QINGCLOUD_CH:        {QINGCLOUD, QINGCLOUD_PRIVATE},
	MICROSOFT_CH:        {AZURE, CMB_CMDB, MICROSOFT_ACS},
	KINGSOFT_PRIVATE_CH: {KINGSOFT_PRIVATE},
	BAIDU_BCE_CH:        {BAIDU_BCE},
	VOLCENGINE_CH:       {VOLCENGINE},
}

const (
	NETWORK_ISP_LCUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	NETWORK_TYPE_WAN   = 3
	NETWORK_TYPE_LAN   = 4

	SUBNET_ISP_LCUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"
)

const (
	VM_STATE_RUNNING   = 4
	VM_STATE_STOPPED   = 9
	VM_STATE_EXCEPTION = 11

	VM_HTYPE_VM_C = 1
	VM_HTYPE_BM_C = 2
	VM_HTYPE_VM_N = 3
	VM_HTYPE_BM_N = 4
	VM_HTYPE_VM_S = 5
	VM_HTYPE_BM_S = 6
)

const (
	VIF_DEFAULT_MAC = "00:00:00:00:00:00"

	VIF_TYPE_WAN = 3
	VIF_TYPE_LAN = 4

	VIF_DEVICE_TYPE_VM             = 1
	VIF_DEVICE_TYPE_VROUTER        = 5
	VIF_DEVICE_TYPE_HOST           = 6
	VIF_DEVICE_TYPE_DHCP_PORT      = 9
	VIF_DEVICE_TYPE_POD            = 10
	VIF_DEVICE_TYPE_POD_SERVICE    = 11
	VIF_DEVICE_TYPE_REDIS_INSTANCE = 12
	VIF_DEVICE_TYPE_RDS_INSTANCE   = 13
	VIF_DEVICE_TYPE_POD_NODE       = 14
	VIF_DEVICE_TYPE_LB             = 15
	VIF_DEVICE_TYPE_NAT_GATEWAY    = 16

	VIF_DEVICE_TYPE_INTERNET                        = 0
	VIF_DEVICE_TYPE_POD_GROUP                       = 101
	VIF_DEVICE_TYPE_SERVICE                         = 102
	VIF_DEVICE_TYPE_POD_CLUSTER                     = 103
	VIF_DEVICE_TYPE_GPROCESS                        = 120
	VIF_DEVICE_TYPE_POD_GROUP_DEPLOYMENT            = 130
	VIF_DEVICE_TYPE_POD_GROUP_STATEFULSET           = 131
	VIF_DEVICE_TYPE_POD_GROUP_RC                    = 132
	VIF_DEVICE_TYPE_POD_GROUP_DAEMON_SET            = 133
	VIF_DEVICE_TYPE_POD_GROUP_REPLICASET_CONTROLLER = 134
	VIF_DEVICE_TYPE_POD_GROUP_CLONESET              = 135
	VIF_DEVICE_TYPE_IP                              = 255
)

const (
	CREATE_METHOD_LEARN         = 0
	CREATE_METHOD_USER_DEFINE   = 1
	CONTACT_CREATE_METHOD_LEARN = 1 // TODO 修改与其他统一
)

const (
	SECURITY_GROUP_RULE_UNKNOWN = 0
	SECURITY_GROUP_RULE_ACCEPT  = 1
	SECURITY_GROUP_RULE_DROP    = 2

	SECURITY_GROUP_RULE_INGRESS = 1
	SECURITY_GROUP_RULE_EGRESS  = 2

	SECURITY_GROUP_IP_TYPE_UNKNOWN = 0
	SECURITY_GROUP_RULE_IPV4       = 1
	SECURITY_GROUP_RULE_IPV6       = 2

	SECURITY_GROUP_RULE_IPV4_CIDR = "0.0.0.0/0"
	SECURITY_GROUP_RULE_IPV6_CIDR = "::/0"
)

const (
	ROUTING_TABLE_TYPE_VPN             = "vpn"
	ROUTING_TABLE_TYPE_LOCAL           = "local"
	ROUTING_TABLE_TYPE_ROUTER          = "router"
	ROUTING_TABLE_TYPE_NAT_GATEWAY     = "nat-gateway"
	ROUTING_TABLE_TYPE_PEER_CONNECTION = "peer-connection"
	ROUTING_TABLE_TYPE_INSTANCE        = "Instance"
	ROUTING_TABLE_TYPE_IP              = "IP"
)

const (
	LB_MODEL_INTERNAL = 1
	LB_MODEL_EXTERNAL = 2

	LB_SERVER_TYPE_VM = 1
	LB_SERVER_TYPE_IP = 2
)

const (
	RDS_UNKNOWN = 0

	RDS_TYPE_MYSQL      = 1
	RDS_TYPE_SQL_SERVER = 2
	RDS_TYPE_PPAS       = 3
	RDS_TYPE_PSQL       = 4 // PostgreSQL
	RDS_TYPE_MARIADB    = 5

	RDS_STATE_RUNNING   = 1
	RDS_STATE_RESTORING = 2

	RDS_SERIES_BASIC = 1
	RDS_SERIES_HA    = 2

	RDS_MODEL_PRIMARY   = 1
	RDS_MODEL_READONLY  = 2
	RDS_MODEL_TEMPORARY = 3
	RDS_MODEL_GUARD     = 4
	RDS_MODEL_SHARE     = 5
)

const (
	REDIS_STATE_RUNNING = 1
)

const (
	INTERVAL_1MINUTE = 60
	INTERVAL_1HOUR   = 3600
	INTERVAL_1DAY    = 86400
	INTERVAL_1WEEK   = 604800
	INTERVAL_1MONTH  = 2678400
	INTERVAL_1YEAR   = 31536000
)

const (
	DATA_SOURCE_NETWORK        = "flow_metrics.network*"
	DATA_SOURCE_APPLICATION    = "flow_metrics.application*"
	DATA_SOURCE_TRAFFIC_POLICY = "flow_metrics.traffic_policy"

	DATA_SOURCE_STATE_EXCEPTION = 0
	DATA_SOURCE_STATE_NORMAL    = 1
)

const (
	IPV4_MAX_MASK = 32
	IPV6_MAX_MASK = 128

	IPV4_DEFAULT_NETMASK = 24
	IPV6_DEFAULT_NETMASK = 64
)

const (
	POD_NODE_TYPE_MASTER = 1
	POD_NODE_TYPE_NODE   = 2

	POD_NODE_STATE_EXCEPTION = 0
	POD_NODE_STATE_NORMAL    = 1

	POD_NODE_SERVER_TYPE_HOST = 1
	POD_NODE_SERVER_TYPE_VM   = 2
)

const (
	POD_SERVICE_TYPE_CLUSTERIP = 1
	POD_SERVICE_TYPE_NODEPORT  = 2
)

const (
	POD_GROUP_DEPLOYMENT            = 1
	POD_GROUP_STATEFULSET           = 2
	POD_GROUP_RC                    = 3
	POD_GROUP_DAEMON_SET            = 4
	POD_GROUP_REPLICASET_CONTROLLER = 5
	POD_GROUP_CLONESET              = 6
)

const (
	POD_STATE_EXCEPTION = 0
	POD_STATE_RUNNING   = 1
)

const (
	K8S_POD_IPV4_NETMASK = 16
	K8S_POD_IPV6_NETMASK = 64
)

const (
	RESOURCE_STATE_CODE_SUCCESS   = 1
	RESOURCE_STATE_CODE_DELETING  = 2
	RESOURCE_STATE_CODE_EXCEPTION = 3
	RESOURCE_STATE_CODE_WARNING   = 4
)

const (
	SUB_DOMAIN_ERROR_DISPLAY_NUM = 10
)

const (
	NODE_NAME_KEY    = "K8S_NODE_NAME_FOR_DEEPFLOW"
	NODE_IP_KEY      = "K8S_NODE_IP_FOR_DEEPFLOW"
	POD_NAME_KEY     = "K8S_POD_NAME_FOR_DEEPFLOW"
	POD_IP_KEY       = "K8S_POD_IP_FOR_DEEPFLOW"
	NAME_SPACE_KEY   = "K8S_NAMESPACE_FOR_DEEPFLOW"
	RUNNING_MODE_KEY = "DEEPFLOW_SERVER_RUNNING_MODE"
)

const (
	CLOUD_METRIC_NAME_TASK_COST       = "controller_cloud_task_cost"
	CLOUD_METRIC_NAME_INFO_COUNT      = "controller_cloud_info_count"
	CLOUD_METRIC_NAME_API_COUNT       = "controller_cloud_api_count"
	CLOUD_METRIC_NAME_API_COST        = "controller_cloud_api_cost"
	GENESIS_METRIC_NAME_K8SINFO_DELAY = "controller_genesis_k8sinfo_delay"
)

var (
	TCP = 6
	UDP = 17
)

var ProtocolMap = map[string]int{
	"TCP": TCP,
	"UDP": UDP,
}

var CloudMonitorExceptionAPI = map[string]string{
	ALIYUN_EN:            "NetworkInterfaceSet,ListenerPortAndProtocol,BackendServer,SnatTableEntry,ForwardTableEntry,KVStoreZone,RouteEntry,Permission",
	TENCENT_EN:           "Listeners,NetworkInterfaceSet",
	OPENSTACK_EN:         "services,users",
	QINGCLOUD_EN:         "DescribeSecurityGroupIPSets,DescribeSecurityGroupRules,DescribeLoadBalancerListeners,DescribeLoadBalancerBackends,DescribeNics,DescribeEips",
	APSARA_STACK_EN:      "NetworkInterfaceSet,ListenerPortAndProtocol,BackendServer,SnatTableEntry,ForwardTableEntry,RouteEntry,Permission",
	TENCENT_TCE_EN:       "DescribeNetworkInterfacesEx,DescribeSecurityGroupPolicy",
	QINGCLOUD_PRIVATE_EN: "DescribeSecurityGroupIPSets,DescribeSecurityGroupRules,DescribeLoadBalancerListeners,DescribeLoadBalancerBackends,DescribeNics,DescribeEips",
}

const (
	STRINGS_JOIN_COMMA = ","
)

const (
	TAPMODE_LOCAL    = 0
	TAPMODE_MIRROR   = 1
	TAPMODE_ANALYZER = 2
	TAPMODE_DECAP    = 3
)

const (
	AGENT_IDENTIFIE_IP_AND_MAC = 1
	AGENT_IDENTIFIE_IP         = 2
)

var VtapTapModeName = map[int]string{
	TAPMODE_LOCAL:    "本地",
	TAPMODE_MIRROR:   "镜像",
	TAPMODE_ANALYZER: "专属",
	TAPMODE_DECAP:    "隧道解封装",
}

var VTapToChangeTapModes = map[int][]int{
	VTAP_TYPE_KVM:                  []int{TAPMODE_LOCAL, TAPMODE_MIRROR},
	VTAP_TYPE_ESXI:                 []int{TAPMODE_LOCAL, TAPMODE_MIRROR},
	VTAP_TYPE_WORKLOAD_V:           []int{TAPMODE_LOCAL, TAPMODE_MIRROR},
	VTAP_TYPE_WORKLOAD_P:           []int{TAPMODE_LOCAL, TAPMODE_MIRROR},
	VTAP_TYPE_DEDICATED:            []int{TAPMODE_ANALYZER},
	VTAP_TYPE_POD_HOST:             []int{TAPMODE_LOCAL, TAPMODE_MIRROR},
	VTAP_TYPE_POD_VM:               []int{TAPMODE_LOCAL, TAPMODE_MIRROR},
	VTAP_TYPE_TUNNEL_DECAPSULATION: []int{TAPMODE_DECAP},
	VTAP_TYPE_HYPER_V:              []int{TAPMODE_LOCAL, TAPMODE_MIRROR},
	VTAP_TYPE_K8S_SIDECAR:          []int{TAPMODE_LOCAL, TAPMODE_MIRROR},
}

type DataChanged string

const (
	DATA_CHANGED_VTAP          DataChanged = "vtap"
	DATA_CHANGED_ANALYZER      DataChanged = "analyzer"
	DATA_CHANGED_PLATFORM_DATA DataChanged = "platform_data"
	DATA_CHANGED_FLOW_ACL      DataChanged = "flow_acl"
	DATA_CHANGED_GROUP         DataChanged = "group"
	DATA_CHANGED_TAP_TYPE      DataChanged = "tap_type"
	DATA_CHANGED_SERVICE       DataChanged = "service"
)

const (
	BILLING_METHOD_LICENSE = "license"
	BILLING_METHOD_VOUCHER = "voucher"
)

const (
	PROCESS_INSTANCE_TYPE = 120 // used in process event
)

// plugin
const (
	PLUGIN_TYPE_WASM = 1
	PLUGIN_TYPE_SO   = 2
)

var (
	PluginTypeName = map[int]string{
		PLUGIN_TYPE_WASM: "wasm",
		PLUGIN_TYPE_SO:   "so",
	}
)

const (
	PROMETHEUS_TARGET_CREATE_METHOD_RECORDER   = 1
	PROMETHEUS_TARGET_CREATE_METHOD_PROMETHEUS = 2
)

const (
	ANALYZER_ALLOC_BY_INGESTED_DATA = "by-ingested-data"
	ANALYZER_ALLOC_BY_AGENT_COUNT   = "by-agent-count"
)

const (
	DATA_SOURCE_DEEPFLOW_SYSTEM_INTERVAL = 10
)

const (
	RUNNING_MODE_STANDALONE = "STANDALONE"
)

const (
	HEADER_KEY_CONTENT_TYPE = "Content-Type"
	HEADER_KEY_ACCEPT       = "Accept"
	CONTENT_TYPE_JSON       = "application/json"
	CONTEXT_TYPE_FORM       = "application/x-www-form-urlencoded"
	ACCEPT_JSON             = "application/json, text/plain"

	HEADER_KEY_X_ORG_ID    = "X-Org-Id"
	HEADER_KEY_X_USER_TYPE = "X-User-Type"
	HEADER_KEY_X_USER_ID   = "X-User-Id"
	HEADER_KEY_X_APP_KEY   = "X-App-Key"

	USER_TYPE_SUPER_ADMIN = 1
	USER_TYPE_ADMIN       = 2
	USER_ID_SUPER_ADMIN   = 1

	INGESTER_BODY_ORG_ID = "org-id"
)

const (
	SET_RESOURCE_TYPE_DOMAIN             = "domain"
	SET_RESOURCE_TYPE_SUB_DOMAIN         = "sub_domain"
	SET_RESOURCE_TYPE_AGENT              = "agent"
	SET_RESOURCE_TYPE_AGENT_GROUP        = "agent_group"
	SET_RESOURCE_TYPE_AGENT_GROUP_CONFIG = "agent_group_config"
	SET_RESOURCE_TYPE_DATA_SOURCE        = "datasource"
)

const TRISOLARIS_NODE_TYPE_MASTER = "master"
