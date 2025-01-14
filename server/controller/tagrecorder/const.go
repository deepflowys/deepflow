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

package tagrecorder

import (
	"github.com/deepflowio/deepflow/server/controller/common"
)

const (
	RESOURCE_TYPE_REGION            = "region"
	RESOURCE_TYPE_AZ                = "az"
	RESOURCE_TYPE_HOST              = "host"
	RESOURCE_TYPE_VPC               = "epc"
	RESOURCE_TYPE_NAT_GATEWAY       = "nat_gateway"
	RESOURCE_TYPE_LB                = "lb"
	RESOURCE_TYPE_VL2               = "vl2"
	RESOURCE_TYPE_REDIS             = "redis_instance"
	RESOURCE_TYPE_RDS               = "rds_instance"
	RESOURCE_TYPE_VM                = "vm"
	RESOURCE_TYPE_VGW               = "vgateway"
	RESOURCE_TYPE_DHCP_PORT         = "dhcp_port"
	RESOURCE_TYPE_IP                = "ip"
	RESOURCE_TYPE_POD_CLUSTER       = "pod_cluster"
	RESOURCE_TYPE_POD_NODE          = "pod_node"
	RESOURCE_TYPE_POD_NAMESPACE     = "pod_namespace"
	RESOURCE_TYPE_POD_GROUP         = "pod_group"
	RESOURCE_TYPE_POD_SERVICE       = "pod_service"
	RESOURCE_TYPE_POD               = "pod"
	RESOURCE_TYPE_INTERNET          = "internet"
	RESOURCE_TYPE_INTERNET_IP       = "internet_ip"
	RESOURCE_TYPE_VINTERFACE        = "vinterface"
	RESOURCE_TYPE_WANIP             = "wan_ip"
	RESOURCE_TYPE_LANIP             = "lan_ip"
	RESOURCE_TYPE_NAT_RULE          = "nat_rule"
	RESOURCE_TYPE_NAT_VM_CONNECTION = "nat_vm_connection"
	RESOURCE_TYPE_LB_LISTENER       = "lb_listener"
	RESOURCE_TYPE_LB_TARGET_SERVER  = "lb_target_server"
	RESOURCE_TYPE_LB_VM_CONNECTION  = "lb_vm_connection"
	RESOURCE_TYPE_POD_GROUP_PORT    = "pod_group_port"
	RESOURCE_TYPE_POD_INGRESS       = "pod_ingress"
	RESOURCE_TYPE_SERVICE           = "service"
	RESOURCE_TYPE_GPROCESS          = "gprocess"
)

const (
	RESOURCE_TYPE_CH_K8S_ANNOTATION  = "ch_k8s_annotation"
	RESOURCE_TYPE_CH_K8S_ANNOTATIONS = "ch_k8s_annotations"
	RESOURCE_TYPE_CH_K8S_ENV         = "ch_k8s_env"
	RESOURCE_TYPE_CH_K8S_ENVS        = "ch_k8s_envs"

	RESOURCE_TYPE_CH_K8S_LABEL         = "ch_k8s_label"
	RESOURCE_TYPE_CH_K8S_LABELS        = "ch_k8s_labels"
	RESOURCE_TYPE_CH_VM_CLOUD_TAG      = "ch_chost_cloud_tag"
	RESOURCE_TYPE_CH_POD_NS_CLOUD_TAG  = "ch_pod_namespace_cloud_tag"
	RESOURCE_TYPE_CH_VM_CLOUD_TAGS     = "ch_chost_cloud_tags"
	RESOURCE_TYPE_CH_POD_NS_CLOUD_TAGS = "ch_pod_namespace_cloud_tags"
	RESOURCE_TYPE_CH_OS_APP_TAG        = "ch_os_app_tag"
	RESOURCE_TYPE_CH_OS_APP_TAGS       = "ch_os_app_tags"
	RESOURCE_TYPE_CH_REGION            = "ch_region"
	RESOURCE_TYPE_CH_AZ                = "ch_az"
	RESOURCE_TYPE_CH_VPC               = "ch_vpc"
	RESOURCE_TYPE_CH_DEVICE            = "ch_device"
	RESOURCE_TYPE_CH_IP_RELATION       = "ch_ip_relation"
	RESOURCE_TYPE_CH_IP_RESOURCE       = "ch_ip_resource"
	RESOURCE_TYPE_CH_USER              = "ch_user"

	RESOURCE_TYPE_CH_NETWORK        = "ch_network"
	RESOURCE_TYPE_CH_POD            = "ch_pod"
	RESOURCE_TYPE_CH_POD_GROUP      = "ch_pod_group"
	RESOURCE_TYPE_CH_POD_GROUP_TYPE = "ch_pod_group_type"
	RESOURCE_TYPE_CH_POD_INGRESS    = "ch_pod_ingress"
	RESOURCE_TYPE_CH_POD_NAMESPACE  = "ch_pod_namespace"
	RESOURCE_TYPE_CH_POD_NODE       = "ch_pod_node"
	RESOURCE_TYPE_CH_POD_CLUSTER    = "ch_pod_cluster"
	RESOURCE_TYPE_TAP_TYPE          = "ch_tap_type"
	RESOURCE_TYPE_CH_VTAP           = "ch_vtap"
	RESOURCE_TYPE_CH_VTAP_PORT      = "ch_vtap_port"
	RESOURCE_TYPE_CH_LB_LISTENER    = "ch_lb_listener"
	RESOURCE_TYPE_CH_STRING_ENUM    = "ch_string_enum"
	RESOURCE_TYPE_CH_INT_ENUM       = "ch_int_enum"
	RESOURCE_TYPE_CH_NODE_TYPE      = "ch_node_type"
	RESOURCE_TYPE_CH_GPROCESS       = "ch_gprocess"
	RESOURCE_TYPE_CH_POD_SERVICE    = "ch_pod_service"
	RESOURCE_TYPE_CH_CHOST          = "ch_chost"
	RESOURCE_TYPE_CH_POLICY         = "ch_policy"
	RESOURCE_TYPE_CH_NPB_TUNNEL     = "ch_npb_tunnel"
	RESOURCE_TYPE_CH_ALARM_POLICY   = "ch_alarm_policy"

	RESOURCE_TYPE_CH_POD_GROUP_DEPLOYMENT            = "pod_group_deployment"
	RESOURCE_TYPE_CH_POD_GROUP_STATEFULSET           = "pod_group_statefulset"
	RESOURCE_TYPE_CH_POD_GROUP_RC                    = "pod_group_rc"
	RESOURCE_TYPE_CH_POD_GROUP_DAEMON_SET            = "pod_group_daemon_set"
	RESOURCE_TYPE_CH_POD_GROUP_REPLICASET_CONTROLLER = "pod_group_replicaset_controller"
	RESOURCE_TYPE_CH_POD_GROUP_CLONESET              = "pod_group_cloneset"

	RESOURCE_TYPE_CH_PROMETHEUS_METRIC_APP_LABEL_LAYOUT = "ch_promytheus_metric_app_label_layout"
	RESOURCE_TYPE_CH_TARGET_LABEL                       = "ch_target_label"
	RESOURCE_TYPE_CH_APP_LABEL                          = "ch_app_label"
	RESOURCE_TYPE_CH_LABEL_NAME                         = "ch_prometheus_label_name"
	RESOURCE_TYPE_CH_METRIC_NAME                        = "ch_prometheus_metric_name"
	RESOURCE_TYPE_CH_PROMETHEUS_TARGET_LABEL_LAYOUT     = "ch_prometheus_target_label_layout"
)

const (
	CH_DICTIONARY_POD_K8S_ANNOTATION          = "pod_k8s_annotation_map"
	CH_DICTIONARY_POD_K8S_ANNOTATIONS         = "pod_k8s_annotations_map"
	CH_DICTIONARY_POD_SERVICE_K8S_ANNOTATION  = "pod_service_k8s_annotation_map"
	CH_DICTIONARY_POD_SERVICE_K8S_ANNOTATIONS = "pod_service_k8s_annotations_map"
	CH_DICTIONARY_POD_K8S_ENV                 = "pod_k8s_env_map"
	CH_DICTIONARY_POD_K8S_ENVS                = "pod_k8s_envs_map"

	CH_DICTIONARY_REGION        = "region_map"
	CH_DICTIONARY_AZ            = "az_map"
	CH_DICTIONARY_DEVICE        = "device_map" // vm, host, vgw, dhcp_port, pod, pod_service, pod_node, redis, rds, lb, nat
	CH_DICTIONARY_VPC           = "l3_epc_map"
	CH_DICTIONARY_VL2           = "subnet_map"
	CH_DICTIONARY_POD_CLUSTER   = "pod_cluster_map"
	CH_DICTIONARY_POD_NAMESPACE = "pod_ns_map"
	CH_DICTIONARY_POD_NODE      = "pod_node_map"
	CH_DICTIONARY_POD_GROUP     = "pod_group_map"
	CH_DICTIONARY_POD_INGRESS   = "pod_ingress_map"
	CH_DICTIONARY_POD           = "pod_map"
	CH_DICTIONARY_VTAP_PORT     = "vtap_port_map"
	CH_DICTIONARY_TAP_TYPE      = "tap_type_map"
	CH_DICTIONARY_VTAP          = "vtap_map"
	CH_DICTIONARY_LB_LISTENER   = "lb_listener_map"
	CH_DICTIONARY_USER          = "user_map"

	CH_DICTIONARY_POD_K8S_LABEL  = "pod_k8s_label_map"
	CH_DICTIONARY_POD_K8S_LABELS = "pod_k8s_labels_map"

	CH_DICTIONARY_POD_SERVICE_K8S_LABEL  = "pod_service_k8s_label_map"
	CH_DICTIONARY_POD_SERVICE_K8S_LABELS = "pod_service_k8s_labels_map"

	CH_DICTIONARY_CHOST_CLOUD_TAG   = "chost_cloud_tag_map"
	CH_DICTIONARY_POD_NS_CLOUD_TAG  = "pod_ns_cloud_tag_map"
	CH_DICTIONARY_CHOST_CLOUD_TAGS  = "chost_cloud_tags_map"
	CH_DICTIONARY_POD_NS_CLOUD_TAGS = "pod_ns_cloud_tags_map"
	CH_DICTIONARY_OS_APP_TAG        = "os_app_tag_map"
	CH_DICTIONARY_OS_APP_TAGS       = "os_app_tags_map"

	CH_DICTIONARY_POD_NODE_PORT  = "pod_node_port_map"
	CH_DICTIONARY_POD_GROUP_PORT = "pod_group_port_map"
	CH_DICTIONARY_POD_PORT       = "pod_port_map"
	CH_DICTIONARY_DEVICE_PORT    = "device_port_map"
	CH_DICTIONARY_IP_PORT        = "ip_port_map"
	CH_DICTIONARY_SERVER_PORT    = "server_port_map"

	CH_DICTIONARY_IP_RELATION = "ip_relation_map"
	CH_DICTIONARY_IP_RESOURCE = "ip_resource_map"

	CH_DICTIONARY_POD_SERVICE = "pod_service_map"
	CH_DICTIONARY_CHOST       = "chost_map"

	CH_STRING_DICTIONARY_ENUM = "string_enum_map"
	CH_INT_DICTIONARY_ENUM    = "int_enum_map"

	CH_DICTIONARY_NODE_TYPE = "node_type_map"
	CH_DICTIONARY_GPROCESS  = "gprocess_map"

	CH_DICTIONARY_POLICY     = "policy_map"
	CH_DICTIONARY_NPB_TUNNEL = "npb_tunnel_map"

	CH_DICTIONARY_ALARM_POLICY = "alarm_policy_map"

	CH_TARGET_LABEL                       = "target_label_map"
	CH_APP_LABEL                          = "app_label_map"
	CH_PROMETHEUS_LABEL_NAME              = "prometheus_label_name_map"
	CH_PROMETHEUS_METRIC_NAME             = "prometheus_metric_name_map"
	CH_PROMETHEUS_METRIC_APP_LABEL_LAYOUT = "prometheus_metric_app_label_layout_map"
	CH_PROMETHEUS_TARGET_LABEL_LAYOUT     = "prometheus_target_label_layout_map"

	CH_APP_LABEL_LIVE_VIEW    = "app_label_live_view"
	CH_TARGET_LABEL_LIVE_VIEW = "target_label_live_view"
)

const (
	CH_DEVICE_TYPE_IP        = 64000
	CH_DEVICE_TYPE_INTERNET  = 63999
	CH_DEVICE_TYPE_GPROCESS  = 120
	CH_DEVICE_TYPE_POD_GROUP = 101
	CH_DEVICE_TYPE_SERVICE   = 102

	CH_VTAP_PORT_TYPE_TAP_MAC = 1
	CH_VTAP_PORT_TYPE_MAC     = 2
	CH_VTAP_PORT_NAME_MAX     = 10
)

var CH_IP_RESOURCE_TAGS = []string{
	"region_id", "region_name", "az_id", "az_name", "host_id", "host_name",
	"chost_id", "chost_name", "l3_epc_id", "l3_epc_name", "subnet_id", "subnet_name",
	"router_id", "router_name", "dhcpgw_id", "dhcpgw_name", "lb_id", "lb_name",
	"lb_listener_id", "lb_listener_name", "natgw_id", "natgw_name", "redis_id",
	"redis_name", "rds_id", "rds_name", "pod_cluster_id", "pod_cluster_name",
	"pod_ns_id", "pod_ns_name", "pod_node_id", "pod_node_name",
	"pod_ingress_id", "pod_ingress_name", "pod_service_id", "pod_service_name",
	"pod_group_id", "pod_group_name", "pod_id", "pod_name",
}

const (
	SOURCE_MYSQL                  = "MYSQL"
	SOURCE_POSTGRESQL             = "POSTGRESQL"
	SQL_CREATE_DICT               = "CREATE DICTIONARY %s.%s\n"
	SQL_REPLICA                   = "REPLICA (HOST '%s' PRIORITY 1)"
	SQL_SOURCE_MYSQL              = "SOURCE(%s(%sPORT %d USER '%s' PASSWORD '%s' %sDB %s TABLE %s INVALIDATE_QUERY 'select(select updated_at from %s order by updated_at desc limit 1) as updated_at'))\n"
	SQL_LIFETIME                  = "LIFETIME(MIN 30 MAX %d)\n"
	SQL_LAYOUT_FLAT               = "LAYOUT(FLAT())"
	SQL_LAYOUT_COMPLEX_KEY_HASHED = "LAYOUT(COMPLEX_KEY_HASHED())"

	CREATE_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_VL2_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64,\n" +
		"    `l3_epc_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_AZ_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_REGION_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_VPC_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `uid` String,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_TAP_TYPE_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `value` UInt64,\n" +
		"    `name` String\n" +
		")\n" +
		"PRIMARY KEY value\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_VTAP_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `type` Int64,\n" +
		"    `team_id` UInt64,\n" +
		"    `host_id` Int64,\n" +
		"    `host_name` String,\n" +
		"    `chost_id` Int64,\n" +
		"    `chost_name` String,\n" +
		"    `pod_node_id` Int64,\n" +
		"    `pod_node_name` String\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_DEVICE_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `devicetype` UInt64,\n" +
		"    `deviceid` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `uid` String,\n" +
		"    `hostname` String,\n" +
		"    `ip` String,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY devicetype, deviceid\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_VTAP_PORT_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `vtap_id` UInt64,\n" +
		"    `tap_port` UInt64,\n" +
		"    `name` String,\n" +
		"    `host_id` Int64,\n" +
		"    `host_name` String,\n" +
		"    `chost_id` Int64,\n" +
		"    `chost_name` String,\n" +
		"    `pod_node_id` Int64,\n" +
		"    `pod_node_name` String,\n" +
		"    `device_type` UInt64,\n" +
		"    `device_id` UInt64,\n" +
		"    `device_name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `team_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY vtap_id, tap_port\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_PORT_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `protocol` UInt64,\n" +
		"    `port` UInt64,\n" +
		"    `port_lb_id` UInt64,\n" +
		"    `port_lb_name` String,\n" +
		"    `port_lb_listener_id` UInt64,\n" +
		"    `port_lb_listener_name` String,\n" +
		"    `port_pod_service_id` UInt64,\n" +
		"    `port_pod_service_name` String\n" +
		")\n" +
		"PRIMARY KEY id, protocol, port\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_IP_PORT_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `ip` String,\n" +
		"    `subnet_id` UInt64,\n" +
		"    `protocol` UInt64,\n" +
		"    `port` UInt64,\n" +
		"    `port_lb_id` UInt64,\n" +
		"    `port_lb_name` String,\n" +
		"    `port_lb_listener_id` UInt64,\n" +
		"    `port_lb_listener_name` String,\n" +
		"    `port_pod_service_id` UInt64,\n" +
		"    `port_pod_service_name` String\n" +
		")\n" +
		"PRIMARY KEY ip, subnet_id, protocol, port\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_DEVICE_PORT_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `devicetype` UInt64,\n" +
		"    `deviceid` UInt64,\n" +
		"    `protocol` UInt64,\n" +
		"    `port` UInt64,\n" +
		"    `port_lb_id` UInt64,\n" +
		"    `port_lb_name` String,\n" +
		"    `port_lb_listener_id` UInt64,\n" +
		"    `port_lb_listener_name` String,\n" +
		"    `port_pod_service_id` UInt64,\n" +
		"    `port_pod_service_name` String\n" +
		")\n" +
		"PRIMARY KEY devicetype, deviceid, protocol, port\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_SERVER_PORT_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `server_port` UInt64,\n" +
		"    `server_port_name` String\n" +
		")\n" +
		"PRIMARY KEY server_port\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_IP_RELATION_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `l3_epc_id` UInt64,\n" +
		"    `ip` String,\n" +
		"    `natgw_id` UInt64,\n" +
		"    `natgw_name` String,\n" +
		"    `lb_id` UInt64,\n" +
		"    `lb_name` String,\n" +
		"    `lb_listener_id` UInt64,\n" +
		"    `lb_listener_name` String,\n" +
		"    `pod_ingress_id` UInt64,\n" +
		"    `pod_ingress_name` String,\n" +
		"    `pod_service_id` UInt64,\n" +
		"    `pod_service_name` String,\n" +
		"    `team_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY l3_epc_id, ip\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_ID_NAME_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_NPB_TUNNEL_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `team_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_POD_NODE_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64,\n" +
		"    `pod_cluster_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_POD_INGRESS_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `sub_domain_id` UInt64,\n" +
		"    `pod_cluster_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_LB_LISTENER_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `team_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_K8S_LABEL_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `key` String,\n" +
		"    `value` String,\n" +
		"    `l3_epc_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id, key\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_K8S_LABELS_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `labels` String,\n" +
		"    `l3_epc_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_IP_RESOURCE_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `ip` String,\n" +
		"    `subnet_id` UInt64,\n" +
		"    `subnet_name` String,\n" +
		"    `region_id` UInt64,\n" +
		"    `region_name` String,\n" +
		"    `az_id` UInt64,\n" +
		"    `az_name` String,\n" +
		"    `host_id` UInt64,\n" +
		"    `host_name` String,\n" +
		"    `chost_id` UInt64,\n" +
		"    `chost_name` String,\n" +
		"    `l3_epc_id` UInt64,\n" +
		"    `l3_epc_name` String,\n" +
		"    `router_id` UInt64,\n" +
		"    `router_name` String,\n" +
		"    `dhcpgw_id` UInt64,\n" +
		"    `dhcpgw_name` String,\n" +
		"    `lb_id` UInt64,\n" +
		"    `lb_name` String,\n" +
		"    `lb_listener_id` UInt64,\n" +
		"    `lb_listener_name` String,\n" +
		"    `natgw_id` UInt64,\n" +
		"    `natgw_name` String,\n" +
		"    `redis_id` UInt64,\n" +
		"    `redis_name` String,\n" +
		"    `rds_id` UInt64,\n" +
		"    `rds_name` String,\n" +
		"    `pod_cluster_id` UInt64,\n" +
		"    `pod_cluster_name` String,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `pod_ns_name` String,\n" +
		"    `pod_node_id` UInt64,\n" +
		"    `pod_node_name` String,\n" +
		"    `pod_ingress_id` UInt64,\n" +
		"    `pod_ingress_name` String,\n" +
		"    `pod_service_id` UInt64,\n" +
		"    `pod_service_name` String,\n" +
		"    `pod_group_id` UInt64,\n" +
		"    `pod_group_name` String,\n" +
		"    `pod_id` UInt64,\n" +
		"    `pod_name` String,\n" +
		"    `uid` String\n" +
		")\n" +
		"PRIMARY KEY ip, subnet_id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_STRING_ENUM_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `tag_name` String,\n" +
		"    `value` String,\n" +
		"    `name_zh` String,\n" +
		"    `name_en` String,\n" +
		"    `description_zh` String,\n" +
		"    `description_en` String\n" +
		")\n" +
		"PRIMARY KEY tag_name, value\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_INT_ENUM_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `tag_name` String,\n" +
		"    `value` UInt64,\n" +
		"    `name_zh` String,\n" +
		"    `name_en` String,\n" +
		"    `description_zh` String,\n" +
		"    `description_en` String\n" +
		")\n" +
		"PRIMARY KEY tag_name, value\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_NODE_TYPE_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `resource_type` UInt64,\n" +
		"    `node_type` String\n" +
		")\n" +
		"PRIMARY KEY resource_type\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_CLOUD_TAG_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `key` String,\n" +
		"    `value` String,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id, key\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_POD_NS_CLOUD_TAG_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `key` String,\n" +
		"    `value` String,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id, key\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_CLOUD_TAGS_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `cloud_tags` String,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_POD_NS_CLOUD_TAGS_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `cloud_tags` String,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_OS_APP_TAG_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `pid` UInt64,\n" +
		"    `key` String,\n" +
		"    `value` String,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY pid, key\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_OS_APP_TAGS_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `pid` UInt64,\n" +
		"    `os_app_tags` String,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY pid\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT

	CREATE_K8S_ANNOTATION_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `key` String,\n" +
		"    `value` String,\n" +
		"    `l3_epc_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id, key\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_K8S_ANNOTATIONS_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `annotations` String,\n" +
		"    `l3_epc_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_K8S_ENV_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `key` String,\n" +
		"    `value` String,\n" +
		"    `l3_epc_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id, key\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_K8S_ENVS_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `envs` String,\n" +
		"    `l3_epc_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_PROMETHEUS_LABEL_NAME_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_PROMETHEUS_METRIC_APP_LABEL_LAYOUT_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `metric_name` String,\n" +
		"    `app_label_name` String,\n" +
		"    `app_label_column_index` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_APP_LABEL_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `label_name_id` UInt64,\n" +
		"    `label_value` String,\n" +
		"    `label_value_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY label_name_id, label_value_id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_TARGET_LABEL_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `metric_id` UInt64,\n" +
		"    `label_name_id` UInt64,\n" +
		"    `label_value` String,\n" +
		"    `target_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY metric_id, label_name_id, target_id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_PROMETHEUS_TARGET_LABEL_LAYOUT_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `target_id` UInt64,\n" +
		"    `target_label_names` String,\n" +
		"    `target_label_values` String\n" +
		")\n" +
		"PRIMARY KEY target_id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_POD_NS_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `pod_cluster_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_POD_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `pod_cluster_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `pod_node_id` UInt64,\n" +
		"    `pod_service_id` UInt64,\n" +
		"    `pod_group_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_POD_SERVICE_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `pod_cluster_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_CHOST_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `host_id` UInt64,\n" +
		"    `l3_epc_id` UInt64,\n" +
		"    `hostname` String,\n" +
		"    `ip` String,\n" +
		"    `subnet_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_POD_GROUP_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `pod_group_type` UInt64,\n" +
		"    `icon_id` Int64,\n" +
		"    `pod_cluster_id` UInt64,\n" +
		"    `pod_ns_id` UInt64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_GPROCESS_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` UInt64,\n" +
		"    `name` String,\n" +
		"    `icon_id` Int64,\n" +
		"    `chost_id` Int64,\n" +
		"    `l3_epc_id` Int64,\n" +
		"    `team_id` UInt64,\n" +
		"    `domain_id` UInt64,\n" +
		"    `sub_domain_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
	CREATE_POLICY_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `tunnel_type` UInt64,\n" +
		"    `acl_gid` UInt64,\n" +
		"    `id` Int64,\n" +
		"    `name` String,\n" +
		"    `team_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY tunnel_type, acl_gid\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_COMPLEX_KEY_HASHED
	CREATE_AlARM_POLICY_DICTIONARY_SQL = SQL_CREATE_DICT +
		"(\n" +
		"    `id` Int64,\n" +
		"    `name` String,\n" +
		"    `user_id` Int64,\n" +
		"    `team_id` UInt64\n" +
		")\n" +
		"PRIMARY KEY id\n" +
		SQL_SOURCE_MYSQL +
		SQL_LIFETIME +
		SQL_LAYOUT_FLAT
)

const (
	CREATE_APP_LABEL_LIVE_VIEW_SQL = "CREATE LIVE VIEW %s.app_label_live_view WITH PERIODIC REFRESH %d\n" +
		"(\n" +
		"    `label_name_id` UInt64,\n" +
		"    `label_value_id` UInt64,\n" +
		"    `label_value` String\n" +
		") AS\n" +
		"SELECT *\n" +
		"FROM %s.app_label_map"
	CREATE_TARGET_LABEL_LIVE_VIEW_SQL = "CREATE LIVE VIEW %s.target_label_live_view WITH PERIODIC REFRESH %d\n" +
		"(\n" +
		"    `metric_id` UInt64,\n" +
		"    `label_name_id` UInt64,\n" +
		"    `target_id` UInt64,\n" +
		"    `label_value` String\n" +
		") AS\n" +
		"SELECT *\n" +
		"FROM %s.target_label_map"
)

var DBNodeTypeToResourceType = map[string]string{
	"region":      RESOURCE_TYPE_REGION,
	"az":          RESOURCE_TYPE_AZ,
	"host":        RESOURCE_TYPE_HOST,
	"l3_epc":      RESOURCE_TYPE_VPC,
	"subnet":      RESOURCE_TYPE_VL2,
	"vm":          RESOURCE_TYPE_VM,
	"router":      RESOURCE_TYPE_VGW,
	"dhcp_port":   RESOURCE_TYPE_DHCP_PORT,
	"nat_gateway": RESOURCE_TYPE_NAT_GATEWAY,
	"lb":          RESOURCE_TYPE_LB,
	"redis":       RESOURCE_TYPE_REDIS,
	"rds":         RESOURCE_TYPE_RDS,
	"pod_cluster": RESOURCE_TYPE_POD_CLUSTER,
	"pod_node":    RESOURCE_TYPE_POD_NODE,
	"pod_ns":      RESOURCE_TYPE_POD_NAMESPACE,
	"pod_group":   RESOURCE_TYPE_POD_GROUP,
	"pod_service": RESOURCE_TYPE_POD_SERVICE,
	"pod":         RESOURCE_TYPE_POD,
	"ip":          RESOURCE_TYPE_IP,
	"internet":    RESOURCE_TYPE_INTERNET,
	"gprocess":    RESOURCE_TYPE_GPROCESS,
}

var CREATE_SQL_MAP = map[string]string{
	CH_DICTIONARY_REGION:                 CREATE_REGION_DICTIONARY_SQL,
	CH_DICTIONARY_AZ:                     CREATE_AZ_DICTIONARY_SQL,
	CH_DICTIONARY_VPC:                    CREATE_VPC_DICTIONARY_SQL,
	CH_DICTIONARY_VL2:                    CREATE_VL2_DICTIONARY_SQL,
	CH_DICTIONARY_POD_CLUSTER:            CREATE_DICTIONARY_SQL,
	CH_DICTIONARY_POD_NAMESPACE:          CREATE_POD_NS_DICTIONARY_SQL,
	CH_DICTIONARY_POD_NODE:               CREATE_POD_NODE_DICTIONARY_SQL,
	CH_DICTIONARY_POD_GROUP:              CREATE_POD_GROUP_DICTIONARY_SQL,
	CH_DICTIONARY_POD:                    CREATE_POD_DICTIONARY_SQL,
	CH_DICTIONARY_DEVICE:                 CREATE_DEVICE_DICTIONARY_SQL,
	CH_DICTIONARY_VTAP_PORT:              CREATE_VTAP_PORT_DICTIONARY_SQL,
	CH_DICTIONARY_TAP_TYPE:               CREATE_TAP_TYPE_DICTIONARY_SQL,
	CH_DICTIONARY_VTAP:                   CREATE_VTAP_DICTIONARY_SQL,
	CH_DICTIONARY_POD_NODE_PORT:          CREATE_PORT_DICTIONARY_SQL,
	CH_DICTIONARY_POD_GROUP_PORT:         CREATE_PORT_DICTIONARY_SQL,
	CH_DICTIONARY_POD_PORT:               CREATE_PORT_DICTIONARY_SQL,
	CH_DICTIONARY_DEVICE_PORT:            CREATE_DEVICE_PORT_DICTIONARY_SQL,
	CH_DICTIONARY_IP_PORT:                CREATE_IP_PORT_DICTIONARY_SQL,
	CH_DICTIONARY_SERVER_PORT:            CREATE_SERVER_PORT_DICTIONARY_SQL,
	CH_DICTIONARY_IP_RELATION:            CREATE_IP_RELATION_DICTIONARY_SQL,
	CH_DICTIONARY_LB_LISTENER:            CREATE_LB_LISTENER_DICTIONARY_SQL,
	CH_DICTIONARY_POD_INGRESS:            CREATE_POD_INGRESS_DICTIONARY_SQL,
	CH_DICTIONARY_POD_K8S_LABEL:          CREATE_K8S_LABEL_DICTIONARY_SQL,
	CH_DICTIONARY_POD_K8S_LABELS:         CREATE_K8S_LABELS_DICTIONARY_SQL,
	CH_DICTIONARY_IP_RESOURCE:            CREATE_IP_RESOURCE_DICTIONARY_SQL,
	CH_DICTIONARY_NODE_TYPE:              CREATE_NODE_TYPE_DICTIONARY_SQL,
	CH_STRING_DICTIONARY_ENUM:            CREATE_STRING_ENUM_SQL,
	CH_INT_DICTIONARY_ENUM:               CREATE_INT_ENUM_SQL,
	CH_DICTIONARY_CHOST_CLOUD_TAG:        CREATE_CLOUD_TAG_DICTIONARY_SQL,
	CH_DICTIONARY_POD_NS_CLOUD_TAG:       CREATE_POD_NS_CLOUD_TAG_DICTIONARY_SQL,
	CH_DICTIONARY_CHOST_CLOUD_TAGS:       CREATE_CLOUD_TAGS_DICTIONARY_SQL,
	CH_DICTIONARY_POD_NS_CLOUD_TAGS:      CREATE_POD_NS_CLOUD_TAGS_DICTIONARY_SQL,
	CH_DICTIONARY_OS_APP_TAG:             CREATE_OS_APP_TAG_DICTIONARY_SQL,
	CH_DICTIONARY_OS_APP_TAGS:            CREATE_OS_APP_TAGS_DICTIONARY_SQL,
	CH_DICTIONARY_GPROCESS:               CREATE_GPROCESS_DICTIONARY_SQL,
	CH_DICTIONARY_POD_SERVICE_K8S_LABEL:  CREATE_K8S_LABEL_DICTIONARY_SQL,
	CH_DICTIONARY_POD_SERVICE_K8S_LABELS: CREATE_K8S_LABELS_DICTIONARY_SQL,
	CH_DICTIONARY_USER:                   CREATE_ID_NAME_DICTIONARY_SQL,

	CH_DICTIONARY_POD_K8S_ANNOTATION:          CREATE_K8S_ANNOTATION_DICTIONARY_SQL,
	CH_DICTIONARY_POD_K8S_ANNOTATIONS:         CREATE_K8S_ANNOTATIONS_DICTIONARY_SQL,
	CH_DICTIONARY_POD_SERVICE_K8S_ANNOTATION:  CREATE_K8S_ANNOTATION_DICTIONARY_SQL,
	CH_DICTIONARY_POD_SERVICE_K8S_ANNOTATIONS: CREATE_K8S_ANNOTATIONS_DICTIONARY_SQL,
	CH_DICTIONARY_POD_K8S_ENV:                 CREATE_K8S_ENV_DICTIONARY_SQL,
	CH_DICTIONARY_POD_K8S_ENVS:                CREATE_K8S_ENVS_DICTIONARY_SQL,
	CH_DICTIONARY_POD_SERVICE:                 CREATE_POD_SERVICE_DICTIONARY_SQL,
	CH_DICTIONARY_CHOST:                       CREATE_CHOST_DICTIONARY_SQL,

	CH_DICTIONARY_POLICY:     CREATE_POLICY_DICTIONARY_SQL,
	CH_DICTIONARY_NPB_TUNNEL: CREATE_NPB_TUNNEL_DICTIONARY_SQL,

	CH_DICTIONARY_ALARM_POLICY: CREATE_AlARM_POLICY_DICTIONARY_SQL,

	CH_PROMETHEUS_LABEL_NAME:              CREATE_PROMETHEUS_LABEL_NAME_DICTIONARY_SQL,
	CH_PROMETHEUS_METRIC_NAME:             CREATE_PROMETHEUS_LABEL_NAME_DICTIONARY_SQL,
	CH_PROMETHEUS_METRIC_APP_LABEL_LAYOUT: CREATE_PROMETHEUS_METRIC_APP_LABEL_LAYOUT_DICTIONARY_SQL,
	CH_APP_LABEL:                          CREATE_APP_LABEL_SQL,
	CH_TARGET_LABEL:                       CREATE_TARGET_LABEL_SQL,
	CH_PROMETHEUS_TARGET_LABEL_LAYOUT:     CREATE_PROMETHEUS_TARGET_LABEL_LAYOUT_DICTIONARY_SQL,

	CH_APP_LABEL_LIVE_VIEW:    CREATE_APP_LABEL_LIVE_VIEW_SQL,
	CH_TARGET_LABEL_LIVE_VIEW: CREATE_TARGET_LABEL_LIVE_VIEW_SQL,
}

var VTAP_TYPE_TO_DEVICE_TYPE = map[int]int{
	common.VTAP_TYPE_KVM:         common.VIF_DEVICE_TYPE_HOST,
	common.VTAP_TYPE_ESXI:        common.VIF_DEVICE_TYPE_HOST,
	common.VTAP_TYPE_WORKLOAD_V:  common.VIF_DEVICE_TYPE_VM,
	common.VTAP_TYPE_WORKLOAD_P:  common.VIF_DEVICE_TYPE_VM,
	common.VTAP_TYPE_POD_HOST:    common.VIF_DEVICE_TYPE_POD_NODE,
	common.VTAP_TYPE_POD_VM:      common.VIF_DEVICE_TYPE_POD_NODE,
	common.VTAP_TYPE_HYPER_V:     common.VIF_DEVICE_TYPE_HOST,
	common.VTAP_TYPE_K8S_SIDECAR: common.VIF_DEVICE_TYPE_POD,
}

var RESOURCE_TYPE_TO_NODE_TYPE = map[int]string{
	common.VIF_DEVICE_TYPE_VM:                              "chost",
	common.VIF_DEVICE_TYPE_VROUTER:                         "router",
	common.VIF_DEVICE_TYPE_HOST:                            RESOURCE_TYPE_HOST,
	common.VIF_DEVICE_TYPE_DHCP_PORT:                       "dhcpgw",
	common.VIF_DEVICE_TYPE_POD:                             RESOURCE_TYPE_POD,
	common.VIF_DEVICE_TYPE_POD_SERVICE:                     RESOURCE_TYPE_POD_SERVICE,
	common.VIF_DEVICE_TYPE_REDIS_INSTANCE:                  "redis",
	common.VIF_DEVICE_TYPE_RDS_INSTANCE:                    "rds",
	common.VIF_DEVICE_TYPE_POD_NODE:                        RESOURCE_TYPE_POD_NODE,
	common.VIF_DEVICE_TYPE_POD_CLUSTER:                     RESOURCE_TYPE_POD_CLUSTER,
	common.VIF_DEVICE_TYPE_LB:                              RESOURCE_TYPE_LB,
	common.VIF_DEVICE_TYPE_NAT_GATEWAY:                     "natgw",
	common.VIF_DEVICE_TYPE_INTERNET:                        RESOURCE_TYPE_INTERNET_IP,
	common.VIF_DEVICE_TYPE_POD_GROUP:                       RESOURCE_TYPE_POD_GROUP,
	common.VIF_DEVICE_TYPE_SERVICE:                         RESOURCE_TYPE_SERVICE,
	common.VIF_DEVICE_TYPE_GPROCESS:                        RESOURCE_TYPE_GPROCESS,
	common.VIF_DEVICE_TYPE_POD_GROUP_DEPLOYMENT:            RESOURCE_TYPE_POD_GROUP,
	common.VIF_DEVICE_TYPE_POD_GROUP_STATEFULSET:           RESOURCE_TYPE_POD_GROUP,
	common.VIF_DEVICE_TYPE_POD_GROUP_RC:                    RESOURCE_TYPE_POD_GROUP,
	common.VIF_DEVICE_TYPE_POD_GROUP_DAEMON_SET:            RESOURCE_TYPE_POD_GROUP,
	common.VIF_DEVICE_TYPE_POD_GROUP_REPLICASET_CONTROLLER: RESOURCE_TYPE_POD_GROUP,
	common.VIF_DEVICE_TYPE_POD_GROUP_CLONESET:              RESOURCE_TYPE_POD_GROUP,
	common.VIF_DEVICE_TYPE_IP:                              RESOURCE_TYPE_IP,
}

var RESOURCE_POD_GROUP_TYPE_MAP = map[int]int{
	common.POD_GROUP_DEPLOYMENT:            common.VIF_DEVICE_TYPE_POD_GROUP_DEPLOYMENT,
	common.POD_GROUP_STATEFULSET:           common.VIF_DEVICE_TYPE_POD_GROUP_STATEFULSET,
	common.POD_GROUP_RC:                    common.VIF_DEVICE_TYPE_POD_GROUP_RC,
	common.POD_GROUP_DAEMON_SET:            common.VIF_DEVICE_TYPE_POD_GROUP_DAEMON_SET,
	common.POD_GROUP_REPLICASET_CONTROLLER: common.VIF_DEVICE_TYPE_POD_GROUP_REPLICASET_CONTROLLER,
	common.POD_GROUP_CLONESET:              common.VIF_DEVICE_TYPE_POD_GROUP_CLONESET,
}

const TrisolarisNodeTypeMaster = "master"

var SUB_DOMAIN_RESOURCE_TYPES = []string{
	common.RESOURCE_TYPE_POD_SERVICE_EN, common.RESOURCE_TYPE_POD_EN, common.RESOURCE_TYPE_POD_GROUP_EN,
	common.RESOURCE_TYPE_POD_NODE_EN, common.RESOURCE_TYPE_POD_CLUSTER_EN, common.RESOURCE_TYPE_PROCESS_EN,
	common.RESOURCE_TYPE_POD_INGRESS_EN, common.RESOURCE_TYPE_POD_NAMESPACE_EN, common.RESOURCE_TYPE_NETWORK_EN,
}
