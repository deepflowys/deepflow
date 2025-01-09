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

package clickhouse

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"bou.ke/monkey"
	"github.com/jarcoal/httpmock"

	//"github.com/k0kubun/pp"

	"github.com/deepflowio/deepflow/server/querier/common"
	"github.com/deepflowio/deepflow/server/querier/config"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/client"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/metrics"
	"github.com/deepflowio/deepflow/server/querier/parse"
)

/* var (
	 parsetest = []struct {
		 input string
	 }{{
		 input: "select Rspread(byte) as rspread_byte from l4_flow_log",
	 }}
 ) */

var (
	parseSQL = []struct {
		name       string
		input      string
		output     []string
		db         string
		datasource string
		wantErr    string
	}{{
		input:  "select byte from l4_flow_log limit 1",
		output: []string{"SELECT byte_tx+byte_rx AS `byte` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Sum(byte)/Time_interval as sum_byte, time(time, 120) as time_120 from l4_flow_log group by time_120 having Sum(byte)>=0 limit 10 offset 20",
		output: []string{"WITH toStartOfInterval(time, toIntervalSecond(120)) + toIntervalSecond(arrayJoin([0]) * 120) AS `_time_120` SELECT toUnixTimestamp(`_time_120`) AS `time_120`, divide(SUM(byte_tx+byte_rx), 120) AS `sum_byte` FROM flow_log.`l4_flow_log` GROUP BY `time_120` HAVING SUM(byte_tx+byte_rx) >= 0 LIMIT 20, 10"},
	}, {
		input:  "select Sum(log_count) as sum_log_count from l4_flow_log order by sum_log_count desc limit 1",
		output: []string{"SELECT SUM(1) AS `sum_log_count` FROM flow_log.`l4_flow_log` ORDER BY `sum_log_count` desc LIMIT 1"},
	}, {
		input:  "select Uniq(ip_0) as uniq_ip_0 from l4_flow_log limit 1",
		output: []string{"SELECT uniq((is_ipv4, ip4_0, ip6_0)) AS `uniq_ip_0` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Uniq(ip_0, region_0, region_id_0) as uniq_0 from l4_flow_log limit 1",
		output: []string{"SELECT uniq((is_ipv4, ip4_0, ip6_0, region_id_0, region_id_0)) AS `uniq_0` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Max(byte) as max_byte, Sum(log_count) as sum_log_count from l4_flow_log having Sum(byte)>=0 limit 1",
		output: []string{"SELECT MAX(byte_tx+byte_rx) AS `max_byte`, SUM(1) AS `sum_log_count` FROM flow_log.`l4_flow_log` HAVING SUM(byte_tx+byte_rx) >= 0 LIMIT 1"},
	}, {
		input:  "select (Max(byte_tx) + Sum(byte_tx))/1 as max_byte_tx from l4_flow_log limit 1",
		output: []string{"SELECT divide(plus(MAX(byte_tx), SUM(byte_tx)), 1) AS `max_byte_tx` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select AAvg(byte_tx) as aavg_byte_tx from l4_flow_log where `time`>=60 and `time`<=180 having Spread(byte_tx)>=0 limit 1",
		output: []string{"SELECT AVG(byte_tx) AS `aavg_byte_tx` FROM flow_log.`l4_flow_log` WHERE `time` >= 60 AND `time` <= 180 HAVING minus(MAX(byte_tx), MIN(byte_tx)) >= 0 LIMIT 1"},
	}, {
		input:  "select Avg(byte_tx) as avg_byte_tx from l4_flow_log where `time`>=60 and `time`<=180 having Spread(byte_tx)>=0 limit 1",
		output: []string{"SELECT sum(byte_tx)/(121/1) AS `avg_byte_tx` FROM flow_log.`l4_flow_log` WHERE `time` >= 60 AND `time` <= 180 HAVING minus(MAX(byte_tx), MIN(byte_tx)) >= 0 LIMIT 1"},
	}, {
		input:  "select Stddev(byte_tx) as stddev_byte_tx from l4_flow_log limit 1",
		output: []string{"SELECT stddevPopStable(byte_tx) AS `stddev_byte_tx` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Max(byte_tx) as max_byte_tx from l4_flow_log order by max_byte_tx limit 1",
		output: []string{"SELECT MAX(byte_tx) AS `max_byte_tx` FROM flow_log.`l4_flow_log` ORDER BY `max_byte_tx` asc LIMIT 1"},
	}, {
		input:  "select Spread(byte_tx) as spread_byte_tx from l4_flow_log where `time`>=60 and `time`<=180 limit 1",
		output: []string{"SELECT minus(MAX(byte_tx), MIN(byte_tx)) AS `spread_byte_tx` FROM flow_log.`l4_flow_log` WHERE `time` >= 60 AND `time` <= 180 LIMIT 1"},
	}, {
		input:  "select Rspread(byte_tx) as rspread_byte_tx from l4_flow_log where `time`>=60 and `time`<=180 limit 1",
		output: []string{"SELECT divide(MAX(byte_tx)+1e-15, MIN(byte_tx)+1e-15) AS `rspread_byte_tx` FROM flow_log.`l4_flow_log` WHERE `time` >= 60 AND `time` <= 180 LIMIT 1"},
	}, {
		input:  "select Rspread(rtt) as rspread_rtt from l4_flow_log limit 1",
		output: []string{"SELECT divide(MAXIf(rtt, rtt > 0)+1e-15, MINIf(rtt, rtt > 0)+1e-15) AS `rspread_rtt` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Percentile(byte_tx, 50) as percentile_byte_tx from l4_flow_log limit 1",
		output: []string{"SELECT quantile(50)(byte_tx) AS `percentile_byte_tx` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Avg(rtt) as avg_rtt from l4_flow_log where time >= 100+1 and time <= 102 limit 1",
		output: []string{"SELECT AVGIf(rtt, rtt > 0) AS `avg_rtt` FROM flow_log.`l4_flow_log` WHERE `time` >= 100 + 1 AND `time` <= 102 LIMIT 1"},
	}, {
		input:  "select AAvg(rtt) as aavg_rtt from l4_flow_log where time >= 100+1 and time <= 102 limit 1",
		output: []string{"SELECT AVGIf(rtt, rtt > 0) AS `aavg_rtt` FROM flow_log.`l4_flow_log` WHERE `time` >= 100 + 1 AND `time` <= 102 LIMIT 1"},
	}, {
		input:  "select Max(byte_tx) as max_byte_tx, Avg(rtt) as avg_rtt from l4_flow_log limit 1",
		output: []string{"SELECT MAX(byte_tx) AS `max_byte_tx`, AVGIf(rtt, rtt > 0) AS `avg_rtt` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Max(byte_tx) as max_byte_tx, AAvg(rtt) as aavg_rtt from l4_flow_log limit 1",
		output: []string{"SELECT MAX(byte_tx) AS `max_byte_tx`, AVGIf(rtt, rtt > 0) AS `aavg_rtt` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select ((Max(byte_tx))+Avg(rtt ))/(1-Avg(rtt )) as avg_rtt from l4_flow_log limit 1",
		output: []string{"SELECT divide(plus(MAX(byte_tx), AVGIf(rtt, rtt > 0)), minus(1, AVGIf(rtt, rtt > 0))) AS `avg_rtt` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select ((Max(byte_tx))+AAvg(rtt ))/(1-AAvg(rtt )) as aavg_rtt from l4_flow_log limit 1",
		output: []string{"SELECT divide(plus(MAX(byte_tx), AVGIf(rtt, rtt > 0)), minus(1, AVGIf(rtt, rtt > 0))) AS `aavg_rtt` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Apdex(rtt, 100) as apdex_rtt_100 from l4_flow_log limit 1",
		output: []string{"WITH if(COUNT()>0, divide(plus(SUM(if(rtt<=100,1,0)), SUM(if(100<rtt AND rtt<=100*4,0.5,0))), COUNT()), null) AS `divide_0diveider_as_null_plus_apdex_satisfy_rtt_100_apdex_toler_rtt_100_count_` SELECT `divide_0diveider_as_null_plus_apdex_satisfy_rtt_100_apdex_toler_rtt_100_count_`*100 AS `apdex_rtt_100` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Max(byte) as max_byte, time(time,120) as time_120 from l4_flow_log group by time_120 having Min(byte)>=0 limit 1",
		output: []string{"WITH toStartOfInterval(time, toIntervalSecond(120)) + toIntervalSecond(arrayJoin([0]) * 120) AS `_time_120` SELECT toUnixTimestamp(`_time_120`) AS `time_120`, MAX(byte_tx+byte_rx) AS `max_byte` FROM flow_log.`l4_flow_log` GROUP BY `time_120` HAVING MIN(byte_tx+byte_rx) >= 0 LIMIT 1"},
	}, {
		input:  "select Max(byte) as max_byte, time(time,86400) as time_120 from l4_flow_log group by time_120 having Min(byte)>=0 limit 1",
		output: []string{"WITH toStartOfInterval(time, toIntervalDay(1)) + toIntervalDay(arrayJoin([0]) * 1) AS `_time_120` SELECT toUnixTimestamp(`_time_120`) AS `time_120`, MAX(byte_tx+byte_rx) AS `max_byte` FROM flow_log.`l4_flow_log` GROUP BY `time_120` HAVING MIN(byte_tx+byte_rx) >= 0 LIMIT 1"},
	}, {
		input:  "select Max(byte) as 'max_byte',region_0,chost_1,lb_1 from l4_flow_log group by region_0,chost_1,lb_1 limit 1",
		output: []string{"WITH if(l3_device_type_1 = 1, l3_device_type_1, 0) AS `device_type_chost_1`, if(l3_device_type_1 = 15, l3_device_type_1, 0) AS `device_type_lb_1` SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, dictGet('flow_tag.device_map', 'name', (toUInt64(device_type_chost_1),toUInt64(l3_device_id_1))) AS `chost_1`, device_type_chost_1, dictGet('flow_tag.device_map', 'name', (toUInt64(device_type_lb_1),toUInt64(l3_device_id_1))) AS `lb_1`, device_type_lb_1, MAX(byte_tx+byte_rx) AS `max_byte` FROM flow_log.`l4_flow_log` WHERE (l3_device_id_1!=0 AND l3_device_type_1=1) AND (l3_device_id_1!=0 AND l3_device_type_1=15) GROUP BY `region_id_0`, `l3_device_id_1`, `device_type_chost_1`, `device_type_lb_1` LIMIT 1"},
	}, {
		input:  "select Percentage(Max(byte)+100,100) as percentage_max_byte_100 from l4_flow_log limit 1",
		output: []string{"SELECT divide(plus(MAX(byte_tx+byte_rx), 100), 100)*100 AS `percentage_max_byte_100` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		input:  "select Sum(rtt) as sum_rtt from l4_flow_log having Percentage(Max(byte), 100) >= 1 limit 1",
		output: []string{"SELECT SUMIf(rtt, rtt > 0) AS `sum_rtt` FROM flow_log.`l4_flow_log` HAVING divide(MAX(byte_tx+byte_rx), 100)*100 >= 1 LIMIT 1"},
	}, {
		input:  "select time(time, 60) as toi, PerSecond(Sum(byte)+100) as persecond_max_byte_100 from l4_flow_log group by toi limit 1",
		output: []string{"WITH toStartOfInterval(time, toIntervalSecond(60)) + toIntervalSecond(arrayJoin([0]) * 60) AS `_toi` SELECT toUnixTimestamp(`_toi`) AS `toi`, divide(plus(SUM(byte_tx+byte_rx), 100), 60) AS `persecond_max_byte_100` FROM flow_log.`l4_flow_log` GROUP BY `toi` LIMIT 1"},
	}, {
		input:  "select auto_instance_0,ip_0 from l7_flow_log where ip_0='1.1.1.1' and auto_instance_0='abc' and auto_instance_0 regexp 'abc' and auto_instance_id_0=2 group by auto_instance_0,ip_0",
		output: []string{"WITH if(auto_instance_type_0 IN (0, 255), if(is_ipv4 = 1, ip4_0, NULL), NULL) AS `auto_instance_ip4_0`, if(auto_instance_type_0 IN (0, 255), if(is_ipv4 = 0, ip6_0, NULL), NULL) AS `auto_instance_ip6_0` SELECT if(auto_instance_type_0 in (0,255),if(is_ipv4=1, IPv4NumToString(auto_instance_ip4_0), IPv6NumToString(auto_instance_ip6_0)),dictGet('flow_tag.device_map', 'name', (toUInt64(auto_instance_type_0),toUInt64(auto_instance_id_0)))) AS `auto_instance_0`, auto_instance_ip4_0, auto_instance_ip6_0, auto_instance_type_0, if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)) AS `ip_0` FROM flow_log.`l7_flow_log` WHERE (((if(is_ipv4=1, ip4_0 = toIPv4OrNull('1.1.1.1'), ip6_0 = toIPv6OrNull('1.1.1.1'))))) AND (if(auto_instance_type_0 in (0,255),if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)) = 'abc',(toUInt64(auto_instance_id_0),toUInt64(auto_instance_type_0)) GLOBAL IN (SELECT deviceid,devicetype FROM flow_tag.device_map WHERE name = 'abc'))) AND (if(auto_instance_type_0 in (0,255),match(if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)),'abc'),(toUInt64(auto_instance_id_0),toUInt64(auto_instance_type_0)) GLOBAL IN (SELECT deviceid,devicetype FROM flow_tag.device_map WHERE match(name,'abc')))) AND (if(auto_instance_type_0 in (0,255),subnet_id_0 = 2,auto_instance_id_0 = 2)) GROUP BY `is_ipv4`, `auto_instance_ip4_0`, `auto_instance_ip6_0`, `auto_instance_type_0`, `auto_instance_id_0`, `ip4_0`, `ip6_0` LIMIT 10000"},
	}, {
		input:  "select pod_service_0 from l7_flow_log where pod_service_0 !='xx' group by pod_service_0",
		output: []string{"SELECT dictGet('flow_tag.device_map', 'name', (toUInt64(11),toUInt64(service_id_0))) AS `pod_service_0` FROM flow_log.`l7_flow_log` WHERE (not(toUInt64(service_id_0) GLOBAL IN (SELECT deviceid FROM flow_tag.device_map WHERE name = 'xx' AND devicetype=11))) AND (service_id_0!=0) GROUP BY `service_id_0` LIMIT 10000"},
	}, {
		input:  "select pod_service_id_0 from l7_flow_log where pod_service_id_0 !=3 group by pod_service_id_0",
		output: []string{"SELECT service_id_0 AS `pod_service_id_0` FROM flow_log.`l7_flow_log` WHERE (not(service_id_0 = 3)) AND (service_id_0!=0) GROUP BY `service_id_0` LIMIT 10000"},
	}, {
		input:  "select region_id_0 from l7_flow_log where pod_ingress_0 !='xx' group by region_id_0",
		output: []string{"SELECT region_id_0 FROM flow_log.`l7_flow_log` WHERE (not(((if(is_ipv4=1,IPv4NumToString(ip4_0),IPv6NumToString(ip6_0)),toUInt64(l3_epc_id_0)) GLOBAL IN (SELECT ip,l3_epc_id FROM flow_tag.ip_relation_map WHERE pod_ingress_name = 'xx')) OR (toUInt64(service_id_0) GLOBAL IN (SELECT pod_service_id FROM flow_tag.ip_relation_map WHERE pod_ingress_name = 'xx')))) GROUP BY `region_id_0` LIMIT 10000"},
	}, {
		input:  "select node_type(region_0) as `node_type_0`,mask(ip_0,33) as `mask_ip_0` from l7_flow_log group by `mask_ip_0`,`node_type_0`",
		output: []string{"WITH if(is_ipv4, IPv4NumToString(bitAnd(ip4_0, 4294967295)), IPv6NumToString(bitAnd(ip6_0, toFixedString(unhex('ffffffff800000000000000000000000'), 16)))) AS `mask_ip_0` SELECT 'region' AS `node_type_0`, `mask_ip_0` FROM flow_log.`l7_flow_log` GROUP BY `mask_ip_0` LIMIT 10000"},
	}, {
		input:  "select region_id_0 from l7_flow_log group by region_id_0,chost_id_1",
		output: []string{"SELECT region_id_0, if(l3_device_type_1=1,l3_device_id_1, 0) AS `chost_id_1` FROM flow_log.`l7_flow_log` WHERE (l3_device_id_1!=0 AND l3_device_type_1=1) GROUP BY `region_id_0`, `chost_id_1` LIMIT 10000"},
	}, {
		input:  "SELECT ip_0 FROM l4_flow_log WHERE  ((is_internet_1=1) OR (is_internet_0=1)) GROUP BY ip_0 limit 1",
		output: []string{"SELECT if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)) AS `ip_0` FROM flow_log.`l4_flow_log` WHERE (((l3_epc_id_1 = -2)) OR ((l3_epc_id_0 = -2))) GROUP BY `is_ipv4`, `ip4_0`, `ip6_0` LIMIT 1"},
	}, {
		input:  "select Sum(byte) as `流量总量`, region_0 as `区域` from l4_flow_log where 1=1 group by `区域` order by `流量总量` desc",
		output: []string{"SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `区域`, SUM(byte_tx+byte_rx) AS `流量总量` FROM flow_log.`l4_flow_log` WHERE 1 = 1 GROUP BY `区域` ORDER BY `流量总量` desc LIMIT 10000"},
	}, {
		input:  "select byte as `123` from l4_flow_log where 1=1 group by `123` order by `123` limit 1 ",
		output: []string{"SELECT byte_tx+byte_rx AS `123` FROM flow_log.`l4_flow_log` WHERE 1 = 1 GROUP BY `123` ORDER BY `123` asc LIMIT 1"},
	}, {
		input:  "select byte from l4_flow_log where ip>=('1.1.1.1/24','2.2.2.2') and ip<='::/24'",
		output: []string{"SELECT byte_tx+byte_rx AS `byte` FROM flow_log.`l4_flow_log` WHERE (((if(is_ipv4=1, ip4 >= toIPv4OrNull('1.1.1.255'), ip6 >= toIPv6OrNull('1.1.1.255'))) OR (if(is_ipv4=1, ip4 >= toIPv4OrNull('2.2.2.2'), ip6 >= toIPv6OrNull('2.2.2.2'))))) AND (((if(is_ipv4=1, ip4 <= toIPv4OrNull('::'), ip6 <= toIPv6OrNull('::'))))) LIMIT 10000"},
	}, {
		input:  "select `k8s.label.statefulset.kubernetes.io/pod-name_0` from l4_flow_log where `k8s.label.statefulset.kubernetes.io/pod-name_0`='opensource-loki-0' group by `k8s.label.statefulset.kubernetes.io/pod-name_0`",
		output: []string{"SELECT if(dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(service_id_0),'statefulset.kubernetes.io/pod-name'))!='', dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(service_id_0),'statefulset.kubernetes.io/pod-name')), dictGet('flow_tag.pod_k8s_label_map', 'value', (toUInt64(pod_id_0),'statefulset.kubernetes.io/pod-name')) ) AS `k8s.label.statefulset.kubernetes.io/pod-name_0` FROM flow_log.`l4_flow_log` WHERE ((toUInt64(service_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_label_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name')) OR (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_label_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name'))) AND (((toUInt64(service_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_label_map WHERE key='statefulset.kubernetes.io/pod-name')) OR (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_label_map WHERE key='statefulset.kubernetes.io/pod-name')))) GROUP BY `k8s.label.statefulset.kubernetes.io/pod-name_0` LIMIT 10000"},
	}, {
		input:  "select `k8s.label.statefulset.kubernetes.io/pod-name_0` as `k8s.label.abc` from l4_flow_log where `k8s.label.abc`='opensource-loki-0' group by `k8s.label.abc`",
		output: []string{"SELECT if(dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(service_id_0),'statefulset.kubernetes.io/pod-name'))!='', dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(service_id_0),'statefulset.kubernetes.io/pod-name')), dictGet('flow_tag.pod_k8s_label_map', 'value', (toUInt64(pod_id_0),'statefulset.kubernetes.io/pod-name')) ) AS `k8s.label.abc` FROM flow_log.`l4_flow_log` WHERE ((toUInt64(service_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_label_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name')) OR (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_label_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name'))) AND (((toUInt64(service_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_label_map WHERE key='statefulset.kubernetes.io/pod-name')) OR (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_label_map WHERE key='statefulset.kubernetes.io/pod-name')))) GROUP BY `k8s.label.abc` LIMIT 10000"},
	}, {
		input:  "select `attribute.cc` as `attribute.abc` from l7_flow_log where `attribute.abc`='opensource-loki-0' group by `attribute.abc`",
		output: []string{"SELECT attribute_values[indexOf(attribute_names,'cc')] AS `attribute.abc` FROM flow_log.`l7_flow_log` WHERE attribute_values[indexOf(attribute_names,'cc')] = 'opensource-loki-0' AND (attribute_values[indexOf(attribute_names,'cc')] != '') GROUP BY `attribute.abc` LIMIT 10000"},
	}, {
		input:  "select `tag.cc` as `tag.abc` from cpu where `tag.abc`='opensource-loki-0' group by `tag.abc`",
		output: []string{"SELECT tag_values[indexOf(tag_names,'cc')] AS `tag.abc` FROM ext_metrics.`metrics` WHERE (virtual_table_name='cpu') AND tag_values[indexOf(tag_names,'cc')] = 'opensource-loki-0' AND (tag_values[indexOf(tag_names,'cc')] != '') GROUP BY `tag.abc` LIMIT 10000"},
		db:     "ext_metrics",
	}, {
		input:  "select `metrics.storageclass_annotations` AS `job_info` from prometheus_kube",
		output: []string{"SELECT if(indexOf(metrics_float_names, 'storageclass_annotations')=0,null,metrics_float_values[indexOf(metrics_float_names, 'storageclass_annotations')]) AS `job_info` FROM ext_metrics.`metrics` WHERE (virtual_table_name='prometheus_kube') LIMIT 10000"},
		db:     "ext_metrics",
	}, {
		input:  "select Sum(`metrics.pending`) from `deepflow_server.queue`",
		output: []string{"SELECT SUM(if(indexOf(metrics_float_names, 'pending')=0,null,metrics_float_values[indexOf(metrics_float_names, 'pending')])) AS `Sum(metrics.pending)` FROM deepflow_tenant.`deepflow_collector` WHERE (virtual_table_name='deepflow_server.queue') LIMIT 10000"},
		db:     "deepflow_tenant",
	}, {
		input:  "select `k8s.label_0` from l7_flow_log",
		output: []string{"SELECT if(dictGetOrDefault('flow_tag.pod_service_k8s_labels_map', 'labels', toUInt64(service_id_0),'{}')!='{}', dictGetOrDefault('flow_tag.pod_service_k8s_labels_map', 'labels', toUInt64(service_id_0),'{}'), dictGetOrDefault('flow_tag.pod_k8s_labels_map', 'labels', toUInt64(pod_id_0),'{}'))  AS `k8s.label_0` FROM flow_log.`l7_flow_log` LIMIT 10000"},
	}, {
		input:  "select `metrics.xxx.yyy` as xxx from l7_flow_log",
		output: []string{"SELECT if(indexOf(metrics_names, 'xxx.yyy')=0,null,metrics_values[indexOf(metrics_names, 'xxx.yyy')]) AS `xxx` FROM flow_log.`l7_flow_log` LIMIT 10000"},
	}, {
		input:  "select `metrics.xxx` as xxx from cpu",
		output: []string{"SELECT if(indexOf(metrics_float_names, 'xxx')=0,null,metrics_float_values[indexOf(metrics_float_names, 'xxx')]) AS `xxx` FROM ext_metrics.`metrics` WHERE (virtual_table_name='cpu') LIMIT 10000"},
		db:     "ext_metrics",
	}, {
		input:  "select Percentile(`metrics.xxx`, 0.9) as xxx from cpu",
		output: []string{"SELECT quantile(0.9)(`_sum_if(indexOf(metrics_float_names, xxx)=0,null,metrics_float_values[indexOf(metrics_float_names, xxx)])`) AS `xxx` FROM (SELECT SUM(if(indexOf(metrics_float_names, 'xxx')=0,null,metrics_float_values[indexOf(metrics_float_names, 'xxx')])) AS `_sum_if(indexOf(metrics_float_names, xxx)=0,null,metrics_float_values[indexOf(metrics_float_names, xxx)])` FROM ext_metrics.`metrics` WHERE (virtual_table_name='cpu')) LIMIT 10000"},
		db:     "ext_metrics",
	}, {
		input:  "select Sum(packet_count) as count from l4_packet",
		output: []string{"SELECT SUM(packet_count) AS `count` FROM flow_log.`l4_packet` LIMIT 10000"},
	}, {
		input:  "select Sum(packet_count) as count from l7_packet",
		output: []string{"SELECT SUM(packet_count) AS `count` FROM flow_log.`l7_packet` LIMIT 10000"},
	}, {
		input:  "select Sum(byte_tx) as max_byte from l4_flow_log order by length(tap_side) desc, `length(tap_side)`",
		output: []string{"SELECT SUM(byte_tx) AS `max_byte` FROM flow_log.`l4_flow_log` ORDER BY length(tap_side) desc,`length(tap_side)` asc LIMIT 10000"},
	}, {
		input:  "select Enum(tap_side) from l7_flow_log limit 0, 50",
		output: []string{"WITH dictGetOrDefault('flow_tag.string_enum_map', 'name_en', ('observation_point',observation_point), observation_point) AS `Enum(tap_side)` SELECT `Enum(tap_side)` FROM flow_log.`l7_flow_log` LIMIT 0, 50"},
	}, {
		input:  "select AAvg(`byte_tx`) AS `AAvg(byte_tx)`,icon_id(chost_0) as `xx`,region_0 from vtap_flow_edge_port where `time` >= 60 AND `time` <= 180 group by region_0 limit 1",
		output: []string{"SELECT `xx`, region_0, AVG(`_sum_byte_tx`) AS `AAvg(byte_tx)` FROM (WITH if(l3_device_type_0=1, dictGet('flow_tag.device_map', 'icon_id', (toUInt64(1),toUInt64(l3_device_id_0))), 0) AS `xx` SELECT `xx`, dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, region_id_0, SUM(byte_tx) AS `_sum_byte_tx` FROM flow_metrics.`network_map` WHERE `time` >= 60 AND `time` <= 180 GROUP BY `xx`, `region_id_0`) GROUP BY `xx`, `region_id_0`, `region_0` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		input:  "select Avg(`byte_tx`) AS `Avg(byte_tx)`,icon_id(chost_0) as `xx`,region_0 from vtap_flow_edge_port where `time` >= 60 AND `time` <= 180 group by region_0 limit 1",
		output: []string{"WITH if(l3_device_type_0=1, dictGet('flow_tag.device_map', 'icon_id', (toUInt64(1),toUInt64(l3_device_id_0))), 0) AS `xx` SELECT `xx`, dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, sum(byte_tx)/(121/1) AS `Avg(byte_tx)` FROM flow_metrics.`network_map` WHERE `time` >= 60 AND `time` <= 180 GROUP BY `xx`, `region_id_0` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		input:  "select Avg(`rtt`) AS `Avg(rtt)`,Max(`byte`) AS `Max(byte)`,region_0 from vtap_flow_edge_port where `time` >= 60 AND `time` <= 180 group by region_0 limit 1",
		output: []string{"SELECT region_0, AVGIf(`_div__sum_rtt_sum__sum_rtt_count`, `_div__sum_rtt_sum__sum_rtt_count` > 0) AS `Avg(rtt)`, MAX(`_sum_byte`) AS `Max(byte)` FROM (WITH if(SUM(rtt_count)>0, divide(SUM(rtt_sum), SUM(rtt_count)), null) AS `divide_0diveider_as_null_sum_rtt_sum_sum_rtt_count` SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, region_id_0, `divide_0diveider_as_null_sum_rtt_sum_sum_rtt_count` AS `_div__sum_rtt_sum__sum_rtt_count`, SUM(byte) AS `_sum_byte` FROM flow_metrics.`network_map` WHERE `time` >= 60 AND `time` <= 180 GROUP BY `region_id_0`) GROUP BY `region_id_0`, `region_0` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		input:  "select request from l7_flow_log where Enum(tap_side)='xxx' limit 0, 50",
		output: []string{"SELECT if(type IN [0, 2],1,0) AS `request` FROM flow_log.`l7_flow_log` WHERE (observation_point GLOBAL IN (SELECT value FROM flow_tag.string_enum_map WHERE name_en = 'xxx' and tag_name='observation_point') OR observation_point = 'xxx') LIMIT 0, 50"},
	}, {
		input:  "select request from l7_flow_log where Enum(tap_side) like 'xxx' limit 0, 50",
		output: []string{"SELECT if(type IN [0, 2],1,0) AS `request` FROM flow_log.`l7_flow_log` WHERE (observation_point GLOBAL IN (SELECT value FROM flow_tag.string_enum_map WHERE name_en ilike 'xxx' and tag_name='observation_point')) LIMIT 0, 50"},
	}, {
		input:  "select Histogram(Sum(byte),10) AS histo from l4_flow_log",
		output: []string{"SELECT histogramIf(10)(assumeNotNull(`_sum_byte_tx+byte_rx`),`_sum_byte_tx+byte_rx`>0) AS `histo` FROM (SELECT SUM(byte_tx+byte_rx) AS `_sum_byte_tx+byte_rx` FROM flow_log.`l4_flow_log` LIMIT 10000)"},
	}, {
		input:  "select Sum(log_count) from event",
		output: []string{"SELECT SUM(1) AS `Sum(log_count)` FROM event.`event` LIMIT 10000"},
		db:     "event",
	}, {
		input:  "select Sum(session_length) from l7_flow_log",
		output: []string{"SELECT SUM(if(request_length>0,request_length,0)+if(response_length>0,response_length,0)) AS `Sum(session_length)` FROM flow_log.`l7_flow_log` LIMIT 10000"},
	}, {
		input:  "select region_0 from l7_flow_log where region regexp '系统*'",
		output: []string{"SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0` FROM flow_log.`l7_flow_log` WHERE (toUInt64(region_id) GLOBAL IN (SELECT id FROM flow_tag.region_map WHERE match(name,'系统*'))) LIMIT 10000"},
	}, {
		input:  "select time(time, 0.2) as toi, PerSecond(Sum(byte)+100) as persecond_max_byte_100 from l4_flow_log group by toi limit 1",
		output: []string{"WITH toStartOfInterval(time, toIntervalSecond(1)) + toIntervalSecond(arrayJoin([0]) * 1) AS `_toi` SELECT toUnixTimestamp(`_toi`) AS `toi`, divide(plus(SUM(byte_tx+byte_rx), 100), 1) AS `persecond_max_byte_100` FROM flow_log.`l4_flow_log` GROUP BY `toi` LIMIT 1"},
	}, {
		input:  "select time(time, 1.2) as toi, AAvg(`byte_tx`) AS `AAvg(byte_tx)` from vtap_flow_edge_port group by toi limit 1",
		output: []string{"WITH toStartOfInterval(_time, toIntervalSecond(2)) + toIntervalSecond(arrayJoin([0]) * 2) AS `_toi` SELECT toUnixTimestamp(`_toi`) AS `toi`, AVG(`_sum_byte_tx`) AS `AAvg(byte_tx)` FROM (WITH toStartOfInterval(time, toIntervalSecond(1)) AS `_time` SELECT _time, SUM(byte_tx) AS `_sum_byte_tx` FROM flow_metrics.`network_map` GROUP BY `_time`) GROUP BY `toi` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		input:  "select time(time, 1.2) as toi, Avg(`byte_tx`) AS `Avg(byte_tx)` from vtap_flow_edge_port group by toi limit 1",
		output: []string{"WITH toStartOfInterval(time, toIntervalSecond(2)) + toIntervalSecond(arrayJoin([0]) * 2) AS `_toi` SELECT toUnixTimestamp(`_toi`) AS `toi`, sum(byte_tx)/(2/1) AS `Avg(byte_tx)` FROM flow_metrics.`network_map` GROUP BY `toi` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		input:  "SELECT time(time,5,1,0) as toi, AAvg(`metrics.dropped`) AS `AAvg(metrics.dropped)` FROM `deepflow_agent_collect_sender` GROUP BY  toi ORDER BY toi desc",
		output: []string{"WITH toStartOfInterval(_time, toIntervalSecond(5)) + toIntervalSecond(arrayJoin([0]) * 5) AS `_toi` SELECT toUnixTimestamp(`_toi`) AS `toi`, AVG(`_sum_if(indexOf(metrics_float_names, dropped)=0,null,metrics_float_values[indexOf(metrics_float_names, dropped)])`) AS `AAvg(metrics.dropped)` FROM (WITH toStartOfInterval(time, toIntervalSecond(1)) AS `_time` SELECT _time, SUM(if(indexOf(metrics_float_names, 'dropped')=0,null,metrics_float_values[indexOf(metrics_float_names, 'dropped')])) AS `_sum_if(indexOf(metrics_float_names, dropped)=0,null,metrics_float_values[indexOf(metrics_float_names, dropped)])` FROM deepflow_tenant.`deepflow_collector` WHERE (virtual_table_name='deepflow_agent_collect_sender') GROUP BY `_time`) GROUP BY `toi` ORDER BY `toi` desc LIMIT 10000"},
		db:     "deepflow_tenant",
	}, {
		input:  "SELECT time(time,5,1,0) as toi, Avg(`metrics.dropped`) AS `Avg(metrics.dropped)` FROM `deepflow_agent_collect_sender` GROUP BY  toi ORDER BY toi desc",
		output: []string{"WITH toStartOfInterval(time, toIntervalSecond(5)) + toIntervalSecond(arrayJoin([0]) * 5) AS `_toi` SELECT toUnixTimestamp(`_toi`) AS `toi`, sum(if(indexOf(metrics_float_names, 'dropped')=0,null,metrics_float_values[indexOf(metrics_float_names, 'dropped')]))/(5/1) AS `Avg(metrics.dropped)` FROM deepflow_tenant.`deepflow_collector` WHERE (virtual_table_name='deepflow_agent_collect_sender') GROUP BY `toi` ORDER BY `toi` desc LIMIT 10000"},
		db:     "deepflow_tenant",
	}, {
		input:  "SELECT time(time,120,1,0) as toi, AAvg(`metrics.dropped`) AS `AAvg(metrics.dropped)` FROM `deepflow_agent_collect_sender` GROUP BY  toi ORDER BY toi desc",
		output: []string{"WITH toStartOfInterval(_time, toIntervalSecond(120)) + toIntervalSecond(arrayJoin([0]) * 120) AS `_toi` SELECT toUnixTimestamp(`_toi`) AS `toi`, AVG(`_sum_if(indexOf(metrics_float_names, dropped)=0,null,metrics_float_values[indexOf(metrics_float_names, dropped)])`) AS `AAvg(metrics.dropped)` FROM (WITH toStartOfInterval(time, toIntervalSecond(1)) AS `_time` SELECT _time, SUM(if(indexOf(metrics_float_names, 'dropped')=0,null,metrics_float_values[indexOf(metrics_float_names, 'dropped')])) AS `_sum_if(indexOf(metrics_float_names, dropped)=0,null,metrics_float_values[indexOf(metrics_float_names, dropped)])` FROM deepflow_tenant.`deepflow_collector` WHERE (virtual_table_name='deepflow_agent_collect_sender') GROUP BY `_time`) GROUP BY `toi` ORDER BY `toi` desc LIMIT 10000"},
		db:     "deepflow_tenant",
	}, {
		input:  "SELECT time(time,120,1,0) as toi, Avg(`metrics.dropped`) AS `Avg(metrics.dropped)` FROM `deepflow_agent_collect_sender` GROUP BY  toi ORDER BY toi desc",
		output: []string{"WITH toStartOfInterval(time, toIntervalSecond(120)) + toIntervalSecond(arrayJoin([0]) * 120) AS `_toi` SELECT toUnixTimestamp(`_toi`) AS `toi`, sum(if(indexOf(metrics_float_names, 'dropped')=0,null,metrics_float_values[indexOf(metrics_float_names, 'dropped')]))/(120/1) AS `Avg(metrics.dropped)` FROM deepflow_tenant.`deepflow_collector` WHERE (virtual_table_name='deepflow_agent_collect_sender') GROUP BY `toi` ORDER BY `toi` desc LIMIT 10000"},
		db:     "deepflow_tenant",
	}, {
		input:  "SELECT time(time,120,1,0,30) as toi, Avg(`metrics.dropped`) AS `Avg(metrics.dropped)` FROM `deepflow_agent_collect_sender` GROUP BY  toi ORDER BY toi desc",
		output: []string{"WITH toStartOfInterval(time-30, toIntervalSecond(120)) + toIntervalSecond(arrayJoin([0]) * 120) + 30 AS `_toi` SELECT toUnixTimestamp(`_toi`) AS `toi`, sum(if(indexOf(metrics_float_names, 'dropped')=0,null,metrics_float_values[indexOf(metrics_float_names, 'dropped')]))/(120/1) AS `Avg(metrics.dropped)` FROM deepflow_tenant.`deepflow_collector` WHERE (virtual_table_name='deepflow_agent_collect_sender') GROUP BY `toi` ORDER BY `toi` desc LIMIT 10000"},
		db:     "deepflow_tenant",
	}, {
		input:  "SELECT chost_id_0 from l4_flow_log WHERE NOT exist(chost_0) LIMIT 1",
		output: []string{"SELECT if(l3_device_type_0=1,l3_device_id_0, 0) AS `chost_id_0` FROM flow_log.`l4_flow_log` WHERE NOT (l3_device_type_0=1) LIMIT 1"},
	}, {
		input:  "SELECT response_code, `attribute.a.b`, `attribute.c.d` AS attr_c_d from l7_flow_log WHERE exist(response_code) AND exist(`attribute.a.b`) AND exist(`attribute.c.d`) LIMIT 1",
		output: []string{"SELECT response_code, attribute_values[indexOf(attribute_names,'a.b')] AS `attribute.a.b`, attribute_values[indexOf(attribute_names,'c.d')] AS `attr_c_d` FROM flow_log.`l7_flow_log` WHERE ((isNotNull(response_code))) AND ((attribute_values[indexOf(attribute_names,'a.b')] != '')) AND ((attribute_values[indexOf(attribute_names,'c.d')] != '')) LIMIT 1"},
	}, {
		input:  "SELECT `cloud.tag.xx_0` from l4_flow_log WHERE NOT exist(`cloud.tag.xx_0`) LIMIT 1",
		output: []string{"SELECT if(if(l3_device_type_0=1, dictGet('flow_tag.chost_cloud_tag_map', 'value', (toUInt64(l3_device_id_0),'xx')), '')!='',if(l3_device_type_0=1, dictGet('flow_tag.chost_cloud_tag_map', 'value', (toUInt64(l3_device_id_0),'xx')), ''), dictGet('flow_tag.pod_ns_cloud_tag_map', 'value', (toUInt64(pod_ns_id_0),'xx')) ) AS `cloud.tag.xx_0` FROM flow_log.`l4_flow_log` WHERE NOT (((toUInt64(l3_device_id_0) GLOBAL IN (SELECT id FROM flow_tag.chost_cloud_tag_map WHERE key='xx') AND l3_device_type_0=1) OR (toUInt64(pod_ns_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_ns_cloud_tag_map WHERE key='xx')))) LIMIT 1"},
	}, {
		input:  "select `k8s.annotation.statefulset.kubernetes.io/pod-name_0` from l4_flow_log where `k8s.annotation.statefulset.kubernetes.io/pod-name_0`='opensource-loki-0' group by `k8s.annotation.statefulset.kubernetes.io/pod-name_0`",
		output: []string{"SELECT if(dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(service_id_0),'statefulset.kubernetes.io/pod-name'))!='', dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(service_id_0),'statefulset.kubernetes.io/pod-name')), dictGet('flow_tag.pod_k8s_annotation_map', 'value', (toUInt64(pod_id_0),'statefulset.kubernetes.io/pod-name')) ) AS `k8s.annotation.statefulset.kubernetes.io/pod-name_0` FROM flow_log.`l4_flow_log` WHERE ((toUInt64(service_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_annotation_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name')) OR (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_annotation_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name'))) AND (((toUInt64(service_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_annotation_map WHERE key='statefulset.kubernetes.io/pod-name')) OR (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_annotation_map WHERE key='statefulset.kubernetes.io/pod-name')))) GROUP BY `k8s.annotation.statefulset.kubernetes.io/pod-name_0` LIMIT 10000"},
	}, {
		input:  "select `k8s.annotation.statefulset.kubernetes.io/pod-name_0` as `k8s.annotation.abc` from l4_flow_log where `k8s.annotation.abc`='opensource-loki-0' group by `k8s.annotation.abc`",
		output: []string{"SELECT if(dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(service_id_0),'statefulset.kubernetes.io/pod-name'))!='', dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(service_id_0),'statefulset.kubernetes.io/pod-name')), dictGet('flow_tag.pod_k8s_annotation_map', 'value', (toUInt64(pod_id_0),'statefulset.kubernetes.io/pod-name')) ) AS `k8s.annotation.abc` FROM flow_log.`l4_flow_log` WHERE ((toUInt64(service_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_annotation_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name')) OR (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_annotation_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name'))) AND (((toUInt64(service_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_annotation_map WHERE key='statefulset.kubernetes.io/pod-name')) OR (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_annotation_map WHERE key='statefulset.kubernetes.io/pod-name')))) GROUP BY `k8s.annotation.abc` LIMIT 10000"},
	}, {
		input:  "select `k8s.annotation_0` from l7_flow_log",
		output: []string{"SELECT if(dictGetOrDefault('flow_tag.pod_service_k8s_annotations_map', 'annotations', toUInt64(service_id_0),'{}')!='{}', dictGetOrDefault('flow_tag.pod_service_k8s_annotations_map', 'annotations', toUInt64(service_id_0),'{}'), dictGetOrDefault('flow_tag.pod_k8s_annotations_map', 'annotations', toUInt64(pod_id_0),'{}'))  AS `k8s.annotation_0` FROM flow_log.`l7_flow_log` LIMIT 10000"},
	}, {
		input:  "select `k8s.env.statefulset.kubernetes.io/pod-name_0` from l4_flow_log group by `k8s.env.statefulset.kubernetes.io/pod-name_0`",
		output: []string{"SELECT dictGet('flow_tag.pod_k8s_env_map', 'value', (toUInt64(pod_id_0),'statefulset.kubernetes.io/pod-name')) AS `k8s.env.statefulset.kubernetes.io/pod-name_0` FROM flow_log.`l4_flow_log` WHERE (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_env_map WHERE key='statefulset.kubernetes.io/pod-name')) GROUP BY `k8s.env.statefulset.kubernetes.io/pod-name_0` LIMIT 10000"},
	}, {
		input:  "select `k8s.env.statefulset.kubernetes.io/pod-name_0` from l4_flow_log where `k8s.env.statefulset.kubernetes.io/pod-name_0`='opensource-loki-0' group by `k8s.env.statefulset.kubernetes.io/pod-name_0`",
		output: []string{"SELECT dictGet('flow_tag.pod_k8s_env_map', 'value', (toUInt64(pod_id_0),'statefulset.kubernetes.io/pod-name')) AS `k8s.env.statefulset.kubernetes.io/pod-name_0` FROM flow_log.`l4_flow_log` WHERE toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_env_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name') AND (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_env_map WHERE key='statefulset.kubernetes.io/pod-name')) GROUP BY `k8s.env.statefulset.kubernetes.io/pod-name_0` LIMIT 10000"},
	}, {
		input:  "select `k8s.env.statefulset.kubernetes.io/pod-name_0` as `k8s.env.abc` from l4_flow_log where `k8s.env.abc`='opensource-loki-0' group by `k8s.env.abc`",
		output: []string{"SELECT dictGet('flow_tag.pod_k8s_env_map', 'value', (toUInt64(pod_id_0),'statefulset.kubernetes.io/pod-name')) AS `k8s.env.abc` FROM flow_log.`l4_flow_log` WHERE toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_env_map WHERE value = 'opensource-loki-0' and key='statefulset.kubernetes.io/pod-name') AND (toUInt64(pod_id_0) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_env_map WHERE key='statefulset.kubernetes.io/pod-name')) GROUP BY `k8s.env.abc` LIMIT 10000"},
	}, {
		input:  "select `k8s.env_0` from l7_flow_log",
		output: []string{"SELECT dictGetOrDefault('flow_tag.pod_k8s_envs_map', 'envs', toUInt64(pod_id_0),'{}')  AS `k8s.env_0` FROM flow_log.`l7_flow_log` LIMIT 10000"},
	}, {
		input:  "SELECT Sum(log_count) as sum_log_count FROM l7_flow_log  WHERE `会话长度`>=893689408 ",
		output: []string{"SELECT SUM(1) AS `sum_log_count` FROM flow_log.`l7_flow_log` WHERE `会话长度` >= 893689408 LIMIT 10000"},
	}, {
		input:  "select session_length AS `会话长度` from l7_flow_log where `session_length`<=392037",
		output: []string{"SELECT if(request_length>0,request_length,0)+if(response_length>0,response_length,0) AS `会话长度` FROM flow_log.`l7_flow_log` WHERE if(request_length>0,request_length,0)+if(response_length>0,response_length,0) <= 392037 LIMIT 10000"},
	}, {
		input:  "SELECT node_type(is_internet_0) as `client_node_type` , icon_id(is_internet_0) as `client_icon_id`,  node_type(is_internet_1) as `server_node_type`, is_internet_0, is_internet_1 FROM l4_flow_log GROUP BY is_internet_0, is_internet_1 limit 1",
		output: []string{"WITH if(l3_epc_id_0=-2,dictGet('flow_tag.device_map', 'icon_id', (toUInt64(63999),toUInt64(63999))),0) AS `client_icon_id` SELECT if(l3_epc_id_0=-2,'internet','') AS `client_node_type`, `client_icon_id`, if(l3_epc_id_1=-2,'internet','') AS `server_node_type`, if(l3_epc_id_0=-2,1,0) AS `is_internet_0`, if(l3_epc_id_1=-2,1,0) AS `is_internet_1` FROM flow_log.`l4_flow_log` GROUP BY `client_node_type`, `client_icon_id`, `server_node_type`, `is_internet_0`, `is_internet_1` LIMIT 1"},
	}, {
		input:  "select Enum(pod_group_type_0) ,pod_group_type_0 from l7_flow_log where Enum(pod_group_type_0)!='Deployment' limit 10",
		output: []string{"WITH dictGetOrDefault('flow_tag.int_enum_map', 'name_en', ('pod_group_type',toUInt64(dictGet('flow_tag.pod_group_map', 'pod_group_type', (toUInt64(pod_group_id_0))))), dictGet('flow_tag.pod_group_map', 'pod_group_type', (toUInt64(pod_group_id_0)))) AS `Enum(pod_group_type_0)` SELECT `Enum(pod_group_type_0)`, dictGet('flow_tag.pod_group_map', 'pod_group_type', (toUInt64(pod_group_id_0))) AS `pod_group_type_0` FROM flow_log.`l7_flow_log` WHERE (not(toUInt64(dictGet('flow_tag.pod_group_map', 'pod_group_type', (toUInt64(pod_group_id_0)))) GLOBAL IN (SELECT value FROM flow_tag.int_enum_map WHERE name_en = 'Deployment' and tag_name='pod_group_type') AND pod_group_id_0!=0)) LIMIT 10"},
	}, {
		name:   "count_1",
		input:  "select Count(row) as a from l7_flow_log having a > 0 ",
		output: []string{"SELECT COUNT(1) AS `a` FROM flow_log.`l7_flow_log` HAVING a > 0 LIMIT 10000"},
	}, {
		name:   "count_2",
		input:  "select Count(row) from l7_flow_log having Count(row) > 0 ",
		output: []string{"SELECT COUNT(1) AS `Count(row)` FROM flow_log.`l7_flow_log` HAVING COUNT(1) > 0 LIMIT 10000"},
	}, {
		name:   "count_3",
		input:  "select Avg(`byte_tx`) AS `Avg(byte_tx)`,icon_id(chost_0) as `xx`, Count(row) as `c`, region_0 from vtap_flow_edge_port group by region_0 having `c` > 0 limit 1",
		output: []string{"WITH if(l3_device_type_0=1, dictGet('flow_tag.device_map', 'icon_id', (toUInt64(1),toUInt64(l3_device_id_0))), 0) AS `xx` SELECT `xx`, dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, sum(byte_tx)/(1/1) AS `Avg(byte_tx)`, COUNT(1) AS `c` FROM flow_metrics.`network_map` GROUP BY `xx`, `region_id_0` HAVING c > 0 LIMIT 1"},
		db:     "flow_metrics",
	}, {
		name:   "count_3_aavg",
		input:  "select AAvg(`byte_tx`) AS `AAvg(byte_tx)`,icon_id(chost_0) as `xx`, Count(row) as `c`, region_0 from vtap_flow_edge_port group by region_0 having `c` > 0 limit 1",
		output: []string{"SELECT `xx`, region_0, AVG(`_sum_byte_tx`) AS `AAvg(byte_tx)`, SUM(`_count_1`) AS `c` FROM (WITH if(l3_device_type_0=1, dictGet('flow_tag.device_map', 'icon_id', (toUInt64(1),toUInt64(l3_device_id_0))), 0) AS `xx` SELECT `xx`, dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, region_id_0, SUM(byte_tx) AS `_sum_byte_tx`, COUNT(1) AS `_count_1` FROM flow_metrics.`network_map` GROUP BY `xx`, `region_id_0`) GROUP BY `xx`, `region_id_0`, `region_0` HAVING c > 0 LIMIT 1"},
		db:     "flow_metrics",
	}, {
		name:   "topk_1",
		db:     "flow_metrics",
		input:  "select pod_ns, topK(pod, 10) from `vtap_app_port.1h` WHERE time>=1687315761 AND time<=1687316661 group by pod_ns limit 10",
		output: []string{"SELECT dictGet('flow_tag.pod_ns_map', 'name', (toUInt64(pod_ns_id))) AS `pod_ns`, topK(10)(dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id)))) FROM flow_metrics.`application.1h` WHERE `time` >= 1687315761 AND `time` <= 1687316661 AND (pod_ns_id!=0) GROUP BY `pod_ns_id` LIMIT 10"},
	}, {
		name:   "topk_2",
		db:     "flow_metrics",
		input:  "select pod_ns, topK(pod, pod_cluster_id, service_id, 10) from `vtap_app_port.1h` WHERE time>=1694069050 AND time<=1694990640 group by pod_ns limit 10",
		output: []string{"SELECT dictGet('flow_tag.pod_ns_map', 'name', (toUInt64(pod_ns_id))) AS `pod_ns`, topK(10)((dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id))),pod_cluster_id,service_id)) FROM flow_metrics.`application.1h` WHERE `time` >= 1694069050 AND `time` <= 1694990640 AND (pod_ns_id!=0) GROUP BY `pod_ns_id` LIMIT 10"},
	}, {
		name:   "topk_enum",
		db:     "flow_log",
		input:  "select TopK(protocol,2) from l4_flow_log limit 2",
		output: []string{"SELECT arrayStringConcat(topKIf(2)(dictGetOrDefault('flow_tag.int_enum_map', 'name_en', ('protocol',toUInt64(protocol)), protocol), dictGetOrDefault('flow_tag.int_enum_map', 'name_en', ('protocol',toUInt64(protocol)), protocol) != ''), ',') AS `TopK_2(protocol)` FROM flow_log.`l4_flow_log` LIMIT 2"},
	}, {
		name:   "select_enum",
		db:     "flow_log",
		input:  "select protocol from l4_flow_log limit 2",
		output: []string{"SELECT protocol FROM flow_log.`l4_flow_log` LIMIT 2"},
	}, {
		name:   "any_1",
		db:     "flow_metrics",
		input:  "select pod_ns, any(pod) from `vtap_app_port.1h` WHERE time>=1694069050 AND time<=1694990640 group by pod_ns limit 10",
		output: []string{"SELECT dictGet('flow_tag.pod_ns_map', 'name', (toUInt64(pod_ns_id))) AS `pod_ns`, any(dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id)))) FROM flow_metrics.`application.1h` WHERE `time` >= 1694069050 AND `time` <= 1694990640 AND (pod_ns_id!=0) GROUP BY `pod_ns_id` LIMIT 10"},
	}, {
		name:   "any_2",
		db:     "flow_metrics",
		input:  "select pod_ns, any(pod, pod_cluster_id, service_id) from `vtap_app_port.1h` WHERE time>=1694069050 AND time<=1694990640 group by pod_ns limit 10",
		output: []string{"SELECT dictGet('flow_tag.pod_ns_map', 'name', (toUInt64(pod_ns_id))) AS `pod_ns`, any((dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id))),pod_cluster_id,service_id)) FROM flow_metrics.`application.1h` WHERE `time` >= 1694069050 AND `time` <= 1694990640 AND (pod_ns_id!=0) GROUP BY `pod_ns_id` LIMIT 10"},
	}, {
		input:  "SELECT is_internet_0, is_internet_1 FROM l4_flow_log GROUP BY is_internet_0, is_internet_1 limit 1",
		output: []string{"SELECT if(l3_epc_id_0=-2,1,0) AS `is_internet_0`, if(l3_epc_id_1=-2,1,0) AS `is_internet_1` FROM flow_log.`l4_flow_log` GROUP BY `is_internet_0`, `is_internet_1` LIMIT 1"},
	}, {
		name:   "TopK_1",
		input:  "select TopK(ip_0, 10) from l4_flow_log limit 1",
		output: []string{"SELECT arrayStringConcat(topKIf(10)(if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)), if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)) != ''), ',') AS `TopK_10(ip_0)` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		name:   "TopK_2",
		input:  "select TopK(ip_0, pod_0, 10) from l4_flow_log limit 1",
		output: []string{"SELECT topKIf(10)((if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)), dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id_0)))), (if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)) != '' AND dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id_0))) != '')) AS `TopK_10(ip_0, pod_0)` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		name:    "TopK_err",
		input:   "select TopK(ip_0, 111) from l4_flow_log limit 1",
		wantErr: "function [TopK] argument [111] value range is incorrect, it should be within [1, 100]",
	}, {
		name:       "TopK_3",
		input:      "SELECT TopK(`region`,3) AS `TopK_3(区域)` FROM `vtap_app_port` WHERE (time>=1705370520 AND time<=1705371300)",
		output:     []string{"SELECT arrayStringConcat(topKArray(3)(`_grouparray_dictGet(flow_tag.region_map, name, (toUInt64(region_id)))_dictGet('flow_tag.region_map', 'name', (toUInt64(region_id))) != ''`), ',') AS `TopK_3(区域)` FROM (SELECT groupArrayIf(dictGet('flow_tag.region_map', 'name', (toUInt64(region_id))), dictGet('flow_tag.region_map', 'name', (toUInt64(region_id))) != '') AS `_grouparray_dictGet(flow_tag.region_map, name, (toUInt64(region_id)))_dictGet('flow_tag.region_map', 'name', (toUInt64(region_id))) != ''` FROM flow_metrics.`application.1m` WHERE (`time` >= 1705370520 AND `time` <= 1705371300)) LIMIT 10000"},
		db:         "flow_metrics",
		datasource: "1m",
	}, {
		name:   "Any_1",
		input:  "select Any(ip_0) from l4_flow_log limit 1",
		output: []string{"SELECT anyIf(if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)), if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)) != '') AS `Any(ip_0)` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		name:   "Any_2",
		input:  "select Any(ip_0, pod_0) from l4_flow_log limit 1",
		output: []string{"SELECT anyIf((if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)), dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id_0)))), (if(is_ipv4=1, IPv4NumToString(ip4_0), IPv6NumToString(ip6_0)) != '' AND dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id_0))) != '')) AS `Any(ip_0, pod_0)` FROM flow_log.`l4_flow_log` LIMIT 1"},
	}, {
		name:   "layered_0",
		input:  "select Avg(`byte_tx`) AS `Avg(byte_tx)`, region_0 from vtap_flow_edge_port group by region_0 limit 1",
		output: []string{"SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, sum(byte_tx)/(1/1) AS `Avg(byte_tx)` FROM flow_metrics.`network_map` GROUP BY `region_id_0` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		name:   "layered_0_aavg",
		input:  "select AAvg(`byte_tx`) AS `AAvg(byte_tx)`, region_0 from vtap_flow_edge_port group by region_0 limit 1",
		output: []string{"SELECT region_0, AVG(`_sum_byte_tx`) AS `AAvg(byte_tx)` FROM (SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, region_id_0, SUM(byte_tx) AS `_sum_byte_tx` FROM flow_metrics.`network_map` GROUP BY `region_id_0`) GROUP BY `region_id_0`, `region_0` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		name:   "division>=0_l4_flow_log",
		input:  "select Avg(`l7_error_ratio`) AS `Avg(l7_error_ratio)`, Avg(`retrans_syn_ratio`) AS `Avg(retrans_syn_ratio)`, Avg(`retrans_synack_ratio`) AS `Avg(retrans_synack_ratio)`, Avg(`l7_client_error_ratio`) AS `Avg(l7_client_error_ratio)`, Avg(`l7_server_error_ratio`) AS `Avg(l7_server_error_ratio)`, auto_service_id from l4_flow_log group by auto_service_id limit 1",
		output: []string{"WITH if(SUMIf(l7_response, l7_response>0)>0, divide(SUM(l7_error), SUMIf(l7_response, l7_response>0)), null) AS `divide_0diveider_as_null_sum_l7_error_sum_l7_response_l7_response>0`, if(SUMIf(syn_count, syn_count>0)>0, divide(SUM(retrans_syn), SUMIf(syn_count, syn_count>0)), null) AS `divide_0diveider_as_null_sum_retrans_syn_sum_syn_count_syn_count>0`, if(SUMIf(synack_count, synack_count>0)>0, divide(SUM(retrans_synack), SUMIf(synack_count, synack_count>0)), null) AS `divide_0diveider_as_null_sum_retrans_synack_sum_synack_count_synack_count>0`, if(SUMIf(l7_response, l7_response>0)>0, divide(SUM(l7_client_error), SUMIf(l7_response, l7_response>0)), null) AS `divide_0diveider_as_null_sum_l7_client_error_sum_l7_response_l7_response>0`, if(SUMIf(l7_response, l7_response>0)>0, divide(SUM(l7_server_error), SUMIf(l7_response, l7_response>0)), null) AS `divide_0diveider_as_null_sum_l7_server_error_sum_l7_response_l7_response>0` SELECT if(auto_service_type in (0,255),subnet_id,auto_service_id) AS `auto_service_id`, `divide_0diveider_as_null_sum_l7_error_sum_l7_response_l7_response>0`*100 AS `Avg(l7_error_ratio)`, `divide_0diveider_as_null_sum_retrans_syn_sum_syn_count_syn_count>0`*100 AS `Avg(retrans_syn_ratio)`, `divide_0diveider_as_null_sum_retrans_synack_sum_synack_count_synack_count>0`*100 AS `Avg(retrans_synack_ratio)`, `divide_0diveider_as_null_sum_l7_client_error_sum_l7_response_l7_response>0`*100 AS `Avg(l7_client_error_ratio)`, `divide_0diveider_as_null_sum_l7_server_error_sum_l7_response_l7_response>0`*100 AS `Avg(l7_server_error_ratio)` FROM flow_log.`l4_flow_log` GROUP BY `auto_service_id` LIMIT 1"},
	}, {
		name:   "division>=0_l4_flow_log_aavg",
		input:  "select AAvg(`l7_error_ratio`) AS `AAvg(l7_error_ratio)`, AAvg(`retrans_syn_ratio`) AS `AAvg(retrans_syn_ratio)`, AAvg(`retrans_synack_ratio`) AS `AAvg(retrans_synack_ratio)`, AAvg(`l7_client_error_ratio`) AS `AAvg(l7_client_error_ratio)`, AAvg(`l7_server_error_ratio`) AS `AAvg(l7_server_error_ratio)`, auto_service_id from l4_flow_log group by auto_service_id limit 1",
		output: []string{"SELECT if(auto_service_type in (0,255),subnet_id,auto_service_id) AS `auto_service_id`, AVGIf(l7_error/l7_response, l7_response>0)*100 AS `AAvg(l7_error_ratio)`, AVGIf(retrans_syn/syn_count, syn_count>0)*100 AS `AAvg(retrans_syn_ratio)`, AVGIf(retrans_synack/synack_count, synack_count>0)*100 AS `AAvg(retrans_synack_ratio)`, AVGIf(l7_client_error/l7_response, l7_response>0)*100 AS `AAvg(l7_client_error_ratio)`, AVGIf(l7_server_error/l7_response, l7_response>0)*100 AS `AAvg(l7_server_error_ratio)` FROM flow_log.`l4_flow_log` GROUP BY `auto_service_id` LIMIT 1"},
	}, {
		name:   "division>=0_l7_flow_log",
		input:  "select Avg(`error_ratio`) AS `Avg(error_ratio)`, auto_service_id from l7_flow_log group by auto_service_id limit 1",
		output: []string{"WITH if(SUMIf(if(type IN [1, 2],1,0), if(type IN [1, 2],1,0)>0)>0, divide(SUM(if(response_status IN [4, 3],1,0)), SUMIf(if(type IN [1, 2],1,0), if(type IN [1, 2],1,0)>0)), null) AS `divide_0diveider_as_null_sum_if(response_status IN [4, 3],1,0)_sum_if(type IN [1, 2],1,0)_if(type IN [1, 2],1,0)>0` SELECT if(auto_service_type in (0,255),subnet_id,auto_service_id) AS `auto_service_id`, `divide_0diveider_as_null_sum_if(response_status IN [4, 3],1,0)_sum_if(type IN [1, 2],1,0)_if(type IN [1, 2],1,0)>0`*100 AS `Avg(error_ratio)` FROM flow_log.`l7_flow_log` GROUP BY `auto_service_id` LIMIT 1"},
	}, {
		name:   "division>=0_l7_flow_log_aavg",
		input:  "select AAvg(`error_ratio`) AS `AAvg(error_ratio)`, auto_service_id from l7_flow_log group by auto_service_id limit 1",
		output: []string{"SELECT if(auto_service_type in (0,255),subnet_id,auto_service_id) AS `auto_service_id`, AVGIf(if(response_status IN [4, 3],1,0)/if(type IN [1, 2],1,0), if(type IN [1, 2],1,0)>0)*100 AS `AAvg(error_ratio)` FROM flow_log.`l7_flow_log` GROUP BY `auto_service_id` LIMIT 1"},
	}, {
		name:   "division>=0_vtap_app_port",
		input:  "select Avg(`rrt`) AS `Avg(rrt)`, Avg(`error_ratio`) AS `Avg(error_ratio)`, auto_service_id from vtap_app_port group by auto_service_id limit 1",
		output: []string{"WITH if(SUMIf(rrt_count, rrt_count>0)>0, divide(SUM(rrt_sum), SUMIf(rrt_count, rrt_count>0)), null) AS `divide_0diveider_as_null_sum_rrt_sum_sum_rrt_count_rrt_count>0`, if(SUMIf(response, response>0)>0, divide(SUM(error), SUMIf(response, response>0)), null) AS `divide_0diveider_as_null_sum_error_sum_response_response>0` SELECT if(auto_service_type in (0,255),subnet_id,auto_service_id) AS `auto_service_id`, `divide_0diveider_as_null_sum_rrt_sum_sum_rrt_count_rrt_count>0` AS `Avg(rrt)`, `divide_0diveider_as_null_sum_error_sum_response_response>0`*100 AS `Avg(error_ratio)` FROM flow_metrics.`application` GROUP BY `auto_service_id` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		name:   "division>=0_vtap_app_port_aavg",
		input:  "select AAvg(`rrt`) AS `AAvg(rrt)`, AAvg(`error_ratio`) AS `AAvg(error_ratio)`, auto_service_id from vtap_app_port group by auto_service_id limit 1",
		output: []string{"SELECT auto_service_id, AVGArray(arrayFilter(x -> x>0, `_grouparray_rrt_sum/rrt_count`)) AS `AAvg(rrt)`, AVG(`_div__sum_error__sum_response`)*100 AS `AAvg(error_ratio)` FROM (WITH if(SUM(response)>0, divide(SUM(error), SUM(response)), null) AS `divide_0diveider_as_null_sum_error_sum_response` SELECT if(auto_service_type in (0,255),subnet_id,auto_service_id) AS `auto_service_id`, groupArrayIf(rrt_sum/rrt_count, rrt_sum/rrt_count > 0) AS `_grouparray_rrt_sum/rrt_count`, `divide_0diveider_as_null_sum_error_sum_response` AS `_div__sum_error__sum_response` FROM flow_metrics.`application` GROUP BY `auto_service_id`) GROUP BY `auto_service_id` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		name:   "division>=0_vtap_flow_edge_port",
		input:  "select Avg(`bpp`) AS `Avg(bpp)`, Avg(`retrans_syn_ratio`) AS `Avg(retrans_syn_ratio)`, auto_service_id from vtap_flow_edge_port group by auto_service_id limit 1",
		output: []string{"WITH if(SUMIf(packet, packet>0)>0, divide(SUM(byte), SUMIf(packet, packet>0)), null) AS `divide_0diveider_as_null_sum_byte_sum_packet_packet>0`, if(SUMIf(syn_count, syn_count>0)>0, divide(SUM(retrans_syn), SUMIf(syn_count, syn_count>0)), null) AS `divide_0diveider_as_null_sum_retrans_syn_sum_syn_count_syn_count>0` SELECT if(auto_service_type in (0,255),subnet_id,auto_service_id) AS `auto_service_id`, `divide_0diveider_as_null_sum_byte_sum_packet_packet>0` AS `Avg(bpp)`, `divide_0diveider_as_null_sum_retrans_syn_sum_syn_count_syn_count>0`*100 AS `Avg(retrans_syn_ratio)` FROM flow_metrics.`network_map` GROUP BY `auto_service_id` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		name:   "division>=0_vtap_flow_edge_port_aavg",
		input:  "select AAvg(`bpp`) AS `AAvg(bpp)`, AAvg(`retrans_syn_ratio`) AS `AAvg(retrans_syn_ratio)`, auto_service_id from vtap_flow_edge_port group by auto_service_id limit 1",
		output: []string{"SELECT auto_service_id, AVG(`_div__sum_byte__sum_packet`) AS `AAvg(bpp)`, AVG(`_div__sum_retrans_syn__sum_syn_count`)*100 AS `AAvg(retrans_syn_ratio)` FROM (WITH if(SUM(packet)>0, divide(SUM(byte), SUM(packet)), null) AS `divide_0diveider_as_null_sum_byte_sum_packet`, if(SUM(syn_count)>0, divide(SUM(retrans_syn), SUM(syn_count)), null) AS `divide_0diveider_as_null_sum_retrans_syn_sum_syn_count` SELECT if(auto_service_type in (0,255),subnet_id,auto_service_id) AS `auto_service_id`, `divide_0diveider_as_null_sum_byte_sum_packet` AS `_div__sum_byte__sum_packet`, `divide_0diveider_as_null_sum_retrans_syn_sum_syn_count` AS `_div__sum_retrans_syn__sum_syn_count` FROM flow_metrics.`network_map` GROUP BY `auto_service_id`) GROUP BY `auto_service_id` LIMIT 1"},
		db:     "flow_metrics",
	}, {
		name:   "exist_trans_support_tag_0",
		input:  "SELECT pod from l4_flow_log WHERE exist(pod_0) AND exist(host_1) AND exist(vpc_0) AND exist(auto_instance_1) AND exist(auto_service_0) LIMIT 1",
		output: []string{"SELECT dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id))) AS `pod` FROM flow_log.`l4_flow_log` WHERE (pod_id_0!=0) AND (host_id_1!=0) AND (l3_epc_id_0!=-2) AND (auto_instance_type_1 not in (101,102)) AND (auto_service_type_0 not in (10)) LIMIT 1"},
	}, {
		name:   "exist_trans_support_tag_1",
		input:  "SELECT pod from vtap_app_port WHERE exist(auto_instance) AND exist(auto_service) LIMIT 1",
		output: []string{"SELECT dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id))) AS `pod` FROM flow_metrics.`application` WHERE (auto_instance_type not in (101,102)) AND (auto_service_type not in (10)) LIMIT 1"},
		db:     "flow_metrics",
	}, {
		name:   "l2_vpc_filter_trans",
		input:  "SELECT pod from l4_flow_log WHERE exist(l2_vpc_0) AND l2_vpc!=1 GROUP BY l2_vpc_1 LIMIT 1",
		output: []string{"SELECT dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id))) AS `pod`, dictGet('flow_tag.l3_epc_map', 'name', (toUInt64(epc_id_1))) AS `l2_vpc_1` FROM flow_log.`l4_flow_log` WHERE (epc_id_0!=0) AND (toUInt64(epc_id) GLOBAL IN (SELECT id FROM flow_tag.l3_epc_map WHERE name != 1)) AND (epc_id_1!=0) GROUP BY `epc_id_1` LIMIT 1"},
	}, {
		name:       "with_multi_query",
		db:         "flow_metrics",
		datasource: "1m",
		input:      "WITH query1 AS (SELECT PerSecond(Avg(`request`)) AS `请求速率`, Avg(`server_error_ratio`) AS `服务端异常比例`, Avg(`rrt`) AS `响应时延`, node_type(region_0) AS `client_node_type`, icon_id(region_0) AS `client_icon_id`, region_id_0, region_0, Enum(tap_side), tap_side, is_internet_0, node_type(region_1) AS `server_node_type`, icon_id(region_1) AS `server_icon_id`, region_id_1, region_1, is_internet_1 FROM vtap_app_edge_port WHERE time>=1704338640 AND time<=1704339600 GROUP BY region_0, tap_side, is_internet_0, region_id_0, `client_node_type`, region_1, is_internet_1, region_id_1, `server_node_type` ORDER BY `请求速率` DESC LIMIT 50 OFFSET 0), query2 AS (SELECT Avg(`packet_tx`) AS `Avg(发送包数)`, node_type(region_0) AS `client_node_type`, icon_id(region_0) AS `client_icon_id`, region_id_0, region_0, Enum(tap_side), tap_side, is_internet_0, node_type(region_1) AS `server_node_type`, icon_id(region_1) AS `server_icon_id`, region_id_1, region_1, is_internet_1 FROM vtap_flow_edge_port WHERE time>=1704338640 AND time<=1704339600 GROUP BY region_0, tap_side, is_internet_0, region_id_0, `client_node_type`, region_1, is_internet_1, region_id_1, `server_node_type` LIMIT 50) SELECT query1.`请求速率` AS `请求速率`, query1.`服务端异常比例` AS `服务端异常比例`, query1.`响应时延` AS `响应时延`, query1.`client_node_type` AS `client_node_type`, query1.`client_icon_id` AS `client_icon_id`, query1.`region_id_0` AS `region_id_0`, query1.`region_0` AS `region_0`, query1.`Enum(tap_side)` AS `Enum(tap_side)`, query1.`tap_side` AS `tap_side`, query1.`is_internet_0` AS `is_internet_0`, query1.`server_node_type` AS `server_node_type`, query1.`server_icon_id` AS `server_icon_id`, query1.`region_id_1` AS `region_id_1`, query1.`region_1` AS `region_1`, query1.`is_internet_1` AS `is_internet_1`, query2.`Avg(发送包数)` AS `Avg(发送包数)` FROM query1 LEFT JOIN query2 ON query1.`region_0` = query2.`region_0` AND query1.`tap_side` = query2.`tap_side` AND query1.`is_internet_0` = query2.`is_internet_0` AND query1.`region_id_0` = query2.`region_id_0` AND query1.`client_node_type` = query2.`client_node_type` AND query1.`region_1` = query2.`region_1` AND query1.`is_internet_1` = query2.`is_internet_1` AND query1.`region_id_1` = query2.`region_id_1` AND query1.`server_node_type` = query2.`server_node_type`",
		output:     []string{"WITH query1 AS (WITH dictGet('flow_tag.region_map', 'icon_id', (toUInt64(region_id_0))) AS `client_icon_id`, dictGetOrDefault('flow_tag.string_enum_map', 'name_en', ('observation_point',observation_point), observation_point) AS `Enum(tap_side)`, dictGet('flow_tag.region_map', 'icon_id', (toUInt64(region_id_1))) AS `server_icon_id`, if(SUMIf(response, response>0)>0, divide(SUM(server_error), SUMIf(response, response>0)), null) AS `divide_0diveider_as_null_sum_server_error_sum_response_response>0`, if(SUMIf(rrt_count, rrt_count>0)>0, divide(SUM(rrt_sum), SUMIf(rrt_count, rrt_count>0)), null) AS `divide_0diveider_as_null_sum_rrt_sum_sum_rrt_count_rrt_count>0` SELECT 'region' AS `client_node_type`, `client_icon_id`, region_id_0, dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, `Enum(tap_side)`, observation_point AS `tap_side`, if(l3_epc_id_0=-2,1,0) AS `is_internet_0`, 'region' AS `server_node_type`, `server_icon_id`, region_id_1, dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_1))) AS `region_1`, if(l3_epc_id_1=-2,1,0) AS `is_internet_1`, divide(sum(request)/(1020/60), 60) AS `请求速率`, `divide_0diveider_as_null_sum_server_error_sum_response_response>0`*100 AS `服务端异常比例`, `divide_0diveider_as_null_sum_rrt_sum_sum_rrt_count_rrt_count>0` AS `响应时延` FROM flow_metrics.`application_map.1m` WHERE `time` >= 1704338640 AND `time` <= 1704339600 GROUP BY `region_id_0`, `observation_point`, `is_internet_0`, `region_id_1`, `is_internet_1` ORDER BY `请求速率` desc LIMIT 0, 50), query2 AS (WITH dictGet('flow_tag.region_map', 'icon_id', (toUInt64(region_id_0))) AS `client_icon_id`, dictGetOrDefault('flow_tag.string_enum_map', 'name_en', ('observation_point',observation_point), observation_point) AS `Enum(tap_side)`, dictGet('flow_tag.region_map', 'icon_id', (toUInt64(region_id_1))) AS `server_icon_id` SELECT 'region' AS `client_node_type`, `client_icon_id`, region_id_0, dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, `Enum(tap_side)`, observation_point AS `tap_side`, if(l3_epc_id_0=-2,1,0) AS `is_internet_0`, 'region' AS `server_node_type`, `server_icon_id`, region_id_1, dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_1))) AS `region_1`, if(l3_epc_id_1=-2,1,0) AS `is_internet_1`, sum(packet_tx)/(1020/60) AS `Avg(发送包数)` FROM flow_metrics.`network_map.1m` WHERE `time` >= 1704338640 AND `time` <= 1704339600 GROUP BY `region_id_0`, `observation_point`, `is_internet_0`, `region_id_1`, `is_internet_1` LIMIT 50) SELECT query1.`请求速率` AS `请求速率`, query1.`服务端异常比例` AS `服务端异常比例`, query1.`响应时延` AS `响应时延`, query1.`client_node_type` AS `client_node_type`, query1.`client_icon_id` AS `client_icon_id`, query1.`region_id_0` AS `region_id_0`, query1.`region_0` AS `region_0`, query1.`Enum(tap_side)` AS `Enum(tap_side)`, query1.`tap_side` AS `tap_side`, query1.`is_internet_0` AS `is_internet_0`, query1.`server_node_type` AS `server_node_type`, query1.`server_icon_id` AS `server_icon_id`, query1.`region_id_1` AS `region_id_1`, query1.`region_1` AS `region_1`, query1.`is_internet_1` AS `is_internet_1`, query2.`Avg(发送包数)` AS `Avg(发送包数)` FROM query1 LEFT JOIN query2 ON query1.`region_0` = query2.`region_0` AND query1.`tap_side` = query2.`tap_side` AND query1.`is_internet_0` = query2.`is_internet_0` AND query1.`region_id_0` = query2.`region_id_0` AND query1.`client_node_type` = query2.`client_node_type` AND query1.`region_1` = query2.`region_1` AND query1.`is_internet_1` = query2.`is_internet_1` AND query1.`region_id_1` = query2.`region_id_1` AND query1.`server_node_type` = query2.`server_node_type`"},
	}, {
		name:       "test_slimit",
		db:         "flow_metrics",
		datasource: "1m",
		input:      "SELECT time(time,1,1,0) as toi, PerSecond(Avg(`byte`)) AS `流量速率`, pod as pod FROM `vtap_flow_port` WHERE time>=1705040184 AND time<=1705045184 GROUP BY toi, pod ORDER BY toi desc SLIMIT 5",
		output:     []string{"WITH toStartOfInterval(time, toIntervalSecond(60)) + toIntervalSecond(arrayJoin([0]) * 60) AS `_toi` SELECT dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id))) AS `pod`, toUnixTimestamp(`_toi`) AS `toi`, divide(sum(byte)/(60/60), 60) AS `流量速率` FROM flow_metrics.`network.1m` WHERE (pod) GLOBAL IN (SELECT dictGet('flow_tag.pod_map', 'name', (toUInt64(pod_id))) AS `pod` FROM flow_metrics.`network.1m` WHERE `time` >= 1705040184 AND `time` <= 1705045184 AND (pod_id!=0) GROUP BY `pod_id` LIMIT 5) AND `time` >= 1705040184 AND `time` <= 1705045184 AND (pod_id!=0) GROUP BY `toi`, `pod_id` ORDER BY `toi` desc LIMIT 10000"},
	}, {
		name:       "test_host_hostname_ip",
		db:         "flow_metrics",
		datasource: "1m",
		input:      "SELECT region as region, host_hostname_id, host_ip_id, host_hostname, host_ip, node_type(host_ip) as `node_type`, icon_id(host_ip) as icon_id FROM `vtap_flow_port` WHERE time>=1705040184 AND time<=1705045184 AND host_ip != '1.1.1.1' AND host_hostname_id != 1 AND host_ip_id != 2 GROUP BY region, host_hostname_id, host_ip_id, host_hostname, host_ip, `node_type` limit 5",
		output:     []string{"WITH dictGet('flow_tag.device_map', 'icon_id', (toUInt64(6),toUInt64(host_id))) AS `icon_id` SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id))) AS `region`, host_id AS `host_hostname_id`, host_id AS `host_ip_id`, dictGet('flow_tag.device_map', 'hostname', (toUInt64(6),toUInt64(host_id))) AS `host_hostname`, dictGet('flow_tag.device_map', 'ip', (toUInt64(6),toUInt64(host_id))) AS `host_ip`, 'host' AS `node_type`, `icon_id` FROM flow_metrics.`network.1m` WHERE `time` >= 1705040184 AND `time` <= 1705045184 AND (not(toUInt64(host_id) GLOBAL IN (SELECT deviceid FROM flow_tag.device_map WHERE ip = '1.1.1.1' AND devicetype=6))) AND (not(host_id = 1)) AND (not(host_id = 2)) AND (host_id!=0) AND (host_id!=0) AND (host_id!=0) AND (host_id!=0) GROUP BY `region_id`, `host_id` LIMIT 5"},
	}, {
		name:       "test_chost_hostname_ip",
		db:         "flow_metrics",
		datasource: "1m",
		input:      "SELECT region_0 as region_0, chost_hostname_id_0, chost_ip_id_0, chost_hostname_0, chost_ip_0, node_type(chost_hostname_0) as `client_node_type`, icon_id(chost_hostname_0) as `client_icon_id` FROM `vtap_flow_edge_port` WHERE time>=1705040184 AND time<=1705045184 AND chost_hostname_0 != 'a' AND chost_hostname_id_0 != 1 AND chost_ip_id_0 != 2 GROUP BY region_0, chost_hostname_id_0, chost_ip_id_0, chost_hostname_0, chost_ip_0, `client_node_type` limit 5",
		output:     []string{"WITH if(l3_device_type_0 = 1, l3_device_type_0, 0) AS `device_type_chost_hostname_0`, if(l3_device_type_0 = 1, l3_device_type_0, 0) AS `device_type_chost_ip_0`, if(l3_device_type_0=1, dictGet('flow_tag.device_map', 'icon_id', (toUInt64(1),toUInt64(l3_device_id_0))), 0) AS `client_icon_id` SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0`, if(l3_device_type_0=1,l3_device_id_0, 0) AS `chost_hostname_id_0`, if(l3_device_type_0=1,l3_device_id_0, 0) AS `chost_ip_id_0`, dictGet('flow_tag.device_map', 'hostname', (toUInt64(device_type_chost_hostname_0),toUInt64(l3_device_id_0))) AS `chost_hostname_0`, device_type_chost_hostname_0, dictGet('flow_tag.device_map', 'ip', (toUInt64(device_type_chost_ip_0),toUInt64(l3_device_id_0))) AS `chost_ip_0`, device_type_chost_ip_0, 'chost' AS `client_node_type`, `client_icon_id` FROM flow_metrics.`network_map.1m` WHERE `time` >= 1705040184 AND `time` <= 1705045184 AND (not(toUInt64(l3_device_id_0) GLOBAL IN (SELECT deviceid FROM flow_tag.device_map WHERE hostname = 'a' AND devicetype=1) AND l3_device_type_0=1)) AND (not(l3_device_id_0 = 1 AND l3_device_type_0=1)) AND (not(l3_device_id_0 = 2 AND l3_device_type_0=1)) AND (l3_device_id_0!=0 AND l3_device_type_0=1) AND (l3_device_id_0!=0 AND l3_device_type_0=1) AND (l3_device_id_0!=0 AND l3_device_type_0=1) AND (l3_device_id_0!=0 AND l3_device_type_0=1) GROUP BY `client_icon_id`, `region_id_0`, `chost_hostname_id_0`, `chost_ip_id_0`, `l3_device_id_0`, `device_type_chost_hostname_0`, `device_type_chost_ip_0` LIMIT 5"},
	}, {
		name:       "test_pod_node_hostname_ip",
		db:         "flow_metrics",
		datasource: "1m",
		input:      "SELECT pod_node_hostname_id_1, pod_node_ip_id_0, pod_node_hostname_1, pod_node_ip_0, node_type(pod_node_ip_0) as `client_node_type`, node_type(pod_node_hostname_1) as `server_node_type`, icon_id(pod_node_ip_0) as `client_icon_id`, icon_id(pod_node_hostname_1) as `server_icon_id` FROM `vtap_flow_edge_port` WHERE time>=1705040184 AND time<=1705045184 AND pod_node_hostname_1 != 'a' AND pod_node_hostname_id_1 != 1 AND pod_node_hostname_id_1 != 2 GROUP BY pod_node_hostname_id_1, pod_node_ip_id_0, pod_node_hostname_1, pod_node_ip_0, `client_node_type`, `server_node_type` limit 5",
		output:     []string{"WITH dictGet('flow_tag.pod_node_map', 'icon_id', (toUInt64(pod_node_id_0))) AS `client_icon_id`, dictGet('flow_tag.pod_node_map', 'icon_id', (toUInt64(pod_node_id_1))) AS `server_icon_id` SELECT pod_node_id_1 AS `pod_node_hostname_id_1`, pod_node_id_0 AS `pod_node_ip_id_0`, dictGet('flow_tag.device_map', 'hostname', (toUInt64(14),toUInt64(pod_node_id_1))) AS `pod_node_hostname_1`, dictGet('flow_tag.device_map', 'ip', (toUInt64(14),toUInt64(pod_node_id_0))) AS `pod_node_ip_0`, 'pod_node' AS `client_node_type`, 'pod_node' AS `server_node_type`, `client_icon_id`, `server_icon_id` FROM flow_metrics.`network_map.1m` WHERE `time` >= 1705040184 AND `time` <= 1705045184 AND (not(toUInt64(pod_node_id_1) GLOBAL IN (SELECT deviceid FROM flow_tag.device_map WHERE hostname = 'a' AND devicetype=14))) AND (not(pod_node_id_1 = 1)) AND (not(pod_node_id_1 = 2)) AND (pod_node_id_1!=0) AND (pod_node_id_0!=0) AND (pod_node_id_1!=0) AND (pod_node_id_0!=0) GROUP BY `pod_node_id_1`, `pod_node_id_0` LIMIT 5"},
	}, {
		name:       "test_host_ip_exist",
		db:         "flow_metrics",
		datasource: "1m",
		input:      "SELECT region as region FROM `vtap_flow_port` WHERE time>=1705040184 AND time<=1705045184 AND exist(host_ip) GROUP BY region limit 5",
		output:     []string{"SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id))) AS `region` FROM flow_metrics.`network.1m` WHERE `time` >= 1705040184 AND `time` <= 1705045184 AND (host_id!=0) GROUP BY `region_id` LIMIT 5"},
	}, {
		name:       "test_chost_hostname_exist",
		db:         "flow_metrics",
		datasource: "1m",
		input:      "SELECT region_0 as region_0 FROM `vtap_flow_edge_port` WHERE time>=1705040184 AND time<=1705045184 AND exist(chost_hostname_0) GROUP BY region_0 limit 5",
		output:     []string{"SELECT dictGet('flow_tag.region_map', 'name', (toUInt64(region_id_0))) AS `region_0` FROM flow_metrics.`network_map.1m` WHERE `time` >= 1705040184 AND `time` <= 1705045184 AND (l3_device_id_0!=0 AND l3_device_type_0=1) GROUP BY `region_id_0` LIMIT 5"},
	}, {
		name:   "test_show_1",
		db:     "flow_metrics",
		input:  "SHOW tag chost values from vtap_flow_edge_port where chost = ''",
		output: []string{"SELECT id AS `value`, name AS `display_name` FROM flow_tag.`chost_map` WHERE (display_name = '') GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_show_2",
		db:     "flow_metrics",
		input:  "SHOW tag chost values from vtap_flow_edge_port where chost_id = 1",
		output: []string{"SELECT id AS `value`, name AS `display_name` FROM flow_tag.`chost_map` WHERE (value = 1) GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_show_3",
		db:     "flow_metrics",
		input:  "SHOW tag host values from vtap_flow_port where host like '*xx'",
		output: []string{"SELECT deviceid AS `value`, name AS `display_name`, uid FROM flow_tag.`device_map` WHERE (display_name ilike '%xx') AND devicetype = 6 GROUP BY `value`, `display_name`, `uid` ORDER BY length(display_name) asc LIMIT 10000"},
	}, {
		name:   "test_show_host_ip",
		db:     "flow_metrics",
		input:  "SHOW tag host_ip values from vtap_flow_port where host_ip like '*xx'",
		output: []string{"SELECT deviceid AS `value`, ip AS `display_name` FROM flow_tag.`device_map` WHERE (display_name ilike '%xx') AND devicetype = 6 AND not(display_name = '') GROUP BY `value`, `display_name` ORDER BY length(display_name) asc LIMIT 10000"},
	}, {
		name:   "test_show_host_ip_id",
		db:     "flow_metrics",
		input:  "SHOW tag host_ip values from vtap_flow_port where host_ip_id != 1",
		output: []string{"SELECT deviceid AS `value`, ip AS `display_name` FROM flow_tag.`device_map` WHERE (not(value = 1)) AND devicetype = 6 AND not(display_name = '') GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_show_chost_ip_vpc",
		db:     "flow_metrics",
		input:  "SHOW tag chost_ip values from vtap_flow_port where vpc_id != '1'",
		output: []string{"SELECT id AS `value`, ip AS `display_name` FROM flow_tag.`chost_map` WHERE (not(l3_epc_id = '1')) AND not(display_name = '') GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_show_chost_ip",
		db:     "flow_metrics",
		input:  "SHOW tag chost_ip values from network",
		output: []string{"SELECT id AS `value`, ip AS `display_name` FROM flow_tag.`chost_map` WHERE not(display_name = '') GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_show_chost_ip_where",
		db:     "flow_metrics",
		input:  "SHOW tag chost_ip values from vtap_flow_port where chost_ip_id != 1",
		output: []string{"SELECT id AS `value`, ip AS `display_name` FROM flow_tag.`chost_map` WHERE (not(value = 1)) AND not(display_name = '') GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_show_chost_subnet_where",
		db:     "flow_metrics",
		input:  "SHOW tag chost values from vtap_flow_port where subnet_id != 1 and subnet !='xx'",
		output: []string{"SELECT id AS `value`, name AS `display_name` FROM flow_tag.`chost_map` WHERE (not(subnet_id = 1) AND not(toUInt64(subnet_id) GLOBAL IN (SELECT id FROM flow_tag.subnet_map WHERE name = 'xx'))) GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_show_subnet_vpc_where",
		db:     "flow_metrics",
		input:  "SHOW tag subnet values from vtap_flow_port where vpc_id != 1 and vpc !='xx'",
		output: []string{"SELECT id AS `value`, name AS `display_name` FROM flow_tag.`subnet_map` WHERE (not(l3_epc_id = 1) AND not(toUInt64(l3_epc_id) GLOBAL IN (SELECT id FROM flow_tag.l3_epc_map WHERE name = 'xx'))) GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_application_log_body",
		db:     "application_log",
		input:  "SELECT user, user_id FROM log WHERE body!='log' LIMIT 1",
		output: []string{"SELECT dictGet('flow_tag.user_map', 'name', (toUInt64(user_id))) AS `user`, user_id FROM application_log.`log` WHERE NOT (hasToken(body,'log')) LIMIT 1"},
	}, {
		name:   "test_showsql",
		db:     "flow_log",
		input:  "SHOW tag  region values from l7_flow_log",
		output: []string{"SELECT id AS `value`, name AS `display_name` FROM flow_tag.`region_map` GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_showsql",
		db:     "profile",
		input:  "SHOW tag host values from in_process",
		output: []string{"SELECT deviceid AS `value`, name AS `display_name`, uid FROM flow_tag.`device_map` WHERE devicetype = 6 GROUP BY `value`, `display_name`, `uid` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_showsql",
		db:     "flow_metrics",
		input:  "SHOW tag chost values from application",
		output: []string{"SELECT id AS `value`, name AS `display_name` FROM flow_tag.`chost_map` GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:   "test_showsql",
		db:     "event",
		input:  "SHOW tag end_time values from perf_event",
		output: []string{"SELECT field_value AS `value`, value AS `display_name` FROM flow_tag.`event_custom_field_value` WHERE `table` = 'perf_event' AND field_type = 'tag' AND field_name = 'end_time' GROUP BY `value`, `display_name` ORDER BY sum(count) desc LIMIT 10000"},
	}, {
		name:   "test_showsql",
		db:     "prometheus",
		input:  "SHOW tag region values from node_processes_state where id='1'",
		output: []string{"SELECT id AS `value`, name AS `display_name` FROM flow_tag.`region_map` WHERE (display_name = '1') GROUP BY `value`, `display_name` ORDER BY `value` asc LIMIT 10000"},
	}, {
		name:  "test_showsql",
		db:    "deepflow_tenant",
		input: "SHOW tags from deepflow_agent_queue",
		output: []string{
			"SELECT key FROM (SELECT key FROM flow_tag.pod_service_k8s_label_map UNION ALL SELECT key FROM flow_tag.pod_k8s_label_map) GROUP BY key",
			"SELECT key FROM (SELECT key FROM flow_tag.pod_k8s_annotation_map UNION ALL SELECT key FROM flow_tag.pod_service_k8s_annotation_map) GROUP BY key",
			"SELECT key FROM flow_tag.pod_k8s_env_map GROUP BY key",
			"SELECT key FROM (SELECT key FROM flow_tag.chost_cloud_tag_map UNION ALL SELECT key FROM flow_tag.pod_ns_cloud_tag_map) GROUP BY key",
			"SELECT key FROM flow_tag.os_app_tag_map GROUP BY key",
			"SELECT field_name AS tag_name, table FROM flow_tag.deepflow_tenant_custom_field WHERE table='deepflow_agent_queue' AND field_type='tag' GROUP BY tag_name, table ORDER BY tag_name ASC LIMIT 10000",
		},
	}, {
		name:   "test_showsql",
		db:     "deepflow_tenant",
		input:  "SHOW tables",
		output: []string{"SELECT table FROM flow_tag.deepflow_tenant_custom_field GROUP BY table"},
	}, {
		name:   "test_showsql",
		db:     "_prometheus",
		input:  "SHOW tag-values",
		output: []string{"SELECT field_name AS `label_name`, field_value AS `label_value` FROM flow_tag.`prometheus_custom_field_value` GROUP BY `label_name`, `label_value` ORDER BY `label_name` asc LIMIT 10000"},
	}, {
		name:   "test_alert",
		db:     "event",
		input:  "SELECT Count(row), alert_policy, alert_policy_id, event_level, auto_service_0, auto_service_type_0, auto_service_type, auto_service FROM alert_event where auto_service='abc' AND auto_service_type_1=1 GROUP BY alert_policy, alert_policy_id, event_level, auto_service_0, auto_service_type_0, auto_service_type, auto_service LIMIT 1",
		output: []string{"SELECT dictGet('flow_tag.alarm_policy_map', 'name', (toUInt64(policy_id))) AS `alert_policy`, policy_id AS `alert_policy_id`, event_level, tag_string_values[indexOf(tag_string_names,'auto_service_0')] AS `auto_service_0`, tag_int_values[indexOf(tag_int_names,'auto_service_type_0')] AS `auto_service_type_0`, tag_int_values[indexOf(tag_int_names,'auto_service_type')] AS `auto_service_type`, tag_string_values[indexOf(tag_string_names,'auto_service')] AS `auto_service`, COUNT(1) AS `Count(row)` FROM event.`alert_event` WHERE if(indexOf(tag_string_names,'auto_service')=0 AND indexOf(tag_string_names,'auto_service_0')=0 AND indexOf(tag_string_names,'auto_service_1')=0,1!=1,(tag_string_values[indexOf(tag_string_names,'auto_service')] = 'abc' OR tag_string_values[indexOf(tag_string_names,'auto_service_0')] = 'abc' OR tag_string_values[indexOf(tag_string_names,'auto_service_1')] = 'abc')) AND if(indexOf(tag_int_names,'auto_service_type_1')=0,NULL,tag_int_values[indexOf(tag_int_names,'auto_service_type_1')]) = 1 AND (policy_id!=0) GROUP BY `policy_id`, `event_level`, `auto_service_0`, `auto_service_type_0`, `auto_service_type`, `auto_service` LIMIT 1"},
	}}
)

func TestGetSql(t *testing.T) {
	var c *client.Client
	result := &common.Result{}
	args := &common.QuerierParams{}
	monkey.PatchInstanceMethod(reflect.TypeOf(c), "DoQuery", func(*client.Client, *client.QueryParams) (*common.Result, error) {
		return result, nil
	})
	Load()
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	mockDatasources()

	for i, pcase := range parseSQL {
		if len(pcase.output) == 0 {
			pcase.output = append(pcase.output, pcase.input)
		}
		db := pcase.db
		if db == "" {
			db = "flow_log"
		}
		// test language en
		e := CHEngine{DB: db, Language: "en"}
		if pcase.datasource != "" {
			e.DataSource = pcase.datasource
		}
		e.Context = context.Background()
		e.Init()
		var (
			err     error
			out     []string
			sqlList []string
			outSql  string
		)
		if strings.HasPrefix(pcase.input, "WITH") {
			outSql, _, _, err = e.ParseWithSql(pcase.input)
			out = append(out, outSql)
		} else if strings.Contains(pcase.input, "SLIMIT") || strings.Contains(pcase.input, "slimit") {
			outSql, _, _, err = e.ParseSlimitSql(pcase.input, args)
			out = append(out, outSql)
		} else {
			DebugInfo := &client.DebugInfo{}
			if strings.HasPrefix(pcase.input, "SHOW") {
				_, sqlList, _, err = e.ParseShowSql(pcase.input, args, DebugInfo)
				e.DB = "flow_tag"
			}
			if err == nil {
				count := len(DebugInfo.Debug)
				if count > 0 {
					for _, debug := range DebugInfo.Debug {
						out = append(out, debug.Sql)
					}
				} else {
					if len(sqlList) == 0 {
						sqlList = append(sqlList, pcase.input)
					}
					for _, input := range sqlList {
						parser := parse.Parser{Engine: &e}
						err = parser.ParseSQL(input)
						out = append(out, parser.Engine.ToSQLString())
					}
				}
			}
		}
		if !reflect.DeepEqual(out, pcase.output) {
			caseName := pcase.name
			if pcase.name == "" {
				caseName = strconv.Itoa(i)
			}
			if err != nil && pcase.wantErr == err.Error() {
				continue
			}
			t.Errorf("\nParse [%s]\n\t%q \n get: \n\t%q \n want: \n\t%q", caseName, pcase.input, out, pcase.output)
			if err != nil {
				t.Errorf("\nerror %v", err)
			}
		}
	}
}

/* func TestGetSqltest(t *testing.T) {
	 for _, pcase := range parsetest {
		 e := CHEngine{DB: "flow_log"}
		 e.Init()
		 parser := parse.Parser{Engine: &e}
		 parser.ParseSQL(pcase.input)
		 out := parser.Engine.ToSQLString()
		 pp.Println(out)
	 }
 } */

func Load() error {
	ServerCfg := config.DefaultConfig()
	config.Cfg = &ServerCfg.QuerierConfig
	dir := "../../db_descriptions"
	dbDescriptions, err := common.LoadDbDescriptions(dir)
	if err != nil {
		return err
	}
	err = LoadDbDescriptions(dbDescriptions)
	if err != nil {
		return err
	}
	metrics.DB_DESCRIPTIONS = dbDescriptions
	return nil
}

func mockDatasources() {
	httpmock.RegisterResponder(
		"GET", "http://localhost:20417/v1/data-sources/",
		func(req *http.Request) (*http.Response, error) {
			values := req.URL.Query()
			name := values.Get("name")
			if name == "" {
				name = "1s"
			}

			return httpmock.NewStringResponse(200,
				fmt.Sprintf(`{"DATA":[{"NAME":"%s","INTERVAL":%d}]}`,
					name, convertNameToInterval(name)),
			), nil
		},
	)
}

func convertNameToInterval(name string) (interval int) {
	switch name {
	case "1s":
		return 1
	case "1m":
		return 60
	case "1h":
		return 3600
	case "1d":
		return 86400
	default:
		return 0
	}
}
