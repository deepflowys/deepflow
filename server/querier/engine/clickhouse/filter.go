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
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/Knetic/govaluate"
	"github.com/deepflowio/deepflow/server/libs/utils"
	"github.com/deepflowio/deepflow/server/querier/common"
	"github.com/deepflowio/deepflow/server/querier/config"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/client"
	chCommon "github.com/deepflowio/deepflow/server/querier/engine/clickhouse/common"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/tag"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/trans_prometheus"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/view"
	"github.com/xwb1989/sqlparser"
	"inet.af/netaddr"
)

type Where struct {
	filter *view.Filters
	withs  []view.Node
	time   *view.Time
}

func (w *Where) Format(m *view.Model) {
	w.filter.Withs = w.withs
	if !w.filter.IsNull() {
		m.AddFilter(w.filter)
	}
}

type Having struct {
	Where
}

func (h *Having) Format(m *view.Model) {
	h.filter.Withs = h.withs
	if !h.filter.IsNull() {
		m.AddHaving(h.filter)
	}
}

func GetWhere(name, value string) WhereStatement {
	switch name {
	case "time":
		return &TimeTag{Value: value}
	default:
		return &WhereTag{Tag: name, Value: value}
	}
}

func TransWhereTagFunction(db, table string, name string, args []string) (filter string) {
	funcName := strings.ToLower(name)
	switch funcName {
	case "exist":
		// Unsupported tags, filter is 1 = 1
		filter = "1=1"
		if db == "flow_tag" {
			return
		}
		if len(args) != 1 {
			errorMessage := fmt.Sprintf("The parameters of function %s are not 1", funcName)
			log.Error(errorMessage)
			return
		}
		resource := strings.ToLower(strings.Trim(args[0], "`"))
		suffix := ""
		resourceNoSuffix := resource
		if slices.Contains([]string{"l4_flow_log", "l7_flow_log", "application_map", "network_map", "vtap_flow_edge_port", "vtap_app_edge_port"}, table) {
			if strings.HasSuffix(resource, "_0") {
				suffix = "_0"
				resourceNoSuffix = strings.TrimSuffix(resourceNoSuffix, "_0")
			} else if strings.HasSuffix(resource, "_1") {
				suffix = "_1"
				resourceNoSuffix = strings.TrimSuffix(resourceNoSuffix, "_1")
			}
		}
		resourceNoID := strings.TrimSuffix(resourceNoSuffix, "_id")
		deviceTypeValue, ok := tag.DEVICE_MAP[resourceNoID]
		if ok {
			if resourceNoID == "pod_service" {
				serviceTagSuffix := "service_id" + suffix
				filter = fmt.Sprintf("%s != 0", serviceTagSuffix)
			} else {
				deviceTypeTagSuffix := "l3_device_type" + suffix
				filter = fmt.Sprintf("%s=%d", deviceTypeTagSuffix, deviceTypeValue)
			}
			return
		} else if strings.HasPrefix(resourceNoID, "k8s.label.") {
			podIDSuffix := "pod_id" + suffix
			serviceIDSuffix := "service_id" + suffix
			tagNoPreffix := strings.TrimPrefix(resourceNoID, "k8s.label.")
			filter = fmt.Sprintf("((toUInt64(%s) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_label_map WHERE key='%s')) OR (toUInt64(%s) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_label_map WHERE key='%s')))", serviceIDSuffix, tagNoPreffix, podIDSuffix, tagNoPreffix)
		} else if strings.HasPrefix(resourceNoID, "k8s.annotation.") {
			podIDSuffix := "pod_id" + suffix
			serviceIDSuffix := "service_id" + suffix
			tagNoPreffix := strings.TrimPrefix(resourceNoID, "k8s.annotation.")
			filter = fmt.Sprintf("((toUInt64(%s) GLOBAL IN (SELECT id FROM flow_tag.pod_service_k8s_annotation_map WHERE key='%s')) OR (toUInt64(%s) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_annotation_map WHERE key='%s')))", serviceIDSuffix, tagNoPreffix, podIDSuffix, tagNoPreffix)
		} else if strings.HasPrefix(resourceNoID, "k8s.env.") {
			podIDSuffix := "pod_id" + suffix
			tagNoPreffix := strings.TrimPrefix(resourceNoID, "k8s.env.")
			filter = fmt.Sprintf("toUInt64(%s) GLOBAL IN (SELECT id FROM flow_tag.pod_k8s_env_map WHERE key='%s')", podIDSuffix, tagNoPreffix)
		} else if strings.HasPrefix(resourceNoID, "cloud.tag.") {
			deviceIDSuffix := "l3_device_id" + suffix
			deviceTypeSuffix := "l3_device_type" + suffix
			podNSIDSuffix := "pod_ns_id" + suffix
			tagNoPreffix := strings.TrimPrefix(resourceNoID, "cloud.tag.")
			filter = fmt.Sprintf("((toUInt64(%s) GLOBAL IN (SELECT id FROM flow_tag.chost_cloud_tag_map WHERE key='%s') AND %s=1) OR (toUInt64(%s) GLOBAL IN (SELECT id FROM flow_tag.pod_ns_cloud_tag_map WHERE key='%s')))", deviceIDSuffix, tagNoPreffix, deviceTypeSuffix, podNSIDSuffix, tagNoPreffix)
		} else if strings.HasPrefix(resourceNoID, "os.app.") {
			processIDSuffix := "gprocess_id" + suffix
			tagNoPreffix := strings.TrimPrefix(resourceNoID, "os.app.")
			filter = fmt.Sprintf("toUInt64(%s) GLOBAL IN (SELECT pid FROM flow_tag.os_app_tag_map WHERE key='%s')", processIDSuffix, tagNoPreffix)
		} else if deviceTypeValue, ok = tag.TAP_PORT_DEVICE_MAP[resourceNoID]; ok {
			filter = fmt.Sprintf("(toUInt64(agent_id),toUInt64(capture_nic)) GLOBAL IN (SELECT vtap_id,tap_port FROM flow_tag.vtap_port_map WHERE tap_port!=0 AND device_type=%d)", deviceTypeValue)
		} else if common.IsValueInSliceString(resourceNoID, tag.TAG_RESOURCE_TYPE_DEFAULT) ||
			resourceNoID == "host" || resourceNoID == "service" {
			filter = strings.Join([]string{resourceNoID, "_id", suffix, "!=0"}, "")
		} else if resourceNoID == "vpc" {
			filter = strings.Join([]string{"l3_epc_id", suffix, "!=-2"}, "")
		} else if resourceNoID == "l2_vpc" {
			filter = strings.Join([]string{"epc_id", suffix, "!=0"}, "")
		} else if common.IsValueInSliceString(resourceNoID, tag.TAG_RESOURCE_TYPE_AUTO) {
			if resourceNoID == "auto_instance" {
				filter = strings.Join([]string{"auto_instance_type", suffix, " not in (101,102)"}, "")
			} else {
				filter = strings.Join([]string{"auto_service_type", suffix, " not in (10)"}, "")
			}
		} else if resourceInfo, ok := tag.HOSTNAME_IP_DEVICE_MAP[resourceNoID]; ok {
			deviceTypeValue = resourceInfo.ResourceType
			deviceTypeValueStr := strconv.Itoa(deviceTypeValue)
			if deviceTypeValue == tag.VIF_DEVICE_TYPE_VM {
				filter = "l3_device_id" + suffix + "!=0 AND l3_device_type" + suffix + "=" + deviceTypeValueStr
			} else {
				filter = resourceInfo.ResourceName + "_id" + suffix + "!=0"
			}
		} else {
			// non-resource tags
			engine := &CHEngine{DB: db, Table: table}
			notNullExpr, ok := GetNotNullFilter(args[0], engine)
			if !ok {
				return
			}
			filter = notNullExpr.(*view.Expr).Value
		}
	}
	return
}

type WhereStatement interface {
	Trans(sqlparser.Expr, *Where, *CHEngine) (view.Node, error)
}

type WhereTag struct {
	Tag   string
	Value string
}

// k8s.label, k8s.annotation, cloud.tag
func TransLabelFilter(translator, regexpTranslator, key, op, value string) (filter string) {
	switch strings.ToLower(op) {
	case "match":
		filter = fmt.Sprintf(regexpTranslator, op, value, key, op, value, key)
	case "not match":
		filter = "not(" + fmt.Sprintf(regexpTranslator, "match", value, key, "match", value, key) + ")"
	case "not ilike":
		filter = "not(" + fmt.Sprintf(translator, "ilike", value, key, "ilike", value, key) + ")"
	case "not in":
		filter = "not(" + fmt.Sprintf(translator, "in", value, key, "in", value, key) + ")"
	case "!=":
		filter = "not(" + fmt.Sprintf(translator, "=", value, key, "=", value, key) + ")"
	default:
		filter = fmt.Sprintf(translator, op, value, key, op, value, key)
	}
	return
}

// k8s.env, os.app
func TransEnvFilter(translator, regexpTranslator, key, op, value string) (filter string) {
	switch strings.ToLower(op) {
	case "match":
		filter = fmt.Sprintf(regexpTranslator, op, value, key)
	case "not match":
		filter = "not(" + fmt.Sprintf(regexpTranslator, "match", value, key) + ")"
	case "not ilike":
		filter = "not(" + fmt.Sprintf(translator, "ilike", value, key) + ")"
	case "not in":
		filter = "not(" + fmt.Sprintf(translator, "in", value, key) + ")"
	case "!=":
		filter = "not(" + fmt.Sprintf(translator, "=", value, key) + ")"
	default:
		filter = fmt.Sprintf(translator, op, value, key)
	}
	return
}

// service, chost, chost_hostname, chost_ip, router, dhcpgw, redis, rds, lb_listener,
// natgw, lb, host, host_hostname, host_ip, pod_node, pod_node_hostname, pod_node_ip,
// pod_group_type
func TransChostFilter(translator, regexpTranslator, op, value string) (filter string) {
	switch strings.ToLower(op) {
	case "match":
		filter = fmt.Sprintf(regexpTranslator, op, value)
	case "not match":
		filter = "not(" + fmt.Sprintf(regexpTranslator, "match", value) + ")"
	case "not ilike":
		filter = "not(" + fmt.Sprintf(translator, "ilike", value) + ")"
	case "not in":
		filter = "not(" + fmt.Sprintf(translator, "in", value) + ")"
	case "!=":
		filter = "not(" + fmt.Sprintf(translator, "=", value) + ")"
	default:
		filter = fmt.Sprintf(translator, op, value)
	}
	return
}

func TransAlertEventNoSuffixFilter(translator, regexpTranslator, op, value string) (filter string) {
	switch strings.ToLower(op) {
	case "match":
		filter = fmt.Sprintf(regexpTranslator, op, value, op, value, op, value)
	case "not match":
		filter = "not(" + fmt.Sprintf(regexpTranslator, "match", value, "match", value, "match", value) + ")"
	case "not ilike":
		filter = "not(" + fmt.Sprintf(translator, "ilike", value, "ilike", value, "ilike", value) + ")"
	case "not in":
		filter = "not(" + fmt.Sprintf(translator, "in", value, "in", value, "in", value) + ")"
	case "!=":
		filter = "not(" + fmt.Sprintf(translator, "=", value, "=", value, "=", value) + ")"
	default:
		filter = fmt.Sprintf(translator, op, value, op, value, op, value)
	}
	return
}

// pod_ingress, pod_service, ip
// x_request_id, syscall_thread, syscall_coroutine, syscall_cap_seq, syscall_trace_id, tcp_seq
func TransIngressFilter(translator, regexpTranslator, op, value string) (filter string) {
	switch strings.ToLower(op) {
	case "match":
		filter = fmt.Sprintf(regexpTranslator, op, value, op, value)
	case "not match":
		filter = "not(" + fmt.Sprintf(regexpTranslator, "match", value, "match", value) + ")"
	case "not ilike":
		filter = "not(" + fmt.Sprintf(translator, "ilike", value, "ilike", value) + ")"
	case "not in":
		filter = "not(" + fmt.Sprintf(translator, "in", value, "in", value) + ")"
	case "!=":
		filter = "not(" + fmt.Sprintf(translator, "=", value, "=", value) + ")"
	default:
		filter = fmt.Sprintf(translator, op, value, op, value)
	}
	return
}

// The tag is not in the tagResourceMap default
func TransTagFilter(whereTag, postAsTag, value, op, db, table, originFilter string, isRemoteRead bool, e *CHEngine) (filter string, err error) {
	tagItem := &tag.Tag{}
	ok := false
	noSuffixTag := strings.TrimSuffix(whereTag, "_0")
	noSuffixTag = strings.TrimSuffix(noSuffixTag, "_1")
	noIDTag := strings.TrimSuffix(noSuffixTag, "_id")
	switch noIDTag {
	case "mac", "tunnel_tx_mac", "tunnel_rx_mac":
		macValue := strings.TrimLeft(value, "(")
		macValue = strings.TrimRight(macValue, ")")
		macSlice := strings.Split(macValue, ",")
		macs := []string{}
		for _, valueStr := range macSlice {
			valueStr = strings.TrimSpace(valueStr)
			valueStr = strings.Trim(valueStr, "'")
			mac, err := net.ParseMAC(valueStr)
			if err != nil {
				return filter, err
			}
			valueUInt64 := utils.Mac2Uint64(mac)
			macs = append(macs, fmt.Sprintf("'%v'", valueUInt64))
		}
		if len(macs) != 0 {
			macsStr := strings.Join(macs, ",")
			if strings.ToLower(op) == "in" || strings.ToLower(op) == "not in" {
				macsStr = "(" + macsStr + ")"
			}
			if slices.Contains([]string{"tunnel_tx_mac", "tunnel_rx_mac"}, whereTag) {
				switch strings.ToLower(op) {
				case "!=":
					filter = fmt.Sprintf("not (%s_0 %s %s OR %s_1 %s %s)", postAsTag, "=", macsStr, whereTag, "=", macsStr)
				case "not in":
					filter = fmt.Sprintf("not (%s_0 %s %s OR %s_1 %s %s)", postAsTag, "in", macsStr, whereTag, "in", macsStr)
				default:
					filter = fmt.Sprintf("(%s_0 %s %s OR %s_1 %s %s)", postAsTag, op, macsStr, whereTag, op, macsStr)
				}
			} else {
				filter = fmt.Sprintf("%s %s %s", postAsTag, op, macsStr)
			}
		}
	case "tap_port", "capture_nic":
		if whereTag == "tap_port" {
			whereTag = "capture_nic"
		}
		macValue := strings.TrimLeft(value, "(")
		macValue = strings.TrimRight(macValue, ")")
		macSlice := strings.Split(macValue, ",")
		macs := []string{}
		for _, valueStr := range macSlice {
			valueStr = strings.TrimSpace(valueStr)
			valueStr = strings.Trim(valueStr, "'")
			ip := net.ParseIP(valueStr)
			if ip != nil {
				ip4 := ip.To4()
				if ip4 != nil {
					ipUint32 := utils.IpToUint32(ip4)
					macs = append(macs, fmt.Sprintf("'%v'", ipUint32))
				} else {
					return filter, fmt.Errorf("invalid ipv4 mac: %s", valueStr)
				}
			} else {
				macValueStr := "00:00:" + valueStr
				mac, err := net.ParseMAC(macValueStr)
				if err != nil {
					macs = append(macs, fmt.Sprintf("'%v'", valueStr))
				} else {
					valueUInt64 := utils.Mac2Uint64(mac)
					macs = append(macs, fmt.Sprintf("'%v'", valueUInt64))
				}
			}
		}
		if len(macs) != 0 {
			macsStr := strings.Join(macs, ",")
			if strings.ToLower(op) == "in" || strings.ToLower(op) == "not in" {
				macsStr = "(" + macsStr + ")"
			}
			filter = fmt.Sprintf("%s %s %s", postAsTag, op, macsStr)
		}
	case "body":
		if strings.Contains(op, "match") {
			filter = fmt.Sprintf("%s(%s,%s)", op, postAsTag, value)
		} else {
			filter = fmt.Sprintf("%s %s %s", postAsTag, op, value)
		}
		switch strings.ToLower(op) {
		case "=", "!=":
			tokenRegStr := "^[\u4E00-\u9FA5a-zA-Z0-9\\s]+$"
			tokenReg := regexp.MustCompile(tokenRegStr)
			if !tokenReg.MatchString(strings.Trim(value, "'")) {
				tokenErr := fmt.Errorf("body can only contain letters or numbers and be separated by whitespace， please check:  %s", value)
				return "", tokenErr
			}
			if strings.Contains(value, " ") {
				valueSlice := strings.Split(strings.Trim(value, "'"), " ")
				var filterSlice []string
				for _, token := range valueSlice {
					filterSlice = append(filterSlice, fmt.Sprintf("%s(%s,'%s')", "hasToken", postAsTag, token))
				}
				filter = strings.Join(filterSlice, " AND ")
			} else {
				filter = fmt.Sprintf("%s(%s,%s)", "hasToken", postAsTag, value)
			}
			if op == "!=" {
				filter = fmt.Sprintf("NOT (%s)", filter)
			}
		}
	default:
		tagName := strings.Trim(whereTag, "`")
		// map item tag
		nameNoPreffix, _, transKey := common.TransMapItem(tagName, table)
		if transKey != "" {
			tagItem, _ = tag.GetTag(transKey, db, table, "default")
			if strings.HasPrefix(tagName, "os.app.") || strings.HasPrefix(tagName, "k8s.env.") {
				filter = TransEnvFilter(tagItem.WhereTranslator, tagItem.WhereRegexpTranslator, nameNoPreffix, op, value)
			} else {
				filter = TransLabelFilter(tagItem.WhereTranslator, tagItem.WhereRegexpTranslator, nameNoPreffix, op, value)
			}
		} else if strings.HasPrefix(tagName, "tag.") || strings.HasPrefix(tagName, "attribute.") {
			if strings.HasPrefix(tagName, "tag.") {
				if isRemoteRead {
					filter, err := GetRemoteReadFilter(tagName, table, op, value, originFilter, e)
					if err != nil {
						return filter, err
					}
				}
				if db == chCommon.DB_NAME_PROMETHEUS {
					filter, err = GetPrometheusFilter(tagName, table, op, value, e)
					if err != nil {
						return filter, err
					}
				} else {
					tagItem, ok = tag.GetTag("tag.", db, table, "default")
				}
			} else {
				tagItem, ok = tag.GetTag("attribute.", db, table, "default")
			}
			if ok {
				nameNoPreffix := strings.TrimPrefix(tagName, "tag.")
				nameNoPreffix = strings.TrimPrefix(nameNoPreffix, "attribute.")
				nameNoPreffix = strings.Trim(nameNoPreffix, "`")
				if strings.Contains(op, "match") {
					filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, op, nameNoPreffix, value)
				} else {
					filter = fmt.Sprintf(tagItem.WhereTranslator, nameNoPreffix, op, value)
				}
			}
		} else {
			if strings.Contains(op, "match") {
				filter = fmt.Sprintf("%s(%s,%s)", op, postAsTag, value)
			} else {
				filter = fmt.Sprintf("%s %s %s", postAsTag, op, value)
			}
		}
	}
	return
}

func (t *WhereTag) Trans(expr sqlparser.Expr, w *Where, e *CHEngine) (view.Node, error) {
	op := expr.(*sqlparser.ComparisonExpr).Operator
	asTagMap := e.AsTagMap
	db := e.DB
	table := e.Table
	isRemoteRead := false
	remoteRead := e.Context.Value("remote_read")
	if remoteRead != nil {
		isRemoteRead = remoteRead.(bool)
	}
	originFilter := sqlparser.String(expr)
	filter := ""
	var err error

	whereTag := t.Tag
	if strings.ToLower(op) == "like" || strings.ToLower(op) == "not like" {
		t.Value = strings.ReplaceAll(t.Value, "*", "%")
		if strings.ToLower(op) == "like" {
			op = "ilike"
		} else {
			op = "not ilike"
		}
	} else if strings.ToLower(op) == "regexp" || strings.ToLower(op) == "not regexp" {
		// check regexp format
		// 检查正则表达式格式
		_, err := regexp.Compile(strings.Trim(t.Value, "'"))
		if err != nil {
			error := fmt.Errorf("%s : %s", err, t.Value)
			return nil, error
		}
		if strings.ToLower(op) == "regexp" {
			op = "match"
		} else {
			op = "not match"
		}
	}
	if db == "flow_tag" {
		if t.Tag == "vpc" || t.Tag == "vpc_id" {
			t.Tag = strings.Replace(t.Tag, "vpc", "l3_epc", 1)
		}
		filter := ""
		switch t.Tag {
		case "value", "devicetype", "device_type", "tag_name", "field_name", "field_type", "1", "user_id", "team_id", "app_service", "app_instance":
			if table == "user_map" && t.Tag == "user_id" {
				tagItem, ok := tag.GetTag("value", db, table, "default")
				if ok {
					switch strings.ToLower(op) {
					case "match":
						filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value)
					case "not match":
						filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value) + ")"
					case "not ilike":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "ilike", t.Value) + ")"
					case "not in":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "in", t.Value) + ")"
					case "!=":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "=", t.Value) + ")"
					default:
						filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
					}
					return &view.Expr{Value: filter}, nil
				}
			} else {
				filter = fmt.Sprintf("%s %s %s", t.Tag, op, t.Value)
			}

		case "type":
			if table == "vtap_map" {
				filter = fmt.Sprintf("%s %s %s", t.Tag, op, t.Value)
			} else {
				tagItem, ok := tag.GetTag("value", db, table, "default")
				if ok {
					switch strings.ToLower(op) {
					case "match":
						filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value)
					case "not match":
						filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value) + ")"
					case "not ilike":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "ilike", t.Value) + ")"
					case "not in":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "in", t.Value) + ")"
					case "!=":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "=", t.Value) + ")"
					default:
						filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
					}
					return &view.Expr{Value: filter}, nil
				}
			}
		case "key", "table":
			filter = fmt.Sprintf("`%s` %s %s", t.Tag, op, t.Value)
		case "display_name":
			tagItem, ok := tag.GetTag("display_name", db, table, "default")
			if ok {
				switch strings.ToLower(op) {
				case "match":
					filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value)
				case "not match":
					filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value) + ")"
				case "not ilike":
					filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "ilike", t.Value) + ")"
				case "not in":
					filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "in", t.Value) + ")"
				case "!=":
					filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "=", t.Value) + ")"
				default:
					filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
				}
			}
		default:
			switch table {
			case "int_enum_map", "string_enum_map":
				tagItem, ok := tag.GetTag("enum_tag_id", db, table, "default")
				if ok {
					switch strings.ToLower(op) {
					case "match":
						filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value)
					case "not match":
						filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value) + ")"
					case "not ilike":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "ilike", t.Value) + ")"
					case "not in":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "in", t.Value) + ")"
					case "!=":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "=", t.Value) + ")"
					default:
						filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
					}
					return &view.Expr{Value: filter}, nil
				}
			case "ip_relation_map":
				tagItem, ok := tag.GetTag("value", db, table, "default")
				if ok {
					switch strings.ToLower(op) {
					case "match":
						filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value)
					case "not match":
						filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value) + ")"
					case "not ilike":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "ilike", t.Value) + ")"
					case "not in":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "in", t.Value) + ")"
					case "!=":
						filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "=", t.Value) + ")"
					default:
						filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
					}
					return &view.Expr{Value: filter}, nil
				}
			case "pod_ns_map", "pod_group_map", "pod_service_map", "pod_map", "chost_map", "gprocess_map", "pod_ingress_map", "pod_node_map", "subnet_map":
				checkTag := strings.TrimSuffix(t.Tag, "_id")
				if slices.Contains(chCommon.SHOW_TAG_VALUE_MAP[table], checkTag) {
					if strings.HasSuffix(t.Tag, "_id") {
						if checkTag == strings.TrimSuffix(table, "_map") || checkTag == common.CHOST_HOSTNAME || checkTag == common.CHOST_IP {
							tagItem, ok := tag.GetTag("value", db, table, "default")
							if ok {
								switch strings.ToLower(op) {
								case "match":
									filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value)
								case "not match":
									filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value) + ")"
								case "not ilike":
									filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "ilike", t.Value) + ")"
								case "not in":
									filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "in", t.Value) + ")"
								case "!=":
									filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "=", t.Value) + ")"
								default:
									filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
								}
								return &view.Expr{Value: filter}, nil
							}
						} else {
							tagItem, ok := tag.GetTag("other_id", db, table, "default")
							if ok {
								switch strings.ToLower(op) {
								case "match":
									filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, t.Tag, "match", t.Value)
								case "not match":
									filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, t.Tag, "match", t.Value) + ")"
								case "not ilike":
									filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, t.Tag, "ilike", t.Value) + ")"
								case "not in":
									filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, t.Tag, "in", t.Value) + ")"
								case "!=":
									filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, t.Tag, "=", t.Value) + ")"
								default:
									filter = fmt.Sprintf(tagItem.WhereTranslator, t.Tag, op, t.Value)
								}
							}
						}
					} else {
						if t.Tag == strings.TrimSuffix(table, "_map") || t.Tag == common.CHOST_HOSTNAME || t.Tag == common.CHOST_IP {
							tagItem, ok := tag.GetTag("display_name", db, table, "default")
							if ok {
								switch strings.ToLower(op) {
								case "match":
									filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value)
								case "not match":
									filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value) + ")"
								case "not ilike":
									filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "ilike", t.Value) + ")"
								case "not in":
									filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "in", t.Value) + ")"
								case "!=":
									filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "=", t.Value) + ")"
								default:
									filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
								}
							}
						} else {
							if t.Tag == "host" {
								tagItem, ok := tag.GetTag("device_name", db, table, "default")
								if ok {
									deviceType, ok := tag.TAG_RESOURCE_TYPE_DEVICE_MAP[t.Tag]
									if ok {
										switch strings.ToLower(op) {
										case "match":
											filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, t.Tag, "match", t.Value, deviceType)
										case "not match":
											filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, t.Tag, "match", t.Value, deviceType) + ")"
										case "not ilike":
											filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, t.Tag, "ilike", t.Value, deviceType) + ")"
										case "not in":
											filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, t.Tag, "in", t.Value, deviceType) + ")"
										case "!=":
											filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, t.Tag, "=", t.Value, deviceType) + ")"
										default:
											filter = fmt.Sprintf(tagItem.WhereTranslator, t.Tag, op, t.Value, deviceType)
										}
									}
								}
							} else {
								tagItem, ok := tag.GetTag("other_name", db, table, "default")
								if ok {
									tagMap := t.Tag + "_map"
									if t.Tag == "vpc" || t.Tag == "l2_vpc" {
										tagMap = "l3_epc_map"
									}
									switch strings.ToLower(op) {
									case "match":
										filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, t.Tag, tagMap, "match", t.Value)
									case "not match":
										filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, t.Tag, tagMap, "match", t.Value) + ")"
									case "not ilike":
										filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, t.Tag, tagMap, "ilike", t.Value) + ")"
									case "not in":
										filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, t.Tag, tagMap, "in", t.Value) + ")"
									case "!=":
										filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, t.Tag, tagMap, "=", t.Value) + ")"
									default:
										filter = fmt.Sprintf(tagItem.WhereTranslator, t.Tag, tagMap, op, t.Value)
									}
								}
							}
						}
					}
				} else {
					error := errors.New(fmt.Sprintf("show tag %s values not support filter tag: %s", strings.TrimSuffix(table, "_map"), t.Tag))
					return nil, error
				}
			default:
				if strings.HasPrefix(t.Tag, "tag.") || strings.HasPrefix(t.Tag, "attribute.") || strings.HasPrefix(t.Tag, "k8s.label.") || strings.HasPrefix(t.Tag, "k8s.env.") || strings.HasPrefix(t.Tag, "k8s.annotation.") || strings.HasPrefix(t.Tag, "cloud.tag.") || strings.HasPrefix(t.Tag, "os.app.") {
					tagItem, ok := tag.GetTag("value", db, table, "default")
					if ok {
						switch strings.ToLower(op) {
						case "match":
							filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value)
						case "not match":
							filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value) + ")"
						case "not ilike":
							filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "ilike", t.Value) + ")"
						case "not in":
							filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "in", t.Value) + ")"
						case "!=":
							filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "=", t.Value) + ")"
						default:
							filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
						}
					}
				} else {
					tagItem, ok := tag.GetTag("value", db, table, "default")
					if !strings.HasSuffix(t.Tag, "_id") {
						tagItem, ok = tag.GetTag("display_name", db, table, "default")
					}
					if ok {
						switch strings.ToLower(op) {
						case "match":
							filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value)
						case "not match":
							filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", t.Value) + ")"
						case "not ilike":
							filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "ilike", t.Value) + ")"
						case "not in":
							filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "in", t.Value) + ")"
						case "!=":
							filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, "=", t.Value) + ")"
						default:
							filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
						}
					} else {
						filter = fmt.Sprintf("`%s` %s %s", t.Tag, op, t.Value)
					}
				}
			}
		}
		return &view.Expr{Value: filter}, nil
	} else if table == "alert_event" {
		tagName := strings.Trim(t.Tag, "`")
		tagItem, ok := tag.GetTag(tagName, db, table, "default")
		if !ok {
			preAsTag, ok := asTagMap[t.Tag]
			if ok {
				tagName = strings.Trim(preAsTag, "`")
				tagItem, ok = tag.GetTag(tagName, db, table, "default")
			}
		}
		noSuffixTag := strings.TrimSuffix(tagName, "_0")
		noSuffixTag = strings.TrimSuffix(noSuffixTag, "_1")
		noIDTag := noSuffixTag
		if !slices.Contains([]string{"_id", "x_request_id", "syscall_trace_id"}, noSuffixTag) {
			noIDTag = strings.TrimSuffix(noSuffixTag, "_id")
		}
		if ok {
			switch noIDTag {
			case "service", "chost", "router", "dhcpgw", "redis", "rds", "lb_listener", "natgw", "lb", "host", "pod_node", "region", "az",
				"pod_ns", "pod_group", "pod", "pod_cluster", "subnet", "gprocess", "pod_ingress", "pod_service", "ip", "vpc", "l2_vpc",
				"auto_instance", "auto_service", "auto_instance_type", "auto_service_type":
				if !strings.HasSuffix(strings.Trim(t.Tag, "`"), "_0") && !strings.HasSuffix(strings.Trim(t.Tag, "`"), "_1") {
					filter = TransAlertEventNoSuffixFilter(tagItem.WhereTranslator, tagItem.WhereRegexpTranslator, op, t.Value)
				} else {
					filter = TransChostFilter(tagItem.WhereTranslator, tagItem.WhereRegexpTranslator, op, t.Value)
				}
			case "alert_policy", "user":
				filter = TransChostFilter(tagItem.WhereTranslator, tagItem.WhereRegexpTranslator, op, t.Value)
			default:
				if strings.Contains(op, "match") {
					filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, op, t.Value)
				} else {
					filter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
				}
			}
		} else {
			switch noIDTag {
			case "pod_group_type", "host_ip", "host_hostname", "chost_ip", "chost_hostname", "pod_node_ip", "pod_node_hostname", "province",
				"is_internet", "tcp_flags_bit", "l2_end", "l3_end", "nat_real_ip", "nat_real_port", "process_id", "process_kname", "k8s.label",
				"k8s.annotation", "k8s.env", "cloud.tag", "os.app":
				_, err := strconv.Atoi(t.Value)
				if strings.HasSuffix(strings.Trim(t.Tag, "`"), "_0") || strings.HasSuffix(strings.Trim(t.Tag, "`"), "_1") {
					if err != nil {
						tagItem, ok = tag.GetTag("string_tags", db, table, "default")
					} else {
						tagItem, ok = tag.GetTag("int_tags", db, table, "default")
					}
					if strings.Contains(op, "match") {
						filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, op, tagName, tagName, t.Value)
					} else {
						filter = fmt.Sprintf(tagItem.WhereTranslator, tagName, tagName, op, t.Value)
					}
				} else {
					if err != nil {
						tagItem, ok = tag.GetTag("string_tags_no_suffix", db, table, "default")
					} else {
						tagItem, ok = tag.GetTag("int_tags_no_suffix", db, table, "default")
					}
					if strings.Contains(op, "match") {
						filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, op, tagName, tagName, t.Value, op, tagName, tagName, t.Value, op, tagName, tagName, t.Value)
					} else {
						filter = fmt.Sprintf(tagItem.WhereTranslator, tagName, tagName, op, t.Value, tagName, tagName, op, t.Value, tagName, tagName, op, t.Value)
					}
				}
			default:
				if strings.HasPrefix(strings.Trim(t.Tag, "`"), "k8s.label.") || strings.HasPrefix(strings.Trim(t.Tag, "`"), "k8s.annotation.") || strings.HasPrefix(strings.Trim(t.Tag, "`"), "k8s.env.") || strings.HasPrefix(strings.Trim(t.Tag, "`"), "cloud.tag.") || strings.HasPrefix(strings.Trim(t.Tag, "`"), "kos.app.") {
					_, err := strconv.Atoi(t.Value)
					if strings.HasSuffix(strings.Trim(t.Tag, "`"), "_0") || strings.HasSuffix(strings.Trim(t.Tag, "`"), "_1") {
						if err != nil {
							tagItem, ok = tag.GetTag("string_tags", db, table, "default")
						} else {
							tagItem, ok = tag.GetTag("int_tags", db, table, "default")
						}
						if strings.Contains(op, "match") {
							filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, op, tagName, tagName, t.Value)
						} else {
							filter = fmt.Sprintf(tagItem.WhereTranslator, tagName, tagName, op, t.Value)
						}
					} else {
						if err != nil {
							tagItem, ok = tag.GetTag("string_tags_no_suffix", db, table, "default")
						} else {
							tagItem, ok = tag.GetTag("int_tags_no_suffix", db, table, "default")
						}
						if strings.Contains(op, "match") {
							filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, op, tagName, tagName, t.Value, op, tagName, tagName, t.Value, op, tagName, tagName, t.Value)
						} else {
							filter = fmt.Sprintf(tagItem.WhereTranslator, tagName, tagName, op, t.Value, tagName, tagName, op, t.Value, tagName, tagName, op, t.Value)
						}
					}
				} else {
					switch strings.Trim(t.Tag, "`") {
					case "policy_type", "metric_value", "event_level", "team_id", "user_id", "target_tags", "_query_region", "_target_uid", "1", "_id":
						if strings.Contains(op, "match") {
							filter = fmt.Sprintf("%s(%s,%s)", op, t.Tag, t.Value)
						} else {
							filter = fmt.Sprintf("%s %s %s", t.Tag, op, t.Value)
						}
					default:
						_, err := strconv.Atoi(t.Value)
						if err != nil {
							tagItem, ok = tag.GetTag("string_tags", db, table, "default")
						} else {
							tagItem, ok = tag.GetTag("int_tags", db, table, "default")
						}
						if strings.Contains(op, "match") {
							filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, op, tagName, tagName, t.Value)
						} else {
							filter = fmt.Sprintf(tagItem.WhereTranslator, tagName, tagName, op, t.Value)
						}
					}
				}
			}
		}
		return &view.Expr{Value: filter}, nil
	} else {
		if t.Tag == "tap_port" {
			t.Tag = "capture_nic"
		}
		tagItem, ok := tag.GetTag(strings.Trim(t.Tag, "`"), db, table, "default")
		if !ok {
			preAsTag, ok := asTagMap[t.Tag]
			if ok {
				whereTag = preAsTag
				tagItem, ok = tag.GetTag(strings.Trim(preAsTag, "`"), db, table, "default")
				if !ok {
					filter, err = TransTagFilter(whereTag, t.Tag, t.Value, op, db, table, originFilter, isRemoteRead, e)
					if err != nil {
						return nil, err
					}
					return &view.Expr{Value: filter}, nil
				}
			} else {
				filter, err = TransTagFilter(whereTag, t.Tag, t.Value, op, db, table, originFilter, isRemoteRead, e)
				if err != nil {
					return nil, err
				}
				return &view.Expr{Value: filter}, nil
			}
		}
		// Only vtap_acl translate policy_id
		if strings.Trim(t.Tag, "`") == "policy_id" && table != chCommon.TABLE_NAME_VTAP_ACL {
			filter = fmt.Sprintf("%s %s %s", t.Tag, op, t.Value)
			return &view.Expr{Value: filter}, nil
		}
		whereFilter := tagItem.WhereTranslator
		if whereFilter != "" {
			noSuffixTag := strings.TrimSuffix(whereTag, "_0")
			noSuffixTag = strings.TrimSuffix(noSuffixTag, "_1")
			noIDTag := noSuffixTag
			if !slices.Contains([]string{"_id", "x_request_id", "syscall_trace_id"}, noSuffixTag) {
				noIDTag = strings.TrimSuffix(noSuffixTag, "_id")
			}
			switch noIDTag {
			case "ip_version":
				versionValue := strings.TrimLeft(t.Value, "(")
				versionValue = strings.TrimRight(versionValue, ")")
				versionSlice := strings.Split(versionValue, ",")
				versions := []string{}
				for _, valueStr := range versionSlice {
					ipVersion := "0"
					if valueStr == "4" {
						ipVersion = "1"
					}
					versions = append(versions, ipVersion)
				}
				if len(versions) != 0 {
					versionsStr := strings.Join(versions, ",")
					if strings.ToLower(op) == "in" || strings.ToLower(op) == "not in" {
						versionsStr = "(" + versionsStr + ")"
					}
					whereFilter = fmt.Sprintf(tagItem.WhereTranslator, op, versionsStr)
				}
			case "is_internet":
				internetValue := strings.TrimLeft(t.Value, "(")
				internetValue = strings.TrimRight(internetValue, ")")
				internetSlice := strings.Split(internetValue, ",")
				hasTrue := false
				hasFalse := false
				newOP := ""
				for _, valueStr := range internetSlice {
					if valueStr == "1" || valueStr == "'1'" {
						hasTrue = true
					} else {
						hasFalse = true
					}
				}
				if hasTrue == true && hasFalse == true {
					whereFilter = "1=1"
				} else if hasTrue == true {
					if op == "=" || strings.ToLower(op) == "in" {
						newOP = "="
					} else {
						newOP = "!="
					}
				} else {
					if op == "=" || strings.ToLower(op) == "in" {
						newOP = "!="
					} else {
						newOP = "="
					}
				}
				whereFilter = fmt.Sprintf(tagItem.WhereTranslator, newOP)
			case "_id":
				// When there is a time range in the conditions, there is no need to add redundant time filtering
				if w.time.TimeStart > 0 && w.time.TimeEnd > 0 {
					whereFilter = fmt.Sprintf("%s %s %s", t.Tag, op, t.Value)
				} else {
					idValue := strings.TrimLeft(t.Value, "(")
					idValue = strings.TrimRight(idValue, ")")
					idSlice := strings.Split(idValue, ",")
					whereFilters := []string{}
					for _, valueStr := range idSlice {
						valueStr = strings.Trim(t.Value, "'")
						valueInt, err := strconv.ParseUint(valueStr, 10, 64)
						if err != nil {
							log.Error(err)
							return nil, err
						}
						idFilter := fmt.Sprintf(tagItem.WhereTranslator, op, t.Value, valueInt)
						whereFilters = append(whereFilters, "("+idFilter+")")
					}
					if len(whereFilters) != 0 {
						equalFilter := "(" + strings.Join(whereFilters, " OR ") + ")"
						switch strings.ToLower(op) {
						case "not in":
							whereFilter = "not(" + equalFilter + ")"
						case "!=":
							whereFilter = "not(" + equalFilter + ")"
						default:
							whereFilter = equalFilter
						}
					}
				}
			case "ip", "tunnel_tx_ip", "tunnel_rx_ip", "nat_real_ip":
				equalFilter := ""
				ipValues := strings.TrimLeft(t.Value, "(")
				ipValues = strings.TrimRight(ipValues, ")")
				ipSlice := strings.Split(ipValues, ",")
				ipOp := strings.ToLower(op)
				ipsFilter := ""
				cidrIPs := []string{}
				cidrFilters := []string{}
				ips := []string{}
				for _, ipValue := range ipSlice {
					ipValue = strings.Trim(ipValue, " ")
					if strings.Contains(ipValue, "/") {
						cidrIPs = append(cidrIPs, ipValue)
					} else {
						ips = append(ips, ipValue)
					}
				}
				for _, cidrIP := range cidrIPs {
					cidrIP = strings.Trim(cidrIP, "'")
					cidr, err := netaddr.ParseIPPrefix(cidrIP)
					if err != nil {
						return nil, err
					}
					minIP := "'" + cidr.Masked().Range().From().String() + "'"
					maxIP := "'" + cidr.Masked().Range().To().String() + "'"
					cidrFilter := ""
					if ipOp == ">=" || ipOp == ">" {
						if slices.Contains([]string{"tunnel_tx_ip", "tunnel_rx_ip"}, whereTag) {
							cidrFilter = fmt.Sprintf(tagItem.WhereTranslator, ipOp, maxIP, ipOp, maxIP, ipOp, maxIP, ipOp, maxIP)
						} else if strings.Contains(whereTag, "nat_real_ip") {
							cidrFilter = fmt.Sprintf(tagItem.WhereTranslator, ipOp, maxIP)
						} else {
							cidrFilter = fmt.Sprintf(tagItem.WhereTranslator, ipOp, maxIP, ipOp, maxIP)
						}
					} else if ipOp == "<=" || ipOp == "<" {
						if slices.Contains([]string{"tunnel_tx_ip", "tunnel_rx_ip"}, whereTag) {
							cidrFilter = fmt.Sprintf(tagItem.WhereTranslator, ipOp, minIP, ipOp, minIP, ipOp, minIP, ipOp, minIP)
						} else if strings.Contains(whereTag, "nat_real_ip") {
							cidrFilter = fmt.Sprintf(tagItem.WhereTranslator, ipOp, minIP)
						} else {
							cidrFilter = fmt.Sprintf(tagItem.WhereTranslator, ipOp, minIP, ipOp, minIP)
						}
					} else {
						if slices.Contains([]string{"tunnel_tx_ip", "tunnel_rx_ip"}, whereTag) {
							cidrFilter = fmt.Sprintf("((%s_0 >= %s AND %s_0 <= %s) OR (%s_1 >= %s AND %s_1 <= %s))", whereTag, minIP, whereTag, maxIP, whereTag, minIP, whereTag, maxIP)
						} else if strings.Contains(whereTag, "nat_real_ip") {
							cidrFilter = "(" + fmt.Sprintf(tagItem.WhereTranslator, ">=", minIP) + " AND " + fmt.Sprintf(tagItem.WhereTranslator, "<=", maxIP) + ")"
						} else {
							cidrFilter = "(" + fmt.Sprintf(tagItem.WhereTranslator, ">=", minIP, ">=", minIP) + " AND " + fmt.Sprintf(tagItem.WhereTranslator, "<=", maxIP, "<=", maxIP) + ")"
						}

					}
					cidrFilters = append(cidrFilters, cidrFilter)
				}
				cidrFilterStr := ""
				if len(cidrFilters) != 0 {
					cidrFilterStr = "(" + strings.Join(cidrFilters, " OR ") + ")"
				}
				if len(ips) != 0 {
					if ipOp == ">=" || ipOp == "<=" || ipOp == ">" || ipOp == "<" {
						ipFilters := []string{}
						for _, ip := range ips {
							if slices.Contains([]string{"tunnel_tx_ip", "tunnel_rx_ip"}, whereTag) {
								ipFilters = append(ipFilters, fmt.Sprintf(tagItem.WhereTranslator, ipOp, ip, ipOp, ip, ipOp, ip, ipOp, ip))
							} else if strings.Contains(whereTag, "nat_real_ip") {
								ipFilters = append(ipFilters, fmt.Sprintf(tagItem.WhereTranslator, ipOp, ip))
							} else {
								ipFilters = append(ipFilters, fmt.Sprintf(tagItem.WhereTranslator, ipOp, ip, ipOp, ip))
							}
						}
						ipsFilter = "(" + strings.Join(ipFilters, " OR ") + ")"
					} else {
						ipsStr := strings.Join(ips, ",")
						equalOP := ""
						if ipOp == "in" || ipOp == "not in" {
							ipsStr = "(" + ipsStr + ")"
							equalOP = "in"
						} else {
							equalOP = "="
						}
						if slices.Contains([]string{"tunnel_tx_ip", "tunnel_rx_ip"}, whereTag) {
							ipsFilter = "(" + fmt.Sprintf(tagItem.WhereTranslator, equalOP, ipsStr, equalOP, ipsStr, equalOP, ipsStr, equalOP, ipsStr) + ")"
						} else if strings.Contains(whereTag, "nat_real_ip") {
							ipsFilter = "(" + fmt.Sprintf(tagItem.WhereTranslator, equalOP, ipsStr) + ")"
						} else {
							ipsFilter = "(" + fmt.Sprintf(tagItem.WhereTranslator, equalOP, ipsStr, equalOP, ipsStr) + ")"
						}
					}
				}
				finalFilters := []string{}
				if cidrFilterStr != "" {
					finalFilters = append(finalFilters, cidrFilterStr)
				}
				if ipsFilter != "" {
					finalFilters = append(finalFilters, ipsFilter)
				}
				equalFilter = "(" + strings.Join(finalFilters, " OR ") + ")"
				switch ipOp {
				case "not in":
					whereFilter = "not(" + equalFilter + ")"
				case "!=":
					whereFilter = "not(" + equalFilter + ")"
				default:
					whereFilter = equalFilter
				}
			case "service", "chost", "chost_hostname", "chost_ip", "router", "dhcpgw", "redis", "rds", "lb_listener",
				"natgw", "lb", "host", "host_hostname", "host_ip", "pod_node", "pod_node_hostname", "pod_node_ip", "user",
				"pod_group_type", "region", "az", "pod_ns", "pod_group", "pod", "pod_cluster", "subnet", "gprocess", "pod_service":
				whereFilter = TransChostFilter(tagItem.WhereTranslator, tagItem.WhereRegexpTranslator, op, t.Value)
			case "pod_ingress", "x_request_id", "syscall_thread", "syscall_coroutine", "syscall_cap_seq", "syscall_trace_id", "tcp_seq":
				whereFilter = TransIngressFilter(tagItem.WhereTranslator, tagItem.WhereRegexpTranslator, op, t.Value)
			case "auto_instance", "auto_service":
				if strings.Contains(op, "match") {
					whereFilter = fmt.Sprintf(tagItem.WhereRegexpTranslator, op, t.Value, op, t.Value)
				} else {
					whereFilter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value, op, t.Value)
				}
			case "acl_gids":
				whereFilter = fmt.Sprintf(tagItem.WhereTranslator, t.Value)
			default:
				if strings.Contains(op, "match") {
					whereFilter = fmt.Sprintf(tagItem.WhereRegexpTranslator, op, t.Value)
				} else {
					whereFilter = fmt.Sprintf(tagItem.WhereTranslator, op, t.Value)
				}
			}
		} else {
			if strings.Contains(op, "match") {
				filter = fmt.Sprintf("%s(%s,%s)", op, t.Tag, t.Value)
			} else {
				filter = fmt.Sprintf("%s %s %s", t.Tag, op, t.Value)
			}
			return &view.Expr{Value: filter}, nil
		}
		return &view.Expr{Value: "(" + whereFilter + ")"}, nil
	}

}

func GetPrometheusFilter(promTag, table, op, value string, e *CHEngine) (string, error) {
	filter := ""
	nameNoPreffix := strings.TrimPrefix(promTag, "tag.")
	metricID, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricNameToID[table]
	if !ok {
		errorMessage := fmt.Sprintf("%s not found", table)
		return filter, common.NewError(common.RESOURCE_NOT_FOUND, errorMessage)
	}
	labelNameID, ok := trans_prometheus.ORGPrometheus[e.ORGID].LabelNameToID[nameNoPreffix]
	if !ok {
		if value == "''" {
			filter = fmt.Sprintf("1%s1", op)
		} else {
			filter = "1!=1"
		}
		debugMessage := fmt.Sprintf("%s not found", nameNoPreffix)
		log.Debug(debugMessage)
		return filter, nil
	}
	// Determine whether the tag is app_label or target_label
	isAppLabel := false
	if appLabels, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricAppLabelLayout[table]; ok {
		for _, appLabel := range appLabels {
			if appLabel.AppLabelName == nameNoPreffix {
				isAppLabel = true
				if value == "''" {
					filter = fmt.Sprintf("app_label_value_id_%d %s 0", appLabel.AppLabelColumnIndex, op)
					return filter, nil
				}
				if strings.Contains(op, "match") {
					filter = fmt.Sprintf("toUInt64(app_label_value_id_%d) GLOBAL IN (SELECT label_value_id FROM flow_tag.app_label_live_view WHERE label_name_id=%d and %s(label_value,%s))", appLabel.AppLabelColumnIndex, labelNameID, op, value)
				} else {
					filter = fmt.Sprintf("toUInt64(app_label_value_id_%d) GLOBAL IN (SELECT label_value_id FROM flow_tag.app_label_live_view WHERE label_name_id=%d and label_value %s %s)", appLabel.AppLabelColumnIndex, labelNameID, op, value)
				}
				break
			}
		}
	}
	if !isAppLabel {
		if strings.Contains(op, "match") {
			filter = fmt.Sprintf("toUInt64(target_id) GLOBAL IN (SELECT target_id FROM flow_tag.target_label_live_view WHERE metric_id=%d and label_name_id=%d and %s(label_value,%s))", metricID, labelNameID, op, value)
		} else {
			filter = fmt.Sprintf("toUInt64(target_id) GLOBAL IN (SELECT target_id FROM flow_tag.target_label_live_view WHERE metric_id=%d and label_name_id=%d and label_value %s %s)", metricID, labelNameID, op, value)
		}
	}
	return filter, nil
}

func GetRemoteReadFilter(promTag, table, op, value, originFilter string, e *CHEngine) (string, error) {
	filter := ""
	sql := ""
	isAppLabel := false
	nameNoPreffix := strings.TrimPrefix(promTag, "tag.")
	metricID, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricNameToID[table]
	if !ok {
		errorMessage := fmt.Sprintf("%s not found", table)
		return filter, common.NewError(common.RESOURCE_NOT_FOUND, errorMessage)
	}
	labelNameID, ok := trans_prometheus.ORGPrometheus[e.ORGID].LabelNameToID[nameNoPreffix]
	if !ok {
		if value == "''" {
			filter = fmt.Sprintf("1%s1", op)
		} else {
			filter = "1!=1"
		}
		debugMessage := fmt.Sprintf("%s not found", nameNoPreffix)
		log.Debug(debugMessage)
		return filter, nil
	}
	prometheusSubqueryCache := GetPrometheusSubqueryCache()
	// Determine whether the tag is app_label or target_label
	if appLabels, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricAppLabelLayout[table]; ok {
		for _, appLabel := range appLabels {
			if appLabel.AppLabelName == nameNoPreffix {
				isAppLabel = true
				entryKey := common.EntryKey{ORGID: e.ORGID, Filter: originFilter}
				cacheFilter, ok := prometheusSubqueryCache.Get(entryKey)
				if ok {
					filter = cacheFilter.Filter
					timeout := cacheFilter.Time
					if time.Since(timeout) < time.Duration(config.Cfg.PrometheusIdSubqueryLruTimeout) {
						return filter, nil
					}
				}
				if value == "''" {
					filter = fmt.Sprintf("app_label_value_id_%d %s 0", appLabel.AppLabelColumnIndex, op)
					entryValue := common.EntryValue{Time: time.Now(), Filter: filter}
					entryKey := common.EntryKey{ORGID: e.ORGID, Filter: originFilter}
					prometheusSubqueryCache.Add(entryKey, entryValue)
					return filter, nil
				}

				// lru timeout
				if strings.Contains(op, "match") {
					sql = fmt.Sprintf("SELECT label_value_id FROM flow_tag.app_label_live_view WHERE label_name_id=%d and %s(label_value,%s) GROUP BY label_value_id", labelNameID, op, value)
				} else {
					sql = fmt.Sprintf("SELECT label_value_id FROM flow_tag.app_label_live_view WHERE label_name_id=%d and label_value %s %s GROUP BY label_value_id", labelNameID, op, value)
				}
				chClient := client.Client{
					Host:     config.Cfg.Clickhouse.Host,
					Port:     config.Cfg.Clickhouse.Port,
					UserName: config.Cfg.Clickhouse.User,
					Password: config.Cfg.Clickhouse.Password,
					DB:       "flow_tag",
				}
				appLabelRst, err := chClient.DoQuery(&client.QueryParams{Sql: sql, ORGID: e.ORGID})
				if err != nil {
					return "", err
				}
				valueIDs := []string{}
				for _, v := range appLabelRst.Values {
					valueID := v.([]interface{})[0]
					valueIDUInt64 := valueID.(uint64)
					valueIDString := fmt.Sprintf("%d", valueIDUInt64)
					valueIDs = append(valueIDs, valueIDString)
				}
				valueIDFilter := strings.Join(valueIDs, ",")
				if valueIDFilter == "" {
					filter = "1!=1"
				} else {
					filter = fmt.Sprintf("app_label_value_id_%d IN (%s)", appLabel.AppLabelColumnIndex, valueIDFilter)
				}
				entryValue := common.EntryValue{Time: time.Now(), Filter: filter}
				prometheusSubqueryCache.Add(entryKey, entryValue)
				return filter, nil
			}
		}
	}
	if !isAppLabel {
		transFilter := ""
		if strings.Contains(op, "match") {
			transFilter = fmt.Sprintf("SELECT target_id FROM flow_tag.target_label_live_view WHERE metric_id=%d and label_name_id=%d and %s(label_value,%s) GROUP BY target_id", metricID, labelNameID, op, value)
		} else {
			transFilter = fmt.Sprintf("SELECT target_id FROM flow_tag.target_label_live_view WHERE metric_id=%d and label_name_id=%d and label_value %s %s GROUP BY target_id", metricID, labelNameID, op, value)
		}
		targetLabelFilter := TargetLabelFilter{OriginFilter: originFilter, TransFilter: transFilter}
		e.TargetLabelFilters = append(e.TargetLabelFilters, targetLabelFilter)
		filter = ""
	}
	return filter, nil
}

type TimeTag struct {
	Value string
}

func (t *TimeTag) Trans(expr sqlparser.Expr, w *Where, e *CHEngine) (view.Node, error) {
	compareExpr := expr.(*sqlparser.ComparisonExpr)
	time, err := strconv.ParseInt(t.Value, 10, 64)
	if err == nil {
	} else {
		timeExpr, err := govaluate.NewEvaluableExpression(t.Value)
		if err != nil {
			return nil, err
		}
		timeValue, err := timeExpr.Evaluate(nil)
		if err != nil {
			return nil, err
		}
		time = int64(timeValue.(float64))
	}
	newTime := time
	if compareExpr.Operator == ">=" || compareExpr.Operator == ">" {
		// Derivative operator start time forward
		if e.IsDerivative && w.time.Interval > 0 {
			newTime -= int64(w.time.Interval)
		}
		w.time.AddTimeStart(newTime)
		w.time.TimeStartOperator = compareExpr.Operator
	} else if compareExpr.Operator == "<=" || compareExpr.Operator == "<" {
		w.time.AddTimeEnd(time)
		w.time.TimeEndOperator = compareExpr.Operator
	}
	newValue := sqlparser.String(compareExpr)
	if newTime != time {
		newValue = strings.Replace(newValue, strconv.FormatInt(time, 10), strconv.FormatInt(newTime, 10), 1)
	}
	return &view.Expr{Value: newValue}, nil
}

type WhereFunction struct {
	Function view.Node
	Value    string
}

func (f *WhereFunction) Trans(expr sqlparser.Expr, w *Where, e *CHEngine) (view.Node, error) {
	db := e.DB
	table := e.Table
	language := e.Language
	opName := expr.(*sqlparser.ComparisonExpr).Operator
	op, opType := view.GetOperator(expr.(*sqlparser.ComparisonExpr).Operator)
	right := view.Expr{Value: ""}
	if opType == view.OPERATOER_UNKNOWN {
		return nil, errors.New(fmt.Sprintf("opeartor: %s not support", expr.(*sqlparser.ComparisonExpr).Operator))
	}
	function := strings.Trim(f.Function.ToString(), "`")
	if strings.HasPrefix(function, "Enum(") {
		tagName := strings.TrimPrefix(function, "Enum(")
		tagName = strings.TrimSuffix(tagName, ")")
		tagName = strings.Trim(tagName, "`")
		tagEnum := strings.TrimSuffix(tagName, "_0")
		tagEnum = strings.TrimSuffix(tagEnum, "_1")
		nameColumn := ""
		if language != "" {
			nameColumn = "name_" + language
		} else {
			cfgLang := ""
			if config.Cfg.Language == "en" {
				cfgLang = "en"
			} else {
				cfgLang = "zh"
			}
			nameColumn = "name_" + cfgLang
		}
		if db == "flow_tag" {
			if strings.ToLower(opName) == "like" || strings.ToLower(opName) == "not like" {
				f.Value = strings.ReplaceAll(f.Value, "*", "%")
				if strings.ToLower(opName) == "like" {
					opName = "ilike"
				} else {
					opName = "not ilike"
				}
			} else if strings.ToLower(opName) == "regexp" || strings.ToLower(opName) == "not regexp" {
				// check regexp format
				// 检查正则表达式格式
				_, err := regexp.Compile(strings.Trim(f.Value, "'"))
				if err != nil {
					error := fmt.Errorf("%s : %s", err, f.Value)
					return nil, error
				}
				if strings.ToLower(opName) == "regexp" {
					opName = "match"
				} else {
					opName = "not match"
				}
			}
			filter := ""
			if tagName == "app_service" || tagName == "app_instance" {
				tagItem, ok := tag.GetTag("other_id", db, table, "default")
				if ok {
					if strings.Contains(opName, "match") {
						filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, opName, tagName, f.Value)
					} else {
						filter = fmt.Sprintf(tagItem.WhereTranslator, tagName, opName, f.Value)
					}
				}
				return &view.Expr{Value: "(" + filter + ")"}, nil
			}
			tagItem, ok := tag.GetTag("enum_tag_name", db, table, "default")
			if ok {
				switch strings.ToLower(opName) {
				case "match":
					filter = fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", nameColumn, f.Value)
				case "not match":
					filter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", nameColumn, f.Value) + ")"
				case "not ilike":
					filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "ilike", f.Value) + ")"
				case "not in":
					filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "in", f.Value) + ")"
				case "!=":
					filter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "=", f.Value) + ")"
				default:
					filter = fmt.Sprintf(tagItem.WhereTranslator, nameColumn, opName, f.Value)
				}
			}
			return &view.Expr{Value: "(" + filter + ")"}, nil

		}
		var isIntEnum = true
		tagDescription, ok := tag.TAG_DESCRIPTIONS[tag.TagDescriptionKey{
			DB: db, Table: table, TagName: tagEnum,
		}]
		if !ok {
			return nil, errors.New(fmt.Sprintf("no tag %s in %s.%s", tagName, db, table))
		}
		_, isStringEnumOK := tag.TAG_STRING_ENUMS[tagDescription.EnumFile]
		if isStringEnumOK {
			isIntEnum = false
		}
		if tagName == "tap_side" {
			tagName = "observation_point"
		} else if tagName == "tap_port_type" {
			tagName = "capture_nic_type"
		}
		tagItem, ok := tag.GetTag(tagName, db, table, "enum")
		if !ok {
			right = view.Expr{Value: f.Value}
		} else {
			whereFilter := tagItem.WhereTranslator
			if strings.ToLower(opName) == "like" || strings.ToLower(opName) == "not like" {
				f.Value = strings.ReplaceAll(f.Value, "*", "%")
				if strings.ToLower(opName) == "like" {
					opName = "ilike"
				} else {
					opName = "not ilike"
				}
			} else if strings.ToLower(opName) == "regexp" || strings.ToLower(opName) == "not regexp" {
				// check regexp format
				// 检查正则表达式格式
				_, err := regexp.Compile(strings.Trim(f.Value, "'"))
				if err != nil {
					error := fmt.Errorf("%s : %s", err, f.Value)
					return nil, error
				}
				if strings.ToLower(opName) == "regexp" {
					opName = "match"
				} else {
					opName = "not match"
				}
			}
			if tagName == "app_service" || tagName == "app_instance" {
				if strings.Contains(opName, "match") {
					whereFilter = fmt.Sprintf(tagItem.WhereRegexpTranslator, opName, tagName, f.Value)
				} else {
					whereFilter = fmt.Sprintf(tagItem.WhereTranslator, tagName, opName, f.Value)
				}
				return &view.Expr{Value: "(" + whereFilter + ")"}, nil
			}
			enumFileName := tagDescription.EnumFile
			switch strings.ToLower(expr.(*sqlparser.ComparisonExpr).Operator) {
			case "=":
				//when enum function operator is '=' , add 'or tag = xxx'
				if isIntEnum {
					intValue, err := strconv.Atoi(strings.Trim(f.Value, "'"))
					if err == nil {
						// when value type is int, add toUInt64() function
						if strings.Contains(tagName, "pod_group_type") {
							podGroupTag := strings.Replace(tagName, "pod_group_type", "pod_group_id", -1)
							whereFilter = "(" + fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "=", f.Value, enumFileName) + ") OR " + "dictGet('flow_tag.pod_group_map', 'pod_group_type', (toUInt64(" + podGroupTag + ")))" + " = " + "toUInt64(" + strconv.Itoa(intValue) + ")"
						} else {
							whereFilter = fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "=", f.Value, enumFileName) + " OR " + tagName + " = " + "toUInt64(" + strconv.Itoa(intValue) + ")"
						}
					} else {
						whereFilter = fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "=", f.Value, enumFileName)
					}
				} else {
					whereFilter = fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "=", f.Value, enumFileName) + " OR " + tagName + " = " + f.Value
				}
			case "!=":
				//when enum function operator is '!=', add 'and tag != xxx'
				if isIntEnum {
					intValue, err := strconv.Atoi(strings.Trim(f.Value, "'"))
					if err == nil {
						// when value type is int, add toUInt64() function
						if strings.Contains(tagName, "pod_group_type") {
							podGroupTag := strings.Replace(tagName, "pod_group_type", "pod_group_id", -1)
							whereFilter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "=", f.Value, enumFileName) + ") AND " + "dictGet('flow_tag.pod_group_map', 'pod_group_type', (toUInt64(" + podGroupTag + ")))" + " != " + "toUInt64(" + strconv.Itoa(intValue) + ")"
						} else {
							whereFilter = fmt.Sprintf(tagItem.WhereTranslator, nameColumn, opName, f.Value, enumFileName) + " AND " + tagName + " != " + "toUInt64(" + strconv.Itoa(intValue) + ")"
						}
					} else {
						if strings.Contains(tagName, "pod_group_type") {
							whereFilter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "=", f.Value, enumFileName) + ")"
						} else {
							whereFilter = fmt.Sprintf(tagItem.WhereTranslator, nameColumn, opName, f.Value, enumFileName)
						}
					}
				} else {
					whereFilter = fmt.Sprintf(tagItem.WhereTranslator, nameColumn, opName, f.Value, enumFileName) + " AND " + tagName + " != " + f.Value
				}
			case "not match":
				if strings.Contains(tagName, "pod_group_type") {
					whereFilter = "not(" + fmt.Sprintf(tagItem.WhereRegexpTranslator, "match", nameColumn, f.Value, enumFileName) + ")"
				} else {
					whereFilter = fmt.Sprintf(tagItem.WhereRegexpTranslator, opName, nameColumn, f.Value, enumFileName)
				}
			case "not in":
				if strings.Contains(tagName, "pod_group_type") {
					whereFilter = "not(" + fmt.Sprintf(tagItem.WhereTranslator, nameColumn, "in", f.Value, enumFileName) + ")"
				} else {
					whereFilter = fmt.Sprintf(tagItem.WhereTranslator, nameColumn, opName, f.Value, enumFileName)
				}
			default:
				if strings.Contains(opName, "match") {
					whereFilter = fmt.Sprintf(tagItem.WhereRegexpTranslator, opName, nameColumn, f.Value, enumFileName)
				} else {
					whereFilter = fmt.Sprintf(tagItem.WhereTranslator, nameColumn, opName, f.Value, enumFileName)
				}
			}
			return &view.Expr{Value: "(" + whereFilter + ")"}, nil
		}
	} else if function == "FastFilter(trace_id)" {
		traceConfig := config.TraceConfig
		TypeIsIncrementalId := traceConfig.Type == chCommon.IndexTypeIncremetalId
		FormatIsHex := traceConfig.IncrementalIdLocation.Format == chCommon.FormatHex
		if traceConfig.Disabled {
			filter := fmt.Sprintf("trace_id %s %s", opName, f.Value)
			return &view.Expr{Value: "(" + filter + ")"}, nil
		}
		switch strings.ToLower(opName) {
		case "=", "!=":
			traceID := strings.TrimSpace(f.Value)
			traceID = strings.Trim(traceID, "'")
			traceIDIndex, err := utils.GetTraceIdIndex(traceID, TypeIsIncrementalId, FormatIsHex, traceConfig.IncrementalIdLocation.Start, traceConfig.IncrementalIdLocation.Length)
			// if err != nil or index is zero, not use trace_id_index
			if err != nil || traceIDIndex == 0 {
				errMessage := fmt.Sprintf("%s or trace_id_index =0", err.Error())
				log.Error(errMessage)
				filter := fmt.Sprintf("trace_id %s %s", opName, f.Value)
				return &view.Expr{Value: "(" + filter + ")"}, nil
			}
			filter := fmt.Sprintf("trace_id_index %s %d", opName, traceIDIndex)
			return &view.Expr{Value: "(" + filter + ")"}, nil
		case "in", "not in":
			traceIDIndexSlice := []string{}
			traceIDs := strings.Split(strings.Trim(f.Value, "()"), ",")
			for _, traceID := range traceIDs {
				traceID = strings.TrimSpace(traceID)
				traceID = strings.Trim(traceID, "'")
				traceIDIndex, err := utils.GetTraceIdIndex(traceID, TypeIsIncrementalId, FormatIsHex, traceConfig.IncrementalIdLocation.Start, traceConfig.IncrementalIdLocation.Length)
				// if err != nil or index is zero, not use trace_id_index
				if err != nil || traceIDIndex == 0 {
					errMessage := fmt.Sprintf("%s or trace_id_index =0", err.Error())
					log.Error(errMessage)
					filter := fmt.Sprintf("trace_id %s %s", opName, f.Value)
					return &view.Expr{Value: "(" + filter + ")"}, nil
				}
				traceIDIndexSlice = append(traceIDIndexSlice, strconv.FormatUint(traceIDIndex, 10))
			}
			traceIDIndexs := fmt.Sprintf("(%s)", strings.Join(traceIDIndexSlice, ","))
			filter := fmt.Sprintf("trace_id_index %s %s", opName, traceIDIndexs)
			return &view.Expr{Value: "(" + filter + ")"}, nil
		}
	} else {
		right = view.Expr{Value: f.Value}
	}
	w.withs = append(w.withs, f.Function.GetWiths()...)
	return &view.BinaryExpr{Left: f.Function, Right: &right, Op: op}, nil
}
