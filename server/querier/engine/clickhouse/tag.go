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
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/deepflowio/deepflow/server/querier/common"
	chCommon "github.com/deepflowio/deepflow/server/querier/engine/clickhouse/common"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/metrics"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/packet_batch"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/tag"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/trans_prometheus"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/view"
)

func GetMultiTag(stmts []Statement, name string) []Statement {
	for _, suffix := range []string{"", "_0", "_1"} {
		ip4Suffix := "ip4" + suffix
		ip6Suffix := "ip6" + suffix
		deviceTypeSuffix := "l3_device_type" + suffix
		// auto
		for _, resourceName := range []string{"auto_instance", "auto_service"} {
			if name == resourceName+suffix {
				resourceTypeSuffix := "auto_service_type" + suffix
				ip4Alias := "auto_service_ip4" + suffix
				ip6Alias := "auto_service_ip6" + suffix
				if resourceName == "auto_instance" {
					resourceTypeSuffix = "auto_instance_type" + suffix
					ip4Alias = "auto_instance_ip4" + suffix
					ip6Alias = "auto_instance_ip6" + suffix
				}
				ip4WithValue := fmt.Sprintf("if(%s IN (0, 255), if(is_ipv4 = 1, %s, NULL), NULL)", resourceTypeSuffix, ip4Suffix)
				ip6WithValue := fmt.Sprintf("if(%s IN (0, 255), if(is_ipv4 = 0, %s, NULL), NULL)", resourceTypeSuffix, ip6Suffix)
				stmts = append(stmts, &SelectTag{Value: ip4Alias, Withs: []view.Node{&view.With{Value: ip4WithValue, Alias: ip4Alias}}})
				stmts = append(stmts, &SelectTag{Value: ip6Alias, Withs: []view.Node{&view.With{Value: ip6WithValue, Alias: ip6Alias}}})
				stmts = append(stmts, &SelectTag{Value: resourceTypeSuffix})
			}
		}
		// device
		for resourceStr, deviceTypeValue := range tag.DEVICE_MAP {
			if resourceStr == "pod_service" {
				continue
			} else if name == resourceStr+suffix {
				deviceAlias := "device_type_" + name
				deviceWithValue := fmt.Sprintf("if(%s = %d, %s, 0)", deviceTypeSuffix, deviceTypeValue, deviceTypeSuffix)
				stmts = append(stmts, &SelectTag{Value: deviceAlias, Withs: []view.Node{&view.With{Value: deviceWithValue, Alias: deviceAlias}}})
			}
		}
		for resource, _ := range tag.HOSTNAME_IP_DEVICE_MAP {
			if slices.Contains([]string{common.CHOST_HOSTNAME, common.CHOST_IP}, resource) && name == resource+suffix {
				deviceAlias := "device_type_" + name
				deviceWithValue := fmt.Sprintf("if(%s = %d, %s, 0)", deviceTypeSuffix, tag.VIF_DEVICE_TYPE_VM, deviceTypeSuffix)
				stmts = append(stmts, &SelectTag{Value: deviceAlias, Withs: []view.Node{&view.With{Value: deviceWithValue, Alias: deviceAlias}}})
			}
		}
	}
	return stmts
}

func GetTagTranslator(name, alias string, e *CHEngine) ([]Statement, string, error) {
	db := e.DB
	table := e.Table
	var stmts []Statement
	selectTag := name
	if alias != "" {
		selectTag = alias
	}
	labelType := ""
	nameNoBackQuote := strings.Trim(name, "`")
	tagItem, ok := tag.GetTag(nameNoBackQuote, db, table, "default")
	if table == "alert_event" {
		if slices.Contains(tag.AUTO_CUSTOM_TAG_NAMES, nameNoBackQuote) {
			autoTagMap := tagItem.TagTranslatorMap
			autoTagSlice := []string{}
			for autoTagKey, _ := range autoTagMap {
				autoTagSlice = append(autoTagSlice, autoTagKey)
			}
			sort.Strings(autoTagSlice)
			for _, autoTagKey := range autoTagSlice {
				stmts = append(stmts, &SelectTag{Value: autoTagMap[autoTagKey], Alias: "`" + autoTagKey + "`"})
			}
		} else if ok {
			tagTranslator := tagItem.TagTranslator
			stmts = append(stmts, &SelectTag{Value: tagTranslator, Alias: selectTag})
		} else {
			stmts = append(stmts, &SelectTag{Value: name, Alias: alias})
		}
		return stmts, labelType, nil
	}
	if !ok {
		name := strings.Trim(name, "`")
		// map item tag
		nameNoPreffix, _, transKey := common.TransMapItem(name, table)
		if transKey != "" {
			tagItem, _ = tag.GetTag(transKey, db, table, "default")
			TagTranslatorStr := name
			if strings.HasPrefix(name, "os.app.") || strings.HasPrefix(name, "k8s.env.") {
				TagTranslatorStr = fmt.Sprintf(tagItem.TagTranslator, nameNoPreffix)
			} else {
				TagTranslatorStr = fmt.Sprintf(tagItem.TagTranslator, nameNoPreffix, nameNoPreffix, nameNoPreffix)
			}
			stmts = append(stmts, &SelectTag{Value: TagTranslatorStr, Alias: selectTag})
		} else if slices.Contains(tag.AUTO_CUSTOM_TAG_NAMES, name) {
			autoTagMap := tagItem.TagTranslatorMap
			autoTagSlice := []string{}
			for autoTagKey, _ := range autoTagMap {
				autoTagSlice = append(autoTagSlice, autoTagKey)
			}
			sort.Strings(autoTagSlice)
			for _, autoTagKey := range autoTagSlice {
				if autoTagMap[autoTagKey] != "" {
					stmts = append(stmts, &SelectTag{Value: autoTagMap[autoTagKey], Alias: "`" + autoTagKey + "`"})
				} else {
					stmts = append(stmts, &SelectTag{Value: "`" + autoTagKey + "`"})
				}
			}
			// callback
			stmts = append(stmts, &SelectTag{Value: name})
		} else if strings.HasPrefix(name, "tag.") || strings.HasPrefix(name, "attribute.") {
			if strings.HasPrefix(name, "tag.") {
				if db == chCommon.DB_NAME_PROMETHEUS {
					TagTranslatorStr, labelType, err := GetPrometheusSingleTagTranslator(name, e)
					if err != nil {
						return nil, "", err
					}
					stmts = append(stmts, &SelectTag{Value: TagTranslatorStr, Alias: selectTag})
					return stmts, labelType, nil
				}
				tagItem, ok = tag.GetTag("tag.", db, table, "default")
			} else {
				tagItem, ok = tag.GetTag("attribute.", db, table, "default")
			}
			nameNoPreffix := strings.TrimPrefix(name, "tag.")
			nameNoPreffix = strings.TrimPrefix(nameNoPreffix, "attribute.")
			TagTranslatorStr := fmt.Sprintf(tagItem.TagTranslator, nameNoPreffix)
			stmts = append(stmts, &SelectTag{Value: TagTranslatorStr, Alias: selectTag})
		}
	} else {
		// Only vtap_acl translate policy_id
		if strings.Trim(name, "`") == "policy_id" && table != chCommon.TABLE_NAME_VTAP_ACL {
			stmts = append(stmts, &SelectTag{Value: selectTag})
		} else if name == "metrics" {
			tagTranslator := ""
			if db == "flow_log" || db == chCommon.DB_NAME_APPLICATION_LOG {
				tagTranslator = fmt.Sprintf(tagItem.TagTranslator, "metrics_names", "metrics_values")
			} else {
				tagTranslator = fmt.Sprintf(tagItem.TagTranslator, "metrics_float_names", "metrics_float_values")
			}
			stmts = append(stmts, &SelectTag{Value: tagTranslator, Alias: selectTag})
		} else if name == "tag" && db == chCommon.DB_NAME_PROMETHEUS {
			tagTranslator, _, err := GetPrometheusAllTagTranslator(e)
			if err != nil {
				return nil, "", err
			}
			stmts = append(stmts, &SelectTag{Value: tagTranslator, Alias: selectTag})
		} else if slices.Contains(tag.AUTO_CUSTOM_TAG_NAMES, strings.Trim(selectTag, "`")) {
			autoTagMap := tagItem.TagTranslatorMap
			autoTagSlice := []string{}
			for autoTagKey, _ := range autoTagMap {
				autoTagSlice = append(autoTagSlice, autoTagKey)
			}
			sort.Strings(autoTagSlice)
			for _, autoTagKey := range autoTagSlice {
				if autoTagMap[autoTagKey] != "" {
					stmts = append(stmts, &SelectTag{Value: autoTagMap[autoTagKey], Alias: "`" + autoTagKey + "`"})
				} else {
					stmts = append(stmts, &SelectTag{Value: "`" + autoTagKey + "`"})
				}
			}
			// callback
			stmts = append(stmts, &SelectTag{Value: name})
		} else if tagItem.TagTranslator != "" {
			if name != "packet_batch" || table != "l4_packet" {
				stmts = append(stmts, &SelectTag{Value: tagItem.TagTranslator, Alias: selectTag})
				stmts = GetMultiTag(stmts, name)
			}
		} else if alias != "" {
			stmts = append(stmts, &SelectTag{Value: name, Alias: selectTag})
		} else {
			stmts = append(stmts, &SelectTag{Value: selectTag})
		}
	}
	return stmts, labelType, nil
}

func GetPrometheusSingleTagTranslator(tag string, e *CHEngine) (string, string, error) {
	table := e.Table
	labelType := ""
	TagTranslatorStr := ""
	nameNoPreffix := strings.TrimPrefix(tag, "tag.")
	metricID, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricNameToID[table]
	if !ok {
		errorMessage := fmt.Sprintf("%s not found", table)
		return "", "", common.NewError(common.RESOURCE_NOT_FOUND, errorMessage)
	}
	labelNameID, ok := trans_prometheus.ORGPrometheus[e.ORGID].LabelNameToID[nameNoPreffix]
	if !ok {
		errorMessage := fmt.Sprintf("%s not found", nameNoPreffix)
		return "", "", errors.New(errorMessage)
	}
	// Determine whether the tag is app_label or target_label
	isAppLabel := false
	if appLabels, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricAppLabelLayout[table]; ok {
		for _, appLabel := range appLabels {
			if appLabel.AppLabelName == nameNoPreffix {
				isAppLabel = true
				labelType = "app"
				TagTranslatorStr = fmt.Sprintf("dictGet('flow_tag.app_label_map', 'label_value', (toUInt64(%d), toUInt64(app_label_value_id_%d)))", labelNameID, appLabel.AppLabelColumnIndex)
				break
			}
		}
	}
	if !isAppLabel {
		labelType = "target"
		TagTranslatorStr = fmt.Sprintf("dictGet('flow_tag.target_label_map', 'label_value', (toUInt64(%d), toUInt64(%d), toUInt64(target_id)))", metricID, labelNameID)
	}
	return TagTranslatorStr, labelType, nil
}

func GetPrometheusAllTagTranslator(e *CHEngine) (string, string, error) {
	table := e.Table
	tagTranslatorStr := ""
	appLabelTranslatorStr := ""
	labelFastTranslatorSlice := []string{}
	if appLabels, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricAppLabelLayout[table]; ok {
		// appLabel
		appLabelTranslatorSlice := []string{}
		for _, appLabel := range appLabels {
			if labelNameID, ok := trans_prometheus.ORGPrometheus[e.ORGID].LabelNameToID[appLabel.AppLabelName]; ok {
				appLabelTranslator := fmt.Sprintf("'%s',dictGet('flow_tag.app_label_map', 'label_value', (toUInt64(%d), toUInt64(app_label_value_id_%d)))", appLabel.AppLabelName, labelNameID, appLabel.AppLabelColumnIndex)
				appLabelTranslatorSlice = append(appLabelTranslatorSlice, appLabelTranslator)
				labelFastTranslatorSlice = append(labelFastTranslatorSlice, fmt.Sprintf("app_label_value_id_%d", appLabel.AppLabelColumnIndex))
			}
		}
		appLabelTranslatorStr = strings.Join(appLabelTranslatorSlice, ",")
	}

	// targetLabel
	targetLabelTranslatorStr := "CAST((splitByString(', ', dictGet('flow_tag.prometheus_target_label_layout_map', 'target_label_names', toUInt64(target_id))), splitByString(', ', dictGet('flow_tag.prometheus_target_label_layout_map', 'target_label_values', toUInt64(target_id)))), 'Map(String, String)')"
	if appLabelTranslatorStr != "" {
		tagTranslatorStr = "toJSONString(mapUpdate(map(" + appLabelTranslatorStr + ")," + targetLabelTranslatorStr + "))"
	} else {
		tagTranslatorStr = "toJSONString(" + targetLabelTranslatorStr + ")"
	}
	labelFastTranslatorSlice = append(labelFastTranslatorSlice, "target_id")
	labelFastTranslatorStr := fmt.Sprintf("array(%s)", strings.Join(labelFastTranslatorSlice, ","))
	return tagTranslatorStr, labelFastTranslatorStr, nil
}

func GetMetricsTag(name string, alias string, e *CHEngine) (Statement, error) {
	metricStruct, ok := metrics.GetMetrics(strings.Trim(name, "`"), e.DB, e.Table, e.ORGID, e.DynamicTag)
	if !ok {
		return nil, nil
	}
	if alias == "" && metricStruct.DBField != name {
		alias = name
	}
	return &SelectTag{Value: metricStruct.DBField, Alias: alias}, nil
}

func GetDefaultTag(name string, alias string) Statement {
	return &SelectTag{Value: name, Alias: alias}
}

type SelectTag struct {
	Value string
	Alias string
	Flag  int
	Withs []view.Node
}

func (t *SelectTag) Format(m *view.Model) {
	if slices.Contains(tag.AUTO_CUSTOM_TAG_NAMES, strings.Trim(t.Value, "`")) {
		m.AddCallback(strings.Trim(t.Value, "`"), ColumnNameSwap([]interface{}{strings.Trim(t.Value, "`")}))
	} else {
		m.AddTag(&view.Tag{Value: t.Value, Alias: t.Alias, Flag: t.Flag, Withs: t.Withs})
		if common.IsValueInSliceString(t.Value, []string{"tap_port", "capture_nic", "mac_0", "mac_1", "tunnel_tx_mac_0", "tunnel_tx_mac_1", "tunnel_rx_mac_0", "tunnel_rx_mac_1"}) {
			alias := t.Value
			if t.Alias != "" {
				alias = t.Alias
			}
			m.AddCallback(t.Value, MacTranslate([]interface{}{t.Value, alias}))
		}
		if t.Value == "packet_batch" {
			m.AddCallback(t.Value, packet_batch.PacketBatchFormat([]interface{}{}))
		}
	}

}
