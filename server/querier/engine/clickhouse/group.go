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
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/deepflowio/deepflow/server/querier/common"
	chCommon "github.com/deepflowio/deepflow/server/querier/engine/clickhouse/common"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/tag"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/trans_prometheus"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/view"
)

func GetMultiGroup(stmts []Statement, name string, asTagMap map[string]string) []Statement {
	isMulti := false
	for _, suffix := range []string{"", "_0", "_1"} {
		ip4Suffix := "ip4" + suffix
		ip6Suffix := "ip6" + suffix
		deviceTypeSuffix := "l3_device_type" + suffix
		deviceIDSuffix := "l3_device_id" + suffix
		// auto
		for _, resourceName := range []string{"auto_instance", "auto_service"} {
			if name == resourceName+suffix {
				isMulti = true
				resourceTypeSuffix := "auto_service_type" + suffix
				resourceIDSuffix := "auto_service_id" + suffix
				ip4Alias := "auto_service_ip4" + suffix
				ip6Alias := "auto_service_ip6" + suffix
				if resourceName == "auto_instance" {
					resourceTypeSuffix = "auto_instance_type" + suffix
					resourceIDSuffix = "auto_instance_id" + suffix
					ip4Alias = "auto_instance_ip4" + suffix
					ip6Alias = "auto_instance_ip6" + suffix
				}
				ip4WithValue := fmt.Sprintf("if(%s IN (0, 255), if(is_ipv4 = 1, %s, NULL), NULL)", resourceTypeSuffix, ip4Suffix)
				ip6WithValue := fmt.Sprintf("if(%s IN (0, 255), if(is_ipv4 = 0, %s, NULL), NULL)", resourceTypeSuffix, ip6Suffix)
				stmts = append(stmts, &GroupTag{Value: "is_ipv4"})
				stmts = append(stmts, &GroupTag{Value: ip4Alias, Withs: []view.Node{&view.With{Value: ip4WithValue, Alias: ip4Alias}}})
				stmts = append(stmts, &GroupTag{Value: ip6Alias, Withs: []view.Node{&view.With{Value: ip6WithValue, Alias: ip6Alias}}})
				stmts = append(stmts, &GroupTag{Value: resourceTypeSuffix})
				stmts = append(stmts, &GroupTag{Value: resourceIDSuffix})
			}
		}
		// ip
		if name == "ip"+suffix {
			isMulti = true
			stmts = append(stmts, &GroupTag{Value: "is_ipv4"})
			stmts = append(stmts, &GroupTag{Value: ip4Suffix})
			stmts = append(stmts, &GroupTag{Value: ip6Suffix})
		}
		// device
		for resourceStr, deviceTypeValue := range tag.DEVICE_MAP {
			if resourceStr == "pod_service" {
				continue
			} else if name == resourceStr+suffix {
				isMulti = true
				deviceAlias := "device_type_" + name
				deviceWithValue := fmt.Sprintf("if(%s = %d, %s, 0)", deviceTypeSuffix, deviceTypeValue, deviceTypeSuffix)
				stmts = append(stmts, &GroupTag{Value: deviceIDSuffix})
				stmts = append(stmts, &GroupTag{Value: deviceAlias, Withs: []view.Node{&view.With{Value: deviceWithValue, Alias: deviceAlias}}})
			}
		}
		for resource, _ := range tag.HOSTNAME_IP_DEVICE_MAP {
			if slices.Contains([]string{common.CHOST_HOSTNAME, common.CHOST_IP}, resource) && name == resource+suffix {
				isMulti = true
				deviceAlias := "device_type_" + name
				deviceWithValue := fmt.Sprintf("if(%s = %d, %s, 0)", deviceTypeSuffix, tag.VIF_DEVICE_TYPE_VM, deviceTypeSuffix)
				stmts = append(stmts, &GroupTag{Value: deviceIDSuffix})
				stmts = append(stmts, &GroupTag{Value: deviceAlias, Withs: []view.Node{&view.With{Value: deviceWithValue, Alias: deviceAlias}}})
			}
		}
	}
	if !isMulti {
		stmts = append(stmts, &GroupTag{Value: name, AsTagMap: asTagMap})
	}
	return stmts
}

func GetGroup(name string, e *CHEngine) ([]Statement, error) {
	asTagMap := e.AsTagMap
	db := e.DB
	table := e.Table
	var stmts []Statement
	if asTagMap[name] == "time" {
		return stmts, nil
	}
	tagItem, ok := tag.GetTag(name, db, table, "default")
	if ok {
		// Only vtap_acl translate policy_id
		if name == "policy_id" && table != chCommon.TABLE_NAME_VTAP_ACL {
			stmts = append(stmts, &GroupTag{Value: name, AsTagMap: asTagMap})
		} else if db == chCommon.DB_NAME_PROMETHEUS && strings.Contains(name, "tag") {
			tagTranslatorStr := GetPrometheusGroup(name, e)
			if tagTranslatorStr == name {
				stmts = append(stmts, &GroupTag{Value: tagTranslatorStr, AsTagMap: asTagMap})
			} else {
				stmts = append(stmts, &GroupTag{Value: tagTranslatorStr, Alias: name, AsTagMap: asTagMap})
			}
		} else if slices.Contains(tag.AUTO_CUSTOM_TAG_NAMES, strings.Trim(name, "`")) {
			autoTagMap := tagItem.TagTranslatorMap
			for autoTagKey, _ := range autoTagMap {
				stmts = append(stmts, &GroupTag{Value: "`" + autoTagKey + "`"})
			}
		} else if tagItem.GroupTranslator != "" {
			stmts = append(stmts, &GroupTag{Value: tagItem.GroupTranslator, AsTagMap: asTagMap})
		} else {
			if table == "alert_event" {
				stmts = append(stmts, &GroupTag{Value: name, AsTagMap: asTagMap})
			} else {
				stmts = GetMultiGroup(stmts, name, asTagMap)
			}
		}
	} else {
		if db == chCommon.DB_NAME_PROMETHEUS && strings.Contains(name, "tag.") {
			tagTranslatorStr := GetPrometheusGroup(name, e)
			if tagTranslatorStr == name {
				stmts = append(stmts, &GroupTag{Value: tagTranslatorStr, AsTagMap: asTagMap})
			} else {
				stmts = append(stmts, &GroupTag{Value: tagTranslatorStr, Alias: name, AsTagMap: asTagMap})
			}
		} else if slices.Contains(tag.AUTO_CUSTOM_TAG_NAMES, strings.Trim(name, "`")) {
			tagItem, ok := tag.GetTag(strings.Trim(name, "`"), db, table, "default")
			if ok {
				autoTagMap := tagItem.TagTranslatorMap
				autoTagSlice := []string{}
				for autoTagKey, _ := range autoTagMap {
					autoTagSlice = append(autoTagSlice, autoTagKey)
				}
				sort.Strings(autoTagSlice)
				for _, autoTagKey := range autoTagSlice {
					stmts = append(stmts, &GroupTag{Value: "`" + autoTagKey + "`", AsTagMap: asTagMap})
				}
			}
		} else {
			if !strings.Contains(name, "node_type") && !strings.Contains(name, "icon_id") {
				stmts = append(stmts, &GroupTag{Value: name, AsTagMap: asTagMap})
			} else if db == chCommon.DB_NAME_FLOW_TAG {
				stmts = append(stmts, &GroupTag{Value: name, AsTagMap: asTagMap})
			}
		}
	}
	return stmts, nil
}

func GetPrometheusGroup(name string, e *CHEngine) string {
	table := e.Table
	asTagMap := e.AsTagMap
	nameNoPreffix := strings.Trim(name, "`")
	if nameNoPreffix == "tag" {
		tagTranslatorStr, _, _ := GetPrometheusAllTagTranslator(e)
		return tagTranslatorStr
	}
	labelName := strings.TrimPrefix(nameNoPreffix, "tag.")
	labelNameID, ok := trans_prometheus.ORGPrometheus[e.ORGID].LabelNameToID[labelName]
	if !ok {
		errorMessage := fmt.Sprintf("%s not found", labelName)
		log.Error(errorMessage)
		return name
	}
	metricID, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricNameToID[table]
	if !ok {
		errorMessage := fmt.Sprintf("%s not found", table)
		log.Error(errorMessage)
		return name
	}
	tagTranslatorStr := ""
	_, ok = asTagMap[name]
	if ok {
		tagTranslatorStr = name
	} else if strings.HasPrefix(nameNoPreffix, "tag.") {
		nameNoPreffix := strings.Trim(name, "`")
		nameNoPreffix = strings.TrimPrefix(nameNoPreffix, "tag.")
		// Determine whether the tag is app_label or target_label
		isAppLabel := false
		if appLabels, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricAppLabelLayout[table]; ok {
			for _, appLabel := range appLabels {
				if appLabel.AppLabelName == nameNoPreffix {
					isAppLabel = true
					tagTranslatorStr = fmt.Sprintf("dictGet('flow_tag.app_label_map', 'label_value', (toUInt64(%d), toUInt64(app_label_value_id_%d)))", labelNameID, appLabel.AppLabelColumnIndex)
					break
				}
			}
		}
		if !isAppLabel {
			tagTranslatorStr = fmt.Sprintf("dictGet('flow_tag.target_label_map', 'label_value', (toUInt64(%d), toUInt64(%d), toUInt64(target_id)))", metricID, labelNameID)
		}
	} else {
		tagTranslatorStr = name
	}
	return tagTranslatorStr
}

func GetPrometheusNotNullFilter(name string, e *CHEngine) (view.Node, bool) {
	table := e.Table
	asTagMap := e.AsTagMap
	nameNoPreffix := strings.Trim(name, "`")
	preAsTag, ok := asTagMap[name]
	if ok {
		nameNoPreffix = strings.Trim(preAsTag, "`")
	}
	nameNoPreffix = strings.TrimPrefix(nameNoPreffix, "tag.")
	filter := ""
	metricID, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricNameToID[table]
	if !ok {
		return &view.Expr{}, false
	}
	labelNameID, ok := trans_prometheus.ORGPrometheus[e.ORGID].LabelNameToID[nameNoPreffix]
	if !ok {
		return &view.Expr{}, false
	}
	// Determine whether the tag is app_label or target_label
	isAppLabel := false
	if appLabels, ok := trans_prometheus.ORGPrometheus[e.ORGID].MetricAppLabelLayout[table]; ok {
		for _, appLabel := range appLabels {
			if appLabel.AppLabelName == nameNoPreffix {
				isAppLabel = true
				filter = fmt.Sprintf("toUInt64(app_label_value_id_%d) GLOBAL IN (SELECT label_value_id FROM flow_tag.app_label_live_view WHERE label_name_id=%d)", appLabel.AppLabelColumnIndex, labelNameID)
				break
			}
		}
	}
	if !isAppLabel {
		filter = fmt.Sprintf("toUInt64(target_id) GLOBAL IN (SELECT target_id FROM flow_tag.target_label_live_view WHERE metric_id=%d and label_name_id=%d)", metricID, labelNameID)
	}
	return &view.Expr{Value: "(" + filter + ")"}, true
}

func GetNotNullFilter(name string, e *CHEngine) (view.Node, bool) {
	asTagMap := e.AsTagMap
	db := e.DB
	table := e.Table
	preAsTag, preASOK := asTagMap[name]
	if preASOK {
		if db == chCommon.DB_NAME_PROMETHEUS && strings.HasPrefix(preAsTag, "`tag.") {
			return GetPrometheusNotNullFilter(name, e)
		}
	} else {
		if db == chCommon.DB_NAME_PROMETHEUS && strings.HasPrefix(name, "`tag.") {
			return GetPrometheusNotNullFilter(name, e)
		}
	}

	tagItem, ok := tag.GetTag(strings.Trim(name, "`"), db, table, "default")
	if !ok {
		if preASOK {
			tagItem, ok = tag.GetTag(strings.Trim(preAsTag, "`"), db, table, "default")
			if !ok {
				preAsTag := strings.Trim(preAsTag, "`")
				// map item tag
				filterName, _, transKey := common.TransMapItem(preAsTag, table)
				if transKey != "" {
					tagItem, _ = tag.GetTag(transKey, db, table, "default")
					filter := name
					if strings.HasPrefix(preAsTag, "os.app.") || strings.HasPrefix(preAsTag, "k8s.env.") {
						filter = fmt.Sprintf(tagItem.NotNullFilter, filterName)
					} else {
						filter = fmt.Sprintf(tagItem.NotNullFilter, filterName, filterName)
					}
					return &view.Expr{Value: "(" + filter + ")"}, true
				} else if strings.HasPrefix(preAsTag, "tag.") {
					if db == chCommon.DB_NAME_PROMETHEUS {
						return &view.Expr{}, false
					}
					tagItem, ok = tag.GetTag("tag.", db, table, "default")
					filterName := strings.TrimPrefix(strings.Trim(preAsTag, "`"), "tag.")
					filter := fmt.Sprintf(tagItem.NotNullFilter, filterName)
					return &view.Expr{Value: "(" + filter + ")"}, true
				} else if strings.HasPrefix(preAsTag, "attribute.") {
					tagItem, ok = tag.GetTag("attribute.", db, table, "default")
					filterName := strings.TrimPrefix(strings.Trim(preAsTag, "`"), "attribute.")
					filter := fmt.Sprintf(tagItem.NotNullFilter, filterName)
					return &view.Expr{Value: "(" + filter + ")"}, true
				} else if common.IsValueInSliceString(preAsTag, []string{"request_id", "response_code", "span_kind", "request_length", "response_length", "sql_affected_rows"}) {
					filter := fmt.Sprintf("%s is not null", preAsTag)
					return &view.Expr{Value: "(" + filter + ")"}, true
				}
				return &view.Expr{}, false
			}
			filter := tagItem.NotNullFilter
			if filter == "" {
				return &view.Expr{}, false
			}
			return &view.Expr{Value: "(" + filter + ")"}, true
		} else {
			noBackQuoteName := strings.Trim(name, "`")
			// map item tag
			filterName, _, transKey := common.TransMapItem(noBackQuoteName, table)
			if transKey != "" {
				tagItem, _ = tag.GetTag(transKey, db, table, "default")
				filter := name
				if strings.HasPrefix(noBackQuoteName, "os.app.") || strings.HasPrefix(noBackQuoteName, "k8s.env.") {
					filter = fmt.Sprintf(tagItem.NotNullFilter, filterName)
				} else {
					filter = fmt.Sprintf(tagItem.NotNullFilter, filterName, filterName)
				}
				return &view.Expr{Value: "(" + filter + ")"}, true
			} else if strings.HasPrefix(noBackQuoteName, "tag.") {
				if db == chCommon.DB_NAME_PROMETHEUS {
					return &view.Expr{}, false
				}
				tagItem, ok = tag.GetTag("tag.", db, table, "default")
				filterName := strings.TrimPrefix(strings.Trim(name, "`"), "tag.")
				filter := fmt.Sprintf(tagItem.NotNullFilter, filterName)
				return &view.Expr{Value: "(" + filter + ")"}, true
			} else if strings.HasPrefix(noBackQuoteName, "attribute.") {
				tagItem, ok = tag.GetTag("attribute.", db, table, "default")
				filterName := strings.TrimPrefix(strings.Trim(name, "`"), "attribute.")
				filter := fmt.Sprintf(tagItem.NotNullFilter, filterName)
				return &view.Expr{Value: "(" + filter + ")"}, true
			} else if common.IsValueInSliceString(noBackQuoteName, []string{"request_id", "response_code", "span_kind", "request_length", "response_length", "sql_affected_rows"}) {
				filter := fmt.Sprintf("%s is not null", name)
				return &view.Expr{Value: "(" + filter + ")"}, true
			}
			return &view.Expr{}, false
		}
	}
	if tagItem.NotNullFilter == "" {
		return &view.Expr{}, false
	}
	filter := tagItem.NotNullFilter
	return &view.Expr{Value: "(" + filter + ")"}, true
}

func FormatInnerTime(m *view.Model) {
	if m.DB != chCommon.DB_NAME_FLOW_LOG && m.Time.Interval == 0 && m.MetricsLevelFlag == view.MODEL_METRICS_LEVEL_FLAG_LAYERED && m.HasAggFunc == true {
		withValue := fmt.Sprintf(
			"toStartOfInterval(time, toIntervalSecond(%d))",
			m.Time.DatasourceInterval,
		)
		withAlias := "_time"
		withs := []view.Node{&view.With{Value: withValue, Alias: withAlias}}
		m.AddTag(&view.Tag{Value: withAlias, Withs: withs, Flag: view.NODE_FLAG_METRICS_INNER})
		m.AddGroup(&view.Group{Value: withAlias, Flag: view.GROUP_FLAG_METRICS_INNTER})
	}
}

type GroupTag struct {
	Value    string
	Alias    string
	Withs    []view.Node
	AsTagMap map[string]string
}

func (g *GroupTag) Format(m *view.Model) {
	if len(g.Withs) == 0 {
		m.AddGroup(&view.Group{Value: g.Value, Alias: g.Alias})
	} else {
		m.AddGroup(&view.Group{Value: g.Value, Withs: g.Withs})
	}
}
