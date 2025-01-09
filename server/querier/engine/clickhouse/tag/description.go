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

package tag

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"

	logging "github.com/op/go-logging"

	"github.com/deepflowio/deepflow/server/querier/common"
	"github.com/deepflowio/deepflow/server/querier/config"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/client"
	ckcommon "github.com/deepflowio/deepflow/server/querier/engine/clickhouse/common"
)

var log = logging.MustGetLogger("clickhouse.tag")

// [db][table][tag]*TagDescription
type TagDescriptionKey struct {
	DB      string
	Table   string
	TagName string
}

var TAG_DESCRIPTION_KEYS = []TagDescriptionKey{}
var TAG_DESCRIPTIONS = map[TagDescriptionKey]*TagDescription{}

// key=tagEnumFile
var TAG_ENUMS = map[string][]*TagEnum{}
var TAG_INT_ENUMS = map[string][]*TagEnum{}
var TAG_STRING_ENUMS = map[string][]*TagEnum{}

var AUTO_CUSTOM_TAG_NAMES = []string{}
var AUTO_CUSTOM_TAG_MAP = map[string][]string{}
var AUTO_CUSTOM_TAG_CHECK_MAP = map[string][]string{}

var tagNativeTagDB = []string{ckcommon.DB_NAME_EXT_METRICS, ckcommon.DB_NAME_DEEPFLOW_ADMIN, ckcommon.DB_NAME_DEEPFLOW_TENANT, ckcommon.DB_NAME_PROFILE, ckcommon.DB_NAME_PROMETHEUS}
var noCustomTagTable = []string{"traffic_policy", "l4_packet", "l7_packet", "alert_event"}
var noCustomTagDB = []string{ckcommon.DB_NAME_DEEPFLOW_ADMIN, ckcommon.DB_NAME_DEEPFLOW_TENANT}

var tagTypeToOperators = map[string][]string{
	"resource":        []string{"=", "!=", "IN", "NOT IN", "LIKE", "NOT LIKE", "REGEXP", "NOT REGEXP"},
	"int":             []string{"=", "!=", "IN", "NOT IN", ">=", "<=", ">", "<"},
	"int_enum":        []string{"=", "!=", "IN", "NOT IN", ">=", "<=", ">", "<"},
	"string":          []string{"=", "!=", "IN", "NOT IN", "LIKE", "NOT LIKE", "REGEXP", "NOT REGEXP"},
	"tokenize_string": []string{"=", "!=", "IN", "NOT IN", "LIKE", "NOT LIKE", "REGEXP", "NOT REGEXP"},
	"string_enum":     []string{"=", "!=", "IN", "NOT IN", "LIKE", "NOT LIKE", "REGEXP", "NOT REGEXP"},
	"ip":              []string{"=", "!=", "IN", "NOT IN", ">=", "<=", ">", "<"},
	"time":            []string{"=", "!=", ">=", "<="},
	"mac":             []string{"=", "!=", "IN", "NOT IN"},
	"id":              []string{"=", "!=", "IN", "NOT IN"},
	"default":         []string{"=", "!=", "IN", "NOT IN"},
}
var TAG_RESOURCE_TYPE_DEVICE_MAP = map[string]int{
	"chost":       VIF_DEVICE_TYPE_VM,
	"router":      VIF_DEVICE_TYPE_VROUTER,
	"dhcpgw":      VIF_DEVICE_TYPE_DHCP_PORT,
	"pod_service": VIF_DEVICE_TYPE_POD_SERVICE,
	"redis":       VIF_DEVICE_TYPE_REDIS_INSTANCE,
	"rds":         VIF_DEVICE_TYPE_RDS_INSTANCE,
	"lb":          VIF_DEVICE_TYPE_LB,
	"natgw":       VIF_DEVICE_TYPE_NAT_GATEWAY,
	"host":        VIF_DEVICE_TYPE_HOST,
	"service":     VIF_DEVICE_TYPE_SERVICE,
}

type TagDescription struct {
	Name                  string
	ClientName            string
	ServerName            string
	DisplayName           string
	DisplayNameZH         string
	DisplayNameEN         string
	Type                  string
	EnumFile              string
	Category              string
	Description           string
	DescriptionZH         string
	DescriptionEN         string
	Operators             []string
	Permissions           []bool
	RelatedTag            string
	Deprecated            bool
	NotSupportedOperators []string
	Table                 string
}

func NewTagDescription(
	name, clientName, serverName, displayName, displayNameZH, displayNameEN, tagType, enumFile, category string,
	permissions []bool, description, descriptionZH, descriptionEN, relatedTag string, deprecated bool, notSupportedOperators []string, table string,
) *TagDescription {
	operators, ok := tagTypeToOperators[tagType]
	if !ok {
		operators, _ = tagTypeToOperators["default"]
	}
	return &TagDescription{
		Name:                  name,
		ClientName:            clientName,
		ServerName:            serverName,
		DisplayName:           displayName,
		DisplayNameZH:         displayNameZH,
		DisplayNameEN:         displayNameEN,
		Type:                  tagType,
		EnumFile:              enumFile,
		Category:              category,
		Operators:             operators,
		Permissions:           permissions,
		Description:           description,
		DescriptionZH:         descriptionZH,
		DescriptionEN:         descriptionEN,
		RelatedTag:            relatedTag,
		Deprecated:            deprecated,
		NotSupportedOperators: notSupportedOperators,
		Table:                 table,
	}
}

type TagEnum struct {
	Value         interface{}
	DisplayNameZH interface{}
	DisplayNameEN interface{}
	DescriptionZH interface{}
	DescriptionEN interface{}
	TagType       interface{}
}

func NewTagEnum(value, displayNameZH, displayNameEN, descriptionZH, descriptionEN, tagType interface{}) *TagEnum {
	return &TagEnum{
		Value:         value,
		DisplayNameZH: displayNameZH,
		DisplayNameEN: displayNameEN,
		DescriptionZH: descriptionZH,
		DescriptionEN: descriptionEN,
		TagType:       tagType,
	}
}

func LoadTagDescriptions(tagData map[string]interface{}) error {
	// 生成tag description
	enumFileToTagType := make(map[string]string)
	for db, dbTagData := range tagData {
		if db == "enum" {
			continue
		}
		for table, tableTagData := range dbTagData.(map[string]interface{}) {
			// common.Result
			for i, tag := range tableTagData.([][]interface{}) {
				if strings.Contains(table, ".") {
					continue
				}
				if len(tag) < 7 {
					return errors.New(
						fmt.Sprintf("get tag failed! db:%s table:%s, tag:%v", db, table, tag),
					)
				}
				// 0 - Name
				// 1 - ClientName
				// 2 - ServerName
				// 3 - DisplayName
				// 4 - Type
				// 5 - EnumFile
				// 6 - Category
				// 7 - Permissions
				// 8 - Description
				// 9 - RelatedTag
				// 10 - Deprecated
				// 11 - NotSupportedOperators
				// 12 - Table

				permissions, err := ckcommon.ParsePermission(tag[6])
				if err != nil {
					return errors.New(
						fmt.Sprintf(
							"parse tag permission failed! db:%s table:%s, tag:%v, err:%s",
							db, table, tag, err.Error(),
						),
					)
				}
				deprecated := false
				deprecatedNum, err := strconv.Atoi(tag[7].(string))
				if err != nil {
					return errors.New(
						fmt.Sprintf(
							"parse tag deprecated failed! db:%s table:%s, tag:%v, err:%s",
							db, table, tag, err.Error(),
						),
					)
				}
				if deprecatedNum == 1 {
					deprecated = true
				}
				notSupportedOperators := []string{}
				if len(tag) >= 9 {
					notSupportedOperators = ckcommon.ParseNotSupportedOperator(tag[8])
				}
				key := TagDescriptionKey{DB: db, Table: table, TagName: tag[0].(string)}
				tagLanguage := dbTagData.(map[string]interface{})[table+"."+config.Cfg.Language].([][]interface{})[i]
				tagLanguageZH := dbTagData.(map[string]interface{})[table+".ch"].([][]interface{})[i]
				tagLanguageEN := dbTagData.(map[string]interface{})[table+".en"].([][]interface{})[i]
				TAG_DESCRIPTION_KEYS = append(TAG_DESCRIPTION_KEYS, key)

				enumFile := tag[4].(string)
				displayName := tagLanguage[1].(string)
				displayNameZH := tagLanguageZH[1].(string)
				displayNameEN := tagLanguageEN[1].(string)
				des := tagLanguage[2].(string)
				desZH := tagLanguageZH[2].(string)
				desEN := tagLanguageEN[2].(string)
				description := NewTagDescription(
					tag[0].(string), tag[1].(string), tag[2].(string), displayName, displayNameZH, displayNameEN,
					tag[3].(string), enumFile, tag[5].(string), permissions, des, desZH, desEN, "", deprecated, notSupportedOperators, table,
				)
				TAG_DESCRIPTIONS[key] = description
				enumFileToTagType[enumFile] = tag[3].(string)
			}
		}
	}

	// 生成tag enum值
	tagEnumData, ok := tagData["enum"]
	if ok {
		tagMap := map[string][][6]interface{}{}
		for tagEnumFile, enumData := range tagEnumData.(map[string]interface{}) {
			tagName := strings.TrimSuffix(tagEnumFile, ".ch")
			tagName = strings.TrimSuffix(tagName, ".en")
			values, ok := tagMap[tagName]
			if !ok {
				valuesLen := len(enumData.([][]interface{}))
				values = make([][6]interface{}, valuesLen)
			}
			// 根据tagEnumFile获取tagTypeToOperators
			tagType, _ := enumFileToTagType[tagName]
			for i, enumValue := range enumData.([][]interface{}) {
				// 如果是int/int_enum，则将value转为interface
				if tagType == "int" || tagType == "int_enum" || tagType == "bit_enum" {
					value, _ := strconv.Atoi(enumValue[0].(string))
					values[i][0] = value
					if strings.HasSuffix(tagEnumFile, ".en") {
						values[i][2] = enumValue[1]
						values[i][4] = enumValue[2]
					} else if strings.HasSuffix(tagEnumFile, ".ch") {
						values[i][1] = enumValue[1]
						values[i][3] = enumValue[2]
					} else {
						values[i][1] = enumValue[1]
						values[i][2] = enumValue[1]
						values[i][3] = enumValue[2]
						values[i][4] = enumValue[2]
					}
				} else if tagType == "string_enum" {
					values[i][0] = enumValue[0]
					if strings.HasSuffix(tagEnumFile, ".en") {
						values[i][2] = enumValue[1]
						values[i][4] = enumValue[2]
					} else if strings.HasSuffix(tagEnumFile, ".ch") {
						values[i][1] = enumValue[1]
						values[i][3] = enumValue[2]
					} else {
						values[i][1] = enumValue[1]
						values[i][2] = enumValue[1]
						values[i][3] = enumValue[2]
						values[i][4] = enumValue[2]
					}
				}
				values[i][5] = tagType
			}
			tagMap[tagName] = values
		}
		for tagName, values := range tagMap {
			tagEnums := []*TagEnum{}
			tagIntEnums := []*TagEnum{}
			tagStringEnums := []*TagEnum{}
			for _, datas := range values {
				tagType, _ := datas[5].(string)
				if tagType == "string_enum" {
					tagStringEnums = append(tagStringEnums, NewTagEnum(datas[0], datas[1], datas[2], datas[3], datas[4], datas[5]))
				} else {
					tagIntEnums = append(tagIntEnums, NewTagEnum(datas[0], datas[1], datas[2], datas[3], datas[4], datas[5]))
				}
				tagEnums = append(tagEnums, NewTagEnum(datas[0], datas[1], datas[2], datas[3], datas[4], datas[5]))
			}
			if len(tagIntEnums) > 0 {
				TAG_INT_ENUMS[tagName] = tagIntEnums
			}
			if len(tagStringEnums) > 0 {
				TAG_STRING_ENUMS[tagName] = tagStringEnums
			}
			TAG_ENUMS[tagName] = tagEnums
		}
	} else {
		return errors.New("get tag enum failed! ")
	}

	// 获取用户自定义tag的设置, 并创建翻译map
	// Obtain user defined auto custom tag settings and supplement translation maps
	if len(config.Cfg.AutoCustomTags) != 0 {
		for _, AutoCustomTag := range config.Cfg.AutoCustomTags {
			tagName := AutoCustomTag.TagName
			if tagName != "" {
				for _, suffix := range []string{"", "_0", "_1"} {
					tagFields := AutoCustomTag.TagFields
					ipFlag := true
					if !slices.Contains(tagFields, "ip") {
						for _, tagValue := range tagFields {
							if slices.Contains(TAG_RESOURCE_TYPE_AUTO, tagValue) {
								ipFlag = false
								break
							}
						}
						if ipFlag {
							tagFields = append(tagFields, "ip")
						}
					}
					if len(tagFields) != 0 {
						var selectPrefixTranslator string
						var nodeTypeTranslator string
						var iconIDTranslator string
						tagNameSuffix := tagName + suffix
						AUTO_CUSTOM_TAG_NAMES = append(AUTO_CUSTOM_TAG_NAMES, tagNameSuffix)
						AUTO_CUSTOM_TAG_MAP[tagNameSuffix] = []string{}
						TagResoureMap[tagNameSuffix] = map[string]*Tag{
							"default": NewTag(
								"",
								"",
								"",
								"",
								"",
							),
							"node_type": NewTag(
								"",
								"",
								"",
								"",
								"",
							),
							"icon_id": NewTag(
								"",
								"",
								"",
								"",
								"",
							),
						}
						AlarmEventResourceMap[tagNameSuffix] = map[string]*Tag{
							"default": NewTag(
								"",
								"",
								"",
								"",
								"",
							),
						}
						TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap = map[string]string{}
						AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap = map[string]string{}
						for _, tagValue := range tagFields {
							AUTO_CUSTOM_TAG_MAP[tagNameSuffix] = append(AUTO_CUSTOM_TAG_MAP[tagNameSuffix], tagValue+suffix)
							// Used to check if the tags in the auto group tags are not duplicated with the tags in the auto custom tags
							switch tagValue {
							case "auto_instance", "auto_service":
								for deviceName, _ := range AutoMap {
									AUTO_CUSTOM_TAG_CHECK_MAP[tagValue] = append(AUTO_CUSTOM_TAG_CHECK_MAP[tagValue], deviceName+suffix)
								}
								autoMap := map[string]map[string]int{
									"auto_instance": AutoPodMap,
									"auto_service":  AutoServiceMap,
								}
								for deviceName, _ := range autoMap[tagValue] {
									AUTO_CUSTOM_TAG_CHECK_MAP[tagNameSuffix] = append(AUTO_CUSTOM_TAG_CHECK_MAP[tagNameSuffix], deviceName+suffix)
								}
							}
							if selectPrefixTranslator == "" {
								switch tagValue {
								case "ip":
									tagValueName := tagValue + suffix
									ip4Suffix := "ip4" + suffix
									ip6Suffix := "ip6" + suffix
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = "if(is_ipv4=1, IPv4NumToString(" + ip4Suffix + "), IPv6NumToString(" + ip6Suffix + "))"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
									ipTagTranslator := fmt.Sprintf("if(is_ipv4=1, IPv4NumToString(%s), IPv6NumToString(%s))", ip4Suffix, ip6Suffix)
									iconIDTranslator = fmt.Sprintf("%s, %s", ipTagTranslator+"!=''", "dictGet('flow_tag.device_map', 'icon_id', (toUInt64(64000),toUInt64(64000)))")
									nodeTypeTranslator = fmt.Sprintf("%s, '%s'", ipTagTranslator+"!=''", tagValue)
								case "auto_instance", "auto_service":
									tagAutoIDSuffix := tagValue + "_id" + suffix
									tagAutoTypeSuffix := tagValue + "_type" + suffix
									autoIDSuffix := "auto_service_id" + suffix
									autoTypeSuffix := "auto_service_type" + suffix
									if tagValue == "auto_instance" {
										autoTypeSuffix = "auto_instance_type" + suffix
										autoIDSuffix = "auto_instance_id" + suffix
									}
									autoNameSuffix := tagValue + suffix
									ip4Suffix := "ip4" + suffix
									ip6Suffix := "ip6" + suffix
									subnetIDSuffix := "subnet_id" + suffix
									nodeTypeStrSuffix := "dictGet('flow_tag.node_type_map', 'node_type', toUInt64(" + autoTypeSuffix + "))"
									internetIconDictGet := "dictGet('flow_tag.device_map', 'icon_id', (toUInt64(63999),toUInt64(63999)))"
									ipIconDictGet := "dictGet('flow_tag.device_map', 'icon_id', (toUInt64(64000),toUInt64(64000)))"
									autoIconDictGet := fmt.Sprintf("dictGet('flow_tag.device_map', 'icon_id', (toUInt64(%s),toUInt64(%s)))", autoTypeSuffix, autoIDSuffix)
									iconIDStrSuffix := fmt.Sprintf("multiIf(%s=%d,%s,%s=%d,%s,%s)", autoTypeSuffix, VIF_DEVICE_TYPE_INTERNET, internetIconDictGet, autoTypeSuffix, VIF_DEVICE_TYPE_IP, ipIconDictGet, autoIconDictGet)
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagAutoIDSuffix] = "if(" + autoTypeSuffix + " in (0,255)," + subnetIDSuffix + "," + autoIDSuffix + ")"
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+autoNameSuffix] = "if(" + autoTypeSuffix + " in (0,255),if(is_ipv4=1, IPv4NumToString(" + ip4Suffix + "), IPv6NumToString(" + ip6Suffix + ")),dictGet('flow_tag.device_map', 'name', (toUInt64(" + autoTypeSuffix + "),toUInt64(" + autoIDSuffix + "))))"
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagAutoTypeSuffix] = autoTypeSuffix
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagAutoIDSuffix] = "tag_int_values[indexOf(tag_int_names,'" + autoIDSuffix + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[autoNameSuffix] = "tag_string_values[indexOf(tag_string_names,'" + autoNameSuffix + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagAutoTypeSuffix] = "tag_int_values[indexOf(tag_int_names,'" + autoTypeSuffix + "')]"
									selectPrefixTranslator = "if(" + autoTypeSuffix + " in (0,255)," + subnetIDSuffix + "," + autoIDSuffix + ")!=0"
									iconIDPrefixTranslator := iconIDStrSuffix + "!=0"
									nodeTypePrefixTranslator := nodeTypeStrSuffix + "!=''"
									iconIDTranslator = fmt.Sprintf("%s, %s", iconIDPrefixTranslator, iconIDStrSuffix)
									nodeTypeTranslator = fmt.Sprintf("%s, %s", nodeTypePrefixTranslator, nodeTypeStrSuffix)
								case "region", "az", "pod_node", "pod_ns", "pod_group", "pod", "pod_cluster", "subnet", "gprocess", "lb_listener", "pod_ingress":
									tagValueName := tagValue + suffix
									tagValueID := tagValue + "_id" + suffix
									tagValueMap := tagValue + "_map"
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueID] = tagValueID
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("dictGet('flow_tag.%s', 'name', toUInt64(%s))", tagValueMap, tagValueID)
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueID] = "tag_int_values[indexOf(tag_int_names,'" + tagValueID + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
									selectPrefixTranslator = tagValueID + "!=0"
									iconIDTranslator = fmt.Sprintf("%s, dictGet('flow_tag.%s', 'icon_id', toUInt64(%s))", selectPrefixTranslator, tagValueMap, tagValueID)
									nodeTypeTranslator = fmt.Sprintf("%s, '%s'", selectPrefixTranslator, tagValue)
								case "vpc", "l2_vpc":
									tagValueName := tagValue + suffix
									tagValueID := tagValue + "_id" + suffix
									EPCIDSuffix := "epc_id" + suffix
									tagValueMap := "l3_epc_map"
									if tagValue == "vpc" || tagValue == "l2_vpc" {
										EPCIDSuffix = "l3_epc_id" + suffix
									}
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueID] = EPCIDSuffix
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("dictGet('flow_tag.%s', 'name', toUInt64(%s))", tagValueMap, EPCIDSuffix)
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueID] = "tag_int_values[indexOf(tag_int_names,'" + tagValueID + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
									selectPrefixTranslator = EPCIDSuffix + "!=-2"
									iconIDTranslator = fmt.Sprintf("%s, dictGet('flow_tag.%s', 'icon_id', toUInt64(%s))", selectPrefixTranslator, tagValueMap, EPCIDSuffix)
									nodeTypeTranslator = fmt.Sprintf("%s, '%s'", selectPrefixTranslator, tagValue)
								case "service", "pod_service":
									tagValueName := tagValue + suffix
									tagValueID := tagValue + "_id" + suffix
									serviceID := "service_id" + suffix
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueID] = serviceID
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("dictGet('flow_tag.device_map', 'name', (toUInt64(11), toUInt64(%s)))", serviceID)
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueID] = "tag_int_values[indexOf(tag_int_names,'" + tagValueID + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
									selectPrefixTranslator = serviceID + "!=0"
									iconIDTranslator = fmt.Sprintf("%s, dictGet('flow_tag.device_map', 'icon_id', (toUInt64(11), toUInt64(%s)))", selectPrefixTranslator, serviceID)
									nodeTypeTranslator = fmt.Sprintf("%s, '%s'", selectPrefixTranslator, tagValue)

								default:
									if strings.HasPrefix(tagValue, "cloud.tag.") {
										tagValueName := tagValue + suffix
										deviceTypeSuffix := "l3_device_type" + suffix
										podNSIDSuffix := "pod_ns_id" + suffix
										deviceIDSuffix := "l3_device_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "cloud.tag.")
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = "if(if(" + deviceTypeSuffix + "=1, dictGet('flow_tag.chost_cloud_tag_map', 'value', (toUInt64(" + deviceIDSuffix + "),'" + tagKey + "')), '')!='',if(" + deviceTypeSuffix + "=1, dictGet('flow_tag.chost_cloud_tag_map', 'value', (toUInt64(" + deviceIDSuffix + "),'" + tagKey + "')), ''), dictGet('flow_tag.pod_ns_cloud_tag_map', 'value', (toUInt64(" + podNSIDSuffix + "),'" + tagKey + "')))"
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "(if(" + deviceTypeSuffix + "=1, dictGet('flow_tag.chost_cloud_tag_map', 'value', (toUInt64(" + deviceIDSuffix + "),'" + tagKey + "')), '')!='') OR (dictGet('flow_tag.pod_ns_cloud_tag_map', 'value', (toUInt64(" + podNSIDSuffix + "),'" + tagKey + "'))!= '')"
										selectPrefixTranslator = tagSelectPrefixTranslaterStr
										iconIDTranslator = fmt.Sprintf("%s, 0", selectPrefixTranslator)
										nodeTypeTranslator = fmt.Sprintf("%s, '%s'", selectPrefixTranslator, tagValue)
									} else if strings.HasPrefix(tagValue, "k8s.label.") {
										tagValueName := tagValue + suffix
										podIDSuffix := "pod_id" + suffix
										serviceIDSuffix := "service_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "k8s.label.")
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = "if(dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "'))!='', dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "')), dictGet('flow_tag.pod_k8s_label_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "')))"
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "(dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "'))!='') OR (dictGet('flow_tag.pod_k8s_label_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "'))!='')"
										selectPrefixTranslator = tagSelectPrefixTranslaterStr
										iconIDTranslator = fmt.Sprintf("%s, 0", selectPrefixTranslator)
										nodeTypeTranslator = fmt.Sprintf("%s, '%s'", selectPrefixTranslator, tagValue)
									} else if strings.HasPrefix(tagValue, "k8s.annotation.") {
										tagValueName := tagValue + suffix
										podIDSuffix := "pod_id" + suffix
										serviceIDSuffix := "service_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "k8s.annotation.")
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = "if(dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "'))!='', dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(" + serviceIDSuffix + "),'%s')), dictGet('flow_tag.pod_k8s_annotation_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "')))"
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "(dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "'))!='') OR (dictGet('flow_tag.pod_k8s_annotation_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "'))!='')"
										selectPrefixTranslator = tagSelectPrefixTranslaterStr
										iconIDTranslator = fmt.Sprintf("%s, 0", selectPrefixTranslator)
										nodeTypeTranslator = fmt.Sprintf("%s, '%s'", selectPrefixTranslator, tagValue)
									} else if strings.HasPrefix(tagValue, "k8s.env.") {
										tagValueName := tagValue + suffix
										podIDSuffix := "pod_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "k8s.env.")
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = "dictGet('flow_tag.pod_k8s_env_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "'))"
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "dictGet('flow_tag.pod_k8s_env_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "'))!=''"
										selectPrefixTranslator = tagSelectPrefixTranslaterStr
										iconIDTranslator = fmt.Sprintf("%s, 0", selectPrefixTranslator)
										nodeTypeTranslator = fmt.Sprintf("%s, '%s'", selectPrefixTranslator, tagValue)
									} else if strings.HasPrefix(tagValue, "os.app.") {
										tagValueName := tagValue + suffix
										processIDSuffix := "gprocess_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "os.app.")
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = "dictGet('flow_tag.os_app_tag_map', 'value', (toUInt64(" + processIDSuffix + "),'" + tagKey + "'))"
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "dictGet('flow_tag.os_app_tag_map', 'value', (toUInt64(" + processIDSuffix + "),'" + tagKey + "'))!=''"
										selectPrefixTranslator = tagSelectPrefixTranslaterStr
										iconIDTranslator = fmt.Sprintf("%s, 0", selectPrefixTranslator)
										nodeTypeTranslator = fmt.Sprintf("%s, '%s'", selectPrefixTranslator, tagValue)
									}
									deviceType, ok := TAG_RESOURCE_TYPE_DEVICE_MAP[tagValue]
									if ok {
										nameDeviceType := deviceType
										if tagValue == "service" {
											nameDeviceType, ok = TAG_RESOURCE_TYPE_DEVICE_MAP["pod_service"]
										}
										tagValueName := tagValue + suffix
										tagValueID := tagValue + "_id" + suffix
										deviceIDSuffix := "l3_device_id" + suffix
										deviceTypeSuffix := "l3_device_type" + suffix
										diviceIDTranslator := fmt.Sprintf("if(%s=%d, %s, -1)", deviceTypeSuffix, deviceType, deviceIDSuffix)
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueID] = diviceIDTranslator
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("dictGet('flow_tag.device_map', 'name', (toUInt64(%d), toUInt64(%s)))", nameDeviceType, deviceIDSuffix)
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueID] = "tag_int_values[indexOf(tag_int_names,'" + tagValueID + "')]"
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										selectPrefixTranslator = fmt.Sprintf("(%s!=0 AND %s=%d)", deviceIDSuffix, deviceTypeSuffix, deviceType)
										iconIDTranslator = fmt.Sprintf("%s, dictGet('flow_tag.device_map', 'icon_id', (toUInt64(%d), toUInt64(%s)))", selectPrefixTranslator, nameDeviceType, deviceIDSuffix)
										nodeTypeTranslator = fmt.Sprintf("%s, '%s'", selectPrefixTranslator, tagValue)
									}
								}
							} else {
								switch tagValue {
								case "ip":
									tagValueName := tagValue + suffix
									ip4Suffix := "ip4" + suffix
									ip6Suffix := "ip6" + suffix
									ipTagTranslator := fmt.Sprintf("IF(%s, '', if(is_ipv4=1, IPv4NumToString(%s), IPv6NumToString(%s)))", selectPrefixTranslator, ip4Suffix, ip6Suffix)
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = ipTagTranslator
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
									iconIDTranslator += fmt.Sprintf(", %s, %s", ipTagTranslator+"!=''", "dictGet('flow_tag.device_map', 'icon_id', (toUInt64(64000),toUInt64(64000)))")
									nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", ipTagTranslator+"!=''", tagValue)
								case "auto_instance", "auto_service":
									tagAutoIDSuffix := tagValue + "_id" + suffix
									tagAutoTypeSuffix := tagValue + "_type" + suffix
									autoIDSuffix := "auto_service_id" + suffix
									autoTypeSuffix := "auto_service_type" + suffix
									if tagValue == "auto_instance" {
										autoTypeSuffix = "auto_instance_type" + suffix
										autoIDSuffix = "auto_instance_id" + suffix
									}
									autoNameSuffix := tagValue + suffix
									ip4Suffix := "ip4" + suffix
									ip6Suffix := "ip6" + suffix
									subnetIDSuffix := "subnet_id" + suffix
									nodeTypeStrSuffix := "dictGet('flow_tag.node_type_map', 'node_type', toUInt64(" + autoTypeSuffix + "))"
									internetIconDictGet := "dictGet('flow_tag.device_map', 'icon_id', (toUInt64(63999),toUInt64(63999)))"
									ipIconDictGet := "dictGet('flow_tag.device_map', 'icon_id', (toUInt64(64000),toUInt64(64000)))"
									autoIconDictGet := fmt.Sprintf("dictGet('flow_tag.device_map', 'icon_id', (toUInt64(%s),toUInt64(%s)))", autoTypeSuffix, autoIDSuffix)
									iconIDStrSuffix := fmt.Sprintf("multiIf(%s=%d,%s,%s=%d,%s,%s)", autoTypeSuffix, VIF_DEVICE_TYPE_INTERNET, internetIconDictGet, autoTypeSuffix, VIF_DEVICE_TYPE_IP, ipIconDictGet, autoIconDictGet)
									tagNameSelectFilterStr := "if(" + autoTypeSuffix + " in (0,255),if(is_ipv4=1, IPv4NumToString(" + ip4Suffix + "), IPv6NumToString(" + ip6Suffix + ")),dictGet('flow_tag.device_map', 'name', (toUInt64(" + autoTypeSuffix + "),toUInt64(" + autoIDSuffix + "))))"
									tagIDSelectFilterStr := "if(" + autoTypeSuffix + " in (0,255)," + subnetIDSuffix + "," + autoIDSuffix + ")"
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagAutoIDSuffix] = fmt.Sprintf("IF(%s, -1, %s)", selectPrefixTranslator, tagIDSelectFilterStr)
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+autoNameSuffix] = fmt.Sprintf("IF(%s, '', %s)", selectPrefixTranslator, tagNameSelectFilterStr)
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagAutoTypeSuffix] = fmt.Sprintf("IF(%s, -1, %s)", selectPrefixTranslator, autoTypeSuffix)
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagAutoIDSuffix] = "tag_int_values[indexOf(tag_int_names,'" + autoIDSuffix + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[autoNameSuffix] = "tag_string_values[indexOf(tag_string_names,'" + autoNameSuffix + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagAutoTypeSuffix] = "tag_int_values[indexOf(tag_int_names,'" + autoTypeSuffix + "')]"
									iconIDPrefixTranslator := iconIDStrSuffix + "!=0"
									nodeTypePrefixTranslator := nodeTypeStrSuffix + "!=''"
									selectPrefixTranslator += " OR if(" + autoTypeSuffix + " in (0,255)," + subnetIDSuffix + "," + autoIDSuffix + ")!=0"
									iconIDTranslator += fmt.Sprintf(", %s, %s", iconIDPrefixTranslator, iconIDStrSuffix)
									nodeTypeTranslator += fmt.Sprintf(", %s, %s", nodeTypePrefixTranslator, nodeTypeStrSuffix)
								case "region", "az", "pod_node", "pod_ns", "pod_group", "pod", "pod_cluster", "subnet", "gprocess", "lb_listener", "pod_ingress":
									tagValueName := tagValue + suffix
									tagValueID := tagValue + "_id" + suffix
									tagValueMap := tagValue + "_map"
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueID] = fmt.Sprintf("IF(%s, -1, %s)", selectPrefixTranslator, tagValueID)
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("IF(%s, '', dictGet('flow_tag.%s', 'name', toUInt64(%s)))", selectPrefixTranslator, tagValueMap, tagValueID)
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueID] = "tag_int_values[indexOf(tag_int_names,'" + tagValueID + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
									selectPrefixTranslator += " OR " + tagValueID + "!=0"
									iconIDTranslator += fmt.Sprintf(", %s, dictGet('flow_tag.%s', 'icon_id', toUInt64(%s))", tagValueID+"!=0", tagValueMap, tagValueID)
									nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", tagValueID+"!=0", tagValue)
								case "vpc", "l2_vpc":
									tagValueName := tagValue + suffix
									tagValueID := tagValue + "_id" + suffix
									tagValueMap := "l3_epc_map"
									EPCIDSuffix := "epc_id" + suffix
									if tagValue == "vpc" {
										EPCIDSuffix = "l3_epc_id" + suffix
									}
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueID] = fmt.Sprintf("IF(%s, -1, %s)", selectPrefixTranslator, EPCIDSuffix)
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("IF(%s, '', dictGet('flow_tag.%s', 'name', toUInt64(%s)))", selectPrefixTranslator, tagValueMap, EPCIDSuffix)
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueID] = "tag_int_values[indexOf(tag_int_names,'" + tagValueID + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
									selectPrefixTranslator += " OR " + EPCIDSuffix + "!=-2"
									iconIDTranslator += fmt.Sprintf(", %s, dictGet('flow_tag.%s', 'icon_id', toUInt64(%s))", EPCIDSuffix+"!=-2", tagValueMap, EPCIDSuffix)
									nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", EPCIDSuffix+"!=-2", tagValue)
								case "service", "pod_service":
									tagValueName := tagValue + suffix
									tagValueID := tagValue + "_id" + suffix
									serviceID := "service_id" + suffix
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueID] = fmt.Sprintf("IF(%s, -1, %s)", selectPrefixTranslator, serviceID)
									TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("IF(%s, '', dictGet('flow_tag.device_map', 'name', (toUInt64(11), toUInt64(%s) )))", selectPrefixTranslator, serviceID)
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueID] = "tag_int_values[indexOf(tag_int_names,'" + tagValueID + "')]"
									AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
									selectPrefixTranslator += " OR " + serviceID + "!=0"
									iconIDTranslator += fmt.Sprintf(", %s, dictGet('flow_tag.device_map', 'icon_id', (toUInt64(11), toUInt64(%s)))", selectPrefixTranslator, serviceID)
									nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", selectPrefixTranslator, tagValue)
								default:
									if strings.HasPrefix(tagValue, "cloud.tag.") {
										tagValueName := tagValue + suffix
										deviceTypeSuffix := "l3_device_type" + suffix
										podNSIDSuffix := "pod_ns_id" + suffix
										deviceIDSuffix := "l3_device_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "cloud.tag.")
										tagSelectFilterStr := "if(if(" + deviceTypeSuffix + "=1, dictGet('flow_tag.chost_cloud_tag_map', 'value', (toUInt64(" + deviceIDSuffix + "),'" + tagKey + "')), '')!='',if(" + deviceTypeSuffix + "=1, dictGet('flow_tag.chost_cloud_tag_map', 'value', (toUInt64(" + deviceIDSuffix + "),'" + tagKey + "')), ''), dictGet('flow_tag.pod_ns_cloud_tag_map', 'value', (toUInt64(" + podNSIDSuffix + "),'" + tagKey + "')))"
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("IF(%s, '', %s)", selectPrefixTranslator, tagSelectFilterStr)
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "(if(" + deviceTypeSuffix + "=1, dictGet('flow_tag.chost_cloud_tag_map', 'value', (toUInt64(" + deviceIDSuffix + "),'" + tagKey + "')), '')!='') OR (dictGet('flow_tag.pod_ns_cloud_tag_map', 'value', (toUInt64(" + podNSIDSuffix + "),'" + tagKey + "'))!= '')"
										selectPrefixTranslator += " OR " + tagSelectPrefixTranslaterStr
										iconIDTranslator += fmt.Sprintf(", %s, 0", tagSelectPrefixTranslaterStr)
										nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", tagSelectPrefixTranslaterStr, tagValue)
									} else if strings.HasPrefix(tagValue, "k8s.label.") {
										tagValueName := tagValue + suffix
										podIDSuffix := "pod_id" + suffix
										serviceIDSuffix := "service_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "k8s.label.")
										tagSelectFilterStr := "if(dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "'))!='', dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "')), dictGet('flow_tag.pod_k8s_label_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "')))"
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("IF(%s, '', %s)", selectPrefixTranslator, tagSelectFilterStr)
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "(dictGet('flow_tag.pod_service_k8s_label_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "'))!='') OR (dictGet('flow_tag.pod_k8s_label_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "'))!='')"
										selectPrefixTranslator += " OR " + tagSelectPrefixTranslaterStr
										iconIDTranslator += fmt.Sprintf(", %s, 0", tagSelectPrefixTranslaterStr)
										nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", tagSelectPrefixTranslaterStr, tagValue)
									} else if strings.HasPrefix(tagValue, "k8s.annotation.") {
										tagValueName := tagValue + suffix
										podIDSuffix := "pod_id" + suffix
										serviceIDSuffix := "service_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "k8s.annotation.")
										tagSelectFilterStr := "if(dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "'))!='', dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(" + serviceIDSuffix + "),'%s')), dictGet('flow_tag.pod_k8s_annotation_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "')))"
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("IF(%s, '', %s)", selectPrefixTranslator, tagSelectFilterStr)
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "(dictGet('flow_tag.pod_service_k8s_annotation_map', 'value', (toUInt64(" + serviceIDSuffix + "),'" + tagKey + "'))!='') OR (dictGet('flow_tag.pod_k8s_annotation_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "'))!='')"
										selectPrefixTranslator += " OR " + tagSelectPrefixTranslaterStr
										iconIDTranslator += fmt.Sprintf(", %s, 0", tagSelectPrefixTranslaterStr)
										nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", tagSelectPrefixTranslaterStr, tagValue)
									} else if strings.HasPrefix(tagValue, "k8s.env.") {
										tagValueName := tagValue + suffix
										podIDSuffix := "pod_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "k8s.env.")
										tagSelectFilterStr := "dictGet('flow_tag.pod_k8s_env_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "'))"
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("IF(%s, '', %s)", selectPrefixTranslator, tagSelectFilterStr)
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "dictGet('flow_tag.pod_k8s_env_map', 'value', (toUInt64(" + podIDSuffix + "),'" + tagKey + "'))!=''"
										selectPrefixTranslator += " OR " + tagSelectPrefixTranslaterStr
										iconIDTranslator += fmt.Sprintf(", %s, 0", tagSelectPrefixTranslaterStr)
										nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", tagSelectPrefixTranslaterStr, tagValue)
									} else if strings.HasPrefix(tagValue, "os.app.") {
										tagValueName := tagValue + suffix
										processIDSuffix := "gprocess_id" + suffix
										tagKey := strings.TrimPrefix(tagValue, "os.app.")
										tagSelectFilterStr := "dictGet('flow_tag.os_app_tag_map', 'value', (toUInt64(" + processIDSuffix + "),'" + tagKey + "'))"
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("IF(%s, '', %s)", selectPrefixTranslator, tagSelectFilterStr)
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										tagSelectPrefixTranslaterStr := "dictGet('flow_tag.os_app_tag_map', 'value', (toUInt64(" + processIDSuffix + "),'" + tagKey + "'))!=''"
										selectPrefixTranslator += " OR " + tagSelectPrefixTranslaterStr
										iconIDTranslator += fmt.Sprintf(", %s, 0", tagSelectPrefixTranslaterStr)
										nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", tagSelectPrefixTranslaterStr, tagValue)
									}
									deviceType, ok := TAG_RESOURCE_TYPE_DEVICE_MAP[tagValue]
									if ok {
										nameDeviceType := deviceType
										if tagValue == "service" {
											nameDeviceType, ok = TAG_RESOURCE_TYPE_DEVICE_MAP["pod_service"]
										}
										tagValueName := tagValue + suffix
										tagValueID := tagValue + "_id" + suffix
										deviceIDSuffix := "l3_device_id" + suffix
										deviceTypeSuffix := "l3_device_type" + suffix
										diviceIDTranslator := fmt.Sprintf("if(%s=%d, %s, -1)", deviceTypeSuffix, deviceType, deviceIDSuffix)
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueID] = fmt.Sprintf("IF(%s, -1, %s)", selectPrefixTranslator, diviceIDTranslator)
										TagResoureMap[tagNameSuffix]["default"].TagTranslatorMap[tagNameSuffix+"_"+tagValueName] = fmt.Sprintf("IF(%s, '', dictGet('flow_tag.device_map', 'name', (toUInt64(%d), toUInt64(%s) )))", selectPrefixTranslator, nameDeviceType, deviceIDSuffix)
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueID] = "tag_int_values[indexOf(tag_int_names,'" + tagValueID + "')]"
										AlarmEventResourceMap[tagNameSuffix]["default"].TagTranslatorMap[tagValueName] = "tag_string_values[indexOf(tag_string_names,'" + tagValueName + "')]"
										deviceSelectPrefixTranslator := fmt.Sprintf("%s!=0 AND %s=%d", deviceIDSuffix, deviceTypeSuffix, deviceType)
										selectPrefixTranslator += fmt.Sprintf(" OR (%s)", deviceSelectPrefixTranslator)
										iconIDTranslator += fmt.Sprintf(", %s, dictGet('flow_tag.device_map', 'icon_id', (toUInt64(%d), toUInt64(%s)))", deviceSelectPrefixTranslator, nameDeviceType, deviceIDSuffix)
										nodeTypeTranslator += fmt.Sprintf(", %s, '%s'", deviceSelectPrefixTranslator, tagValue)
									}
								}
							}
							TagResoureMap[tagNameSuffix]["node_type"].TagTranslator = "multiIf(" + nodeTypeTranslator + ", '')"
							TagResoureMap[tagNameSuffix]["icon_id"].TagTranslator = "multiIf(" + iconIDTranslator + ", 0)"

						}
					}
				}
			}
		}
	}

	return nil
}

// Get static tags
func GetStaticTagDescriptions(db, table string) (response *common.Result, err error) {
	response = &common.Result{
		Columns: []interface{}{
			"name", "client_name", "server_name", "display_name", "display_name_zh", "display_name_en", "type", "category",
			"operators", "permissions", "description", "description_zh", "description_en", "related_tag", "deprecated", "not_supported_operators", "table",
		},
		Values: []interface{}{},
	}
	for _, key := range TAG_DESCRIPTION_KEYS {
		if key.DB != db || (key.Table != table && !slices.Contains([]string{ckcommon.DB_NAME_EXT_METRICS, ckcommon.DB_NAME_DEEPFLOW_ADMIN, ckcommon.DB_NAME_DEEPFLOW_TENANT, ckcommon.DB_NAME_PROMETHEUS}, db)) {
			continue
		}
		tag, _ := TAG_DESCRIPTIONS[key]
		response.Values = append(
			response.Values,
			[]interface{}{
				tag.Name, tag.ClientName, tag.ServerName, tag.DisplayName, tag.DisplayNameZH, tag.DisplayNameEN, tag.Type,
				tag.Category, tag.Operators, tag.Permissions, tag.Description, tag.DescriptionZH, tag.DescriptionEN, tag.RelatedTag, tag.Deprecated, tag.NotSupportedOperators, "",
			},
		)
	}

	// auto_custom_tag
	if len(config.Cfg.AutoCustomTags) != 0 {
		for _, AutoCustomTag := range config.Cfg.AutoCustomTags {
			tagName := AutoCustomTag.TagName
			tagDisplayName := tagName
			if AutoCustomTag.DisplayName != "" {
				tagDisplayName = AutoCustomTag.DisplayName
			}
			if slices.Contains(common.PEER_TABLES, table) {
				response.Values = append(response.Values, []interface{}{
					tagName, tagName + "_0", tagName + "_1", tagDisplayName, tagDisplayName, tagDisplayName, "auto_custom_tag",
					"Custom Tag", []string{}, []bool{true, true, true}, AutoCustomTag.Description, AutoCustomTag.Description, AutoCustomTag.Description, AutoCustomTag.TagFields, false, []string{}, "",
				})
			} else if table == "alert_event" {
				response.Values = append(response.Values, []interface{}{
					tagName, tagName + "_0", tagName + "_1", tagDisplayName, tagDisplayName, tagDisplayName, "auto_custom_tag",
					"Custom Tag", []string{}, []bool{true, true, true}, AutoCustomTag.Description, AutoCustomTag.Description, AutoCustomTag.Description, AutoCustomTag.TagFields, false, []string{"select", "group"}, "",
				})
			} else if !slices.Contains(noCustomTagDB, db) && !slices.Contains(noCustomTagTable, table) {
				response.Values = append(response.Values, []interface{}{
					tagName, tagName, tagName, tagDisplayName, tagDisplayName, tagDisplayName, "auto_custom_tag",
					"Custom Tag", []string{}, []bool{true, true, true}, AutoCustomTag.Description, AutoCustomTag.Description, AutoCustomTag.Description, AutoCustomTag.TagFields, false, []string{}, "",
				})
			}
		}
	}

	if slices.Contains([]string{ckcommon.DB_NAME_EXT_METRICS, ckcommon.DB_NAME_DEEPFLOW_ADMIN, ckcommon.DB_NAME_DEEPFLOW_TENANT, ckcommon.DB_NAME_PROFILE, ckcommon.DB_NAME_PROMETHEUS}, db) {
		response.Values = append(response.Values, []interface{}{
			"tag", "tag", "tag", "tag", "tag", "tag", "map",
			"Native Tag", []string{}, []bool{true, true, true}, "tag", "tag", "tag", "", false, []string{}, "",
		})
	}
	return
}

// Get dynamic tags
func GetDynamicTagDescriptions(db, table, rawSql, queryCacheTTL, orgID string, useQueryCache bool, ctx context.Context, DebugInfo *client.DebugInfo) (response *common.Result, err error) {
	response = &common.Result{
		Columns: []interface{}{
			"name", "client_name", "server_name", "display_name", "display_name_zh", "display_name_en", "type", "category",
			"operators", "permissions", "description", "description_zh", "description_en", "related_tag", "deprecated", "not_supported_operators", "table",
		},
		Values: []interface{}{},
	}
	notSupportOperator := []string{}
	if table == "alert_event" {
		notSupportOperator = []string{"select", "group"}
	}

	// 查询 k8s_label
	chClient := client.Client{
		Host:     config.Cfg.Clickhouse.Host,
		Port:     config.Cfg.Clickhouse.Port,
		UserName: config.Cfg.Clickhouse.User,
		Password: config.Cfg.Clickhouse.Password,
		DB:       "flow_tag",
		Context:  ctx,
	}

	k8sLabelSql := "SELECT key FROM (SELECT key FROM flow_tag.pod_service_k8s_label_map UNION ALL SELECT key FROM flow_tag.pod_k8s_label_map) GROUP BY key"
	chClient.Debug = client.NewDebug(k8sLabelSql)
	k8sLabelRst, err := chClient.DoQuery(&client.QueryParams{Sql: k8sLabelSql, UseQueryCache: useQueryCache, QueryCacheTTL: queryCacheTTL, ORGID: orgID})
	if DebugInfo != nil {
		DebugInfo.Debug = append(DebugInfo.Debug, *chClient.Debug)
	}
	if err != nil {
		return
	}
	for _, _key := range k8sLabelRst.Values {
		key := _key.([]interface{})[0]
		labelKey := "k8s.label." + key.(string)
		if slices.Contains(common.PEER_TABLES, table) {
			response.Values = append(response.Values, []interface{}{
				labelKey, labelKey + "_0", labelKey + "_1", labelKey, labelKey, labelKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		} else if !slices.Contains(noCustomTagDB, db) && !slices.Contains(noCustomTagTable, table) {
			response.Values = append(response.Values, []interface{}{
				labelKey, labelKey, labelKey, labelKey, labelKey, labelKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		}
	}
	// 查询 k8s_annotation
	k8sAnnotationSql := "SELECT key FROM (SELECT key FROM flow_tag.pod_k8s_annotation_map UNION ALL SELECT key FROM flow_tag.pod_service_k8s_annotation_map) GROUP BY key"
	chClient.Debug = client.NewDebug(k8sAnnotationSql)
	k8sAnnotationRst, err := chClient.DoQuery(&client.QueryParams{
		Sql:           k8sAnnotationSql,
		UseQueryCache: useQueryCache,
		QueryCacheTTL: queryCacheTTL,
		ORGID:         orgID,
	})
	if DebugInfo != nil {
		DebugInfo.Debug = append(DebugInfo.Debug, *chClient.Debug)
	}
	if err != nil {
		return
	}
	for _, _key := range k8sAnnotationRst.Values {
		key := _key.([]interface{})[0]
		annotationKey := "k8s.annotation." + key.(string)
		if slices.Contains(common.PEER_TABLES, table) {
			response.Values = append(response.Values, []interface{}{
				annotationKey, annotationKey + "_0", annotationKey + "_1", annotationKey, annotationKey, annotationKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		} else if !slices.Contains(noCustomTagDB, db) && !slices.Contains(noCustomTagTable, table) {
			response.Values = append(response.Values, []interface{}{
				annotationKey, annotationKey, annotationKey, annotationKey, annotationKey, annotationKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		}
	}

	// 查询 k8s_env
	podK8senvSql := "SELECT key FROM flow_tag.pod_k8s_env_map GROUP BY key"
	chClient.Debug = client.NewDebug(podK8senvSql)
	podK8senvRst, err := chClient.DoQuery(&client.QueryParams{
		Sql: podK8senvSql, UseQueryCache: useQueryCache, QueryCacheTTL: queryCacheTTL, ORGID: orgID})
	if DebugInfo != nil {
		DebugInfo.Debug = append(DebugInfo.Debug, *chClient.Debug)
	}
	if err != nil {
		return
	}
	for _, _key := range podK8senvRst.Values {
		key := _key.([]interface{})[0]
		envKey := "k8s.env." + key.(string)
		if slices.Contains(common.PEER_TABLES, table) {
			response.Values = append(response.Values, []interface{}{
				envKey, envKey + "_0", envKey + "_1", envKey, envKey, envKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		} else if !slices.Contains(noCustomTagDB, db) && !slices.Contains(noCustomTagTable, table) {
			response.Values = append(response.Values, []interface{}{
				envKey, envKey, envKey, envKey, envKey, envKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		}
	}

	// 查询cloud.tag
	cloudTagSql := "SELECT key FROM (SELECT key FROM flow_tag.chost_cloud_tag_map UNION ALL SELECT key FROM flow_tag.pod_ns_cloud_tag_map) GROUP BY key"
	chClient.Debug = client.NewDebug(cloudTagSql)
	cloudTagRst, err := chClient.DoQuery(&client.QueryParams{Sql: cloudTagSql, UseQueryCache: useQueryCache, QueryCacheTTL: queryCacheTTL, ORGID: orgID})
	if DebugInfo != nil {
		DebugInfo.Debug = append(DebugInfo.Debug, *chClient.Debug)
	}
	if err != nil {
		return
	}
	for _, _key := range cloudTagRst.Values {
		key := _key.([]interface{})[0]
		chostCloudTagKey := "cloud.tag." + key.(string)
		if slices.Contains(common.PEER_TABLES, table) {
			response.Values = append(response.Values, []interface{}{
				chostCloudTagKey, chostCloudTagKey + "_0", chostCloudTagKey + "_1", chostCloudTagKey, chostCloudTagKey, chostCloudTagKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		} else if !slices.Contains(noCustomTagDB, db) && !slices.Contains(noCustomTagTable, table) {
			response.Values = append(response.Values, []interface{}{
				chostCloudTagKey, chostCloudTagKey, chostCloudTagKey, chostCloudTagKey, chostCloudTagKey, chostCloudTagKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		}
	}

	// 查询 os.app
	osAPPTagSql := "SELECT key FROM flow_tag.os_app_tag_map GROUP BY key"
	chClient.Debug = client.NewDebug(osAPPTagSql)
	osAPPTagRst, err := chClient.DoQuery(&client.QueryParams{Sql: osAPPTagSql, UseQueryCache: useQueryCache, QueryCacheTTL: queryCacheTTL, ORGID: orgID})
	if DebugInfo != nil {
		DebugInfo.Debug = append(DebugInfo.Debug, *chClient.Debug)
	}
	if err != nil {
		return
	}
	for _, _key := range osAPPTagRst.Values {
		key := _key.([]interface{})[0]
		osAPPTagKey := "os.app." + key.(string)
		if slices.Contains(common.PEER_TABLES, table) {
			response.Values = append(response.Values, []interface{}{
				osAPPTagKey, osAPPTagKey + "_0", osAPPTagKey + "_1", osAPPTagKey, osAPPTagKey, osAPPTagKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		} else if !slices.Contains(noCustomTagDB, db) && !slices.Contains(noCustomTagTable, table) {
			response.Values = append(response.Values, []interface{}{
				osAPPTagKey, osAPPTagKey, osAPPTagKey, osAPPTagKey, osAPPTagKey, osAPPTagKey, "map_item",
				"Custom Tag", tagTypeToOperators["string"], []bool{true, true, true}, "", "", "", "", false, notSupportOperator, "",
			})
		}
	}

	// 查询外部字段
	if !slices.Contains([]string{ckcommon.DB_NAME_EXT_METRICS, ckcommon.DB_NAME_FLOW_LOG, ckcommon.DB_NAME_DEEPFLOW_ADMIN, ckcommon.DB_NAME_DEEPFLOW_TENANT, ckcommon.DB_NAME_EVENT, ckcommon.DB_NAME_PROMETHEUS, ckcommon.DB_NAME_APPLICATION_LOG, "_prometheus"}, db) || (db == "flow_log" && table != "l7_flow_log") {
		return response, nil
	}
	externalChClient := client.Client{
		Host:     config.Cfg.Clickhouse.Host,
		Port:     config.Cfg.Clickhouse.Port,
		UserName: config.Cfg.Clickhouse.User,
		Password: config.Cfg.Clickhouse.Password,
		DB:       "flow_tag",
		Context:  ctx,
	}
	rawSql = strings.ReplaceAll(rawSql, " where ", " WHERE ")
	rawSql = strings.ReplaceAll(rawSql, " limit ", " LIMIT ")
	limit := "10000"
	var whereSql string
	if strings.Contains(rawSql, "WHERE") {
		whereSql = strings.Split(rawSql, "WHERE")[1]
	}
	if strings.Contains(rawSql, "WHERE") {
		whereSql = strings.Split(rawSql, "WHERE")[1]
		if strings.Contains(whereSql, " LIMIT ") {
			limit = strings.Split(whereSql, " LIMIT ")[1]
			whereSql = strings.Split(whereSql, " LIMIT ")[0]
		}
	} else {
		if strings.Contains(rawSql, " LIMIT ") {
			limit = strings.Split(rawSql, " LIMIT ")[1]
		}
	}
	externalSql := ""
	if whereSql != "" {
		whereSql = strings.ReplaceAll(whereSql, " name ", " field_name ")
		if db == "_prometheus" {
			externalSql = fmt.Sprintf("SELECT field_name AS tag_name, table FROM flow_tag.prometheus_custom_field WHERE field_type='tag' AND (%s) GROUP BY tag_name, table ORDER BY tag_name ASC LIMIT %s", whereSql, limit)
		} else if table == "" {
			externalSql = fmt.Sprintf("SELECT field_name AS tag_name, table FROM flow_tag.%s_custom_field WHERE field_type='tag' AND (%s) GROUP BY tag_name, table ORDER BY tag_name ASC LIMIT %s", db, whereSql, limit)
		} else if table == "alert_event" {
			externalSql = fmt.Sprintf("SELECT field_name AS tag_name, table, field_value_type FROM flow_tag.%s_custom_field WHERE table='%s' AND field_type='tag' AND (%s) GROUP BY tag_name, table, field_value_type ORDER BY tag_name ASC LIMIT %s", db, table, whereSql, limit)
		} else {
			externalSql = fmt.Sprintf("SELECT field_name AS tag_name, table FROM flow_tag.%s_custom_field WHERE table='%s' AND field_type='tag' AND (%s) GROUP BY tag_name, table ORDER BY tag_name ASC LIMIT %s", db, table, whereSql, limit)
		}
	} else {
		if db == "_prometheus" {
			externalSql = fmt.Sprintf("SELECT field_name AS tag_name, table FROM flow_tag.prometheus_custom_field WHERE field_type='tag' GROUP BY tag_name, table ORDER BY tag_name ASC LIMIT %s", limit)
		} else if table == "" {
			externalSql = fmt.Sprintf("SELECT field_name AS tag_name, table FROM flow_tag.%s_custom_field WHERE field_type='tag' GROUP BY tag_name, table ORDER BY tag_name ASC LIMIT %s", db, limit)
		} else if table == "alert_event" {
			externalSql = fmt.Sprintf("SELECT field_name AS tag_name, table, field_value_type FROM flow_tag.%s_custom_field WHERE table='%s' AND field_type='tag' GROUP BY tag_name, table, field_value_type ORDER BY tag_name ASC LIMIT %s", db, table, limit)
		} else {
			externalSql = fmt.Sprintf("SELECT field_name AS tag_name, table FROM flow_tag.%s_custom_field WHERE table='%s' AND field_type='tag' GROUP BY tag_name, table ORDER BY tag_name ASC LIMIT %s", db, table, limit)
		}
	}
	externalChClient.Debug = client.NewDebug(externalSql)
	externalRst, err := externalChClient.DoQuery(&client.QueryParams{Sql: externalSql, UseQueryCache: useQueryCache, QueryCacheTTL: queryCacheTTL, ORGID: orgID})
	if DebugInfo != nil {
		DebugInfo.Debug = append(DebugInfo.Debug, *externalChClient.Debug)
	}
	if err != nil {
		return
	}
	for _, _tagName := range externalRst.Values {
		tagName := _tagName.([]interface{})[0]
		tableName := _tagName.([]interface{})[1]
		if db == "_prometheus" {
			externalTag := tagName.(string)
			response.Values = append(response.Values, []interface{}{
				externalTag, externalTag, externalTag, externalTag, externalTag, externalTag, "map_item",
				"Native Tag", tagTypeToOperators["string"], []bool{true, true, true}, externalTag, externalTag, externalTag, "", false, notSupportOperator, tableName,
			})
		} else if table == "alert_event" {
			externalTag := tagName.(string)
			var categoryValue string
			// fieltValueType := _tagName.([]interface{})[2]
			if strings.HasPrefix(externalTag, "cloud.tag.") || strings.HasPrefix(externalTag, "k8s.label.") || strings.HasPrefix(externalTag, "os.app.") || strings.HasPrefix(externalTag, "k8s.annotation.") || strings.HasPrefix(externalTag, "k8s.env.") {
				categoryValue = "Custom Tag"
				response.Values = append(response.Values, []interface{}{
					externalTag, externalTag, externalTag, externalTag, externalTag, externalTag, "map_item",
					categoryValue, tagTypeToOperators["string"], []bool{true, true, true}, externalTag, externalTag, externalTag, "", false, notSupportOperator, tableName,
				})
			} else if strings.HasPrefix(externalTag, "tag.") || strings.HasPrefix(externalTag, "attribute.") {
				categoryValue = "Native Tag"
				response.Values = append(response.Values, []interface{}{
					externalTag, externalTag, externalTag, externalTag, externalTag, externalTag, "map_item",
					categoryValue, tagTypeToOperators["string"], []bool{true, true, true}, externalTag, externalTag, externalTag, "", false, notSupportOperator, tableName,
				})
			} else {
				categoryValue = _tagName.([]interface{})[2].(string)
				response.Values = append(response.Values, []interface{}{
					externalTag, externalTag, externalTag, externalTag, externalTag, externalTag, categoryValue,
					categoryValue, tagTypeToOperators[categoryValue], []bool{true, true, true}, externalTag, externalTag, externalTag, "", false, notSupportOperator, tableName,
				})
			}

		} else if slices.Contains(tagNativeTagDB, db) {
			externalTag := "tag." + tagName.(string)
			response.Values = append(response.Values, []interface{}{
				externalTag, externalTag, externalTag, externalTag, externalTag, externalTag, "map_item",
				"Native Tag", tagTypeToOperators["string"], []bool{true, true, true}, externalTag, externalTag, externalTag, "", false, notSupportOperator, tableName,
			})
		} else {
			externalTag := "attribute." + tagName.(string)
			response.Values = append(response.Values, []interface{}{
				externalTag, externalTag, externalTag, externalTag, externalTag, externalTag, "map_item",
				"Native Tag", tagTypeToOperators["string"], []bool{true, true, true}, externalTag, externalTag, externalTag, "", false, notSupportOperator, tableName,
			})
		}
	}
	return
}

func GetAlertEventTagDescriptions(staticTag, dynamicTag *common.Result) (response *common.Result, err error) {
	response = &common.Result{
		Columns: []interface{}{
			"name", "client_name", "server_name", "display_name", "display_name_zh", "display_name_en", "type", "category",
			"operators", "permissions", "description", "description_zh", "description_en", "related_tag", "deprecated", "not_supported_operators", "table",
		},
		Values: []interface{}{},
	}
	valuesMap := map[string]interface{}{}
	staticValues := staticTag.Values
	dynamiicValues := dynamicTag.Values
	for _, staticItem := range staticValues {
		valuesMap[staticItem.([]interface{})[0].(string)] = staticItem
	}

	// if dynamiic tag_name not in staticvalues, add to result
	for _, dynamicItem := range dynamiicValues {
		dynamicTagName := strings.TrimSuffix(dynamicItem.([]interface{})[0].(string), "_0")
		dynamicTagName = strings.TrimSuffix(dynamicTagName, "_1")
		dynamicTagName = strings.TrimSuffix(dynamicTagName, "_id")
		if valuesMap[dynamicTagName] == nil && valuesMap[dynamicItem.([]interface{})[0].(string)] == nil {
			valuesMap[dynamicTagName] = dynamicItem
		}
	}

	for _, value := range valuesMap {
		response.Values = append(response.Values, value)
	}

	return response, nil
}

func GetTagDescriptions(db, table, rawSql, queryCacheTTL, orgID string, useQueryCache bool, ctx context.Context, DebugInfo *client.DebugInfo) (response *common.Result, err error) {
	// 把`1m`的反引号去掉
	table = strings.Trim(table, "`")
	response = &common.Result{
		Columns: []interface{}{
			"name", "client_name", "server_name", "display_name", "display_name_zh", "display_name_en", "type", "category",
			"operators", "permissions", "description", "description_zh", "description_en", "related_tag", "deprecated", "not_supported_operators", "table",
		},
		Values: []interface{}{},
	}

	staticResponse, err := GetStaticTagDescriptions(db, table)
	if err != nil {
		return
	}
	DynamicResponse, err := GetDynamicTagDescriptions(db, table, rawSql, queryCacheTTL, orgID, useQueryCache, ctx, DebugInfo)
	if err != nil {
		return
	}
	if table == "alert_event" {
		return GetAlertEventTagDescriptions(staticResponse, DynamicResponse)
	}
	response.Values = append(response.Values, staticResponse.Values...)
	response.Values = append(response.Values, DynamicResponse.Values...)
	return
}

func GetTagValuesDescriptions(db, rawSql, queryCacheTTL, orgID string, useQueryCache bool, ctx context.Context) (sqlList []string, err error) {
	var whereSql string
	var sql string
	limit := "10000"
	rawSql = strings.ReplaceAll(rawSql, " where ", " WHERE ")
	rawSql = strings.ReplaceAll(rawSql, " limit ", " LIMIT ")
	if strings.Contains(rawSql, "WHERE") {
		whereSql = strings.Split(rawSql, "WHERE")[1]
		if strings.Contains(whereSql, " LIMIT ") {
			limit = strings.Split(whereSql, " LIMIT ")[1]
			whereSql = strings.Split(whereSql, " LIMIT ")[0]

		}
	} else {
		if strings.Contains(rawSql, " LIMIT ") {
			limit = strings.Split(rawSql, " LIMIT ")[1]
		}
	}
	switch db {
	case ckcommon.DB_NAME_PROMETHEUS, "_prometheus":
		if whereSql != "" {
			sql = fmt.Sprintf("SELECT field_name AS label_name, field_value AS label_value FROM prometheus_custom_field_value WHERE %s GROUP BY label_name, label_value ORDER BY label_name ASC LIMIT %s", whereSql, limit)
		} else {
			sql = fmt.Sprintf("SELECT field_name AS label_name, field_value AS label_value FROM prometheus_custom_field_value GROUP BY label_name, label_value ORDER BY label_name ASC LIMIT %s", limit)
		}
		sqlList = append(sqlList, sql)
		return sqlList, nil
	}
	return sqlList, nil
}

func GetEnumTagValues(db, table, sql string) (map[string][]interface{}, error) {
	// 把`1m`的反引号去掉
	table = strings.Trim(table, "`")
	// 获取tagEnumFile
	sqlSplit := strings.Split(sql, " ")
	tag := sqlSplit[2]
	tag = strings.Trim(tag, "'")
	response := map[string][]interface{}{}
	if tag == "all_int_enum" {
		for key, tagValue := range TAG_INT_ENUMS {
			tagValues := []interface{}{}
			for _, value := range tagValue {
				tagValues = append(tagValues, []interface{}{value.Value, value.DisplayNameZH, value.DisplayNameEN, value.DescriptionZH, value.DescriptionEN})
			}
			response[key] = tagValues
		}
	}
	if tag == "all_string_enum" {
		for key, tagValue := range TAG_STRING_ENUMS {
			tagValues := []interface{}{}
			for _, value := range tagValue {
				tagValues = append(tagValues, []interface{}{value.Value, value.DisplayNameZH, value.DisplayNameEN, value.DescriptionZH, value.DescriptionEN})
			}
			response[key] = tagValues
		}
	}
	return response, nil
}

func GetEnumTags(db, table, sql string) (*common.Result, error) {
	response := &common.Result{
		Columns: []interface{}{
			"name", "client_name", "server_name", "display_name", "display_name_zh", "display_name_en", "type", "category",
			"operators", "permissions", "description", "description_zh", "description_en", "related_tag", "deprecated", "not_supported_operators", "table",
		},
		Values: []interface{}{},
	}
	enumTagSlice := []string{}

	for _, tagDescription := range TAG_DESCRIPTIONS {
		if (tagDescription.Type == "int_enum" || tagDescription.Type == "string_enum") && tagDescription.Name != "app_service" && tagDescription.Name != "app_instance" {
			if !slices.Contains(enumTagSlice, tagDescription.Name) {
				enumTagSlice = append(enumTagSlice, tagDescription.Name)
				response.Values = append(response.Values,
					[]interface{}{
						tagDescription.Name, tagDescription.ClientName, tagDescription.ServerName, tagDescription.DisplayName, tagDescription.DisplayNameZH, tagDescription.DisplayNameEN, tagDescription.Type,
						tagDescription.Category, tagDescription.Operators, tagDescription.Permissions, tagDescription.Description, tagDescription.DescriptionZH, tagDescription.DescriptionEN, tagDescription.RelatedTag, tagDescription.Deprecated, tagDescription.NotSupportedOperators, "",
					})
			}

		}
	}
	return response, nil
}

func GetEnumTagAllValues(db, table, sql, language string) ([]string, error) {
	sqlList := []string{}
	sqlSplit := strings.Fields(sql)
	tag := sqlSplit[2]
	tag_name := ""
	tag_names := []string{}

	for _, tagDescription := range TAG_DESCRIPTIONS {
		if tagDescription.Name == tag {
			_, isEnumOK := TAG_ENUMS[tagDescription.EnumFile]
			if !isEnumOK {
				return sqlList, errors.New(fmt.Sprintf("tag %s is not enum", tag))
			}
			_, isStringEnumOK := TAG_STRING_ENUMS[tagDescription.EnumFile]
			if isStringEnumOK {
				table = "string_enum_map"
				tag_name = tagDescription.EnumFile
				if !slices.Contains(tag_names, "'"+tag_name+"'") {
					tag_names = append(tag_names, "'"+tag_name+"'")
				}
			}
			_, isIntEnumOK := TAG_INT_ENUMS[tagDescription.EnumFile]
			if isIntEnumOK {
				table = "int_enum_map"
				tag_name = tagDescription.EnumFile
				if !slices.Contains(tag_names, "'"+tag_name+"'") {
					tag_names = append(tag_names, "'"+tag_name+"'")
				}
			}
		}
	}
	nameColumn := ""
	descriptionColumn := ""
	if language != "" {
		nameColumn = "name_" + language
		descriptionColumn = "description_" + language
	} else {
		cfgLang := ""
		if config.Cfg.Language == "en" {
			cfgLang = "en"
		} else {
			cfgLang = "zh"
		}
		nameColumn = "name_" + cfgLang
		descriptionColumn = "description_" + cfgLang
	}
	sql = fmt.Sprintf("SELECT value, %s AS display_name, %s AS description FROM %s WHERE tag_name IN (%s) GROUP BY value, display_name, description ORDER BY value ASC", nameColumn, descriptionColumn, table, strings.Join(tag_names, ","))
	log.Debug(sql)
	sqlList = append(sqlList, sql)
	return sqlList, nil
}

func GetTagValues(db, table, sql, queryCacheTTL, orgID, language string, useQueryCache bool) (*common.Result, []string, error) {
	var sqlList []string
	// 把`1m`的反引号去掉
	table = strings.Trim(table, "`")
	// 获取tagEnumFile
	sqlSplit := strings.Fields(sql)
	tag := sqlSplit[2]
	if strings.HasPrefix(tag, "`") && strings.HasSuffix(tag, "`") {
		tag = strings.TrimPrefix(tag, "`")
		tag = strings.TrimSuffix(tag, "`")
	}
	if strings.Contains(tag, "'") || strings.Contains(tag, "`") {
		return nil, sqlList, errors.New(fmt.Sprintf("Tags containing single quotes (') or backquote (`) or space are not supported: %s ", tag))
	}
	var rgx = regexp.MustCompile(`(?i)show|SHOW +tag +\S+ +values +from|FROM +\S+( +(where|WHERE \S+ like|LIKE \S+))?`)
	sqlOk := rgx.MatchString(sql)
	if !sqlOk {
		return nil, sqlList, errors.New(fmt.Sprintf("sql synax error: %s ", sql))
	}
	showSqlList := strings.Split(sql, " WHERE ")
	if len(showSqlList) == 1 {
		showSqlList = strings.Split(sql, " where ")
	}

	// K8s Labels是动态的,不需要去tag_description里确认
	if strings.HasPrefix(tag, "k8s.label.") || strings.HasPrefix(tag, "k8s.annotation.") || strings.HasPrefix(tag, "k8s.env.") || strings.HasPrefix(tag, "cloud.tag.") || strings.HasPrefix(tag, "os.app.") {
		return GetTagResourceValues(db, table, sql)
	}
	// 外部字段是动态的,不需要去tag_description里确认
	if strings.HasPrefix(tag, "tag.") || strings.HasPrefix(tag, "attribute.") {
		return GetExternalTagValues(db, table, sql)
	}
	if db == "ext_metrics" {
		table = "ext_common"
	} else if db == ckcommon.DB_NAME_DEEPFLOW_ADMIN {
		table = "deepflow_server"
	} else if db == ckcommon.DB_NAME_DEEPFLOW_TENANT {
		table = "deepflow_collector"
	} else if db == ckcommon.DB_NAME_PROMETHEUS {
		table = "samples"
	}
	tagDescription, ok := TAG_DESCRIPTIONS[TagDescriptionKey{
		DB: db, Table: table, TagName: tag,
	}]
	if !ok {
		if table == "alert_event" {
			return nil, sqlList, nil
		} else {
			return nil, sqlList, errors.New(fmt.Sprintf("no tag %s in %s.%s", tag, db, table))
		}
	}
	// 根据tagEnumFile获取values
	_, isEnumOK := TAG_ENUMS[tagDescription.EnumFile]
	if !isEnumOK {
		return GetTagResourceValues(db, table, sql)
	}
	_, isStringEnumOK := TAG_STRING_ENUMS[tagDescription.EnumFile]
	if isStringEnumOK {
		table = "string_enum_map"
		tag = tagDescription.EnumFile
	}
	_, isIntEnumOK := TAG_INT_ENUMS[tagDescription.EnumFile]
	if isIntEnumOK {
		table = "int_enum_map"
		tag = tagDescription.EnumFile
	}

	var limitSql string
	var likeSql string
	var whereSql string
	var orderBy = "value"
	limitList := strings.Split(sql, " LIMIT ")
	if len(limitList) <= 1 {
		limitList = strings.Split(sql, " limit ")
	}
	likeSql = limitList[0]
	if len(limitList) > 1 {
		limitSql = " LIMIT " + limitList[1]
	}
	likeList := strings.Split(likeSql, " WHERE ")
	if len(likeList) == 1 {
		likeList = strings.Split(likeSql, " where ")
	}
	if len(likeList) > 1 {
		if strings.Trim(likeList[1], " ") != "" {
			if strings.Contains(likeList[1], " like ") || strings.Contains(likeList[1], " LIKE ") {
				whereSql = " AND (" + strings.ReplaceAll(likeList[1], "*", "%") + ")"
			} else {
				whereSql = " AND (" + likeList[1] + ")"
			}
		}
	}
	if strings.Contains(strings.ToLower(sql), "like") || strings.Contains(strings.ToLower(sql), "regexp") {
		orderBy = "length(display_name)"
	}
	nameColumn := ""
	descriptionColumn := ""
	if language != "" {
		nameColumn = "name_" + language
		descriptionColumn = "description_" + language
	} else {
		cfgLang := ""
		if config.Cfg.Language == "en" {
			cfgLang = "en"
		} else {
			cfgLang = "zh"
		}
		nameColumn = "name_" + cfgLang
		descriptionColumn = "description_" + cfgLang
	}
	// querier will be called later, so there is no need to display the declaration db
	sql = fmt.Sprintf("SELECT value, %s AS display_name, %s AS description FROM %s WHERE tag_name='%s' %s GROUP BY value, display_name, description ORDER BY %s ASC %s", nameColumn, descriptionColumn, table, tag, whereSql, orderBy, limitSql)
	log.Debug(sql)
	sqlList = append(sqlList, sql)
	return nil, sqlList, nil

}

func GetTagResourceValues(db, table, rawSql string) (*common.Result, []string, error) {
	// Resource tag showtagvalues:
	// Device resources, auto_instance, auto_service and resources without their own map, use device_map.
	// Resources that have their own map, use their own map.

	sqlSplit := strings.Fields(rawSql)
	tag := sqlSplit[2]
	tag = strings.Trim(tag, "'")
	if strings.HasPrefix(tag, "`") && strings.HasSuffix(tag, "`") {
		tag = strings.TrimPrefix(tag, "`")
		tag = strings.TrimSuffix(tag, "`")
	}
	var sqlList []string
	var sql string
	var whereSql string
	var limitSql string
	var orderBy = "value"
	if strings.Contains(strings.ToLower(rawSql), "like") || strings.Contains(strings.ToLower(rawSql), "regexp") {
		orderBy = "length(display_name)"
	}

	if strings.Contains(rawSql, " WHERE ") || strings.Contains(rawSql, " where ") {
		if len(strings.Split(rawSql, " WHERE ")) == 1 {
			whereSql = strings.Split(rawSql, " where ")[1]
		} else {
			whereSql = strings.Split(rawSql, " WHERE ")[1]
		}
		whereLimitList := strings.Split(whereSql, " LIMIT ")
		if len(whereLimitList) <= 1 {
			whereLimitList = strings.Split(whereSql, " limit ")
		}

		if strings.Trim(whereLimitList[0], " ") != "" {
			if strings.Contains(whereLimitList[0], " like ") || strings.Contains(whereLimitList[0], " LIKE ") {
				whereSql = " WHERE (" + strings.ReplaceAll(whereLimitList[0], "*", "%") + ")"
			} else {
				whereSql = " WHERE (" + whereLimitList[0] + ")"
			}
		}
	}

	limitList := strings.Split(rawSql, " LIMIT ")
	if len(limitList) <= 1 {
		limitList = strings.Split(rawSql, " limit ")
	}
	if len(limitList) > 1 {
		limitSql = " LIMIT " + limitList[1]
	}
	// querier will be called later, so there is no need to display the declaration db
	deviceType, ok := TAG_RESOURCE_TYPE_DEVICE_MAP[tag]
	if ok {
		if tag == "chost" || tag == "pod_service" {
			sql = fmt.Sprintf("SELECT id as value,name AS display_name FROM %s %s GROUP BY value, display_name ORDER BY %s ASC %s", tag+"_map", whereSql, orderBy, limitSql)

		} else {
			if whereSql != "" {
				whereSql += fmt.Sprintf("AND devicetype=%d", deviceType)
			} else {
				whereSql = fmt.Sprintf("WHERE devicetype=%d", deviceType)
			}
			sql = fmt.Sprintf("SELECT deviceid AS value,name AS display_name,uid FROM device_map %s GROUP BY value, display_name, uid ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
		}
	} else if common.IsValueInSliceString(tag, TAG_RESOURCE_TYPE_DEFAULT) {
		sql = fmt.Sprintf("SELECT id as value,name AS display_name FROM %s %s GROUP BY value, display_name ORDER BY %s ASC %s", tag+"_map", whereSql, orderBy, limitSql)
	} else if common.IsValueInSliceString(tag, TAG_RESOURCE_TYPE_AUTO) {
		var autoDeviceTypes []string
		for _, deviceType := range AutoMap {
			autoDeviceTypes = append(autoDeviceTypes, strconv.Itoa(deviceType))
		}
		autoMap := map[string]map[string]int{
			"auto_instance": AutoPodMap,
			"auto_service":  AutoServiceMap,
		}
		for _, deviceType := range autoMap[tag] {
			autoDeviceTypes = append(autoDeviceTypes, strconv.Itoa(deviceType))
		}
		if whereSql != "" {
			whereSql += fmt.Sprintf("AND devicetype in (%s)", strings.Join(autoDeviceTypes, ","))
		} else {
			whereSql = fmt.Sprintf("WHERE devicetype in (%s)", strings.Join(autoDeviceTypes, ","))
		}
		sql = fmt.Sprintf(
			"SELECT deviceid AS value,name AS display_name,devicetype AS device_type,uid, icon_id FROM device_map %s GROUP BY value, display_name, device_type, uid, icon_id ORDER BY %s ASC %s",
			whereSql, orderBy, limitSql,
		)
	} else if tag == "vpc" || tag == "l2_vpc" {
		sql = fmt.Sprintf("SELECT id as value,name AS display_name,uid FROM l3_epc_map %s GROUP BY value, display_name, uid ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else if tag == "ip" {
		sql = fmt.Sprintf("SELECT ip as value,ip AS display_name FROM ip_relation_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else if tag == "tap" || tag == "capture_network_type" {
		sql = fmt.Sprintf("SELECT value, name AS display_name FROM tap_type_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else if tag == "vtap" || tag == "agent" {
		sql = fmt.Sprintf("SELECT id as value, name AS display_name FROM vtap_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else if tag == "lb_listener" {
		sql = fmt.Sprintf("SELECT id as value, name AS display_name FROM lb_listener_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else if tag == "policy" || tag == "npb_tunnel" {
		sql = fmt.Sprintf("SELECT id as value, name AS display_name FROM %s_map %s GROUP BY value, display_name ORDER BY %s ASC %s", tag, whereSql, orderBy, limitSql)
	} else if tag == "alert_policy" {
		sql = fmt.Sprintf("SELECT id AS value, name AS display_name FROM alarm_policy_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else if tag == "app_service" {
		if whereSql != "" {
			whereSql += fmt.Sprintf(" AND `table`='%s' AND app_service!=''", table)
		} else {
			whereSql = fmt.Sprintf(" WHERE `table`='%s' AND app_service!=''", table)
		}
		sql = fmt.Sprintf("SELECT app_service AS value, app_service AS display_name FROM %s_app_service %s GROUP BY value, display_name ORDER BY %s ASC %s", db, whereSql, orderBy, limitSql)
	} else if tag == "app_instance" {
		if whereSql != "" {
			whereSql += fmt.Sprintf(" AND `table`='%s' AND app_instance!=''", table)
		} else {
			whereSql = fmt.Sprintf(" WHERE `table`='%s' AND app_instance!=''", table)
		}
		sql = fmt.Sprintf("SELECT app_instance AS value, app_instance AS display_name FROM %s_app_service %s GROUP BY value, display_name ORDER BY %s ASC %s", db, whereSql, orderBy, limitSql)
	} else if tag == "user" {
		sql = fmt.Sprintf("SELECT id AS value, name AS display_name FROM user_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else if tag == common.TAP_PORT_HOST || tag == common.TAP_PORT_CHOST || tag == common.TAP_PORT_POD_NODE || tag == common.CAPTURE_NIC_HOST || tag == common.CAPTURE_NIC_CHOST || tag == common.CAPTURE_NIC_POD_NODE {
		if whereSql != "" {
			whereSql += fmt.Sprintf(" AND device_type=%d", TAP_PORT_DEVICE_MAP[tag])
		} else {
			whereSql = fmt.Sprintf(" WHERE device_type=%d", TAP_PORT_DEVICE_MAP[tag])
		}
		sql = fmt.Sprintf("SELECT device_id AS value, device_name AS display_name FROM vtap_port_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else if tag == "pod_ingress" {
		sql = fmt.Sprintf("SELECT id as value, name AS display_name FROM pod_ingress_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else if strings.HasPrefix(tag, "k8s.label.") {
		labelTag := strings.TrimPrefix(tag, "k8s.label.")
		if whereSql != "" {
			whereSql += fmt.Sprintf(" AND `key`='%s'", labelTag)
		} else {
			whereSql = fmt.Sprintf("WHERE `key`='%s'", labelTag)
		}
		results := &common.Result{}
		for _, table := range []string{"pod_service_k8s_label_map", "pod_k8s_label_map"} {
			sql = fmt.Sprintf("SELECT value, value AS display_name FROM %s %s GROUP BY value, display_name ORDER BY %s ASC %s", table, whereSql, orderBy, limitSql)
			log.Debug(sql)
			sqlList = append(sqlList, sql)
		}
		return results, sqlList, nil
	} else if strings.HasPrefix(tag, "k8s.annotation.") {
		annotationTag := strings.TrimPrefix(tag, "k8s.annotation.")
		if whereSql != "" {
			whereSql += fmt.Sprintf(" AND `key`='%s'", annotationTag)
		} else {
			whereSql = fmt.Sprintf("WHERE `key`='%s'", annotationTag)
		}
		results := &common.Result{}
		for _, table := range []string{"pod_service_k8s_annotation_map", "pod_k8s_annotation_map"} {
			sql = fmt.Sprintf("SELECT value, value AS display_name FROM %s %s GROUP BY value, display_name ORDER BY %s ASC %s", table, whereSql, orderBy, limitSql)
			log.Debug(sql)
			sqlList = append(sqlList, sql)
		}
		return results, sqlList, nil
	} else if strings.HasPrefix(tag, "k8s.env.") {
		envTag := strings.TrimPrefix(tag, "k8s.env.")
		if whereSql != "" {
			whereSql += fmt.Sprintf(" AND `key`='%s'", envTag)
		} else {
			whereSql = fmt.Sprintf("WHERE `key`='%s'", envTag)
		}
		results := &common.Result{}

		sql = fmt.Sprintf("SELECT value, value AS display_name FROM pod_k8s_env_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
		log.Debug(sql)
		sqlList = append(sqlList, sql)

		return results, sqlList, nil
	} else if strings.HasPrefix(tag, "cloud.tag.") {
		cloudTag := strings.TrimPrefix(tag, "cloud.tag.")
		if whereSql != "" {
			whereSql += fmt.Sprintf(" AND `key`='%s'", cloudTag)
		} else {
			whereSql = fmt.Sprintf("WHERE `key`='%s'", cloudTag)
		}
		results := &common.Result{}
		for _, table := range []string{"chost_cloud_tag_map", "pod_ns_cloud_tag_map"} {
			sql = fmt.Sprintf("SELECT value, value AS display_name FROM %s %s GROUP BY value, display_name ORDER BY %s ASC %s", table, whereSql, orderBy, limitSql)
			log.Debug(sql)
			sqlList = append(sqlList, sql)
		}
		return results, sqlList, nil
	} else if strings.HasPrefix(tag, "os.app.") {
		osAPPTag := strings.TrimPrefix(tag, "os.app.")
		if whereSql != "" {
			whereSql += fmt.Sprintf(" AND `key`='%s'", osAPPTag)
		} else {
			whereSql = fmt.Sprintf("WHERE `key`='%s'", osAPPTag)
		}
		sql = fmt.Sprintf("SELECT value, value AS display_name FROM os_app_tag_map %s GROUP BY value, display_name ORDER BY %s ASC %s", whereSql, orderBy, limitSql)
	} else {
		for resourceName, resourceInfo := range HOSTNAME_IP_DEVICE_MAP {
			if tag != resourceName {
				continue
			}

			if resourceInfo.ResourceType == VIF_DEVICE_TYPE_VM {
				if whereSql != "" {
					whereSql += " AND display_name!=''"
				} else {
					whereSql = " WHERE display_name!=''"
				}
				sql = strings.Join([]string{
					"SELECT id AS value,", resourceInfo.FieldName, "AS display_name",
					"FROM chost_map", whereSql,
					"GROUP BY value, display_name",
					"ORDER BY", orderBy, "ASC", limitSql,
				}, " ")
			} else {
				deviceTypeStr := strconv.Itoa(resourceInfo.ResourceType)
				if whereSql != "" {
					whereSql += " AND devicetype=" + deviceTypeStr
				} else {
					whereSql = " WHERE devicetype=" + deviceTypeStr
				}
				sql = strings.Join([]string{
					"SELECT deviceid AS value,", resourceInfo.FieldName, "AS display_name",
					"FROM device_map", whereSql, "AND display_name!=''",
					"GROUP BY value, display_name",
					"ORDER BY", orderBy, "ASC", limitSql,
				}, " ")
			}
			break
		}
	}
	if sql == "" {
		return GetExternalTagValues(db, table, rawSql)
	}
	log.Debug(sql)
	sqlList = append(sqlList, sql)
	return nil, sqlList, nil
}

func GetExternalTagValues(db, table, rawSql string) (*common.Result, []string, error) {
	sqlSplit := strings.Fields(rawSql)
	tag := sqlSplit[2]
	tag = strings.Trim(tag, "'")
	if strings.HasPrefix(tag, "`") && strings.HasSuffix(tag, "`") {
		tag = strings.TrimPrefix(tag, "`")
		tag = strings.TrimSuffix(tag, "`")
	}
	tag = strings.TrimPrefix(tag, "tag.")
	tag = strings.TrimPrefix(tag, "attribute.")
	var sqlList []string
	var whereSql string
	var orderBy = "sum(count) DESC"
	if strings.Contains(strings.ToLower(rawSql), "like") || strings.Contains(strings.ToLower(rawSql), "regexp") {
		orderBy = "length(display_name) ASC"
	}
	limitSql := "LIMIT 10000"
	if strings.Contains(rawSql, " WHERE ") || strings.Contains(rawSql, " where ") {
		if len(strings.Split(rawSql, " WHERE ")) == 1 {
			whereSql = strings.Split(rawSql, " where ")[1]
		} else {
			whereSql = strings.Split(rawSql, " WHERE ")[1]
		}
		whereLimitList := strings.Split(whereSql, " LIMIT ")
		if len(whereLimitList) <= 1 {
			whereLimitList = strings.Split(whereSql, " limit ")
		}
		if strings.Trim(whereLimitList[0], " ") != "" {
			if strings.Contains(whereLimitList[0], " like ") || strings.Contains(whereLimitList[0], " LIKE ") {
				whereSql = " AND (" + strings.ReplaceAll(whereLimitList[0], "*", "%") + ")"
			} else {
				whereSql = " AND (" + whereLimitList[0] + ")"
			}
		}
	}
	limitList := strings.Split(rawSql, " LIMIT ")
	if len(limitList) <= 1 {
		limitList = strings.Split(rawSql, " limit ")
	}
	if len(limitList) > 1 {
		limitSql = " LIMIT " + limitList[1]
	}

	var sql string
	if whereSql != "" {
		sql = fmt.Sprintf("SELECT field_value AS value, value AS display_name FROM %s_custom_field_value WHERE `table`='%s' AND field_type='tag' AND field_name='%s' %s GROUP BY value, display_name ORDER BY %s %s", db, table, tag, whereSql, orderBy, limitSql)
	} else {
		sql = fmt.Sprintf("SELECT field_value AS value, value AS display_name FROM %s_custom_field_value WHERE `table`='%s' AND field_type='tag' AND field_name='%s' GROUP BY value, display_name ORDER BY %s %s", db, table, tag, orderBy, limitSql)
	}
	log.Debug(sql)
	sqlList = append(sqlList, sql)
	return nil, sqlList, nil
}
