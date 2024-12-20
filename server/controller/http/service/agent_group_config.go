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

package service

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
	"gorm.io/gorm"

	agentconf "github.com/deepflowio/deepflow/server/agent_config"
	"github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/config"
	"github.com/deepflowio/deepflow/server/controller/db/metadb"
	"github.com/deepflowio/deepflow/server/controller/db/metadb/model"
	httpcommon "github.com/deepflowio/deepflow/server/controller/http/common"
	"github.com/deepflowio/deepflow/server/controller/trisolaris/refresh"
)

var (
	DataTypeYAML = 1
	DataTypeJSON = 2
)

type AgentGroupConfig struct {
	cfg *config.ControllerConfig

	resourceAccess *ResourceAccess // FIXME 实际没有使用此数据做权限控制，重构 UserInfo 传递方式

	dataType int
}

func NewAgentGroupConfig(userInfo *httpcommon.UserInfo, cfg *config.ControllerConfig) *AgentGroupConfig {
	return &AgentGroupConfig{
		cfg:            cfg,
		resourceAccess: &ResourceAccess{Fpermit: cfg.FPermit, UserInfo: userInfo},
	}
}

func (a *AgentGroupConfig) GetAgentGroupConfigTemplateJson() ([]byte, error) {
	dbInfo, err := metadb.GetDB(a.resourceAccess.UserInfo.ORGID)
	if err != nil {
		return nil, err
	}
	var tapTypes []model.TapType
	if err := dbInfo.Where("value != ?", common.TAP_TYPE_VALUE_CLOUD_NETWORK).Select("value", "name").Find(&tapTypes).Error; err != nil {
		return nil, err
	}
	tapTypInfos := make([]map[string]interface{}, len(tapTypes))
	for i, tapType := range tapTypes {
		tapTypInfos[i] = map[string]interface{}{
			strconv.Itoa(tapType.Value): map[string]interface{}{
				"ch": tapType.Name,
				"en": tapType.Name,
			},
		}
	}
	tapTypeInfosYamlBytes, err := yaml.Marshal(tapTypInfos)
	if err != nil {
		return nil, err
	}
	var tapTypeInfosNode yaml.Node
	if err := yaml.Unmarshal(tapTypeInfosYamlBytes, &tapTypeInfosNode); err != nil {
		return nil, err
	}
	var plugins []model.Plugin
	if err := dbInfo.Select("type", "id", "name").Find(&plugins).Error; err != nil {
		return nil, err
	}
	wasmPluginInfos := make([]map[string]interface{}, 0)
	soPluginInfos := make([]map[string]interface{}, 0)
	for _, plugin := range plugins {
		if plugin.Type == 1 {
			wasmPluginInfos = append(wasmPluginInfos, map[string]interface{}{
				plugin.Name: map[string]interface{}{
					"ch": plugin.Name,
					"en": plugin.Name,
				},
			})
		} else if plugin.Type == 2 {
			soPluginInfos = append(soPluginInfos, map[string]interface{}{
				plugin.Name: map[string]interface{}{
					"ch": plugin.Name,
					"en": plugin.Name,
				},
			})
		}
	}
	wasmPluginInfosYamlBytes, err := yaml.Marshal(wasmPluginInfos)
	if err != nil {
		return nil, err
	}
	soPluginInfosYamlBytes, err := yaml.Marshal(soPluginInfos)
	if err != nil {
		return nil, err
	}
	var wasmPluginInfosNode yaml.Node
	if err := yaml.Unmarshal(wasmPluginInfosYamlBytes, &wasmPluginInfosNode); err != nil {
		return nil, err
	}
	var soPluginInfosNode yaml.Node
	if err := yaml.Unmarshal(soPluginInfosYamlBytes, &soPluginInfosNode); err != nil {
		return nil, err
	}
	var domains []model.Domain
	if err := dbInfo.Select("id", "name").Find(&domains).Error; err != nil {
		return nil, err
	}
	domainInfos := make([]map[string]interface{}, len(domains))
	for i, domain := range domains {
		domainInfos[i] = map[string]interface{}{
			strconv.Itoa(domain.ID): map[string]interface{}{
				"ch": domain.Name,
				"en": domain.Name,
			},
		}
	}
	domainKeyToInfoYamlBytes, err := yaml.Marshal(domainInfos)
	if err != nil {
		return nil, err
	}
	var domainKeyToInfoNode yaml.Node
	if err := yaml.Unmarshal(domainKeyToInfoYamlBytes, &domainKeyToInfoNode); err != nil {
		return nil, err
	}
	// TODO get from ck
	l7Protocols := []string{
		"HTTP", "HTTP2", "Dubbo", "gRPC", "SOFARPC", "FastCGI", "bRPC", "Tars", "Some/IP", "MySQL", "PostgreSQL",
		"Oracle", "Redis", "MongoDB", "Memcached", "Kafka", "MQTT", "AMQP", "OpenWire", "NATS", "Pulsar", "ZMTP",
		"RocketMQ", "DNS", "TLS", "Custom"}
	l7ProtocolsYamlBytes, err := yaml.Marshal(l7Protocols)
	if err != nil {
		return nil, err
	}
	var l7ProtocolsNode yaml.Node
	if err := yaml.Unmarshal(l7ProtocolsYamlBytes, &l7ProtocolsNode); err != nil {
		return nil, err
	}
	dynamicOptions := agentconf.DynamicOptions{
		"inputs.cbpf.physical_mirror.default_capture_network_type_comment.enum_options":                tapTypeInfosNode.Content[0],
		"outputs.flow_log.filters.l4_capture_network_types_comment.enum_options":                       tapTypeInfosNode.Content[0],
		"outputs.flow_log.filters.l7_capture_network_types_comment.enum_options":                       tapTypeInfosNode.Content[0],
		"inputs.resources.pull_resource_from_controller.domain_filter_comment.enum_options":            domainKeyToInfoNode.Content[0],
		"plugins.wasm_plugins_comment.enum_options":                                                    wasmPluginInfosNode.Content[0],
		"plugins.so_plugins_comment.enum_options":                                                      soPluginInfosNode.Content[0],
		"inputs.ebpf.socket.preprocess.out_of_order_reassembly_protocols_comment.enum_options":         l7ProtocolsNode.Content[0],
		"inputs.ebpf.socket.preprocess.segmentation_reassembly_protocols_comment.enum_options":         l7ProtocolsNode.Content[0],
		"processors.request_log.application_protocol_inference.enabled_protocols_comment.enum_options": l7ProtocolsNode.Content[0],
		"processors.request_log.filters.port_number_prefilters_comment.enum_options":                   l7ProtocolsNode.Content[0],
		"processors.request_log.filters.tag_filters_comment.enum_options":                              l7ProtocolsNode.Content[0],
	}
	return agentconf.ConvertTemplateYAMLToJSON(dynamicOptions)
}

func (a *AgentGroupConfig) GetAgentGroupConfig(groupLcuuid string, dataType int) ([]byte, error) {
	dbInfo, err := metadb.GetDB(a.resourceAccess.UserInfo.ORGID)
	if err != nil {
		log.Errorf("failed to get db info: %v", err)
		return nil, err
	}
	var data agentconf.MySQLAgentGroupConfiguration
	if err := dbInfo.Where("agent_group_lcuuid = ?", groupLcuuid).First(&data).Error; err != nil {
		log.Errorf("failed to get agent_group_configuration (agent group lcuuid %s): %v", groupLcuuid, err)
		return nil, err
	}

	return a.strToBytes(data.Yaml, dataType)
}

func (a *AgentGroupConfig) strToBytes(data string, returnType int) ([]byte, error) {
	if returnType == DataTypeJSON {
		if data == "" {
			return []byte("{}"), nil
		}
		return agentconf.ConvertYAMLToJSON([]byte(data))
	} else {
		return []byte(data), nil
	}
}

func (a *AgentGroupConfig) GetAgentGroupConfigs(dataType int) (interface{}, error) {
	dbInfo, err := metadb.GetDB(a.resourceAccess.UserInfo.ORGID)
	if err != nil {
		return nil, err
	}
	var data []agentconf.MySQLAgentGroupConfiguration
	if err := dbInfo.Find(&data).Error; err != nil {
		return nil, err
	}
	if dataType == DataTypeJSON {
		var jsonArray bytes.Buffer
		jsonArray.WriteByte('{')
		for i, d := range data {
			if i > 0 {
				jsonArray.WriteByte(',')
			}
			bs, err := a.strToBytes(d.Yaml, dataType)
			if err != nil {
				return nil, err
			}
			jsonArray.WriteString(`"` + d.AgentGroupLcuuid + `":`)
			jsonArray.Write(bs)
		}
		jsonArray.WriteByte('}')
		return jsonArray.Bytes(), nil
	} else {
		return data, nil
	}
}

func (a *AgentGroupConfig) getStringYaml(data interface{}, dataType int) (string, error) {
	if dataType == DataTypeJSON {
		bytes, err := agentconf.ConvertJSONToYAMLAndValidate(data.(map[string]interface{}))
		return string(bytes), err
	} else {
		err := agentconf.ValidateYAML(data.([]byte))
		if err != nil {
			return "", fmt.Errorf("yaml validate failed: %v, please check the yaml format", err)
		}
		return string(data.([]byte)), nil
	}
}

func (a *AgentGroupConfig) CreateAgentGroupConfig(groupLcuuid string, data interface{}, dataType int) ([]byte, error) {
	dbInfo, err := metadb.GetDB(a.resourceAccess.UserInfo.ORGID)
	if err != nil {
		return nil, err
	}
	if dataType == DataTypeJSON {
		log.Infof("create agent group config, group lcuuid: %s, data: %#v, data type: %d", groupLcuuid, data, dataType, dbInfo.LogPrefixORGID)
	} else {
		log.Infof("create agent group config, group lcuuid: %s, data: %s, data type: %d", groupLcuuid, string(data.([]byte)), dataType, dbInfo.LogPrefixORGID)
	}
	var agentGroup model.VTapGroup
	if err := dbInfo.Where("lcuuid = ?", groupLcuuid).First(&agentGroup).Error; err != nil {
		return nil, err
	}

	strYaml, err := a.getStringYaml(data, dataType)
	if err != nil {
		log.Errorf("failed to convert data to yaml: %v", err)
		return nil, err
	}

	var agentGroupConfig agentconf.MySQLAgentGroupConfiguration
	if err := dbInfo.Where("agent_group_lcuuid = ?", groupLcuuid).First(&agentGroupConfig).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			newConfig := &agentconf.MySQLAgentGroupConfiguration{
				Lcuuid:           uuid.New().String(),
				AgentGroupLcuuid: groupLcuuid,
				Yaml:             strYaml,
			}
			if err := dbInfo.Create(newConfig).Error; err != nil {
				log.Errorf("failed to insert agent_group_configuration (agent group lcuuid %s): %v", groupLcuuid, err)
				return nil, err
			}
		} else {
			log.Errorf("failed to get agent_group_configuration (agent group lcuuid %s): %v", groupLcuuid, err)
			return nil, err
		}
	} else {
		// TODO(weiqiang): duplicate and verify
		agentGroupConfig.Yaml = strYaml
		if err := dbInfo.Save(&agentGroupConfig).Error; err != nil {
			log.Errorf("failed to update agent_group_configuration (agent group lcuuid %s): %v", groupLcuuid, err)
			return nil, err
		}
	}

	refresh.RefreshCache(dbInfo.GetORGID(), []common.DataChanged{common.DATA_CHANGED_VTAP})
	return a.strToBytes(agentGroupConfig.Yaml, dataType)
}

func (a *AgentGroupConfig) UpdateAgentGroupConfig(groupLcuuid string, data interface{}, dataType int) ([]byte, error) {
	dbInfo, err := metadb.GetDB(a.resourceAccess.UserInfo.ORGID)
	if err != nil {
		return nil, err
	}
	if dataType == DataTypeJSON {
		log.Infof("update agent group config, group lcuuid: %s, data: %#v, data type: %d", groupLcuuid, data, dataType, dbInfo.LogPrefixORGID)
	} else {
		log.Infof("update agent group config, group lcuuid: %s, data: %s, data type: %d", groupLcuuid, string(data.([]byte)), dataType, dbInfo.LogPrefixORGID)
	}
	var agentGroup model.VTapGroup
	if err := dbInfo.Where("lcuuid = ?", groupLcuuid).First(&agentGroup).Error; err != nil {
		log.Errorf("failed to get vtap_group (lcuuid %s): %v", groupLcuuid, err)
		return nil, err
	}

	strYaml, err := a.getStringYaml(data, dataType)
	if err != nil {
		log.Errorf("failed to convert data to yaml: %v", err)
		return nil, err
	}

	var agentGroupConfig agentconf.MySQLAgentGroupConfiguration
	if err := dbInfo.Where("agent_group_lcuuid = ?", groupLcuuid).First(&agentGroupConfig).Error; err != nil {
		log.Errorf("failed to get agent_group_configuration (agent group lcuuid %s): %v", groupLcuuid, err)
		return nil, err
	} else {
		agentGroupConfig.Yaml = strYaml
		if err := dbInfo.Save(&agentGroupConfig).Error; err != nil {
			log.Errorf("failed to update agent_group_configuration (agent group lcuuid %s): %v", groupLcuuid, err)
			return nil, err
		}
	}

	refresh.RefreshCache(dbInfo.GetORGID(), []common.DataChanged{common.DATA_CHANGED_VTAP})
	return a.GetAgentGroupConfig(groupLcuuid, dataType)
}

func (a *AgentGroupConfig) DeleteAgentGroupConfig(groupLcuuid string) error {
	dbInfo, err := metadb.GetDB(a.resourceAccess.UserInfo.ORGID)
	if err != nil {
		return err
	}

	db := dbInfo.GetGORMDB()
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("agent_group_lcuuid = ?", groupLcuuid).Delete(&agentconf.MySQLAgentGroupConfiguration{}).Error; err != nil {
			return fmt.Errorf("failed to delete agent_group_configuration (agent group lcuuid %s): %v", groupLcuuid, err)
		}
		if err := tx.Where("vtap_group_lcuuid = ?", groupLcuuid).Delete(&agentconf.AgentGroupConfigModel{}).Error; err != nil {
			return fmt.Errorf("failed to delete vtap_group_configuration (agent group lcuuid %s): %v", groupLcuuid, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	refresh.RefreshCache(dbInfo.GetORGID(), []common.DataChanged{common.DATA_CHANGED_VTAP})
	return nil
}
