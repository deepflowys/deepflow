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
	"fmt"
	"io/ioutil"
	"time"

	"github.com/deepflowio/deepflow/server/libs/eventapi"
	"github.com/deepflowio/deepflow/server/libs/nativetag"
	"github.com/deepflowio/deepflow/server/libs/queue"
	"github.com/deepflowio/deepflow/server/libs/tracetree"
	logging "github.com/op/go-logging"
	yaml "gopkg.in/yaml.v2"
)

var log = logging.MustGetLogger("server_common")

const QUEUE_SIZE = 1 << 16

type ControllerIngesterShared struct {
	ResourceEventQueue *queue.OverwriteQueue
	TraceTreeQueue     *queue.OverwriteQueue
}

func NewControllerIngesterShared() *ControllerIngesterShared {
	return &ControllerIngesterShared{
		ResourceEventQueue: queue.NewOverwriteQueue(
			"controller-to-ingester-resource_event", QUEUE_SIZE,
			queue.OptionFlushIndicator(time.Second*3),
			queue.OptionRelease(func(p interface{}) { p.(*eventapi.ResourceEvent).Release() })),
		TraceTreeQueue: queue.NewOverwriteQueue(
			"querier-to-ingester-trace_tree", QUEUE_SIZE,
			queue.OptionFlushIndicator(time.Second*3),
			queue.OptionRelease(func(p interface{}) { p.(*tracetree.TraceTree).Release() })),
	}
}

type Config struct {
	Ingester IngesterConfig `yaml:"ingester"`
}

type IngesterConfig struct {
	Exporters []ExportersConfig `yaml:"exporters"`
}

type ExportersConfig struct {
	Enabled bool `yaml:"enabled"`
}

func ExportersEnabled(configPath string) bool {
	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Error("Read config file error:", err)
		return false
	}
	config := Config{}
	if err = yaml.Unmarshal(configBytes, &config); err != nil {
		log.Error("Unmarshal yaml error:", err)
		return false
	}
	for _, v := range config.Ingester.Exporters {
		if v.Enabled {
			log.Info("exporters enabled")
			return true
		}
	}
	return false
}

type OrgHanderInterface interface {
	DropOrg(orgId uint16) error
	UpdateNativeTag(uint16, nativetag.NativeTagTable, *nativetag.NativeTag) error
}

var ingesterOrgHanders []OrgHanderInterface

func SetOrgHandler(orgHandler OrgHanderInterface) {
	ingesterOrgHanders = append(ingesterOrgHanders, orgHandler)
}

/*
* 调用此接口删除组织时，ingester 会删除 ClickHouse 中所有该组织的数据库，并清理内存中对应的 ClickHouse session。
* 注意：当 deepflow-agent 携带的 org_id 在 ClickHouse 中没有对应的数据库时，
* 同时满足下面两个条件，则 ingester 会自动为该 org_id 创建 ClickHouse 数据库：
* 1. ingester 最近一次（每分钟获取一次）从 controller 获取到的 org_id list 中存在该 org_id
* 2. 该 org_id 最近没有被删除过，或者被删除的时间早于 ingester 获取到 org_id list 的时间
* 因此，被删除的 org_id 不要立即复用，以避免被删除的组织中有一些 deepflow-agent 仍然在运行且未进入逃逸状态时，会将数据写入到复用此 org_id 的数据库中。
* 另外，注意当 org 被删除时，controller 需要确保下发的 org_id list 中不要包含被删除的 org_id。
* ----------------------------------------------------------------------
* When calling this interface to delete an organization,
* Ingester will delete all the databases of that organization in ClickHouse and clean up the corresponding ClickHouse sessions in memory.
* Note: When the org_id carried by the Deepflow-agent does not have a corresponding database in ClickHouse,
* If both of the following conditions are met simultaneously, Ingester will automatically create a ClickHouse database for the org_id:
* 1. The latest (obtained every minute) org_id list obtained by Ingester from the Controller contains the same org_id
* 2. The org_id has not been deleted recently, or it was deleted earlier than the time when Ingester obtained the org_id list
* Therefore, the deleted org_id should not be reused immediately to avoid some Deepflow-agents in the deleted organization still running and not entering the escape state,
* which will write data to the database that reuses this org_id.
* Additionally, please note that when an org is deleted, the controller needs to ensure that the issued org_i list does not contain the deleted org_id.
 */
func DropOrg(orgId uint16) error {
	log.Info("drop org id:", orgId)
	if ingesterOrgHanders == nil {
		return fmt.Errorf("ingesterOrgHanders is nil, drop org id %d failed", orgId)
	}
	for _, ingesterOrgHander := range ingesterOrgHanders {
		err := ingesterOrgHander.DropOrg(orgId)
		if err != nil {
			return err
		}
	}
	return nil
}

// 1. When starting, you need to call the interface, and set 'isStartUp' to 'true'.
// 2. When adding or removing native_tag, you need to call the interface, and set 'isStartUp' to 'false'
func UpdateNativeTag(isStartUp bool, orgId uint16, table nativetag.NativeTagTable, nativeTag *nativetag.NativeTag) error {
	log.Infof("isstart %v orgId %d update %s native tag: %+v", isStartUp, orgId, table.Table(), nativeTag)
	if nativeTag == nil {
		return nil
	}

	if !isStartUp {
		if ingesterOrgHanders == nil {
			err := fmt.Errorf("ingester is not ready, update native tag failed")
			log.Error(err)
			return err
		}
		for _, ingesterOrgHander := range ingesterOrgHanders {
			err := ingesterOrgHander.UpdateNativeTag(orgId, table, nativeTag)
			if err != nil {
				log.Error(err)
				return err
			}
		}
	}
	nativetag.UpdateNativeTag(orgId, table, nativeTag)
	return nil
}
