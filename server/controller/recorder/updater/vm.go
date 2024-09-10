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

package updater

import (
	"encoding/json"

	cloudcommon "github.com/deepflowio/deepflow/server/controller/cloud/common"
	cloudmodel "github.com/deepflowio/deepflow/server/controller/cloud/model"
	ctrlrcommon "github.com/deepflowio/deepflow/server/controller/common"
	mysqlmodel "github.com/deepflowio/deepflow/server/controller/db/mysql/model"
	"github.com/deepflowio/deepflow/server/controller/recorder/cache"
	"github.com/deepflowio/deepflow/server/controller/recorder/cache/diffbase"
	"github.com/deepflowio/deepflow/server/controller/recorder/db"
	"github.com/deepflowio/deepflow/server/controller/recorder/pubsub/message"
)

type VM struct {
	UpdaterBase[
		cloudmodel.VM,
		*diffbase.VM,
		*mysqlmodel.VM,
		mysqlmodel.VM,
		*message.VMAdd,
		message.VMAdd,
		*message.VMUpdate,
		message.VMUpdate,
		*message.VMFieldsUpdate,
		message.VMFieldsUpdate,
		*message.VMDelete,
		message.VMDelete]
}

func NewVM(wholeCache *cache.Cache, cloudData []cloudmodel.VM) *VM {
	updater := &VM{
		newUpdaterBase[
			cloudmodel.VM,
			*diffbase.VM,
			*mysqlmodel.VM,
			mysqlmodel.VM,
			*message.VMAdd,
			message.VMAdd,
			*message.VMUpdate,
			message.VMUpdate,
			*message.VMFieldsUpdate,
			message.VMFieldsUpdate,
			*message.VMDelete,
		](
			ctrlrcommon.RESOURCE_TYPE_VM_EN,
			wholeCache,
			db.NewVM(),
			wholeCache.DiffBaseDataSet.VMs,
			cloudData,
		),
	}
	updater.dataGenerator = updater
	updater.initDBOperator()
	return updater
}

func (m *VM) getDiffBaseByCloudItem(cloudItem *cloudmodel.VM) (diffBase *diffbase.VM, exists bool) {
	diffBase, exists = m.diffBaseData[cloudItem.Lcuuid]
	return
}

func (m *VM) generateDBItemToAdd(cloudItem *cloudmodel.VM) (*mysqlmodel.VM, bool) {
	vpcID, exists := m.cache.ToolDataSet.GetVPCIDByLcuuid(cloudItem.VPCLcuuid)
	if !exists {
		log.Error(resourceAForResourceBNotFound(
			ctrlrcommon.RESOURCE_TYPE_VPC_EN, cloudItem.VPCLcuuid,
			ctrlrcommon.RESOURCE_TYPE_VM_EN, cloudItem.Lcuuid,
		), m.metadata.LogPrefixes)
		return nil, false
	}
	var hostID int
	if cloudItem.LaunchServer != "" {
		hostID, _ = m.cache.ToolDataSet.GetHostIDByIP(cloudItem.LaunchServer)
	}
	cloudTags := map[string]string{}
	if cloudItem.CloudTags != nil {
		cloudTags = cloudItem.CloudTags
	}
	dbItem := &mysqlmodel.VM{
		Name:         cloudItem.Name,
		Label:        cloudItem.Label,
		IP:           cloudItem.IP,
		Hostname:     cloudItem.Hostname,
		UID:          cloudItem.Label,
		State:        cloudItem.State,
		HType:        cloudItem.HType,
		LaunchServer: cloudItem.LaunchServer,
		HostID:       hostID,
		Domain:       m.metadata.Domain.Lcuuid,
		Region:       cloudItem.RegionLcuuid,
		AZ:           cloudItem.AZLcuuid,
		VPCID:        vpcID,
		CloudTags:    cloudTags,
	}
	dbItem.Lcuuid = cloudItem.Lcuuid
	if !cloudItem.CreatedAt.IsZero() {
		dbItem.CreatedAt = cloudItem.CreatedAt
	}
	return dbItem, true
}

func (m *VM) getUpdateableFields() []string {
	return []string{"epc_id", "name", "label", "ip", "hostname", "state", "htype", "launch_server", "host_id", "region", "az", "cloud_tags"}
}

func (m *VM) generateUpdateInfo(diffBase *diffbase.VM, cloudItem *cloudmodel.VM) (*message.VMFieldsUpdate, map[string]interface{}, bool) {
	structInfo := new(message.VMFieldsUpdate)
	mapInfo := make(map[string]interface{})
	if diffBase.VPCLcuuid != cloudItem.VPCLcuuid {
		vpcID, exists := m.cache.ToolDataSet.GetVPCIDByLcuuid(cloudItem.VPCLcuuid)
		if !exists {
			log.Error(resourceAForResourceBNotFound(
				ctrlrcommon.RESOURCE_TYPE_VPC_EN, cloudItem.VPCLcuuid,
				ctrlrcommon.RESOURCE_TYPE_VM_EN, cloudItem.Lcuuid,
			), m.metadata.LogPrefixes)
			return nil, nil, false
		}
		mapInfo["epc_id"] = vpcID
		structInfo.VPCID.SetNew(vpcID) // TODO is old value needed?
		structInfo.VPCLcuuid.Set(diffBase.VPCLcuuid, cloudItem.VPCLcuuid)
	}
	if diffBase.Name != cloudItem.Name {
		mapInfo["name"] = cloudItem.Name
		structInfo.Name.Set(diffBase.Name, cloudItem.Name)
	}
	if diffBase.Label != cloudItem.Label {
		mapInfo["label"] = cloudItem.Label
		structInfo.Label.Set(diffBase.Label, cloudItem.Label)
	}
	if diffBase.IP != cloudItem.IP {
		mapInfo["ip"] = cloudItem.IP
		structInfo.IP.Set(diffBase.IP, cloudItem.IP)
	}
	if diffBase.Hostname != cloudItem.Hostname {
		mapInfo["hostname"] = cloudItem.Hostname
		structInfo.Hostname.Set(diffBase.Hostname, cloudItem.Hostname)
	}
	if diffBase.State != cloudItem.State {
		mapInfo["state"] = cloudItem.State
		structInfo.State.Set(diffBase.State, cloudItem.State)
	}
	if diffBase.HType != cloudItem.HType {
		mapInfo["htype"] = cloudItem.HType
		structInfo.HType.Set(diffBase.HType, cloudItem.HType)
	}
	if diffBase.LaunchServer != cloudItem.LaunchServer {
		mapInfo["launch_server"] = cloudItem.LaunchServer
		structInfo.LaunchServer.Set(diffBase.LaunchServer, cloudItem.LaunchServer)
	}
	if cloudItem.LaunchServer != "" {
		hostID, _ := m.cache.ToolDataSet.GetHostIDByIP(cloudItem.LaunchServer)
		if diffBase.HostID != hostID {
			mapInfo["host_id"] = hostID
			structInfo.HostID.Set(diffBase.HostID, hostID)
		}
	}
	if diffBase.RegionLcuuid != cloudItem.RegionLcuuid {
		mapInfo["region"] = cloudItem.RegionLcuuid
		structInfo.RegionLcuuid.Set(diffBase.RegionLcuuid, cloudItem.RegionLcuuid)
	}
	if diffBase.AZLcuuid != cloudItem.AZLcuuid {
		mapInfo["az"] = cloudItem.AZLcuuid
		structInfo.AZLcuuid.Set(diffBase.AZLcuuid, cloudItem.AZLcuuid)
	}
	if cloudcommon.DiffMap(diffBase.CloudTags, cloudItem.CloudTags) {
		updateTags := map[string]string{}
		if cloudItem.CloudTags != nil {
			updateTags = cloudItem.CloudTags
		}
		tagsJson, _ := json.Marshal(updateTags)
		mapInfo["cloud_tags"] = tagsJson
		structInfo.CloudTags.Set(diffBase.CloudTags, cloudItem.CloudTags)
	}

	return structInfo, mapInfo, len(mapInfo) > 0
}

func (m *VM) setUpdatedFields(dbItem *mysqlmodel.VM, updateInfo *message.VMFieldsUpdate) {
	if updateInfo.VPCID.IsDifferent() {
		dbItem.VPCID = updateInfo.VPCID.GetNew()
	}
	if updateInfo.Name.IsDifferent() {
		dbItem.Name = updateInfo.Name.GetNew()
	}
	if updateInfo.Label.IsDifferent() {
		dbItem.Label = updateInfo.Label.GetNew()
	}
	if updateInfo.IP.IsDifferent() {
		dbItem.IP = updateInfo.IP.GetNew()
	}
	if updateInfo.Hostname.IsDifferent() {
		dbItem.Hostname = updateInfo.Hostname.GetNew()
	}
	if updateInfo.State.IsDifferent() {
		dbItem.State = updateInfo.State.GetNew()
	}
	if updateInfo.HType.IsDifferent() {
		dbItem.HType = updateInfo.HType.GetNew()
	}
	if updateInfo.LaunchServer.IsDifferent() {
		dbItem.LaunchServer = updateInfo.LaunchServer.GetNew()
	}
	if updateInfo.HostID.IsDifferent() {
		dbItem.HostID = updateInfo.HostID.GetNew()
	}
	if updateInfo.RegionLcuuid.IsDifferent() {
		dbItem.Region = updateInfo.RegionLcuuid.GetNew()
	}
	if updateInfo.AZLcuuid.IsDifferent() {
		dbItem.AZ = updateInfo.AZLcuuid.GetNew()
	}
	if updateInfo.CloudTags.IsDifferent() {
		dbItem.CloudTags = updateInfo.CloudTags.GetNew()
	}
}
