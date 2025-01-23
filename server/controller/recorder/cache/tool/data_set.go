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

package tool

import (
	"errors"
	"fmt"
	"strings"

	cloudmodel "github.com/deepflowio/deepflow/server/controller/cloud/model"
	"github.com/deepflowio/deepflow/server/controller/common"
	ctrlrcommon "github.com/deepflowio/deepflow/server/controller/common"
	metadbmodel "github.com/deepflowio/deepflow/server/controller/db/metadb/model"
	rcommon "github.com/deepflowio/deepflow/server/controller/recorder/common"
)

// 各类资源的映射关系，用于刷新资源时，转换所需数据
type DataSet struct {
	metadata *rcommon.Metadata

	LogController

	// 仅资源变更事件所需的数据
	EventDataSet

	// process device data
	containerIDToPodID map[string]int

	azLcuuidToID map[string]int

	regionLcuuidToID map[string]int
	regionIDToLcuuid map[int]string

	hostLcuuidToID map[string]int
	hostIDToLcuuid map[int]string

	vmLcuuidToID map[string]int
	vmIDToLcuuid map[int]string

	vpcLcuuidToID map[string]int
	vpcIDToLcuuid map[int]string

	publicNetworkID   int
	networkLcuuidToID map[string]int
	networkIDToLcuuid map[int]string

	subnetLcuuidToID map[string]int
	subnetIDToLcuuid map[int]string

	vrouterLcuuidToID map[string]int
	vrouterIDToLcuuid map[int]string

	dhcpPortLcuuidToID map[string]int
	dhcpPortIDToLcuuid map[int]string

	vinterfaceLcuuidToID         map[string]int
	vinterfaceLcuuidToType       map[string]int
	vinterfaceLcuuidToIndex      map[string]int
	vinterfaceLcuuidToNetworkID  map[string]int
	vinterfaceLcuuidToDeviceType map[string]int
	vinterfaceLcuuidToDeviceID   map[string]int
	vinterfaceLcuuidToMac        map[string]string

	natGatewayLcuuidToID map[string]int
	natGatewayIDToLcuuid map[int]string

	lbLcuuidToID map[string]int
	lbIDToLcuuid map[int]string

	lbListenerLcuuidToID map[string]int

	rdsInstanceLcuuidToID map[string]int
	rdsInstanceIDToLcuuid map[int]string

	redisInstanceLcuuidToID map[string]int
	redisInstanceIDToLcuuid map[int]string

	podClusterLcuuidToID map[string]int

	podNodeLcuuidToID map[string]int
	podNodeIDToLcuuid map[int]string

	podNamespaceLcuuidToID map[string]int

	podIngressLcuuidToID     map[string]int
	podIngressIDToLcuuid     map[int]string
	podIngressRuleLcuuidToID map[string]int

	podServiceLcuuidToID map[string]int
	podServiceIDToLcuuid map[int]string

	podGroupLcuuidToID map[string]int
	podGroupIDToLcuuid map[int]string
	podGroupIDToType   map[int]int

	podReplicaSetLcuuidToID map[string]int
	podReplicaSetIDToLcuuid map[int]string

	podLcuuidToID map[string]int
	podIDToLcuuid map[int]string

	vtapIDToType           map[int]int
	vtapIDToLaunchServerID map[int]int
}

func NewDataSet(md *rcommon.Metadata) *DataSet {
	return &DataSet{
		metadata: md,

		EventDataSet:       NewEventDataSet(),
		containerIDToPodID: make(map[string]int),

		azLcuuidToID: make(map[string]int),

		regionLcuuidToID: make(map[string]int),
		regionIDToLcuuid: make(map[int]string),

		hostLcuuidToID: make(map[string]int),
		hostIDToLcuuid: make(map[int]string),

		vmLcuuidToID: make(map[string]int),
		vmIDToLcuuid: make(map[int]string),

		vpcLcuuidToID: make(map[string]int),
		vpcIDToLcuuid: make(map[int]string),

		networkLcuuidToID: make(map[string]int),
		networkIDToLcuuid: make(map[int]string),

		subnetLcuuidToID: make(map[string]int),
		subnetIDToLcuuid: make(map[int]string),

		vrouterLcuuidToID: make(map[string]int),
		vrouterIDToLcuuid: make(map[int]string),

		dhcpPortLcuuidToID: make(map[string]int),
		dhcpPortIDToLcuuid: make(map[int]string),

		vinterfaceLcuuidToID:         make(map[string]int),
		vinterfaceLcuuidToType:       make(map[string]int),
		vinterfaceLcuuidToIndex:      make(map[string]int),
		vinterfaceLcuuidToNetworkID:  make(map[string]int),
		vinterfaceLcuuidToDeviceType: make(map[string]int),
		vinterfaceLcuuidToDeviceID:   make(map[string]int),
		vinterfaceLcuuidToMac:        make(map[string]string),

		natGatewayLcuuidToID: make(map[string]int),
		natGatewayIDToLcuuid: make(map[int]string),

		lbLcuuidToID: make(map[string]int),
		lbIDToLcuuid: make(map[int]string),

		lbListenerLcuuidToID: make(map[string]int),

		rdsInstanceLcuuidToID: make(map[string]int),
		rdsInstanceIDToLcuuid: make(map[int]string),

		redisInstanceLcuuidToID: make(map[string]int),
		redisInstanceIDToLcuuid: make(map[int]string),

		podClusterLcuuidToID: make(map[string]int),

		podNodeLcuuidToID: make(map[string]int),
		podNodeIDToLcuuid: make(map[int]string),

		podNamespaceLcuuidToID: make(map[string]int),

		podIngressLcuuidToID:     make(map[string]int),
		podIngressIDToLcuuid:     make(map[int]string),
		podIngressRuleLcuuidToID: make(map[string]int),

		podServiceLcuuidToID: make(map[string]int),
		podServiceIDToLcuuid: make(map[int]string),

		podGroupLcuuidToID: make(map[string]int),
		podGroupIDToLcuuid: make(map[int]string),
		podGroupIDToType:   make(map[int]int),

		podReplicaSetLcuuidToID: make(map[string]int),
		podReplicaSetIDToLcuuid: make(map[int]string),

		podLcuuidToID: make(map[string]int),
		podIDToLcuuid: make(map[int]string),

		vtapIDToType:           make(map[int]int),
		vtapIDToLaunchServerID: make(map[int]int),
	}
}

func (t *DataSet) GetMetadata() *rcommon.Metadata {
	return t.metadata
}

func (t *DataSet) AddAZ(item *metadbmodel.AZ) {
	t.azLcuuidToID[item.Lcuuid] = item.ID
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_AZ_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteAZ(lcuuid string) {
	delete(t.azLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_AZ_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) GetAZIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.azLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_AZ_EN, lcuuid), t.metadata.LogPrefixes)
	var az metadbmodel.AZ
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&az)
	if result.RowsAffected == 1 {
		t.AddAZ(&az)
		return az.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_AZ_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) AddRegion(item *metadbmodel.Region) {
	t.regionLcuuidToID[item.Lcuuid] = item.ID
	t.regionIDToLcuuid[item.ID] = item.Lcuuid
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_REGION_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteRegion(lcuuid string) {
	delete(t.regionLcuuidToID, lcuuid)
	id, _ := t.GetRegionIDByLcuuid(lcuuid)
	delete(t.regionIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_REGION_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddHost(item *metadbmodel.Host) {
	t.hostLcuuidToID[item.Lcuuid] = item.ID
	t.hostIDToLcuuid[item.ID] = item.Lcuuid
	t.hostIPToID[item.IP] = item.ID
	t.hostIDtoInfo[item.ID] = &hostInfo{Name: item.Name}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.hostIDtoInfo[item.ID].RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(item.AZ); ok {
		t.hostIDtoInfo[item.ID].AZID = azID
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_HOST_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteHost(lcuuid string) {
	id, _ := t.GetHostIDByLcuuid(lcuuid)
	delete(t.hostIDtoInfo, id)
	delete(t.hostLcuuidToID, lcuuid)
	delete(t.hostIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_HOST_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateHost(cloudItem *cloudmodel.Host) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_HOST_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetHostIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetHostInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		t.hostIDtoInfo[id].RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(cloudItem.AZLcuuid); ok {
		t.hostIDtoInfo[id].RegionID = azID
	}
}

func (t *DataSet) AddVM(item *metadbmodel.VM) {
	t.vmLcuuidToID[item.Lcuuid] = item.ID
	t.vmIDToLcuuid[item.ID] = item.Lcuuid
	t.vmIDToInfo[item.ID] = &vmInfo{
		Name:      item.Name,
		VPCID:     item.VPCID,
		NetworkID: item.NetworkID,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.vmIDToInfo[item.ID].RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(item.AZ); ok {
		t.vmIDToInfo[item.ID].AZID = azID
	}
	if item.LaunchServer != "" {
		if hostID, ok := t.GetHostIDByIP(item.LaunchServer); ok {
			t.vmIDToInfo[item.ID].HostID = hostID
		}
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_VM_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateVM(cloudItem *cloudmodel.VM) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_VM_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetVMIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetVMInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(cloudItem.AZLcuuid); ok {
		info.AZID = azID
	}
	if networkID, ok := t.GetNetworkIDByLcuuid(cloudItem.NetworkLcuuid); ok {
		info.NetworkID = networkID
	}
	if vpcID, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = vpcID
	}
	if cloudItem.LaunchServer != "" {
		if hostID, ok := t.GetHostIDByIP(cloudItem.LaunchServer); ok {
			info.HostID = hostID
		}
	}
}

func (t *DataSet) DeleteVM(lcuuid string) {
	id, _ := t.GetVMIDByLcuuid(lcuuid)
	delete(t.vmIDToIPNetworkIDMap, id)
	delete(t.vmIDToLcuuid, id)
	delete(t.vmLcuuidToID, lcuuid)
	delete(t.vmIDToInfo, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_VM_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddVPC(item *metadbmodel.VPC) {
	t.vpcLcuuidToID[item.Lcuuid] = item.ID
	t.vpcIDToLcuuid[item.ID] = item.Lcuuid
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_VPC_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteVPC(lcuuid string) {
	id, _ := t.GetVPCIDByLcuuid(lcuuid)
	delete(t.vpcIDToLcuuid, id)
	delete(t.vpcLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_VPC_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddNetwork(item *metadbmodel.Network) {
	t.networkLcuuidToID[item.Lcuuid] = item.ID
	t.networkIDToLcuuid[item.ID] = item.Lcuuid
	t.networkIDToName[item.ID] = item.Name
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateNetwork(cloudItem *cloudmodel.Network) {
	id, exists := t.GetNetworkIDByLcuuid(cloudItem.Lcuuid)
	if exists {
		t.networkIDToName[id] = cloudItem.Name
	}
	log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteNetwork(lcuuid string) {
	id, _ := t.GetNetworkIDByLcuuid(lcuuid)
	delete(t.networkIDToLcuuid, id)
	delete(t.networkIDToName, id)
	delete(t.networkLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddSubnet(item *metadbmodel.Subnet) {
	t.subnetLcuuidToID[item.Lcuuid] = item.ID
	t.subnetIDToLcuuid[item.ID] = item.Lcuuid
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_SUBNET_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteSubnet(lcuuid string) {
	id, _ := t.GetSubnetIDByLcuuid(lcuuid)
	delete(t.subnetIDToLcuuid, id)
	delete(t.subnetLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_SUBNET_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddVRouter(item *metadbmodel.VRouter) {
	t.vrouterLcuuidToID[item.Lcuuid] = item.ID
	t.vrouterIDToLcuuid[item.ID] = item.Lcuuid
	t.vrouterIDToInfo[item.ID] = &vrouterInfo{
		Name:           item.Name,
		VPCID:          item.VPCID,
		GWLaunchServer: item.GWLaunchServer,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.vrouterIDToInfo[item.ID].RegionID = regionID
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateVRouter(cloudItem *cloudmodel.VRouter) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetVRouterIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetVRouterInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	info.GWLaunchServer = cloudItem.GWLaunchServer
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if vpcID, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = vpcID
	}
}

func (t *DataSet) DeleteVRouter(lcuuid string) {
	id, _ := t.GetVRouterIDByLcuuid(lcuuid)
	delete(t.vrouterIDToInfo, id)
	delete(t.vrouterLcuuidToID, lcuuid)
	delete(t.vrouterIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddDHCPPort(item *metadbmodel.DHCPPort) {
	t.dhcpPortLcuuidToID[item.Lcuuid] = item.ID
	t.dhcpPortIDToLcuuid[item.ID] = item.Lcuuid
	t.dhcpPortIDToInfo[item.ID] = &dhcpPortInfo{
		Name:  item.Name,
		VPCID: item.VPCID,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.dhcpPortIDToInfo[item.ID].RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(item.AZ); ok {
		t.dhcpPortIDToInfo[item.ID].AZID = azID
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateDHCPPort(cloudItem *cloudmodel.DHCPPort) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetDHCPPortIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetDHCPPortInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(cloudItem.AZLcuuid); ok {
		info.AZID = azID
	}
	if vpcID, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = vpcID
	}
}

func (t *DataSet) DeleteDHCPPort(lcuuid string) {
	id, _ := t.GetDHCPPortIDByLcuuid(lcuuid)
	delete(t.dhcpPortIDToInfo, id)
	delete(t.dhcpPortLcuuidToID, lcuuid)
	delete(t.dhcpPortIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddVInterface(item *metadbmodel.VInterface) {
	t.vinterfaceLcuuidToID[item.Lcuuid] = item.ID
	t.vinterfaceIDToLcuuid[item.ID] = item.Lcuuid
	t.vinterfaceLcuuidToNetworkID[item.Lcuuid] = item.NetworkID
	t.vinterfaceLcuuidToDeviceType[item.Lcuuid] = item.DeviceType
	t.vinterfaceLcuuidToDeviceID[item.Lcuuid] = item.DeviceID
	t.vinterfaceLcuuidToMac[item.Lcuuid] = item.Mac
	t.vinterfaceLcuuidToIndex[item.Lcuuid] = item.Index
	t.vinterfaceLcuuidToType[item.Lcuuid] = item.Type

	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateVInterface(cloudItem *cloudmodel.VInterface) {
	t.vinterfaceLcuuidToType[cloudItem.Lcuuid] = cloudItem.Type
	log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteVInterface(lcuuid string) {
	id, _ := t.vinterfaceLcuuidToID[lcuuid]
	delete(t.vinterfaceIDToLcuuid, id)
	delete(t.vinterfaceLcuuidToID, lcuuid)
	delete(t.vinterfaceLcuuidToNetworkID, lcuuid)
	delete(t.vinterfaceLcuuidToDeviceType, lcuuid)
	delete(t.vinterfaceLcuuidToDeviceID, lcuuid)
	delete(t.vinterfaceLcuuidToMac, lcuuid)
	delete(t.vinterfaceLcuuidToIndex, lcuuid)
	delete(t.vinterfaceLcuuidToType, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddWANIP(item *metadbmodel.WANIP) {
	t.wanIPLcuuidToVInterfaceID[item.Lcuuid] = item.VInterfaceID
	t.wanIPLcuuidToIP[item.Lcuuid] = item.IP
	if vifLcuuid, _ := t.GetVInterfaceLcuuidByID(item.VInterfaceID); vifLcuuid != "" {
		deviceType, _ := t.GetDeviceTypeByVInterfaceLcuuid(vifLcuuid)
		deviceID, _ := t.GetDeviceIDByVInterfaceLcuuid(vifLcuuid)
		mac, _ := t.GetMacByVInterfaceLcuuid(vifLcuuid)
		networkID, _ := t.GetNetworkIDByVInterfaceLcuuid(vifLcuuid)
		t.setDeviceToIPNetworkMap(deviceType, deviceID, networkID, IPKey{IP: item.IP, Mac: mac, Lcuuid: item.Lcuuid})
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_WAN_IP_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteWANIP(lcuuid string) {
	if vifID, ok := t.GetVInterfaceIDByWANIPLcuuid(lcuuid); ok {
		if vifLcuuid, _ := t.GetVInterfaceLcuuidByID(vifID); vifLcuuid != "" {
			deviceType, _ := t.GetDeviceTypeByVInterfaceLcuuid(vifLcuuid)
			deviceID, _ := t.GetDeviceIDByVInterfaceLcuuid(vifLcuuid)
			mac, _ := t.GetMacByVInterfaceLcuuid(vifLcuuid)
			networkID, _ := t.GetNetworkIDByVInterfaceLcuuid(vifLcuuid)
			ip, _ := t.GetWANIPByLcuuid(lcuuid)
			t.DeleteDeviceToIPNetworkMapIP(deviceType, deviceID, networkID, IPKey{IP: ip, Mac: mac, Lcuuid: lcuuid})
		}
	}
	delete(t.wanIPLcuuidToVInterfaceID, lcuuid)
	delete(t.wanIPLcuuidToIP, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_WAN_IP_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddLANIP(item *metadbmodel.LANIP) {
	t.lanIPLcuuidToVInterfaceID[item.Lcuuid] = item.VInterfaceID
	t.lanIPLcuuidToIP[item.Lcuuid] = item.IP
	if vifLcuuid, _ := t.GetVInterfaceLcuuidByID(item.VInterfaceID); vifLcuuid != "" {
		deviceType, _ := t.GetDeviceTypeByVInterfaceLcuuid(vifLcuuid)
		mac, _ := t.GetMacByVInterfaceLcuuid(vifLcuuid)
		deviceID, _ := t.GetDeviceIDByVInterfaceLcuuid(vifLcuuid)
		networkID, _ := t.GetNetworkIDByVInterfaceLcuuid(vifLcuuid)
		t.setDeviceToIPNetworkMap(deviceType, deviceID, networkID, IPKey{IP: item.IP, Mac: mac, Lcuuid: item.Lcuuid})
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_LAN_IP_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteLANIP(lcuuid string) {
	if vifID, ok := t.GetVInterfaceIDByLANIPLcuuid(lcuuid); ok {
		if vifLcuuid, _ := t.GetVInterfaceLcuuidByID(vifID); vifLcuuid != "" {
			deviceType, _ := t.GetDeviceTypeByVInterfaceLcuuid(vifLcuuid)
			deviceID, _ := t.GetDeviceIDByVInterfaceLcuuid(vifLcuuid)
			mac, _ := t.GetMacByVInterfaceLcuuid(vifLcuuid)
			networkID, _ := t.GetNetworkIDByVInterfaceLcuuid(vifLcuuid)
			ip, _ := t.GetLANIPByLcuuid(lcuuid)
			t.DeleteDeviceToIPNetworkMapIP(deviceType, deviceID, networkID, IPKey{IP: ip, Mac: mac, Lcuuid: lcuuid})
		}
	}
	delete(t.lanIPLcuuidToVInterfaceID, lcuuid)
	delete(t.lanIPLcuuidToIP, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_LAN_IP_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddNATGateway(item *metadbmodel.NATGateway) {
	t.natGatewayLcuuidToID[item.Lcuuid] = item.ID
	t.natGatewayIDToLcuuid[item.ID] = item.Lcuuid
	t.natGatewayIDToInfo[item.ID] = &natGatewayInfo{
		Name:  item.Name,
		VPCID: item.VPCID,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.natGatewayIDToInfo[item.ID].RegionID = regionID
	}
	if azID, ok := t.azLcuuidToID[item.AZ]; ok {
		t.natGatewayIDToInfo[item.ID].AZID = azID
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateNATGateway(cloudItem *cloudmodel.NATGateway) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetNATGatewayIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetNATGatewayInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if vpcID, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = vpcID
	}
}

func (t *DataSet) DeleteNATGateway(lcuuid string) {
	id, _ := t.GetNATGatewayIDByLcuuid(lcuuid)
	delete(t.natGatewayIDToInfo, id)
	delete(t.natGatewayLcuuidToID, lcuuid)
	delete(t.natGatewayIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddLB(item *metadbmodel.LB) {
	t.lbLcuuidToID[item.Lcuuid] = item.ID
	t.lbIDToLcuuid[item.ID] = item.Lcuuid
	t.lbIDToInfo[item.ID] = &lbInfo{
		Name:  item.Name,
		VPCID: item.VPCID,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.lbIDToInfo[item.ID].RegionID = regionID
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_LB_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateLB(cloudItem *cloudmodel.LB) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_LB_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetLBIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetLBInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if vpcID, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = vpcID
	}
}

func (t *DataSet) DeleteLB(lcuuid string) {
	id, _ := t.GetLBIDByLcuuid(lcuuid)
	delete(t.lbIDToInfo, id)
	delete(t.lbLcuuidToID, lcuuid)
	delete(t.lbIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_LB_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddLBListener(item *metadbmodel.LBListener) {
	t.lbListenerLcuuidToID[item.Lcuuid] = item.ID
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_LB_LISTENER_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteLBListener(lcuuid string) {
	delete(t.lbListenerLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_LB_LISTENER_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddRDSInstance(item *metadbmodel.RDSInstance) {
	t.rdsInstanceLcuuidToID[item.Lcuuid] = item.ID
	t.rdsInstanceIDToLcuuid[item.ID] = item.Lcuuid
	t.rdsInstanceIDToInfo[item.ID] = &rdsInstanceInfo{
		Name:  item.Name,
		VPCID: item.VPCID,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.rdsInstanceIDToInfo[item.ID].RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(item.AZ); ok {
		t.rdsInstanceIDToInfo[item.ID].AZID = azID
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateRDSInstance(cloudItem *cloudmodel.RDSInstance) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetRDSInstanceIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetRDSInstanceInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(cloudItem.AZLcuuid); ok {
		info.AZID = azID
	}
	if vpcID, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = vpcID
	}
}

func (t *DataSet) DeleteRDSInstance(lcuuid string) {
	id, _ := t.GetRDSInstanceIDByLcuuid(lcuuid)
	delete(t.rdsInstanceIDToInfo, id)
	delete(t.rdsInstanceLcuuidToID, lcuuid)
	delete(t.rdsInstanceIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddRedisInstance(item *metadbmodel.RedisInstance) {
	t.redisInstanceLcuuidToID[item.Lcuuid] = item.ID
	t.redisInstanceIDToLcuuid[item.ID] = item.Lcuuid
	t.redisInstanceIDToInfo[item.ID] = &redisInstanceInfo{
		Name:  item.Name,
		VPCID: item.VPCID,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.redisInstanceIDToInfo[item.ID].RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(item.AZ); ok {
		t.redisInstanceIDToInfo[item.ID].AZID = azID
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdateRedisInstance(cloudItem *cloudmodel.RedisInstance) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetRedisInstanceIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetRedisInstanceInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(cloudItem.AZLcuuid); ok {
		info.AZID = azID
	}
	if vpcID, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = vpcID
	}
}

func (t *DataSet) DeleteRedisInstance(lcuuid string) {
	id, _ := t.GetRedisInstanceIDByLcuuid(lcuuid)
	delete(t.redisInstanceIDToInfo, id)
	delete(t.redisInstanceLcuuidToID, lcuuid)
	delete(t.redisInstanceIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddPodCluster(item *metadbmodel.PodCluster) {
	t.podClusterLcuuidToID[item.Lcuuid] = item.ID
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_POD_CLUSTER_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeletePodCluster(lcuuid string) {
	delete(t.podClusterLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_POD_CLUSTER_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddPodNode(item *metadbmodel.PodNode) {
	t.podNodeLcuuidToID[item.Lcuuid] = item.ID
	t.podNodeIDToLcuuid[item.ID] = item.Lcuuid
	t.podNodeIDToInfo[item.ID] = &podNodeInfo{
		DomainLcuuid: item.Domain,
		Name:         item.Name,
		VPCID:        item.VPCID,
		PodClusterID: item.PodClusterID,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.podNodeIDToInfo[item.ID].RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(item.AZ); ok {
		t.podNodeIDToInfo[item.ID].AZID = azID
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdatePodNode(cloudItem *cloudmodel.PodNode) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id := t.GetPodNodeIDByLcuuid(cloudItem.Lcuuid)
	if id == 0 {
		return
	}
	info, err := t.GetPodNodeInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(cloudItem.AZLcuuid); ok {
		info.AZID = azID
	}
	if vpcID, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = vpcID
	}
	if podClusterLcuuid, ok := t.GetPodClusterIDByLcuuid(cloudItem.PodClusterLcuuid); ok {
		info.PodClusterID = podClusterLcuuid
	}
}

func (t *DataSet) DeletePodNode(lcuuid string) {
	id := t.GetPodNodeIDByLcuuid(lcuuid)
	if id == 0 {
		return
	}
	delete(t.podNodeIDToLcuuid, id)
	delete(t.podNodeIDToInfo, id)
	delete(t.podNodeLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddVMPodNodeConnection(item *metadbmodel.VMPodNodeConnection) {
	t.vmPodNodeConnectionLcuuidToPodNodeID[item.Lcuuid] = item.PodNodeID
	t.podNodeIDToVMID[item.PodNodeID] = item.VMID
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_VM_POD_NODE_CONNECTION_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteVMPodNodeConnection(lcuuid string) {
	podNodeID, _ := t.GetPodNodeIDByVMPodNodeConnectionLcuuid(lcuuid)
	delete(t.podNodeIDToVMID, podNodeID)
	delete(t.vmPodNodeConnectionLcuuidToPodNodeID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_VM_POD_NODE_CONNECTION_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddPodNamespace(item *metadbmodel.PodNamespace) {
	t.podNamespaceLcuuidToID[item.Lcuuid] = item.ID
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_POD_NAMESPACE_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeletePodNamespace(lcuuid string) {
	delete(t.podNamespaceLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_POD_NAMESPACE_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddPodIngress(item *metadbmodel.PodIngress) {
	t.podIngressLcuuidToID[item.Lcuuid] = item.ID
	t.podIngressIDToLcuuid[item.ID] = item.Lcuuid
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeletePodIngress(lcuuid string) {
	id, _ := t.GetPodIngressIDByLcuuid(lcuuid)
	delete(t.podIngressIDToLcuuid, id)
	delete(t.podIngressLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddPodIngressRule(item *metadbmodel.PodIngressRule) {
	t.podIngressRuleLcuuidToID[item.Lcuuid] = item.ID
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_RULE_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeletePodIngressRule(lcuuid string) {
	delete(t.podIngressRuleLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_RULE_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddPodService(item *metadbmodel.PodService) {
	t.podServiceLcuuidToID[item.Lcuuid] = item.ID
	t.podServiceIDToLcuuid[item.ID] = item.Lcuuid
	t.podServiceIDToInfo[item.ID] = &podServiceInfo{
		Name:           item.Name,
		VPCID:          item.VPCID,
		PodClusterID:   item.PodClusterID,
		PodNamespaceID: item.PodNamespaceID,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.podServiceIDToInfo[item.ID].RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(item.AZ); ok {
		t.podServiceIDToInfo[item.ID].AZID = azID
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdatePodService(cloudItem *cloudmodel.PodService) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetPodServiceIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetPodServiceInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(cloudItem.AZLcuuid); ok {
		info.AZID = azID
	}
	if vpcID, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = vpcID
	}
	if podClusterLcuuid, ok := t.GetPodClusterIDByLcuuid(cloudItem.PodClusterLcuuid); ok {
		info.PodClusterID = podClusterLcuuid
	}
	if podNSID, ok := t.GetPodNamespaceIDByLcuuid(cloudItem.PodNamespaceLcuuid); ok {
		info.PodNamespaceID = podNSID
	}
}

func (t *DataSet) DeletePodService(lcuuid string) {
	id, _ := t.GetPodServiceIDByLcuuid(lcuuid)
	delete(t.podServiceIDToInfo, id)
	delete(t.podServiceLcuuidToID, lcuuid)
	delete(t.podServiceIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddPodGroup(item *metadbmodel.PodGroup) {
	t.podGroupLcuuidToID[item.Lcuuid] = item.ID
	t.podGroupIDToLcuuid[item.ID] = item.Lcuuid
	t.podGroupIDToType[item.ID] = item.Type
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeletePodGroup(lcuuid string) {
	id, _ := t.podGroupLcuuidToID[lcuuid]
	delete(t.podGroupIDToLcuuid, id)
	delete(t.podGroupIDToType, id)
	delete(t.podGroupLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) AddPodReplicaSet(item *metadbmodel.PodReplicaSet) {
	t.podReplicaSetLcuuidToID[item.Lcuuid] = item.ID
	t.podReplicaSetIDToLcuuid[item.ID] = item.Lcuuid
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_POD_REPLICA_SET_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeletePodReplicaSet(lcuuid string) {
	id, exists := t.GetPodReplicaSetIDByLcuuid(lcuuid)
	if exists {
		delete(t.podReplicaSetIDToLcuuid, id)
	}
	delete(t.podReplicaSetLcuuidToID, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_POD_REPLICA_SET_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) updateContainerIDToPodID(containerID string, podID int) {
	var containerIDs []string
	if len(containerID) > 0 {
		containerIDs = strings.Split(containerID, ", ")
	}
	for _, id := range containerIDs {
		t.containerIDToPodID[id] = podID
	}
}

func (t *DataSet) AddPod(item *metadbmodel.Pod) {
	t.podLcuuidToID[item.Lcuuid] = item.ID
	t.podIDToLcuuid[item.ID] = item.Lcuuid
	t.podIDToInfo[item.ID] = &podInfo{
		DomainLcuuid:   item.Domain,
		Name:           item.Name,
		VPCID:          item.VPCID,
		PodClusterID:   item.PodClusterID,
		PodNamespaceID: item.PodNamespaceID,
		PodGroupID:     item.PodGroupID,
		PodNodeID:      item.PodNodeID,
	}
	if regionID, ok := t.GetRegionIDByLcuuid(item.Region); ok {
		t.podIDToInfo[item.ID].RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(item.AZ); ok {
		t.podIDToInfo[item.ID].AZID = azID
	}
	t.updateContainerIDToPodID(item.ContainerIDs, item.ID)

	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_POD_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) UpdatePod(cloudItem *cloudmodel.Pod) {
	defer log.Info(updateToolMap(ctrlrcommon.RESOURCE_TYPE_POD_EN, cloudItem.Lcuuid), t.metadata.LogPrefixes)
	id, _ := t.GetPodIDByLcuuid(cloudItem.Lcuuid)
	info, err := t.GetPodInfoByID(id)
	if err != nil {
		log.Error(err, t.metadata.LogPrefixes)
		return
	}
	info.Name = cloudItem.Name
	info.PodNodeID = t.GetPodNodeIDByLcuuid(cloudItem.PodNodeLcuuid)
	if regionID, ok := t.GetRegionIDByLcuuid(cloudItem.RegionLcuuid); ok {
		info.RegionID = regionID
	}
	if azID, ok := t.GetAZIDByLcuuid(cloudItem.AZLcuuid); ok {
		info.AZID = azID
	}
	if id, ok := t.vpcLcuuidToID[cloudItem.Lcuuid]; ok {
		info.VPCID = id
	}
	if id, ok := t.GetPodClusterIDByLcuuid(cloudItem.PodClusterLcuuid); ok {
		info.PodClusterID = id
	}
	if id, ok := t.GetPodNamespaceIDByLcuuid(cloudItem.PodNamespaceLcuuid); ok {
		info.PodNamespaceID = id
	}
	if id, ok := t.GetPodGroupIDByLcuuid(cloudItem.PodGroupLcuuid); ok {
		info.PodGroupID = id
	}

	t.updateContainerIDToPodID(cloudItem.ContainerIDs, id)
}

func (t *DataSet) DeletePod(lcuuid string) {
	id, _ := t.GetPodIDByLcuuid(lcuuid)
	delete(t.podIDToInfo, id)
	delete(t.podIDToIPNetworkIDMap, id)
	delete(t.podLcuuidToID, lcuuid)
	delete(t.podIDToLcuuid, id)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_POD_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) RefreshVTaps(v []*metadbmodel.VTap) {
	t.vtapIDToType = make(map[int]int)
	t.vtapIDToLaunchServerID = make(map[int]int)
	for _, item := range v {
		t.vtapIDToType[item.ID] = item.Type
		t.vtapIDToLaunchServerID[item.ID] = item.LaunchServerID
	}
	log.Infof("refreshed %s count: %d", ctrlrcommon.RESOURCE_TYPE_VTAP_EN, len(v))
}

func (t *DataSet) GetRegionIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.regionLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_REGION_EN, lcuuid), t.metadata.LogPrefixes)
	var region metadbmodel.Region
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&region)
	if result.RowsAffected == 1 {
		t.AddRegion(&region)
		return region.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_REGION_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetRegionLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.regionIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_REGION_EN, id), t.metadata.LogPrefixes)
	var region metadbmodel.Region
	result := t.metadata.DB.Where("id = ?", id).Find(&region)
	if result.RowsAffected == 1 {
		t.AddRegion(&region)
		return region.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_REGION_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetHostIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.hostLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_HOST_EN, lcuuid), t.metadata.LogPrefixes)
	var dbItem metadbmodel.Host
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddHost(&dbItem)
		return dbItem.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_HOST_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetHostLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.hostIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_HOST_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.Host
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddHost(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_HOST_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetHostIDByIP(ip string) (int, bool) {
	id, exists := t.hostIPToID[ip]
	if exists {
		return id, true
	}
	log.Warningf("cache %s id (ip: %s) not found", ctrlrcommon.RESOURCE_TYPE_HOST_EN, ip, t.metadata.LogPrefixes)
	var host metadbmodel.Host
	result := t.metadata.DB.Where("ip = ?", ip).Find(&host)
	if result.RowsAffected == 1 {
		t.AddHost(&host)
		return host.ID, true
	} else {
		log.Errorf("db %s (ip: %s) not found", ctrlrcommon.RESOURCE_TYPE_HOST_EN, ip, t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetVMIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.vmLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VM_EN, lcuuid), t.metadata.LogPrefixes)
	var dbItem metadbmodel.VM
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddVM(&dbItem)
		return dbItem.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VM_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetVMLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.vmIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VM_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.VM
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddVM(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VM_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetVPCIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.vpcLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VPC_EN, lcuuid), t.metadata.LogPrefixes)
	var vpc metadbmodel.VPC
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&vpc)
	if result.RowsAffected == 1 {
		t.AddVPC(&vpc)
		return vpc.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VPC_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetVPCLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.vpcIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VPC_EN, id), t.metadata.LogPrefixes)
	var vpc metadbmodel.VPC
	result := t.metadata.DB.Where("id = ?", id).Find(&vpc)
	if result.RowsAffected == 1 {
		t.AddVPC(&vpc)
		return vpc.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VPC_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetNetworkIDByLcuuid(lcuuid string) (int, bool) {
	if lcuuid == rcommon.PUBLIC_NETWORK_LCUUID {
		return t.publicNetworkID, true
	}
	id, exists := t.networkLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, lcuuid), t.metadata.LogPrefixes)
	var network metadbmodel.Network
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&network)
	if result.RowsAffected == 1 {
		t.AddNetwork(&network)
		return network.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetSubnetIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.subnetLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_SUBNET_EN, lcuuid), t.metadata.LogPrefixes)
	var subnet metadbmodel.Subnet
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&subnet)
	if result.RowsAffected == 1 {
		t.AddSubnet(&subnet)
		return subnet.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_SUBNET_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetSubnetLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.subnetIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_SUBNET_EN, id), t.metadata.LogPrefixes)
	var subnet metadbmodel.Subnet
	result := t.metadata.DB.Where("id = ?", id).Find(&subnet)
	if result.RowsAffected == 1 {
		t.AddSubnet(&subnet)
		return subnet.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_SUBNET_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetNetworkIDByVInterfaceLcuuid(vifLcuuid string) (int, bool) {
	id, exists := t.vinterfaceLcuuidToNetworkID[vifLcuuid]
	if exists {
		return id, true
	}
	log.Warningf("cache %s id (%s lcuuid: %s) not found", ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, vifLcuuid, t.metadata.LogPrefixes)
	var vif metadbmodel.VInterface
	result := t.metadata.DB.Where("lcuuid = ?", vifLcuuid).Find(&vif)
	if result.RowsAffected == 1 {
		t.AddVInterface(&vif)
		return vif.NetworkID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, vifLcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetDeviceTypeByVInterfaceLcuuid(vifLcuuid string) (int, bool) {
	id, exists := t.vinterfaceLcuuidToDeviceType[vifLcuuid]
	if exists {
		return id, true
	}
	log.Warningf("cache device type (%s lcuuid: %s) not found", ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, vifLcuuid, t.metadata.LogPrefixes)
	var vif metadbmodel.VInterface
	result := t.metadata.DB.Where("lcuuid = ?", vifLcuuid).Find(&vif)
	if result.RowsAffected == 1 {
		t.AddVInterface(&vif)
		return vif.DeviceType, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, vifLcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetDeviceIDByVInterfaceLcuuid(vifLcuuid string) (int, bool) {
	id, exists := t.vinterfaceLcuuidToDeviceID[vifLcuuid]
	if exists {
		return id, true
	}
	log.Warningf("cache device id (%s lcuuid: %s) not found", ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, vifLcuuid, t.metadata.LogPrefixes)
	var vif metadbmodel.VInterface
	result := t.metadata.DB.Where("lcuuid = ?", vifLcuuid).Find(&vif)
	if result.RowsAffected == 1 {
		t.AddVInterface(&vif)
		return vif.DeviceID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, vifLcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetMacByVInterfaceLcuuid(vifLcuuid string) (string, bool) {
	mac, exists := t.vinterfaceLcuuidToMac[vifLcuuid]
	if exists {
		return mac, true
	}
	log.Warningf("cache mac (%s lcuuid: %s) not found", ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, vifLcuuid, t.metadata.LogPrefixes)
	var vif metadbmodel.VInterface
	result := t.metadata.DB.Where("lcuuid = ?", vifLcuuid).Find(&vif)
	if result.RowsAffected == 1 {
		t.AddVInterface(&vif)
		return vif.Mac, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, vifLcuuid), t.metadata.LogPrefixes)
		return "", false
	}
}

func (t *DataSet) GetNetworkLcuuidByID(id int) (string, bool) {
	if id == t.publicNetworkID {
		return rcommon.PUBLIC_NETWORK_LCUUID, true
	}
	lcuuid, exists := t.networkIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, id), t.metadata.LogPrefixes)
	var network metadbmodel.Network
	result := t.metadata.DB.Where("id = ?", id).Find(&network)
	if result.RowsAffected == 1 {
		t.AddNetwork(&network)
		return network.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetVRouterIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.vrouterLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, lcuuid), t.metadata.LogPrefixes)
	var dbItem metadbmodel.VRouter
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddVRouter(&dbItem)
		return dbItem.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetVRouterLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.vrouterIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.VRouter
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddVRouter(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetDHCPPortIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.dhcpPortLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, lcuuid), t.metadata.LogPrefixes)
	var dbItem metadbmodel.DHCPPort
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddDHCPPort(&dbItem)
		return dbItem.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetDHCPPortLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.dhcpPortIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.DHCPPort
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddDHCPPort(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetVInterfaceIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.vinterfaceLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, lcuuid), t.metadata.LogPrefixes)
	var vinterface metadbmodel.VInterface
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&vinterface)
	if result.RowsAffected == 1 {
		t.AddVInterface(&vinterface)
		return vinterface.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetVInterfaceTypeByLcuuid(lcuuid string) (int, bool) {
	vt, exists := t.vinterfaceLcuuidToType[lcuuid]
	if exists {
		return vt, true
	}
	log.Warningf("cache %s type (lcuuid: %s) not found", ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, lcuuid, t.metadata.LogPrefixes)
	var vinterface metadbmodel.VInterface
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&vinterface)
	if result.RowsAffected == 1 {
		t.AddVInterface(&vinterface)
		return vinterface.Type, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, lcuuid), t.metadata.LogPrefixes)
		return vt, false
	}
}

func (t *DataSet) GetDeviceLcuuidByID(deviceType, deviceID int) (string, bool) {
	if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_HOST {
		return t.GetHostLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_VM {
		return t.GetVMLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_VROUTER {
		return t.GetVRouterLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_DHCP_PORT {
		return t.GetDHCPPortLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_NAT_GATEWAY {
		return t.GetNATGatewayLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_LB {
		return t.GetLBLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_RDS_INSTANCE {
		return t.GetRDSInstanceLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_REDIS_INSTANCE {
		return t.GetRedisInstanceLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_POD_NODE {
		return t.GetPodNodeLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_POD_SERVICE {
		return t.GetPodServiceLcuuidByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_POD {
		return t.GetPodLcuuidByID(deviceID)
	} else {
		log.Errorf("device type %d not supported", deviceType, t.metadata.LogPrefixes)
		return "", false
	}
}

func (t *DataSet) GetDeviceIDByDeviceLcuuid(deviceType int, deviceLcuuid string) (int, bool) {
	if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_HOST {
		return t.GetHostIDByLcuuid(deviceLcuuid)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_VM {
		return t.GetVMIDByLcuuid(deviceLcuuid)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_VROUTER {
		return t.GetVRouterIDByLcuuid(deviceLcuuid)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_DHCP_PORT {
		return t.GetDHCPPortIDByLcuuid(deviceLcuuid)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_NAT_GATEWAY {
		return t.GetNATGatewayIDByLcuuid(deviceLcuuid)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_LB {
		return t.GetLBIDByLcuuid(deviceLcuuid)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_RDS_INSTANCE {
		return t.GetRDSInstanceIDByLcuuid(deviceLcuuid)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_REDIS_INSTANCE {
		return t.GetRedisInstanceIDByLcuuid(deviceLcuuid)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_POD_NODE {
		return t.GetPodNodeIDByLcuuid(deviceLcuuid), true
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_POD_SERVICE {
		return t.GetPodServiceIDByLcuuid(deviceLcuuid)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_POD {
		return t.GetPodIDByLcuuid(deviceLcuuid)
	} else {
		log.Errorf("device type %d not supported", deviceType, t.metadata.LogPrefixes)
		return 0, false
	}
}

func (t *DataSet) GetDeviceNameByDeviceID(deviceType, deviceID int) (string, error) { // TODO 统一风格，使用bool
	if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_HOST {
		return t.GetHostNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_VM {
		return t.GetVMNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_VROUTER {
		return t.GetVRouterNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_DHCP_PORT {
		return t.GetDHCPPortNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_NAT_GATEWAY {
		return t.GetNATGatewayNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_LB {
		return t.GetLBNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_RDS_INSTANCE {
		return t.GetRDSInstanceNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_REDIS_INSTANCE {
		return t.GetRedisInstanceNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_POD_NODE {
		return t.GetPodNodeNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_POD_SERVICE {
		return t.GetPodServiceNameByID(deviceID)
	} else if deviceType == ctrlrcommon.VIF_DEVICE_TYPE_POD {
		return t.GetPodNameByID(deviceID)
	} else {
		return "", fmt.Errorf("device type %d not supported", deviceType, t.metadata.LogPrefixes)
	}
}

func (t *DataSet) GetNATGatewayIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.natGatewayLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, lcuuid), t.metadata.LogPrefixes)
	var dbItem metadbmodel.NATGateway
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddNATGateway(&dbItem)
		return dbItem.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetNATGatewayLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.natGatewayIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.NATGateway
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddNATGateway(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetLBIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.lbLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_LB_EN, lcuuid), t.metadata.LogPrefixes)
	var dbItem metadbmodel.LB
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddLB(&dbItem)
		return dbItem.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_LB_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetLBLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.lbIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_LB_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.LB
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddLB(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_LB_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetLBListenerIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.lbListenerLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_LB_LISTENER_EN, lcuuid), t.metadata.LogPrefixes)
	var lbListener metadbmodel.LBListener
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&lbListener)
	if result.RowsAffected == 1 {
		t.AddLBListener(&lbListener)
		return lbListener.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_LB_LISTENER_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetRDSInstanceIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.rdsInstanceLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, lcuuid), t.metadata.LogPrefixes)
	var rdsInstance metadbmodel.RDSInstance
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&rdsInstance)
	if result.RowsAffected == 1 {
		t.AddRDSInstance(&rdsInstance)
		return rdsInstance.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetRDSInstanceLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.rdsInstanceIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.RDSInstance
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddRDSInstance(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetRedisInstanceIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.redisInstanceLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, lcuuid), t.metadata.LogPrefixes)
	var redisInstance metadbmodel.RedisInstance
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&redisInstance)
	if result.RowsAffected == 1 {
		t.AddRedisInstance(&redisInstance)
		return redisInstance.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetRedisInstanceLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.redisInstanceIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.RedisInstance
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddRedisInstance(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetPodClusterIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.podClusterLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_CLUSTER_EN, lcuuid), t.metadata.LogPrefixes)
	var dbItem metadbmodel.PodCluster
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddPodCluster(&dbItem)
		return dbItem.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_CLUSTER_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetPodNodeIDByLcuuid(lcuuid string) int {
	if lcuuid == "" {
		return 0
	}
	id, exists := t.podNodeLcuuidToID[lcuuid]
	if exists {
		return id
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, lcuuid), t.metadata.LogPrefixes)
	var podNode metadbmodel.PodNode
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&podNode)
	if result.RowsAffected == 1 {
		t.AddPodNode(&podNode)
		return podNode.ID
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, lcuuid), t.metadata.LogPrefixes)
		return 0
	}
}

func (t *DataSet) GetPodNodeLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.podNodeIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, id), t.metadata.LogPrefixes)
	var podNode metadbmodel.PodNode
	result := t.metadata.DB.Where("id = ?", id).Find(&podNode)
	if result.RowsAffected == 1 {
		t.AddPodNode(&podNode)
		return podNode.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetPodNamespaceIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.podNamespaceLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_NAMESPACE_EN, lcuuid), t.metadata.LogPrefixes)
	var podNamespace metadbmodel.PodNamespace
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&podNamespace)
	if result.RowsAffected == 1 {
		t.AddPodNamespace(&podNamespace)
		return podNamespace.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_NAMESPACE_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetPodIngressIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.podIngressLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_EN, lcuuid), t.metadata.LogPrefixes)
	var podIngress metadbmodel.PodIngress
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&podIngress)
	if result.RowsAffected == 1 {
		t.AddPodIngress(&podIngress)
		return podIngress.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetPodIngressLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.podIngressIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_EN, id), t.metadata.LogPrefixes)
	var podIngress metadbmodel.PodIngress
	result := t.metadata.DB.Where("id = ?", id).Find(&podIngress)
	if result.RowsAffected == 1 {
		t.AddPodIngress(&podIngress)
		return podIngress.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetPodIngressRuleIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.podIngressRuleLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_RULE_EN, lcuuid), t.metadata.LogPrefixes)
	var podIngressRule metadbmodel.PodIngressRule
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&podIngressRule)
	if result.RowsAffected == 1 {
		t.AddPodIngressRule(&podIngressRule)
		return podIngressRule.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_INGRESS_RULE_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetPodServiceIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.podServiceLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, lcuuid), t.metadata.LogPrefixes)
	var podService metadbmodel.PodService
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&podService)
	if result.RowsAffected == 1 {
		t.AddPodService(&podService)
		return podService.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetPodServiceLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.podServiceIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.PodService
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddPodService(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetPodGroupIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.podGroupLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN, lcuuid), t.metadata.LogPrefixes)
	var podGroup metadbmodel.PodGroup
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&podGroup)
	if result.RowsAffected == 1 {
		t.AddPodGroup(&podGroup)
		return podGroup.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetPodGroupTypeByID(id int) (int, bool) {
	podGroupType, exists := t.podGroupIDToType[id]
	if exists {
		return podGroupType, true
	}
	log.Warningf("cache %s type (id: %d) not found", ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN, id, t.metadata.LogPrefixes)
	var podGroup metadbmodel.PodGroup
	result := t.metadata.DB.Where("id = ?", id).Find(&podGroup)
	if result.RowsAffected == 1 {
		t.AddPodGroup(&podGroup)
		return podGroup.Type, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN, id), t.metadata.LogPrefixes)
		return 0, false
	}
}

func (t *DataSet) GetPodGroupLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.podGroupIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN, id), t.metadata.LogPrefixes)
	var podGroup metadbmodel.PodGroup
	result := t.metadata.DB.Where("id = ?", id).Find(&podGroup)
	if result.RowsAffected == 1 {
		t.AddPodGroup(&podGroup)
		return podGroup.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_GROUP_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetPodReplicaSetIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.podReplicaSetLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_REPLICA_SET_EN, lcuuid), t.metadata.LogPrefixes)
	var podReplicaSet metadbmodel.PodReplicaSet
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&podReplicaSet)
	if result.RowsAffected == 1 {
		t.AddPodReplicaSet(&podReplicaSet)
		return podReplicaSet.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_REPLICA_SET_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetPodReplicaSetLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.podReplicaSetIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_REPLICA_SET_EN, id), t.metadata.LogPrefixes)
	var podReplicaSet metadbmodel.PodReplicaSet
	result := t.metadata.DB.Where("id = ?", id).Find(&podReplicaSet)
	if result.RowsAffected == 1 {
		t.AddPodReplicaSet(&podReplicaSet)
		return podReplicaSet.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_REPLICA_SET_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetPodIDByLcuuid(lcuuid string) (int, bool) {
	id, exists := t.podLcuuidToID[lcuuid]
	if exists {
		return id, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_EN, lcuuid), t.metadata.LogPrefixes)
	var pod metadbmodel.Pod
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&pod)
	if result.RowsAffected == 1 {
		t.AddPod(&pod)
		return pod.ID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_POD_EN, lcuuid), t.metadata.LogPrefixes)
		return id, false
	}
}

func (t *DataSet) GetPodLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.podIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.Pod
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddPod(&dbItem)
		return dbItem.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetHostInfoByID(id int) (*hostInfo, error) {
	info, exists := t.hostIDtoInfo[id]
	if exists {
		return info, nil
	}

	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_HOST_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.Host
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddHost(&dbItem)
		return t.hostIDtoInfo[dbItem.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_HOST_EN, id))
}

func (t *DataSet) GetHostNameByID(id int) (string, error) {
	info, err := t.GetHostInfoByID(id)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (t *DataSet) GetVMInfoByID(id int) (*vmInfo, error) {
	info, exists := t.vmIDToInfo[id]
	if exists {
		return info, nil
	}

	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VM_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.VM
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddVM(&dbItem)
		return t.vmIDToInfo[dbItem.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VM_EN, id))
}

func (t *DataSet) GetVMNameByID(id int) (string, error) {
	info, err := t.GetVMInfoByID(id)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (t *DataSet) GetNetworkNameByID(id int) (string, bool) {
	name, exists := t.networkIDToName[id]
	if exists {
		return name, true
	}
	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, id), t.metadata.LogPrefixes)
	var network metadbmodel.Network
	result := t.metadata.DB.Where("id = ?", id).Find(&network)
	if result.RowsAffected == 1 {
		t.AddNetwork(&network)
		return network.Name, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_NETWORK_EN, id), t.metadata.LogPrefixes)
		return name, false
	}
}

func (t *DataSet) GetVRouterInfoByID(id int) (*vrouterInfo, error) {
	info, exists := t.vrouterIDToInfo[id]
	if exists {
		return info, nil
	}
	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, id), t.metadata.LogPrefixes)

	var vRouter metadbmodel.VRouter
	result := t.metadata.DB.Where("id = ?", id).Find(&vRouter)
	if result.RowsAffected == 1 {
		t.AddVRouter(&vRouter)
		return t.vrouterIDToInfo[vRouter.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VROUTER_EN, id))
}

func (t *DataSet) GetVRouterNameByID(id int) (string, error) {
	info, err := t.GetVRouterInfoByID(id)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (t *DataSet) GetDHCPPortInfoByID(id int) (*dhcpPortInfo, error) {
	info, exists := t.dhcpPortIDToInfo[id]
	if exists {
		return info, nil
	}
	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.DHCPPort
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddDHCPPort(&dbItem)
		return t.dhcpPortIDToInfo[id], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_DHCP_PORT_EN, id))
}

func (t *DataSet) GetDHCPPortNameByID(id int) (string, error) {
	info, err := t.GetDHCPPortInfoByID(id)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (t *DataSet) GetLBInfoByID(id int) (*lbInfo, error) {
	info, exists := t.lbIDToInfo[id]
	if exists {
		return info, nil
	}

	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_LB_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.LB
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddLB(&dbItem)
		return t.lbIDToInfo[dbItem.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_LB_EN, id))
}

func (t *DataSet) GetLBNameByID(id int) (string, error) {
	info, err := t.GetLBInfoByID(id)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (t *DataSet) GetNATGatewayInfoByID(id int) (*natGatewayInfo, error) {
	info, exists := t.natGatewayIDToInfo[id]
	if exists {
		return info, nil
	}
	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, id), t.metadata.LogPrefixes)

	var dbItem metadbmodel.NATGateway
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddNATGateway(&dbItem)
		return t.natGatewayIDToInfo[dbItem.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_NAT_GATEWAY_EN, id))
}

func (t *DataSet) GetNATGatewayNameByID(id int) (string, error) {
	info, err := t.GetNATGatewayInfoByID(id)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (t *DataSet) GetRDSInstanceInfoByID(id int) (*rdsInstanceInfo, error) {
	info, exists := t.rdsInstanceIDToInfo[id]
	if exists {
		return info, nil
	}

	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.RDSInstance
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddRDSInstance(&dbItem)
		return t.rdsInstanceIDToInfo[dbItem.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_RDS_INSTANCE_EN, id))
}

func (t *DataSet) GetRDSInstanceNameByID(id int) (string, error) {
	info, err := t.GetRDSInstanceInfoByID(id)
	if err != nil {
		return "", nil
	}
	return info.Name, nil
}

func (t *DataSet) GetRedisInstanceInfoByID(id int) (*redisInstanceInfo, error) {
	info, exists := t.redisInstanceIDToInfo[id]
	if exists {
		return info, nil
	}

	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.RedisInstance
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddRedisInstance(&dbItem)
		return t.redisInstanceIDToInfo[dbItem.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_REDIS_INSTANCE_EN, id))
}

func (t *DataSet) GetRedisInstanceNameByID(id int) (string, error) {
	info, err := t.GetRedisInstanceInfoByID(id)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (t *DataSet) GetPodNodeInfoByID(id int) (*podNodeInfo, error) {
	info, exists := t.podNodeIDToInfo[id]
	if exists {
		return info, nil
	}

	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.PodNode
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddPodNode(&dbItem)
		return t.podNodeIDToInfo[dbItem.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, id))
}

func (t *DataSet) GetPodNodeNameByID(id int) (string, error) {
	info, err := t.GetPodNodeInfoByID(id)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (t *DataSet) GetPodServiceInfoByID(id int) (*podServiceInfo, error) {
	info, exists := t.podServiceIDToInfo[id]
	if exists {
		return info, nil
	}

	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.PodService
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddPodService(&dbItem)
		return t.podServiceIDToInfo[dbItem.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_SERVICE_EN, id))
}

func (t *DataSet) GetPodServiceNameByID(id int) (string, error) {
	info, err := t.GetPodServiceInfoByID(id)
	if err != nil {
		return "", nil
	}
	return info.Name, nil
}

func (t *DataSet) GetPodInfoByID(id int) (*podInfo, error) {
	info, exists := t.podIDToInfo[id]
	if exists {
		return info, nil
	}

	log.Warning(cacheNameByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_EN, id), t.metadata.LogPrefixes)
	var dbItem metadbmodel.Pod
	result := t.metadata.DB.Where("id = ?", id).Find(&dbItem)
	if result.RowsAffected == 1 {
		t.AddPod(&dbItem)
		return t.podIDToInfo[dbItem.ID], nil
	}
	return nil, errors.New(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_POD_EN, id))
}

func (t *DataSet) GetPodNameByID(id int) (string, error) {
	info, err := t.GetPodInfoByID(id)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (t *DataSet) GetVInterfaceLcuuidByID(id int) (string, bool) {
	lcuuid, exists := t.vinterfaceIDToLcuuid[id]
	if exists {
		return lcuuid, true
	}
	log.Warning(cacheLcuuidByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, id), t.metadata.LogPrefixes)
	var vif metadbmodel.VInterface
	result := t.metadata.DB.Where("id = ?", id).Find(&vif)
	if result.RowsAffected == 1 {
		t.AddVInterface(&vif)
		return vif.Lcuuid, true
	} else {
		log.Error(dbResourceByIDNotFound(ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, id), t.metadata.LogPrefixes)
		return lcuuid, false
	}
}

func (t *DataSet) GetVInterfaceIDByWANIPLcuuid(lcuuid string) (int, bool) {
	vifID, exists := t.wanIPLcuuidToVInterfaceID[lcuuid]
	if exists {
		return vifID, true
	}
	log.Warningf("cache %s id (%s lcuuid: %s) not found", ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, ctrlrcommon.RESOURCE_TYPE_WAN_IP_EN, lcuuid, t.metadata.LogPrefixes)
	var wanIP metadbmodel.WANIP
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&wanIP)
	if result.RowsAffected == 1 {
		t.AddWANIP(&wanIP)
		vifID, exists = t.wanIPLcuuidToVInterfaceID[lcuuid]
		return vifID, exists
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_WAN_IP_EN, lcuuid), t.metadata.LogPrefixes)
		return vifID, false
	}
}

func (t *DataSet) GetWANIPByLcuuid(lcuuid string) (string, bool) {
	ip, exists := t.wanIPLcuuidToIP[lcuuid]
	if exists {
		return ip, true
	}
	log.Warning(cacheIPByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_WAN_IP_EN, lcuuid), t.metadata.LogPrefixes)
	var wanIP metadbmodel.WANIP
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&wanIP)
	if result.RowsAffected == 1 {
		t.AddWANIP(&wanIP)
		return wanIP.IP, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_WAN_IP_EN, lcuuid), t.metadata.LogPrefixes)
		return ip, false
	}
}

func (t *DataSet) GetVInterfaceIDByLANIPLcuuid(lcuuid string) (int, bool) {
	vifID, exists := t.lanIPLcuuidToVInterfaceID[lcuuid]
	if exists {
		return vifID, true
	}
	log.Warningf("cache %s id (%s lcuuid: %s) not found", ctrlrcommon.RESOURCE_TYPE_VINTERFACE_EN, ctrlrcommon.RESOURCE_TYPE_LAN_IP_EN, lcuuid, t.metadata.LogPrefixes)
	var lanIP metadbmodel.LANIP
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&lanIP)
	if result.RowsAffected == 1 {
		t.AddLANIP(&lanIP)
		vifID, exists = t.lanIPLcuuidToVInterfaceID[lcuuid]
		return vifID, exists
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_LAN_IP_EN, lcuuid), t.metadata.LogPrefixes)
		return vifID, false
	}
}

func (t *DataSet) GetLANIPByLcuuid(lcuuid string) (string, bool) {
	ip, exists := t.lanIPLcuuidToIP[lcuuid]
	if exists {
		return ip, true
	}
	log.Warning(cacheIPByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_LAN_IP_EN, lcuuid), t.metadata.LogPrefixes)
	var lanIP metadbmodel.LANIP
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&lanIP)
	if result.RowsAffected == 1 {
		t.AddLANIP(&lanIP)
		return lanIP.IP, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_LAN_IP_EN, lcuuid), t.metadata.LogPrefixes)
		return ip, false
	}
}

func (t *DataSet) GetVMIDByPodNodeID(podNodeID int) (int, bool) {
	id, exists := t.podNodeIDToVMID[podNodeID]
	if exists {
		return id, true
	}
	log.Warningf("cache %s id (%s id: %d) not found", ctrlrcommon.RESOURCE_TYPE_VM_EN, ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, podNodeID, t.metadata.LogPrefixes)
	var conn metadbmodel.VMPodNodeConnection
	result := t.metadata.DB.Where("pod_node_id = ?", podNodeID).Find(&conn)
	if result.RowsAffected == 1 {
		t.AddVMPodNodeConnection(&conn)
		return conn.VMID, true
	}
	return 0, false
}

func (t *DataSet) GetPodNodeIDByVMPodNodeConnectionLcuuid(lcuuid string) (int, bool) {
	id, exists := t.vmPodNodeConnectionLcuuidToPodNodeID[lcuuid]
	if exists {
		return id, true
	}
	log.Warningf("cache %s id (%s lcuuid: %s) not found", ctrlrcommon.RESOURCE_TYPE_POD_NODE_EN, ctrlrcommon.RESOURCE_TYPE_VM_POD_NODE_CONNECTION_EN, lcuuid, t.metadata.LogPrefixes)
	var conn metadbmodel.VMPodNodeConnection
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&conn)
	if result.RowsAffected == 1 {
		t.AddVMPodNodeConnection(&conn)
		return conn.PodNodeID, true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_VM_POD_NODE_CONNECTION_EN, lcuuid), t.metadata.LogPrefixes)
		return 0, false
	}

}

func (t *DataSet) AddProcess(item *metadbmodel.Process) {
	t.processLcuuidToInfo[item.Lcuuid] = &processInfo{
		ID:   item.ID,
		Name: item.Name,
	}
	t.GetLogFunc()(addToToolMap(ctrlrcommon.RESOURCE_TYPE_PROCESS_EN, item.Lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) DeleteProcess(lcuuid string) {
	delete(t.processLcuuidToInfo, lcuuid)
	log.Info(deleteFromToolMap(ctrlrcommon.RESOURCE_TYPE_PROCESS_EN, lcuuid), t.metadata.LogPrefixes)
}

func (t *DataSet) GetProcessInfoByLcuuid(lcuuid string) (*processInfo, bool) {
	processInfo, exists := t.processLcuuidToInfo[lcuuid]
	if exists {
		return processInfo, true
	}
	log.Warning(cacheIDByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_REGION_EN, lcuuid), t.metadata.LogPrefixes)
	var process *metadbmodel.Process
	result := t.metadata.DB.Where("lcuuid = ?", lcuuid).Find(&process)
	if result.RowsAffected == 1 {
		t.AddProcess(process)
		return t.processLcuuidToInfo[lcuuid], true
	} else {
		log.Error(dbResourceByLcuuidNotFound(ctrlrcommon.RESOURCE_TYPE_PROCESS_EN, lcuuid), t.metadata.LogPrefixes)
		return nil, false
	}
}

func (t *DataSet) GetPodIDByContainerIDWithoutLog(containerID string) (int, bool) {
	if len(containerID) == 0 {
		return 0, false
	}
	podID, exists := t.containerIDToPodID[containerID]
	if exists {
		return podID, true
	}
	return 0, false
}

func (t *DataSet) GetProcessDeviceTypeAndID(containerID string, vtapID uint32) (deviceType, deviceID int) {
	podID, exists := t.GetPodIDByContainerIDWithoutLog(containerID)
	if len(containerID) != 0 && exists {
		deviceType = common.VIF_DEVICE_TYPE_POD
		deviceID = podID
	} else {
		deviceType = common.VTAP_TYPE_TO_DEVICE_TYPE[t.vtapIDToType[int(vtapID)]]
		deviceID = t.vtapIDToLaunchServerID[int(vtapID)]
	}
	return
}

func (t *DataSet) SetPublicNetworkID(id int) {
	t.publicNetworkID = id
}
