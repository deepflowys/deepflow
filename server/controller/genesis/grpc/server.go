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

package grpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/deepflowio/deepflow/message/agent"
	"github.com/deepflowio/deepflow/message/controller"
	controllercommon "github.com/deepflowio/deepflow/server/controller/common"
	metadbcommon "github.com/deepflowio/deepflow/server/controller/db/metadb/common"
	"github.com/deepflowio/deepflow/server/controller/genesis/common"
	"github.com/deepflowio/deepflow/server/controller/genesis/config"
	kstore "github.com/deepflowio/deepflow/server/controller/genesis/store/kubernetes"
	sstore "github.com/deepflowio/deepflow/server/controller/genesis/store/sync"
	"github.com/deepflowio/deepflow/server/libs/logger"
	"github.com/deepflowio/deepflow/server/libs/queue"
	"google.golang.org/grpc/peer"
)

var log = logger.MustGetLogger("genesis.grpc")

type AgentStats struct {
	OrgID                    int
	TeamID                   int
	VtapID                   uint32
	TeamShortLcuuid          string
	IP                       string
	Proxy                    string
	K8sVersion               uint64
	SyncVersion              uint64
	K8sLastSeen              time.Time
	SyncLastSeen             time.Time
	K8sClusterID             string
	SyncAgentType            agent.AgentType
	SyncDataOperation        *agent.GenesisPlatformData
	SyncProcessDataOperation *agent.GenesisProcessData
}

type SynchronizerServer struct {
	cfg                   config.GenesisConfig
	k8sQueue              queue.QueueWriter
	genesisSyncQueue      queue.QueueWriter
	teamShortLcuuidToInfo sync.Map
	clusterIDToVersion    sync.Map
	vtapToVersion         sync.Map
	vtapToLastSeen        sync.Map
	clusterIDToLastSeen   sync.Map
	agentStatsMap         sync.Map
	gsync                 *sstore.GenesisSync
	gkubernetes           *kstore.GenesisKubernetes
}

func NewGenesisSynchronizerServer(cfg config.GenesisConfig, genesisSyncQueue, k8sQueue queue.QueueWriter,
	gsync *sstore.GenesisSync, gkubernetes *kstore.GenesisKubernetes) *SynchronizerServer {
	return &SynchronizerServer{
		cfg:                 cfg,
		k8sQueue:            k8sQueue,
		genesisSyncQueue:    genesisSyncQueue,
		gsync:               gsync,
		gkubernetes:         gkubernetes,
		vtapToVersion:       sync.Map{},
		vtapToLastSeen:      sync.Map{},
		clusterIDToVersion:  sync.Map{},
		clusterIDToLastSeen: sync.Map{},
		agentStatsMap:       sync.Map{},
	}
}

func (g *SynchronizerServer) GetAgentStats(orgID, vtapID string) (AgentStats, error) {
	vtap := fmt.Sprintf("%s-%s", orgID, vtapID)
	stats, ok := g.agentStatsMap.Load(vtap)
	if !ok {
		return AgentStats{}, errors.New(fmt.Sprintf("not found org id (%s) vtap id (%s) stats", orgID, vtapID))
	}
	return stats.(AgentStats), nil
}

func (g *SynchronizerServer) GenesisSync(ctx context.Context, request *agent.GenesisSyncRequest) (*agent.GenesisSyncResponse, error) {
	k8sClusterID := request.GetKubernetesClusterId()
	remote := ""
	peerIP, _ := peer.FromContext(ctx)
	sourceIP := request.GetSourceIp()
	if sourceIP != "" {
		remote = sourceIP
	} else {
		remote = peerIP.Addr.String()
	}
	version := request.GetVersion()
	if version == 0 {
		log.Warningf("genesis sync ignore message with version 0 from %s", remote)
		return &agent.GenesisSyncResponse{}, nil
	}

	vtapID := request.GetAgentId()
	tType := request.GetAgentType()
	if !common.IsAgentInterestedHost(tType) {
		log.Debugf("genesis sync ignore message from %s agent %s vtap_id %v", tType, remote, vtapID)
		return &agent.GenesisSyncResponse{Version: &version}, nil
	}

	var orgID, teamID int
	teamShortLcuuid := request.GetTeamId()
	if teamShortLcuuid == "" {
		orgID = metadbcommon.DEFAULT_ORG_ID
		teamID = metadbcommon.DEFAULT_TEAM_ID
	} else {
		t, ok := g.teamShortLcuuidToInfo.Load(teamShortLcuuid)
		if ok {
			orgID = t.(common.TeamInfo).OrgID
			teamID = t.(common.TeamInfo).TeamId
		} else {
			teamShortLcuuidToInfo, err := common.GetTeamShortLcuuidToInfo()
			if err != nil {
				log.Errorf("genesis sync from %s team_id %s vtap get team info failed: %s", remote, teamShortLcuuid, err.Error())
				return &agent.GenesisSyncResponse{Version: &version}, nil
			}
			teamInfo, ok := teamShortLcuuidToInfo[teamShortLcuuid]
			if !ok {
				log.Errorf("genesis sync from %s team_id %s not found team info", remote, teamShortLcuuid)
				return &agent.GenesisSyncResponse{Version: &version}, nil
			}
			orgID = teamInfo.OrgID
			teamID = teamInfo.TeamId
			for k, v := range teamShortLcuuidToInfo {
				g.teamShortLcuuidToInfo.Store(k, v)
			}
		}
	}
	vtap := fmt.Sprintf("%d-%d", orgID, vtapID)

	var localVersion uint64 = 0
	if vtapID == 0 {
		log.Infof("genesis sync received message with vtap_id 0 from %s", remote, logger.NewORGPrefix(orgID))
	} else {
		now := time.Now()
		if lTime, ok := g.vtapToLastSeen.Load(vtap); ok {
			lastTime := lTime.(time.Time)
			var agingTime float64 = 0
			if g.cfg.AgingTime < g.cfg.VinterfaceAgingTime {
				agingTime = g.cfg.AgingTime
			} else {
				agingTime = g.cfg.VinterfaceAgingTime
			}
			if now.Sub(lastTime).Seconds() >= agingTime {
				g.vtapToVersion.Store(vtap, uint64(0))
			}
		}
		g.vtapToLastSeen.Store(vtap, now)
		lVersion, ok := g.vtapToVersion.Load(vtap)
		if ok {
			localVersion = lVersion.(uint64)
		}
	}

	platformData := request.GetPlatformData()
	if version == localVersion || platformData == nil {
		log.Debugf("genesis sync renew version %v from ip %s vtap_id %v", version, remote, vtapID, logger.NewORGPrefix(orgID))
		g.genesisSyncQueue.Put(
			common.VIFRPCMessage{
				Peer:        remote,
				VtapID:      vtapID,
				ORGID:       orgID,
				TeamID:      uint32(teamID),
				MessageType: common.TYPE_RENEW,
				Message:     request,
			},
		)
		return &agent.GenesisSyncResponse{Version: &localVersion}, nil
	}

	log.Infof("genesis sync received version %v -> %v from ip %s vtap_id %v", localVersion, version, remote, vtapID, logger.NewORGPrefix(orgID))
	g.genesisSyncQueue.Put(
		common.VIFRPCMessage{
			Peer:         remote,
			VtapID:       vtapID,
			ORGID:        orgID,
			TeamID:       uint32(teamID),
			K8SClusterID: k8sClusterID,
			MessageType:  common.TYPE_UPDATE,
			Message:      request,
		},
	)

	if vtapID != 0 {
		var stats AgentStats
		if s, ok := g.agentStatsMap.Load(vtap); ok {
			stats = s.(AgentStats)
		}
		if sourceIP != "" {
			stats.Proxy = peerIP.Addr.String()
		}
		stats.IP = remote
		stats.OrgID = orgID
		stats.TeamID = teamID
		stats.VtapID = vtapID
		stats.SyncVersion = version
		stats.SyncAgentType = tType
		stats.SyncLastSeen = time.Now()
		stats.K8sClusterID = k8sClusterID
		stats.TeamShortLcuuid = teamShortLcuuid
		stats.SyncProcessDataOperation = request.GetProcessData()
		stats.SyncDataOperation = platformData
		g.agentStatsMap.Store(vtap, stats)
		g.vtapToVersion.Store(vtap, version)
	}
	return &agent.GenesisSyncResponse{Version: &version}, nil
}

func (g *SynchronizerServer) KubernetesAPISync(ctx context.Context, request *agent.KubernetesAPISyncRequest) (*agent.KubernetesAPISyncResponse, error) {
	remote := ""
	peerIP, _ := peer.FromContext(ctx)
	sourceIP := request.GetSourceIp()
	if sourceIP != "" {
		remote = sourceIP
	} else {
		remote = peerIP.Addr.String()
	}
	vtapID := request.GetAgentId()
	if vtapID == 0 {
		log.Warningf("kubernetes api sync received message with vtap_id 0 from %s", remote)
	}
	version := request.GetVersion()
	if version == 0 {
		log.Warningf("kubernetes api sync ignore message with version 0 from ip: %s, vtap id: %d", remote, vtapID)
		return &agent.KubernetesAPISyncResponse{}, nil
	}
	clusterID := request.GetClusterId()
	if clusterID == "" {
		log.Warningf("kubernetes api sync ignore message with cluster id null from ip: %s, vtap id: %v", remote, vtapID)
		return &agent.KubernetesAPISyncResponse{}, nil
	}
	entries := request.GetEntries()

	var orgID, teamID int
	teamShortLcuuid := request.GetTeamId()
	if teamShortLcuuid == "" {
		orgID = metadbcommon.DEFAULT_ORG_ID
		teamID = metadbcommon.DEFAULT_TEAM_ID
	} else {
		t, ok := g.teamShortLcuuidToInfo.Load(teamShortLcuuid)
		if ok {
			orgID = t.(common.TeamInfo).OrgID
			teamID = t.(common.TeamInfo).TeamId
		} else {
			teamShortLcuuidToInfo, err := common.GetTeamShortLcuuidToInfo()
			if err != nil {
				log.Errorf("kubernetes api sync from %s team_id %s vtap get team info failed: %s", remote, teamShortLcuuid, err.Error())
				return &agent.KubernetesAPISyncResponse{}, nil
			}
			teamInfo, ok := teamShortLcuuidToInfo[teamShortLcuuid]
			if !ok {
				log.Errorf("kubernetes api sync %s team_id %s not found team info", remote, teamShortLcuuid)
				return &agent.KubernetesAPISyncResponse{}, nil
			}
			orgID = teamInfo.OrgID
			teamID = teamInfo.TeamId
			for k, v := range teamShortLcuuidToInfo {
				g.teamShortLcuuidToInfo.Store(k, v)
			}
		}
	}
	vtap := fmt.Sprintf("%d-%d", orgID, vtapID)

	var stats AgentStats
	if s, ok := g.agentStatsMap.Load(vtap); ok {
		stats = s.(AgentStats)
	}
	if sourceIP != "" {
		stats.Proxy = peerIP.Addr.String()
	}
	stats.IP = remote
	stats.OrgID = orgID
	stats.TeamID = teamID
	stats.VtapID = vtapID
	stats.K8sClusterID = clusterID
	stats.K8sLastSeen = time.Now()
	stats.K8sVersion = version
	stats.TeamShortLcuuid = teamShortLcuuid
	g.agentStatsMap.Store(vtap, stats)
	now := time.Now()
	if vtapID != 0 {
		if lastTime, ok := g.clusterIDToLastSeen.Load(clusterID); ok {
			if now.Sub(lastTime.(time.Time)).Seconds() >= g.cfg.AgingTime {
				g.clusterIDToVersion.Store(clusterID, uint64(0))
			}
		}
		var localVersion uint64 = 0
		lVersion, ok := g.clusterIDToVersion.Load(clusterID)
		if ok {
			localVersion = lVersion.(uint64)
		}
		log.Infof("kubernetes api sync received version %v -> %v from ip %s vtap_id %v len %v", localVersion, version, remote, vtapID, len(entries), logger.NewORGPrefix(orgID))

		// 如果version有更新，但消息中没有任何kubernetes数据，触发vtap重新上报数据
		if localVersion != version && len(entries) == 0 {
			return &agent.KubernetesAPISyncResponse{Version: &localVersion}, nil
		}

		// 正常推送消息到队列中
		g.k8sQueue.Put(common.K8SRPCMessage{
			Peer:        remote,
			ORGID:       orgID,
			VtapID:      vtapID,
			MessageType: 0,
			Message:     request,
		})

		// 更新内存中的last_seen和version
		g.clusterIDToLastSeen.Store(clusterID, now)
		g.clusterIDToVersion.Store(clusterID, version)
		return &agent.KubernetesAPISyncResponse{Version: &version}, nil
	} else {
		log.Infof("kubernetes api sync received version %v from ip %s no vtap_id", version, remote, logger.NewORGPrefix(orgID))
		//正常上报数据，才推送消息到队列中
		if len(entries) > 0 {
			g.k8sQueue.Put(common.K8SRPCMessage{
				Peer:        remote,
				ORGID:       orgID,
				VtapID:      vtapID,
				MessageType: 0,
				Message:     request,
			})
		}
		// 采集器未自动发现时，触发agent上报完整数据
		return &agent.KubernetesAPISyncResponse{}, nil
	}
}

func (g *SynchronizerServer) GenesisSharingK8S(ctx context.Context, request *controller.GenesisSharingK8SRequest) (*controller.GenesisSharingK8SResponse, error) {
	orgID := request.GetOrgId()
	clusterID := request.GetClusterId()

	if k8sData, ok := g.gkubernetes.GetKubernetesData(int(orgID), clusterID); ok {
		epochStr := k8sData.Epoch.Format(controllercommon.GO_BIRTHDAY)
		return &controller.GenesisSharingK8SResponse{
			Epoch:    &epochStr,
			ErrorMsg: &k8sData.ErrorMSG,
			Entries:  k8sData.Entries,
		}, nil
	}

	return &controller.GenesisSharingK8SResponse{}, nil
}

func (g *SynchronizerServer) GenesisSharingSync(ctx context.Context, request *controller.GenesisSharingSyncRequest) (*controller.GenesisSharingSyncResponse, error) {
	orgID := request.GetOrgId()
	gSyncData := g.gsync.GetGenesisSyncData(int(orgID))

	gSyncIPs := []*controller.GenesisSyncIP{}
	for _, ip := range gSyncData.IPLastSeens {
		ipData := ip
		ipLastSeen := ipData.LastSeen.Format(controllercommon.GO_BIRTHDAY)
		gIP := &controller.GenesisSyncIP{
			Masklen:          &ipData.Masklen,
			Ip:               &ipData.IP,
			Lcuuid:           &ipData.Lcuuid,
			VinterfaceLcuuid: &ipData.VinterfaceLcuuid,
			NodeIp:           &ipData.NodeIP,
			LastSeen:         &ipLastSeen,
			VtapId:           &ipData.VtapID,
		}
		gSyncIPs = append(gSyncIPs, gIP)
	}

	gSyncVIPs := []*controller.GenesisSyncVIP{}
	for _, vip := range gSyncData.VIPs {
		vipData := vip
		gVIP := &controller.GenesisSyncVIP{
			Ip:     &vipData.IP,
			Lcuuid: &vipData.Lcuuid,
			NodeIp: &vipData.NodeIP,
			VtapId: &vipData.VtapID,
		}
		gSyncVIPs = append(gSyncVIPs, gVIP)
	}

	gSyncHosts := []*controller.GenesisSyncHost{}
	for _, host := range gSyncData.Hosts {
		hostData := host
		gHost := &controller.GenesisSyncHost{
			Lcuuid:   &hostData.Lcuuid,
			Hostname: &hostData.Hostname,
			Ip:       &hostData.IP,
			NodeIp:   &hostData.NodeIP,
			VtapId:   &hostData.VtapID,
		}
		gSyncHosts = append(gSyncHosts, gHost)
	}

	gSyncLldps := []*controller.GenesisSyncLldp{}
	for _, l := range gSyncData.Lldps {
		lData := l
		lLastSeen := lData.LastSeen.Format(controllercommon.GO_BIRTHDAY)
		gLldp := &controller.GenesisSyncLldp{
			Lcuuid:                &lData.Lcuuid,
			HostIp:                &lData.HostIP,
			HostInterface:         &lData.HostInterface,
			SystemName:            &lData.SystemName,
			ManagementAddress:     &lData.ManagementAddress,
			VinterfaceLcuuid:      &lData.VinterfaceLcuuid,
			VinterfaceDescription: &lData.VinterfaceDescription,
			NodeIp:                &lData.NodeIP,
			LastSeen:              &lLastSeen,
			VtapId:                &lData.VtapID,
		}
		gSyncLldps = append(gSyncLldps, gLldp)
	}

	gSyncNetworks := []*controller.GenesisSyncNetwork{}
	for _, network := range gSyncData.Networks {
		networkData := network
		gNetwork := &controller.GenesisSyncNetwork{
			SegmentationId: &networkData.SegmentationID,
			NetType:        &networkData.NetType,
			External:       &networkData.External,
			Name:           &networkData.Name,
			Lcuuid:         &networkData.Lcuuid,
			VpcLcuuid:      &networkData.VPCLcuuid,
			NodeIp:         &networkData.NodeIP,
			VtapId:         &networkData.VtapID,
		}
		gSyncNetworks = append(gSyncNetworks, gNetwork)
	}

	gSyncPorts := []*controller.GenesisSyncPort{}
	for _, port := range gSyncData.Ports {
		portData := port
		gPort := &controller.GenesisSyncPort{
			Type:          &portData.Type,
			DeviceType:    &portData.DeviceType,
			Lcuuid:        &portData.Lcuuid,
			Mac:           &portData.Mac,
			DeviceLcuuid:  &portData.DeviceLcuuid,
			NetworkLcuuid: &portData.NetworkLcuuid,
			VpcLcuuid:     &portData.VPCLcuuid,
			NodeIp:        &portData.NodeIP,
			VtapId:        &portData.VtapID,
		}
		gSyncPorts = append(gSyncPorts, gPort)
	}

	gSyncVms := []*controller.GenesisSyncVm{}
	for _, vm := range gSyncData.VMs {
		vmData := vm
		vCreateAt := vmData.CreatedAt.Format(controllercommon.GO_BIRTHDAY)
		gVm := &controller.GenesisSyncVm{
			State:        &vmData.State,
			Lcuuid:       &vmData.Lcuuid,
			Name:         &vmData.Name,
			Label:        &vmData.Label,
			VpcLcuuid:    &vmData.VPCLcuuid,
			LaunchServer: &vmData.LaunchServer,
			NodeIp:       &vmData.NodeIP,
			CreatedAt:    &vCreateAt,
			VtapId:       &vmData.VtapID,
		}
		gSyncVms = append(gSyncVms, gVm)
	}

	gSyncVpcs := []*controller.GenesisSyncVpc{}
	for _, vpc := range gSyncData.VPCs {
		vpcData := vpc
		gVpc := &controller.GenesisSyncVpc{
			Lcuuid: &vpcData.Lcuuid,
			Name:   &vpcData.Name,
			NodeIp: &vpcData.NodeIP,
			VtapId: &vpcData.VtapID,
		}
		gSyncVpcs = append(gSyncVpcs, gVpc)
	}

	gSyncVinterfaces := []*controller.GenesisSyncVinterface{}
	for _, v := range gSyncData.Vinterfaces {
		vData := v
		vLastSeen := vData.LastSeen.Format(controllercommon.GO_BIRTHDAY)
		gVinterface := &controller.GenesisSyncVinterface{
			VtapId:              &vData.VtapID,
			Lcuuid:              &vData.Lcuuid,
			NetnsId:             &vData.NetnsID,
			Name:                &vData.Name,
			Ips:                 &vData.IPs,
			Mac:                 &vData.Mac,
			TapName:             &vData.TapName,
			TapMac:              &vData.TapMac,
			DeviceLcuuid:        &vData.DeviceLcuuid,
			DeviceName:          &vData.DeviceName,
			DeviceType:          &vData.DeviceType,
			IfType:              &vData.IFType,
			HostIp:              &vData.HostIP,
			KubernetesClusterId: &vData.KubernetesClusterID,
			NodeIp:              &vData.NodeIP,
			TeamId:              &vData.TeamID,
			LastSeen:            &vLastSeen,
		}
		gSyncVinterfaces = append(gSyncVinterfaces, gVinterface)
	}

	gSyncProcesses := []*controller.GenesisSyncProcess{}
	for _, p := range gSyncData.Processes {
		pData := p
		pStartTime := pData.StartTime.Format(controllercommon.GO_BIRTHDAY)
		gProcess := &controller.GenesisSyncProcess{
			VtapId:      &pData.VtapID,
			Pid:         &pData.PID,
			Lcuuid:      &pData.Lcuuid,
			NetnsId:     &pData.NetnsID,
			Name:        &pData.Name,
			ProcessName: &pData.ProcessName,
			CmdLine:     &pData.CMDLine,
			User:        &pData.UserName,
			ContainerId: &pData.ContainerID,
			OsAppTags:   &pData.OSAPPTags,
			NodeIp:      &pData.NodeIP,
			StartTime:   &pStartTime,
		}
		gSyncProcesses = append(gSyncProcesses, gProcess)
	}

	return &controller.GenesisSharingSyncResponse{
		Data: &controller.GenesisSyncData{
			Ip:         gSyncIPs,
			Vip:        gSyncVIPs,
			Host:       gSyncHosts,
			Lldp:       gSyncLldps,
			Network:    gSyncNetworks,
			Port:       gSyncPorts,
			Vm:         gSyncVms,
			Vpc:        gSyncVpcs,
			Vinterface: gSyncVinterfaces,
			Process:    gSyncProcesses,
		},
	}, nil
}
