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

package sync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/deepflowio/deepflow/message/agent"
	tridentcommon "github.com/deepflowio/deepflow/message/common"
	api "github.com/deepflowio/deepflow/message/controller"
	ccommon "github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/config"
	"github.com/deepflowio/deepflow/server/controller/db/metadb"
	mmodel "github.com/deepflowio/deepflow/server/controller/db/metadb/model"
	"github.com/deepflowio/deepflow/server/controller/genesis/common"
	"github.com/deepflowio/deepflow/server/controller/genesis/updater"
	"github.com/deepflowio/deepflow/server/controller/model"
	"github.com/deepflowio/deepflow/server/libs/logger"
	"github.com/deepflowio/deepflow/server/libs/queue"
	"google.golang.org/grpc"
)

var log = logger.MustGetLogger("genesis.store.sync")

type GenesisSync struct {
	isMaster bool
	data     atomic.Value
	ctx      context.Context
	cancel   context.CancelFunc
	queue    queue.QueueReader
	config   *config.ControllerConfig
}

func NewGenesisSync(ctx context.Context, isMaster bool, queue queue.QueueReader, config *config.ControllerConfig) *GenesisSync {
	var data atomic.Value
	data.Store(common.GenesisSyncData{})
	ctx, cancel := context.WithCancel(ctx)
	return &GenesisSync{
		isMaster: isMaster,
		ctx:      ctx,
		cancel:   cancel,
		data:     data,
		queue:    queue,
		config:   config,
	}
}

func (g *GenesisSync) receiveGenesisSyncData(sChan chan common.GenesisSyncData) {
	for {
		select {
		case s := <-sChan:
			g.data.Store(s)
		case <-g.ctx.Done():
			break
		}
	}
}

func (g *GenesisSync) GetGenesisSyncData(orgID int) common.GenesisSyncDataResponse {
	syncData := g.data.Load().(common.GenesisSyncData)
	return common.GenesisSyncDataResponse{
		IPLastSeens: syncData.IPLastSeens[orgID],
		VIPs:        syncData.VIPs[orgID],
		VMs:         syncData.VMs[orgID],
		VPCs:        syncData.VPCs[orgID],
		Hosts:       syncData.Hosts[orgID],
		Lldps:       syncData.Lldps[orgID],
		Ports:       syncData.Ports[orgID],
		Networks:    syncData.Networks[orgID],
		Vinterfaces: syncData.Vinterfaces[orgID],
		Processes:   syncData.Processes[orgID],
	}
}

func (g *GenesisSync) GetGenesisSyncResponse(orgID int) (common.GenesisSyncDataResponse, error) {
	retGenesisSyncData := common.GenesisSyncDataResponse{}

	db, err := metadb.GetDB(orgID)
	if err != nil {
		log.Error("get metadb session failed", logger.NewORGPrefix(orgID))
		return retGenesisSyncData, err
	}

	var controllers []mmodel.Controller
	var azControllerConns []mmodel.AZControllerConnection
	var currentRegion string

	db.Where("state <> ?", ccommon.CONTROLLER_STATE_EXCEPTION).Find(&controllers)
	db.Find(&azControllerConns)

	controllerIPToRegion := make(map[string]string)
	for _, conn := range azControllerConns {
		if os.Getenv(ccommon.NODE_IP_KEY) == conn.ControllerIP {
			currentRegion = conn.Region
		}
		controllerIPToRegion[conn.ControllerIP] = conn.Region
	}

	syncIPLcuuidSet := map[string]bool{}
	syncVIPLcuuidSet := map[string]bool{}
	syncHostLcuuidSet := map[string]bool{}
	syncLLDPLcuuidSet := map[string]bool{}
	syncNetworkLcuuidSet := map[string]bool{}
	syncPortLcuuidSet := map[string]bool{}
	syncVMLcuuidSet := map[string]bool{}
	syncVPCLcuuidSet := map[string]bool{}
	syncVinterfaceLcuuidSet := map[string]bool{}
	syncProcessLcuuidSet := map[string]bool{}
	for _, controller := range controllers {
		// skip other region controller
		if region, ok := controllerIPToRegion[controller.IP]; !ok || region != currentRegion {
			continue
		}

		// get effective vtap ids in current controller
		var storages []model.GenesisStorage
		db.Where("node_ip = ?", controller.IP).Find(&storages)
		vtapIDMap := map[uint32]int{0: 0}
		for _, storage := range storages {
			vtapIDMap[storage.VtapID] = 0
		}

		// use pod ip communication in internal region
		serverIP := controller.PodIP
		if serverIP == "" {
			serverIP = controller.IP
		}
		grpcServer := net.JoinHostPort(serverIP, g.config.GrpcPort)
		conn, err := grpc.Dial(grpcServer, grpc.WithInsecure(), grpc.WithMaxMsgSize(g.config.GrpcMaxMessageLength))
		if err != nil {
			msg := "create grpc connection faild:" + err.Error()
			log.Error(msg, logger.NewORGPrefix(orgID))
			return retGenesisSyncData, errors.New(msg)
		}
		defer conn.Close()

		client := api.NewControllerClient(conn)
		reqOrgID := uint32(orgID)
		req := &api.GenesisSharingSyncRequest{
			OrgId: &reqOrgID,
		}
		ret, err := client.GenesisSharingSync(context.Background(), req)
		if err != nil {
			msg := fmt.Sprintf("get genesis sharing sync faild (%s)", err.Error())
			log.Warning(msg, logger.NewORGPrefix(orgID))
			return retGenesisSyncData, errors.New(msg)
		}

		genesisSyncData := ret.GetData()
		genesisSyncIPs := genesisSyncData.GetIp()
		for _, ip := range genesisSyncIPs {
			if _, ok := vtapIDMap[ip.GetVtapId()]; !ok {
				continue
			}
			sIPLcuuid := ip.GetLcuuid()
			if _, ok := syncIPLcuuidSet[sIPLcuuid]; ok {
				continue
			}
			syncIPLcuuidSet[sIPLcuuid] = false
			ipLastSeenStr := ip.GetLastSeen()
			ipLastSeen, _ := time.ParseInLocation(ccommon.GO_BIRTHDAY, ipLastSeenStr, time.Local)
			retGenesisSyncData.IPLastSeens = append(retGenesisSyncData.IPLastSeens, model.GenesisIP{
				Masklen:          ip.GetMasklen(),
				IP:               ip.GetIp(),
				Lcuuid:           sIPLcuuid,
				VinterfaceLcuuid: ip.GetVinterfaceLcuuid(),
				NodeIP:           ip.GetNodeIp(),
				LastSeen:         ipLastSeen,
			})
		}

		genesisSyncVIPs := genesisSyncData.GetVip()
		for _, vip := range genesisSyncVIPs {
			vtapID := vip.GetVtapId()
			if _, ok := vtapIDMap[vtapID]; !ok {
				continue
			}
			sVIPLcuuid := vip.GetLcuuid()
			if _, ok := syncVIPLcuuidSet[sVIPLcuuid]; ok {
				continue
			}
			syncVIPLcuuidSet[sVIPLcuuid] = false
			retGenesisSyncData.VIPs = append(retGenesisSyncData.VIPs, model.GenesisVIP{
				VtapID: vtapID,
				IP:     vip.GetIp(),
				Lcuuid: sVIPLcuuid,
				NodeIP: vip.GetNodeIp(),
			})
		}

		genesisSyncHosts := genesisSyncData.GetHost()
		for _, host := range genesisSyncHosts {
			if _, ok := vtapIDMap[host.GetVtapId()]; !ok {
				continue
			}
			sHostLcuuid := host.GetLcuuid()
			if _, ok := syncHostLcuuidSet[sHostLcuuid]; ok {
				continue
			}
			syncHostLcuuidSet[sHostLcuuid] = false
			retGenesisSyncData.Hosts = append(retGenesisSyncData.Hosts, model.GenesisHost{
				Lcuuid:   sHostLcuuid,
				Hostname: host.GetHostname(),
				IP:       host.GetIp(),
				NodeIP:   host.GetNodeIp(),
			})
		}

		genesisSyncLldps := genesisSyncData.GetLldp()
		for _, l := range genesisSyncLldps {
			if _, ok := vtapIDMap[l.GetVtapId()]; !ok {
				continue
			}
			sLLDPLcuuid := l.GetLcuuid()
			if _, ok := syncLLDPLcuuidSet[sLLDPLcuuid]; ok {
				continue
			}
			syncLLDPLcuuidSet[sLLDPLcuuid] = false
			lLastSeenStr := l.GetLastSeen()
			lLastSeen, _ := time.ParseInLocation(ccommon.GO_BIRTHDAY, lLastSeenStr, time.Local)
			retGenesisSyncData.Lldps = append(retGenesisSyncData.Lldps, model.GenesisLldp{
				Lcuuid:                sLLDPLcuuid,
				HostIP:                l.GetHostIp(),
				HostInterface:         l.GetHostInterface(),
				SystemName:            l.GetSystemName(),
				ManagementAddress:     l.GetManagementAddress(),
				VinterfaceLcuuid:      l.GetVinterfaceLcuuid(),
				VinterfaceDescription: l.GetVinterfaceDescription(),
				NodeIP:                l.GetNodeIp(),
				LastSeen:              lLastSeen,
			})
		}

		genesisSyncNetworks := genesisSyncData.GetNetwork()
		for _, network := range genesisSyncNetworks {
			if _, ok := vtapIDMap[network.GetVtapId()]; !ok {
				continue
			}
			sNetworkLcuuid := network.GetLcuuid()
			if _, ok := syncNetworkLcuuidSet[sNetworkLcuuid]; ok {
				continue
			}
			syncNetworkLcuuidSet[sNetworkLcuuid] = false
			retGenesisSyncData.Networks = append(retGenesisSyncData.Networks, model.GenesisNetwork{
				SegmentationID: network.GetSegmentationId(),
				NetType:        network.GetNetType(),
				External:       network.GetExternal(),
				Name:           network.GetName(),
				Lcuuid:         sNetworkLcuuid,
				VPCLcuuid:      network.GetVpcLcuuid(),
				NodeIP:         network.GetNodeIp(),
			})
		}

		genesisSyncPorts := genesisSyncData.GetPort()
		for _, port := range genesisSyncPorts {
			if _, ok := vtapIDMap[port.GetVtapId()]; !ok {
				continue
			}
			sPortLcuuid := port.GetLcuuid()
			if _, ok := syncPortLcuuidSet[sPortLcuuid]; ok {
				continue
			}
			syncPortLcuuidSet[sPortLcuuid] = false
			retGenesisSyncData.Ports = append(retGenesisSyncData.Ports, model.GenesisPort{
				Type:          port.GetType(),
				DeviceType:    port.GetDeviceType(),
				Lcuuid:        sPortLcuuid,
				Mac:           port.GetMac(),
				DeviceLcuuid:  port.GetDeviceLcuuid(),
				NetworkLcuuid: port.GetNetworkLcuuid(),
				VPCLcuuid:     port.GetVpcLcuuid(),
				NodeIP:        port.GetNodeIp(),
			})
		}

		genesisSyncVms := genesisSyncData.GetVm()
		for _, vm := range genesisSyncVms {
			if _, ok := vtapIDMap[vm.GetVtapId()]; !ok {
				continue
			}
			sVMLcuuid := vm.GetLcuuid()
			if _, ok := syncVMLcuuidSet[sVMLcuuid]; ok {
				continue
			}
			syncVMLcuuidSet[sVMLcuuid] = false
			vCreatedAtStr := vm.GetCreatedAt()
			vCreatedAt, _ := time.ParseInLocation(ccommon.GO_BIRTHDAY, vCreatedAtStr, time.Local)
			retGenesisSyncData.VMs = append(retGenesisSyncData.VMs, model.GenesisVM{
				State:        vm.GetState(),
				Lcuuid:       sVMLcuuid,
				Name:         vm.GetName(),
				Label:        vm.GetLabel(),
				VPCLcuuid:    vm.GetVpcLcuuid(),
				LaunchServer: vm.GetLaunchServer(),
				NodeIP:       vm.GetNodeIp(),
				CreatedAt:    vCreatedAt,
			})
		}

		genesisSyncVpcs := genesisSyncData.GetVpc()
		for _, vpc := range genesisSyncVpcs {
			if _, ok := vtapIDMap[vpc.GetVtapId()]; !ok {
				continue
			}
			sVPCLcuuid := vpc.GetLcuuid()
			if _, ok := syncVPCLcuuidSet[sVPCLcuuid]; ok {
				continue
			}
			syncVPCLcuuidSet[sVPCLcuuid] = false
			retGenesisSyncData.VPCs = append(retGenesisSyncData.VPCs, model.GenesisVpc{
				Lcuuid: sVPCLcuuid,
				Name:   vpc.GetName(),
				NodeIP: vpc.GetNodeIp(),
			})
		}

		genesisSyncVinterfaces := genesisSyncData.GetVinterface()
		for _, v := range genesisSyncVinterfaces {
			if _, ok := vtapIDMap[v.GetVtapId()]; !ok {
				continue
			}
			sVinterfaceLcuuid := v.GetLcuuid()
			if _, ok := syncVinterfaceLcuuidSet[sVinterfaceLcuuid]; ok {
				continue
			}
			syncVinterfaceLcuuidSet[sVinterfaceLcuuid] = false
			vLastSeenStr := v.GetLastSeen()
			vpLastSeen, _ := time.ParseInLocation(ccommon.GO_BIRTHDAY, vLastSeenStr, time.Local)
			retGenesisSyncData.Vinterfaces = append(retGenesisSyncData.Vinterfaces, model.GenesisVinterface{
				VtapID:              v.GetVtapId(),
				Lcuuid:              sVinterfaceLcuuid,
				NetnsID:             v.GetNetnsId(),
				Name:                v.GetName(),
				IPs:                 v.GetIps(),
				Mac:                 v.GetMac(),
				TapName:             v.GetTapName(),
				TapMac:              v.GetTapMac(),
				DeviceLcuuid:        v.GetDeviceLcuuid(),
				DeviceName:          v.GetDeviceName(),
				DeviceType:          v.GetDeviceType(),
				IFType:              v.GetIfType(),
				HostIP:              v.GetHostIp(),
				KubernetesClusterID: v.GetKubernetesClusterId(),
				NodeIP:              v.GetNodeIp(),
				TeamID:              v.GetTeamId(),
				LastSeen:            vpLastSeen,
			})
		}

		genesisSyncProcesses := genesisSyncData.GetProcess()
		for _, p := range genesisSyncProcesses {
			if _, ok := vtapIDMap[p.GetVtapId()]; !ok {
				continue
			}
			sProcessLcuuid := p.GetLcuuid()
			if _, ok := syncProcessLcuuidSet[sProcessLcuuid]; ok {
				continue
			}
			syncProcessLcuuidSet[sProcessLcuuid] = false
			pStartTimeStr := p.GetStartTime()
			pStartTime, _ := time.ParseInLocation(ccommon.GO_BIRTHDAY, pStartTimeStr, time.Local)
			retGenesisSyncData.Processes = append(retGenesisSyncData.Processes, model.GenesisProcess{
				VtapID:      p.GetVtapId(),
				PID:         p.GetPid(),
				Lcuuid:      sProcessLcuuid,
				NetnsID:     p.GetNetnsId(),
				Name:        p.GetName(),
				ProcessName: p.GetProcessName(),
				CMDLine:     p.GetCmdLine(),
				ContainerID: p.GetContainerId(),
				UserName:    p.GetUser(),
				OSAPPTags:   p.GetOsAppTags(),
				NodeIP:      p.GetNodeIp(),
				StartTime:   pStartTime,
			})
		}
	}
	return retGenesisSyncData, nil
}

func (g *GenesisSync) Start() {
	sDataChan := make(chan common.GenesisSyncData)

	go g.receiveGenesisSyncData(sDataChan)

	go func() {
		vStorage := NewSyncStorage(g.ctx, g.config.GenesisCfg, sDataChan)
		vStorage.Start()

		genesisSyncDataByVtap := map[string]common.GenesisSyncDataResponse{}
		vUpdater := updater.NewGenesisSyncRpcUpdater(g.config.GenesisCfg)
		for {
			genesisSyncData := common.GenesisSyncDataResponse{}
			info := g.queue.Get().(common.VIFRPCMessage)
			if info.MessageType == common.TYPE_EXIT {
				log.Warningf("sync from (%s) vtap_id (%v) type (%v)", info.Peer, info.VtapID, info.MessageType, logger.NewORGPrefix(info.ORGID))
				continue
			}

			log.Debugf("sync received (%s) vtap_id (%v) type (%v) received (%s)", info.Peer, info.VtapID, info.MessageType, info.Message, logger.NewORGPrefix(info.ORGID))

			vtap := fmt.Sprintf("%d%d", info.ORGID, info.VtapID)
			if info.MessageType == common.TYPE_RENEW {
				if info.VtapID != 0 {
					peerInfo, ok := genesisSyncDataByVtap[vtap]
					if ok {
						vStorage.Renew(info.ORGID, peerInfo)
					}
				}
			} else if info.MessageType == common.TYPE_UPDATE {
				if info.Message != nil {
					tridentType := info.Message.GetTridentType()
					switch tridentType {
					case tridentcommon.TridentType_TT_PHYSICAL_MACHINE:
						genesisSyncData = vUpdater.UnmarshalWorkloadProtobuf(info.ORGID, info.TeamID, info.VtapID, info.Peer, common.DEVICE_TYPE_PHYSICAL_MACHINE, info.Message)
					case tridentcommon.TridentType_TT_PUBLIC_CLOUD:
						genesisSyncData = vUpdater.UnmarshalWorkloadProtobuf(info.ORGID, info.TeamID, info.VtapID, info.Peer, common.DEVICE_TYPE_PUBLIC_CLOUD, info.Message)
					case tridentcommon.TridentType_TT_HOST_POD, tridentcommon.TridentType_TT_VM_POD, tridentcommon.TridentType_TT_K8S_SIDECAR:
						genesisSyncData = vUpdater.UnmarshalKubernetesProtobuf(info.ORGID, info.TeamID, info.VtapID, info.Peer, info.Message)
					default:
						genesisSyncData = vUpdater.UnmarshalProtobuf(info.ORGID, info.TeamID, info.VtapID, info.Peer, info.Message)
					}
				} else if info.AgentMessage != nil {
					agentType := info.AgentMessage.GetAgentType()
					switch agentType {
					case agent.AgentType_TT_PHYSICAL_MACHINE:
						genesisSyncData = vUpdater.UnmarshalAgentWorkloadProtobuf(info.ORGID, info.TeamID, info.VtapID, info.Peer, common.DEVICE_TYPE_PHYSICAL_MACHINE, info.AgentMessage)
					case agent.AgentType_TT_PUBLIC_CLOUD:
						genesisSyncData = vUpdater.UnmarshalAgentWorkloadProtobuf(info.ORGID, info.TeamID, info.VtapID, info.Peer, common.DEVICE_TYPE_PUBLIC_CLOUD, info.AgentMessage)
					case agent.AgentType_TT_HOST_POD, agent.AgentType_TT_VM_POD, agent.AgentType_TT_K8S_SIDECAR:
						genesisSyncData = vUpdater.UnmarshalAgentKubernetesProtobuf(info.ORGID, info.TeamID, info.VtapID, info.Peer, info.AgentMessage)
					default:
						genesisSyncData = vUpdater.UnmarshalAgentProtobuf(info.ORGID, info.TeamID, info.VtapID, info.Peer, info.AgentMessage)
					}
				} else {
					log.Errorf("genesis sync message data is nil, vtap_id (%d)", info.VtapID, logger.NewORGPrefix(info.ORGID))
					continue
				}

				if info.VtapID != 0 {
					genesisSyncDataByVtap[vtap] = genesisSyncData
				}
				vStorage.Update(info.ORGID, info.VtapID, genesisSyncData)
			}
		}
	}()
}

func (s *GenesisSync) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}
