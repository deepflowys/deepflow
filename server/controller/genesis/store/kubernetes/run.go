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

package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	mcommon "github.com/deepflowio/deepflow/message/common"
	api "github.com/deepflowio/deepflow/message/controller"
	ccommon "github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/config"
	"github.com/deepflowio/deepflow/server/controller/genesis/common"
	"github.com/deepflowio/deepflow/server/controller/statsd"
	"github.com/deepflowio/deepflow/server/libs/logger"
	"github.com/deepflowio/deepflow/server/libs/queue"
	"google.golang.org/grpc"
)

var log = logger.MustGetLogger("genesis.store.kubernetes")

type GenesisKubernetes struct {
	statsdORGID   int
	data          sync.Map
	mutex         sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	queue         queue.QueueReader
	genesisStatsd statsd.GenesisStatsd
	config        *config.ControllerConfig
}

func NewGenesisKubernetes(ctx context.Context, queue queue.QueueReader, config *config.ControllerConfig) *GenesisKubernetes {
	ctx, cancel := context.WithCancel(ctx)
	return &GenesisKubernetes{
		data:   sync.Map{},
		ctx:    ctx,
		cancel: cancel,
		queue:  queue,
		config: config,
		genesisStatsd: statsd.GenesisStatsd{
			K8SInfoDelay: make(map[string][]float64),
		},
	}
}

func (g *GenesisKubernetes) GetStatter() statsd.StatsdStatter {
	return statsd.StatsdStatter{
		OrgID:   g.statsdORGID,
		Element: statsd.GetGenesisStatsd(g.genesisStatsd),
	}
}

func (g *GenesisKubernetes) receiveKubernetesData(kChan chan common.KubernetesInfo) {
	for {
		select {
		case k := <-kChan:
			g.data.Store(fmt.Sprintf("%d-%s", k.ORGID, k.ClusterID), k)
		case <-g.ctx.Done():
			break
		}
	}
}

func (g *GenesisKubernetes) GetKubernetesData(orgID int, clusterID string) (common.KubernetesInfo, bool) {
	k8sDataInterface, ok := g.data.Load(fmt.Sprintf("%d-%s", orgID, clusterID))
	if !ok {
		log.Warningf("kubernetes data not found cluster id (%s)", clusterID, logger.NewORGPrefix(orgID))
		return common.KubernetesInfo{}, false
	}
	k8sData, ok := k8sDataInterface.(common.KubernetesInfo)
	if !ok {
		log.Error("kubernetes data interface assert failed", logger.NewORGPrefix(orgID))
		return common.KubernetesInfo{}, false
	}
	return k8sData, true
}

func (g *GenesisKubernetes) GetKubernetesResponse(orgID int, clusterID string, serverIPs []string) (map[string][]string, error) {
	k8sResp := map[string][]string{}

	k8sInfo, ok := g.GetKubernetesData(orgID, clusterID)

	retFlag := false
	for _, serverIP := range serverIPs {
		grpcServer := net.JoinHostPort(serverIP, g.config.GrpcPort)
		conn, err := grpc.Dial(grpcServer, grpc.WithInsecure(), grpc.WithMaxMsgSize(g.config.GrpcMaxMessageLength))
		if err != nil {
			msg := "create grpc connection faild:" + err.Error()
			log.Error(msg, logger.NewORGPrefix(orgID))
			return k8sResp, errors.New(msg)
		}
		defer conn.Close()

		client := api.NewControllerClient(conn)
		reqOrgID := uint32(orgID)
		req := &api.GenesisSharingK8SRequest{
			OrgId:     &reqOrgID,
			ClusterId: &clusterID,
		}
		ret, err := client.GenesisSharingK8S(context.Background(), req)
		if err != nil {
			msg := fmt.Sprintf("get (%s) genesis sharing k8s failed (%s) ", serverIP, err.Error())
			log.Error(msg, logger.NewORGPrefix(orgID), logger.NewORGPrefix(orgID))
			return k8sResp, errors.New(msg)
		}
		entries := ret.GetEntries()
		if len(entries) == 0 {
			log.Debugf("genesis sharing k8s node (%s) entries length is 0", serverIP, logger.NewORGPrefix(orgID))
			continue
		}
		epochStr := ret.GetEpoch()
		epoch, err := time.ParseInLocation(ccommon.GO_BIRTHDAY, epochStr, time.Local)
		if err != nil {
			log.Error("genesis api sharing k8s format timestr faild:"+err.Error(), logger.NewORGPrefix(orgID))
			return k8sResp, err
		}
		if !epoch.After(k8sInfo.Epoch) {
			continue
		}

		retFlag = true
		k8sInfo = common.KubernetesInfo{
			Epoch:    epoch,
			Entries:  entries,
			ErrorMSG: ret.GetErrorMsg(),
		}
	}
	if !ok && !retFlag {
		return k8sResp, errors.New("no vtap report cluster id:" + clusterID)
	}
	if k8sInfo.ErrorMSG != "" {
		log.Errorf("cluster id (%s) k8s info grpc Error: %s", clusterID, k8sInfo.ErrorMSG, logger.NewORGPrefix(orgID))
		return k8sResp, errors.New(k8sInfo.ErrorMSG)
	}
	if len(k8sInfo.Entries) == 0 {
		return k8sResp, errors.New("not found k8s entries")
	}

	g.mutex.Lock()
	g.statsdORGID = orgID
	g.genesisStatsd.K8SInfoDelay = map[string][]float64{}
	g.genesisStatsd.K8SInfoDelay[clusterID] = []float64{time.Now().Sub(k8sInfo.Epoch).Seconds()}
	statsd.MetaStatsd.RegisterStatsdTable(g)
	g.mutex.Unlock()

	for _, e := range k8sInfo.Entries {
		eType := e.GetType()
		out, err := common.ParseCompressedInfo(e.GetCompressedInfo())
		if err != nil {
			log.Warningf("decode decompress error: %s", err.Error(), logger.NewORGPrefix(orgID))
			return map[string][]string{}, err
		}
		k8sResp[eType] = append(k8sResp[eType], string(out.Bytes()))
	}
	return k8sResp, nil
}

func (g *GenesisKubernetes) Start() {
	kDataChan := make(chan common.KubernetesInfo)

	go g.receiveKubernetesData(kDataChan)

	go func() {
		kStorage := NewKubernetesStorage(g.ctx, g.config.ListenPort, g.config.ListenNodePort, g.config.GenesisCfg, kDataChan)
		kStorage.Start()

		for {
			info := g.queue.Get().(common.K8SRPCMessage)
			if info.MessageType == common.TYPE_EXIT {
				log.Warningf("k8s from (%s) vtap_id (%v) type (%v) exit", info.Peer, info.VtapID, info.MessageType, logger.NewORGPrefix(info.ORGID))
				break
			}
			var version uint64
			var clusterID, errMsg string
			var entries []*mcommon.KubernetesAPIInfo
			if info.Message == nil {
				log.Errorf("k8s message is nil, vtap_id (%d)", info.VtapID, logger.NewORGPrefix(info.ORGID))
				continue
			}
			clusterID = info.Message.GetClusterId()
			version = info.Message.GetVersion()
			errMsg = info.Message.GetErrorMsg()
			entries = info.Message.GetEntries()

			log.Debugf("k8s from %s vtap_id %v received cluster_id %s version %d", info.Peer, info.VtapID, clusterID, version, logger.NewORGPrefix(info.ORGID))
			// 更新和保存内存数据
			kStorage.Add(info.ORGID, common.KubernetesInfo{
				ORGID:     info.ORGID,
				ClusterID: clusterID,
				Epoch:     time.Now(),
				ErrorMSG:  errMsg,
				Version:   version,
				Entries:   entries,
			})
		}
	}()
}

func (s *GenesisKubernetes) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}
