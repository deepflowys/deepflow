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

package genesis

import (
	"context"
	"os"

	ccommon "github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/config"
	"github.com/deepflowio/deepflow/server/controller/db/metadb"
	metadbmodel "github.com/deepflowio/deepflow/server/controller/db/metadb/model"
	"github.com/deepflowio/deepflow/server/controller/genesis/common"
	"github.com/deepflowio/deepflow/server/controller/genesis/grpc"
	kstore "github.com/deepflowio/deepflow/server/controller/genesis/store/kubernetes"
	sstore "github.com/deepflowio/deepflow/server/controller/genesis/store/sync"
	"github.com/deepflowio/deepflow/server/libs/logger"
	"github.com/deepflowio/deepflow/server/libs/queue"
)

var log = logger.MustGetLogger("genesis")
var GenesisService *Genesis

type Genesis struct {
	ctx          context.Context
	config       *config.ControllerConfig
	sync         *sstore.GenesisSync
	kubernetes   *kstore.GenesisKubernetes
	Synchronizer *grpc.SynchronizerServer
}

func NewGenesis(ctx context.Context, isMaster bool, config *config.ControllerConfig) *Genesis {
	syncQueue := queue.NewOverwriteQueue("genesis-sync-data", config.GenesisCfg.QueueLengths)
	kubernetesQueue := queue.NewOverwriteQueue("genesis-k8s-data", config.GenesisCfg.QueueLengths)

	genesisSync := sstore.NewGenesisSync(ctx, isMaster, syncQueue, config)
	genesisSync.Start()

	genesisK8S := kstore.NewGenesisKubernetes(ctx, kubernetesQueue, config)
	genesisK8S.Start()

	synchronizer := grpc.NewGenesisSynchronizerServer(config.GenesisCfg, syncQueue, kubernetesQueue, genesisSync, genesisK8S)

	GenesisService = &Genesis{
		ctx:          ctx,
		config:       config,
		sync:         genesisSync,
		kubernetes:   genesisK8S,
		Synchronizer: synchronizer,
	}
	return GenesisService
}

func (g *Genesis) getServerIPs(orgID int) ([]string, error) {
	db, err := metadb.GetDB(orgID)
	if err != nil {
		log.Error("get metadb session failed", logger.NewORGPrefix(orgID))
		return []string{}, err
	}

	var serverIPs []string
	var controllers []metadbmodel.Controller
	var azControllerConns []metadbmodel.AZControllerConnection
	var currentRegion string

	nodeIP := os.Getenv(ccommon.NODE_IP_KEY)
	err = db.Find(&azControllerConns).Error
	if err != nil {
		log.Warningf("query az_controller_connection failed (%s)", err.Error(), logger.NewORGPrefix(orgID))
		return []string{}, err
	}
	err = db.Where("ip <> ? AND state <> ?", nodeIP, ccommon.CONTROLLER_STATE_EXCEPTION).Find(&controllers).Error
	if err != nil {
		log.Warningf("query controller failed (%s)", err.Error(), logger.NewORGPrefix(orgID))
		return []string{}, err
	}

	controllerIPToRegion := make(map[string]string)
	for _, conn := range azControllerConns {
		if nodeIP == conn.ControllerIP {
			currentRegion = conn.Region
		}
		controllerIPToRegion[conn.ControllerIP] = conn.Region
	}

	for _, controller := range controllers {
		// skip other region controller
		if region, ok := controllerIPToRegion[controller.IP]; !ok || region != currentRegion {
			continue
		}

		// use pod ip communication in internal region
		serverIP := controller.PodIP
		if serverIP == "" {
			serverIP = controller.IP
		}
		serverIPs = append(serverIPs, serverIP)
	}
	return serverIPs, nil
}

func (g *Genesis) GetGenesisSyncData(orgID int) common.GenesisSyncDataResponse {
	return g.sync.GetGenesisSyncData(orgID)
}

func (g *Genesis) GetGenesisSyncResponse(orgID int) (common.GenesisSyncDataResponse, error) {
	return g.sync.GetGenesisSyncResponse(orgID)
}

func (g *Genesis) GetKubernetesData(orgID int, clusterID string) (common.KubernetesInfo, bool) {
	return g.kubernetes.GetKubernetesData(orgID, clusterID)
}

func (g *Genesis) GetKubernetesResponse(orgID int, clusterID string) (map[string][]string, error) {
	serverIPs, err := g.getServerIPs(orgID)
	if err != nil {
		return map[string][]string{}, err
	}
	return g.kubernetes.GetKubernetesResponse(orgID, clusterID, serverIPs)
}
