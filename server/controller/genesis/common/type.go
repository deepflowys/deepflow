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
	"time"

	"github.com/deepflowio/deepflow/message/agent"
	"github.com/deepflowio/deepflow/message/common"
	"github.com/deepflowio/deepflow/server/controller/model"
)

type GenesisSyncType interface {
	model.GenesisVinterface | model.GenesisVpc | model.GenesisHost | model.GenesisVM | model.GenesisVIP | model.GenesisNetwork | model.GenesisPort | model.GenesisLldp | model.GenesisIP | model.GenesisProcess
}

type GenesisSyncData struct {
	IPLastSeens map[int][]model.GenesisIP
	VIPs        map[int][]model.GenesisVIP
	VMs         map[int][]model.GenesisVM
	VPCs        map[int][]model.GenesisVpc
	Hosts       map[int][]model.GenesisHost
	Lldps       map[int][]model.GenesisLldp
	Ports       map[int][]model.GenesisPort
	Networks    map[int][]model.GenesisNetwork
	Vinterfaces map[int][]model.GenesisVinterface
	Processes   map[int][]model.GenesisProcess
}

type GenesisSyncDataResponse struct {
	IPLastSeens []model.GenesisIP
	VIPs        []model.GenesisVIP
	VMs         []model.GenesisVM
	VPCs        []model.GenesisVpc
	Hosts       []model.GenesisHost
	Lldps       []model.GenesisLldp
	Ports       []model.GenesisPort
	Networks    []model.GenesisNetwork
	Vinterfaces []model.GenesisVinterface
	Processes   []model.GenesisProcess
}

type KubernetesInfo struct {
	ORGID     int
	ClusterID string
	ErrorMSG  string
	Version   uint64
	Epoch     time.Time
	Entries   []*common.KubernetesAPIInfo
}

type K8SRPCMessage struct {
	ORGID       int
	MessageType int
	VtapID      uint32
	Peer        string
	Message     *agent.KubernetesAPISyncRequest
}

type VIFRPCMessage struct {
	ORGID        int
	MessageType  int
	TeamID       uint32
	VtapID       uint32
	Peer         string
	K8SClusterID string
	Message      *agent.GenesisSyncRequest
}
