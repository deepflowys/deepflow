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

package synchronize

import (
	"strings"
	"time"

	api "github.com/deepflowio/deepflow/message/trident"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	grpcserver "github.com/deepflowio/deepflow/server/controller/grpc"
	"github.com/deepflowio/deepflow/server/controller/grpc/statsd"
	prometheus "github.com/deepflowio/deepflow/server/controller/prometheus/service/grpc"
	trisolaris "github.com/deepflowio/deepflow/server/controller/trisolaris/services/grpc/synchronize"
)

type service struct {
	vTapEvent                *trisolaris.VTapEvent
	tsdbEvent                *trisolaris.TSDBEvent
	ntpEvent                 *trisolaris.NTPEvent
	upgradeEvent             *trisolaris.UpgradeEvent
	kubernetesClusterIDEvent *trisolaris.KubernetesClusterIDEvent
	processInfoEvent         *trisolaris.ProcessInfoEvent
	pluginEvent              *trisolaris.PluginEvent
	prometheusEvent          *prometheus.SynchronizerEvent
}

func init() {
	grpcserver.Add(newService())
}

func newService() *service {
	return &service{
		vTapEvent:        trisolaris.NewVTapEvent(),
		tsdbEvent:        trisolaris.NewTSDBEvent(),
		ntpEvent:         trisolaris.NewNTPEvent(),
		upgradeEvent:     trisolaris.NewUpgradeEvent(),
		processInfoEvent: trisolaris.NewprocessInfoEvent(),
		pluginEvent:      trisolaris.NewPluginEvent(),
		prometheusEvent:  prometheus.NewSynchronizerEvent(),
	}
}

func (s *service) Register(gs *grpc.Server) error {
	api.RegisterSynchronizerServer(gs, s)
	return nil
}

func (s *service) Sync(ctx context.Context, in *api.SyncRequest) (*api.SyncResponse, error) {
	startTime := time.Now()
	defer func() {
		statsd.AddGrpcCostStatsd(statsd.Sync, int(time.Now().Sub(startTime).Milliseconds()))
	}()
	return s.vTapEvent.Sync(ctx, in)
}

func (s *service) Push(r *api.SyncRequest, in api.Synchronizer_PushServer) error {
	processName := r.GetProcessName()
	if strings.HasPrefix(processName, "trident") || strings.HasPrefix(processName, "deepflow-agent") {
		s.vTapEvent.Push(r, in)
	} else {
		s.tsdbEvent.Push(r, in)
	}
	return nil
}

func (s *service) AnalyzerSync(ctx context.Context, in *api.SyncRequest) (*api.SyncResponse, error) {
	startTime := time.Now()
	defer func() {
		statsd.AddGrpcCostStatsd(statsd.AnalyzerSync, int(time.Now().Sub(startTime).Milliseconds()))
	}()
	return s.tsdbEvent.AnalyzerSync(ctx, in)
}

func (s *service) Upgrade(r *api.UpgradeRequest, in api.Synchronizer_UpgradeServer) error {
	startTime := time.Now()
	defer func() {
		statsd.AddGrpcCostStatsd(statsd.Upgrade, int(time.Now().Sub(startTime).Milliseconds()))
	}()
	return s.upgradeEvent.Upgrade(r, in)
}

func (s *service) Query(ctx context.Context, in *api.NtpRequest) (*api.NtpResponse, error) {
	startTime := time.Now()
	defer func() {
		statsd.AddGrpcCostStatsd(statsd.Query, int(time.Now().Sub(startTime).Milliseconds()))
	}()
	return s.ntpEvent.Query(ctx, in)
}

func (s *service) GetKubernetesClusterID(ctx context.Context, in *api.KubernetesClusterIDRequest) (*api.KubernetesClusterIDResponse, error) {
	startTime := time.Now()
	defer func() {
		statsd.AddGrpcCostStatsd(statsd.GetKubernetesClusterID, int(time.Now().Sub(startTime).Milliseconds()))
	}()
	return s.kubernetesClusterIDEvent.GetKubernetesClusterID(ctx, in)
}

func (s *service) GPIDSync(ctx context.Context, in *api.GPIDSyncRequest) (*api.GPIDSyncResponse, error) {
	startTime := time.Now()
	defer func() {
		statsd.AddGrpcCostStatsd(statsd.GPIDSync, int(time.Now().Sub(startTime).Milliseconds()))
	}()
	return s.processInfoEvent.GPIDSync(ctx, in)
}

func (s *service) ShareGPIDLocalData(ctx context.Context, in *api.ShareGPIDSyncRequests) (*api.ShareGPIDSyncRequests, error) {
	return s.processInfoEvent.ShareGPIDLocalData(ctx, in)
}

func (s *service) GetPrometheusLabelIDs(ctx context.Context, in *api.PrometheusLabelRequest) (*api.PrometheusLabelResponse, error) {
	startTime := time.Now()
	defer func() {
		statsd.AddGrpcCostStatsd(statsd.GetPrometheusLabelIDs, int(time.Now().Sub(startTime).Milliseconds()))
	}()
	resp, err := s.prometheusEvent.GetLabelIDs(ctx, in)
	return resp, err
}

func (s *service) GetPrometheusTargets(ctx context.Context, in *api.PrometheusTargetRequest) (*api.PrometheusTargetResponse, error) {
	return &api.PrometheusTargetResponse{}, nil
	// startTime := time.Now()
	// defer func() {
	//	statsd.AddGrpcCostStatsd(statsd.GetPrometheusTargets, int(time.Now().Sub(startTime).Milliseconds()))
	// }()
	// resp, err := s.prometheusEvent.GetPrometheusTargets(ctx, in)
	// return resp, err
}

func (s *service) Plugin(r *api.PluginRequest, in api.Synchronizer_PluginServer) error {
	return s.pluginEvent.Plugin(r, in)
}

func (s *service) GetUniversalTagNameMaps(ctx context.Context, in *api.UniversalTagNameMapsRequest) (*api.UniversalTagNameMapsResponse, error) {
	return s.tsdbEvent.GetUniversalTagNameMaps(ctx, in)
}

func (s *service) RemoteExecute(in api.Synchronizer_RemoteExecuteServer) error {
	return s.vTapEvent.RemoteExecute(in)
}

func (s *service) GetOrgIDs(ctx context.Context, in *api.OrgIDsRequest) (*api.OrgIDsResponse, error) {
	return s.tsdbEvent.GetOrgIDs(ctx, in)
}
