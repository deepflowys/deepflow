/**
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

package recorder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/op/go-logging"

	cloudmodel "github.com/deepflowio/deepflow/server/controller/cloud/model"
	"github.com/deepflowio/deepflow/server/controller/common"
	metadbmodel "github.com/deepflowio/deepflow/server/controller/db/metadb/model"
	"github.com/deepflowio/deepflow/server/controller/recorder/cache"
	rcommon "github.com/deepflowio/deepflow/server/controller/recorder/common"
	"github.com/deepflowio/deepflow/server/controller/recorder/config"
	"github.com/deepflowio/deepflow/server/controller/recorder/listener"
	"github.com/deepflowio/deepflow/server/controller/recorder/statsd"
	"github.com/deepflowio/deepflow/server/controller/recorder/updater"
	"github.com/deepflowio/deepflow/server/controller/trisolaris/refresh"
	"github.com/deepflowio/deepflow/server/libs/queue"
)

const (
	RefreshTargetDomain = "domain"
	// RefreshTargetDomainExcludeSubDomain
	RefreshTargetSubDomain = "sub_domain"
)

type domain struct {
	metadata *rcommon.Metadata
	statsd   *statsd.DomainStatsd

	eventQueue *queue.OverwriteQueue
	cache      *cache.Cache
	subDomains *subDomains
}

func newDomain(ctx context.Context, cfg config.RecorderConfig, eventQueue *queue.OverwriteQueue, md *rcommon.Metadata) *domain {
	cacheMng := cache.NewCacheManager(ctx, cfg, md)
	return &domain{
		metadata: md,
		statsd:   statsd.NewDomainStatsd(md),

		eventQueue: eventQueue,
		cache:      cacheMng.DomainCache,
		subDomains: newSubDomains(ctx, cfg, eventQueue, md, cacheMng),
	}
}

func (d *domain) CloseStatsd() {
	d.statsd.Close()
	d.subDomains.CloseStatsd()
}

func (d *domain) Refresh(target string, cloudData cloudmodel.Resource) error {
	log.Infof("refresh target: %s", target, d.metadata.LogPrefixes)
	switch target {
	case RefreshTargetDomain:
		log.Info("refresher started, triggered by ticker/hand", d.metadata.LogPrefixes)
		if err := d.refreshDomainExcludeSubDomain(cloudData); err != nil {
			return err
		}
		return d.subDomains.RefreshAll(cloudData.SubDomainResources)
	case RefreshTargetSubDomain:
		log.Info("refresher started, triggered by hand", d.metadata.LogPrefixes)
		return d.subDomains.RefreshOne(cloudData.SubDomainResources)
	default:
		log.Info("invalid refresh target", d.metadata.LogPrefixes)
		return errors.New("invalid refresh target")
	}
}

func (d *domain) refreshDomainExcludeSubDomain(cloudData cloudmodel.Resource) error {
	return d.tryRefresh(cloudData)
}

func (d *domain) tryRefresh(cloudData cloudmodel.Resource) error {
	// 无论是否会更新资源，需先更新domain及subdomain状态
	d.updateStateInfo(cloudData)

	if err := d.shouldRefresh(cloudData); err != nil {
		return err
	}

	select {
	case <-d.cache.RefreshSignal:
		d.cache.IncrementSequence()
		d.cache.SetLogLevel(logging.INFO, cache.RefreshSignalCallerDomain)

		d.refresh(cloudData)

		d.cache.ResetRefreshSignal(cache.RefreshSignalCallerDomain)
		return nil
	default:
		log.Info("domain refresh is running, does nothing", d.metadata.LogPrefixes)
		return RefreshConflictError
	}
}

func (d *domain) shouldRefresh(cloudData cloudmodel.Resource) error {
	if cloudData.Verified {
		if ((d.metadata.Domain.Type != common.CLOUD_TOWER && d.metadata.Domain.Type != common.FUSIONCOMPUTE) && len(cloudData.Networks) == 0) || len(cloudData.VInterfaces) == 0 {
			log.Info("domain has no networks or vinterfaces, does nothing", d.metadata.LogPrefixes)
			return DataMissingError
		}
		if len(cloudData.VMs) == 0 && len(cloudData.Pods) == 0 {
			log.Info("domain has no vms and pods, does nothing", d.metadata.LogPrefixes)
			return DataMissingError
		}
	} else {
		log.Info("domain is not verified, does nothing", d.metadata.LogPrefixes)
		return DataNotVerifiedError
	}
	return nil
}

func (d *domain) refresh(cloudData cloudmodel.Resource) {
	log.Info("domain refresh started", d.metadata.LogPrefixes)

	// 指定创建及更新操作的资源顺序
	// 基本原则：无依赖资源优先；实时性需求高资源优先
	listener := listener.NewWholeDomain(d.metadata.Domain.Lcuuid, d.cache, d.eventQueue)
	domainUpdatersInUpdateOrder := d.getUpdatersInOrder(cloudData)
	d.executeUpdaters(domainUpdatersInUpdateOrder)
	d.notifyOnResourceChanged(domainUpdatersInUpdateOrder)
	listener.OnUpdatersCompleted()

	d.updateSyncedAt(cloudData.SyncAt)

	log.Info("domain refresh completed", d.metadata.LogPrefixes)
}

func (d *domain) getUpdatersInOrder(cloudData cloudmodel.Resource) []updater.ResourceUpdater {
	ip := updater.NewIP(d.cache, cloudData.IPs, nil)
	ip.GetLANIP().RegisterListener(listener.NewLANIP(d.cache, d.eventQueue))
	ip.GetWANIP().RegisterListener(listener.NewWANIP(d.cache, d.eventQueue))

	return []updater.ResourceUpdater{
		updater.NewRegion(d.cache, cloudData.Regions).RegisterListener(
			listener.NewRegion(d.cache)),
		updater.NewAZ(d.cache, cloudData.AZs).RegisterListener(
			listener.NewAZ(d.cache)),
		updater.NewSubDomain(d.cache, cloudData.SubDomains).RegisterListener(
			listener.NewSubDomain(d.cache)),
		updater.NewVPC(d.cache, cloudData.VPCs).RegisterListener(
			listener.NewVPC(d.cache)),
		updater.NewHost(d.cache, cloudData.Hosts).RegisterListener(
			listener.NewHost(d.cache, d.eventQueue)),
		updater.NewVM(d.cache, cloudData.VMs).RegisterListener(
			listener.NewVM(d.cache, d.eventQueue)).BuildStatsd(d.statsd),
		updater.NewPodCluster(d.cache, cloudData.PodClusters).RegisterListener(
			listener.NewPodCluster(d.cache)),
		updater.NewPodNode(d.cache, cloudData.PodNodes).RegisterListener(
			listener.NewPodNode(d.cache, d.eventQueue)),
		updater.NewPodNamespace(d.cache, cloudData.PodNamespaces).RegisterListener(
			listener.NewPodNamespace(d.cache)),
		updater.NewPodIngress(d.cache, cloudData.PodIngresses).RegisterListener(
			listener.NewPodIngress(d.cache)),
		updater.NewPodIngressRule(d.cache, cloudData.PodIngressRules).RegisterListener(
			listener.NewPodIngressRule(d.cache)),
		updater.NewPodService(d.cache, cloudData.PodServices).RegisterListener(
			listener.NewPodService(d.cache, d.eventQueue)),
		updater.NewPodIngressRuleBackend(d.cache, cloudData.PodIngressRuleBackends).RegisterListener(
			listener.NewPodIngressRuleBackend(d.cache)),
		updater.NewPodServicePort(d.cache, cloudData.PodServicePorts).RegisterListener(
			listener.NewPodServicePort(d.cache)),
		updater.NewPodGroup(d.cache, cloudData.PodGroups).RegisterListener(
			listener.NewPodGroup(d.cache)),
		updater.NewPodGroupPort(d.cache, cloudData.PodGroupPorts).RegisterListener(
			listener.NewPodGroupPort(d.cache)),
		updater.NewPodReplicaSet(d.cache, cloudData.PodReplicaSets).RegisterListener(
			listener.NewPodReplicaSet(d.cache)),
		updater.NewPod(d.cache, cloudData.Pods).RegisterListener(
			listener.NewPod(d.cache, d.eventQueue)).BuildStatsd(d.statsd),
		updater.NewNetwork(d.cache, cloudData.Networks).RegisterListener(
			listener.NewNetwork(d.cache)),
		updater.NewSubnet(d.cache, cloudData.Subnets).RegisterListener(
			listener.NewSubnet(d.cache)),
		updater.NewVRouter(d.cache, cloudData.VRouters).RegisterListener(
			listener.NewVRouter(d.cache, d.eventQueue)),
		updater.NewRoutingTable(d.cache, cloudData.RoutingTables).RegisterListener(
			listener.NewRoutingTable(d.cache)),
		updater.NewDHCPPort(d.cache, cloudData.DHCPPorts).RegisterListener(
			listener.NewDHCPPort(d.cache, d.eventQueue)),
		updater.NewNATGateway(d.cache, cloudData.NATGateways).RegisterListener(
			listener.NewNATGateway(d.cache, d.eventQueue)),
		updater.NewNATVMConnection(d.cache, cloudData.NATVMConnections).RegisterListener(
			listener.NewNATVMConnection(d.cache)),
		updater.NewNATRule(d.cache, cloudData.NATRules).RegisterListener(
			listener.NewNATRule(d.cache)),
		updater.NewLB(d.cache, cloudData.LBs).RegisterListener(
			listener.NewLB(d.cache, d.eventQueue)),
		updater.NewLBVMConnection(d.cache, cloudData.LBVMConnections).RegisterListener(
			listener.NewLBVMConnection(d.cache)),
		updater.NewLBListener(d.cache, cloudData.LBListeners).RegisterListener(
			listener.NewLBListener(d.cache)),
		updater.NewLBTargetServer(d.cache, cloudData.LBTargetServers).RegisterListener(
			listener.NewLBTargetServer(d.cache)),
		updater.NewRDSInstance(d.cache, cloudData.RDSInstances).RegisterListener(
			listener.NewRDSInstance(d.cache, d.eventQueue)),
		updater.NewRedisInstance(d.cache, cloudData.RedisInstances).RegisterListener(
			listener.NewRedisInstance(d.cache, d.eventQueue)),
		updater.NewPeerConnection(d.cache, cloudData.PeerConnections).RegisterListener(
			listener.NewPeerConnection(d.cache)),
		updater.NewCEN(d.cache, cloudData.CENs).RegisterListener(
			listener.NewCEN(d.cache)),
		updater.NewVInterface(d.cache, cloudData.VInterfaces, nil).RegisterListener(
			listener.NewVInterface(d.cache)),
		updater.NewFloatingIP(d.cache, cloudData.FloatingIPs).RegisterListener(
			listener.NewFloatingIP(d.cache)),
		ip,
		updater.NewVIP(d.cache, cloudData.VIPs).RegisterListener(
			listener.NewVIP(d.cache)),
		updater.NewVMPodNodeConnection(d.cache, cloudData.VMPodNodeConnections).RegisterListener( // VMPodNodeConnection需放在最后
			listener.NewVMPodNodeConnection(d.cache)),
		updater.NewProcess(d.cache, cloudData.Processes).RegisterListener(
			listener.NewProcess(d.cache, d.eventQueue)),
	}
}

// TODO common
func (r *domain) executeUpdaters(updatersInUpdateOrder []updater.ResourceUpdater) {
	for _, updater := range updatersInUpdateOrder {
		updater.HandleAddAndUpdate()
	}

	// 删除操作的顺序，是创建的逆序
	// 特殊资源：VMPodNodeConnection虽然是末序创建，但需要末序删除，序号-1；
	// 原因：避免数据量大时，此数据删除后，云主机、容器节点还在，导致采集器类型变化
	processUpdater := updatersInUpdateOrder[len(updatersInUpdateOrder)-1]
	vmPodNodeConnectionUpdater := updatersInUpdateOrder[len(updatersInUpdateOrder)-2]
	// 因为 processUpdater 是 -1，VMPodNodeConnection 是 -2，特殊处理后，逆序删除从 -3 开始
	for i := len(updatersInUpdateOrder) - 3; i >= 0; i-- {
		updatersInUpdateOrder[i].HandleDelete()
	}
	processUpdater.HandleDelete()
	vmPodNodeConnectionUpdater.HandleDelete()
}

// TODO common
func (d *domain) notifyOnResourceChanged(updatersInUpdateOrder []updater.ResourceUpdater) {
	platformDataChanged := isPlatformDataChanged(updatersInUpdateOrder)
	if platformDataChanged {
		log.Info("domain data changed, refresh platform data", d.metadata.LogPrefixes)
		refresh.RefreshCache(d.metadata.GetORGID(), []common.DataChanged{common.DATA_CHANGED_PLATFORM_DATA})
	}
}

func (d *domain) updateSyncedAt(syncAt time.Time) {
	if syncAt.IsZero() {
		return
	}

	log.Infof("update domain synced_at: %s", syncAt.Format(common.GO_BIRTHDAY), d.metadata.LogPrefixes)
	d.fillStatsd(syncAt)

	var domain metadbmodel.Domain
	err := d.metadata.DB.Where("lcuuid = ?", d.metadata.Domain.Lcuuid).First(&domain).Error
	if err != nil {
		log.Errorf("get domain from db failed: %s", err, d.metadata.LogPrefixes)
		return
	}
	domain.SyncedAt = &syncAt
	d.metadata.DB.Save(&domain)
	log.Debugf("update domain (%+v)", domain, d.metadata.LogPrefixes)
}

func (d *domain) fillStatsd(syncAt time.Time) {
	log.Infof("time now: %s", time.Now().Format(common.GO_BIRTHDAY), d.metadata.LogPrefixes)
	cost := time.Since(syncAt).Seconds()
	d.statsd.GetMonitor(statsd.TagTypeSyncCost).Fill(int(cost))
}

func (d *domain) updateStateInfo(cloudData cloudmodel.Resource) {
	var domain metadbmodel.Domain
	err := d.metadata.DB.Where("lcuuid = ?", d.metadata.Domain.Lcuuid).First(&domain).Error
	if err != nil {
		log.Errorf("get domain from db failed: %s", err, d.metadata.LogPrefixes)
		return
	}
	domain.State, domain.ErrorMsg = d.formatStateInfo(cloudData)
	d.metadata.DB.Save(&domain)
	log.Debugf("update domain (%+v)", domain, d.metadata.LogPrefixes)

	for subDomainLcuuid, subDomainResource := range cloudData.SubDomainResources {
		var subDomain metadbmodel.SubDomain
		err := d.metadata.DB.Where("lcuuid = ?", subDomainLcuuid).First(&subDomain).Error
		if err != nil {
			log.Errorf("get sub_domain (lcuuid: %s) from db failed: %s", subDomainLcuuid, err, d.metadata.LogPrefixes)
			continue
		}
		subDomain.State = subDomainResource.ErrorState
		subDomain.ErrorMsg = subDomainResource.ErrorMessage
		d.metadata.DB.Save(&subDomain)
		log.Debugf("update sub_domain (%+v)", subDomain, d.metadata.LogPrefixes)
	}
}

func (d *domain) formatStateInfo(domainResource cloudmodel.Resource) (state int, errMsg string) {
	log.Infof("cloud state info: %d, %s", domainResource.ErrorState, domainResource.ErrorMessage, d.metadata.LogPrefixes)
	// 状态优先级 exception > warning > sunccess
	stateToLevel := map[int]int{
		common.RESOURCE_STATE_CODE_SUCCESS:   1,
		common.RESOURCE_STATE_CODE_WARNING:   2,
		common.RESOURCE_STATE_CODE_EXCEPTION: 3,
	}

	// domain的状态：取云平台自身状态 + 附属容器集群状态中，优先级最高的状态
	// domain的异常信息：取云平台自身异常 + 最多10个附属容器集群异常，剩余附属容器集群异常省略
	state = domainResource.ErrorState
	errMsg = domainResource.ErrorMessage

	var subDomainErrMsgs []string
	for subDomainLcuuid, subDomainResource := range domainResource.SubDomainResources {
		log.Infof("cloud sub_domain (%s) state info: %d, %s", subDomainLcuuid, subDomainResource.ErrorState, subDomainResource.ErrorMessage, d.metadata.LogPrefixes)
		if stateToLevel[subDomainResource.ErrorState] > stateToLevel[state] {
			state = subDomainResource.ErrorState
		}
		if subDomainResource.ErrorMessage != "" {
			subDomainErrMsgs = append(subDomainErrMsgs, subDomainResource.ErrorMessage)
		}
	}
	subDomainErrNum := len(subDomainErrMsgs)
	if subDomainErrNum != 0 {
		if errMsg != "" {
			errMsg += "\n\n"
		}
		errMsg += fmt.Sprintf("共有%d个附属容器集群存在异常\n", subDomainErrNum)

		var subDomainErrMsgsString string
		if subDomainErrNum > common.SUB_DOMAIN_ERROR_DISPLAY_NUM {
			subDomainErrMsgsString = strings.Join(subDomainErrMsgs[:common.SUB_DOMAIN_ERROR_DISPLAY_NUM], "\n")
			subDomainErrMsgsString += "\n..."
		} else {
			subDomainErrMsgsString = strings.Join(subDomainErrMsgs, "\n")
		}
		errMsg += subDomainErrMsgsString
	}
	return
}

var changeSensitiveResourceTypes = []string{
	common.RESOURCE_TYPE_REGION_EN, common.RESOURCE_TYPE_AZ_EN, common.RESOURCE_TYPE_HOST_EN, common.RESOURCE_TYPE_VM_EN, common.RESOURCE_TYPE_VINTERFACE_EN,
	common.RESOURCE_TYPE_VROUTER_EN, common.RESOURCE_TYPE_NETWORK_EN, common.RESOURCE_TYPE_PEER_CONNECTION_EN,
	common.RESOURCE_TYPE_POD_EN, common.RESOURCE_TYPE_POD_NODE_EN, common.RESOURCE_TYPE_PROCESS_EN,
}

func isPlatformDataChanged(updaters []updater.ResourceUpdater) bool {
	changed := false
	for _, updater := range updaters {
		if common.Contains(changeSensitiveResourceTypes, updater.GetResourceType()) {
			changed = changed || updater.GetChanged()
		}
	}
	return changed
}
