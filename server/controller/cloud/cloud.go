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

package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bitly/go-simplejson"
	mapset "github.com/deckarep/golang-set"
	cmap "github.com/orcaman/concurrent-map/v2"
	"gorm.io/gorm"

	cloudcommon "github.com/deepflowio/deepflow/server/controller/cloud/common"
	"github.com/deepflowio/deepflow/server/controller/cloud/config"
	"github.com/deepflowio/deepflow/server/controller/cloud/model"
	"github.com/deepflowio/deepflow/server/controller/cloud/platform"
	"github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/db/metadb"
	metadbmodel "github.com/deepflowio/deepflow/server/controller/db/metadb/model"
	"github.com/deepflowio/deepflow/server/controller/genesis"
	"github.com/deepflowio/deepflow/server/controller/statsd"
	"github.com/deepflowio/deepflow/server/libs/logger"
	"github.com/deepflowio/deepflow/server/libs/queue"
)

var log = logger.MustGetLogger("cloud")

type Cloud struct {
	orgID                   int
	synchronizing           bool
	db                      *metadb.DB
	cfg                     config.CloudConfig
	cCtx                    context.Context
	cCancel                 context.CancelFunc
	mutex                   sync.RWMutex
	triggerTime             time.Time
	basicInfo               model.BasicInfo
	resource                model.Resource
	platform                platform.Platform
	taskCost                statsd.CloudTaskStatsd
	domainRefreshSignal     *queue.OverwriteQueue
	subDomainRefreshSignals cmap.ConcurrentMap[string, *queue.OverwriteQueue]
	kubernetesGatherTaskMap map[string]*KubernetesGatherTask
}

// TODO 添加参数
func NewCloud(orgID int, domain metadbmodel.Domain, cfg config.CloudConfig, ctx context.Context) *Cloud {
	mysqlDB, err := metadb.GetDB(orgID)
	if err != nil {
		log.Error(err, logger.NewORGPrefix(orgID))
		return nil
	}

	platform, err := platform.NewPlatform(domain, cfg, mysqlDB)
	if err != nil {
		log.Error(err, logger.NewORGPrefix(orgID))
		return nil
	}

	// maybe all types will be supported later
	var triggerTimeString string
	switch domain.Type {
	case common.QINGCLOUD, common.QINGCLOUD_PRIVATE:
		triggerTimeString = cfg.QingCloudConfig.DailyTriggerTime
	case common.FUSIONCOMPUTE:
		triggerTimeString = cfg.FusionComputeConfig.DailyTriggerTime
	default:
	}
	var triggerTime time.Time
	if triggerTimeString != "" {
		triggerTime, err = time.ParseInLocation("15:04", triggerTimeString, time.Local)
		if err != nil {
			log.Errorf("parse cloud (%s) daily trigger time config failed: (%s)", domain.Name, err.Error(), logger.NewORGPrefix(orgID))
		}
	}

	log.Infof("cloud task (%s) init success", domain.Name, logger.NewORGPrefix(orgID))

	cCtx, cCancel := context.WithCancel(ctx)
	return &Cloud{
		orgID:       orgID,
		db:          mysqlDB,
		triggerTime: triggerTime,
		basicInfo: model.BasicInfo{
			OrgID:     orgID,
			TeamID:    domain.TeamID,
			Lcuuid:    domain.Lcuuid,
			Name:      domain.Name,
			Type:      domain.Type,
			CreatedAt: domain.CreatedAt,
		},
		platform:                platform,
		kubernetesGatherTaskMap: make(map[string]*KubernetesGatherTask),
		cfg:                     cfg,
		cCtx:                    cCtx,
		cCancel:                 cCancel,
		resource: model.Resource{
			ErrorState: common.RESOURCE_STATE_CODE_SUCCESS,
		},
		taskCost: statsd.CloudTaskStatsd{
			TaskCost: make(map[string][]float64),
		},
		domainRefreshSignal:     queue.NewOverwriteQueue(fmt.Sprintf("cloud-task-%s", domain.Name), 1),
		subDomainRefreshSignals: cmap.New[*queue.OverwriteQueue](),
	}
}

func (c *Cloud) Start() {
	go c.run()
	go c.startKubernetesGatherTask()
}

func (c *Cloud) Stop() {
	c.platform.ClearDebugLog()
	c.domainRefreshSignal.Close()
	if c.cCancel != nil {
		c.cCancel()
	}
}

func (c *Cloud) UpdateBasicInfoName(name string) {
	c.basicInfo.Name = name
}

func (c *Cloud) GetOrgID() int {
	return c.orgID
}

func (c *Cloud) GetBasicInfo() model.BasicInfo {
	return c.basicInfo
}

func (c *Cloud) GetDomainRefreshSignal() *queue.OverwriteQueue {
	return c.domainRefreshSignal
}

func (c *Cloud) GetSubDomainRefreshSignals() cmap.ConcurrentMap[string, *queue.OverwriteQueue] {
	return c.subDomainRefreshSignals
}

func (c *Cloud) suffixResourceOperation(resource model.Resource) model.Resource {
	hostIPToHostName, vmLcuuidToHostName, err := cloudcommon.GetHostAndVmHostNameByDomain(c.basicInfo.Lcuuid, c.db.DB)
	if err != nil {
		log.Errorf("cloud suffix operation get vtap info error : (%s)", err.Error(), logger.NewORGPrefix(c.orgID))
		return resource
	}

	vinterfaceLcuuidToIP := map[string]string{}
	for _, ip := range resource.IPs {
		vinterfaceLcuuidToIP[ip.VInterfaceLcuuid] = ip.IP
	}
	vmIPToNetworkLcuuid := map[string]string{}
	vmLcuuidToIPs := map[string][]string{}
	for _, vinterface := range resource.VInterfaces {
		if vinterface.DeviceType != common.VIF_DEVICE_TYPE_VM {
			continue
		}
		ip, ok := vinterfaceLcuuidToIP[vinterface.Lcuuid]
		if !ok || ip == "" {
			continue
		}
		vmIPToNetworkLcuuid[vinterface.VPCLcuuid+ip] = vinterface.NetworkLcuuid
		vmLcuuidToIPs[vinterface.DeviceLcuuid] = append(vmLcuuidToIPs[vinterface.DeviceLcuuid], ip)
	}

	var retHosts []model.Host
	for _, host := range resource.Hosts {
		// add hostname to host
		if hostName, ok := hostIPToHostName[host.IP]; ok && host.Hostname == "" {
			host.Hostname = hostName
		}
		retHosts = append(retHosts, host)
	}

	// get hostname of the vm where node is located
	for _, sub := range resource.SubDomainResources {
		nodeLcuuidToHostname := map[string]string{}
		for _, node := range sub.PodNodes {
			nodeLcuuidToHostname[node.Lcuuid] = node.Hostname
		}
		for _, con := range sub.VMPodNodeConnections {
			vmHostname, ok := nodeLcuuidToHostname[con.PodNodeLcuuid]
			if !ok {
				continue
			}
			vmLcuuidToHostName[con.VMLcuuid] = vmHostname
		}
	}

	var retVMs []model.VM
	for _, vm := range resource.VMs {
		// return a default map, when not found cloud tags
		if vm.CloudTags == nil {
			vm.CloudTags = map[string]string{}
		}
		// select the first of the existing ips, when the ip is empty
		if vm.IP == "" {
			ips, ok := vmLcuuidToIPs[vm.Lcuuid]
			if ok && len(ips) > 0 {
				sort.Strings(ips)
				vm.IP = ips[0]
			}
		}
		// add network lcuuid to vm
		if vm.NetworkLcuuid == "" {
			networkLcuuid, ok := vmIPToNetworkLcuuid[vm.VPCLcuuid+vm.IP]
			if ok {
				vm.NetworkLcuuid = networkLcuuid
			}
		}
		// add hostname to vm
		if hostName, ok := vmLcuuidToHostName[vm.Lcuuid]; ok && vm.Hostname == "" {
			vm.Hostname = hostName
		}
		retVMs = append(retVMs, vm)
	}
	resource.Hosts = retHosts
	resource.VMs = retVMs
	return resource
}

func (c *Cloud) GetResource() model.Resource {
	cResource := c.resource
	if c.basicInfo.Type == common.KUBERNETES {
		cResource = c.getKubernetesData()
	}
	if !cResource.Verified {
		return model.Resource{
			ErrorState:   cResource.ErrorState,
			ErrorMessage: cResource.ErrorMessage,
		}
	}

	if c.basicInfo.Type != common.KUBERNETES {
		cResource.SubDomainResources = c.getSubDomainData(cResource)
		cResource = c.appendResourceVIPs(cResource)
	}

	cResource = c.appendAddtionalResourcesData(cResource)
	cResource = c.appendResourceProcess(cResource)
	// don't move c.suffixResourceOperation, it need to always hold the last position
	cResource = c.suffixResourceOperation(cResource)
	return cResource
}

func (c *Cloud) GetSubDomainResource(lcuuid string) model.Resource {
	sResource := model.Resource{}
	if c.basicInfo.Type == common.KUBERNETES {
		return sResource
	}
	cResource := c.resource
	if !cResource.Verified {
		cResource = c.getOwnDomainResource()
	}
	sResource.SubDomainResources = c.getSubDomainDataByLcuuid(lcuuid, cResource)
	sResource = c.appendResourceProcess(sResource)
	return sResource
}

func (c *Cloud) GetKubernetesGatherTaskMap() map[string]*KubernetesGatherTask {
	return c.kubernetesGatherTaskMap
}

func (c *Cloud) GetStatter() statsd.StatsdStatter {
	return statsd.StatsdStatter{
		OrgID:   c.orgID,
		TeamID:  c.basicInfo.TeamID,
		Element: statsd.GetCloudTaskStatsd(c.taskCost),
	}
}

func (c *Cloud) getCloudGatherInterval() int {
	if !c.triggerTime.IsZero() {
		log.Infof("cloud (%s) daily trigger time is (%s), sync timer is default (%d)s", c.basicInfo.Name, c.triggerTime.Format("15:04"), cloudcommon.CLOUD_SYNC_TIMER_DEFAULT, logger.NewORGPrefix(c.orgID))
		return cloudcommon.CLOUD_SYNC_TIMER_DEFAULT
	}
	var domain metadbmodel.Domain
	err := c.db.DB.Where("lcuuid = ?", c.basicInfo.Lcuuid).First(&domain).Error
	if err != nil {
		log.Warningf("get cloud gather interval failed: (%s)", err.Error(), logger.NewORGPrefix(c.orgID))
		return cloudcommon.CLOUD_SYNC_TIMER_DEFAULT
	}
	domainConfig, err := simplejson.NewJson([]byte(domain.Config))
	if err != nil {
		log.Warningf("parse domain (%s) config failed: (%s)", c.basicInfo.Name, err.Error(), logger.NewORGPrefix(c.orgID))
		return cloudcommon.CLOUD_SYNC_TIMER_DEFAULT
	}
	domainSyncTimer := domainConfig.Get("sync_timer").MustInt()
	if domainSyncTimer == 0 {
		return cloudcommon.CLOUD_SYNC_TIMER_DEFAULT
	}
	if domainSyncTimer < cloudcommon.CLOUD_SYNC_TIMER_MIN || domainSyncTimer > cloudcommon.CLOUD_SYNC_TIMER_MAX {
		log.Warningf("cloud sync timer invalid: (%d)", domainSyncTimer, logger.NewORGPrefix(c.orgID))
		return cloudcommon.CLOUD_SYNC_TIMER_DEFAULT
	}
	return domainSyncTimer
}

func (c *Cloud) getCloudData() {
	log.Infof("cloud (%s) assemble data starting", c.basicInfo.Name, logger.NewORGPrefix(c.orgID))

	c.synchronizing = true
	defer func() { c.synchronizing = false }()

	var cResource model.Resource
	var cloudCost float64
	if c.basicInfo.Type != common.KUBERNETES {
		var err error
		startTime := time.Now()
		cResource, err = c.platform.GetCloudData()
		cloudCost = time.Now().Sub(startTime).Seconds()
		// 这里因为任务内部没有对成功的状态赋值状态码，在这里统一处理了
		if err == nil {
			if cResource.ErrorState == 0 {
				cResource.Verified = true
				cResource.ErrorState = common.RESOURCE_STATE_CODE_SUCCESS
			}
			if len(cResource.VMs) == 0 && c.basicInfo.Type != common.FILEREADER {
				cResource = model.Resource{
					ErrorState:   common.RESOURCE_STATE_CODE_WARNING,
					ErrorMessage: "invalid vm count (0).",
				}
			}
		} else {
			if cResource.ErrorState == 0 {
				cResource.ErrorState = common.RESOURCE_STATE_CODE_EXCEPTION
			}
			cResource = model.Resource{
				ErrorState:   cResource.ErrorState,
				ErrorMessage: err.Error(),
			}
		}
		if cResource.Verified {
			cResource.SyncAt = time.Now()
			c.resource = cResource
			c.sendStatsd(cloudCost)
		} else {
			c.resource.ErrorState = cResource.ErrorState
			c.resource.ErrorMessage = cResource.ErrorMessage
			log.Warningf("get cloud (%s) data, verify is (false), error state (%d), error message (%s)", c.basicInfo.Name, cResource.ErrorState, cResource.ErrorMessage, logger.NewORGPrefix(c.orgID))
		}
	}
	// trigger recorder refresh domain resource
	c.domainRefreshSignal.Put(struct{}{})
	log.Infof("cloud (%s) assemble data complete", c.basicInfo.Name, logger.NewORGPrefix(c.orgID))
}

func (c *Cloud) sendStatsd(cloudCost float64) {
	c.taskCost.TaskCost = map[string][]float64{
		c.basicInfo.Lcuuid: []float64{cloudCost},
	}
	statsd.MetaStatsd.RegisterStatsdTable(c)
}

func (c *Cloud) ClientTrigger() error {
	if c.synchronizing {
		return fmt.Errorf("cloud (%s) is synchronizing, please try again later", c.basicInfo.Name)
	}
	go c.getCloudData()
	return nil
}

func (c *Cloud) dailyTrigger() bool {
	if c.triggerTime.IsZero() {
		return true
	}

	now := time.Now()
	dailyTime := time.Date(now.Year(), now.Month(), now.Day(), c.triggerTime.Hour(), c.triggerTime.Minute(), 0, 0, time.Local)
	timeSub := now.Sub(dailyTime)
	if timeSub >= 0 && timeSub <= time.Minute {
		return true
	}
	log.Infof("now is not trigger time (%s), task (%s) not running", dailyTime.Format(common.GO_BIRTHDAY), c.basicInfo.Name, logger.NewORGPrefix(c.orgID))
	return false
}

func (c *Cloud) run() {
	log.Infof("cloud (%s) started", c.basicInfo.Name, logger.NewORGPrefix(c.orgID))

	if err := c.platform.CheckAuth(); err != nil {
		log.Errorf("cloud (%+v) check auth failed", c.basicInfo, logger.NewORGPrefix(c.orgID))
	}

	// execute immediately upon startup
	if c.dailyTrigger() {
		c.getCloudData()
	}

	cloudGatherInterval := c.getCloudGatherInterval()
	c.basicInfo.Interval = cloudGatherInterval
	ticker := time.NewTicker(time.Second * time.Duration(cloudGatherInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.synchronizing {
				continue
			}
			if !c.dailyTrigger() {
				continue
			}
			c.getCloudData()
		case <-c.cCtx.Done():
			log.Infof("cloud (%s) stopped", c.basicInfo.Name, logger.NewORGPrefix(c.orgID))
			return
		}
	}
}

func (c *Cloud) startKubernetesGatherTask() {
	log.Infof("cloud (%s) kubernetes gather task started", c.basicInfo.Name, logger.NewORGPrefix(c.orgID))
	c.runKubernetesGatherTask()
	go func() {
		for range time.Tick(time.Duration(c.cfg.KubernetesGatherInterval) * time.Second) {
			c.runKubernetesGatherTask()
		}
	}()
}

func (c *Cloud) runKubernetesGatherTask() {
	var domain metadbmodel.Domain
	err := c.db.DB.Where("lcuuid = ?", c.basicInfo.Lcuuid).First(&domain).Error
	if err != nil {
		log.Error(err, logger.NewORGPrefix(c.orgID))
		return
	}

	if c.basicInfo.Type == common.KUBERNETES {
		// Kubernetes平台，只会有一个KubernetesGatherTask
		// - 如果已存在KubernetesGatherTask，则无需启动新的Task
		// Kubernetes平台，无需考虑KubernetesGatherTask的更新/删除，会在Cloud层面统一处理
		if len(c.kubernetesGatherTaskMap) != 0 {
			return
		}
		kubernetesGatherTask := NewKubernetesGatherTask(c.cCtx, c.db, &domain, nil, c.cfg, false)
		if kubernetesGatherTask == nil {
			return
		}
		c.mutex.Lock()
		c.kubernetesGatherTaskMap[domain.Lcuuid] = kubernetesGatherTask
		c.kubernetesGatherTaskMap[domain.Lcuuid].Start(c.domainRefreshSignal)
		c.mutex.Unlock()

	} else {
		// 附属容器集群的处理
		var subDomains []metadbmodel.SubDomain
		var oldSubDomains = mapset.NewSet()
		var newSubDomains = mapset.NewSet()
		var delSubDomains = mapset.NewSet()
		var addSubDomains = mapset.NewSet()
		var intersectSubDomains = mapset.NewSet()

		for lcuuid := range c.kubernetesGatherTaskMap {
			oldSubDomains.Add(lcuuid)
		}

		c.db.DB.Where("domain = ?", c.basicInfo.Lcuuid).Find(&subDomains)
		lcuuidToSubDomain := make(map[string]*metadbmodel.SubDomain)
		for index, subDomain := range subDomains {
			lcuuidToSubDomain[subDomain.Lcuuid] = &subDomains[index]
			newSubDomains.Add(subDomain.Lcuuid)
		}

		// 对于删除的subDomain，停止Task，并移除管理
		delSubDomains = oldSubDomains.Difference(newSubDomains)
		for _, subDomain := range delSubDomains.ToSlice() {
			lcuuid := subDomain.(string)
			c.kubernetesGatherTaskMap[lcuuid].Stop()
			c.mutex.Lock()
			delete(c.kubernetesGatherTaskMap, lcuuid)
			c.mutex.Unlock()
			kGatherQueue, ok := c.subDomainRefreshSignals.Get(lcuuid)
			if ok {
				kGatherQueue.Close()
				c.subDomainRefreshSignals.Remove(lcuuid)
			}
		}

		// 对于新增的subDomain，启动Task，并纳入Manger管理
		addSubDomains = newSubDomains.Difference(oldSubDomains)
		for _, subDomain := range addSubDomains.ToSlice() {
			lcuuid := subDomain.(string)
			kubernetesGatherTask := NewKubernetesGatherTask(c.cCtx, c.db, &domain, lcuuidToSubDomain[lcuuid], c.cfg, true)
			if kubernetesGatherTask == nil {
				continue
			}

			gatherQueue := queue.NewOverwriteQueue(fmt.Sprintf("sub-domain-task-%s", lcuuid), 1)
			c.subDomainRefreshSignals.SetIfAbsent(lcuuid, gatherQueue)
			c.mutex.Lock()
			c.kubernetesGatherTaskMap[lcuuid] = kubernetesGatherTask
			c.kubernetesGatherTaskMap[lcuuid].Start(gatherQueue)
			c.mutex.Unlock()
		}

		// 检查已有subDomain是否存在配置修改
		// 如果存在配置修改，则停止已有Task，并移除管理；同时启动新的Task，并纳入Cloud管理
		intersectSubDomains = newSubDomains.Intersect(oldSubDomains)
		for _, subDomain := range intersectSubDomains.ToSlice() {
			lcuuid := subDomain.(string)
			oldSubDomain := c.kubernetesGatherTaskMap[lcuuid]
			newSubDomain := lcuuidToSubDomain[lcuuid]
			if oldSubDomain.SubDomainConfig != newSubDomain.Config || oldSubDomain.kubernetesGather.Name != newSubDomain.Name || oldSubDomain.kubernetesGather.TeamID != newSubDomain.TeamID {
				log.Infof("oldSubDomainConfig: %s", oldSubDomain.SubDomainConfig, logger.NewORGPrefix(c.orgID))
				log.Infof("newSubDomainConfig: %s", newSubDomain.Config, logger.NewORGPrefix(c.orgID))
				c.kubernetesGatherTaskMap[lcuuid].Stop()
				kubernetesGatherTask := NewKubernetesGatherTask(c.cCtx, c.db, &domain, lcuuidToSubDomain[lcuuid], c.cfg, true)
				if kubernetesGatherTask == nil {
					continue
				}

				gatherQueue, ok := c.subDomainRefreshSignals.Get(lcuuid)
				if !ok {
					gatherQueue = queue.NewOverwriteQueue(fmt.Sprintf("sub-domain-task-%s", lcuuid), 1)
					c.subDomainRefreshSignals.Set(lcuuid, gatherQueue)
				}
				c.mutex.Lock()
				delete(c.kubernetesGatherTaskMap, lcuuid)
				c.kubernetesGatherTaskMap[lcuuid] = kubernetesGatherTask
				c.kubernetesGatherTaskMap[lcuuid].Start(gatherQueue)
				c.mutex.Unlock()
			}
		}
	}
}

func (c *Cloud) appendAddtionalResourcesData(resource model.Resource) model.Resource {
	dbItem, err := getContentFromAdditionalResource(c.basicInfo.Lcuuid, c.db.DB)
	if err != nil {
		log.Errorf("domain (lcuuid: %s) get additional resources failed: %s", c.basicInfo.Lcuuid, err, logger.NewORGPrefix(c.orgID))
		return resource
	}
	if dbItem == nil {
		log.Debugf("domain (lcuuid: %s) has no additional resources to append", c.basicInfo.Lcuuid, logger.NewORGPrefix(c.orgID))
		return resource
	}

	content := dbItem.CompressedContent
	if len(dbItem.CompressedContent) == 0 {
		content = []byte(dbItem.Content)
	}
	var additionalResource model.AdditionalResource
	if err = json.Unmarshal(content, &additionalResource); err != nil {
		log.Errorf("domain (lcuuid: %s) json unmarshal content failed: %s", c.basicInfo.Lcuuid, err.Error(), logger.NewORGPrefix(c.orgID))
		return resource
	}

	resource.AZs = append(resource.AZs, additionalResource.AZs...)
	resource.VPCs = append(resource.VPCs, additionalResource.VPCs...)
	resource.Networks = append(resource.Networks, additionalResource.Subnets...)
	resource.Subnets = append(resource.Subnets, additionalResource.SubnetCIDRs...)
	resource.Hosts = append(resource.Hosts, additionalResource.Hosts...)
	resource.VMs = append(resource.VMs, additionalResource.CHosts...)
	resource.VInterfaces = append(resource.VInterfaces, additionalResource.VInterfaces...)
	resource.IPs = append(resource.IPs, additionalResource.IPs...)
	resource = c.appendCloudTags(resource, additionalResource)
	resource.LBs = append(resource.LBs, additionalResource.LB...)
	resource.LBListeners = append(resource.LBListeners, additionalResource.LBListeners...)
	resource.LBTargetServers = append(resource.LBTargetServers, additionalResource.LBTargetServers...)
	resource.PeerConnections = append(resource.PeerConnections, additionalResource.PeerConnections...)
	return resource
}

// getContentFromAdditionalResource gets domain_additional_resource by uuid, get the field content if it exists,
// otherwise get the field compressed_content.
// old content field: content
// new centent field: compressed_content
func getContentFromAdditionalResource(domainUUID string, db *gorm.DB) (*metadbmodel.DomainAdditionalResource, error) {
	var dbItem metadbmodel.DomainAdditionalResource
	result := db.Select("content").Where("domain = ? and content!=''", domainUUID).First(&dbItem)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		result = db.Select("compressed_content").Where("domain = ?", domainUUID).First(&dbItem)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
	}

	if result.RowsAffected != 0 {
		return &dbItem, nil
	}
	if result.Error != nil {
		return nil, result.Error
	}
	return &dbItem, nil
}

func (c *Cloud) appendCloudTags(resource model.Resource, additionalResource model.AdditionalResource) model.Resource {
	chostCloudTags := additionalResource.CHostCloudTags
	for i, chost := range resource.VMs {
		value, ok := chostCloudTags[chost.Lcuuid]
		if !ok {
			continue
		}
		if len(chost.CloudTags) != 0 {
			log.Infof("vm (%s) already tags (%v), do not need to add (%v)", chost.Name, chost.CloudTags, value, logger.NewORGPrefix(c.orgID))
			continue
		}
		resource.VMs[i].CloudTags = value
	}
	podNamespaceCloudTags := additionalResource.PodNamespaceCloudTags
	for i, podNamespace := range resource.PodNamespaces {
		if value, ok := podNamespaceCloudTags[podNamespace.Lcuuid]; ok {
			resource.PodNamespaces[i].CloudTags = value
		}
	}

	additionalSubdomainResources := additionalResource.SubDomainResources
	for subdomainUUID, subdomainResource := range resource.SubDomainResources {
		additionalSubdomainResource, ok := additionalSubdomainResources[subdomainUUID]
		if !ok {
			continue
		}
		for i, podNamespace := range subdomainResource.PodNamespaces {
			if additionalSubdomainResource.PodNamespaceCloudTags != nil {
				if value, ok := additionalSubdomainResource.PodNamespaceCloudTags[podNamespace.Lcuuid]; ok {
					subdomainResource.PodNamespaces[i].CloudTags = value
				}
			}
		}
	}
	return resource
}

func (c *Cloud) appendResourceProcess(resource model.Resource) model.Resource {

	if genesis.GenesisService == nil {
		log.Error("genesis service is nil", logger.NewORGPrefix(c.orgID))
		return resource
	}

	genesisSyncData, err := genesis.GenesisService.GetGenesisSyncResponse(c.orgID)
	if err != nil {
		log.Error(err.Error(), logger.NewORGPrefix(c.orgID))
		return resource
	}

	vtapIDToLcuuid, err := cloudcommon.GetVTapSubDomainMappingByDomain(c.basicInfo.Lcuuid, c.db.DB)
	if err != nil {
		log.Errorf("domain (%s) add process failed: %s", c.basicInfo.Name, err.Error(), logger.NewORGPrefix(c.orgID))
		return resource
	}

	for _, sProcess := range genesisSyncData.Processes {
		lcuuid, ok := vtapIDToLcuuid[int(sProcess.VtapID)]
		if !ok {
			continue
		}
		name, processName := sProcess.Name, sProcess.ProcessName
		if len(sProcess.Name) > c.cfg.ProcessNameLenMax {
			name = sProcess.Name[:c.cfg.ProcessNameLenMax]
		}
		if len(sProcess.ProcessName) > c.cfg.ProcessNameLenMax {
			processName = sProcess.ProcessName[:c.cfg.ProcessNameLenMax]
		}
		process := model.Process{
			Lcuuid:      sProcess.Lcuuid,
			Name:        name,
			VTapID:      sProcess.VtapID,
			PID:         sProcess.PID,
			NetnsID:     sProcess.NetnsID,
			ProcessName: processName,
			CommandLine: sProcess.CMDLine,
			UserName:    sProcess.UserName,
			ContainerID: sProcess.ContainerID,
			StartTime:   sProcess.StartTime,
			OSAPPTags:   sProcess.OSAPPTags,
		}
		if resource.Verified && lcuuid == "" {
			resource.Processes = append(resource.Processes, process)
			continue
		}
		subDomainResource, ok := resource.SubDomainResources[lcuuid]
		if !ok || !subDomainResource.Verified {
			continue
		}
		process.SubDomainLcuuid = lcuuid
		subDomainResource.Processes = append(subDomainResource.Processes, process)
		resource.SubDomainResources[lcuuid] = subDomainResource
	}
	return resource
}

func (c *Cloud) appendResourceVIPs(resource model.Resource) model.Resource {

	if genesis.GenesisService == nil {
		log.Error("genesis service is nil", logger.NewORGPrefix(c.orgID))
		return resource
	}

	genesisSyncData, err := genesis.GenesisService.GetGenesisSyncResponse(c.orgID)
	if err != nil {
		log.Error(err.Error(), logger.NewORGPrefix(c.orgID))
		return resource
	}

	vtapIDToLcuuid, err := cloudcommon.GetVTapSubDomainMappingByDomain(c.basicInfo.Lcuuid, c.db.DB)
	if err != nil {
		log.Errorf("domain (%s) add vip failed: %s", c.basicInfo.Name, err.Error(), logger.NewORGPrefix(c.orgID))
		return resource
	}

	for _, vip := range genesisSyncData.VIPs {
		lcuuid, ok := vtapIDToLcuuid[int(vip.VtapID)]
		if !ok || lcuuid != "" {
			continue
		}
		resource.VIPs = append(resource.VIPs, model.VIP{
			Lcuuid: vip.Lcuuid,
			IP:     vip.IP,
			VTapID: vip.VtapID,
		})
	}
	return resource
}
