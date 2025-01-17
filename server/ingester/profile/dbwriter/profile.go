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

package dbwriter

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/google/gopacket/layers"

	basecommon "github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/ingester/flow_tag"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/grpc"
	"github.com/deepflowio/deepflow/server/libs/nativetag"
	"github.com/deepflowio/deepflow/server/libs/pool"
	"github.com/deepflowio/deepflow/server/libs/utils"
)

const (
	DefaultPartition  = ckdb.TimeFuncHour
	LabelTraceID      = "trace_id"
	LabelSpanName     = "span_name"
	LabelAppService   = "app_service"
	LabelAppInstance  = "app_instance"
	LabelLanguageType = "profile_language_type"
	LabelProfileID    = "profile_id"
)

var InProcessCounter uint32

type InProcessProfile struct {
	_id  uint64
	Time uint32

	// Profile
	AppService         string `json:"app_service"`
	ProfileLocationStr string `json:"profile_location_str"` // package/(class/struct)/function name, e.g.: java/lang/Thread.run
	ProfileValue       int64  `json:"profile_value"`
	// profile_event_type 的取值与 profile_value_unit 对应关系见下
	// profile_event_type: relations between profile_event_type and profile_value_unit is under the struct definition
	ProfileEventType       string   `json:"profile_event_type"` // event_type, e.g.: cpu/itimer...
	ProfileValueUnit       string   `json:"profile_value_unit"`
	ProfileCreateTimestamp int64    `json:"profile_create_timestamp"` // 数据上传时间 while data upload to server
	ProfileInTimestamp     int64    `json:"profile_in_timestamp"`     // 数据写入时间 while data write in storage
	ProfileLanguageType    string   `json:"profile_language_type"`    // e.g.: Golang/Java/Python...
	ProfileID              string   `json:"profile_id"`
	TraceID                string   `json:"trace_id"`
	SpanName               string   `json:"span_name"`
	AppInstance            string   `json:"app_instance"`
	TagNames               []string `json:"tag_names"`
	TagValues              []string `json:"tag_values"`
	CompressionAlgo        string   `json:"compression_algo"`
	// Ebpf Profile Infos
	ProcessID        uint32 `json:"process_id"`
	ProcessStartTime int64  `json:"process_start_time"`
	GPID             uint32

	// Universal Tag
	VtapID       uint16
	RegionID     uint16
	AZID         uint16
	SubnetID     uint16
	L3EpcID      int32
	HostID       uint16
	PodID        uint32
	PodNodeID    uint32
	PodNSID      uint16
	PodClusterID uint16
	PodGroupID   uint32

	AutoInstanceID   uint32
	AutoInstanceType uint8
	AutoServiceID    uint32
	AutoServiceType  uint8

	IP4    uint32 `json:"ip4"`
	IP6    net.IP `json:"ip6"`
	IsIPv4 bool   `json:"is_ipv4"`

	L3DeviceType uint8
	L3DeviceID   uint32
	ServiceID    uint32

	// Not stored, only determines which database to store in.
	// When Orgid is 0 or 1, it is stored in database 'profile', otherwise stored in '<OrgId>_profile'.
	OrgId  uint16
	TeamID uint16
}

// profile_event_type <-> profile_value_unit relation
/*
| profile_event_type               | unit             | desc                                                     |
|----------------------------------|------------------|----------------------------------------------------------|
| cpu                              | samples          | cpu time, count by profile interval, e.g.: 1 sample/10ms |
| inuse_objects                    | objects          | count                                                    |
| alloc_objects                    | objects          | count                                                    |
| inuse_space                      | bytes            | byte(b)                                                  |
| alloc_space                      | bytes            | byte                                                     |
| goroutines                       | goroutines       | count                                                    |
| mutex_duration                   | lock_nanoseconds | ns                                                       |
| mutex_count                      | lock_samples     | count                                                    |
| block_duration                   | lock_nanoseconds | ns                                                       |
| block_count                      | lock_samples     | count                                                    |
| itimer(java)                     | samples          | cpu time                                                 |
| wall(java)                       | samples          | cpu time                                                 |
| alloc_in_new_tlab_objects(java)  | objects          | count                                                    |
| alloc_in_new_tlab_bytes(java)    | bytes            | byte                                                     |
| alloc_outside_tlab_objects(java) | objects          | count                                                    |
| alloc_outside_tlab_bytes(java)   | bytes            | byte                                                     |
| lock_count(java)                 | lock_samples     | count                                                    |
| lock_duration(java)              | lock_nanoseconds | ns                                                       |
| on-cpu(eBPF)                     | microseconds     | cpu time, DeepFlow-Agent as profiler                     |
*/

func ProfileColumns() []*ckdb.Column {
	return []*ckdb.Column{
		// profile information
		ckdb.NewColumn("time", ckdb.DateTime),
		ckdb.NewColumn("_id", ckdb.UInt64).SetCodec(ckdb.CodecDoubleDelta),
		ckdb.NewColumn("ip4", ckdb.IPv4).SetComment("IPv4地址"),
		ckdb.NewColumn("ip6", ckdb.IPv6).SetComment("IPV6地址"),
		ckdb.NewColumn("is_ipv4", ckdb.UInt8).SetComment("是否为IPv4地址").SetIndex(ckdb.IndexMinmax),

		ckdb.NewColumn("app_service", ckdb.LowCardinalityString).SetComment("应用名称, 用户配置上报"),
		ckdb.NewColumn("profile_location_str", ckdb.String).SetComment("单次 profile 堆栈"),
		ckdb.NewColumn("profile_value", ckdb.Int64).SetComment("profile self value"),
		ckdb.NewColumn("profile_value_unit", ckdb.LowCardinalityString).SetComment("profile value 的单位"),
		ckdb.NewColumn("profile_event_type", ckdb.LowCardinalityString).SetComment("剖析类型"),
		ckdb.NewColumn("profile_create_timestamp", ckdb.DateTime64us).SetIndex(ckdb.IndexSet).SetComment("client 端聚合时间"),
		ckdb.NewColumn("profile_in_timestamp", ckdb.DateTime64us).SetComment("DeepFlow 的写入时间，同批上报的批次数据具备相同的值"),
		ckdb.NewColumn("profile_language_type", ckdb.LowCardinalityString).SetComment("语言类型"),
		ckdb.NewColumn("profile_id", ckdb.String).SetComment("含义等同 l7_flow_log 的 span_id"),
		ckdb.NewColumn("trace_id", ckdb.String).SetComment("含义等同 l7_flow_log 的 trace_id"),
		ckdb.NewColumn("span_name", ckdb.String).SetComment("含义等同 l7_flow_log 的 endpoint"),
		ckdb.NewColumn("app_instance", ckdb.LowCardinalityString).SetComment("应用实例名称, 用户上报"),
		ckdb.NewColumn("tag_names", ckdb.ArrayLowCardinalityString).SetComment("profile 上报的 tagnames"),
		ckdb.NewColumn("tag_values", ckdb.ArrayString).SetComment("profile 上报的 tagvalues"),
		ckdb.NewColumn("compression_algo", ckdb.LowCardinalityString).SetComment("压缩算法"),
		ckdb.NewColumn("process_id", ckdb.UInt32).SetComment("进程 id"),
		ckdb.NewColumn("process_start_time", ckdb.DateTime64ms).SetComment("进程启动时间"),
		ckdb.NewColumn("gprocess_id", ckdb.UInt32).SetComment("Process"),

		// universal tag
		ckdb.NewColumn("agent_id", ckdb.UInt16).SetIndex(ckdb.IndexSet),
		ckdb.NewColumn("region_id", ckdb.UInt16).SetComment("云平台区域ID"),
		ckdb.NewColumn("az_id", ckdb.UInt16).SetComment("可用区ID"),
		ckdb.NewColumn("subnet_id", ckdb.UInt16).SetComment("ip对应的子网ID"),
		ckdb.NewColumn("l3_epc_id", ckdb.Int32).SetComment("ip对应的EPC ID"),
		ckdb.NewColumn("host_id", ckdb.UInt16).SetComment("宿主机ID"),
		ckdb.NewColumn("pod_id", ckdb.UInt32).SetComment("容器ID"),
		ckdb.NewColumn("pod_node_id", ckdb.UInt32).SetComment("容器节点ID"),
		ckdb.NewColumn("pod_ns_id", ckdb.UInt16).SetComment("容器命名空间ID"),
		ckdb.NewColumn("pod_cluster_id", ckdb.UInt16).SetComment("容器集群ID"),
		ckdb.NewColumn("pod_group_id", ckdb.UInt32).SetComment("容器组ID"),

		ckdb.NewColumn("auto_instance_id", ckdb.UInt32),
		ckdb.NewColumn("auto_instance_type", ckdb.UInt8),
		ckdb.NewColumn("auto_service_id", ckdb.UInt32),
		ckdb.NewColumn("auto_service_type", ckdb.UInt8),

		ckdb.NewColumn("l3_device_type", ckdb.UInt8).SetComment("资源类型"),
		ckdb.NewColumn("l3_device_id", ckdb.UInt32).SetComment("资源ID"),
		ckdb.NewColumn("service_id", ckdb.UInt32).SetComment("服务ID"),
		ckdb.NewColumn("team_id", ckdb.UInt16).SetComment("团队ID"),
	}
}

func GenProfileCKTable(cluster, dbName, tableName, storagePolicy, ckdbType string, ttl int, coldStorage *ckdb.ColdStorage) *ckdb.Table {
	timeKey := "time"
	engine := ckdb.MergeTree
	orderKeys := []string{"app_service", "profile_language_type", timeKey, "ip4", "ip6"}

	return &ckdb.Table{
		Version:         basecommon.CK_VERSION,
		Database:        dbName,
		DBType:          ckdbType,
		LocalName:       tableName + ckdb.LOCAL_SUBFFIX,
		GlobalName:      tableName,
		Columns:         ProfileColumns(),
		TimeKey:         timeKey,
		TTL:             ttl,
		PartitionFunc:   DefaultPartition,
		Engine:          engine,
		Cluster:         cluster,
		StoragePolicy:   storagePolicy,
		ColdStorage:     *coldStorage,
		OrderKeys:       orderKeys,
		PrimaryKeyCount: len(orderKeys),
	}
}

func (p *InProcessProfile) NativeTagVersion() uint32 {
	return nativetag.GetTableNativeTagsVersion(p.OrgId, nativetag.PROFILE)
}

func (p *InProcessProfile) OrgID() uint16 {
	return p.OrgId
}

var poolInProcess = pool.NewLockFreePool(func() *InProcessProfile {
	return new(InProcessProfile)
})

func (p *InProcessProfile) Release() {
	ReleaseInProcess(p)
}

func (p *InProcessProfile) String() string {
	return fmt.Sprintf("InProcessProfile:  %+v\n", *p)
}

func AcquireInProcess() *InProcessProfile {
	l := poolInProcess.Get()
	return l
}

func ReleaseInProcess(p *InProcessProfile) {
	if p == nil {
		return
	}
	tagNames := p.TagNames[:0]
	tagValues := p.TagValues[:0]
	*p = InProcessProfile{}
	p.TagNames = tagNames
	p.TagValues = tagValues
	poolInProcess.Put(p)
}

func (p *InProcessProfile) Clone() *InProcessProfile {
	c := AcquireInProcess()
	*c = *p
	c.TagNames = make([]string, len(p.TagNames))
	copy(p.TagNames, p.TagNames)
	c.TagValues = make([]string, len(p.TagValues))
	copy(p.TagValues, p.TagValues)
	return c
}

func (p *InProcessProfile) FillProfile(createTime time.Time,
	profileUnit string,
	profileLabels map[string]string,
	platformData *grpc.PlatformInfoTable,
	vtapID, orgId, teamId uint16,
	podID uint32,
	profileName string,
	eventType string,
	location string,
	compressionAlgo string,
	self int64,
	inTimeStamp time.Time,
	languageType string,
	pid uint32,
	stime int64,
	tagNames []string,
	tagValues []string) {

	p.Time = uint32(createTime.Unix())
	p._id = genID(uint32(createTime.UnixNano()/int64(time.Second)), &InProcessCounter, vtapID)
	p.VtapID = vtapID
	p.PodID = podID
	p.AppService = profileName
	p.ProfileLocationStr = location
	p.CompressionAlgo = compressionAlgo
	p.ProfileEventType = eventType
	p.ProfileValue = self
	p.ProfileValueUnit = profileUnit
	p.ProfileCreateTimestamp = createTime.UnixMicro()
	p.ProfileInTimestamp = inTimeStamp.UnixMicro()
	p.ProfileLanguageType = languageType
	if profileLabels != nil {
		p.ProfileID = profileLabels[LabelProfileID]
		p.SpanName = profileLabels[LabelSpanName]
	}
	// app_instance should upload by user with label, if empty use app_service
	if p.AppInstance == "" {
		p.AppInstance = p.AppService
	}
	p.ProcessID = pid
	p.ProcessStartTime = stime
	p.GPID = platformData.QueryProcessInfo(orgId, vtapID, pid)
	tagNames = append(tagNames, LabelAppService, LabelLanguageType, LabelTraceID, LabelSpanName, LabelAppInstance)
	tagValues = append(tagValues, p.AppService, p.ProfileLanguageType, p.TraceID, p.SpanName, p.AppInstance)
	p.TagNames = tagNames
	p.TagValues = tagValues

	p.OrgId, p.TeamID = orgId, teamId
	p.fillResource(vtapID, podID, platformData)
}

func genID(time uint32, counter *uint32, vtapID uint16) uint64 {
	count := atomic.AddUint32(counter, 1)
	return uint64(time)<<32 | ((uint64(vtapID) & 0x3fff) << 18) | (uint64(count) & 0x03ffff)
}

func (p *InProcessProfile) fillResource(vtapID uint16, podID uint32, platformData *grpc.PlatformInfoTable) {
	vtapInfo := platformData.QueryVtapInfo(p.OrgId, vtapID)
	var vtapPlatformInfo *grpc.Info
	if vtapInfo != nil {
		p.L3EpcID = vtapInfo.EpcId
		p.PodClusterID = uint16(vtapInfo.PodClusterId)
		vtapIP := net.ParseIP(vtapInfo.Ip)
		// get vtap platformInfo, incase can not find Pod by container (maybe containerID is empty)
		if vtapIP != nil {
			if ip4 := vtapIP.To4(); ip4 != nil {
				// fill ip from Vtap first, can be overwritten by podInfo later
				IP4 := utils.IpToUint32(ip4)
				vtapPlatformInfo = platformData.QueryIPV4Infos(p.OrgId, vtapInfo.EpcId, IP4)
				if p.IP4 == 0 && (len(p.IP6) == 0 || p.IP6.Equal(net.IPv6zero)) {
					p.IP4 = IP4
					p.IsIPv4 = true
				}
			} else {
				IP6 := vtapIP
				vtapPlatformInfo = platformData.QueryIPV6Infos(p.OrgId, vtapInfo.EpcId, IP6)
				if p.IP4 == 0 && (len(p.IP6) == 0 || p.IP6.Equal(net.IPv6zero)) {
					p.IP6 = IP6
					p.IsIPv4 = false
				}
			}
		}
	}

	var info *grpc.Info
	// 1. try to find platform info by podID first
	if podID != 0 {
		info = platformData.QueryPodIdInfo(p.OrgId, podID)
		if info != nil {
			// rewirte ip from podInfo
			p.IsIPv4, p.IP4, p.IP6 = info.IsIPv4, info.IP4, info.IP6
		}
	}

	// 2. try to fix platform info by IP
	if info == nil {
		// app profile: submit IP from agent
		// ebpf profile with hostnetwork: when PodID get nil infos, try to get info from PodNodeID
		if p.IsIPv4 {
			info = platformData.QueryIPV4Infos(p.OrgId, p.L3EpcID, p.IP4)
		} else {
			info = platformData.QueryIPV6Infos(p.OrgId, p.L3EpcID, p.IP6)
		}
	}

	podGroupType := uint8(0)
	if info != nil {
		p.RegionID = uint16(info.RegionID)
		p.AZID = uint16(info.AZID)
		p.SubnetID = uint16(info.SubnetID)
		p.HostID = uint16(info.HostID)
		if p.PodID == 0 {
			p.PodID = info.PodID
		}
		p.PodNodeID = info.PodNodeID
		p.PodNSID = uint16(info.PodNSID)
		if p.PodClusterID == 0 {
			p.PodClusterID = uint16(info.PodClusterID)
		}
		p.PodGroupID = info.PodGroupID
		podGroupType = info.PodGroupType
		p.L3DeviceType = uint8(info.DeviceType)
		p.L3DeviceID = info.DeviceID
		p.ServiceID = platformData.QueryService(p.OrgId, p.PodID, p.PodNodeID, uint32(p.PodClusterID), p.PodGroupID, p.L3EpcID, !p.IsIPv4, p.IP4, p.IP6, layers.IPProtocolTCP, 0)
	}

	// fix up when all resource match failed
	if vtapPlatformInfo != nil {
		p.fillInfraInfo(vtapPlatformInfo)
	}

	p.AutoInstanceID, p.AutoInstanceType = basecommon.GetAutoInstance(p.PodID, p.GPID, p.PodNodeID, p.L3DeviceID, uint32(p.SubnetID), p.L3DeviceType, p.L3EpcID)
	p.AutoServiceID, p.AutoServiceType = basecommon.GetAutoService(p.ServiceID, p.PodGroupID, p.GPID, uint32(p.PodClusterID), p.L3DeviceID, uint32(p.SubnetID), p.L3DeviceType, podGroupType, p.L3EpcID)

}

func (p *InProcessProfile) fillPodInfo(vtapID uint16, containerID string, platformData *grpc.PlatformInfoTable) {
	if containerID == "" {
		log.Debugf("%s-%s uploaded empty containerID by vtapID: %d", p.AppService, p.ProfileEventType, vtapID)
		return
	}
	podInfo := platformData.QueryPodContainerInfo(p.OrgId, vtapID, containerID)
	if podInfo != nil {
		p.PodID = podInfo.PodId
		ip := net.ParseIP(podInfo.Ip)
		// ip is nil means pod maybe `hostNetwork` and PodIP equals NodeIP
		if ip == nil {
			ip = net.ParseIP(podInfo.PodNodeIp)
		}
		if ip != nil {
			if ip4 := ip.To4(); ip4 != nil {
				p.IsIPv4 = true
				p.IP4 = utils.IpToUint32(ip4)
			} else {
				p.IP6 = ip
			}
		}
	}
}

func (p *InProcessProfile) fillInfraInfo(vtapPlatformInfo *grpc.Info) {
	// when all resource match failed, still confirm pod is run on vtap node
	if p.RegionID == 0 {
		p.RegionID = uint16(vtapPlatformInfo.RegionID)
	}
	if p.AZID == 0 {
		p.AZID = uint16(vtapPlatformInfo.AZID)
	}
	if p.SubnetID == 0 {
		p.SubnetID = uint16(vtapPlatformInfo.SubnetID)
	}
	if p.HostID == 0 {
		p.HostID = uint16(vtapPlatformInfo.HostID)
	}
	if p.PodNodeID == 0 {
		p.PodNodeID = vtapPlatformInfo.PodNodeID
	}
	if p.L3DeviceID == 0 {
		p.L3DeviceID = uint32(vtapPlatformInfo.DeviceID)
		p.L3DeviceType = uint8(vtapPlatformInfo.DeviceType)
	}
}

func (p *InProcessProfile) GenerateFlowTags(cache *flow_tag.FlowTagCache) {
	flowTagInfo := &cache.FlowTagInfoBuffer
	*flowTagInfo = flow_tag.FlowTagInfo{
		Table:   fmt.Sprintf("%s.%s", p.ProfileLanguageType, p.ProfileEventType),
		VpcId:   p.L3EpcID,
		PodNsId: p.PodNSID,
		OrgId:   p.OrgId,
		TeamID:  p.TeamID,
	}
	cache.Fields = cache.Fields[:0]
	cache.FieldValues = cache.FieldValues[:0]

	// tags
	flowTagInfo.FieldType = flow_tag.FieldTag
	for i, name := range p.TagNames {
		if p.TagValues[i] == "" {
			continue
		}
		flowTagInfo.FieldName = name

		// tag + value
		flowTagInfo.FieldValue = p.TagValues[i]
		if old, ok := cache.FieldValueCache.AddOrGet(*flowTagInfo, p.Time); ok {
			if old+cache.CacheFlushTimeout >= p.Time {
				continue
			} else {
				cache.FieldValueCache.Add(*flowTagInfo, p.Time)
			}
		}
		tagFieldValue := flow_tag.AcquireFlowTag(flow_tag.TagFieldValue)
		tagFieldValue.Timestamp = p.Time
		tagFieldValue.FlowTagInfo = *flowTagInfo
		cache.FieldValues = append(cache.FieldValues, tagFieldValue)

		// only tag
		flowTagInfo.FieldValue = ""
		if old, ok := cache.FieldCache.AddOrGet(*flowTagInfo, p.Time); ok {
			if old+cache.CacheFlushTimeout >= p.Time {
				continue
			} else {
				cache.FieldCache.Add(*flowTagInfo, p.Time)
			}
		}
		tagField := flow_tag.AcquireFlowTag(flow_tag.TagField)
		tagField.Timestamp = p.Time
		tagField.FlowTagInfo = *flowTagInfo
		cache.Fields = append(cache.Fields, tagField)
	}
}
