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
	"reflect"
	"sync/atomic"
	"unsafe"

	basecommon "github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/ingester/event/common"
	exportercommon "github.com/deepflowio/deepflow/server/ingester/exporters/common"
	"github.com/deepflowio/deepflow/server/ingester/exporters/config"
	utag "github.com/deepflowio/deepflow/server/ingester/exporters/universal_tag"
	"github.com/deepflowio/deepflow/server/ingester/flow_tag"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/nativetag"
	"github.com/deepflowio/deepflow/server/libs/pool"
	"github.com/deepflowio/deepflow/server/libs/utils"
)

const (
	DefaultPartition          = ckdb.TimeFuncTwelveHour
	DefaultPerfEventPartition = ckdb.TimeFuncHour
	IO_EVENT_TYPE_READ        = "read"
	IO_EVENT_TYPE_WRITE       = "write"
)

type SignalSource uint8

const (
	SIGNAL_SOURCE_UNKNOWN SignalSource = iota
	SIGNAL_SOURCE_RESOURCE
	SIGNAL_SOURCE_IO
	SIGNAL_SOURCE_K8S
)

type EventStore struct {
	pool.ReferenceCount

	Time uint32 `json:"time" category:"$tag" sub:"flow_info"` // s
	_id  uint64 `json:"_id" category:"$tag" sub:"flow_info"`

	StartTime int64 `json:"start_time" category:"$tag" sub:"flow_info"` // us
	EndTime   int64 `json:"end_time" category:"$tag" sub:"flow_info"`   // us

	Tagged uint8

	SignalSource     uint8  `json:"signal_source" category:"$tag" sub:"capture_info" enumfile:"perf_event_signal_source"` // Resource / File IO
	EventType        string `json:"event_type" category:"$tag" sub:"event_info" enumfile:"perf_event_type"`
	EventDescription string
	ProcessKName     string `json:"process_kname" category:"$tag" sub:"service_info"` // us

	GProcessID uint32 `json:"gprocess_id" category:"$tag" sub:"universal_tag"`

	RegionID     uint16 `json:"region_id" category:"$tag" sub:"universal_tag"`
	AZID         uint16 `json:"az_id" category:"$tag" sub:"universal_tag"`
	L3EpcID      int32  `json:"l3_epc_id" category:"$tag" sub:"universal_tag"`
	HostID       uint16 `json:"host_id" category:"$tag" sub:"universal_tag"`
	PodID        uint32 `json:"pod_id" category:"$tag" sub:"universal_tag"`
	PodNodeID    uint32 `json:"host_node_id" category:"$tag" sub:"universal_tag"`
	PodNSID      uint16 `json:"pod_ns_id" category:"$tag" sub:"universal_tag"`
	PodClusterID uint16 `json:"pod_cluster_id" category:"$tag" sub:"universal_tag"`
	PodGroupID   uint32 `json:"pod_group_id" category:"$tag" sub:"universal_tag"`
	L3DeviceType uint8  `json:"l3_device_type" category:"$tag" sub:"universal_tag"`
	L3DeviceID   uint32 `json:"l3_device_id" category:"$tag" sub:"universal_tag"`
	ServiceID    uint32 `json:"service_id" category:"$tag" sub:"universal_tag"`
	VTAPID       uint16 `json:"agent_id" category:"$tag" sub:"universal_tag"`
	SubnetID     uint16 `json:"subnet_id" category:"$tag" sub:"universal_tag"`
	IsIPv4       bool   `json:"is_ipv4" category:"$tag" sub:"network_layer"`
	IP4          uint32 `json:"ip4" category:"$tag" sub:"network_layer" to_string:"IPv4String"`
	IP6          net.IP `json:"ip6" category:"$tag" sub:"network_layer"  to_string:"IPv6String"`

	// Not stored, only determines which database to store in.
	// When Orgid is 0 or 1, it is stored in database 'event', otherwise stored in '<OrgId>_event'.
	OrgId  uint16 `json:"org_id" category:"$tag"`
	TeamID uint16 `json:"team_id" category:"$tag"`

	AutoInstanceID   uint32 `json:"auto_instance_id" category:"$tag" sub:"universal_tag"`
	AutoInstanceType uint8  `json:"auto_instance_type" category:"$tag" sub:"universal_tag" enumfile:"auto_instance_type"`
	AutoServiceID    uint32 `json:"auto_service_id" category:"$tag" sub:"universal_tag"`
	AutoServiceType  uint8  `json:"auto_service_type" category:"$tag" sub:"universal_tag" enumfile:"auto_service_type"`

	AppInstance string `json:"app_instance" category:"$tag" sub:"service_info"`

	AttributeNames  []string `json:"attribute_names" category:"$tag" sub:"native_tag" data_type:"[]string"`
	AttributeValues []string `json:"attribute_values" category:"$tag" sub:"native_tag" data_type:"[]string"`

	HasMetrics bool
	Bytes      uint32 `json:"bytes" category:"$metrics" sub:"throughput"`
	Duration   uint64 `json:"duration" category:"$metrics" sub:"delay"`
}

func (e *EventStore) NativeTagVersion() uint32 {
	if e.HasMetrics {
		return nativetag.GetTableNativeTagsVersion(e.OrgId, nativetag.EVENT_PERF_EVENT)
	}
	return nativetag.GetTableNativeTagsVersion(e.OrgId, nativetag.EVENT_EVENT)
}

func (e *EventStore) OrgID() uint16 {
	return e.OrgId
}

func (e *EventStore) Table() string {
	if e.HasMetrics {
		return common.PERF_EVENT.TableName()
	}
	return common.RESOURCE_EVENT.TableName() // the same as common.K8S_EVENT.TableName()
}

func (e *EventStore) Release() {
	ReleaseEventStore(e)
}

func (e *EventStore) DataSource() uint32 {
	if e.HasMetrics {
		return uint32(config.PERF_EVENT)
	}
	return uint32(config.MAX_DATASOURCE_ID)
}

func (e *EventStore) EncodeTo(protocol config.ExportProtocol, utags *utag.UniversalTagsManager, cfg *config.ExporterCfg) (interface{}, error) {
	switch protocol {
	case config.PROTOCOL_KAFKA:
		tags := e.QueryUniversalTags(utags)
		k8sLabels := utags.QueryCustomK8sLabels(e.OrgId, e.PodID)
		return exportercommon.EncodeToJson(e, int(e.DataSource()), cfg, tags, tags, k8sLabels, k8sLabels), nil
	default:
		return nil, fmt.Errorf("event unsupport export to %s", protocol)
	}
}

func (e *EventStore) QueryUniversalTags(utags *utag.UniversalTagsManager) *utag.UniversalTags {
	return utags.QueryUniversalTags(e.OrgId,
		e.RegionID, e.AZID, e.HostID, e.PodNSID, e.PodClusterID, e.SubnetID, e.VTAPID,
		uint8(e.L3DeviceType), e.AutoServiceType, e.AutoInstanceType,
		e.L3DeviceID, e.AutoServiceID, e.AutoInstanceID, e.PodNodeID, e.PodGroupID, e.PodID, uint32(e.L3EpcID), 0, e.ServiceID,
		e.IsIPv4, e.IP4, e.IP6,
	)
}

func (e *EventStore) GetFieldValueByOffsetAndKind(offset uintptr, kind reflect.Kind, dataType utils.DataType) interface{} {
	return utils.GetValueByOffsetAndKind(uintptr(unsafe.Pointer(e)), offset, kind, dataType)
}

func (e *EventStore) TimestampUs() int64 {
	return int64(e.EndTime)
}

var EventCounter uint32

func (e *EventStore) SetId(time, analyzerID uint32) {
	count := atomic.AddUint32(&EventCounter, 1)
	// The high 32 bits of time, 23-32 bits represent analyzerId, the low 22 bits are counter
	e._id = uint64(time)<<32 | uint64(analyzerID&0x3ff)<<22 | (uint64(count) & 0x3fffff)
}

func EventColumns(hasMetrics bool) []*ckdb.Column {
	columns := []*ckdb.Column{
		ckdb.NewColumn("time", ckdb.DateTime),
		ckdb.NewColumn("_id", ckdb.UInt64),
		ckdb.NewColumn("start_time", ckdb.DateTime64us).SetComment("精度: 微秒"),
		ckdb.NewColumn("end_time", ckdb.DateTime64us).SetComment("精度: 微秒"),

		ckdb.NewColumn("tagged", ckdb.UInt8).SetComment("标签是否为填充, 用于调试"),

		ckdb.NewColumn("signal_source", ckdb.UInt8).SetComment("事件来源"),
		ckdb.NewColumn("event_type", ckdb.LowCardinalityString).SetComment("事件类型"),
		ckdb.NewColumn("event_desc", ckdb.String).SetComment("事件信息"),
		ckdb.NewColumn("process_kname", ckdb.String).SetComment("进程名"),

		ckdb.NewColumn("gprocess_id", ckdb.UInt32).SetComment("全局进程ID"),

		ckdb.NewColumn("region_id", ckdb.UInt16).SetComment("云平台区域ID"),
		ckdb.NewColumn("az_id", ckdb.UInt16).SetComment("可用区ID"),
		ckdb.NewColumn("l3_epc_id", ckdb.Int32).SetComment("ip对应的EPC ID"),
		ckdb.NewColumn("host_id", ckdb.UInt16).SetComment("宿主机ID"),
		ckdb.NewColumn("pod_id", ckdb.UInt32).SetComment("容器ID"),
		ckdb.NewColumn("pod_node_id", ckdb.UInt32).SetComment("容器节点ID"),
		ckdb.NewColumn("pod_ns_id", ckdb.UInt16).SetComment("容器命名空间ID"),
		ckdb.NewColumn("pod_cluster_id", ckdb.UInt16).SetComment("容器集群ID"),
		ckdb.NewColumn("pod_group_id", ckdb.UInt32).SetComment("容器组ID"),

		ckdb.NewColumn("l3_device_type", ckdb.UInt8).SetComment("资源类型"),
		ckdb.NewColumn("l3_device_id", ckdb.UInt32).SetComment("资源ID"),
		ckdb.NewColumn("service_id", ckdb.UInt32).SetComment("服务ID"),
		ckdb.NewColumn("agent_id", ckdb.UInt16).SetComment("采集器ID"),
		ckdb.NewColumn("subnet_id", ckdb.UInt16),
		ckdb.NewColumn("is_ipv4", ckdb.UInt8),
		ckdb.NewColumn("ip4", ckdb.IPv4),
		ckdb.NewColumn("ip6", ckdb.IPv6),

		ckdb.NewColumn("team_id", ckdb.UInt16).SetComment("Team ID"),

		ckdb.NewColumn("auto_instance_id", ckdb.UInt32),
		ckdb.NewColumn("auto_instance_type", ckdb.UInt8),
		ckdb.NewColumn("auto_service_id", ckdb.UInt32),
		ckdb.NewColumn("auto_service_type", ckdb.UInt8),
		ckdb.NewColumn("app_instance", ckdb.String).SetComment("app instance"),

		ckdb.NewColumn("attribute_names", ckdb.ArrayLowCardinalityString).SetComment("额外的属性"),
		ckdb.NewColumn("attribute_values", ckdb.ArrayString).SetComment("额外的属性对应的值"),
	}
	if hasMetrics {
		columns = append(columns,
			ckdb.NewColumn("bytes", ckdb.UInt32),
			ckdb.NewColumn("duration", ckdb.UInt64).SetComment("精度: 微秒"),
		)
	}
	return columns
}

func GenEventCKTable(cluster, storagePolicy, table, ckdbType string, ttl int, coldStorage *ckdb.ColdStorage) *ckdb.Table {
	timeKey := "time"
	engine := ckdb.MergeTree
	orderKeys := []string{timeKey, "signal_source", "event_type", "l3_epc_id", "l3_device_type", "l3_device_id"}
	hasMetrics := false
	partition := DefaultPartition
	if table == common.PERF_EVENT.TableName() {
		hasMetrics = true
		partition = DefaultPerfEventPartition
	}

	return &ckdb.Table{
		Version:         basecommon.CK_VERSION,
		Database:        EVENT_DB,
		DBType:          ckdbType,
		LocalName:       table + ckdb.LOCAL_SUBFFIX,
		GlobalName:      table,
		Columns:         EventColumns(hasMetrics),
		TimeKey:         timeKey,
		TTL:             ttl,
		PartitionFunc:   partition,
		Engine:          engine,
		Cluster:         cluster,
		StoragePolicy:   storagePolicy,
		ColdStorage:     *coldStorage,
		OrderKeys:       orderKeys,
		PrimaryKeyCount: len(orderKeys),
	}
}

func (e *EventStore) GenerateNewFlowTags(cache *flow_tag.FlowTagCache) {
	// reset temporary buffers
	flowTagInfo := &cache.FlowTagInfoBuffer
	*flowTagInfo = flow_tag.FlowTagInfo{
		Table:   e.Table(),
		VpcId:   e.L3EpcID,
		PodNsId: e.PodNSID,
		OrgId:   e.OrgId,
		TeamID:  e.TeamID,
	}
	cache.Fields = cache.Fields[:0]
	cache.FieldValues = cache.FieldValues[:0]

	// tags
	flowTagInfo.FieldType = flow_tag.FieldTag
	for i, name := range e.AttributeNames {
		flowTagInfo.FieldName = name

		// tag + value
		flowTagInfo.FieldValue = e.AttributeValues[i]
		if old, ok := cache.FieldValueCache.AddOrGet(*flowTagInfo, e.Time); ok {
			if old+cache.CacheFlushTimeout >= e.Time {
				// If there is no new fieldValue, of course there will be no new field.
				// So we can just skip the rest of the process in the loop.
				continue
			} else {
				cache.FieldValueCache.Add(*flowTagInfo, e.Time)
			}
		}
		tagFieldValue := flow_tag.AcquireFlowTag(flow_tag.TagFieldValue)
		tagFieldValue.Timestamp = e.Time
		tagFieldValue.FlowTagInfo = *flowTagInfo
		cache.FieldValues = append(cache.FieldValues, tagFieldValue)

		// only tag
		flowTagInfo.FieldValue = ""
		if old, ok := cache.FieldCache.AddOrGet(*flowTagInfo, e.Time); ok {
			if old+cache.CacheFlushTimeout >= e.Time {
				continue
			} else {
				cache.FieldCache.Add(*flowTagInfo, e.Time)
			}
		}
		tagField := flow_tag.AcquireFlowTag(flow_tag.TagField)
		tagField.Timestamp = e.Time
		tagField.FlowTagInfo = *flowTagInfo
		cache.Fields = append(cache.Fields, tagField)
	}
}

var eventPool = pool.NewLockFreePool(func() *EventStore {
	return &EventStore{
		AttributeNames:  []string{},
		AttributeValues: []string{},
	}
})

func AcquireEventStore() *EventStore {
	e := eventPool.Get()
	e.Reset()
	return e
}

func ReleaseEventStore(e *EventStore) {
	if e == nil || e.SubReferenceCount() {
		return
	}
	attributeNames := e.AttributeNames[:0]
	attributeValues := e.AttributeValues[:0]
	*e = EventStore{}
	e.AttributeNames = attributeNames
	e.AttributeValues = attributeValues
	e.IsIPv4 = true

	eventPool.Put(e)
}
