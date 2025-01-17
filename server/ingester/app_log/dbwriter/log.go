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
	"strings"
	"sync/atomic"

	basecommon "github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/ingester/exporters/config"
	"github.com/deepflowio/deepflow/server/ingester/flow_tag"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/nativetag"
	"github.com/deepflowio/deepflow/server/libs/pool"
)

const (
	DefaultPartition = ckdb.TimeFuncHour
)

const (
	LOG_TYPE_USER   = "user"   // User process log
	LOG_TYPE_SYSTEM = "system" // All processes on the server where DeepFlow Server is located
	LOG_TYPE_AUDIT  = "audit"  // DeepFlow Audit log (operation log)
	LOG_TYPE_AGENT  = "agent"  // DeepFlow Agent log
)

func StringToLogType(str string) string {
	// lowerStr cannot be returned. This comes from the json field and will be overwritten by the next parsing.
	lowerStr := strings.ToLower(str)
	switch lowerStr {
	case LOG_TYPE_SYSTEM:
		return LOG_TYPE_SYSTEM
	case LOG_TYPE_AUDIT:
		return LOG_TYPE_AUDIT
	case LOG_TYPE_AGENT:
		return LOG_TYPE_AGENT
	default:
		return LOG_TYPE_USER
	}
}

func LogTypeEnumArgs() string {
	return fmt.Sprintf("'%s' = 1, '%s' = 2, '%s' = 3, '%s' = 4", LOG_TYPE_USER, LOG_TYPE_SYSTEM, LOG_TYPE_AUDIT, LOG_TYPE_AGENT)
}

type ApplicationLogStore struct {
	pool.ReferenceCount

	Time      uint32 `json:"time" category:"$tag" sub:"flow_info"` // s
	Timestamp int64  `json:"timestamp" category:"$tag" sub:"flow_info"`
	_id       uint64 `json:"_id" category:"$tag" sub:"flow_info"`
	Type      string

	TraceID    string
	SpanID     string
	TraceFlags uint32

	SeverityNumber uint8 // numerical value of the severity(also known as log level id)

	Body string

	AppService string `json:"app_service" category:"$tag" sub:"service_info"` // service name

	GProcessID   uint32 `json:"gprocess_id" category:"$tag" sub:"universal_tag"`
	AgentID      uint16 `json:"agent_id" category:"$tag" sub:"universal_tag"`
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
	SubnetID     uint16 `json:"subnet_id" category:"$tag" sub:"universal_tag"`
	IsIPv4       bool   `json:"is_ipv4" category:"$tag" sub:"universal_tag"`
	IP4          uint32 `json:"ip4" category:"$tag" sub:"network_layer" to_string:"IPv4String"`
	IP6          net.IP `json:"ip6" category:"$tag" sub:"network_layer"  to_string:"IPv6String"`

	// Not stored, only determines which database to store in.
	// When Orgid is 0 or 1, it is stored in database 'event', otherwise stored in '<OrgId>_event'.
	OrgId  uint16
	TeamID uint16
	UserID uint32

	AutoInstanceID   uint32 `json:"auto_instance_id" category:"$tag" sub:"universal_tag"`
	AutoInstanceType uint8  `json:"auto_instance_type" category:"$tag" sub:"universal_tag" enumfile:"auto_instance_type"`
	AutoServiceID    uint32 `json:"auto_service_id" category:"$tag" sub:"universal_tag"`
	AutoServiceType  uint8  `json:"auto_service_type" category:"$tag" sub:"universal_tag" enumfile:"auto_service_type"`

	AttributeNames  []string `json:"attribute_names" category:"$tag" sub:"native_tag" data_type:"[]string"`
	AttributeValues []string `json:"attribute_values" category:"$tag" sub:"native_tag" data_type:"[]string"`

	MetricsNames  []string  `json:"metrics_names" category:"$metrics" data_type:"[]string"`
	MetricsValues []float64 `json:"metrics_values" category:"$metrics" data_type:"[]float64"`
}

func (l *ApplicationLogStore) NativeTagVersion() uint32 {
	return nativetag.GetTableNativeTagsVersion(l.OrgId, nativetag.APPLICATION_LOG)
}

func (l *ApplicationLogStore) OrgID() uint16 {
	return l.OrgId
}

func (l *ApplicationLogStore) Table() string {
	return LOG_TABLE
}

func (l *ApplicationLogStore) Release() {
	ReleaseApplicationLogStore(l)
}

func (l *ApplicationLogStore) DataSource() uint32 {
	return uint32(config.MAX_DATASOURCE_ID)
}

var LogCounter uint32

func (l *ApplicationLogStore) SetId(time, analyzerID uint32) {
	count := atomic.AddUint32(&LogCounter, 1)
	// The high 32 bits of time, 23-32 bits represent analyzerId, the low 22 bits are counter
	l._id = uint64(time)<<32 | uint64(analyzerID&0x3ff)<<22 | (uint64(count) & 0x3fffff)
}

func LogColumns() []*ckdb.Column {
	columns := []*ckdb.Column{
		ckdb.NewColumn("time", ckdb.DateTime),
		ckdb.NewColumn("timestamp", ckdb.DateTime64us).SetComment("presion: us"),
		ckdb.NewColumn("_id", ckdb.UInt64).SetComment("Unique ID"),
		ckdb.NewColumn("_type", ckdb.ENUM8).SetTypeArgs(LogTypeEnumArgs()).SetIndex(ckdb.IndexNone).SetComment("log type"),
		ckdb.NewColumn("trace_id", ckdb.String).SetCodec(ckdb.CodecZSTD).SetIndex(ckdb.IndexBloomfilter).SetComment("Trace ID"),
		ckdb.NewColumn("span_id", ckdb.String).SetCodec(ckdb.CodecZSTD).SetIndex(ckdb.IndexBloomfilter).SetComment("Span ID"),
		ckdb.NewColumn("trace_flags", ckdb.UInt32).SetComment("W3C trace flag, currently not support yet"),
		ckdb.NewColumn("severity_number", ckdb.UInt8).SetIndex(ckdb.IndexNone).SetComment("numerical value of the severity(also known as log level id)"),
		ckdb.NewColumn("body", ckdb.String).SetIndex(ckdb.IndexTokenbf).SetCodec(ckdb.CodecZSTD).SetComment("log content"),
		ckdb.NewColumn("app_service", ckdb.LowCardinalityString).SetIndex(ckdb.IndexBloomfilter).SetComment("Application Service (service name)"),

		ckdb.NewColumn("gprocess_id", ckdb.UInt32).SetComment("Global Process ID"),
		ckdb.NewColumn("agent_id", ckdb.UInt16).SetComment("Agent ID"),
		ckdb.NewColumn("region_id", ckdb.UInt16).SetComment("Region ID"),
		ckdb.NewColumn("az_id", ckdb.UInt16).SetComment("Availability Zone ID"),
		ckdb.NewColumn("l3_epc_id", ckdb.Int32).SetComment("VPC ID"),
		ckdb.NewColumn("host_id", ckdb.UInt16).SetComment("M Hypervisor ID"),
		ckdb.NewColumn("pod_id", ckdb.UInt32).SetComment("K8s POD ID"),
		ckdb.NewColumn("pod_node_id", ckdb.UInt32).SetComment("K8s Node ID"),
		ckdb.NewColumn("pod_ns_id", ckdb.UInt16).SetComment("K8s Namespace ID"),
		ckdb.NewColumn("pod_cluster_id", ckdb.UInt16).SetComment("K8s Cluster ID"),
		ckdb.NewColumn("pod_group_id", ckdb.UInt32).SetComment("K8s Workload ID"),

		ckdb.NewColumn("l3_device_type", ckdb.UInt8).SetComment("Resource Type"),
		ckdb.NewColumn("l3_device_id", ckdb.UInt32).SetComment("Resource ID"),
		ckdb.NewColumn("service_id", ckdb.UInt32).SetComment("Service ID"),
		ckdb.NewColumn("subnet_id", ckdb.UInt16).SetComment("Subnet ID"),
		ckdb.NewColumn("is_ipv4", ckdb.UInt8).SetIndex(ckdb.IndexNone),
		ckdb.NewColumn("ip4", ckdb.IPv4),
		ckdb.NewColumn("ip6", ckdb.IPv6),

		ckdb.NewColumn("team_id", ckdb.UInt16).SetComment("Team ID"),
		ckdb.NewColumn("user_id", ckdb.UInt32).SetComment("User ID"),

		ckdb.NewColumn("auto_instance_id", ckdb.UInt32).SetComment("Instance - K8s POD First"),
		ckdb.NewColumn("auto_instance_type", ckdb.UInt8).SetComment("Type - K8s POD First"),
		ckdb.NewColumn("auto_service_id", ckdb.UInt32).SetComment("Instance - K8s Service First"),
		ckdb.NewColumn("auto_service_type", ckdb.UInt8).SetComment("Type - K8s Service First"),

		ckdb.NewColumn("attribute_names", ckdb.ArrayLowCardinalityString).SetComment("Extra Attributes"),
		ckdb.NewColumn("attribute_values", ckdb.ArrayString).SetCodec(ckdb.CodecZSTD).SetComment("the value of the extra attributes"),
		ckdb.NewColumn("metrics_names", ckdb.ArrayLowCardinalityString).SetComment("Extra Metrics"),
		ckdb.NewColumn("metrics_values", ckdb.ArrayFloat64).SetComment("the value of the extra metrics"),
	}
	return columns
}

func GenLogCKTable(cluster, storagePolicy, table, ckdbType string, ttl int, coldStorage *ckdb.ColdStorage) *ckdb.Table {
	timeKey := "time"
	engine := ckdb.MergeTree
	orderKeys := []string{"app_service", timeKey, "timestamp"}
	partition := DefaultPartition

	return &ckdb.Table{
		Version:         basecommon.CK_VERSION,
		Database:        LOG_DB,
		DBType:          ckdbType,
		LocalName:       table + ckdb.LOCAL_SUBFFIX,
		GlobalName:      table,
		Columns:         LogColumns(),
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

func (l *ApplicationLogStore) GenerateNewFlowTags(cache *flow_tag.FlowTagCache) {
	// reset temporary buffers
	flowTagInfo := &cache.FlowTagInfoBuffer
	*flowTagInfo = flow_tag.FlowTagInfo{
		Table:   l.Table(),
		VpcId:   l.L3EpcID,
		PodNsId: l.PodNSID,
		OrgId:   l.OrgId,
		TeamID:  l.TeamID,
	}
	cache.Fields = cache.Fields[:0]
	cache.FieldValues = cache.FieldValues[:0]

	// tags
	flowTagInfo.FieldType = flow_tag.FieldTag
	for i, name := range l.AttributeNames {
		flowTagInfo.FieldName = name

		// tag + value
		flowTagInfo.FieldValue = l.AttributeValues[i]
		if old, ok := cache.FieldValueCache.AddOrGet(*flowTagInfo, l.Time); ok {
			if old+cache.CacheFlushTimeout >= l.Time {
				// If there is no new fieldValue, of course there will be no new field.
				// So we can just skip the rest of the process in the loop.
				continue
			} else {
				cache.FieldValueCache.Add(*flowTagInfo, l.Time)
			}
		}
		tagFieldValue := flow_tag.AcquireFlowTag(flow_tag.TagFieldValue)
		tagFieldValue.Timestamp = l.Time
		tagFieldValue.FlowTagInfo = *flowTagInfo
		cache.FieldValues = append(cache.FieldValues, tagFieldValue)

		// only tag
		flowTagInfo.FieldValue = ""
		if old, ok := cache.FieldCache.AddOrGet(*flowTagInfo, l.Time); ok {
			if old+cache.CacheFlushTimeout >= l.Time {
				continue
			} else {
				cache.FieldCache.Add(*flowTagInfo, l.Time)
			}
		}
		tagField := flow_tag.AcquireFlowTag(flow_tag.TagField)
		tagField.Timestamp = l.Time
		tagField.FlowTagInfo = *flowTagInfo
		cache.Fields = append(cache.Fields, tagField)
	}

	// metrics
	flowTagInfo.FieldType = flow_tag.FieldMetrics
	flowTagInfo.FieldValue = ""
	for _, name := range l.MetricsNames {
		flowTagInfo.FieldName = name
		if old, ok := cache.FieldCache.AddOrGet(*flowTagInfo, l.Time); ok {
			if old+cache.CacheFlushTimeout >= l.Time {
				continue
			} else {
				cache.FieldCache.Add(*flowTagInfo, l.Time)
			}
		}
		tagField := flow_tag.AcquireFlowTag(flow_tag.TagField)
		tagField.Timestamp = l.Time
		tagField.FlowTagInfo = *flowTagInfo
		cache.Fields = append(cache.Fields, tagField)
	}
}

var logPool = pool.NewLockFreePool(func() *ApplicationLogStore {
	return &ApplicationLogStore{
		IsIPv4:          true,
		AttributeNames:  []string{},
		AttributeValues: []string{},
		MetricsNames:    []string{},
		MetricsValues:   []float64{},
	}
})

func AcquireApplicationLogStore() *ApplicationLogStore {
	e := logPool.Get()
	e.Reset()
	return e
}

func ReleaseApplicationLogStore(l *ApplicationLogStore) {
	if l == nil || l.SubReferenceCount() {
		return
	}
	attributeNames := l.AttributeNames[:0]
	attributeValues := l.AttributeValues[:0]
	metricsNames := l.MetricsNames[:0]
	metricsValues := l.MetricsValues[:0]
	*l = ApplicationLogStore{}
	l.AttributeNames = attributeNames
	l.AttributeValues = attributeValues
	l.MetricsNames = metricsNames
	l.MetricsValues = metricsValues
	l.IsIPv4 = true

	logPool.Put(l)
}
