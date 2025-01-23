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
	"github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/ingester/flow_tag"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/datatype"
	flow_metrics "github.com/deepflowio/deepflow/server/libs/flow-metrics"
	"github.com/deepflowio/deepflow/server/libs/pool"
)

const (
	DefaultPartition = ckdb.TimeFuncTwoHour
)

type ExtMetrics struct {
	Timestamp uint32 // s
	MsgType   datatype.MessageType

	UniversalTag flow_metrics.UniversalTag

	VTableName string
	Host       string

	AgentID uint16

	// Not stored, only determines which database to store in.
	// When Orgid is 0 or 1, it is stored in database '<DatabaseName()>', otherwise stored in '<OrgId>_<DatabaseName()>'.
	OrgId, RawOrgId uint16 // RawOrgId is read from server-stats message, only used to distinguish which database data is written to
	TeamID          uint16

	TagNames  []string
	TagValues []string

	MetricsFloatNames  []string
	MetricsFloatValues []float64
}

func (m *ExtMetrics) IsValid() bool {
	return len(m.TagNames) == len(m.TagValues) && len(m.MetricsFloatNames) == len(m.MetricsFloatValues)
}

func (m *ExtMetrics) DatabaseName() string {
	switch m.MsgType {
	case datatype.MESSAGE_TYPE_DFSTATS:
		return DEEPFLOW_TENANT_DB
	case datatype.MESSAGE_TYPE_SERVER_DFSTATS:
		if ckdb.IsValidOrgID(m.RawOrgId) {
			return DEEPFLOW_TENANT_DB
		} else {
			return DEEPFLOW_ADMIN_DB
		}
	default:
		return EXT_METRICS_DB
	}
}

func (m *ExtMetrics) TableName() string {
	switch m.MsgType {
	case datatype.MESSAGE_TYPE_DFSTATS:
		return DEEPFLOW_TENANT_COLLECTOR_TABLE
	case datatype.MESSAGE_TYPE_SERVER_DFSTATS:
		if ckdb.IsValidOrgID(m.RawOrgId) {
			return DEEPFLOW_TENANT_COLLECTOR_TABLE
		} else {
			return DEEPFLOW_ADMIN_SERVER_TABLE
		}
	default:
		return EXT_METRICS_TABLE
	}
}

func (m *ExtMetrics) VirtualTableName() string {
	return m.VTableName
}

func (m *ExtMetrics) OrgID() uint16 {
	return m.OrgId
}

func (m *ExtMetrics) Columns() []*ckdb.Column {
	columns := []*ckdb.Column{}

	columns = append(columns, ckdb.NewColumnWithGroupBy("time", ckdb.DateTime))
	if m.MsgType != datatype.MESSAGE_TYPE_DFSTATS && m.MsgType != datatype.MESSAGE_TYPE_SERVER_DFSTATS {
		columns = flow_metrics.GenUniversalTagColumns(columns)
	}
	columns = append(columns,
		ckdb.NewColumn("virtual_table_name", ckdb.LowCardinalityString).SetComment("虚拟表名"),
		ckdb.NewColumn("team_id", ckdb.UInt16).SetComment("团队ID"),
		ckdb.NewColumn("tag_names", ckdb.ArrayLowCardinalityString).SetComment("额外的tag"),
		ckdb.NewColumn("tag_values", ckdb.ArrayLowCardinalityString).SetComment("额外的tag对应的值"),
		ckdb.NewColumn("metrics_float_names", ckdb.ArrayLowCardinalityString).SetComment("额外的float类型metrics"),
		ckdb.NewColumn("metrics_float_values", ckdb.ArrayFloat64).SetComment("额外的float metrics值"),
		ckdb.NewColumn("host", ckdb.LowCardinalityString),
	)

	return columns
}

func (m *ExtMetrics) Release() {
	ReleaseExtMetrics(m)
}

func (m *ExtMetrics) GenCKTable(cluster, storagePolicy, ckdbType string, ttl int, coldStorage *ckdb.ColdStorage) *ckdb.Table {
	timeKey := "time"
	engine := ckdb.MergeTree

	// order key
	orderKeys := []string{"virtual_table_name", timeKey}
	if m.MsgType != datatype.MESSAGE_TYPE_DFSTATS && m.MsgType != datatype.MESSAGE_TYPE_SERVER_DFSTATS {
		// order key in universal tags
		orderKeys = append(orderKeys, "l3_epc_id")
		orderKeys = append(orderKeys, "ip4")
		orderKeys = append(orderKeys, "ip6")
	}

	return &ckdb.Table{
		Version:         common.CK_VERSION,
		Database:        m.DatabaseName(),
		DBType:          ckdbType,
		LocalName:       m.TableName() + ckdb.LOCAL_SUBFFIX,
		GlobalName:      m.TableName(),
		Columns:         m.Columns(),
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

// Check if there is a TagName/TagValue/MetricsName not in fieldCache or fieldValueCache, and store the newly appeared item in cache.
func (m *ExtMetrics) GenerateNewFlowTags(cache *flow_tag.FlowTagCache) {
	tableName := m.TableName()
	if m.VirtualTableName() != "" {
		tableName = m.VirtualTableName()
	}

	// reset temporary buffers
	flowTagInfo := &cache.FlowTagInfoBuffer
	*flowTagInfo = flow_tag.FlowTagInfo{
		Table:   tableName,
		VpcId:   m.UniversalTag.L3EpcID,
		PodNsId: m.UniversalTag.PodNSID,
		VtapId:  m.UniversalTag.VTAPID,
		OrgId:   m.OrgId,
		TeamID:  m.TeamID,
	}
	cache.Fields = cache.Fields[:0]
	cache.FieldValues = cache.FieldValues[:0]

	if !m.IsValid() {
		log.Warningf("ext metrics is invalid. %+v", m)
		return
	}
	// tags
	flowTagInfo.FieldType = flow_tag.FieldTag
	for i, name := range m.TagNames {
		flowTagInfo.FieldName = name

		// tag + value
		flowTagInfo.FieldValue = m.TagValues[i]
		if old, ok := cache.FieldValueCache.AddOrGet(*flowTagInfo, m.Timestamp); ok {
			if old+cache.CacheFlushTimeout >= m.Timestamp {
				// If there is no new fieldValue, of course there will be no new field.
				// So we can just skip the rest of the process in the loop.
				continue
			} else {
				cache.FieldValueCache.Add(*flowTagInfo, m.Timestamp)
			}
		}
		tagFieldValue := flow_tag.AcquireFlowTag(flow_tag.TagFieldValue)
		tagFieldValue.Timestamp = m.Timestamp
		tagFieldValue.FlowTagInfo = *flowTagInfo
		cache.FieldValues = append(cache.FieldValues, tagFieldValue)

		// only tag
		flowTagInfo.FieldValue = ""
		if old, ok := cache.FieldCache.AddOrGet(*flowTagInfo, m.Timestamp); ok {
			if old+cache.CacheFlushTimeout >= m.Timestamp {
				continue
			} else {
				cache.FieldCache.Add(*flowTagInfo, m.Timestamp)
			}
		}
		tagField := flow_tag.AcquireFlowTag(flow_tag.TagField)
		tagField.Timestamp = m.Timestamp
		tagField.FlowTagInfo = *flowTagInfo
		cache.Fields = append(cache.Fields, tagField)
	}

	// metrics
	flowTagInfo.FieldType = flow_tag.FieldMetrics
	flowTagInfo.FieldValue = ""
	for _, name := range m.MetricsFloatNames {
		flowTagInfo.FieldName = name
		if old, ok := cache.FieldCache.AddOrGet(*flowTagInfo, m.Timestamp); ok {
			if old+cache.CacheFlushTimeout >= m.Timestamp {
				continue
			} else {
				cache.FieldCache.Add(*flowTagInfo, m.Timestamp)
			}
		}
		tagField := flow_tag.AcquireFlowTag(flow_tag.TagField)
		tagField.Timestamp = m.Timestamp
		tagField.FlowTagInfo = *flowTagInfo
		cache.Fields = append(cache.Fields, tagField)
	}
}

var extMetricsPool = pool.NewLockFreePool(func() *ExtMetrics {
	return &ExtMetrics{}
})

func AcquireExtMetrics() *ExtMetrics {
	return extMetricsPool.Get()
}

var emptyUniversalTag = flow_metrics.UniversalTag{}

func ReleaseExtMetrics(m *ExtMetrics) {
	m.UniversalTag = emptyUniversalTag
	m.TagNames = m.TagNames[:0]
	m.TagValues = m.TagValues[:0]
	m.MetricsFloatNames = m.MetricsFloatNames[:0]
	m.MetricsFloatValues = m.MetricsFloatValues[:0]
	extMetricsPool.Put(m)
}
