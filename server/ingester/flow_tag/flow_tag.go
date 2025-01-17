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

package flow_tag

import (
	"github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/pool"
)

const (
	FLOW_TAG_DB = "flow_tag"
)

type TagType uint8

const (
	TagField TagType = iota
	TagFieldValue
	TagTypeMax
)

func (t TagType) String() string {
	switch t {
	case TagField:
		return "custom_field"
	case TagFieldValue:
		return "custom_field_value"
	default:
		return "invalid tag type"
	}
}

type FieldType uint8

const (
	FieldTag FieldType = iota
	FieldMetrics
)

func (t FieldType) String() string {
	switch t {
	case FieldTag:
		return "tag"
	case FieldMetrics:
		return "metrics"
	default:
		return "invalid field type"
	}
}

type FieldValueType uint8

const (
	FieldValueTypeAuto FieldValueType = iota
	FieldValueTypeString
	FieldValueTypeFloat
	FieldValueTypeInt
)

func (t FieldValueType) String() string {
	switch t {
	case FieldValueTypeString:
		return "string"
	case FieldValueTypeFloat:
		return "float"
	case FieldValueTypeInt:
		return "int"
	default:
		return "invalid"
	}
}

// This structure will be used as a map key, and it is hoped to be as compact as possible in terms of memory layout.
// In addition, in order to distinguish as early as possible when comparing two values, put the highly distinguishable fields at the front.
type FlowTagInfo struct {
	Table          string // Represents virtual_table_name in ext_metrics
	FieldName      string
	FieldValue     string
	FieldValueType FieldValueType
	VtapId         uint16

	// IDs only for prometheus
	TableId      uint32
	FieldNameId  uint32
	FieldValueId uint32

	VpcId     int32 // XXX: can use int16
	PodNsId   uint16
	FieldType FieldType

	// Not stored, only determines which database to store in.
	// When Orgid is 0 or 1, it is stored in database 'flow_tag', otherwise stored in '<OrgId>_flow_tag'.
	OrgId  uint16
	TeamID uint16
}

type FlowTag struct {
	pool.ReferenceCount
	TagType

	Timestamp uint32 // s
	FlowTagInfo
}

func (t *FlowTag) NativeTagVersion() uint32 {
	return 0
}

func (t *FlowTag) OrgID() uint16 {
	return t.OrgId
}

func (t *FlowTag) Columns() []*ckdb.Column {
	columns := []*ckdb.Column{}
	columns = append(columns,
		ckdb.NewColumn("time", ckdb.DateTime),
		ckdb.NewColumn("table", ckdb.LowCardinalityString),
		ckdb.NewColumn("vpc_id", ckdb.Int32),
		ckdb.NewColumn("pod_ns_id", ckdb.UInt16),
		ckdb.NewColumn("field_type", ckdb.LowCardinalityString).SetComment("value: tag, metrics"),
		ckdb.NewColumn("field_name", ckdb.LowCardinalityString),
		ckdb.NewColumn("field_value_type", ckdb.LowCardinalityString).SetComment("value: string, float, int"),
		ckdb.NewColumn("team_id", ckdb.UInt16),
	)
	if t.TagType == TagFieldValue {
		columns = append(columns,
			ckdb.NewColumn("field_value", ckdb.String),
			ckdb.NewColumn("count", ckdb.UInt64))
	}
	return columns
}

func (t *FlowTag) GenCKTable(cluster, storagePolicy, tableName, ckdbType string, ttl int, partition ckdb.TimeFuncType) *ckdb.Table {
	timeKey := "time"
	engine := ckdb.ReplacingMergeTree

	orderKeys := []string{
		"table", "field_type", "field_name", "field_value_type",
	}
	if t.TagType == TagFieldValue {
		orderKeys = append(orderKeys, "field_value")
		engine = ckdb.SummingMergeTree
	}

	return &ckdb.Table{
		Version:         common.CK_VERSION,
		Database:        FLOW_TAG_DB,
		DBType:          ckdbType,
		LocalName:       tableName + ckdb.LOCAL_SUBFFIX,
		GlobalName:      tableName,
		Columns:         t.Columns(),
		TimeKey:         timeKey,
		SummingKey:      "count", // FIXME: not used yet.
		TTL:             ttl,
		PartitionFunc:   partition,
		Engine:          engine,
		Cluster:         cluster,
		StoragePolicy:   storagePolicy,
		OrderKeys:       orderKeys,
		PrimaryKeyCount: len(orderKeys),
	}
}

func (t *FlowTag) Release() {
	ReleaseFlowTag(t)
}

var flowTagPool = pool.NewLockFreePool(func() *FlowTag {
	return &FlowTag{}
})

func AcquireFlowTag(tagType TagType) *FlowTag {
	f := flowTagPool.Get()
	f.ReferenceCount.Reset()
	f.TagType = tagType
	return f
}

var emptyFlowTag = FlowTag{}

func ReleaseFlowTag(t *FlowTag) {
	if t == nil || t.SubReferenceCount() {
		return
	}
	*t = emptyFlowTag
	flowTagPool.Put(t)
}
