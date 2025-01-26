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
	"strings"

	"github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/ingester/flow_tag"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/datatype/prompb"
	flow_metrics "github.com/deepflowio/deepflow/server/libs/flow-metrics"
	"github.com/deepflowio/deepflow/server/libs/pool"
	"github.com/prometheus/common/model"
)

const (
	DefaultPartition = ckdb.TimeFuncTwoHour
)

type PrometheusSampleInterface interface {
	DatabaseName() string
	TableName() string
	OrgID() uint16
	Columns(int) []*ckdb.Column
	AppLabelLen() int
	GenCKTable(string, string, string, int, *ckdb.ColdStorage, int) *ckdb.Table
	GenerateNewFlowTags(*flow_tag.FlowTagCache, string, *prompb.TimeSeries, []prompb.Label, []uint32, []uint32)
	VpcId() int32
	PodNsId() uint16
	Release()

	NewColumnBlock() ckdb.CKColumnBlock
	AppendToColumnBlock(ckdb.CKColumnBlock)
	NativeTagVersion() uint32
}

type PrometheusSample struct {
	PrometheusSampleMini
	UniversalTag flow_metrics.UniversalTag
}

type PrometheusSampleMini struct {
	Timestamp uint32 // s
	VtapId    uint16
	MetricID  uint32
	TargetID  uint32

	// Not stored, only determines which database to store in.
	// When Orgid is 0 or 1, it is stored in database 'prometheus', otherwise stored in '<OrgId>_prometheus'.
	OrgId            uint16
	TeamID           uint16
	AppLabelValueIDs []uint32

	Value float64
}

func (m *PrometheusSampleMini) DatabaseName() string {
	return PROMETHEUS_DB
}

func (m *PrometheusSampleMini) TableName() string {
	return PROMETHEUS_TABLE
}

func (m *PrometheusSampleMini) AppLabelLen() int {
	return len(m.AppLabelValueIDs)
}

func (m *PrometheusSampleMini) VpcId() int32 {
	return 0
}

func (m *PrometheusSampleMini) PodNsId() uint16 {
	return 0
}

func (m *PrometheusSampleMini) NativeTagVersion() uint32 {
	return 0
}

func (m *PrometheusSampleMini) OrgID() uint16 {
	return m.OrgId
}

func (m *PrometheusSampleMini) Columns(appLabelColumnCount int) []*ckdb.Column {
	columns := []*ckdb.Column{}

	columns = append(columns, ckdb.NewColumnWithGroupBy("time", ckdb.DateTime))
	columns = append(columns,
		ckdb.NewColumn("metric_id", ckdb.UInt32).SetComment("encoded ID of the metric name"),
		ckdb.NewColumn("target_id", ckdb.UInt32).SetComment("the encoded ID of the target"),
		ckdb.NewColumn("team_id", ckdb.UInt16).SetComment("the team ID"),
	)
	for i := 1; i <= appLabelColumnCount; i++ {
		columns = append(columns, ckdb.NewColumn(fmt.Sprintf("app_label_value_id_%d", i), ckdb.UInt32))
	}
	columns = append(columns, ckdb.NewColumn("value", ckdb.Float64))

	return columns
}

func (m *PrometheusSampleMini) GenCKTable(cluster, storagePolicy, ckdbType string, ttl int, coldStorage *ckdb.ColdStorage, appLabelColumnCount int) *ckdb.Table {
	timeKey := "time"
	engine := ckdb.MergeTree
	// order key
	orderKeys := []string{"metric_id", timeKey, "target_id"}

	return &ckdb.Table{
		Version:         common.CK_VERSION,
		Database:        m.DatabaseName(),
		DBType:          ckdbType,
		LocalName:       m.TableName() + ckdb.LOCAL_SUBFFIX,
		GlobalName:      m.TableName(),
		Columns:         m.Columns(appLabelColumnCount),
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
func (m *PrometheusSampleMini) GenerateNewFlowTags(cache *flow_tag.FlowTagCache, metricName string, timeSeries *prompb.TimeSeries, extraLabels []prompb.Label, tsLabelNameIDs, tsLabelValueIDs []uint32) {
	// reset temporary buffers
	flowTagInfo := &cache.FlowTagInfoBuffer
	*flowTagInfo = flow_tag.FlowTagInfo{
		FieldType: flow_tag.FieldTag,
		TableId:   m.MetricID,
		VpcId:     m.VpcId(),
		PodNsId:   m.PodNsId(),
		VtapId:    m.VtapId,
		TeamID:    m.TeamID,
		OrgId:     m.OrgId,
	}
	cache.Fields = cache.Fields[:0]
	cache.FieldValues = cache.FieldValues[:0]

	// j is used for tsLabelNameIDs/tsLabelValueIDs index, will add 1 at first
	j := -1
	metricHasSkipped := false // prevent repeated judgment of the same string
	tsLen, extraLen := len(timeSeries.Labels), len(extraLabels)
	var label *prompb.Label
	for i := 0; i < tsLen+extraLen; i++ {
		if i < tsLen {
			label = &timeSeries.Labels[i]
		} else {
			label = &extraLabels[i-tsLen]
		}
		if !metricHasSkipped && label.Name == model.MetricNameLabel {
			metricHasSkipped = true
			continue
		}
		j++

		// tag + value
		flowTagInfo.FieldNameId = tsLabelNameIDs[j]
		flowTagInfo.FieldValueId = tsLabelValueIDs[j]
		lruKey1, lruKey2 := genLru128Key(flowTagInfo)
		if old := cache.PrometheusFieldValueCache.AddOrGet(lruKey1, lruKey2, m.Timestamp); old != nil {
			oldv, _ := (*old).(uint32)
			if oldv+cache.CacheFlushTimeout >= m.Timestamp {
				// If there is no new fieldValue, of course there will be no new field.
				// So we can just skip the rest of the process in the loop.
				continue
			} else {
				*old = m.Timestamp
			}
		}
		// metricNames,labelName,labelValue is get from promb.TimeSeries with unsafe string pointer, it need clone
		if flowTagInfo.Table == "" {
			flowTagInfo.Table = strings.Clone(metricName)
		}
		fieldName := strings.Clone(label.Name)

		tagFieldValue := flow_tag.AcquireFlowTag(flow_tag.TagFieldValue)
		tagFieldValue.Timestamp = m.Timestamp
		tagFieldValue.FlowTagInfo = *flowTagInfo
		tagFieldValue.FlowTagInfo.FieldName = fieldName
		tagFieldValue.FlowTagInfo.FieldValue = strings.Clone(label.Value)
		cache.FieldValues = append(cache.FieldValues, tagFieldValue)

		// only tag
		flowTagInfo.FieldValueId = 0
		lruKey1, lruKey2 = genLru128Key(flowTagInfo)
		if old := cache.PrometheusFieldCache.AddOrGet(lruKey1, lruKey2, m.Timestamp); old != nil {
			oldv, _ := (*old).(uint32)
			if oldv+cache.CacheFlushTimeout >= m.Timestamp {
				continue
			} else {
				*old = m.Timestamp
			}
		}
		tagField := flow_tag.AcquireFlowTag(flow_tag.TagField)
		tagField.Timestamp = m.Timestamp
		tagField.FlowTagInfo = *flowTagInfo
		tagField.FlowTagInfo.FieldName = fieldName
		cache.Fields = append(cache.Fields, tagField)
	}
}

func (m *PrometheusSampleMini) Release() {
	ReleasePrometheusSampleMini(m)
}

func (m *PrometheusSample) DatabaseName() string {
	return m.PrometheusSampleMini.DatabaseName()
}

func (m *PrometheusSample) TableName() string {
	return m.PrometheusSampleMini.DatabaseName()
}

func (m *PrometheusSample) NativeTagVersion() uint32 {
	return 0
}

func (m *PrometheusSample) OrgID() uint16 {
	return m.OrgId
}

func (m *PrometheusSample) Columns(appLabelColumnCount int) []*ckdb.Column {
	columns := m.PrometheusSampleMini.Columns(appLabelColumnCount)
	columns = flow_metrics.GenUniversalTagColumns(columns)
	return columns
}

func (m *PrometheusSample) Release() {
	ReleasePrometheusSample(m)
}

func (m *PrometheusSample) GenCKTable(cluster, storagePolicy, ckdbType string, ttl int, coldStorage *ckdb.ColdStorage, appLabelColumnCount int) *ckdb.Table {
	table := m.PrometheusSampleMini.GenCKTable(cluster, storagePolicy, ckdbType, ttl, coldStorage, appLabelColumnCount)
	table.Columns = m.Columns(appLabelColumnCount)
	return table
}

func genLru128Key(f *flow_tag.FlowTagInfo) (uint64, uint64) {
	return uint64(f.TableId)<<32 | uint64(f.FieldNameId), uint64(f.FieldValueId)<<32 | uint64(f.OrgId)<<16 | uint64(f.TeamID)
}

func (m *PrometheusSample) VpcId() int32 {
	return m.UniversalTag.L3EpcID
}

func (m *PrometheusSample) PodNsId() uint16 {
	return m.UniversalTag.PodNSID
}

// Check if there is a TagName/TagValue/MetricsName not in fieldCache or fieldValueCache, and store the newly appeared item in cache.
func (m *PrometheusSample) GenerateNewFlowTags(cache *flow_tag.FlowTagCache, metricName string, timeSeries *prompb.TimeSeries, extraLabels []prompb.Label, tsLabelNameIDs, tsLabelValueIDs []uint32) {
	m.PrometheusSampleMini.GenerateNewFlowTags(cache, metricName, timeSeries, extraLabels, tsLabelNameIDs, tsLabelValueIDs)
}

var prometheusSampleMiniPool = pool.NewLockFreePool(func() *PrometheusSampleMini {
	return &PrometheusSampleMini{}
})

func AcquirePrometheusSampleMini() *PrometheusSampleMini {
	return prometheusSampleMiniPool.Get()
}

func ReleasePrometheusSampleMini(p *PrometheusSampleMini) {
	p.AppLabelValueIDs = p.AppLabelValueIDs[:0]
	prometheusSampleMiniPool.Put(p)
}

var prometheusSamplePool = pool.NewLockFreePool(func() *PrometheusSample {
	return &PrometheusSample{}
})

func AcquirePrometheusSample() *PrometheusSample {
	return prometheusSamplePool.Get()
}

var emptyUniversalTag = flow_metrics.UniversalTag{}

func ReleasePrometheusSample(p *PrometheusSample) {
	p.UniversalTag = emptyUniversalTag
	p.AppLabelValueIDs = p.AppLabelValueIDs[:0]
	prometheusSamplePool.Put(p)
}
