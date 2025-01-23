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
	"github.com/ClickHouse/ch-go/proto"

	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/datatype"
	flow_metrics "github.com/deepflowio/deepflow/server/libs/flow-metrics"
	"github.com/deepflowio/deepflow/server/libs/utils"
)

type ExtMetricsBlock struct {
	MsgType datatype.MessageType
	ColTime proto.ColDateTime
	*flow_metrics.UniversalTagBlock
	ColVirtualTableName   *proto.ColLowCardinality[string]
	ColTeamId             proto.ColUInt16
	ColTagNames           *proto.ColArr[string]
	ColTagValues          *proto.ColArr[string]
	ColMetricsFloatNames  *proto.ColArr[string]
	ColMetricsFloatValues *proto.ColArr[float64]
	ColHost               *proto.ColLowCardinality[string]
}

func (b *ExtMetricsBlock) Reset() {
	b.ColTime.Reset()
	if b.MsgType != datatype.MESSAGE_TYPE_DFSTATS && b.MsgType != datatype.MESSAGE_TYPE_SERVER_DFSTATS {
		b.UniversalTagBlock.Reset()
	}
	b.ColVirtualTableName.Reset()
	b.ColTeamId.Reset()
	b.ColTagNames.Reset()
	b.ColTagValues.Reset()
	b.ColMetricsFloatNames.Reset()
	b.ColMetricsFloatValues.Reset()
	b.ColHost.Reset()
}

func (b *ExtMetricsBlock) ToInput(input proto.Input) proto.Input {
	input = append(input,
		proto.InputColumn{Name: ckdb.COLUMN_TIME, Data: &b.ColTime},
		proto.InputColumn{Name: ckdb.COLUMN_VIRTUAL_TABLE_NAME, Data: b.ColVirtualTableName},
		proto.InputColumn{Name: ckdb.COLUMN_TEAM_ID, Data: &b.ColTeamId},
		proto.InputColumn{Name: ckdb.COLUMN_TAG_NAMES, Data: b.ColTagNames},
		proto.InputColumn{Name: ckdb.COLUMN_TAG_VALUES, Data: b.ColTagValues},
		proto.InputColumn{Name: ckdb.COLUMN_METRICS_FLOAT_NAMES, Data: b.ColMetricsFloatNames},
		proto.InputColumn{Name: ckdb.COLUMN_METRICS_FLOAT_VALUES, Data: b.ColMetricsFloatValues},
		proto.InputColumn{Name: ckdb.COLUMN_HOST, Data: b.ColHost},
	)
	if b.MsgType != datatype.MESSAGE_TYPE_DFSTATS && b.MsgType != datatype.MESSAGE_TYPE_SERVER_DFSTATS {
		input = b.UniversalTagBlock.ToInput(input)
	}
	return input
}

func (n *ExtMetrics) NewColumnBlock() ckdb.CKColumnBlock {
	return &ExtMetricsBlock{
		MsgType:               n.MsgType,
		UniversalTagBlock:     n.UniversalTag.NewColumnBlock().(*flow_metrics.UniversalTagBlock),
		ColVirtualTableName:   new(proto.ColStr).LowCardinality(),
		ColTagNames:           new(proto.ColStr).LowCardinality().Array(),
		ColTagValues:          new(proto.ColStr).Array(),
		ColMetricsFloatNames:  new(proto.ColStr).LowCardinality().Array(),
		ColMetricsFloatValues: new(proto.ColFloat64).Array(),
		ColHost:               new(proto.ColStr).LowCardinality(),
	}
}

func (n *ExtMetrics) AppendToColumnBlock(b ckdb.CKColumnBlock) {
	block := b.(*ExtMetricsBlock)
	ckdb.AppendColDateTime(&block.ColTime, n.Timestamp)
	if n.MsgType != datatype.MESSAGE_TYPE_DFSTATS && n.MsgType != datatype.MESSAGE_TYPE_SERVER_DFSTATS {
		t := &n.UniversalTag
		t.AppendToColumnBlock(block.UniversalTagBlock)
	}
	block.ColVirtualTableName.Append(n.VTableName)
	block.ColTeamId.Append(n.TeamID)
	block.ColTagNames.Append(n.TagNames)
	block.ColTagValues.Append(n.TagValues)
	block.ColMetricsFloatNames.Append(n.MetricsFloatNames)
	block.ColMetricsFloatValues.Append(n.MetricsFloatValues)
	if i := utils.IndexOf(n.TagNames, ckdb.COLUMN_HOST); i >= 0 {
		block.ColHost.Append(n.TagValues[i])
	} else {
		block.ColHost.Append("")
	}
}
