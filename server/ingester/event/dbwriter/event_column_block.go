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
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/nativetag"
)

type EventBlock struct {
	ColTime             proto.ColDateTime
	ColId               proto.ColUInt64
	ColStartTime        proto.ColDateTime64
	ColEndTime          proto.ColDateTime64
	ColTagged           proto.ColUInt8
	ColSignalSource     proto.ColUInt8
	ColEventType        *proto.ColLowCardinality[string]
	ColEventDesc        proto.ColStr
	ColProcessKname     proto.ColStr
	ColGprocessId       proto.ColUInt32
	ColRegionId         proto.ColUInt16
	ColAzId             proto.ColUInt16
	ColL3EpcId          proto.ColInt32
	ColHostId           proto.ColUInt16
	ColPodId            proto.ColUInt32
	ColPodNodeId        proto.ColUInt32
	ColPodNsId          proto.ColUInt16
	ColPodClusterId     proto.ColUInt16
	ColPodGroupId       proto.ColUInt32
	ColL3DeviceType     proto.ColUInt8
	ColL3DeviceId       proto.ColUInt32
	ColServiceId        proto.ColUInt32
	ColAgentId          proto.ColUInt16
	ColSubnetId         proto.ColUInt16
	ColIsIpv4           proto.ColUInt8
	ColIp4              proto.ColIPv4
	ColIp6              proto.ColIPv6
	ColTeamId           proto.ColUInt16
	ColAutoInstanceId   proto.ColUInt32
	ColAutoInstanceType proto.ColUInt8
	ColAutoServiceId    proto.ColUInt32
	ColAutoServiceType  proto.ColUInt8
	ColAppInstance      proto.ColStr
	ColAttributeNames   *proto.ColArr[string]
	ColAttributeValues  *proto.ColArr[string]

	HasMetrics  bool
	ColBytes    proto.ColUInt32
	ColDuration proto.ColUInt64

	*nativetag.NativeTagsBlock
}

func (b *EventBlock) Reset() {
	b.ColTime.Reset()
	b.ColId.Reset()
	b.ColStartTime.Reset()
	b.ColEndTime.Reset()
	b.ColTagged.Reset()
	b.ColSignalSource.Reset()
	b.ColEventType.Reset()
	b.ColEventDesc.Reset()
	b.ColProcessKname.Reset()
	b.ColGprocessId.Reset()
	b.ColRegionId.Reset()
	b.ColAzId.Reset()
	b.ColL3EpcId.Reset()
	b.ColHostId.Reset()
	b.ColPodId.Reset()
	b.ColPodNodeId.Reset()
	b.ColPodNsId.Reset()
	b.ColPodClusterId.Reset()
	b.ColPodGroupId.Reset()
	b.ColL3DeviceType.Reset()
	b.ColL3DeviceId.Reset()
	b.ColServiceId.Reset()
	b.ColAgentId.Reset()
	b.ColSubnetId.Reset()
	b.ColIsIpv4.Reset()
	b.ColIp4.Reset()
	b.ColIp6.Reset()
	b.ColTeamId.Reset()
	b.ColAutoInstanceId.Reset()
	b.ColAutoInstanceType.Reset()
	b.ColAutoServiceId.Reset()
	b.ColAutoServiceType.Reset()
	b.ColAppInstance.Reset()
	b.ColAttributeNames.Reset()
	b.ColAttributeValues.Reset()
	b.ColBytes.Reset()
	b.ColDuration.Reset()
	if b.NativeTagsBlock != nil {
		b.NativeTagsBlock.Reset()
	}
}

func (b *EventBlock) ToInput(input proto.Input) proto.Input {
	input = append(input,
		proto.InputColumn{Name: ckdb.COLUMN_TIME, Data: &b.ColTime},
		proto.InputColumn{Name: ckdb.COLUMN__ID, Data: &b.ColId},
		proto.InputColumn{Name: ckdb.COLUMN_START_TIME, Data: &b.ColStartTime},
		proto.InputColumn{Name: ckdb.COLUMN_END_TIME, Data: &b.ColEndTime},
		proto.InputColumn{Name: ckdb.COLUMN_TAGGED, Data: &b.ColTagged},
		proto.InputColumn{Name: ckdb.COLUMN_SIGNAL_SOURCE, Data: &b.ColSignalSource},
		proto.InputColumn{Name: ckdb.COLUMN_EVENT_TYPE, Data: b.ColEventType},
		proto.InputColumn{Name: ckdb.COLUMN_EVENT_DESC, Data: &b.ColEventDesc},
		proto.InputColumn{Name: ckdb.COLUMN_PROCESS_KNAME, Data: &b.ColProcessKname},
		proto.InputColumn{Name: ckdb.COLUMN_GPROCESS_ID, Data: &b.ColGprocessId},
		proto.InputColumn{Name: ckdb.COLUMN_REGION_ID, Data: &b.ColRegionId},
		proto.InputColumn{Name: ckdb.COLUMN_AZ_ID, Data: &b.ColAzId},
		proto.InputColumn{Name: ckdb.COLUMN_L3_EPC_ID, Data: &b.ColL3EpcId},
		proto.InputColumn{Name: ckdb.COLUMN_HOST_ID, Data: &b.ColHostId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_ID, Data: &b.ColPodId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_NODE_ID, Data: &b.ColPodNodeId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_NS_ID, Data: &b.ColPodNsId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_CLUSTER_ID, Data: &b.ColPodClusterId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_GROUP_ID, Data: &b.ColPodGroupId},
		proto.InputColumn{Name: ckdb.COLUMN_L3_DEVICE_TYPE, Data: &b.ColL3DeviceType},
		proto.InputColumn{Name: ckdb.COLUMN_L3_DEVICE_ID, Data: &b.ColL3DeviceId},
		proto.InputColumn{Name: ckdb.COLUMN_SERVICE_ID, Data: &b.ColServiceId},
		proto.InputColumn{Name: ckdb.COLUMN_AGENT_ID, Data: &b.ColAgentId},
		proto.InputColumn{Name: ckdb.COLUMN_SUBNET_ID, Data: &b.ColSubnetId},
		proto.InputColumn{Name: ckdb.COLUMN_IS_IPV4, Data: &b.ColIsIpv4},
		proto.InputColumn{Name: ckdb.COLUMN_IP4, Data: &b.ColIp4},
		proto.InputColumn{Name: ckdb.COLUMN_IP6, Data: &b.ColIp6},
		proto.InputColumn{Name: ckdb.COLUMN_TEAM_ID, Data: &b.ColTeamId},
		proto.InputColumn{Name: ckdb.COLUMN_AUTO_INSTANCE_ID, Data: &b.ColAutoInstanceId},
		proto.InputColumn{Name: ckdb.COLUMN_AUTO_INSTANCE_TYPE, Data: &b.ColAutoInstanceType},
		proto.InputColumn{Name: ckdb.COLUMN_AUTO_SERVICE_ID, Data: &b.ColAutoServiceId},
		proto.InputColumn{Name: ckdb.COLUMN_AUTO_SERVICE_TYPE, Data: &b.ColAutoServiceType},
		proto.InputColumn{Name: ckdb.COLUMN_APP_INSTANCE, Data: &b.ColAppInstance},
		proto.InputColumn{Name: ckdb.COLUMN_ATTRIBUTE_NAMES, Data: b.ColAttributeNames},
		proto.InputColumn{Name: ckdb.COLUMN_ATTRIBUTE_VALUES, Data: b.ColAttributeValues},
	)
	if b.HasMetrics {
		input = append(input,
			proto.InputColumn{Name: ckdb.COLUMN_BYTES, Data: &b.ColBytes},
			proto.InputColumn{Name: ckdb.COLUMN_DURATION, Data: &b.ColDuration},
		)
	}
	if b.NativeTagsBlock != nil {
		input = b.NativeTagsBlock.ToInput(input)
	}
	return input
}

func (n *EventStore) NewColumnBlock() ckdb.CKColumnBlock {
	b := &EventBlock{
		HasMetrics:         n.HasMetrics,
		ColEventType:       new(proto.ColStr).LowCardinality(),
		ColAttributeNames:  new(proto.ColStr).LowCardinality().Array(),
		ColAttributeValues: new(proto.ColStr).Array(),
	}
	if n.HasMetrics {
		b.NativeTagsBlock = nativetag.GetTableNativeTagsColumnBlock(n.OrgId, nativetag.EVENT_PERF_EVENT)
	} else {
		b.NativeTagsBlock = nativetag.GetTableNativeTagsColumnBlock(n.OrgId, nativetag.EVENT_EVENT)
	}
	return b
}

func (n *EventStore) AppendToColumnBlock(b ckdb.CKColumnBlock) {
	block := b.(*EventBlock)
	ckdb.AppendColDateTime(&block.ColTime, n.Time)
	block.ColId.Append(n._id)
	ckdb.AppendColDateTime64Micro(&block.ColStartTime, n.StartTime)
	ckdb.AppendColDateTime64Micro(&block.ColEndTime, n.EndTime)
	block.ColTagged.Append(n.Tagged)
	block.ColSignalSource.Append(n.SignalSource)
	block.ColEventType.Append(n.EventType)
	block.ColEventDesc.Append(n.EventDescription)
	block.ColProcessKname.Append(n.ProcessKName)
	block.ColGprocessId.Append(n.GProcessID)
	block.ColRegionId.Append(n.RegionID)
	block.ColAzId.Append(n.AZID)
	block.ColL3EpcId.Append(n.L3EpcID)
	block.ColHostId.Append(n.HostID)
	block.ColPodId.Append(n.PodID)
	block.ColPodNodeId.Append(n.PodNodeID)
	block.ColPodNsId.Append(n.PodNSID)
	block.ColPodClusterId.Append(n.PodClusterID)
	block.ColPodGroupId.Append(n.PodGroupID)
	block.ColL3DeviceType.Append(n.L3DeviceType)
	block.ColL3DeviceId.Append(n.L3DeviceID)
	block.ColServiceId.Append(n.ServiceID)
	block.ColAgentId.Append(n.VTAPID)
	block.ColSubnetId.Append(n.SubnetID)
	block.ColIsIpv4.Append(*(*uint8)(unsafe.Pointer(&n.IsIPv4)))
	block.ColIp4.Append(proto.IPv4(n.IP4))
	ckdb.AppendIPv6(&block.ColIp6, n.IP6)
	block.ColTeamId.Append(n.TeamID)
	block.ColAutoInstanceId.Append(n.AutoInstanceID)
	block.ColAutoInstanceType.Append(n.AutoInstanceType)
	block.ColAutoServiceId.Append(n.AutoServiceID)
	block.ColAutoServiceType.Append(n.AutoServiceType)
	block.ColAppInstance.Append(n.AppInstance)
	block.ColAttributeNames.Append(n.AttributeNames)
	block.ColAttributeValues.Append(n.AttributeValues)
	block.ColBytes.Append(n.Bytes)
	block.ColDuration.Append(n.Duration)
	if block.NativeTagsBlock != nil {
		block.NativeTagsBlock.AppendToColumnBlock(n.AttributeNames, n.AttributeValues, nil, nil)
	}
}
