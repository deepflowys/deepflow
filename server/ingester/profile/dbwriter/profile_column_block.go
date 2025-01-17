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

type ProfileBlock struct {
	ColTime                   proto.ColDateTime
	ColId                     proto.ColUInt64
	ColIp4                    proto.ColIPv4
	ColIp6                    proto.ColIPv6
	ColIsIpv4                 proto.ColUInt8
	ColAppService             *proto.ColLowCardinality[string]
	ColProfileLocationStr     proto.ColStr
	ColProfileValue           proto.ColInt64
	ColProfileValueUnit       *proto.ColLowCardinality[string]
	ColProfileEventType       *proto.ColLowCardinality[string]
	ColProfileCreateTimestamp proto.ColDateTime64
	ColProfileInTimestamp     proto.ColDateTime64
	ColProfileLanguageType    *proto.ColLowCardinality[string]
	ColProfileId              proto.ColStr
	ColTraceId                proto.ColStr
	ColSpanName               proto.ColStr
	ColAppInstance            *proto.ColLowCardinality[string]
	ColTagNames               *proto.ColArr[string]
	ColTagValues              *proto.ColArr[string]
	ColCompressionAlgo        *proto.ColLowCardinality[string]
	ColProcessId              proto.ColUInt32
	ColProcessStartTime       proto.ColDateTime64
	ColGprocessId             proto.ColUInt32
	ColAgentId                proto.ColUInt16
	ColRegionId               proto.ColUInt16
	ColAzId                   proto.ColUInt16
	ColSubnetId               proto.ColUInt16
	ColL3EpcId                proto.ColInt32
	ColHostId                 proto.ColUInt16
	ColPodId                  proto.ColUInt32
	ColPodNodeId              proto.ColUInt32
	ColPodNsId                proto.ColUInt16
	ColPodClusterId           proto.ColUInt16
	ColPodGroupId             proto.ColUInt32
	ColAutoInstanceId         proto.ColUInt32
	ColAutoInstanceType       proto.ColUInt8
	ColAutoServiceId          proto.ColUInt32
	ColAutoServiceType        proto.ColUInt8
	ColL3DeviceType           proto.ColUInt8
	ColL3DeviceId             proto.ColUInt32
	ColServiceId              proto.ColUInt32
	ColTeamId                 proto.ColUInt16
	*nativetag.NativeTagsBlock
}

func (b *ProfileBlock) Reset() {
	b.ColTime.Reset()
	b.ColId.Reset()
	b.ColIp4.Reset()
	b.ColIp6.Reset()
	b.ColIsIpv4.Reset()
	b.ColAppService.Reset()
	b.ColProfileLocationStr.Reset()
	b.ColProfileValue.Reset()
	b.ColProfileValueUnit.Reset()
	b.ColProfileEventType.Reset()
	b.ColProfileCreateTimestamp.Reset()
	b.ColProfileInTimestamp.Reset()
	b.ColProfileLanguageType.Reset()
	b.ColProfileId.Reset()
	b.ColTraceId.Reset()
	b.ColSpanName.Reset()
	b.ColAppInstance.Reset()
	b.ColTagNames.Reset()
	b.ColTagValues.Reset()
	b.ColCompressionAlgo.Reset()
	b.ColProcessId.Reset()
	b.ColProcessStartTime.Reset()
	b.ColGprocessId.Reset()
	b.ColAgentId.Reset()
	b.ColRegionId.Reset()
	b.ColAzId.Reset()
	b.ColSubnetId.Reset()
	b.ColL3EpcId.Reset()
	b.ColHostId.Reset()
	b.ColPodId.Reset()
	b.ColPodNodeId.Reset()
	b.ColPodNsId.Reset()
	b.ColPodClusterId.Reset()
	b.ColPodGroupId.Reset()
	b.ColAutoInstanceId.Reset()
	b.ColAutoInstanceType.Reset()
	b.ColAutoServiceId.Reset()
	b.ColAutoServiceType.Reset()
	b.ColL3DeviceType.Reset()
	b.ColL3DeviceId.Reset()
	b.ColServiceId.Reset()
	b.ColTeamId.Reset()
	if b.NativeTagsBlock != nil {
		b.NativeTagsBlock.Reset()
	}
}

func (b *ProfileBlock) ToInput(input proto.Input) proto.Input {
	input = append(input,
		proto.InputColumn{Name: ckdb.COLUMN_TIME, Data: &b.ColTime},
		proto.InputColumn{Name: ckdb.COLUMN__ID, Data: &b.ColId},
		proto.InputColumn{Name: ckdb.COLUMN_IP4, Data: &b.ColIp4},
		proto.InputColumn{Name: ckdb.COLUMN_IP6, Data: &b.ColIp6},
		proto.InputColumn{Name: ckdb.COLUMN_IS_IPV4, Data: &b.ColIsIpv4},
		proto.InputColumn{Name: ckdb.COLUMN_APP_SERVICE, Data: b.ColAppService},
		proto.InputColumn{Name: ckdb.COLUMN_PROFILE_LOCATION_STR, Data: &b.ColProfileLocationStr},
		proto.InputColumn{Name: ckdb.COLUMN_PROFILE_VALUE, Data: &b.ColProfileValue},
		proto.InputColumn{Name: ckdb.COLUMN_PROFILE_VALUE_UNIT, Data: b.ColProfileValueUnit},
		proto.InputColumn{Name: ckdb.COLUMN_PROFILE_EVENT_TYPE, Data: b.ColProfileEventType},
		proto.InputColumn{Name: ckdb.COLUMN_PROFILE_CREATE_TIMESTAMP, Data: &b.ColProfileCreateTimestamp},
		proto.InputColumn{Name: ckdb.COLUMN_PROFILE_IN_TIMESTAMP, Data: &b.ColProfileInTimestamp},
		proto.InputColumn{Name: ckdb.COLUMN_PROFILE_LANGUAGE_TYPE, Data: b.ColProfileLanguageType},
		proto.InputColumn{Name: ckdb.COLUMN_PROFILE_ID, Data: &b.ColProfileId},
		proto.InputColumn{Name: ckdb.COLUMN_TRACE_ID, Data: &b.ColTraceId},
		proto.InputColumn{Name: ckdb.COLUMN_SPAN_NAME, Data: &b.ColSpanName},
		proto.InputColumn{Name: ckdb.COLUMN_APP_INSTANCE, Data: b.ColAppInstance},
		proto.InputColumn{Name: ckdb.COLUMN_TAG_NAMES, Data: b.ColTagNames},
		proto.InputColumn{Name: ckdb.COLUMN_TAG_VALUES, Data: b.ColTagValues},
		proto.InputColumn{Name: ckdb.COLUMN_COMPRESSION_ALGO, Data: b.ColCompressionAlgo},
		proto.InputColumn{Name: ckdb.COLUMN_PROCESS_ID, Data: &b.ColProcessId},
		proto.InputColumn{Name: ckdb.COLUMN_PROCESS_START_TIME, Data: &b.ColProcessStartTime},
		proto.InputColumn{Name: ckdb.COLUMN_GPROCESS_ID, Data: &b.ColGprocessId},
		proto.InputColumn{Name: ckdb.COLUMN_AGENT_ID, Data: &b.ColAgentId},
		proto.InputColumn{Name: ckdb.COLUMN_REGION_ID, Data: &b.ColRegionId},
		proto.InputColumn{Name: ckdb.COLUMN_AZ_ID, Data: &b.ColAzId},
		proto.InputColumn{Name: ckdb.COLUMN_SUBNET_ID, Data: &b.ColSubnetId},
		proto.InputColumn{Name: ckdb.COLUMN_L3_EPC_ID, Data: &b.ColL3EpcId},
		proto.InputColumn{Name: ckdb.COLUMN_HOST_ID, Data: &b.ColHostId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_ID, Data: &b.ColPodId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_NODE_ID, Data: &b.ColPodNodeId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_NS_ID, Data: &b.ColPodNsId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_CLUSTER_ID, Data: &b.ColPodClusterId},
		proto.InputColumn{Name: ckdb.COLUMN_POD_GROUP_ID, Data: &b.ColPodGroupId},
		proto.InputColumn{Name: ckdb.COLUMN_AUTO_INSTANCE_ID, Data: &b.ColAutoInstanceId},
		proto.InputColumn{Name: ckdb.COLUMN_AUTO_INSTANCE_TYPE, Data: &b.ColAutoInstanceType},
		proto.InputColumn{Name: ckdb.COLUMN_AUTO_SERVICE_ID, Data: &b.ColAutoServiceId},
		proto.InputColumn{Name: ckdb.COLUMN_AUTO_SERVICE_TYPE, Data: &b.ColAutoServiceType},
		proto.InputColumn{Name: ckdb.COLUMN_L3_DEVICE_TYPE, Data: &b.ColL3DeviceType},
		proto.InputColumn{Name: ckdb.COLUMN_L3_DEVICE_ID, Data: &b.ColL3DeviceId},
		proto.InputColumn{Name: ckdb.COLUMN_SERVICE_ID, Data: &b.ColServiceId},
		proto.InputColumn{Name: ckdb.COLUMN_TEAM_ID, Data: &b.ColTeamId},
	)
	if b.NativeTagsBlock != nil {
		input = b.NativeTagsBlock.ToInput(input)
	}
	return input
}

func (n *InProcessProfile) NewColumnBlock() ckdb.CKColumnBlock {
	return &ProfileBlock{
		ColAppService:          new(proto.ColStr).LowCardinality(),
		ColProfileValueUnit:    new(proto.ColStr).LowCardinality(),
		ColProfileEventType:    new(proto.ColStr).LowCardinality(),
		ColProfileLanguageType: new(proto.ColStr).LowCardinality(),
		ColAppInstance:         new(proto.ColStr).LowCardinality(),
		ColCompressionAlgo:     new(proto.ColStr).LowCardinality(),
		ColTagNames:            new(proto.ColStr).LowCardinality().Array(),
		ColTagValues:           new(proto.ColStr).Array(),
		NativeTagsBlock:        nativetag.GetTableNativeTagsColumnBlock(n.OrgId, nativetag.PROFILE),
	}
}

func (n *InProcessProfile) AppendToColumnBlock(b ckdb.CKColumnBlock) {
	block := b.(*ProfileBlock)
	ckdb.AppendColDateTime(&block.ColTime, n.Time)
	block.ColId.Append(n._id)
	block.ColIp4.Append(proto.IPv4(n.IP4))
	ckdb.AppendIPv6(&block.ColIp6, n.IP6)
	block.ColIsIpv4.Append(*(*uint8)(unsafe.Pointer(&n.IsIPv4)))
	block.ColAppService.Append(n.AppService)
	block.ColProfileLocationStr.Append(n.ProfileLocationStr)
	block.ColProfileValue.Append(n.ProfileValue)
	block.ColProfileValueUnit.Append(n.ProfileValueUnit)
	block.ColProfileEventType.Append(n.ProfileEventType)
	ckdb.AppendColDateTime64Micro(&block.ColProfileCreateTimestamp, n.ProfileCreateTimestamp)
	ckdb.AppendColDateTime64Micro(&block.ColProfileInTimestamp, n.ProfileInTimestamp)
	block.ColProfileLanguageType.Append(n.ProfileLanguageType)
	block.ColProfileId.Append(n.ProfileID)
	block.ColTraceId.Append(n.TraceID)
	block.ColSpanName.Append(n.SpanName)
	block.ColAppInstance.Append(n.AppInstance)
	block.ColTagNames.Append(n.TagNames)
	block.ColTagValues.Append(n.TagValues)
	block.ColCompressionAlgo.Append(n.CompressionAlgo)
	block.ColProcessId.Append(n.ProcessID)
	ckdb.AppendColDateTime64Micro(&block.ColProcessStartTime, n.ProcessStartTime)
	block.ColGprocessId.Append(n.GPID)
	block.ColAgentId.Append(n.VtapID)
	block.ColRegionId.Append(n.RegionID)
	block.ColAzId.Append(n.AZID)
	block.ColSubnetId.Append(n.SubnetID)
	block.ColL3EpcId.Append(n.L3EpcID)
	block.ColHostId.Append(n.HostID)
	block.ColPodId.Append(n.PodID)
	block.ColPodNodeId.Append(n.PodNodeID)
	block.ColPodNsId.Append(n.PodNSID)
	block.ColPodClusterId.Append(n.PodClusterID)
	block.ColPodGroupId.Append(n.PodGroupID)
	block.ColAutoInstanceId.Append(n.AutoInstanceID)
	block.ColAutoInstanceType.Append(n.AutoInstanceType)
	block.ColAutoServiceId.Append(n.AutoServiceID)
	block.ColAutoServiceType.Append(n.AutoServiceType)
	block.ColL3DeviceType.Append(n.L3DeviceType)
	block.ColL3DeviceId.Append(n.L3DeviceID)
	block.ColServiceId.Append(n.ServiceID)
	block.ColTeamId.Append(n.TeamID)
	if block.NativeTagsBlock != nil {
		block.NativeTagsBlock.AppendToColumnBlock(n.TagNames, n.TagValues, nil, nil)
	}
}
