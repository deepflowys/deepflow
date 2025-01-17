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
package log_data

import (
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/nativetag"
)

type L7BaseBlock struct {
	*KnowledgeGraphBlock
	ColTime                   proto.ColDateTime
	ColIp40                   proto.ColIPv4
	ColIp41                   proto.ColIPv4
	ColIp60                   proto.ColIPv6
	ColIp61                   proto.ColIPv6
	ColIsIpv4                 proto.ColUInt8
	ColProtocol               proto.ColUInt8
	ColClientPort             proto.ColUInt16
	ColServerPort             proto.ColUInt16
	ColFlowId                 proto.ColUInt64
	ColCaptureNetworkTypeId   proto.ColUInt8
	ColNatSource              proto.ColUInt8
	ColCaptureNicType         proto.ColUInt8
	ColSignalSource           proto.ColUInt16
	ColTunnelType             proto.ColUInt8
	ColCaptureNic             proto.ColUInt32
	ColObservationPoint       *proto.ColLowCardinality[string]
	ColAgentId                proto.ColUInt16
	ColReqTcpSeq              proto.ColUInt32
	ColRespTcpSeq             proto.ColUInt32
	ColStartTime              proto.ColDateTime64
	ColEndTime                proto.ColDateTime64
	ColGprocessId0            proto.ColUInt32
	ColGprocessId1            proto.ColUInt32
	ColBizType                proto.ColUInt8
	ColProcessId0             proto.ColInt32
	ColProcessId1             proto.ColInt32
	ColProcessKname0          proto.ColStr
	ColProcessKname1          proto.ColStr
	ColSyscallTraceIdRequest  proto.ColUInt64
	ColSyscallTraceIdResponse proto.ColUInt64
	ColSyscallThread0         proto.ColUInt32
	ColSyscallThread1         proto.ColUInt32
	ColSyscallCoroutine0      proto.ColUInt64
	ColSyscallCoroutine1      proto.ColUInt64
	ColSyscallCapSeq0         proto.ColUInt32
	ColSyscallCapSeq1         proto.ColUInt32
}

func (b *L7BaseBlock) Reset() {
	b.KnowledgeGraphBlock.Reset()
	b.ColTime.Reset()
	b.ColIp40.Reset()
	b.ColIp41.Reset()
	b.ColIp60.Reset()
	b.ColIp61.Reset()
	b.ColIsIpv4.Reset()
	b.ColProtocol.Reset()
	b.ColClientPort.Reset()
	b.ColServerPort.Reset()
	b.ColFlowId.Reset()
	b.ColCaptureNetworkTypeId.Reset()
	b.ColNatSource.Reset()
	b.ColCaptureNicType.Reset()
	b.ColSignalSource.Reset()
	b.ColTunnelType.Reset()
	b.ColCaptureNic.Reset()
	b.ColObservationPoint.Reset()
	b.ColAgentId.Reset()
	b.ColReqTcpSeq.Reset()
	b.ColRespTcpSeq.Reset()
	b.ColStartTime.Reset()
	b.ColEndTime.Reset()
	b.ColGprocessId0.Reset()
	b.ColGprocessId1.Reset()
	b.ColBizType.Reset()
	b.ColProcessId0.Reset()
	b.ColProcessId1.Reset()
	b.ColProcessKname0.Reset()
	b.ColProcessKname1.Reset()
	b.ColSyscallTraceIdRequest.Reset()
	b.ColSyscallTraceIdResponse.Reset()
	b.ColSyscallThread0.Reset()
	b.ColSyscallThread1.Reset()
	b.ColSyscallCoroutine0.Reset()
	b.ColSyscallCoroutine1.Reset()
	b.ColSyscallCapSeq0.Reset()
	b.ColSyscallCapSeq1.Reset()
}

func (b *L7BaseBlock) ToInput(input proto.Input) proto.Input {
	input = b.KnowledgeGraphBlock.ToInput(input)
	return append(input,
		proto.InputColumn{Name: ckdb.COLUMN_TIME, Data: &b.ColTime},
		proto.InputColumn{Name: ckdb.COLUMN_IP4_0, Data: &b.ColIp40},
		proto.InputColumn{Name: ckdb.COLUMN_IP4_1, Data: &b.ColIp41},
		proto.InputColumn{Name: ckdb.COLUMN_IP6_0, Data: &b.ColIp60},
		proto.InputColumn{Name: ckdb.COLUMN_IP6_1, Data: &b.ColIp61},
		proto.InputColumn{Name: ckdb.COLUMN_IS_IPV4, Data: &b.ColIsIpv4},
		proto.InputColumn{Name: ckdb.COLUMN_PROTOCOL, Data: &b.ColProtocol},
		proto.InputColumn{Name: ckdb.COLUMN_CLIENT_PORT, Data: &b.ColClientPort},
		proto.InputColumn{Name: ckdb.COLUMN_SERVER_PORT, Data: &b.ColServerPort},
		proto.InputColumn{Name: ckdb.COLUMN_FLOW_ID, Data: &b.ColFlowId},
		proto.InputColumn{Name: ckdb.COLUMN_CAPTURE_NETWORK_TYPE_ID, Data: &b.ColCaptureNetworkTypeId},
		proto.InputColumn{Name: ckdb.COLUMN_NAT_SOURCE, Data: &b.ColNatSource},
		proto.InputColumn{Name: ckdb.COLUMN_CAPTURE_NIC_TYPE, Data: &b.ColCaptureNicType},
		proto.InputColumn{Name: ckdb.COLUMN_SIGNAL_SOURCE, Data: &b.ColSignalSource},
		proto.InputColumn{Name: ckdb.COLUMN_TUNNEL_TYPE, Data: &b.ColTunnelType},
		proto.InputColumn{Name: ckdb.COLUMN_CAPTURE_NIC, Data: &b.ColCaptureNic},
		proto.InputColumn{Name: ckdb.COLUMN_OBSERVATION_POINT, Data: b.ColObservationPoint},
		proto.InputColumn{Name: ckdb.COLUMN_AGENT_ID, Data: &b.ColAgentId},
		proto.InputColumn{Name: ckdb.COLUMN_REQ_TCP_SEQ, Data: &b.ColReqTcpSeq},
		proto.InputColumn{Name: ckdb.COLUMN_RESP_TCP_SEQ, Data: &b.ColRespTcpSeq},
		proto.InputColumn{Name: ckdb.COLUMN_START_TIME, Data: &b.ColStartTime},
		proto.InputColumn{Name: ckdb.COLUMN_END_TIME, Data: &b.ColEndTime},
		proto.InputColumn{Name: ckdb.COLUMN_GPROCESS_ID_0, Data: &b.ColGprocessId0},
		proto.InputColumn{Name: ckdb.COLUMN_GPROCESS_ID_1, Data: &b.ColGprocessId1},
		proto.InputColumn{Name: ckdb.COLUMN_BIZ_TYPE, Data: &b.ColBizType},
		proto.InputColumn{Name: ckdb.COLUMN_PROCESS_ID_0, Data: &b.ColProcessId0},
		proto.InputColumn{Name: ckdb.COLUMN_PROCESS_ID_1, Data: &b.ColProcessId1},
		proto.InputColumn{Name: ckdb.COLUMN_PROCESS_KNAME_0, Data: &b.ColProcessKname0},
		proto.InputColumn{Name: ckdb.COLUMN_PROCESS_KNAME_1, Data: &b.ColProcessKname1},
		proto.InputColumn{Name: ckdb.COLUMN_SYSCALL_TRACE_ID_REQUEST, Data: &b.ColSyscallTraceIdRequest},
		proto.InputColumn{Name: ckdb.COLUMN_SYSCALL_TRACE_ID_RESPONSE, Data: &b.ColSyscallTraceIdResponse},
		proto.InputColumn{Name: ckdb.COLUMN_SYSCALL_THREAD_0, Data: &b.ColSyscallThread0},
		proto.InputColumn{Name: ckdb.COLUMN_SYSCALL_THREAD_1, Data: &b.ColSyscallThread1},
		proto.InputColumn{Name: ckdb.COLUMN_SYSCALL_COROUTINE_0, Data: &b.ColSyscallCoroutine0},
		proto.InputColumn{Name: ckdb.COLUMN_SYSCALL_COROUTINE_1, Data: &b.ColSyscallCoroutine1},
		proto.InputColumn{Name: ckdb.COLUMN_SYSCALL_CAP_SEQ_0, Data: &b.ColSyscallCapSeq0},
		proto.InputColumn{Name: ckdb.COLUMN_SYSCALL_CAP_SEQ_1, Data: &b.ColSyscallCapSeq1},
	)
}

func (n *L7Base) NewColumnBlock() ckdb.CKColumnBlock {
	return &L7BaseBlock{
		KnowledgeGraphBlock: n.KnowledgeGraph.NewColumnBlock().(*KnowledgeGraphBlock),
		ColObservationPoint: new(proto.ColStr).LowCardinality(),
	}
}

func (n *L7Base) AppendToColumnBlock(b ckdb.CKColumnBlock) {
	block := b.(*L7BaseBlock)
	n.KnowledgeGraph.AppendToColumnBlock(block.KnowledgeGraphBlock)
	ckdb.AppendColDateTime(&block.ColTime, n.Time)
	block.ColIp40.Append(proto.IPv4(n.IP40))
	block.ColIp41.Append(proto.IPv4(n.IP41))
	ckdb.AppendIPv6(&block.ColIp60, n.IP60)
	ckdb.AppendIPv6(&block.ColIp61, n.IP61)
	block.ColIsIpv4.Append(*(*uint8)(unsafe.Pointer(&n.IsIPv4)))
	block.ColProtocol.Append(n.Protocol)
	block.ColClientPort.Append(n.ClientPort)
	block.ColServerPort.Append(n.ServerPort)
	block.ColFlowId.Append(n.FlowID)
	block.ColCaptureNetworkTypeId.Append(n.TapType)
	block.ColNatSource.Append(n.NatSource)
	block.ColCaptureNicType.Append(n.TapPortType)
	block.ColSignalSource.Append(n.SignalSource)
	block.ColTunnelType.Append(n.TunnelType)
	block.ColCaptureNic.Append(n.TapPort)
	block.ColObservationPoint.Append(n.TapSide)
	block.ColAgentId.Append(n.VtapID)
	block.ColReqTcpSeq.Append(n.ReqTcpSeq)
	block.ColRespTcpSeq.Append(n.RespTcpSeq)
	ckdb.AppendColDateTime64Micro(&block.ColStartTime, n.StartTime)
	ckdb.AppendColDateTime64Micro(&block.ColEndTime, n.EndTime)
	block.ColGprocessId0.Append(n.GPID0)
	block.ColGprocessId1.Append(n.GPID1)
	block.ColBizType.Append(n.BizType)
	block.ColProcessId0.Append(int32(n.ProcessID0))
	block.ColProcessId1.Append(int32(n.ProcessID1))
	block.ColProcessKname0.Append(n.ProcessKName0)
	block.ColProcessKname1.Append(n.ProcessKName1)
	block.ColSyscallTraceIdRequest.Append(n.SyscallTraceIDRequest)
	block.ColSyscallTraceIdResponse.Append(n.SyscallTraceIDResponse)
	block.ColSyscallThread0.Append(n.SyscallThread0)
	block.ColSyscallThread1.Append(n.SyscallThread1)
	block.ColSyscallCoroutine0.Append(n.SyscallCoroutine0)
	block.ColSyscallCoroutine1.Append(n.SyscallCoroutine1)
	block.ColSyscallCapSeq0.Append(n.SyscallCapSeq0)
	block.ColSyscallCapSeq1.Append(n.SyscallCapSeq1)
}

type L7FlowLogBlock struct {
	*L7BaseBlock
	ColId                   proto.ColUInt64
	ColL7Protocol           proto.ColUInt8
	ColL7ProtocolStr        *proto.ColLowCardinality[string]
	ColVersion              *proto.ColLowCardinality[string]
	ColType                 proto.ColUInt8
	ColIsTls                proto.ColUInt8
	ColRequestType          *proto.ColLowCardinality[string]
	ColRequestDomain        proto.ColStr
	ColRequestResource      proto.ColStr
	ColEndpoint             proto.ColStr
	ColRequestId            *proto.ColNullable[uint64]
	ColResponseStatus       proto.ColUInt8
	ColResponseCode         *proto.ColNullable[int32]
	ColResponseException    proto.ColStr
	ColResponseResult       proto.ColStr
	ColHttpProxyClient      proto.ColStr
	ColXRequestId0          proto.ColStr
	ColXRequestId1          proto.ColStr
	ColTraceId              proto.ColStr
	ColTraceIdIndex         proto.ColUInt64
	ColSpanId               proto.ColStr
	ColParentSpanId         proto.ColStr
	ColSpanKind             *proto.ColNullable[uint8]
	ColAppService           *proto.ColLowCardinality[string]
	ColAppInstance          *proto.ColLowCardinality[string]
	ColResponseDuration     proto.ColUInt64
	ColRequestLength        *proto.ColNullable[int64]
	ColResponseLength       *proto.ColNullable[int64]
	ColSqlAffectedRows      *proto.ColNullable[uint64]
	ColDirectionScore       proto.ColUInt8
	ColCapturedRequestByte  proto.ColUInt32
	ColCapturedResponseByte proto.ColUInt32
	ColAttributeNames       *proto.ColArr[string]
	ColAttributeValues      *proto.ColArr[string]
	ColMetricsNames         *proto.ColArr[string]
	ColMetricsValues        *proto.ColArr[float64]
	ColEvents               proto.ColStr
	*nativetag.NativeTagsBlock
}

func (b *L7FlowLogBlock) Reset() {
	b.L7BaseBlock.Reset()
	b.ColId.Reset()
	b.ColL7Protocol.Reset()
	b.ColL7ProtocolStr.Reset()
	b.ColVersion.Reset()
	b.ColType.Reset()
	b.ColIsTls.Reset()
	b.ColRequestType.Reset()
	b.ColRequestDomain.Reset()
	b.ColRequestResource.Reset()
	b.ColEndpoint.Reset()
	b.ColRequestId.Reset()
	b.ColResponseStatus.Reset()
	b.ColResponseCode.Reset()
	b.ColResponseException.Reset()
	b.ColResponseResult.Reset()
	b.ColHttpProxyClient.Reset()
	b.ColXRequestId0.Reset()
	b.ColXRequestId1.Reset()
	b.ColTraceId.Reset()
	b.ColTraceIdIndex.Reset()
	b.ColSpanId.Reset()
	b.ColParentSpanId.Reset()
	b.ColSpanKind.Reset()
	b.ColAppService.Reset()
	b.ColAppInstance.Reset()
	b.ColResponseDuration.Reset()
	b.ColRequestLength.Reset()
	b.ColResponseLength.Reset()
	b.ColSqlAffectedRows.Reset()
	b.ColDirectionScore.Reset()
	b.ColCapturedRequestByte.Reset()
	b.ColCapturedResponseByte.Reset()
	b.ColAttributeNames.Reset()
	b.ColAttributeValues.Reset()
	b.ColMetricsNames.Reset()
	b.ColMetricsValues.Reset()
	b.ColEvents.Reset()
	if b.NativeTagsBlock != nil {
		b.NativeTagsBlock.Reset()
	}
}

func (b *L7FlowLogBlock) ToInput(input proto.Input) proto.Input {
	input = b.L7BaseBlock.ToInput(input)
	input = append(input,
		proto.InputColumn{Name: ckdb.COLUMN__ID, Data: &b.ColId},
		proto.InputColumn{Name: ckdb.COLUMN_L7_PROTOCOL, Data: &b.ColL7Protocol},
		proto.InputColumn{Name: ckdb.COLUMN_L7_PROTOCOL_STR, Data: b.ColL7ProtocolStr},
		proto.InputColumn{Name: ckdb.COLUMN_VERSION, Data: b.ColVersion},
		proto.InputColumn{Name: ckdb.COLUMN_TYPE, Data: &b.ColType},
		proto.InputColumn{Name: ckdb.COLUMN_IS_TLS, Data: &b.ColIsTls},
		proto.InputColumn{Name: ckdb.COLUMN_REQUEST_TYPE, Data: b.ColRequestType},
		proto.InputColumn{Name: ckdb.COLUMN_REQUEST_DOMAIN, Data: &b.ColRequestDomain},
		proto.InputColumn{Name: ckdb.COLUMN_REQUEST_RESOURCE, Data: &b.ColRequestResource},
		proto.InputColumn{Name: ckdb.COLUMN_ENDPOINT, Data: &b.ColEndpoint},
		proto.InputColumn{Name: ckdb.COLUMN_REQUEST_ID, Data: b.ColRequestId},
		proto.InputColumn{Name: ckdb.COLUMN_RESPONSE_STATUS, Data: &b.ColResponseStatus},
		proto.InputColumn{Name: ckdb.COLUMN_RESPONSE_CODE, Data: b.ColResponseCode},
		proto.InputColumn{Name: ckdb.COLUMN_RESPONSE_EXCEPTION, Data: &b.ColResponseException},
		proto.InputColumn{Name: ckdb.COLUMN_RESPONSE_RESULT, Data: &b.ColResponseResult},
		proto.InputColumn{Name: ckdb.COLUMN_HTTP_PROXY_CLIENT, Data: &b.ColHttpProxyClient},
		proto.InputColumn{Name: ckdb.COLUMN_X_REQUEST_ID_0, Data: &b.ColXRequestId0},
		proto.InputColumn{Name: ckdb.COLUMN_X_REQUEST_ID_1, Data: &b.ColXRequestId1},
		proto.InputColumn{Name: ckdb.COLUMN_TRACE_ID, Data: &b.ColTraceId},
		proto.InputColumn{Name: ckdb.COLUMN_TRACE_ID_INDEX, Data: &b.ColTraceIdIndex},
		proto.InputColumn{Name: ckdb.COLUMN_SPAN_ID, Data: &b.ColSpanId},
		proto.InputColumn{Name: ckdb.COLUMN_PARENT_SPAN_ID, Data: &b.ColParentSpanId},
		proto.InputColumn{Name: ckdb.COLUMN_SPAN_KIND, Data: b.ColSpanKind},
		proto.InputColumn{Name: ckdb.COLUMN_APP_SERVICE, Data: b.ColAppService},
		proto.InputColumn{Name: ckdb.COLUMN_APP_INSTANCE, Data: b.ColAppInstance},
		proto.InputColumn{Name: ckdb.COLUMN_RESPONSE_DURATION, Data: &b.ColResponseDuration},
		proto.InputColumn{Name: ckdb.COLUMN_REQUEST_LENGTH, Data: b.ColRequestLength},
		proto.InputColumn{Name: ckdb.COLUMN_RESPONSE_LENGTH, Data: b.ColResponseLength},
		proto.InputColumn{Name: ckdb.COLUMN_SQL_AFFECTED_ROWS, Data: b.ColSqlAffectedRows},
		proto.InputColumn{Name: ckdb.COLUMN_DIRECTION_SCORE, Data: &b.ColDirectionScore},
		proto.InputColumn{Name: ckdb.COLUMN_CAPTURED_REQUEST_BYTE, Data: &b.ColCapturedRequestByte},
		proto.InputColumn{Name: ckdb.COLUMN_CAPTURED_RESPONSE_BYTE, Data: &b.ColCapturedResponseByte},
		proto.InputColumn{Name: ckdb.COLUMN_ATTRIBUTE_NAMES, Data: b.ColAttributeNames},
		proto.InputColumn{Name: ckdb.COLUMN_ATTRIBUTE_VALUES, Data: b.ColAttributeValues},
		proto.InputColumn{Name: ckdb.COLUMN_METRICS_NAMES, Data: b.ColMetricsNames},
		proto.InputColumn{Name: ckdb.COLUMN_METRICS_VALUES, Data: b.ColMetricsValues},
		proto.InputColumn{Name: ckdb.COLUMN_EVENTS, Data: &b.ColEvents},
	)
	if b.NativeTagsBlock != nil {
		return b.NativeTagsBlock.ToInput(input)
	}
	return input
}

func (n *L7FlowLog) NewColumnBlock() ckdb.CKColumnBlock {
	return &L7FlowLogBlock{
		L7BaseBlock:        n.L7Base.NewColumnBlock().(*L7BaseBlock),
		ColL7ProtocolStr:   new(proto.ColStr).LowCardinality(),
		ColVersion:         new(proto.ColStr).LowCardinality(),
		ColRequestType:     new(proto.ColStr).LowCardinality(),
		ColAppService:      new(proto.ColStr).LowCardinality(),
		ColAppInstance:     new(proto.ColStr).LowCardinality(),
		ColRequestId:       new(proto.ColUInt64).Nullable(),
		ColResponseCode:    new(proto.ColInt32).Nullable(),
		ColSpanKind:        new(proto.ColUInt8).Nullable(),
		ColRequestLength:   new(proto.ColInt64).Nullable(),
		ColResponseLength:  new(proto.ColInt64).Nullable(),
		ColSqlAffectedRows: new(proto.ColUInt64).Nullable(),
		ColAttributeNames:  new(proto.ColStr).LowCardinality().Array(),
		ColAttributeValues: new(proto.ColStr).Array(),
		ColMetricsNames:    new(proto.ColStr).LowCardinality().Array(),
		ColMetricsValues:   new(proto.ColFloat64).Array(),
		NativeTagsBlock:    nativetag.GetTableNativeTagsColumnBlock(n.OrgId, nativetag.L7_FLOW_LOG),
	}
}

func (n *L7FlowLog) AppendToColumnBlock(b ckdb.CKColumnBlock) {
	block := b.(*L7FlowLogBlock)
	n.L7Base.AppendToColumnBlock(block.L7BaseBlock)
	block.ColId.Append(n._id)
	block.ColL7Protocol.Append(n.L7Protocol)
	block.ColL7ProtocolStr.Append(n.L7ProtocolStr)
	block.ColVersion.Append(n.Version)
	block.ColType.Append(n.Type)
	block.ColIsTls.Append(n.IsTLS)
	block.ColRequestType.Append(n.RequestType)
	block.ColRequestDomain.Append(n.RequestDomain)
	block.ColRequestResource.Append(n.RequestResource)
	block.ColEndpoint.Append(n.Endpoint)
	ckdb.AppendColNullable(block.ColRequestId, n.RequestId)
	block.ColResponseStatus.Append(n.ResponseStatus)
	ckdb.AppendColNullable(block.ColResponseCode, n.ResponseCode)
	block.ColResponseException.Append(n.ResponseException)
	block.ColResponseResult.Append(n.ResponseResult)
	block.ColHttpProxyClient.Append(n.HttpProxyClient)
	block.ColXRequestId0.Append(n.XRequestId0)
	block.ColXRequestId1.Append(n.XRequestId1)
	block.ColTraceId.Append(n.TraceId)
	block.ColTraceIdIndex.Append(n.TraceIdIndex)
	block.ColSpanId.Append(n.SpanId)
	block.ColParentSpanId.Append(n.ParentSpanId)
	ckdb.AppendColNullable(block.ColSpanKind, n.spanKind)
	block.ColAppService.Append(n.AppService)
	block.ColAppInstance.Append(n.AppInstance)
	block.ColResponseDuration.Append(n.ResponseDuration)
	ckdb.AppendColNullable(block.ColRequestLength, n.RequestLength)
	ckdb.AppendColNullable(block.ColResponseLength, n.ResponseLength)
	ckdb.AppendColNullable(block.ColSqlAffectedRows, n.SqlAffectedRows)
	block.ColDirectionScore.Append(n.DirectionScore)
	block.ColCapturedRequestByte.Append(n.CapturedRequestByte)
	block.ColCapturedResponseByte.Append(n.CapturedResponseByte)
	block.ColAttributeNames.Append(n.AttributeNames)
	block.ColAttributeValues.Append(n.AttributeValues)
	block.ColMetricsNames.Append(n.MetricsNames)
	block.ColMetricsValues.Append(n.MetricsValues)
	block.ColEvents.Append(n.Events)
	if block.NativeTagsBlock != nil {
		block.NativeTagsBlock.AppendToColumnBlock(n.AttributeNames, n.AttributeValues, n.MetricsNames, n.MetricsValues)
	}
}
