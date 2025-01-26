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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/deepflowio/deepflow/server/ingester/config"
	"github.com/deepflowio/deepflow/server/ingester/flow_log/common"
	flowlogCfg "github.com/deepflowio/deepflow/server/ingester/flow_log/config"
	"github.com/deepflowio/deepflow/server/ingester/flow_tag"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/datatype"
	"github.com/deepflowio/deepflow/server/libs/datatype/pb"
	flow_metrics "github.com/deepflowio/deepflow/server/libs/flow-metrics"
	"github.com/deepflowio/deepflow/server/libs/grpc"
	"github.com/deepflowio/deepflow/server/libs/nativetag"
	"github.com/deepflowio/deepflow/server/libs/pool"
	"github.com/deepflowio/deepflow/server/libs/utils"

	"github.com/google/gopacket/layers"
	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("flow_log.log_data")

type L7Base struct {
	// 知识图谱
	KnowledgeGraph

	Time uint32 `json:"time" category:"$tag" sub:"flow_info"` // s
	// 网络层
	IP40     uint32 `json:"ip4_0" category:"$tag" sub:"network_layer" to_string:"IPv4String"`
	IP41     uint32 `json:"ip4_1" category:"$tag" sub:"network_layer" to_string:"IPv4String"`
	IP60     net.IP `json:"ip6_0" category:"$tag" sub:"network_layer" to_string:"IPv6String"`
	IP61     net.IP `json:"ip6_1" category:"$tag" sub:"network_layer" to_string:"IPv6String"`
	IsIPv4   bool   `json:"is_ipv4" category:"$tag" sub:"network_layer"`
	Protocol uint8  `json:"protocol" category:"$tag" sub:"network_layer" enumfile:"l7_ip_protocol"`

	// 传输层
	ClientPort uint16 `json:"client_port" category:"$tag" sub:"transport_layer" `
	ServerPort uint16 `json:"server_port" category:"$tag" sub:"transport_layer"`

	// 流信息
	FlowID       uint64 `json:"flow_id" category:"$tag" sub:"flow_info"`
	TapType      uint8  `json:"capture_network_type_id" category:"$tag" sub:"capture_info"`
	NatSource    uint8  `json:"nat_source" category:"$tag" sub:"capture_info" enumfile:"nat_source"`
	TapPortType  uint8  `json:"capture_nic_type category:"$tag" sub:"capture_info"`
	SignalSource uint16 `json:"signal_source" category:"$tag" sub:"capture_info" enumfile:"l7_signal_source"`
	TunnelType   uint8  `json:"tunnel_type" category:"$tag" sub:"tunnel_info"`
	TapPort      uint32 `json:"capture_nic" category:"$tag" sub:"capture_info"`
	TapSide      string `json:"observation_point" category:"$tag" sub:"capture_info" enumfile:"observation_point"`
	TapSideEnum  uint8
	VtapID       uint16 `json:"agent_id" category:"$tag" sub:"capture_info"`
	ReqTcpSeq    uint32 `json:"req_tcp_seq" category:"$tag" sub:"transport_layer"`
	RespTcpSeq   uint32 `json:"resp_tcp_seq" category:"$tag" sub:"transport_layer"`
	StartTime    int64  `json:"start_time" category:"$tag" sub:"flow_info"` // us
	EndTime      int64  `json:"end_time" category:"$tag" sub:"flow_info"`   // us
	GPID0        uint32 `json:"gprocess_id_0" category:"$tag" sub:"universal_tag"`
	GPID1        uint32 `json:"gprocess_id_1" category:"$tag" sub:"universal_tag"`
	BizType      uint8  `json:"biz_type" category:"$tag" sub:"capture_info"`

	ProcessID0             uint32 `json:"process_id_0" category:"$tag" sub:"service_info"`
	ProcessID1             uint32 `json:"process_id_1" category:"$tag" sub:"service_info"`
	ProcessKName0          string `json:"process_kname_0" category:"$tag" sub:"service_info"`
	ProcessKName1          string `json:"process_kname_1" category:"$tag" sub:"service_info"`
	SyscallTraceIDRequest  uint64 `json:"syscall_trace_id_request" category:"$tag" sub:"tracing_info"`
	SyscallTraceIDResponse uint64 `json:"syscall_trace_id_response" category:"$tag" sub:"tracing_info"`
	SyscallThread0         uint32 `json:"syscall_thread_0" category:"$tag" sub:"tracing_info"`
	SyscallThread1         uint32 `json:"syscall_thread_1" category:"$tag" sub:"tracing_info"`
	SyscallCoroutine0      uint64 `json:"syscall_coroutine_0" category:"$tag" sub:"tracing_info"`
	SyscallCoroutine1      uint64 `json:"syscall_coroutine_1" category:"$tag" sub:"tracing_info"`
	SyscallCapSeq0         uint32 `json:"syscall_cap_seq_0" category:"$tag" sub:"tracing_info"`
	SyscallCapSeq1         uint32 `json:"syscall_cap_seq_1" category:"$tag" sub:"tracing_info"`

	EncodedSpan []byte
}

func L7BaseColumns() []*ckdb.Column {
	columns := []*ckdb.Column{}
	// 知识图谱
	columns = append(columns, KnowledgeGraphColumns...)
	columns = append(columns,
		ckdb.NewColumn("time", ckdb.DateTime).SetComment("精度: 秒"),
		// 网络层
		ckdb.NewColumn("ip4_0", ckdb.IPv4),
		ckdb.NewColumn("ip4_1", ckdb.IPv4),
		ckdb.NewColumn("ip6_0", ckdb.IPv6),
		ckdb.NewColumn("ip6_1", ckdb.IPv6),
		ckdb.NewColumn("is_ipv4", ckdb.UInt8).SetIndex(ckdb.IndexMinmax),
		ckdb.NewColumn("protocol", ckdb.UInt8).SetIndex(ckdb.IndexMinmax),

		// 传输层
		ckdb.NewColumn("client_port", ckdb.UInt16),
		ckdb.NewColumn("server_port", ckdb.UInt16).SetIndex(ckdb.IndexSet),

		// 流信息
		ckdb.NewColumn("flow_id", ckdb.UInt64).SetIndex(ckdb.IndexMinmax),
		ckdb.NewColumn("capture_network_type_id", ckdb.UInt8).SetIndex(ckdb.IndexSet),
		ckdb.NewColumn("nat_source", ckdb.UInt8).SetIndex(ckdb.IndexSet),
		ckdb.NewColumn("capture_nic_type", ckdb.UInt8).SetIndex(ckdb.IndexNone),
		ckdb.NewColumn("signal_source", ckdb.UInt16).SetIndex(ckdb.IndexNone),
		ckdb.NewColumn("tunnel_type", ckdb.UInt8).SetIndex(ckdb.IndexNone),
		ckdb.NewColumn("capture_nic", ckdb.UInt32).SetIndex(ckdb.IndexNone),
		ckdb.NewColumn("observation_point", ckdb.LowCardinalityString),
		ckdb.NewColumn("agent_id", ckdb.UInt16).SetIndex(ckdb.IndexSet),
		ckdb.NewColumn("req_tcp_seq", ckdb.UInt32),
		ckdb.NewColumn("resp_tcp_seq", ckdb.UInt32),
		ckdb.NewColumn("start_time", ckdb.DateTime64us).SetComment("精度: 微秒"),
		ckdb.NewColumn("end_time", ckdb.DateTime64us).SetComment("精度: 微秒"),
		ckdb.NewColumn("gprocess_id_0", ckdb.UInt32).SetComment("全局客户端进程ID"),
		ckdb.NewColumn("gprocess_id_1", ckdb.UInt32).SetComment("全局服务端进程ID"),
		ckdb.NewColumn("biz_type", ckdb.UInt8).SetComment("Business Type"),

		ckdb.NewColumn("process_id_0", ckdb.Int32).SetComment("客户端进程ID"),
		ckdb.NewColumn("process_id_1", ckdb.Int32).SetComment("服务端进程ID"),
		ckdb.NewColumn("process_kname_0", ckdb.String).SetComment("客户端进程名"),
		ckdb.NewColumn("process_kname_1", ckdb.String).SetComment("服务端进程名"),
		ckdb.NewColumn("syscall_trace_id_request", ckdb.UInt64).SetComment("SyscallTraceID-请求"),
		ckdb.NewColumn("syscall_trace_id_response", ckdb.UInt64).SetComment("SyscallTraceID-响应"),
		ckdb.NewColumn("syscall_thread_0", ckdb.UInt32).SetComment("Syscall线程-请求"),
		ckdb.NewColumn("syscall_thread_1", ckdb.UInt32).SetComment("Syscall线程-响应"),
		ckdb.NewColumn("syscall_coroutine_0", ckdb.UInt64).SetComment("Request Syscall Coroutine"),
		ckdb.NewColumn("syscall_coroutine_1", ckdb.UInt64).SetComment("Response Syscall Coroutine"),
		ckdb.NewColumn("syscall_cap_seq_0", ckdb.UInt32).SetComment("Syscall序列号-请求"),
		ckdb.NewColumn("syscall_cap_seq_1", ckdb.UInt32).SetComment("Syscall序列号-响应"),
	)

	return columns
}

type L7FlowLog struct {
	pool.ReferenceCount
	_id uint64 `json:"_id" category:"$tag" sub:"flow_info"`

	L7Base

	L7Protocol    uint8  `json:"l7_protocol" category:"$tag" sub:"application_layer" enumfile:"l7_protocol"`
	L7ProtocolStr string `json:"l7_protocol_str" category:"$tag" sub:"application_layer"`
	Version       string `json:"version" category:"$tag" sub:"application_layer"`
	Type          uint8  `json:"type" category:"$tag" sub:"application_layer" enumfile:"l7_log_type"`
	IsTLS         uint8  `json:"is_tls" category:"$tag" sub:"application_layer"`

	RequestType     string `json:"request_type" category:"$tag" sub:"application_layer"`
	RequestDomain   string `json:"request_domain" category:"$tag" sub:"application_layer"`
	RequestResource string `json:"request_resource" category:"$tag" sub:"application_layer"`
	Endpoint        string `json:"end_point" category:"$tag" sub:"service_info"`

	// 数据库nullabled类型的字段, 需使用指针传值写入。如果值无意义，应传递nil.
	RequestId *uint64 `json:"request_id" category:"$tag" sub:"application_layer" data_type:"*uint64"`
	requestId uint64

	ResponseStatus    uint8  `json:"response_status" category:"$tag" sub:"application_layer" enumfile:"response_status"`
	ResponseCode      *int32 `json:"response_code" category:"$tag" sub:"application_layer" data_type:"*int32"`
	responseCode      int32
	ResponseException string `json:"response_exception" category:"$tag" sub:"application_layer"`
	ResponseResult    string `json:"response_result" category:"$tag" sub:"application_layer"`

	HttpProxyClient string `json:"http_proxy_client" category:"$tag" sub:"tracing_info"`
	XRequestId0     string `json:"x_request_id_0" category:"$tag" sub:"tracing_info"`
	XRequestId1     string `json:"x_request_id_1" category:"$tag" sub:"tracing_info"`
	TraceId         string `json:"trace_id" category:"$tag" sub:"tracing_info"`
	TraceIdIndex    uint64
	SpanId          string `json:"span_id" category:"$tag" sub:"tracing_info"`
	ParentSpanId    string `json:"parent_span_id" category:"$tag" sub:"tracing_info"`
	SpanKind        uint8
	spanKind        *uint8 `json:"span_kind" category:"$tag" sub:"tracing_info" enumfile:"span_kind" data_type:"*uint8"`
	AppService      string `json:"app_service" category:"$tag" sub:"service_info"`
	AppInstance     string `json:"app_instance" category:"$tag" sub:"service_info"`

	ResponseDuration uint64 `json:"response_duration" category:"$metrics" sub:"delay"`
	RequestLength    *int64 `json:"request_length" category:"$metrics" sub:"throughput" data_type:"*int64"`
	requestLength    int64
	ResponseLength   *int64 `json:"response_length" category:"$metrics" sub:"throughput" data_type:"*int64"`
	responseLength   int64
	SqlAffectedRows  *uint64 `json:"sql_affected_rows" category:"$metrics" sub:"throughput" data_type:"*uint64"`
	sqlAffectedRows  uint64
	DirectionScore   uint8 `json:"direction_score" category:"$metrics" sub:"l4_throughput"`

	// For Packet signal sources, it represents the packet length captured by AF_PACKET, excluding the layer 4 headers; for eBPF signal sources, it indicates the number of bytes for a single system call, and note that when TCP stream reassembly is enabled, it represents the total number of bytes from multiple system calls.
	CapturedRequestByte  uint32 `json:"captured_request_byte" category:"$metrics" sub:"throughput"`
	CapturedResponseByte uint32 `json:"captured_response_byte" category:"$metrics" sub:"throughput"`

	AttributeNames  []string `json:"attribute_names" category:"$tag" sub:"native_tag" data_type:"[]string"`
	AttributeValues []string `json:"attribute_values" category:"$tag" sub:"native_tag" data_type:"[]string"`

	MetricsNames  []string  `json:"metrics_names" category:"$metrics" data_type:"[]string"`
	MetricsValues []float64 `json:"metrics_values" category:"$metrics" data_type:"[]float64"`

	Events string `json:"events" category:"$tag" sub:"application_layer"`
}

func L7FlowLogColumns() []*ckdb.Column {
	l7Columns := []*ckdb.Column{}
	l7Columns = append(l7Columns, ckdb.NewColumn("_id", ckdb.UInt64))
	l7Columns = append(l7Columns, L7BaseColumns()...)
	l7Columns = append(l7Columns,
		ckdb.NewColumn("l7_protocol", ckdb.UInt8).SetIndex(ckdb.IndexNone).SetComment("0:未知 1:其他, 20:http1, 21:http2, 40:dubbo, 60:mysql, 80:redis, 100:kafka, 101:mqtt, 120:dns"),
		ckdb.NewColumn("l7_protocol_str", ckdb.LowCardinalityString).SetIndex(ckdb.IndexNone).SetComment("应用协议"),
		ckdb.NewColumn("version", ckdb.LowCardinalityString).SetComment("协议版本"),
		ckdb.NewColumn("type", ckdb.UInt8).SetIndex(ckdb.IndexNone).SetComment("日志类型, 0:请求, 1:响应, 2:会话"),
		ckdb.NewColumn("is_tls", ckdb.UInt8),

		ckdb.NewColumn("request_type", ckdb.LowCardinalityString).SetComment("请求类型, HTTP请求方法、SQL命令类型、NoSQL命令类型、MQ命令类型、DNS查询类型"),
		ckdb.NewColumn("request_domain", ckdb.String).SetIndex(ckdb.IndexBloomfilter).SetComment("请求域名, HTTP主机名、RPC服务名称、DNS查询域名"),
		ckdb.NewColumn("request_resource", ckdb.String).SetIndex(ckdb.IndexBloomfilter).SetComment("请求资源, HTTP路径、RPC方法名称、SQL命令、NoSQL命令"),
		ckdb.NewColumn("endpoint", ckdb.String).SetIndex(ckdb.IndexBloomfilter).SetComment("端点"),
		ckdb.NewColumn("request_id", ckdb.UInt64Nullable).SetComment("请求ID, HTTP请求ID、RPC请求ID、MQ请求ID、DNS请求ID"),

		ckdb.NewColumn("response_status", ckdb.UInt8).SetComment("响应状态 0:正常, 1:异常 ,2:不存在，3:服务端异常, 4:客户端异常"),
		ckdb.NewColumn("response_code", ckdb.Int32Nullable).SetComment("响应码, HTTP响应码、RPC响应码、SQL响应码、MQ响应码、DNS响应码"),
		ckdb.NewColumn("response_exception", ckdb.String).SetComment("响应异常"),
		ckdb.NewColumn("response_result", ckdb.String).SetComment("响应结果, DNS解析地址"),

		ckdb.NewColumn("http_proxy_client", ckdb.String).SetComment("HTTP代理客户端"),
		ckdb.NewColumn("x_request_id_0", ckdb.String).SetIndex(ckdb.IndexBloomfilter).SetComment("XRequestID0"),
		ckdb.NewColumn("x_request_id_1", ckdb.String).SetIndex(ckdb.IndexBloomfilter).SetComment("XRequestID1"),
		ckdb.NewColumn("trace_id", ckdb.String).SetIndex(ckdb.IndexBloomfilter).SetComment("TraceID"),
		ckdb.NewColumn("trace_id_index", ckdb.UInt64).SetIndex(ckdb.IndexMinmax).SetComment("TraceIDIndex"),
		ckdb.NewColumn("span_id", ckdb.String).SetComment("SpanID"),
		ckdb.NewColumn("parent_span_id", ckdb.String).SetComment("ParentSpanID"),
		ckdb.NewColumn("span_kind", ckdb.UInt8Nullable).SetComment("SpanKind"),
		ckdb.NewColumn("app_service", ckdb.LowCardinalityString).SetComment("app service"),
		ckdb.NewColumn("app_instance", ckdb.LowCardinalityString).SetComment("app instance"),

		ckdb.NewColumn("response_duration", ckdb.UInt64),
		ckdb.NewColumn("request_length", ckdb.Int64Nullable).SetComment("请求长度"),
		ckdb.NewColumn("response_length", ckdb.Int64Nullable).SetComment("响应长度"),
		ckdb.NewColumn("sql_affected_rows", ckdb.UInt64Nullable).SetComment("sql影响行数"),
		ckdb.NewColumn("direction_score", ckdb.UInt8).SetIndex(ckdb.IndexMinmax),
		ckdb.NewColumn("captured_request_byte", ckdb.UInt32),
		ckdb.NewColumn("captured_response_byte", ckdb.UInt32),

		ckdb.NewColumn("attribute_names", ckdb.ArrayLowCardinalityString).SetComment("额外的属性"),
		ckdb.NewColumn("attribute_values", ckdb.ArrayString).SetComment("额外的属性对应的值"),
		ckdb.NewColumn("metrics_names", ckdb.ArrayLowCardinalityString).SetComment("额外的指标"),
		ckdb.NewColumn("metrics_values", ckdb.ArrayFloat64).SetComment("额外的指标对应的值"),
		ckdb.NewColumn("events", ckdb.String).SetComment("OTel events"),
	)
	return l7Columns
}

func (h *L7FlowLog) NativeTagVersion() uint32 {
	return nativetag.GetTableNativeTagsVersion(h.KnowledgeGraph.OrgId, nativetag.L7_FLOW_LOG)
}

func (h *L7FlowLog) OrgID() uint16 {
	return h.KnowledgeGraph.OrgId
}

func base64ToHexString(str string) string {
	if len(str) < 2 || str[len(str)-1] != '=' {
		return str
	}
	bytes, err := base64.StdEncoding.DecodeString(str)
	if err == nil {
		return hex.EncodeToString(bytes)
	}
	return str
}

// for empty traceId, the traceId-index is the value of the previous traceId-index + 1, not 0.
// when the traceId-index data is stored in CK, the generated minmax index will have min non-zero, which improves the filtering performance of the minmax index
var lastTraceIdIndex uint64

func ParseTraceIdIndex(traceId string, traceIdIndexCfg *config.TraceIdWithIndex) uint64 {
	if traceIdIndexCfg.Disabled {
		return 0
	}
	if len(traceId) == 0 {
		return lastTraceIdIndex + 1
	}
	index, err := utils.GetTraceIdIndex(traceId, traceIdIndexCfg.TypeIsIncrementalId, traceIdIndexCfg.FormatIsHex, traceIdIndexCfg.IncrementalIdLocation.Start, traceIdIndexCfg.IncrementalIdLocation.Length)
	if err != nil {
		log.Debugf("parse traceIdIndex failed err %s", err)
		return lastTraceIdIndex + 1
	}
	lastTraceIdIndex = index
	return index
}

func (h *L7FlowLog) Fill(l *pb.AppProtoLogsData, platformData *grpc.PlatformInfoTable, cfg *flowlogCfg.Config) {
	h.L7Base.Fill(l, platformData)

	h.Type = uint8(l.Base.Head.MsgType)
	h.IsTLS = uint8(l.Flags & 0x1)
	h.L7Protocol = uint8(l.Base.Head.Proto)
	if l.ExtInfo != nil && l.ExtInfo.ProtocolStr != "" {
		h.L7ProtocolStr = l.ExtInfo.ProtocolStr
	} else {
		h.L7ProtocolStr = datatype.L7Protocol(h.L7Protocol).String(h.IsTLS == 1)
	}

	h.ResponseStatus = uint8(datatype.STATUS_NOT_EXIST)
	h.ResponseDuration = l.Base.Head.Rrt / uint64(time.Microsecond)
	// 协议结构统一, 不再为每个协议定义单独结构
	h.fillL7FlowLog(l, cfg)
}

// requestLength,responseLength 等于 -1 会认为是没有值. responseCode=-32768 会认为没有值
func (h *L7FlowLog) fillL7FlowLog(l *pb.AppProtoLogsData, cfg *flowlogCfg.Config) {
	h.Version = l.Version
	h.requestLength = int64(l.ReqLen)
	h.responseLength = int64(l.RespLen)
	h.sqlAffectedRows = uint64(l.RowEffect)
	if h.sqlAffectedRows != 0 {
		h.SqlAffectedRows = &h.sqlAffectedRows
	}
	h.DirectionScore = uint8(l.DirectionScore)
	h.CapturedRequestByte = l.CapturedRequestByte
	h.CapturedResponseByte = l.CapturedResponseByte

	if l.Req != nil {
		h.RequestDomain = l.Req.Domain
		h.RequestResource = l.Req.Resource
		h.RequestType = l.Req.ReqType
		if h.requestLength != -1 && h.Type != uint8(datatype.MSG_T_RESPONSE) {
			h.RequestLength = &h.requestLength
		}
		h.Endpoint = l.Req.Endpoint
	}

	if l.Resp != nil && h.Type != uint8(datatype.MSG_T_REQUEST) {
		h.ResponseResult = l.Resp.Result
		h.responseCode = l.Resp.Code
		h.ResponseStatus = uint8(l.Resp.Status)
		h.ResponseException = l.Resp.Exception
		if h.ResponseException == "" {
			h.fillExceptionDesc(l)
		}

		if h.responseCode != datatype.L7PROTOCOL_LOG_RESP_CODE_NONE {
			h.ResponseCode = &h.responseCode
		}
		if h.responseLength != -1 {
			h.ResponseLength = &h.responseLength
		}
	}

	if l.ExtInfo != nil {
		h.requestId = uint64(l.ExtInfo.RequestId)
		if h.requestId != 0 {
			h.RequestId = &h.requestId
		}
		h.AppService = l.ExtInfo.ServiceName
		h.XRequestId0 = l.ExtInfo.XRequestId_0
		h.XRequestId1 = l.ExtInfo.XRequestId_1
		h.HttpProxyClient = l.ExtInfo.ClientIp
		if l.ExtInfo.HttpUserAgent != "" {
			h.AttributeNames = append(h.AttributeNames, "http_user_agent")
			h.AttributeValues = append(h.AttributeValues, l.ExtInfo.HttpUserAgent)
		}
		if l.ExtInfo.HttpReferer != "" {
			h.AttributeNames = append(h.AttributeNames, "http_referer")
			h.AttributeValues = append(h.AttributeValues, l.ExtInfo.HttpReferer)
		}
		if l.ExtInfo.RpcService != "" {
			h.AttributeNames = append(h.AttributeNames, "rpc_service")
			h.AttributeValues = append(h.AttributeValues, l.ExtInfo.RpcService)
		}
		h.AttributeNames = append(h.AttributeNames, l.ExtInfo.AttributeNames...)
		h.AttributeValues = append(h.AttributeValues, l.ExtInfo.AttributeValues...)
		h.MetricsNames = append(h.MetricsNames, l.ExtInfo.MetricsNames...)
		h.MetricsValues = append(h.MetricsValues, l.ExtInfo.MetricsValues...)
	}
	if l.TraceInfo != nil {
		h.SpanId = l.TraceInfo.SpanId
		h.TraceId = l.TraceInfo.TraceId
		h.ParentSpanId = l.TraceInfo.ParentSpanId
	}
	h.TraceIdIndex = ParseTraceIdIndex(h.TraceId, &cfg.Base.TraceIdWithIndex)

	// 处理内置协议特殊情况
	switch datatype.L7Protocol(h.L7Protocol) {
	case datatype.L7_PROTOCOL_KAFKA:
		if l.Req != nil {
			if h.responseCode == 0 && !IsKafkaSupportedCommand(l.Req.ReqType) {
				h.ResponseStatus = uint8(datatype.STATUS_NOT_EXIST)
				h.ResponseCode = nil
			}
			h.RequestId = &h.requestId
		}
	case datatype.L7_PROTOCOL_SOFARPC:
		// assume protobuf and sofa rpc Always have request_id and maybe equal to 0
		h.RequestId = &h.requestId
	}
}

func IsKafkaSupportedCommand(cmd string) bool {
	for _, supportedCmd := range []datatype.KafkaCommand{datatype.Fetch, datatype.Produce, datatype.JoinGroup, datatype.LeaveGroup, datatype.SyncGroup} {
		if cmd == datatype.KafkaCommandString[supportedCmd] {
			return true
		}
	}
	return false
}

func (h *L7FlowLog) fillExceptionDesc(l *pb.AppProtoLogsData) {
	if h.ResponseStatus != uint8(datatype.STATUS_SERVER_ERROR) && h.ResponseStatus != uint8(datatype.STATUS_CLIENT_ERROR) {
		return
	}
	code := l.Resp.Code
	switch datatype.L7Protocol(h.L7Protocol) {
	case datatype.L7_PROTOCOL_HTTP_1, datatype.L7_PROTOCOL_HTTP_2:
		h.ResponseException = GetHTTPExceptionDesc(uint16(code))
	case datatype.L7_PROTOCOL_DNS:
		h.ResponseException = GetDNSExceptionDesc(uint16(code))
	case datatype.L7_PROTOCOL_DUBBO:
		h.ResponseException = GetDubboExceptionDesc(uint16(code))
	case datatype.L7_PROTOCOL_KAFKA:
		h.ResponseException = GetKafkaExceptionDesc(int16(code))
	case datatype.L7_PROTOCOL_MQTT:
		if l.Version != "5" {
			h.ResponseException = GetMQTTV3ExceptionDesc(uint16(code))
		} else {
			h.ResponseException = GetMQTTV5ExceptionDesc(uint16(code))
		}
	case datatype.L7_PROTOCOL_MYSQL, datatype.L7_PROTOCOL_REDIS:
		fallthrough
	default:
		h.ResponseException = l.Resp.Exception
	}
}

func (h *L7FlowLog) Release() {
	ReleaseL7FlowLog(h)
}

func (h *L7FlowLog) StartTime() time.Duration {
	return time.Duration(h.L7Base.StartTime) * time.Microsecond
}

func (h *L7FlowLog) EndTime() time.Duration {
	return time.Duration(h.L7Base.EndTime) * time.Microsecond
}

func (h *L7FlowLog) String() string {
	return fmt.Sprintf("L7FlowLog: %+v\n", *h)
}

func (h *L7FlowLog) ID() uint64 {
	return h._id
}

func (h *L7FlowLog) SetID(id uint64) {
	h._id = id
}

func (b *L7Base) Fill(log *pb.AppProtoLogsData, platformData *grpc.PlatformInfoTable) {
	l := log.Base
	// 网络层
	if l.IsIpv6 == 1 {
		b.IsIPv4 = false
		if len(b.IP60) > 0 {
			b.IP60 = b.IP60[:0]
		}
		b.IP60 = append(b.IP60, l.Ip6Src...)
		if len(b.IP61) > 0 {
			b.IP61 = b.IP61[:0]
		}
		b.IP61 = append(b.IP61, l.Ip6Dst...)
	} else {
		b.IsIPv4 = true
		b.IP40 = l.IpSrc
		b.IP41 = l.IpDst
	}

	// 传输层
	b.ClientPort = uint16(l.PortSrc)
	b.ServerPort = uint16(l.PortDst)

	// 流信息
	b.FlowID = l.FlowId

	b.TapType = uint8(l.TapType)
	tunnelType := datatype.TunnelType(0)
	var natSource datatype.NATSource
	b.TapPort, b.TapPortType, natSource, tunnelType = datatype.TapPort(l.TapPort).SplitToPortTypeTunnel()
	b.NatSource = uint8(natSource)
	b.SignalSource = uint16(datatype.SIGNAL_SOURCE_PACKET)
	if b.TapPortType == datatype.TAPPORT_FROM_OTEL {
		b.SignalSource = uint16(datatype.SIGNAL_SOURCE_OTEL)
	} else if b.TapPortType == datatype.TAPPORT_FROM_EBPF {
		b.SignalSource = uint16(datatype.SIGNAL_SOURCE_EBPF)
	}
	b.TunnelType = uint8(tunnelType)
	b.TapSide = flow_metrics.TAPSideEnum(l.TapSide).String()
	b.TapSideEnum = uint8(l.TapSide)

	b.VtapID = uint16(l.VtapId)
	b.ReqTcpSeq = l.ReqTcpSeq
	b.RespTcpSeq = l.RespTcpSeq
	b.StartTime = int64(l.StartTime) / int64(time.Microsecond)
	b.EndTime = int64(l.EndTime) / int64(time.Microsecond)
	b.Time = uint32(l.EndTime / uint64(time.Second))
	b.GPID0 = l.Gpid_0
	b.GPID1 = l.Gpid_1
	b.BizType = uint8(l.BizType)

	b.ProcessID0 = l.ProcessId_0
	b.ProcessID1 = l.ProcessId_1
	b.ProcessKName0 = l.ProcessKname_0
	b.ProcessKName1 = l.ProcessKname_1
	b.SyscallTraceIDRequest = l.SyscallTraceIdRequest
	b.SyscallTraceIDResponse = l.SyscallTraceIdResponse
	b.SyscallThread0 = l.SyscallTraceIdThread_0
	b.SyscallThread1 = l.SyscallTraceIdThread_1
	b.SyscallCoroutine0 = l.SyscallCoroutine_0
	b.SyscallCoroutine1 = l.SyscallCoroutine_1
	b.SyscallCapSeq0 = l.SyscallCapSeq_0
	b.SyscallCapSeq1 = l.SyscallCapSeq_1

	// 知识图谱
	b.Protocol = uint8(log.Base.Protocol)

	b.KnowledgeGraph.FillL7(l, platformData, layers.IPProtocol(b.Protocol))
}

func (k *KnowledgeGraph) FillL7(l *pb.AppProtoLogsBaseInfo, platformData *grpc.PlatformInfoTable, protocol layers.IPProtocol) {
	k.fill(
		platformData,
		l.IsIpv6 == 1, l.IsVipInterfaceSrc == 1, l.IsVipInterfaceDst == 1,
		l.L3EpcIdSrc, l.L3EpcIdDst,
		l.IpSrc, l.IpDst,
		l.Ip6Src, l.Ip6Dst,
		l.MacSrc, l.MacDst,
		l.Gpid_0, l.Gpid_1,
		uint16(l.VtapId), l.PodId_0, l.PodId_1,
		uint16(l.PortDst),
		l.TapSide,
		protocol,
	)
}

var poolL7FlowLog = pool.NewLockFreePool(func() *L7FlowLog {
	return new(L7FlowLog)
})

func AcquireL7FlowLog() *L7FlowLog {
	l := poolL7FlowLog.Get()
	l.ReferenceCount.Reset()
	return l
}

func ReleaseL7FlowLog(l *L7FlowLog) {
	if l == nil {
		return
	}
	if l.SubReferenceCount() {
		return
	}
	*l = L7FlowLog{}
	poolL7FlowLog.Put(l)
}

var L7FlowLogCounter uint32

func ProtoLogToL7FlowLog(orgId, teamId uint16, l *pb.AppProtoLogsData, platformData *grpc.PlatformInfoTable, cfg *flowlogCfg.Config) *L7FlowLog {
	h := AcquireL7FlowLog()
	h.OrgId, h.TeamID = orgId, teamId
	h._id = genID(uint32(l.Base.EndTime/uint64(time.Second)), &L7FlowLogCounter, platformData.QueryAnalyzerID())
	h.Fill(l, platformData, cfg)
	return h
}

var extraFieldNamesNeedWriteFlowTag = [3]string{"app_service", "endpoint", "app_instance"}

func (h *L7FlowLog) GenerateNewFlowTags(cache *flow_tag.FlowTagCache) {
	l := 2
	L3EpcIDs := [2]int32{h.L3EpcID0, h.L3EpcID1}
	PodNSIDs := [2]uint16{h.PodNSID0, h.PodNSID1}
	if h.L3EpcID0 == h.L3EpcID1 && h.PodNSID0 == h.PodNSID1 {
		l = 1
	}

	time := uint32(h.L7Base.EndTime / US_TO_S_DEVISOR)

	extraFieldValuesNeedWriteFlowTag := [3]string{h.AppService, h.Endpoint, h.AppInstance}

	attributeNames := append(h.AttributeNames, extraFieldNamesNeedWriteFlowTag[:]...)
	attributeValues := append(h.AttributeValues, extraFieldValuesNeedWriteFlowTag[:]...)

	// avoid panic caused by different attributes lengths
	namesLen, valuesLen := len(attributeNames), len(attributeValues)
	minNamesLen := namesLen
	if namesLen != valuesLen {
		log.Warningf("the lengths of AttributeNames (%v) and attributeValues (%v) is different", attributeNames, attributeValues)
		if namesLen > valuesLen {
			minNamesLen = valuesLen
		}
	}

	cache.Fields = cache.Fields[:0]
	cache.FieldValues = cache.FieldValues[:0]

	for idx := 0; idx < l; idx++ {
		// reset temporary buffers
		flowTagInfo := &cache.FlowTagInfoBuffer
		*flowTagInfo = flow_tag.FlowTagInfo{
			Table:   common.L7_FLOW_ID.String(),
			VpcId:   L3EpcIDs[idx],
			PodNsId: PodNSIDs[idx],
			OrgId:   h.OrgId,
			TeamID:  h.TeamID,
		}

		for i, name := range attributeNames[:minNamesLen] {
			if attributeValues[i] == "" {
				continue
			}
			flowTagInfo.FieldName = name

			// tag + value
			flowTagInfo.FieldValue = attributeValues[i]

			if old, ok := cache.FieldValueCache.AddOrGet(*flowTagInfo, time); ok {
				if old+cache.CacheFlushTimeout >= time {
					// If there is no new fieldValue, of course there will be no new field.
					// So we can just skip the rest of the process in the loop.
					continue
				} else {
					cache.FieldValueCache.Add(*flowTagInfo, time)
				}
			}
			tagFieldValue := flow_tag.AcquireFlowTag(flow_tag.TagFieldValue)
			tagFieldValue.Timestamp = time
			tagFieldValue.FlowTagInfo = *flowTagInfo
			cache.FieldValues = append(cache.FieldValues, tagFieldValue)

			// The tag key in extraFieldNamesNeedWriteFlowTag does not need to be written into flow_tag.
			if i >= len(h.AttributeNames) {
				continue
			}
			// only tag
			flowTagInfo.FieldValue = ""
			if old, ok := cache.FieldCache.AddOrGet(*flowTagInfo, time); ok {
				if old+cache.CacheFlushTimeout >= time {
					continue
				} else {
					cache.FieldCache.Add(*flowTagInfo, time)
				}
			}
			tagField := flow_tag.AcquireFlowTag(flow_tag.TagField)
			tagField.Timestamp = time
			tagField.FlowTagInfo = *flowTagInfo
			cache.Fields = append(cache.Fields, tagField)
		}

		// metrics
		flowTagInfo.FieldType = flow_tag.FieldMetrics
		flowTagInfo.FieldValue = ""
		for _, name := range h.MetricsNames {
			flowTagInfo.FieldName = name
			if old, ok := cache.FieldCache.AddOrGet(*flowTagInfo, time); ok {
				if old+cache.CacheFlushTimeout >= time {
					continue
				} else {
					cache.FieldCache.Add(*flowTagInfo, time)
				}
			}
			tagField := flow_tag.AcquireFlowTag(flow_tag.TagField)
			tagField.Timestamp = time
			tagField.FlowTagInfo = *flowTagInfo
			cache.Fields = append(cache.Fields, tagField)
		}

	}
}
