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

package datatype

import (
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/google/gopacket/layers"

	"github.com/deepflowio/deepflow/server/libs/datatype/pb"
	"github.com/deepflowio/deepflow/server/libs/pool"
	. "github.com/deepflowio/deepflow/server/libs/utils"
)

type CloseType uint8

const (
	CloseTypeUnknown CloseType = iota

	// 流日志CloseType和流统计指标量之间的对应关系，见
	// trident/collector/quadruple_generator.go: init函数
	//	case datatype.CloseTypeTCPServerRst:
	//		_CLOSE_TYPE_METERS[flowType].ServerRstFlow = 1
	//	...
	// 流统计指标量和数据库字段名之间的对应关系，见
	// droplet-libs/flow-metrics/basic_meter.go: Anomaly结构体定义
	//	ClientRstFlow       uint64 `db:"client_rst_flow"`
	//	...
	// 数据库字段名和页面文案之间的对应关系，见
	// droplet-libs/flow-metrics/basic_meter.go: AnomalyColumns函数
	//	ANOMALY_CLIENT_RST_FLOW: {"client_rst_flow", "传输-客户端重置"},
	//	...

	CloseTypeFinish                //  1: 正常结束
	CloseTypeTCPServerRst          //  2: 传输-服务端重置
	CloseTypeTimeout               //  3: 连接超时
	_                              //  4: 【废弃】CloseTypeFlood
	CloseTypeForcedReport          //  5: 周期性上报
	_                              //  6: 【废弃】CloseTypeFoecedClose
	CloseTypeServerSynMiss         //  7: 建连-服务端 SYN 缺失
	CloseTypeServerHalfClose       //  8: 断连-服务端半关
	CloseTypeTCPClientRst          //  9: 传输-客户端重置
	CloseTypeClientAckMiss         // 10: 建连-客户端 ACK 缺失
	CloseTypeClientHalfClose       // 11: 断连-客户端半关
	_                              // 12: 【废弃】CloseTypeClientNoResponse
	CloseTypeClientSourcePortReuse // 13: 建连-客户端端口复用
	_                              // 14: 【废弃】CloseTypeClientSYNRetryLack
	CloseTypeServerReset           // 15: 建连-服务端直接重置
	_                              // 16: 【废弃】CloseTypeServerNoResponse
	CloseTypeServerQueueLack       // 17: 传输-服务端队列溢出
	CloseTypeClientEstablishReset  // 18: 建连-客户端其他重置
	CloseTypeServerEstablishReset  // 19: 建连-服务端其他重置
	CloseTypeTCPFinClientRst       // 20: 正常结束-客户端重置
	MaxCloseType
)

func (t CloseType) IsClientError() bool {
	return t == CloseTypeClientAckMiss || t == CloseTypeTCPClientRst ||
		t == CloseTypeClientSourcePortReuse ||
		t == CloseTypeClientEstablishReset
}

func (t CloseType) IsServerError() bool {
	return t == CloseTypeTCPServerRst || t == CloseTypeTimeout ||
		t == CloseTypeServerSynMiss ||
		t == CloseTypeServerReset || t == CloseTypeServerQueueLack || t == CloseTypeServerEstablishReset
}

type DeviceType uint8

const (
	unknownDeviceType DeviceType = iota
	VM
	VGw
	ThirdPartyDevice
	VMWAF
	NSPVGateway
	HostDevice
	NetworkDevice
	FloatingIP
)

type SignalSource uint8

const (
	SIGNAL_SOURCE_PACKET SignalSource = iota
	SIGNAL_SOURCE_XFLOW
	_
	SIGNAL_SOURCE_EBPF
	SIGNAL_SOURCE_OTEL
)

type TcpPerfCountsPeer struct {
	RetransCount uint32
	ZeroWinCount uint32
}

// size = 20 * 4B = 80Byte
// UDPPerfStats仅有2个字段，复用ARTSum, ARTCount
type TCPPerfStats struct { // 除特殊说明外，均为每个流统计周期（目前是自然分）清零
	RTTClientMax uint32 // us，Trident保证时延最大值不会超过3600s，能容纳在u32内
	RTTServerMax uint32 // us
	SRTMax       uint32 // us
	ARTMax       uint32 // us，UDP复用

	RTT            uint32 // us，TCP建连过程，只会计算出一个RTT
	RTTClientSum   uint32 // us，假定一条流在一分钟内的时延加和不会超过u32
	RTTServerSum   uint32 // us
	SRTSum         uint32 // us
	ARTSum         uint32 // us，UDP复用
	RTTClientCount uint32
	RTTServerCount uint32
	SRTCount       uint32
	ARTCount       uint32 // UDP复用

	TcpPerfCountsPeers [2]TcpPerfCountsPeer
	TotalRetransCount  uint32 // 整个Flow生命周期的统计量
}

type L4Protocol uint8

const (
	L4_PROTOCOL_UNKOWN L4Protocol = iota
	L4_PROTOCOL_TCP
	L4_PROTOCOL_UDP

	L4_PROTOCOL_MAX
)

type L7Protocol uint8

const (
	L7_PROTOCOL_UNKNOWN   L7Protocol = 0
	L7_PROTOCOL_HTTP_1    L7Protocol = 20
	L7_PROTOCOL_HTTP_2    L7Protocol = 21
	L7_PROTOCOL_DUBBO     L7Protocol = 40
	L7_PROTOCOL_GRPC      L7Protocol = 41
	L7_PROTOCOL_SOFARPC   L7Protocol = 43
	L7_PROTOCOL_FASTCGI   L7Protocol = 44
	L7_PROTOCOL_BRPC      L7Protocol = 45
	L7_PROTOCOL_TARS      L7Protocol = 46
	L7_PROTOCOL_SOME_IP   L7Protocol = 47
	L7_PROTOCOL_MYSQL     L7Protocol = 60
	L7_PROTOCOL_POSTGRE   L7Protocol = 61
	L7_PROTOCOL_ORACLE    L7Protocol = 62
	L7_PROTOCOL_REDIS     L7Protocol = 80
	L7_PROTOCOL_MONGODB   L7Protocol = 81
	L7_PROTOCOL_MEMCACHED L7Protocol = 82
	L7_PROTOCOL_KAFKA     L7Protocol = 100
	L7_PROTOCOL_MQTT      L7Protocol = 101
	L7_PROTOCOL_AMQP      L7Protocol = 102
	L7_PROTOCOL_OPENWIRE  L7Protocol = 103
	L7_PROTOCOL_NATS      L7Protocol = 104
	L7_PROTOCOL_PULSAR    L7Protocol = 105
	L7_PROTOCOL_ZMTP      L7Protocol = 106
	L7_PROTOCOL_ROCKETMQ  L7Protocol = 107
	L7_PROTOCOL_DNS       L7Protocol = 120
	L7_PROTOCOL_TLS       L7Protocol = 121
	L7_PROTOCOL_CUSTOM    L7Protocol = 127
)

// size = 9 * 4B = 36B
type L7PerfStats struct {
	RequestCount   uint32
	ResponseCount  uint32
	ErrClientCount uint32 // client端原因导致的响应异常数量
	ErrServerCount uint32 // server端原因导致的响应异常数量
	ErrTimeout     uint32 // request请求timeout数量
	RRTCount       uint32 // u32可记录40000M时延，一条流在一分钟内的请求数远无法达到此数值
	RRTSum         uint64 // us RRT(Request Response Time)
	RRTMax         uint32 // us RRT(Request Response Time)，Trident保证在3600s以内
}

// size = 80B + 36B + 2B = 118B
type FlowPerfStats struct {
	TCPPerfStats
	L7PerfStats
	L4Protocol
	L7Protocol
}

type FlowMetricsPeer struct {
	// 注意字节对齐!
	NatRealIp net.IP // IsVIP为true，通过MAC查询对应的IP

	ByteCount        uint64        // 每个流统计周期（目前是自然秒）清零
	L3ByteCount      uint64        // 每个流统计周期的L3载荷量
	L4ByteCount      uint64        // 每个流统计周期的L4载荷量
	PacketCount      uint64        // 每个流统计周期（目前是自然秒）清零
	TotalByteCount   uint64        // 整个Flow生命周期的统计量
	TotalPacketCount uint64        // 整个Flow生命周期的统计量
	First, Last      time.Duration // 整个Flow生命周期首包和尾包的时间戳

	L3EpcID       int32
	IsL2End       bool
	IsL3End       bool
	IsActiveHost  bool
	IsDevice      bool  // true表明是从平台数据中获取的
	TCPFlags      uint8 // 每个流统计周期的TCP Flags或运算
	TotalTCPFlags uint8 // 整个Flow生命周期的TCP Flags或运算
	// TODO: IsVIPInterface、IsVIP流日志没有存储，Encode\Decode可以不做
	IsVIPInterface bool // 目前仅支持微软Mux设备，从grpc Interface中获取
	IsVIP          bool // 从grpc cidr中获取
	IsLocalMac     bool // 同EndpointInfo中的IsLocalMac, 流日志中不需要存储
	IsLocalIp      bool // 同EndpointInfo中的IsLocalIp, 流日志中不需要存储
}

const (
	FLOW_METRICS_PEER_SRC = iota
	FLOW_METRICS_PEER_DST
	FLOW_METRICS_PEER_MAX
)

type FlowKey struct {
	VtapId  uint16
	TapType TapType
	TapPort TapPort // 采集端口信息类型 + 采集端口信息
	/* L2 */
	MACSrc MacInt
	MACDst MacInt
	/* L3 */
	IPSrc IPv4Int
	IPDst IPv4Int
	/* L3 IPv6 */
	IP6Src net.IP
	IP6Dst net.IP
	/* L4 */
	PortSrc uint16
	PortDst uint16
	Proto   layers.IPProtocol
}

type TunnelField struct {
	TxIP0, TxIP1   IPv4Int // 对应发送方向的源目的隧道IP
	RxIP0, RxIP1   IPv4Int // 对应接收方向的源目的隧道IP
	TxMAC0, TxMAC1 uint32  // 对应发送方向的源目的隧道MAC，低4字节
	RxMAC0, RxMAC1 uint32  // 对应接收方向的源目的隧道MAC，低4字节
	TxId, RxId     uint32
	Type           TunnelType
	Tier           uint8
	IsIPv6         bool
}

func (f *TunnelField) WriteToPB(p *pb.TunnelField) {
	p.TxIp0 = f.TxIP0
	p.TxIp1 = f.TxIP1
	p.RxIp0 = f.RxIP0
	p.RxIp1 = f.RxIP1
	p.TxMac0 = f.TxMAC0
	p.TxMac1 = f.TxMAC1
	p.RxMac0 = f.RxMAC0
	p.RxMac1 = f.RxMAC1
	p.TxId = f.TxId
	p.RxId = f.RxId
	p.TunnelType = uint32(f.Type)
	p.Tier = uint32(f.Tier)
	if f.IsIPv6 {
		p.IsIpv6 = 1
	}
}

func (t *TunnelField) String() string {
	if t.Type == TUNNEL_TYPE_NONE {
		return "none"
	}
	return fmt.Sprintf("%s, tx_id: %d, rx_id: %d, tier: %d, tx_0: %s %08x, tx_1: %s %08x, rx_0: %s %08x, rx_1: %s %08x",
		t.Type, t.TxId, t.RxId, t.Tier,
		IpFromUint32(t.TxIP0), t.TxMAC0, IpFromUint32(t.TxIP1), t.TxMAC1,
		IpFromUint32(t.RxIP0), t.RxMAC0, IpFromUint32(t.RxIP1), t.RxMAC1)
}

// 结构或顺序变化，需要同步修改Encode和Decode
type Flow struct {
	// 注意字节对齐!
	FlowKey
	FlowMetricsPeers [FLOW_METRICS_PEER_MAX]FlowMetricsPeer

	Tunnel TunnelField

	FlowID uint64

	// TCP Seq
	SYNSeq           uint32
	SYNACKSeq        uint32
	LastKeepaliveSeq uint32
	LastKeepaliveAck uint32

	/* Timers */
	StartTime    time.Duration
	EndTime      time.Duration
	Duration     time.Duration
	FlowStatTime time.Duration // 取整至流统计周期的开始

	/* L2 */
	VLAN    uint16
	EthType layers.EthernetType

	/* TCP Perf Data */
	*FlowPerfStats

	CloseType
	SignalSource
	IsActiveService bool
	QueueHash       uint8
	IsNewFlow       bool
	Reversed        bool
	TapSide         uint8

	AclGids []uint16
}

func (t SignalSource) String() string {
	switch t {
	case SIGNAL_SOURCE_PACKET:
		return "Packet"
	case SIGNAL_SOURCE_XFLOW:
		return "xFlow"
	case SIGNAL_SOURCE_EBPF:
		return "eBPF"
	case SIGNAL_SOURCE_OTEL:
		return "OTel"
	default:
		return "unknown"
	}
}

func (_ *FlowKey) SequentialMerge(_ *FlowKey) {
	// 所有字段均无需改变
}

func (f *FlowKey) WriteToPB(p *pb.FlowKey) {
	p.VtapId = uint32(f.VtapId)
	p.TapType = uint32(f.TapType)
	p.TapPort = uint64(f.TapPort)
	p.MacSrc = uint64(f.MACSrc)
	p.MacDst = uint64(f.MACDst)
	p.IpSrc = f.IPSrc
	p.IpDst = f.IPDst
	p.Ip6Src = f.IP6Src
	p.Ip6Dst = f.IP6Dst
	p.PortSrc = uint32(f.PortSrc)
	p.PortDst = uint32(f.PortDst)
	p.Proto = uint32(f.Proto)
}

func (f *TcpPerfCountsPeer) SequentialMerge(rhs *TcpPerfCountsPeer) {
	f.RetransCount += rhs.RetransCount
	f.ZeroWinCount += rhs.ZeroWinCount
}

func (t *TcpPerfCountsPeer) WriteToPB(p *pb.TcpPerfCountsPeer) {
	p.RetransCount = t.RetransCount
	p.ZeroWinCount = t.ZeroWinCount
}

func (f *TCPPerfStats) SequentialMerge(rhs *TCPPerfStats) {
	if f.RTTClientMax < rhs.RTTClientMax {
		f.RTTClientMax = rhs.RTTClientMax
	}
	if f.RTTServerMax < rhs.RTTServerMax {
		f.RTTServerMax = rhs.RTTServerMax
	}
	if f.SRTMax < rhs.SRTMax {
		f.SRTMax = rhs.SRTMax
	}
	if f.ARTMax < rhs.ARTMax {
		f.ARTMax = rhs.ARTMax
	}

	if f.RTT < rhs.RTT {
		f.RTT = rhs.RTT
	}
	f.RTTClientSum += rhs.RTTClientSum
	f.RTTServerSum += rhs.RTTServerSum
	f.SRTSum += rhs.SRTSum
	f.ARTSum += rhs.ARTSum
	f.RTTClientCount += rhs.RTTClientCount
	f.RTTServerCount += rhs.RTTServerCount
	f.SRTCount += rhs.SRTCount
	f.ARTCount += rhs.ARTCount
	f.TcpPerfCountsPeers[0].SequentialMerge(&rhs.TcpPerfCountsPeers[0])
	f.TcpPerfCountsPeers[1].SequentialMerge(&rhs.TcpPerfCountsPeers[1])
	f.TotalRetransCount = rhs.TotalRetransCount
}

func (f *TCPPerfStats) Reverse() {
	f.RTTClientSum, f.RTTServerSum = f.RTTServerSum, f.RTTClientSum
	f.RTTClientCount, f.RTTServerCount = f.RTTServerCount, f.RTTClientCount
	f.TcpPerfCountsPeers[0], f.TcpPerfCountsPeers[1] = f.TcpPerfCountsPeers[1], f.TcpPerfCountsPeers[0]
}

func (f *TCPPerfStats) WriteToPB(p *pb.TCPPerfStats, l4Protocol L4Protocol) {
	if l4Protocol == L4_PROTOCOL_TCP {
		p.RttClientMax = f.RTTClientMax
		p.RttServerMax = f.RTTServerMax
		p.SrtMax = f.SRTMax
		p.ArtMax = f.ARTMax

		p.Rtt = f.RTT
		p.SrtSum = f.SRTSum
		p.ArtSum = f.ARTSum

		p.SrtCount = f.SRTCount
		p.ArtCount = f.ARTCount

		if p.CountsPeerTx == nil {
			p.CountsPeerTx = &pb.TcpPerfCountsPeer{}
		}
		f.TcpPerfCountsPeers[0].WriteToPB(p.CountsPeerTx)

		if p.CountsPeerRx == nil {
			p.CountsPeerRx = &pb.TcpPerfCountsPeer{}
		}
		f.TcpPerfCountsPeers[1].WriteToPB(p.CountsPeerRx)
		p.TotalRetransCount = f.TotalRetransCount
	} else {
		*p = pb.TCPPerfStats{}
		p.ArtMax = f.ARTMax
		p.ArtSum = f.ARTSum
		p.ArtCount = f.ARTCount
	}
}

func (f *FlowMetricsPeer) SequentialMerge(rhs *FlowMetricsPeer) {
	f.ByteCount += rhs.ByteCount
	f.L3ByteCount += rhs.L3ByteCount
	f.L4ByteCount += rhs.L4ByteCount
	f.PacketCount += rhs.PacketCount
	f.TotalByteCount = rhs.TotalByteCount
	f.TotalPacketCount = rhs.TotalPacketCount
	f.First = rhs.First
	f.Last = rhs.Last
	f.TCPFlags |= rhs.TCPFlags
	f.L3EpcID = rhs.L3EpcID
	f.IsL2End = rhs.IsL2End
	f.IsL3End = rhs.IsL3End
	f.IsActiveHost = rhs.IsActiveHost
	f.IsDevice = rhs.IsDevice
	f.IsVIPInterface = rhs.IsVIPInterface
	f.IsVIP = rhs.IsVIP
	f.IsLocalMac = rhs.IsLocalMac
	f.IsLocalIp = rhs.IsLocalIp
}

func (f *FlowMetricsPeer) WriteToPB(p *pb.FlowMetricsPeer) {
	p.ByteCount = f.ByteCount
	p.L3ByteCount = f.L3ByteCount
	p.L4ByteCount = f.L4ByteCount
	p.PacketCount = f.PacketCount
	p.TotalByteCount = f.TotalByteCount
	p.TotalPacketCount = f.TotalPacketCount
	p.First = uint64(f.First)
	p.Last = uint64(f.Last)
	p.TcpFlags = uint32(f.TCPFlags)
	p.L3EpcId = f.L3EpcID
	p.IsL2End = Bool2UInt32(f.IsL2End)
	p.IsL3End = Bool2UInt32(f.IsL3End)
	p.IsActiveHost = Bool2UInt32(f.IsActiveHost)
	p.IsDevice = Bool2UInt32(f.IsDevice)
	p.IsVipInterface = Bool2UInt32(f.IsVIPInterface)
	p.IsVip = Bool2UInt32(f.IsVIP)
}

// FIXME 注意：由于FlowGenerator中TCPPerfStats在Flow方向调整之后才获取到，
// 因此这里不包含对TCPPerfStats的反向。
func (f *Flow) Reverse() {
	f.Reversed = !f.Reversed
	f.TapSide = 0 // 反向后需要重新计算
	f.Tunnel.TxIP0, f.Tunnel.TxIP1, f.Tunnel.RxIP0, f.Tunnel.RxIP1 = f.Tunnel.RxIP0, f.Tunnel.RxIP1, f.Tunnel.TxIP0, f.Tunnel.TxIP1
	f.Tunnel.TxMAC0, f.Tunnel.TxMAC1, f.Tunnel.RxMAC0, f.Tunnel.RxMAC1 = f.Tunnel.RxMAC0, f.Tunnel.RxMAC1, f.Tunnel.TxMAC0, f.Tunnel.TxMAC1
	f.Tunnel.TxId, f.Tunnel.RxId = f.Tunnel.RxId, f.Tunnel.TxId
	f.MACSrc, f.MACDst = f.MACDst, f.MACSrc
	f.IPSrc, f.IPDst = f.IPDst, f.IPSrc
	f.IP6Src, f.IP6Dst = f.IP6Dst, f.IP6Src
	f.PortSrc, f.PortDst = f.PortDst, f.PortSrc
	flowMetricsPeerSrc := &f.FlowMetricsPeers[FLOW_METRICS_PEER_SRC]
	flowMetricsPeerDst := &f.FlowMetricsPeers[FLOW_METRICS_PEER_DST]
	*flowMetricsPeerSrc, *flowMetricsPeerDst = *flowMetricsPeerDst, *flowMetricsPeerSrc
}

func (f *Flow) SequentialMerge(rhs *Flow) {
	f.FlowKey.SequentialMerge(&rhs.FlowKey)
	f.FlowMetricsPeers[FLOW_METRICS_PEER_SRC].SequentialMerge(&rhs.FlowMetricsPeers[FLOW_METRICS_PEER_SRC])
	f.FlowMetricsPeers[FLOW_METRICS_PEER_DST].SequentialMerge(&rhs.FlowMetricsPeers[FLOW_METRICS_PEER_DST])

	f.EndTime = rhs.EndTime
	f.Duration = rhs.Duration

	if rhs.FlowPerfStats != nil {
		if f.FlowPerfStats == nil {
			f.FlowPerfStats = AcquireFlowPerfStats()
		}
		f.FlowPerfStats.SequentialMerge(rhs.FlowPerfStats)
	}

	f.CloseType = rhs.CloseType
	f.IsActiveService = rhs.IsActiveService
	f.Reversed = rhs.Reversed
	if rhs.VLAN > 0 {
		f.VLAN = rhs.VLAN
	}

	// 若flow中存在KeepAlive报文，f.LastKeepaliveSeq及f.LastKeepaliveAck保存最后采集到的信息
	if rhs.LastKeepaliveSeq != 0 {
		f.LastKeepaliveSeq = rhs.LastKeepaliveSeq
	}
	if rhs.LastKeepaliveAck != 0 {
		f.LastKeepaliveAck = rhs.LastKeepaliveAck
	}

	// merge AclGids
	for _, newAclGid := range rhs.AclGids {
		has := false
		for _, oldAclGid := range f.AclGids {
			if newAclGid == oldAclGid {
				has = true
				break
			}
		}
		if !has {
			f.AclGids = append(f.AclGids, newAclGid)
		}
	}
}

func (f *Flow) WriteToPB(p *pb.Flow) {
	if p.FlowKey == nil {
		p.FlowKey = &pb.FlowKey{}
	}
	f.FlowKey.WriteToPB(p.FlowKey)

	if p.MetricsPeerSrc == nil {
		p.MetricsPeerSrc = &pb.FlowMetricsPeer{}
	}
	f.FlowMetricsPeers[FLOW_METRICS_PEER_SRC].WriteToPB(p.MetricsPeerSrc)

	if p.MetricsPeerDst == nil {
		p.MetricsPeerDst = &pb.FlowMetricsPeer{}
	}
	f.FlowMetricsPeers[FLOW_METRICS_PEER_DST].WriteToPB(p.MetricsPeerDst)

	if p.Tunnel == nil {
		p.Tunnel = &pb.TunnelField{}
	}
	f.Tunnel.WriteToPB(p.Tunnel)

	p.FlowId = f.FlowID

	p.SynSeq = f.SYNSeq
	p.SynackSeq = f.SYNACKSeq
	p.LastKeepaliveSeq = f.LastKeepaliveSeq
	p.LastKeepaliveAck = f.LastKeepaliveAck

	p.StartTime = uint64(f.StartTime)
	p.EndTime = uint64(f.EndTime)
	p.Duration = uint64(f.Duration)

	p.EthType = uint32(f.EthType)
	p.Vlan = uint32(f.VLAN)
	if f.FlowPerfStats != nil {
		p.HasPerfStats = 1
		if p.PerfStats == nil {
			p.PerfStats = &pb.FlowPerfStats{}
		}
		f.FlowPerfStats.WriteToPB(p.PerfStats)
	} else {
		p.HasPerfStats = 0
		p.PerfStats = nil
	}

	p.CloseType = uint32(f.CloseType)
	p.SignalSource = uint32(f.SignalSource)
	p.IsActiveService = Bool2UInt32(f.IsActiveService)
	p.IsNewFlow = Bool2UInt32(f.IsNewFlow)
	p.TapSide = uint32(f.TapSide)
	p.AclGids = make([]uint32, len(f.AclGids))
	for i := range f.AclGids {
		p.AclGids[i] = uint32(f.AclGids[i])
	}
}

func formatStruct(s interface{}) string {
	formatted := ""
	t := reflect.TypeOf(s)
	formatted += fmt.Sprintf("formatted kind:%v,%v;", t.Kind(), t.NumField())
	if t.Kind() != reflect.Struct {
		return ""
	}

	v := reflect.ValueOf(s)
	for i := 0; i < t.NumField(); i++ {
		if i > 0 {
			formatted += " "
		}
		formatted += fmt.Sprintf("%v: %v", t.Field(i).Name, v.Field(i))
	}
	return formatted
}

func (p L7Protocol) String(isTLS bool) string {
	switch p {
	case L7_PROTOCOL_HTTP_1:
		if isTLS {
			return "HTTP_TLS"
		} else {
			return "HTTP"
		}
	case L7_PROTOCOL_HTTP_2:
		if isTLS {
			return "HTTP2_TLS"
		} else {
			return "HTTP2"
		}
	case L7_PROTOCOL_DUBBO:
		if isTLS {
			return "Dubbo_TLS"
		} else {
			return "Dubbo"
		}
	case L7_PROTOCOL_GRPC:
		if isTLS {
			return "gRPC_TLS"
		} else {
			return "gRPC"
		}
	case L7_PROTOCOL_SOFARPC:
		if isTLS {
			return "SofaRPC_TLS"
		} else {
			return "SofaRPC"
		}
	case L7_PROTOCOL_FASTCGI:
		if isTLS {
			return "FastCGI_TLS"
		} else {
			return "FastCGI"
		}
	case L7_PROTOCOL_BRPC:
		if isTLS {
			return "bRPC_TLS"
		} else {
			return "bRPC"
		}
	case L7_PROTOCOL_TARS:
		if isTLS {
			return "Tars_TLS"
		} else {
			return "Tars"
		}
	case L7_PROTOCOL_SOME_IP:
		if isTLS {
			return "SOME/IP_TLS"
		} else {
			return "SOME/IP"
		}
	case L7_PROTOCOL_MYSQL:
		if isTLS {
			return "MySQL_TLS"
		} else {
			return "MySQL"
		}
	case L7_PROTOCOL_POSTGRE:
		if isTLS {
			return "PostgreSQL_TLS"
		} else {
			return "PostgreSQL"
		}
	case L7_PROTOCOL_ORACLE:
		if isTLS {
			return "Oracle_TLS"
		} else {
			return "Oracle"
		}
	case L7_PROTOCOL_REDIS:
		if isTLS {
			return "Redis_TLS"
		} else {
			return "Redis"
		}
	case L7_PROTOCOL_MONGODB:
		if isTLS {
			return "MongoDB_TLS"
		} else {
			return "MongoDB"
		}
	case L7_PROTOCOL_MEMCACHED:
		if isTLS {
			return "Memcached_TLS"
		} else {
			return "Memcached"
		}
	case L7_PROTOCOL_KAFKA:
		if isTLS {
			return "Kafka_TLS"
		} else {
			return "Kafka"
		}
	case L7_PROTOCOL_MQTT:
		if isTLS {
			return "MQTT_TLS"
		} else {
			return "MQTT"
		}
	case L7_PROTOCOL_AMQP:
		if isTLS {
			return "AMQP_TLS"
		} else {
			return "AMQP"
		}
	case L7_PROTOCOL_OPENWIRE:
		if isTLS {
			return "OpenWire_TLS"
		} else {
			return "OpenWire"
		}
	case L7_PROTOCOL_NATS:
		if isTLS {
			return "NATS_TLS"
		} else {
			return "NATS"
		}
	case L7_PROTOCOL_PULSAR:
		if isTLS {
			return "Pulsar_TLS"
		} else {
			return "Pulsar"
		}
	case L7_PROTOCOL_ZMTP:
		if isTLS {
			return "ZMTP_TLS"
		} else {
			return "ZMTP"
		}
	case L7_PROTOCOL_ROCKETMQ:
		if isTLS {
			return "RocketMQ_TLS"
		} else {
			return "RocketMQ"
		}
	case L7_PROTOCOL_DNS:
		if isTLS {
			return "DNS_TLS"
		} else {
			return "DNS"
		}
	case L7_PROTOCOL_TLS:
		return "TLS"
	case L7_PROTOCOL_CUSTOM:
		if isTLS {
			return "Custom_TLS"
		} else {
			return "Custom"
		}
	default:
		return "N/A"
	}
}

var L7ProtocolStringMap = map[string]L7Protocol{
	strings.ToLower(L7_PROTOCOL_HTTP_1.String(false)):   L7_PROTOCOL_HTTP_1,
	strings.ToLower(L7_PROTOCOL_HTTP_2.String(false)):   L7_PROTOCOL_HTTP_2,
	strings.ToLower(L7_PROTOCOL_DUBBO.String(false)):    L7_PROTOCOL_DUBBO,
	strings.ToLower(L7_PROTOCOL_GRPC.String(false)):     L7_PROTOCOL_GRPC,
	strings.ToLower(L7_PROTOCOL_SOFARPC.String(false)):  L7_PROTOCOL_SOFARPC,
	strings.ToLower(L7_PROTOCOL_FASTCGI.String(false)):  L7_PROTOCOL_FASTCGI,
	strings.ToLower(L7_PROTOCOL_BRPC.String(false)):     L7_PROTOCOL_BRPC,
	strings.ToLower(L7_PROTOCOL_TARS.String(false)):     L7_PROTOCOL_TARS,
	strings.ToLower(L7_PROTOCOL_SOME_IP.String(false)):  L7_PROTOCOL_SOME_IP,
	strings.ToLower(L7_PROTOCOL_MYSQL.String(false)):    L7_PROTOCOL_MYSQL,
	strings.ToLower(L7_PROTOCOL_POSTGRE.String(false)):  L7_PROTOCOL_POSTGRE,
	strings.ToLower(L7_PROTOCOL_ORACLE.String(false)):   L7_PROTOCOL_ORACLE,
	strings.ToLower(L7_PROTOCOL_REDIS.String(false)):    L7_PROTOCOL_REDIS,
	strings.ToLower(L7_PROTOCOL_MONGODB.String(false)):  L7_PROTOCOL_MONGODB,
	strings.ToLower(L7_PROTOCOL_KAFKA.String(false)):    L7_PROTOCOL_KAFKA,
	strings.ToLower(L7_PROTOCOL_MQTT.String(false)):     L7_PROTOCOL_MQTT,
	strings.ToLower(L7_PROTOCOL_AMQP.String(false)):     L7_PROTOCOL_AMQP,
	strings.ToLower(L7_PROTOCOL_OPENWIRE.String(false)): L7_PROTOCOL_OPENWIRE,
	strings.ToLower(L7_PROTOCOL_NATS.String(false)):     L7_PROTOCOL_NATS,
	strings.ToLower(L7_PROTOCOL_PULSAR.String(false)):   L7_PROTOCOL_PULSAR,
	strings.ToLower(L7_PROTOCOL_ZMTP.String(false)):     L7_PROTOCOL_ZMTP,
	strings.ToLower(L7_PROTOCOL_ROCKETMQ.String(false)): L7_PROTOCOL_ROCKETMQ,
	strings.ToLower(L7_PROTOCOL_DNS.String(false)):      L7_PROTOCOL_DNS,
	strings.ToLower(L7_PROTOCOL_TLS.String(false)):      L7_PROTOCOL_TLS,
	strings.ToLower(L7_PROTOCOL_CUSTOM.String(false)):   L7_PROTOCOL_CUSTOM,
	strings.ToLower(L7_PROTOCOL_UNKNOWN.String(false)):  L7_PROTOCOL_UNKNOWN,
}

func (p *L4Protocol) String() string {
	formatted := ""
	switch *p {
	case L4_PROTOCOL_TCP:
		formatted = "tcp"
	case L4_PROTOCOL_UDP:
		formatted = "udp"
	}
	return formatted
}

func (p *L7PerfStats) String() string {
	formatted := ""
	formatted += fmt.Sprintf("RequestCount:%v ", p.RequestCount)
	formatted += fmt.Sprintf("ResponseCount:%v ", p.ResponseCount)
	formatted += fmt.Sprintf("ErrClientCount:%v ", p.ErrClientCount)
	formatted += fmt.Sprintf("ErrServerCount:%v ", p.ErrServerCount)
	formatted += fmt.Sprintf("ErrTimeout:%v ", p.ErrTimeout)
	formatted += fmt.Sprintf("RTTCount:%v ", p.RRTCount)
	formatted += fmt.Sprintf("RTTSum:%v ", p.RRTSum)
	formatted += fmt.Sprintf("RTTMax:%v", p.RRTMax)
	return formatted
}

func (p *TCPPerfStats) String() string {
	formatted := ""
	formatted += fmt.Sprintf("RTTClientMax:%v ", p.RTTClientMax)
	formatted += fmt.Sprintf("RTTServerMax:%v ", p.RTTServerMax)
	formatted += fmt.Sprintf("SRTMax:%v ", p.SRTMax)
	formatted += fmt.Sprintf("ARTMax:%v ", p.ARTMax)
	formatted += fmt.Sprintf("RTT:%v ", p.RTT)
	formatted += fmt.Sprintf("RTTClientSum:%v ", p.RTTClientSum)
	formatted += fmt.Sprintf("RTTServerSum:%v ", p.RTTServerSum)
	formatted += fmt.Sprintf("SRTSum:%v ", p.SRTSum)
	formatted += fmt.Sprintf("ARTSum:%v ", p.ARTSum)
	formatted += fmt.Sprintf("RTTClientCount:%v ", p.RTTClientCount)
	formatted += fmt.Sprintf("RTTServerCount:%v ", p.RTTServerCount)
	formatted += fmt.Sprintf("SRTCount:%v ", p.SRTCount)
	formatted += fmt.Sprintf("ARTCount:%v ", p.ARTCount)
	formatted += fmt.Sprintf("RetransCountSrc:%v ", p.TcpPerfCountsPeers[0].RetransCount)
	formatted += fmt.Sprintf("ZeroWinCountSrc:%v ", p.TcpPerfCountsPeers[0].ZeroWinCount)
	formatted += fmt.Sprintf("RetransCountDst:%v ", p.TcpPerfCountsPeers[1].RetransCount)
	formatted += fmt.Sprintf("ZeroWinCountDst:%v ", p.TcpPerfCountsPeers[1].ZeroWinCount)
	formatted += fmt.Sprintf("TotalRetransCount:%v", p.TotalRetransCount)

	return formatted
}

func (p *FlowPerfStats) String() string {
	if p == nil {
		return ""
	}

	formatted := ""
	formatted += fmt.Sprintf("L4Protocol:%s ", p.L4Protocol.String())
	formatted += fmt.Sprintf("TCPPerfStats:{%s} ", p.TCPPerfStats.String())
	formatted += fmt.Sprintf("\n\tL7Protocol:%s ", p.L7Protocol.String(false))
	formatted += fmt.Sprintf("L7PerfStats:{%s}", p.L7PerfStats.String())
	return formatted
}

func (f *FlowKey) String() string {
	formatted := ""
	formatted += fmt.Sprintf("VtapId: %d ", f.VtapId)
	formatted += fmt.Sprintf("TapType: %d ", f.TapType)
	formatted += fmt.Sprintf("TapPort: %s\n", f.TapPort)
	formatted += fmt.Sprintf("\tMACSrc: %s ", Uint64ToMac(f.MACSrc))
	formatted += fmt.Sprintf("MACDst: %s ", Uint64ToMac(f.MACDst))
	if len(f.IP6Src) > 0 {
		formatted += fmt.Sprintf("IP6Src: %s ", f.IP6Src)
		formatted += fmt.Sprintf("IP6Dst: %s ", f.IP6Dst)
	} else {
		formatted += fmt.Sprintf("IPSrc: %s ", IpFromUint32(f.IPSrc))
		formatted += fmt.Sprintf("IPDst: %s ", IpFromUint32(f.IPDst))
	}
	formatted += fmt.Sprintf("Proto: %v ", f.Proto)
	formatted += fmt.Sprintf("PortSrc: %d ", f.PortSrc)
	formatted += fmt.Sprintf("PortDst: %d", f.PortDst)
	return formatted
}

func (f *FlowMetricsPeer) String() string {
	formatted := ""
	typeOf := reflect.TypeOf(*f)
	valueOf := reflect.ValueOf(*f)
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		value := valueOf.Field(i)
		if field.Type.Name() == "Duration" {
			formatted += fmt.Sprintf("%v: %d ", field.Name, value.Int())
		} else {
			formatted += fmt.Sprintf("%v: %+v ", field.Name, value)
		}
	}
	return formatted
}

func (f *Flow) String() string {
	formatted := fmt.Sprintf("FlowID: %d ", f.FlowID)
	formatted += fmt.Sprintf("SignalSource: %s ", f.SignalSource.String())
	formatted += fmt.Sprintf("Tunnel: %s ", f.Tunnel.String())
	formatted += fmt.Sprintf("CloseType: %d ", f.CloseType)
	formatted += fmt.Sprintf("IsActiveService: %v ", f.IsActiveService)
	formatted += fmt.Sprintf("IsNewFlow: %v ", f.IsNewFlow)
	formatted += fmt.Sprintf("QueueHash: %d ", f.QueueHash)
	formatted += fmt.Sprintf("SYNSeq: %d ", f.SYNSeq)
	formatted += fmt.Sprintf("SYNACKSeq: %d ", f.SYNACKSeq)
	formatted += fmt.Sprintf("LastKeepaliveSeq: %d ", f.LastKeepaliveSeq)
	formatted += fmt.Sprintf("LastKeepaliveAck: %d ", f.LastKeepaliveAck)
	formatted += fmt.Sprintf("FlowStatTime: %d\n", f.FlowStatTime/time.Second)
	formatted += fmt.Sprintf("\tStartTime: %d ", f.StartTime)
	formatted += fmt.Sprintf("EndTime: %d ", f.EndTime)
	formatted += fmt.Sprintf("Duration: %d\n", f.Duration)
	formatted += fmt.Sprintf("\tVLAN: %d ", f.VLAN)
	formatted += fmt.Sprintf("EthType: %d ", f.EthType)
	formatted += fmt.Sprintf("Reversed: %v ", f.Reversed)
	formatted += fmt.Sprintf("%s\n", f.FlowKey.String())
	formatted += fmt.Sprintf("\tFlowMetricsPeerSrc: {%s}\n", f.FlowMetricsPeers[FLOW_METRICS_PEER_SRC].String())
	formatted += fmt.Sprintf("\tFlowMetricsPeerDst: {%s}", f.FlowMetricsPeers[FLOW_METRICS_PEER_DST].String())
	if f.FlowPerfStats != nil {
		formatted += fmt.Sprintf("\n\t%s", f.FlowPerfStats.String())
	}
	return formatted
}

var ZeroFlowPerfStats FlowPerfStats = FlowPerfStats{}
var flowPerfStatsPool = pool.NewLockFreePool(func() *FlowPerfStats {
	return new(FlowPerfStats)
})

func AcquireFlowPerfStats() *FlowPerfStats {
	return flowPerfStatsPool.Get()
}

func ReleaseFlowPerfStats(s *FlowPerfStats) {
	*s = ZeroFlowPerfStats
	flowPerfStatsPool.Put(s)
}

func CloneFlowPerfStats(s *FlowPerfStats) *FlowPerfStats {
	newFlowPerfStats := AcquireFlowPerfStats()
	*newFlowPerfStats = *s
	return newFlowPerfStats
}

func (p *L7PerfStats) WriteToPB(b *pb.L7PerfStats) {
	b.RequestCount = p.RequestCount
	b.ResponseCount = p.ResponseCount
	b.ErrClientCount = p.ErrClientCount
	b.ErrServerCount = p.ErrServerCount
	b.ErrTimeout = p.ErrTimeout
	b.RrtCount = p.RRTCount
	b.RrtSum = p.RRTSum
	b.RrtMax = p.RRTMax
}

func (p *L7PerfStats) SequentialMerge(rhs *L7PerfStats) {
	p.RequestCount += rhs.RequestCount
	p.ResponseCount += rhs.ResponseCount
	p.ErrClientCount += rhs.ErrClientCount
	p.ErrServerCount += rhs.ErrServerCount
	p.ErrTimeout += rhs.ErrTimeout
	p.RRTCount += rhs.RRTCount
	p.RRTSum += rhs.RRTSum
	if p.RRTMax < rhs.RRTMax {
		p.RRTMax = rhs.RRTMax
	}
}

func (f *FlowPerfStats) WriteToPB(p *pb.FlowPerfStats) {
	p.L4Protocol = uint32(f.L4Protocol)
	p.L7Protocol = uint32(f.L7Protocol)

	if p.Tcp == nil {
		p.Tcp = &pb.TCPPerfStats{}
	}
	f.TCPPerfStats.WriteToPB(p.Tcp, f.L4Protocol)

	if p.L7 == nil {
		p.L7 = &pb.L7PerfStats{}
	}
	f.L7PerfStats.WriteToPB(p.L7)
}

func (f *FlowPerfStats) SequentialMerge(rhs *FlowPerfStats) {
	if f.L4Protocol == L4_PROTOCOL_UNKOWN {
		f.L4Protocol = rhs.L4Protocol
	}
	if f.L7Protocol == L7_PROTOCOL_UNKNOWN {
		f.L7Protocol = rhs.L7Protocol
	}
	f.TCPPerfStats.SequentialMerge(&rhs.TCPPerfStats)
	f.L7PerfStats.SequentialMerge(&rhs.L7PerfStats)
}
