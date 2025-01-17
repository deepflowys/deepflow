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
	"fmt"

	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/codec"
	"github.com/deepflowio/deepflow/server/libs/pool"
)

const BLOCK_HEAD_SIZE = 16

type L4Packet struct {
	StartTime int64
	EndTime   int64
	FlowID    uint64
	VtapID    uint16

	// Not stored, only determines which database to store in.
	// When Orgid is 0 or 1, it is stored in database 'flow_log', otherwise stored in '<OrgId>_flow_log'.
	OrgId       uint16
	TeamID      uint16
	PacketCount uint32
	PacketBatch string
}

func L4PacketColumns() []*ckdb.Column {
	return []*ckdb.Column{
		ckdb.NewColumn("time", ckdb.DateTime).SetComment("精度: 秒"),
		ckdb.NewColumn("start_time", ckdb.DateTime64us).SetComment("精度: 微秒"),
		ckdb.NewColumn("end_time", ckdb.DateTime64us).SetComment("精度: 微秒"),
		ckdb.NewColumn("flow_id", ckdb.UInt64).SetIndex(ckdb.IndexMinmax),
		ckdb.NewColumn("agent_id", ckdb.UInt16).SetIndex(ckdb.IndexSet),
		ckdb.NewColumn("team_id", ckdb.UInt16).SetIndex(ckdb.IndexSet),
		ckdb.NewColumn("packet_count", ckdb.UInt32).SetIndex(ckdb.IndexNone),
		ckdb.NewColumn("packet_batch", ckdb.String).SetIndex(ckdb.IndexNone),
	}
}

func (s *L4Packet) NativeTagVersion() uint32 {
	return 0
}

func (s *L4Packet) OrgID() uint16 {
	return s.OrgId
}

func (p *L4Packet) Release() {
	ReleaseL4Packet(p)
}

func (p *L4Packet) String() string {
	return fmt.Sprintf("L4Packet: %+v\n", *p)
}

var poolL4Packet = pool.NewLockFreePool(func() *L4Packet {
	return new(L4Packet)
})

func AcquireL4Packet() *L4Packet {
	l := poolL4Packet.Get()
	return l
}

func ReleaseL4Packet(l *L4Packet) {
	if l == nil {
		return
	}
	*l = L4Packet{}
	poolL4Packet.Put(l)
}

func DecodePacketSequence(vtapID, orgId, teamId uint16, decoder *codec.SimpleDecoder) (*L4Packet, error) {
	l4Packet := AcquireL4Packet()
	l4Packet.VtapID = vtapID
	blockSize := decoder.ReadU32()
	if blockSize <= BLOCK_HEAD_SIZE {
		return l4Packet, fmt.Errorf("vtap id(%d) packet block size(%d) < BLOCK_HEAD_SIZE(%d)", vtapID, blockSize, BLOCK_HEAD_SIZE)
	}
	l4Packet.FlowID = decoder.ReadU64()
	endTimePacketCount := decoder.ReadU64()
	l4Packet.EndTime = int64(endTimePacketCount << 8 >> 8)
	// sequence packet defaults to a maximum of 5s timeout sending, so the minimum value of StartTime is EndTime - 5s
	l4Packet.StartTime = l4Packet.EndTime - 5*US_TO_S_DEVISOR
	l4Packet.PacketCount = uint32(endTimePacketCount >> 56)
	l4Packet.PacketBatch = string(decoder.ReadBytesN(int(blockSize) - BLOCK_HEAD_SIZE))

	l4Packet.OrgId, l4Packet.TeamID = orgId, teamId

	return l4Packet, nil
}
