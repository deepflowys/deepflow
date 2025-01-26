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

	"github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/pool"
)

const (
	DefaultPcapPartition = ckdb.TimeFuncHour
)

type PcapStore struct {
	Time        uint32
	StartTime   int64
	EndTime     int64
	FlowID      uint64
	VtapID      uint16
	PacketCount uint32
	PacketBatch []byte
	AclGids     []uint16

	// Not stored, only determines which database to store in.
	// When Orgid is 0 or 1, it is stored in database 'flow_log', otherwise stored in '<OrgId>_flow_log'.
	OrgId  uint16
	TeamID uint16
}

func PcapStoreColumns() []*ckdb.Column {
	return []*ckdb.Column{
		ckdb.NewColumn("time", ckdb.DateTime).SetComment("精度: 秒"),
		ckdb.NewColumn("start_time", ckdb.DateTime64us).SetComment("精度: 微秒"),
		ckdb.NewColumn("end_time", ckdb.DateTime64us).SetComment("精度: 微秒"),
		ckdb.NewColumn("flow_id", ckdb.UInt64).SetIndex(ckdb.IndexMinmax),
		ckdb.NewColumn("agent_id", ckdb.UInt16).SetIndex(ckdb.IndexSet),
		ckdb.NewColumn("packet_count", ckdb.UInt32).SetIndex(ckdb.IndexNone),
		ckdb.NewColumn("packet_batch", ckdb.String).SetIndex(ckdb.IndexNone).SetComment("data format reference: https://www.ietf.org/archive/id/draft-gharris-opsawg-pcap-01.html"),
		ckdb.NewColumn("acl_gids", ckdb.ArrayUInt16).SetIndex(ckdb.IndexNone),
		ckdb.NewColumn("team_id", ckdb.UInt16).SetIndex(ckdb.IndexNone),
	}
}

func (s *PcapStore) NativeTagVersion() uint32 {
	return 0
}

func (s *PcapStore) OrgID() uint16 {
	return s.OrgId
}

func (p *PcapStore) Release() {
	ReleasePcapStore(p)
}

func (p *PcapStore) String() string {
	return fmt.Sprintf("PcapStore: %+v\n", *p)
}

var poolPcapStore = pool.NewLockFreePool(func() *PcapStore {
	return new(PcapStore)
})

func AcquirePcapStore() *PcapStore {
	l := poolPcapStore.Get()
	return l
}

func ReleasePcapStore(l *PcapStore) {
	if l == nil {
		return
	}
	t := l.PacketBatch[:0]
	ids := l.AclGids[:0]
	*l = PcapStore{}
	l.PacketBatch = t
	l.AclGids = ids
	poolPcapStore.Put(l)
}

func GenPcapCKTable(cluster, storagePolicy, ckdbType string, ttl int, coldStorage *ckdb.ColdStorage) *ckdb.Table {
	timeKey := "time"
	engine := ckdb.MergeTree
	orderKeys := []string{"flow_id", timeKey, "agent_id"}

	return &ckdb.Table{
		Version:         common.CK_VERSION,
		Database:        PCAP_DB,
		DBType:          ckdbType,
		LocalName:       PCAP_TABLE + ckdb.LOCAL_SUBFFIX,
		GlobalName:      PCAP_TABLE,
		Columns:         PcapStoreColumns(),
		TimeKey:         timeKey,
		TTL:             ttl,
		PartitionFunc:   DefaultPcapPartition,
		Engine:          engine,
		Cluster:         cluster,
		StoragePolicy:   storagePolicy,
		ColdStorage:     *coldStorage,
		OrderKeys:       orderKeys,
		PrimaryKeyCount: len(orderKeys),
	}
}
