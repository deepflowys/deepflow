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

type AppServiceTag struct {
	Time        uint32 // s
	Table       string
	AppService  string
	AppInstance string
	TeamID      uint16
	OrgId       uint16
}

func (t *AppServiceTag) NativeTagVersion() uint32 {
	return 0
}

func (t *AppServiceTag) OrgID() uint16 {
	return t.OrgId
}

func AppServiceTagColumns() []*ckdb.Column {
	columns := []*ckdb.Column{}
	columns = append(columns,
		ckdb.NewColumn("time", ckdb.DateTime),
		ckdb.NewColumn("table", ckdb.LowCardinalityString),
		ckdb.NewColumn("app_service", ckdb.LowCardinalityString),
		ckdb.NewColumn("app_instance", ckdb.LowCardinalityString),
		ckdb.NewColumn("team_id", ckdb.UInt16),
	)
	return columns
}

func GenAppServiceTagCKTable(cluster, storagePolicy, tableName, ckdbType string, ttl int, partition ckdb.TimeFuncType) *ckdb.Table {
	timeKey := "time"
	engine := ckdb.ReplacingMergeTree
	orderKeys := []string{"table", "app_service", "app_instance"}

	return &ckdb.Table{
		Version:         common.CK_VERSION,
		Database:        FLOW_TAG_DB,
		DBType:          ckdbType,
		LocalName:       tableName + ckdb.LOCAL_SUBFFIX,
		GlobalName:      tableName,
		Columns:         AppServiceTagColumns(),
		TimeKey:         timeKey,
		TTL:             ttl,
		PartitionFunc:   partition,
		Engine:          engine,
		Cluster:         cluster,
		StoragePolicy:   storagePolicy,
		OrderKeys:       orderKeys,
		PrimaryKeyCount: len(orderKeys),
	}
}

func (t *AppServiceTag) Release() {
	ReleaseAppServiceTag(t)
}

var appServiceTagPool = pool.NewLockFreePool(func() *AppServiceTag {
	return &AppServiceTag{}
})

func AcquireAppServiceTag() *AppServiceTag {
	f := appServiceTagPool.Get()
	return f
}

var emptyAppServiceTag = AppServiceTag{}

func ReleaseAppServiceTag(t *AppServiceTag) {
	if t == nil {
		return
	}
	*t = emptyAppServiceTag
	appServiceTagPool.Put(t)
}
