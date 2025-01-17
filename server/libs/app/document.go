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

package app

import (
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/deepflowio/deepflow/server/libs/ckdb"
	flow_metrics "github.com/deepflowio/deepflow/server/libs/flow-metrics"
	"github.com/deepflowio/deepflow/server/libs/pool"
	"github.com/deepflowio/deepflow/server/libs/utils"
)

const (
	VERSION                   = 20220117 // 修改Document的序列化结构时需同步修改此常量
	LAST_SIMPLE_CODEC_VERSION = 20220111 // 这个版本及之前的版本使用 simple_codec, 之后的版本使用pb_codec
)

type DocumentFlag uint32

const (
	FLAG_PER_SECOND_METRICS DocumentFlag = 1 << iota
)

type Document interface {
	Tags() *flow_metrics.Tag
	Time() uint32
	Flag() DocumentFlag
	Meter() flow_metrics.Meter
	Release()
	OrgID() uint16
	TableID() (uint8, error)
	String() string
	AddReferenceCount()
	AddReferenceCountN(n int32)
	DataSource() uint32
	GetFieldValueByOffsetAndKind(offset uintptr, kind reflect.Kind, dataType utils.DataType) interface{}
	TimestampUs() int64

	NewColumnBlock() ckdb.CKColumnBlock
	AppendToColumnBlock(ckdb.CKColumnBlock)
	NativeTagVersion() uint32
}

type DocumentBase struct {
	pool.ReferenceCount
	Timestamp uint32 `json:"time" category:"$tag" sub:"flow_info"`
	Flags     DocumentFlag
	flow_metrics.Tag
}

func (b *DocumentBase) Tags() *flow_metrics.Tag {
	return &b.Tag
}

func (b *DocumentBase) Time() uint32 {
	return b.Timestamp
}

func (b *DocumentBase) Flag() DocumentFlag {
	return b.Flags
}

func (b *DocumentBase) TableID() (uint8, error) {
	return b.Tag.TableID((b.Flags & FLAG_PER_SECOND_METRICS) == 1)
}

func (b *DocumentBase) NativeTagVersion() uint32 {
	return 0
}

func (b *DocumentBase) OrgID() uint16 {
	return b.Tag.OrgId
}

func (b *DocumentBase) DataSource() uint32 {
	dataSourceId, _ := b.TableID()
	return uint32(dataSourceId)
}

func (e *DocumentBase) TimestampUs() int64 {
	return int64(time.Duration(e.Timestamp) * time.Second / time.Microsecond)
}

type DocumentFlow struct {
	DocumentBase
	flow_metrics.FlowMeter
}

type DocumentApp struct {
	DocumentBase
	flow_metrics.AppMeter
}

type DocumentUsage struct {
	DocumentBase
	flow_metrics.UsageMeter
}

func (d *DocumentFlow) String() string {
	return fmt.Sprintf("\n{\n\ttimestamp: %d\tFlags: b%b\n\ttag: %+v\n\tmeter: %#v\n}\n",
		d.Timestamp, d.Flags, d.Tag, d.FlowMeter)
}

var poolDocumentFlow = pool.NewLockFreePool(func() *DocumentFlow {
	return &DocumentFlow{}
})

func AcquireDocumentFlow() *DocumentFlow {
	d := poolDocumentFlow.Get()
	d.ReferenceCount.Reset()
	return d
}

func ReleaseDocumentFlow(doc *DocumentFlow) {
	if doc == nil || doc.SubReferenceCount() {
		return
	}

	*doc = DocumentFlow{}
	poolDocumentFlow.Put(doc)
}

func (d *DocumentFlow) Release() {
	ReleaseDocumentFlow(d)
}

func (d *DocumentFlow) GetFieldValueByOffsetAndKind(offset uintptr, kind reflect.Kind, dataType utils.DataType) interface{} {
	return utils.GetValueByOffsetAndKind(uintptr(unsafe.Pointer(d)), offset, kind, dataType)
}

func (d *DocumentFlow) GetStringValue(offset uintptr, kind reflect.Kind) string {
	return ""
}

func (d *DocumentFlow) Meter() flow_metrics.Meter {
	return &d.FlowMeter
}

func (d *DocumentApp) String() string {
	return fmt.Sprintf("\n{\n\ttimestamp: %d\tFlags: b%b\n\ttag: %+v\n\tmeter: %#v\n}\n",
		d.Timestamp, d.Flags, d.Tag, d.AppMeter)
}

var poolDocumentApp = pool.NewLockFreePool(func() *DocumentApp {
	return &DocumentApp{}
})

func AcquireDocumentApp() *DocumentApp {
	d := poolDocumentApp.Get()
	d.ReferenceCount.Reset()
	return d
}

func ReleaseDocumentApp(doc *DocumentApp) {
	if doc == nil || doc.SubReferenceCount() {
		return
	}

	*doc = DocumentApp{}
	poolDocumentApp.Put(doc)
}

func (d *DocumentApp) Release() {
	ReleaseDocumentApp(d)
}

func (d *DocumentApp) Meter() flow_metrics.Meter {
	return &d.AppMeter
}

func (d *DocumentApp) GetFieldValueByOffsetAndKind(offset uintptr, kind reflect.Kind, dataType utils.DataType) interface{} {
	return utils.GetValueByOffsetAndKind(uintptr(unsafe.Pointer(d)), offset, kind, dataType)
}

func (d *DocumentUsage) String() string {
	return fmt.Sprintf("\n{\n\ttimestamp: %d\tFlags: b%b\n\ttag: %+v\n\tmeter: %#v\n}\n",
		d.Timestamp, d.Flags, d.Tag, d.UsageMeter)
}

var poolDocumentUsage = pool.NewLockFreePool(func() *DocumentUsage {
	return &DocumentUsage{}
})

func AcquireDocumentUsage() *DocumentUsage {
	d := poolDocumentUsage.Get()
	d.ReferenceCount.Reset()
	return d
}

func ReleaseDocumentUsage(doc *DocumentUsage) {
	if doc == nil || doc.SubReferenceCount() {
		return
	}

	*doc = DocumentUsage{}
	poolDocumentUsage.Put(doc)
}

func (d *DocumentUsage) Release() {
	ReleaseDocumentUsage(d)
}

func (d *DocumentUsage) Meter() flow_metrics.Meter {
	return &d.UsageMeter
}

func (d *DocumentUsage) GetFieldValueByOffsetAndKind(offset uintptr, kind reflect.Kind, dataType utils.DataType) interface{} {
	return utils.GetValueByOffsetAndKind(uintptr(unsafe.Pointer(d)), offset, kind, dataType)
}
