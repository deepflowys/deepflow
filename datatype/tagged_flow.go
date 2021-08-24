package datatype

import (
	"fmt"

	"gitlab.yunshan.net/yunshan/droplet-libs/codec"
	"gitlab.yunshan.net/yunshan/droplet-libs/pool"
)

const (
	VERSION = 20210702
)

type TaggedFlow struct {
	Flow
	Tag

	pool.ReferenceCount
}

func (f *TaggedFlow) SequentialMerge(rhs *TaggedFlow) {
	f.Flow.SequentialMerge(&rhs.Flow)
	// f.Tag.SequentialMerge(rhs.Tag)  // 目前无需发送,不merge
}

func (f *TaggedFlow) Encode(encoder *codec.SimpleEncoder) error {
	f.Flow.Encode(encoder)
	// f.Tag.Encode(encoder)  // 目前无需发送,不encode
	return nil
}

func (f *TaggedFlow) Decode(decoder *codec.SimpleDecoder) {
	f.Flow.Decode(decoder)
	// f.Tag.Decode(decoder)
}

func (f *TaggedFlow) Release() {
	ReleaseTaggedFlow(f)
}

func (f *TaggedFlow) Reverse() {
	f.Flow.Reverse()
	f.Tag.Reverse()
}

var taggedFlowPool = pool.NewLockFreePool(func() interface{} {
	return new(TaggedFlow)
})

func AcquireTaggedFlow() *TaggedFlow {
	f := taggedFlowPool.Get().(*TaggedFlow)
	f.ReferenceCount.Reset()
	return f
}

func ReleaseTaggedFlow(taggedFlow *TaggedFlow) {
	if taggedFlow.SubReferenceCount() {
		return
	}

	if taggedFlow.FlowPerfStats != nil {
		ReleaseFlowPerfStats(taggedFlow.FlowPerfStats)
		taggedFlow.FlowPerfStats = nil
	}
	*taggedFlow = TaggedFlow{}
	taggedFlowPool.Put(taggedFlow)
}

// 注意：不拷贝FlowPerfStats
func CloneTaggedFlowForPacketStat(taggedFlow *TaggedFlow) *TaggedFlow {
	newTaggedFlow := AcquireTaggedFlow()
	*newTaggedFlow = *taggedFlow
	newTaggedFlow.FlowPerfStats = nil
	newTaggedFlow.ReferenceCount.Reset()
	return newTaggedFlow
}

func CloneTaggedFlow(taggedFlow *TaggedFlow) *TaggedFlow {
	newTaggedFlow := AcquireTaggedFlow()
	*newTaggedFlow = *taggedFlow
	newTaggedFlow.ReferenceCount.Reset()
	if taggedFlow.FlowPerfStats != nil {
		newTaggedFlow.FlowPerfStats = CloneFlowPerfStats(taggedFlow.FlowPerfStats)
	}
	return newTaggedFlow
}

func PseudoCloneTaggedFlowHelper(items []interface{}) {
	for _, e := range items {
		e.(*TaggedFlow).AddReferenceCount()
	}
}

func (f *TaggedFlow) String() string {
	return fmt.Sprintf("%s\n\tTag: %+v", f.Flow.String(), f.Tag)
}
