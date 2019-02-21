package mapreduce

import (
	"math/rand"
	"reflect"
	"strconv"
	"time"

	"gitlab.x.lan/application/droplet-app/pkg/mapper/consolelog"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/flow"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/flowtype"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/fps"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/geo"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/perf"
	"gitlab.x.lan/yunshan/droplet-libs/app"
	"gitlab.x.lan/yunshan/droplet-libs/datatype"
	"gitlab.x.lan/yunshan/droplet-libs/queue"
	"gitlab.x.lan/yunshan/droplet-libs/stats"
)

const (
	perfSlots = 2
)

const GEO_FILE_LOCATION = "/usr/share/droplet/ip_info_mini.json"

func NewFlowMapProcess(
	output queue.MultiQueueWriter, input queue.MultiQueueReader,
	inputCount, docsInBuffer, variedDocLimit, windowSize, windowMoveMargin int,
) *FlowHandler {
	return NewFlowHandler([]app.FlowProcessor{
		fps.NewProcessor(),
		flow.NewProcessor(),
		perf.NewProcessor(),
		geo.NewProcessor(GEO_FILE_LOCATION),
		flowtype.NewProcessor(),
		consolelog.NewProcessor(),
	}, output, input, inputCount, docsInBuffer, variedDocLimit, windowSize, windowMoveMargin)
}

type FlowHandler struct {
	numberOfApps int
	processors   []app.FlowProcessor

	flowQueue        queue.MultiQueueReader
	flowQueueCount   int
	zmqAppQueue      queue.MultiQueueWriter
	docsInBuffer     int
	variedDocLimit   int
	windowSize       int
	windowMoveMargin int
}

func NewFlowHandler(
	processors []app.FlowProcessor,
	output queue.MultiQueueWriter, inputs queue.MultiQueueReader,
	inputCount, docsInBuffer, variedDocLimit, windowSize, windowMoveMargin int,
) *FlowHandler {
	return &FlowHandler{
		numberOfApps:     len(processors),
		processors:       processors,
		zmqAppQueue:      output,
		flowQueue:        inputs,
		flowQueueCount:   inputCount,
		docsInBuffer:     docsInBuffer,
		variedDocLimit:   variedDocLimit,
		windowSize:       windowSize,
		windowMoveMargin: windowMoveMargin,
	}
}

type subFlowHandler struct {
	numberOfApps int
	names        []string
	processors   []app.FlowProcessor
	stashes      []*Stash

	flowQueue   queue.MultiQueueReader
	zmqAppQueue queue.MultiQueueWriter

	queueIndex int
	hashKey    queue.HashKey

	counterLatch int
	statItems    []stats.StatItem

	lastFlush        time.Duration
	handlerCounter   []HandlerCounter
	processorCounter [][]ProcessorCounter
}

func (h *FlowHandler) newSubFlowHandler(index int) *subFlowHandler {
	dupProcessors := make([]app.FlowProcessor, h.numberOfApps)
	for i, proc := range h.processors {
		elem := reflect.ValueOf(proc).Elem()
		ref := reflect.New(elem.Type())
		ref.Elem().Set(elem)
		dupProcessors[i] = ref.Interface().(app.FlowProcessor)
		dupProcessors[i].Prepare()
	}
	handler := subFlowHandler{
		numberOfApps: h.numberOfApps,
		names:        make([]string, h.numberOfApps),
		processors:   dupProcessors,
		stashes:      make([]*Stash, h.numberOfApps),

		flowQueue:   h.flowQueue,
		zmqAppQueue: h.zmqAppQueue,

		queueIndex: index,
		hashKey:    queue.HashKey(rand.Int()),

		counterLatch: 0,
		statItems:    make([]stats.StatItem, 0),

		lastFlush: time.Duration(time.Now().UnixNano()),

		handlerCounter:   make([]HandlerCounter, 2),
		processorCounter: make([][]ProcessorCounter, 2),
	}
	handler.processorCounter[0] = make([]ProcessorCounter, handler.numberOfApps)
	handler.processorCounter[1] = make([]ProcessorCounter, handler.numberOfApps)

	for i := 0; i < handler.numberOfApps; i++ {
		handler.names[i] = handler.processors[i].GetName()
		handler.stashes[i] = NewStash(h.docsInBuffer, h.variedDocLimit, h.windowSize, h.windowMoveMargin)
	}
	stats.RegisterCountable("flow-mapper", &handler, stats.OptionStatTags{"index": strconv.Itoa(index)})
	return &handler
}

func (h *subFlowHandler) GetCounter() interface{} {
	oldLatch := h.counterLatch
	if h.counterLatch == 0 {
		h.counterLatch = 1
	} else {
		h.counterLatch = 0
	}
	h.statItems = h.statItems[:0]
	h.statItems = FillStatItems(h.statItems, h.handlerCounter[oldLatch], h.names, h.processorCounter[oldLatch])
	for i := 0; i < h.numberOfApps; i++ {
		h.processorCounter[oldLatch][i] = ProcessorCounter{}
	}
	h.handlerCounter[oldLatch] = HandlerCounter{}

	return h.statItems
}

func (h *subFlowHandler) Closed() bool {
	return false // FIXME: never close?
}

// processorID = -1 for all stash
func (h *subFlowHandler) putToQueue(processorID int) {
	for i, stash := range h.stashes {
		if processorID >= 0 && processorID != i {
			continue
		}
		docs := stash.Dump()
		for j := 0; j < len(docs); j += QUEUE_BATCH_SIZE {
			if j+QUEUE_BATCH_SIZE <= len(docs) {
				h.zmqAppQueue.Put(h.hashKey, docs[j:j+QUEUE_BATCH_SIZE]...)
			} else {
				h.zmqAppQueue.Put(h.hashKey, docs[j:]...)
			}
			h.hashKey++
		}
		h.processorCounter[h.counterLatch][i].emitCounter += uint64(len(docs))
		stash.Clear()
	}
}

func (h *FlowHandler) Start() {
	for i := 0; i < h.flowQueueCount; i++ {
		go h.newSubFlowHandler(i).Process()
	}
}

func isValidFlow(flow *datatype.TaggedFlow) bool {
	startTime := flow.StartTime
	endTime := flow.EndTime
	// we give flow timestamp a tolerance with one minute
	toleranceCurTime := time.Duration(time.Now().UnixNano()) + time.Minute

	if startTime > toleranceCurTime || endTime > toleranceCurTime {
		return false
	}
	if endTime != 0 && endTime < startTime {
		return false
	}

	rightMargin := endTime + 2*time.Minute
	times := [3]time.Duration{
		flow.CurStartTime,
		flow.FlowMetricsPeerSrc.ArrTimeLast,
		flow.FlowMetricsPeerDst.ArrTimeLast,
	}
	for i := 0; i < 3; i++ {
		if times[i] > rightMargin || times[i] > toleranceCurTime {
			return false
		}
	}
	return true
}

func (h *subFlowHandler) Process() error {
	elements := make([]interface{}, QUEUE_BATCH_SIZE)

	for {
		n := h.flowQueue.Gets(queue.HashKey(h.queueIndex), elements)
		for _, e := range elements[:n] {
			if e == nil {
				h.Flush(-1)
				continue
			}

			flow := e.(*datatype.TaggedFlow)
			if !isValidFlow(flow) {
				datatype.ReleaseTaggedFlow(flow)
				h.handlerCounter[h.counterLatch].dropCounter++
				continue
			}

			h.handlerCounter[h.counterLatch].flowCounter++
			for i, processor := range h.processors {
				docs := processor.Process(flow, false)
				rejected := uint64(0)
				h.processorCounter[h.counterLatch][i].docCounter += uint64(len(docs))
				if uint64(len(docs)) > h.processorCounter[h.counterLatch][i].maxCounter {
					h.processorCounter[h.counterLatch][i].maxCounter = uint64(len(docs))
				}
				for {
					docs, rejected = h.stashes[i].Add(docs)
					h.processorCounter[h.counterLatch][i].rejectionCounter += rejected
					if docs == nil {
						break
					}
					h.processorCounter[h.counterLatch][i].flushCounter++
					h.Flush(i)
				}
			}
			datatype.ReleaseTaggedFlow(flow)
		}
		if time.Duration(time.Now().UnixNano())-h.lastFlush >= FLUSH_INTERVAL {
			h.Flush(-1)
		}
	}
}

func (h *subFlowHandler) Flush(processorID int) {
	if processorID == -1 { // 单独Flush某个processor的stash时不更新
		h.lastFlush = time.Duration(time.Now().UnixNano())
	}
	h.putToQueue(processorID)
}
