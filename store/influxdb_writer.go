package store

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/op/go-logging"

	"gitlab.x.lan/yunshan/droplet-libs/app"
	"gitlab.x.lan/yunshan/droplet-libs/pool"
	"gitlab.x.lan/yunshan/droplet-libs/queue"
	"gitlab.x.lan/yunshan/droplet-libs/stats"
)

var log = logging.MustGetLogger("store")

const (
	QUEUE_FETCH_MAX_SIZE   = 1024
	DEFAULT_BATCH_SIZE     = 512 * 1024
	DEFAULT_FLUSH_TIMEOUT  = 5 // 单位 秒
	DEFAULT_QUEUE_SIZE     = 256 * 1024
	INFLUXDB_PRECISION_S   = "s"
	UNIX_TIMESTAMP_TO_TIME = (1969*365 + 1969/4 - 1969/100 + 1969/400) * 24 * 60 * 60
	TIME_BINARY_LEN        = 15
)

type InfluxdbItem interface {
	MarshalToBytes([]byte) int
	GetDBName() string
	Release()
}

type InfluxdbPoint struct {
	db          string
	measurement string
	tag         map[string]string
	field       map[string]int64
	timestamp   uint32 // 秒
}

type Counter struct {
	WriteSuccessCount int64 `statsd:"write-success-count"`
	WriteFailedCount  int64 `statsd:"write-failed-count"`
}

type Confidence struct {
	db          string
	measurement string
	shardID     string
	timestamp   int64 // time.Duration
	status      RepairStatus
}

type PointCache struct {
	bp     client.BatchPoints
	buffer []byte
	offset int
}

var bufferPool = pool.NewLockFreePool(func() interface{} {
	return make([]byte, 0, DEFAULT_BATCH_SIZE+app.MAX_DOC_STRING_LENGTH)
})

func acquireBuffer() []byte {
	return bufferPool.Get().([]byte)
}

// 只能realease acquireBuffer出来的 slice
func releaseBuffer(b []byte) {
	b = b[:0]
	bufferPool.Put(b)
}

var pointCachePool = pool.NewLockFreePool(func() interface{} {
	return &PointCache{}
})

func acquirePointCache() *PointCache {
	return pointCachePool.Get().(*PointCache)
}

func releasePointCache(p *PointCache) {
	if p == nil {
		return
	}
	if p.buffer != nil {
		releaseBuffer(p.buffer)
	}
	*p = PointCache{}
	pointCachePool.Put(p)
}

type WriterInfo struct {
	httpClient  client.Client
	isConnected bool
	writeTime   int64
	pointCache  map[string]*PointCache
	counter     *Counter
	stats.Closable
}

type DBCreateCtl struct {
	HttpClient client.Client
	ExistDBs   map[string]bool
	sync.RWMutex
}

type RetentionPolicy struct {
	name          string
	duration      string
	shardDuration string
	defaultFlag   bool
}

type InfluxdbWriter struct {
	ReplicaEnabled bool
	DataQueues     queue.FixedMultiQueue
	ReplicaQueues  queue.FixedMultiQueue

	Name                    string
	ShardID                 string
	QueueCount              int
	QueueWriterInfosPrimary []*WriterInfo
	QueueWriterInfosReplica []*WriterInfo

	DBCreateCtlPrimary DBCreateCtl
	DBCreateCtlReplica DBCreateCtl

	PrimaryClient client.Client
	BatchSize     int
	FlushTimeout  int64
	RP            RetentionPolicy
	wg            sync.WaitGroup
	exit          bool
}

func NewInfluxdbWriter(addrPrimary, addrReplica, name, shardID string, queueCount int) (*InfluxdbWriter, error) {
	w := &InfluxdbWriter{
		Name:         name,
		QueueCount:   queueCount,
		BatchSize:    DEFAULT_BATCH_SIZE,
		FlushTimeout: int64(DEFAULT_FLUSH_TIMEOUT),
		ShardID:      shardID,
	}

	httpClient, err := client.NewHTTPClient(client.HTTPConfig{Addr: addrPrimary})
	if err != nil {
		log.Error("create influxdb http client failed:", err)
		return nil, err
	}

	if _, _, err = httpClient.Ping(0); err != nil {
		log.Errorf("http connect to influxdb(%s) failed: %s", addrPrimary, err)
		return nil, err
	}
	w.PrimaryClient = httpClient
	w.DBCreateCtlPrimary.HttpClient = httpClient
	w.DBCreateCtlPrimary.ExistDBs = make(map[string]bool)
	w.DataQueues = queue.NewOverwriteQueues(
		name, queue.HashKey(queueCount), DEFAULT_QUEUE_SIZE,
		queue.OptionFlushIndicator(time.Second),
		queue.OptionRelease(func(p interface{}) { p.(InfluxdbItem).Release() }))
	w.QueueWriterInfosPrimary, err = newWriterInfos(addrPrimary, queueCount)
	if err != nil {
		log.Error("create queue writer infos failed:", err)
		return nil, err
	}

	if addrReplica != "" {
		w.ReplicaEnabled = true
		httpClient, err := client.NewHTTPClient(client.HTTPConfig{Addr: addrReplica})
		if err != nil {
			log.Error("create replica influxdb http client failed:", err)
			return nil, err
		}

		if _, _, err = httpClient.Ping(0); err != nil {
			log.Errorf("http connect to influxdb(%s) failed: %s", addrReplica, err)
		}
		w.DBCreateCtlReplica.HttpClient = httpClient
		w.DBCreateCtlReplica.ExistDBs = make(map[string]bool)

		w.QueueWriterInfosReplica, err = newWriterInfos(addrReplica, queueCount)
		if err != nil {
			log.Error("create queue writer infos failed:", err)
			return nil, err
		}

		w.ReplicaQueues = queue.NewOverwriteQueues(
			name+"_replica", queue.HashKey(queueCount), 512,
			queue.OptionFlushIndicator(time.Second),
			queue.OptionRelease(func(p interface{}) { releasePointCache(p.(*PointCache)) }))
	}

	return w, nil
}

func (w *InfluxdbWriter) SetQueueSize(size int) {
	w.DataQueues = queue.NewOverwriteQueues(w.Name, queue.HashKey(w.QueueCount), size,
		queue.OptionFlushIndicator(time.Second),
		queue.OptionRelease(func(p interface{}) { p.(InfluxdbItem).Release() }))
}

func (w *InfluxdbWriter) SetBatchSize(size int) {
	if size < DEFAULT_BATCH_SIZE {
		w.BatchSize = size
	}
}

func (w *InfluxdbWriter) SetBatchTimeout(timeout int64) {
	w.FlushTimeout = timeout
}

func (w *InfluxdbWriter) SetRetentionPolicy(rp, duration, shardDuration string, defaultFlag bool) {
	w.RP.name = rp
	w.RP.duration = duration
	w.RP.shardDuration = shardDuration
	w.RP.defaultFlag = defaultFlag
}

// 高性能接口，需要用户实现InfluxdbItem接口
func (w *InfluxdbWriter) Put(queueID int, item ...interface{}) {
	w.DataQueues.Put(queue.HashKey(queueID), item...)
}

// 普通接口，性能较差，易用
func (w *InfluxdbWriter) PutPoint(queueID int, db, measurement string, tag map[string]string, field map[string]int64, timestamp uint32) {
	w.DataQueues.Put(queue.HashKey(queueID), &InfluxdbPoint{
		db:          db,
		measurement: measurement,
		tag:         tag,
		field:       field,
		timestamp:   timestamp,
	})
}

func (w *InfluxdbWriter) Run() {
	w.createDB(w.PrimaryClient, CONFIDENCE_DB)
	for n := 0; n < w.QueueCount; n++ {
		go w.queueProcess(n)
		if w.ReplicaEnabled {
			go w.queueProcessReplica(n)
		}
	}
}

func MarshalTimestampTo(ts uint32, buffer []byte) int {
	// golang time binary format:
	//     byte 0: version (1)
	//     bytes 1-8: seconds (big-endian)
	//     bytes 9-12: nanoseconds (big-endian)
	//     bytes 13-14: zone offset in minutes (-1 for UTC)
	realTime := uint64(ts) + UNIX_TIMESTAMP_TO_TIME
	buffer[0] = 1
	binary.BigEndian.PutUint64(buffer[1:], realTime)
	binary.BigEndian.PutUint32(buffer[9:], 0)
	buffer[13] = ^byte(0)
	buffer[14] = ^byte(0)
	return TIME_BINARY_LEN
}

func (p *InfluxdbPoint) MarshalToBytes(buffer []byte) int {
	offset := 0
	size := copy(buffer[offset+4:], p.measurement)

	for key, value := range p.tag {
		size += copy(buffer[offset+4+size:], ","+key+"="+value)
	}

	binary.BigEndian.PutUint32(buffer[offset:], uint32(size))
	offset += (4 + size)

	size = 0
	for key, value := range p.field {
		if size != 0 {
			size += copy(buffer[offset+4+size:], ",")
		}
		size += copy(buffer[offset+4+size:], key+"="+strconv.FormatInt(value, 10)+"i")
	}

	binary.BigEndian.PutUint32(buffer[offset:], uint32(size))
	offset += (4 + size)

	offset += MarshalTimestampTo(p.timestamp, buffer[offset:])

	return offset
}

func (p *InfluxdbPoint) GetDBName() string {
	return p.db
}

func (p *InfluxdbPoint) Release() {
}

func newWriterInfos(httpAddr string, count int) ([]*WriterInfo, error) {
	ws := make([]*WriterInfo, count)
	for i := 0; i < count; i++ {
		httpClient, err := client.NewHTTPClient(client.HTTPConfig{Addr: httpAddr})
		if err != nil {
			log.Error("create influxdb http client %d failed: ", i, err)
			return nil, err
		}
		if _, _, err = httpClient.Ping(0); err != nil {
			log.Errorf("http %d connect to influxdb(%s) failed: %s", i, httpAddr, err)
		}
		log.Infof("new influxdb http client %d: %s", i, httpAddr)
		ws[i] = &WriterInfo{
			httpClient: httpClient,
			writeTime:  time.Now().Unix(),
			pointCache: make(map[string]*PointCache),
			counter:    &Counter{},
		}
	}
	return ws, nil
}

func (i *WriterInfo) GetCounter() interface{} {
	var counter *Counter
	counter, i.counter = i.counter, &Counter{}

	return counter
}

func (i *WriterInfo) Close() {
	if i.httpClient != nil {
		i.httpClient.Close()
		i.httpClient = nil
	}
	i.Closable.Close()
}

func newPointCache(db, rp string) *PointCache {
	pc := acquirePointCache()
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        db,
		Precision:       INFLUXDB_PRECISION_S,
		RetentionPolicy: rp,
	})
	if err != nil {
		panic(fmt.Sprintf("create BatchPoints for db %s failed: %s", db, err))
	}
	pc.bp = bp
	pc.buffer = acquireBuffer()
	return pc
}

func (w *InfluxdbWriter) queueProcess(queueID int) {
	stats.RegisterCountable(w.Name, w.QueueWriterInfosPrimary[queueID], stats.OptionStatTags{"thread": strconv.Itoa(queueID)})
	defer w.QueueWriterInfosPrimary[queueID].Close()
	defer w.wg.Done()
	w.wg.Add(1)

	rawItems := make([]interface{}, QUEUE_FETCH_MAX_SIZE)
	for !w.exit {
		n := w.DataQueues.Gets(queue.HashKey(queueID), rawItems)
		for i := 0; i < n; i++ {
			item := rawItems[i]
			if ii, ok := item.(InfluxdbItem); ok {
				w.writeCache(queueID, ii)
			} else if item == nil { // flush ticker
				if time.Now().Unix()-w.QueueWriterInfosPrimary[queueID].writeTime > w.FlushTimeout {
					w.flushWriteCache(queueID)
				}
			} else {
				log.Warning("get influxdb writer queue data type wrong")
			}
		}
	}
}

func (w *InfluxdbWriter) writeCache(queueID int, item InfluxdbItem) bool {
	pointCache := w.QueueWriterInfosPrimary[queueID].pointCache

	db := item.GetDBName()
	if _, ok := pointCache[db]; !ok {
		pointCache[db] = newPointCache(db, w.RP.name)
	}
	buffer := pointCache[db].buffer
	offset := pointCache[db].offset
	size := item.MarshalToBytes(buffer[offset:])
	point, err := models.NewPointFromBytes(buffer[offset : offset+size])
	if err != nil {
		log.Errorf("new model point failed buffer size=%d, err:%s", size, err)
		return false
	}
	pointCache[db].bp.AddPoint(client.NewPointFrom(point))
	pointCache[db].offset += size

	item.Release()

	if pointCache[db].offset > w.BatchSize {
		w.writePrimary(queueID, pointCache[db])
		pointCache[db] = newPointCache(db, w.RP.name)
	}
	return true
}

func (w *InfluxdbWriter) flushWriteCache(queueID int) {
	pointCache := w.QueueWriterInfosPrimary[queueID].pointCache
	for db, pc := range pointCache {
		if len(pc.bp.Points()) <= 0 {
			continue
		}
		log.Debugf("flush %d points to %s", len(pc.bp.Points()), db)
		w.writePrimary(queueID, pc)
		pointCache[db] = newPointCache(db, w.RP.name)
	}
}

func (w *InfluxdbWriter) createDB(httpClient client.Client, db string) bool {
	log.Infof("database %s no exists, create database now.", db)
	res, err := httpClient.Query(client.NewQuery(
		fmt.Sprintf("CREATE DATABASE %s", db), "", ""))
	if err := checkResponse(res, err); err != nil {
		log.Errorf("Create database %s failed, error info: %s", db, err)
		return false
	}

	if w.RP.name != "" {
		if retentionPolicyExists(httpClient, db, w.RP.name) {
			return alterRetentionPolicy(httpClient, db, &w.RP)
		} else {
			return createRetentionPolicy(httpClient, db, &w.RP)
		}
	}

	return true
}

func (w *InfluxdbWriter) writeInfluxdb(writerInfo *WriterInfo, dbCreateCtl *DBCreateCtl, pc *PointCache) bool {
	bp := pc.bp
	writerInfo.writeTime = time.Now().Unix()
	db := bp.Database()

	writeFailedCount := &writerInfo.counter.WriteFailedCount
	writeSuccCount := &writerInfo.counter.WriteSuccessCount

	dbCreateCtl.RLock()
	_, ok := dbCreateCtl.ExistDBs[db]
	dbCreateCtl.RUnlock()

	if !ok {
		if !w.createDB(writerInfo.httpClient, db) {
			*writeFailedCount += int64(len(bp.Points()))
			return false
		}
		dbCreateCtl.Lock()
		dbCreateCtl.ExistDBs[db] = true
		dbCreateCtl.Unlock()
	}

	if err := writerInfo.httpClient.Write(bp); err != nil {
		log.Errorf("httpclient write db(%s) batch points(%d) failed: %s", db, len(bp.Points()), err)
		*writeFailedCount += int64(len(bp.Points()))
		return false
	}
	*writeSuccCount += int64(len(bp.Points()))
	return true
}

func (w *InfluxdbWriter) writePrimary(queueID int, pc *PointCache) bool {
	writerInfo := w.QueueWriterInfosPrimary[queueID]

	if !w.writeInfluxdb(writerInfo, &w.DBCreateCtlPrimary, pc) {
		w.writeConfidence(pc, PRIMARY_FAILED)
		releasePointCache(pc)
		return false
	}

	if w.ReplicaEnabled {
		w.ReplicaQueues.Put(queue.HashKey(queueID), pc)
		return true
	}
	releasePointCache(pc)
	return true
}

func (w *InfluxdbWriter) writeReplica(queueID int, pc *PointCache) bool {
	writerInfo := w.QueueWriterInfosReplica[queueID]
	if !writerInfo.isConnected {
		w.writeConfidence(pc, REPLICA_DISCONNECT)
		releasePointCache(pc)
		return false
	}

	if !w.writeInfluxdb(writerInfo, &w.DBCreateCtlReplica, pc) {
		w.writeConfidence(pc, SYNC_FAILED_1)
		releasePointCache(pc)
		return false
	}

	releasePointCache(pc)
	return true
}

func (w *InfluxdbWriter) queueProcessReplica(queueID int) {
	writerInfo := w.QueueWriterInfosReplica[queueID]
	stats.RegisterCountable(w.Name+"_replica", writerInfo, stats.OptionStatTags{"thread": strconv.Itoa(queueID)})
	defer writerInfo.Close()
	defer w.wg.Done()
	w.wg.Add(1)

	for !w.exit {
		item := w.ReplicaQueues.Get(queue.HashKey(queueID))
		if item == nil { // flush ticker
			if _, _, err := writerInfo.httpClient.Ping(0); err != nil {
				writerInfo.isConnected = false
			} else {
				writerInfo.isConnected = true
			}
			continue
		} else if pc, ok := item.(*PointCache); ok {
			w.writeReplica(queueID, pc)
		} else {
			log.Warning("get influxdb replica writer queue data type wrong (%T)", item)
		}
	}
}

func (w *InfluxdbWriter) writeConfidence(pc *PointCache, status RepairStatus) {
	bp := pc.bp
	confidences := make(map[Confidence]uint8)
	for _, point := range bp.Points() {
		measurement := point.Name()
		// measurement 是以x开头跟16个字符的字符串(64位整数的16进制字符串)
		if len(measurement) == 17 && measurement[0] != 'x' {
			log.Warningf("db(%s) point(%s) get measurement name failed: %s", bp.Database(), point, measurement)
			continue
		}
		confidences[Confidence{
			db:          bp.Database(),
			measurement: measurement,
			timestamp:   point.Time().UnixNano(),
			status:      status,
		}] = 0
	}

	confidenceBP, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        CONFIDENCE_DB,
		Precision:       INFLUXDB_PRECISION_S,
		RetentionPolicy: w.RP.name,
	})

	tags := make(map[string]string)
	fields := make(map[string]interface{})
	for confidence, _ := range confidences {
		tags[TAG_DB] = confidence.db
		tags[TAG_MEASUREMENT] = confidence.measurement
		tags[TAG_ID] = w.ShardID
		fields[FIELD_STATUS] = int64(confidence.status)

		measurement := CONFIDENCE_MEASUREMENT
		if !isStatusNeedRepair(confidence.status) {
			measurement = CONFIDENCE_MEASUREMENT_SYNCED
		}

		if pt, err := client.NewPoint(measurement, tags, fields, time.Unix(0, confidence.timestamp)); err == nil {
			confidenceBP.AddPoint(pt)
		} else {
			log.Warning("new NewPoint failed:", err)
		}
	}

	if len(confidenceBP.Points()) > 0 {
		if err := w.PrimaryClient.Write(confidenceBP); err != nil {
			log.Errorf("httpclient  db(%s) write batch point failed: %s", CONFIDENCE_DB, err)
		}
	}
}

func (w *InfluxdbWriter) Close() {
	w.exit = true
	w.wg.Wait()

	w.DBCreateCtlReplica.HttpClient.Close()
	w.DBCreateCtlPrimary.HttpClient.Close()

	log.Info("Stopped influx writer")
}
