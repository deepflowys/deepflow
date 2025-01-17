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

package ckwriter

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	server_common "github.com/deepflowio/deepflow/server/common"
	"github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/ingester/config"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/grpc"
	"github.com/deepflowio/deepflow/server/libs/nativetag"
	"github.com/deepflowio/deepflow/server/libs/queue"
	"github.com/deepflowio/deepflow/server/libs/stats"
	"github.com/deepflowio/deepflow/server/libs/utils"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("ckwriter")

const (
	FLUSH_TIMEOUT  = 10 * time.Second
	SQL_LOG_LENGTH = 256
	RETRY_COUNT    = 2
)

var ckwriterManager = &CKWriterManager{}

type CKWriterManager struct {
	ckwriters []*CKWriter
	sync.Mutex
}

func RegisterToCkwriterManager(w *CKWriter) {
	ckwriterManager.Lock()
	if len(ckwriterManager.ckwriters) == 0 {
		server_common.SetOrgHandler(ckwriterManager)
		config.AddClickHouseEndpointsOnChange(ckwriterManager)
	}
	ckwriterManager.ckwriters = append(ckwriterManager.ckwriters, w)
	ckwriterManager.Unlock()
}

func (m *CKWriterManager) DropOrg(orgId uint16) error {
	log.Infof("call ckwriters drop org %d", orgId)
	ckwriterManager.Lock()
	for _, ckwriter := range m.ckwriters {
		log.Infof("ckwriter %s drop org %d", ckwriter.name, orgId)
		ckwriter.dropOrg(orgId)
	}
	ckwriterManager.Unlock()
	return nil
}

func (m *CKWriterManager) UpdateNativeTag(orgId uint16, table nativetag.NativeTagTable, nativeTag *nativetag.NativeTag) error {
	return nil
}

func (m *CKWriterManager) EndpointsChange(addrs []string) {
	ckwriterManager.Lock()
	for _, ckwriter := range m.ckwriters {
		log.Infof("ckwriter %s addrs change from %s to %s ", ckwriter.name, ckwriter.addrs, addrs)
		ckwriter.addrs = utils.CloneStringSlice(addrs)
		ckwriter.endpointsChange(addrs)
	}
	ckwriterManager.Unlock()
}

type CKWriter struct {
	addrs         []string
	user          string
	password      string
	timeZone      string
	table         *ckdb.Table
	queueCount    int
	queueSize     int           // 队列长度
	batchSize     int           // 累积多少行数据，一起写入
	flushDuration time.Duration // 超时写入
	counterName   string        // 写入成功失败的统计数据表名称，若写入失败，会根据该数据上报告警

	name          string // 数据库名-表名 用作 queue名字和counter名字
	dataQueues    queue.FixedMultiQueue
	putCounter    int
	ckdbwatcher   *config.Watcher
	queueContexts []*QueueContext

	wg   sync.WaitGroup
	exit bool
}

type QueueContext struct {
	endpointsChange bool
	orgCaches       []*Cache // write caches for all organizations
	addrs           []string
	user, password  string
	conns           []*ch.Client
	connCount       int
	counter         Counter
}

func (qc *QueueContext) EndpointsChange(addrs []string) {
	if !qc.endpointsChange || len(addrs) == 0 {
		return
	}
	for _, conn := range qc.conns {
		if conn != nil {
			conn.Close()
		}
	}
	qc.connCount = len(addrs)
	qc.conns = make([]*ch.Client, qc.connCount)
	for i, addr := range addrs {
		client, err := ch.Dial(
			context.Background(),
			ch.Options{
				Address:          addr,
				User:             qc.user,
				Password:         qc.password,
				HandshakeTimeout: time.Minute,
				DialTimeout:      5 * time.Second,
			},
		)

		if err != nil {
			log.Warningf("dial to %s failed, %s", addr, err)
		} else {
			qc.conns[i] = client
		}
	}
	qc.addrs = addrs
	qc.endpointsChange = false
	for _, cache := range qc.orgCaches {
		cache.tableCreated = false
	}
}

func (qc *QueueContext) Init(addrs []string, user, password, insertTable string) error {
	qc.addrs = addrs
	qc.connCount = len(addrs)
	qc.conns = make([]*ch.Client, qc.connCount)
	for i := 0; i < qc.connCount; i++ {
		client, err := ch.Dial(
			context.Background(),
			ch.Options{
				Address:          addrs[i],
				User:             user,
				Password:         password,
				HandshakeTimeout: time.Minute,
				DialTimeout:      5 * time.Second,
			},
		)
		if err != nil {
			log.Warningf("dial to %s failed, %s", addrs[i], err)
		}
		qc.conns[i] = client
	}
	orgCaches := make([]*Cache, ckdb.MAX_ORG_ID+1)
	for i := range orgCaches {
		orgCaches[i] = new(Cache)
		orgCaches[i].orgID = uint16(i)
		orgCaches[i].queueContext = qc
		orgCaches[i].prepare = fmt.Sprintf("INSERT INTO %s VALUES", insertTable)
	}
	qc.orgCaches = orgCaches
	qc.user, qc.password = user, password
	return nil
}

func (qc *QueueContext) initConn(connIndex int) error {
	if len(qc.addrs) <= connIndex {
		return fmt.Errorf("conn index (%d) is exceeded address range (%d)", connIndex, len(qc.addrs))
	}
	client, err := ch.Dial(
		context.Background(),
		ch.Options{
			Address:          qc.addrs[connIndex],
			User:             qc.user,
			Password:         qc.password,
			HandshakeTimeout: time.Minute,
			DialTimeout:      5 * time.Second,
		},
	)
	if err != nil {
		if qc.counter.WriteFailedCount == 0 {
			log.Warningf("dial to %s failed, %s", qc.addrs[connIndex], err)
		}
	} else {
		qc.conns[connIndex] = client
	}
	return err
}

type CKItem interface {
	OrgID() uint16
	Release()
	NativeTagVersion() uint32
	NewColumnBlock() ckdb.CKColumnBlock
	AppendToColumnBlock(ckdb.CKColumnBlock)
}

func ExecSQL(conn *ch.Client, query string) error {
	if len(query) > SQL_LOG_LENGTH {
		log.Infof("Exec SQL: %s ...", query[:SQL_LOG_LENGTH])
	} else {
		log.Info("Exec SQL: ", query)
	}
	err := conn.Do(context.Background(), ch.Query{Body: query})
	retryTimes := RETRY_COUNT
	for err != nil && retryTimes > 0 {
		log.Warningf("Exec SQL (%s) failed: %s, will retry", query, err)
		time.Sleep(time.Second)
		err = conn.Do(context.Background(), ch.Query{Body: query})
		if err == nil {
			log.Infof("Retry exec SQL (%s) success", query)
			return nil
		}
		retryTimes--
	}
	return err
}

func initTable(conn *ch.Client, timeZone string, t *ckdb.Table, orgID uint16) error {
	if err := ExecSQL(conn, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", t.OrgDatabase(orgID))); err != nil {
		return err
	}

	if err := ExecSQL(conn, t.MakeOrgLocalTableCreateSQL(orgID)); err != nil {
		return err
	}
	if err := ExecSQL(conn, t.MakeOrgGlobalTableCreateSQL(orgID)); err != nil {
		return err
	}

	if t.Aggr1H1D {
		if err := ExecSQL(conn, t.MakeAggrTableCreateSQL1H(orgID)); err != nil {
			log.Warningf("create 1h agg table failed: %s", err)
		}
		if err := ExecSQL(conn, t.MakeAggrMVTableCreateSQL1H(orgID)); err != nil {
			log.Warningf("create 1h mv table failed: %s", err)
		}
		if err := ExecSQL(conn, t.MakeAggrLocalTableCreateSQL1H(orgID)); err != nil {
			log.Warningf("create 1h local table failed: %s", err)
		}
		if err := ExecSQL(conn, t.MakeAggrGlobalTableCreateSQL1H(orgID)); err != nil {
			log.Warningf("create 1h global table failed: %s", err)
		}

		if err := ExecSQL(conn, t.MakeAggrTableCreateSQL1D(orgID)); err != nil {
			log.Warningf("create 1d agg table failed: %s", err)
		}
		if err := ExecSQL(conn, t.MakeAggrMVTableCreateSQL1D(orgID)); err != nil {
			log.Warningf("create 1d mv table failed: %s", err)
		}
		if err := ExecSQL(conn, t.MakeAggrLocalTableCreateSQL1D(orgID)); err != nil {
			log.Warningf("create 1d local table failed: %s", err)
		}
		if err := ExecSQL(conn, t.MakeAggrGlobalTableCreateSQL1D(orgID)); err != nil {
			log.Warningf("create 1d global table failed: %s", err)
		}
	}

	// ByConity not support modify timezone
	if t.DBType == ckdb.CKDBTypeByconity {
		return nil
	}

	for _, c := range t.Columns {
		for _, table := range []string{t.GlobalName, t.LocalName} {
			modTimeZoneSql := c.MakeModifyTimeZoneSQL(t.OrgDatabase(orgID), table, timeZone)
			if modTimeZoneSql == "" {
				break
			}

			if err := ExecSQL(conn, modTimeZoneSql); err != nil {
				log.Warningf("modify time zone failed, error: %s", err)
			}
		}
	}

	return nil
}

func InitTable(addr, user, password, timeZone string, t *ckdb.Table, orgID uint16) error {
	conn, err := ch.Dial(
		context.Background(),
		ch.Options{
			Address:          addr,
			User:             user,
			Password:         password,
			HandshakeTimeout: time.Minute,
			DialTimeout:      5 * time.Second,
		},
	)
	if err != nil {
		return err
	}

	if err := initTable(conn, timeZone, t, orgID); err != nil {
		conn.Close()
		return err
	}
	conn.Close()

	return nil
}

func (w *CKWriter) InitTable(queueID int, orgID uint16) error {
	for _, conn := range w.queueContexts[queueID].conns {
		if err := initTable(conn, w.timeZone, w.table, orgID); err != nil {
			return err
		}
	}

	// in standalone mode, ckdbWatcher will be nil
	if w.ckdbwatcher == nil {
		return nil
	}

	endpoints, err := w.ckdbwatcher.GetClickhouseEndpointsWithoutMyself()
	if err != nil {
		log.Warningf("get clickhouse endpoints without myself failed: %s", err)
		return err
	}

	for _, endpoint := range endpoints {
		err := InitTable(fmt.Sprintf("%s:%d", endpoint.Host, endpoint.Port), w.user, w.password, w.timeZone, w.table, orgID)
		if err != nil {
			log.Warningf("node %s:%d init table failed. err: %s", endpoint.Host, endpoint.Port, err)
		} else {
			log.Infof("node %s:%d init table %s success", endpoint.Host, endpoint.Port, w.table.LocalName)
		}
	}

	return nil
}

func NewCKWriter(addrs []string, user, password, counterName, timeZone string, table *ckdb.Table, queueCount, queueSize, batchSize, flushTimeout int, ckdbwatcher *config.Watcher) (*CKWriter, error) {
	log.Infof("New CK writer: Addrs=%v, user=%s, database=%s, table=%s, queueCount=%d, queueSize=%d, batchSize=%d, flushTimeout=%ds, counterName=%s, timeZone=%s",
		addrs, user, table.Database, table.LocalName, queueCount, queueSize, batchSize, flushTimeout, counterName, timeZone)

	if len(addrs) == 0 {
		return nil, fmt.Errorf("addrs is empty")
	}

	var err error

	// clickhouse init default organization database/tables
	for _, addr := range addrs {
		orgIds := grpc.QueryAllOrgIDs()
		if len(orgIds) > 1 {
			log.Infof("database %s get orgIDs: %v", table.Database, orgIds)
		}
		for _, orgId := range orgIds {
			if err = InitTable(addr, user, password, timeZone, table, orgId); err != nil {
				return nil, err
			}
		}
	}

	queueContexts := make([]*QueueContext, queueCount)
	for i := range queueContexts {
		queueContexts[i] = &QueueContext{}
		insertTable := fmt.Sprintf("%s.`%s`", table.OrgDatabase(uint16(i)), table.LocalName)
		if err := queueContexts[i].Init(addrs, user, password, insertTable); err != nil {
			return nil, err
		}
	}
	name := fmt.Sprintf("%s-%s-%s", table.Database, table.LocalName, counterName)
	dataQueues := queue.NewOverwriteQueues(
		name, queue.HashKey(queueCount), queueSize,
		queue.OptionFlushIndicator(time.Second),
		queue.OptionRelease(func(p interface{}) { p.(CKItem).Release() }),
		common.QUEUE_STATS_MODULE_INGESTER)

	w := &CKWriter{
		addrs:         utils.CloneStringSlice(addrs),
		user:          user,
		password:      password,
		timeZone:      timeZone,
		table:         table,
		queueCount:    queueCount,
		queueSize:     queueSize,
		batchSize:     batchSize,
		flushDuration: time.Duration(flushTimeout) * time.Second,
		counterName:   counterName,
		queueContexts: queueContexts,

		name:        name,
		dataQueues:  dataQueues,
		ckdbwatcher: ckdbwatcher,
	}
	RegisterToCkwriterManager(w)
	return w, nil
}

func (w *CKWriter) dropOrg(orgId uint16) {
	for i, qc := range w.queueContexts {
		log.Debugf("ckwriter %s queue %d drop org %d", w.name, i, orgId)
		qc.orgCaches[orgId].dropTime = uint32(time.Now().Unix())
		qc.orgCaches[orgId].tableCreated = false
	}
}

func (w *CKWriter) endpointsChange(addrs []string) {
	for i := 0; i < len(w.queueContexts); i++ {
		log.Debugf("ckwriter %s queue %d endpoints will change to %s", w.name, i, addrs)
		w.queueContexts[i].endpointsChange = true
	}
}

func (w *CKWriter) Run() {
	for i := 0; i < w.queueCount; i++ {
		go w.queueProcess(i)
	}
}

type Counter struct {
	WriteSuccessCount int64 `statsd:"write-success-count"`
	WriteFailedCount  int64 `statsd:"write-failed-count"`
	RetryCount        int64 `statsd:"retry-count"`
	RetryFailedCount  int64 `statsd:"retry-failed-count"`
	OrgInvalidCount   int64 `statsd:"org-invalid-count"`
	utils.Closable
}

func (i *Counter) GetCounter() interface{} {
	var counter Counter
	counter, *i = *i, Counter{}

	return &counter
}

func (w *CKWriter) Put(items ...interface{}) {
	if w.queueSize == 0 {
		for _, item := range items {
			if ck, ok := item.(CKItem); ok {
				ck.Release()
			}
		}
		return
	}
	w.putCounter++
	w.dataQueues.Put(queue.HashKey(w.putCounter%w.queueCount), items...)
}

type Cache struct {
	queueContext  *QueueContext
	orgID         uint16
	prepare       string
	columnBlock   ckdb.CKColumnBlock
	ItemVersion   uint32
	protoInput    proto.Input
	size          int
	writeCounter  int
	lastWriteTime time.Time
	tableCreated  bool
	dropTime      uint32
}

func (c *Cache) Release() {
	c.columnBlock = nil
	c.protoInput = nil
}

func (c *Cache) OrgIdExists() bool {
	updateTime, exists := grpc.QueryOrgIDExist(c.orgID)
	if updateTime == 0 || c.dropTime == 0 {
		return exists
	}
	// dropped but not update org id list, return false
	if updateTime <= c.dropTime {
		return false
	}
	c.dropTime = 0
	return exists
}

func (w *CKWriter) queueProcess(queueID int) {
	qc := w.queueContexts[queueID]
	common.RegisterCountableForIngester("ckwriter", &(qc.counter), stats.OptionStatTags{"thread": strconv.Itoa(queueID), "table": w.name, "name": w.counterName})

	w.wg.Add(1)
	defer w.wg.Done()

	rawItems := make([]interface{}, 1024)
	orgCaches := qc.orgCaches

	for !w.exit {
		n := w.dataQueues.Gets(queue.HashKey(queueID), rawItems)
		for i := 0; i < n; i++ {
			item := rawItems[i]
			if ckItem, ok := item.(CKItem); ok {
				orgID := ckItem.OrgID()
				if orgID > ckdb.MAX_ORG_ID {
					if qc.counter.OrgInvalidCount == 0 {
						log.Warningf("writer queue (%s) item wrong orgID %d", w.name, orgID)
					}
					qc.counter.OrgInvalidCount++
					continue
				}
				cache := orgCaches[orgID]
				cache.Add(ckItem)
				if cache.size >= w.batchSize {
					w.Write(queueID, cache)
				}
			} else if IsNil(item) { // flush ticker
				now := time.Now()
				for _, cache := range orgCaches {
					if cache.size > 0 && now.Sub(cache.lastWriteTime) > w.flushDuration {
						w.Write(queueID, cache)
					}
				}
			} else {
				log.Warningf("get writer queue data type wrong %T", item)
			}
		}
	}
}

func (c *Cache) Add(item CKItem) error {
	if IsNil(c.columnBlock) ||
		(c.size == 0 && c.ItemVersion != item.NativeTagVersion()) {
		c.columnBlock = item.NewColumnBlock()
		log.Info("orgId %d (%s) update item version from %d to %d", c.orgID, c.prepare, c.ItemVersion, item.NativeTagVersion())
		c.ItemVersion = item.NativeTagVersion()
	}
	item.AppendToColumnBlock(c.columnBlock)
	item.Release()
	c.size++
	return nil
}

func (c *Cache) Write() error {
	if c.size == 0 {
		return nil
	}

	connIndex := c.writeCounter % c.queueContext.connCount
	conn := c.queueContext.conns[connIndex]
	if conn == nil || conn.IsClosed() {
		if err := c.queueContext.initConn(connIndex); err != nil {
			c.writeCounter++
			c.lastWriteTime = time.Now()
			c.size = 0
			c.columnBlock.Reset()
			return err
		}
		conn = c.queueContext.conns[connIndex]
	}
	c.protoInput = c.protoInput[:0]
	input := c.columnBlock.ToInput(c.protoInput)
	c.protoInput = input

	err := conn.Do(context.Background(), ch.Query{
		Body:  c.prepare,
		Input: input,
	})
	c.writeCounter++
	c.lastWriteTime = time.Now()
	c.size = 0
	c.columnBlock.Reset()
	if err != nil {
		return fmt.Errorf("batch item write block failed: %s", err)
	}
	return nil
}

func (w *CKWriter) ResetConnection(queueID, connID int) error {
	var err error
	// FIXME: do reset actually
	if !IsNil(w.queueContexts[queueID].conns[connID]) {
		return nil
	}
	w.queueContexts[queueID].conns[connID], err = ch.Dial(
		context.Background(),
		ch.Options{
			Address:          w.addrs[connID],
			User:             w.user,
			Password:         w.password,
			HandshakeTimeout: time.Minute,
			DialTimeout:      5 * time.Second,
		},
	)
	return err
}

func (w *CKWriter) Write(queueID int, cache *Cache) {
	qc := w.queueContexts[queueID]
	qc.EndpointsChange(w.addrs)
	itemsLen := cache.size
	// Prevent frequent log writing
	logEnabled := qc.counter.WriteFailedCount == 0
	if !cache.OrgIdExists() {
		if logEnabled {
			log.Warningf("table (%s.%s) orgId is not exist, drop (%d) items", w.table.OrgDatabase(cache.orgID), w.table.LocalName, itemsLen)
		}
		qc.counter.OrgInvalidCount += int64(itemsLen)
		cache.Release()
		return
	}
	if !cache.tableCreated {
		err := w.InitTable(queueID, cache.orgID)
		if err != nil {
			if logEnabled {
				log.Warningf("create table (%s.%s) failed, drop (%d) items: %s", w.table.OrgDatabase(cache.orgID), w.table.LocalName, itemsLen, err)
			}
			qc.counter.WriteFailedCount += int64(itemsLen)
			cache.Release()
			return
		}
		cache.tableCreated = true
	}
	if err := cache.Write(); err != nil {
		if logEnabled {
			log.Warningf("write table (%s.%s) failed, drop (%d) items: %s", w.table.OrgDatabase(cache.orgID), w.table.LocalName, itemsLen, err)
		}
		qc.counter.WriteFailedCount += int64(itemsLen)
	} else {
		qc.counter.WriteSuccessCount += int64(itemsLen)
	}
}

func IsNil(i interface{}) bool {
	if i == nil {
		return true
	}
	vi := reflect.ValueOf(i)
	if vi.Kind() == reflect.Ptr {
		return vi.IsNil()
	}
	return false
}

func (w *CKWriter) Close() {
	w.exit = true
	w.wg.Wait()
	for i, qc := range w.queueContexts {
		for _, c := range qc.conns {
			if !IsNil(c) {
				c.Close()
				qc.conns[i] = nil
			}
		}
		qc.counter.Close()
	}

	for _, q := range w.dataQueues {
		q.Close()
	}

	log.Infof("ckwriter %s closed", w.name)
}
