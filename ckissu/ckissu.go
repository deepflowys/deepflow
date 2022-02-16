package ckissu

import (
	"fmt"
	"regexp"
	"strings"

	logging "github.com/op/go-logging"

	"database/sql"

	"gitlab.yunshan.net/yunshan/droplet-libs/ckdb"
	"gitlab.yunshan.net/yunshan/droplet-libs/zerodoc"
	"gitlab.yunshan.net/yunshan/droplet/common"
	"gitlab.yunshan.net/yunshan/droplet/datasource"
)

var log = logging.MustGetLogger("issu")

type Issu struct {
	columnRenames                          []*ColumnRename
	columnAdds                             []*ColumnAdd
	primaryConnection, SecondaryConnection *sql.DB
	primaryAddr, secondaryAddr             string
	username, password                     string
	exit                                   bool
}

type ColumnRename struct {
	Db            string
	Table         string
	OldColumnName string
	NewColumnName string
}

type ColumnAdd struct {
	Db           string
	Table        string
	ColumnName   string
	ColumnType   ckdb.ColumnType
	DefaultValue string
}

type ColumnAdds struct {
	Dbs          []string
	Tables       []string
	ColumnNames  []string
	ColumnType   ckdb.ColumnType
	DefaultValue string
}

var ColumnRename572 = []*ColumnRename{
	&ColumnRename{
		Db:            "flow_log",
		Table:         "l4_flow_log",
		OldColumnName: "retans_tx",
		NewColumnName: "retrans_tx",
	},
	&ColumnRename{
		Db:            "flow_log",
		Table:         "l4_flow_log_local",
		OldColumnName: "retans_tx",
		NewColumnName: "retrans_tx",
	},
}

var ColumnAdd573 = []*ColumnAdds{
	&ColumnAdds{
		Dbs:         []string{"vtap_flow", "vtap_flow_port"},
		Tables:      []string{"1m", "1m_local", "1s", "1s_local"},
		ColumnNames: []string{"service_id"},
		ColumnType:  ckdb.UInt32,
	},
	&ColumnAdds{
		Dbs:         []string{"vtap_flow_edge", "vtap_flow_edge_port"},
		Tables:      []string{"1m", "1m_local", "1s", "1s_local"},
		ColumnNames: []string{"service_id_0", "service_id_1"},
		ColumnType:  ckdb.UInt32,
	},
	&ColumnAdds{
		Dbs:         []string{"flow_log"},
		Tables:      []string{"l4_flow_log", "l4_flow_log_local", "l7_http_log", "l7_http_log_local", "l7_dns_log", "l7_dns_log_local"},
		ColumnNames: []string{"service_id_0", "service_id_1"},
		ColumnType:  ckdb.UInt32,
	},
}

var ColumnAdd600 = []*ColumnAdds{
	&ColumnAdds{
		Dbs:         []string{"vtap_flow_port", "vtap_flow_edge_port"},
		Tables:      []string{"1m", "1m_local", "1s", "1s_local"},
		ColumnNames: []string{"l7_client_error", "l7_server_error", "l7_timeout", "l7_error", "rrt_max"},
		ColumnType:  ckdb.UInt32,
	},
	&ColumnAdds{
		Dbs:         []string{"vtap_flow_port", "vtap_flow_edge_port"},
		Tables:      []string{"1m", "1m_local", "1s", "1s_local"},
		ColumnNames: []string{"rrt_sum"},
		ColumnType:  ckdb.Float64,
	},
	&ColumnAdds{
		Dbs:         []string{"vtap_flow_port", "vtap_flow_edge_port"},
		Tables:      []string{"1m", "1m_local", "1s", "1s_local"},
		ColumnNames: []string{"rrt_count", "l7_request", "l7_response", "tcp_transfer_fail", "tcp_rst_fail"},
		ColumnType:  ckdb.UInt64,
	},
	&ColumnAdds{
		Dbs:         []string{"vtap_flow_edge_port", "vtap_flow_port", "vtap_app_port", "vtap_app_edge_port"},
		Tables:      []string{"1m", "1m_local", "1s", "1s_local"},
		ColumnNames: []string{"tap_port_type", "tunnel_type"},
		ColumnType:  ckdb.UInt8,
	},
	&ColumnAdds{
		Dbs:          []string{"flow_log"},
		Tables:       []string{"l4_flow_log", "l4_flow_log_local", "l7_http_log", "l7_http_log_local", "l7_dns_log", "l7_dns_log_local"},
		ColumnNames:  []string{"tap_side"},
		ColumnType:   ckdb.LowCardinalityString,
		DefaultValue: "'rest'",
	},
	&ColumnAdds{
		Dbs: []string{"flow_log"},
		Tables: []string{"l4_flow_log", "l4_flow_log_local", "l7_http_log", "l7_http_log_local", "l7_dns_log", "l7_dns_log_local",
			"l7_mq_log", "l7_mq_log_local", "l7_sql_log", "l7_sql_log_local", "l7_nosql_log", "l7_nosql_log_local", "l7_rpc_log", "l7_rpc_log_local"},
		ColumnNames: []string{"tap_port_type", "tunnel_type"},
		ColumnType:  ckdb.UInt8,
	},
	&ColumnAdds{
		Dbs:         []string{"flow_log"},
		Tables:      []string{"l4_flow_log", "l4_flow_log_local"},
		ColumnNames: []string{"syn_seq", "syn_ack_seq", "l7_error", "tunnel_tx_mac_0", "tunnel_tx_mac_1", "tunnel_rx_mac_0", "tunnel_rx_mac_1", "last_keepalive_seq_0", "last_keepalive_seq_1"},
		ColumnType:  ckdb.UInt32,
	},
	&ColumnAdds{
		Dbs:         []string{"flow_log"},
		Tables:      []string{"l4_flow_log", "l4_flow_log_local"},
		ColumnNames: []string{"is_new_flow"},
		ColumnType:  ckdb.UInt8,
	},
	&ColumnAdds{
		Dbs:         []string{"flow_log"},
		Tables:      []string{"l7_http_log", "l7_http_log_local", "l7_dns_log", "l7_dns_log_local"},
		ColumnNames: []string{"req_tcp_seq", "resp_tcp_seq"},
		ColumnType:  ckdb.UInt32,
	},
	&ColumnAdds{
		Dbs:          []string{"flow_log"},
		Tables:       []string{"l7_dns_log", "l7_dns_log_local"},
		ColumnNames:  []string{"protocol"},
		ColumnType:   ckdb.UInt8,
		DefaultValue: "17",
	},
	&ColumnAdds{
		Dbs:         []string{"flow_log"},
		Tables:      []string{"l7_http_log", "l7_http_log_local", "l7_dns_log", "l7_dns_log_local"},
		ColumnNames: []string{"status_code"},
		ColumnType:  ckdb.UInt8,
	},
	&ColumnAdds{
		Dbs:         []string{"flow_log"},
		Tables:      []string{"l7_http_log", "l7_http_log_local", "l7_dns_log", "l7_dns_log_local"},
		ColumnNames: []string{"exception_desc"},
		ColumnType:  ckdb.LowCardinalityString,
	},
	&ColumnAdds{
		Dbs:         []string{"flow_log"},
		Tables:      []string{"l7_http_log", "l7_http_log_local"},
		ColumnNames: []string{"response_length"},
		ColumnType:  ckdb.Int64Nullable,
	},
	&ColumnAdds{
		Dbs:         []string{"flow_log"},
		Tables:      []string{"l7_http_log", "l7_http_log_local"},
		ColumnNames: []string{"span_id"},
		ColumnType:  ckdb.String,
	},
}

var ColumnRename600 = []*ColumnRename{
	&ColumnRename{
		Db:            "flow_log",
		Table:         "l7_http_log",
		OldColumnName: "status_code",
		NewColumnName: "answer_code",
	},
	&ColumnRename{
		Db:            "flow_log",
		Table:         "l7_http_log_local",
		OldColumnName: "status_code",
		NewColumnName: "answer_code",
	},
	&ColumnRename{
		Db:            "flow_log",
		Table:         "l7_http_log",
		OldColumnName: "content_length",
		NewColumnName: "request_length",
	},
	&ColumnRename{
		Db:            "flow_log",
		Table:         "l7_http_log_local",
		OldColumnName: "content_length",
		NewColumnName: "request_length",
	},
}

func getTables(connect *sql.DB, db string) ([]string, error) {
	sql := fmt.Sprintf("SHOW TABLES IN %s", db)
	rows, err := connect.Query(sql)
	if err != nil {
		return nil, err
	}
	tables := []string{}
	var table string
	for rows.Next() {
		err := rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil

}

type DatasourceInfo struct {
	db         string
	name       string
	baseTable  string
	summable   string
	unsummable string
	interval   ckdb.TimeFuncType
}

func getDatasourceInfo(connect *sql.DB, db, name string) (*DatasourceInfo, error) {
	sql := fmt.Sprintf("SHOW CREATE TABLE %s.%s_mv", db, name)
	rows, err := connect.Query(sql)
	if err != nil {
		return nil, err
	}
	var createSql string
	for rows.Next() {
		err := rows.Scan(&createSql)
		if err != nil {
			return nil, err
		}
	}
	log.Infof("getDatasourceInfo sql: %s createSql: %s ", sql, createSql)
	var summable, unsummable, interval, baseTable string
	var matchs [4]string
	// 匹配 `packet_tx__agg` AggregateFunction(sum, UInt64), 中的 'sum' 为可累加聚合的方法
	summableReg := regexp.MustCompile("`packet_tx__agg` AggregateFunction.([a-z]+)")
	// 匹配 `rtt_sum__agg` AggregateFunction(avg, Float64), 中的 'avg' 为非可累加聚合的方法
	unsummableReg := regexp.MustCompile("`rtt_sum__agg` AggregateFunction.([a-zA-Z]+)")
	// 匹配 toStartOfHour(time) AS time, 中的 'Hour' 为聚合时长
	intervalReg := regexp.MustCompile("toStartOf([a-zA-Z]+)")
	// 匹配 FROM vtap_flow.`1m_local` 中的'1m' 为原始数据源
	baseTableReg := regexp.MustCompile("FROM .*.`(.*)_local`")

	for i, reg := range []*regexp.Regexp{summableReg, unsummableReg, intervalReg, baseTableReg} {
		submatchs := reg.FindStringSubmatch(createSql)
		if len(submatchs) > 1 {
			matchs[i] = submatchs[1]
		} else {
			return nil, fmt.Errorf("parase %d failed", i)
		}
	}
	summable, unsummable, interval, baseTable = matchs[0], matchs[1], matchs[2], matchs[3]
	if unsummable == "argMax" {
		unsummable = "max"
	} else if unsummable == "argMin" {
		unsummable = "min"
	}
	log.Info("get summable, unsummable, interval, baseTable:", summable, unsummable, interval, baseTable)

	intervalTime := ckdb.TimeFuncHour
	if interval == "Day" {
		intervalTime = ckdb.TimeFuncDay
	} else if interval == "Hour" {
		intervalTime = ckdb.TimeFuncHour
	} else {
		return nil, fmt.Errorf("invalid interval %s", interval)
	}

	return &DatasourceInfo{
		db:         db,
		baseTable:  baseTable,
		name:       name,
		summable:   summable,
		unsummable: unsummable,
		interval:   intervalTime,
	}, nil
}

// 找出自定义数据源和参数
func getUserDefinedDatasourceInfos(connect *sql.DB, db string) ([]*DatasourceInfo, error) {
	tables, err := getTables(connect, db)
	if err != nil {
		return nil, err
	}

	aggTables := []string{}
	aggSuffix := "_agg"
	for _, t := range tables {
		if strings.HasSuffix(t, aggSuffix) {
			aggTables = append(aggTables, t[:len(t)-len(aggSuffix)])
		}
	}

	dSInfos := []*DatasourceInfo{}
	for _, name := range aggTables {
		ds, err := getDatasourceInfo(connect, db, name)
		if err != nil {
			return nil, err
		}
		dSInfos = append(dSInfos, ds)
	}

	return dSInfos, nil
}

func (i *Issu) addColumnDatasource(connect *sql.DB, d *DatasourceInfo) ([]*ColumnAdd, error) {
	// mod table agg, global
	dones := []*ColumnAdd{}

	columnAdds := []*ColumnAdd{}
	var columnAddss = []*ColumnAdds{
		&ColumnAdds{
			Dbs:         []string{d.db},
			Tables:      []string{d.name, d.name + "_agg"},
			ColumnNames: []string{"rrt_count", "l7_request", "l7_response", "tcp_transfer_fail", "tcp_rst_fail"},
			ColumnType:  ckdb.UInt64,
		},
		&ColumnAdds{
			Dbs:         []string{d.db},
			Tables:      []string{d.name, d.name + "_agg"},
			ColumnNames: []string{"rrt_sum"},
			ColumnType:  ckdb.Float64,
		},
		&ColumnAdds{
			Dbs:         []string{d.db},
			Tables:      []string{d.name, d.name + "_agg"},
			ColumnNames: []string{"l7_client_error", "l7_server_error", "l7_timeout", "l7_error", "rrt_max"},
			ColumnType:  ckdb.UInt32,
		},
	}
	if d.db == "vtap_flow_edge_port" {
		columnAddss = append(columnAddss,
			&ColumnAdds{
				Dbs:         []string{d.db},
				Tables:      []string{d.name, d.name + "_agg"},
				ColumnNames: []string{"tap_port_type"},
				ColumnType:  ckdb.UInt8,
			})
	}
	for _, adds := range columnAddss {
		columnAdds = append(columnAdds, getColumnAdds(adds)...)
	}

	for _, addColumn := range columnAdds {
		version, err := i.getTableVersion(connect, addColumn.Db, addColumn.Table)
		if err != nil {
			return dones, err
		}
		if version == common.CK_VERSION {
			continue
		}
		if err := i.addColumn(connect, addColumn); err != nil {
			return dones, err
		}
		dones = append(dones, addColumn)
	}

	if len(dones) == 0 {
		log.Infof("datasource db(%s) table(%s) already updated.", d.db, d.name)
		return nil, nil
	}

	// drop table mv
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s.`%s`", d.db, d.name+"_mv")
	log.Info(sql)
	_, err := connect.Exec(sql)
	if err != nil {
		return nil, err
	}

	rawTable := zerodoc.GetMetricsTables(ckdb.MergeTree, common.CK_VERSION)[zerodoc.MetricsDBNameToID(d.db)]
	// create table mv
	createMvSql := datasource.MakeMVTableCreateSQL(
		rawTable, d.baseTable, d.name,
		d.summable, d.unsummable, d.interval)
	log.Info(createMvSql)
	_, err = connect.Exec(createMvSql)
	if err != nil {
		return nil, err
	}

	// drop table local
	sql = fmt.Sprintf("DROP TABLE IF EXISTS %s.`%s`", d.db, d.name+"_local")
	log.Info(sql)
	_, err = connect.Exec(sql)
	if err != nil {
		return nil, err
	}

	// create table local
	createLocalSql := datasource.MakeCreateTableLocal(
		rawTable, d.baseTable, d.name,
		d.summable, d.unsummable)
	log.Info(createLocalSql)
	_, err = connect.Exec(createLocalSql)
	if err != nil {
		return nil, err

	}
	return dones, nil
}

func NewCKIssu(primaryAddr, secondaryAddr, username, password string) (*Issu, error) {
	i := &Issu{
		primaryAddr:   primaryAddr,
		secondaryAddr: secondaryAddr,
		username:      username,
		password:      password,
		// columnRenames: ColumnRename572,
	}

	columnAdds := []*ColumnAdd{}
	for _, adds := range ColumnAdd600 {
		columnAdds = append(columnAdds, getColumnAdds(adds)...)
	}
	i.columnAdds = columnAdds
	i.columnRenames = ColumnRename600

	var err error
	i.primaryConnection, err = common.NewCKConnection(primaryAddr, username, password)
	if err != nil {
		return nil, err
	}

	if secondaryAddr != "" {
		i.SecondaryConnection, err = common.NewCKConnection(secondaryAddr, username, password)
		if err != nil {
			return nil, err
		}
	}

	return i, nil
}

func (i *Issu) addColumn(connect *sql.DB, c *ColumnAdd) error {
	defaultValue := ""
	if len(c.DefaultValue) > 0 {
		defaultValue = fmt.Sprintf("default %s", c.DefaultValue)
	}
	sql := fmt.Sprintf("ALTER TABLE %s.`%s` ADD COLUMN %s %s %s",
		c.Db, c.Table, c.ColumnName, c.ColumnType, defaultValue)
	log.Info(sql)
	_, err := connect.Exec(sql)
	if err != nil {
		// 如果已经增加，需要跳过该错误
		if strings.Contains(err.Error(), "column with this name already exists") {
			log.Infof("db: %s, table: %s error: %s", c.Db, c.Table, err)
			return nil
		}
		log.Error(err)
		return err
	}
	return nil
}

func (i *Issu) renameColumn(connect *sql.DB, cr *ColumnRename) error {
	// ALTER TABLE flow_log.l4_flow_log  RENAME COLUMN retan_tx TO retran_tx
	sql := fmt.Sprintf("ALTER TABLE %s.%s RENAME COLUMN %s to %s",
		cr.Db, cr.Table, cr.OldColumnName, cr.NewColumnName)
	log.Info("rename column: ", sql)
	_, err := connect.Exec(sql)
	if err != nil {
		// 如果已经修改过，就会报错不存在column，需要跳过该错误
		// Code: 10. DB::Exception: Received from localhost:9000. DB::Exception: Wrong column name. Cannot find column `retan_tx` to rename.
		if strings.Contains(err.Error(), "Cannot find column") ||
			strings.Contains(err.Error(), "column with this name already exists") {
			log.Infof("db: %s, table: %s error: %s", cr.Db, cr.Table, err)
			return nil
		}
		log.Error(err)
		return err
	}
	return nil
}

func (i *Issu) getTableVersion(connect *sql.DB, db, table string) (string, error) {
	sql := fmt.Sprintf("SELECT comment FROM system.columns WHERE database='%s' AND table='%s' AND name='time'",
		db, table)
	rows, err := connect.Query(sql)
	if err != nil {
		return "", err
	}
	var version string
	for rows.Next() {
		err := rows.Scan(&version)
		if err != nil {
			return "", err
		}
	}
	return version, nil
}

func (i *Issu) setTableVersion(connect *sql.DB, db, table string) error {
	sql := fmt.Sprintf("ALTER TABLE %s.`%s` COMMENT COLUMN time '%s'",
		db, table, common.CK_VERSION)
	_, err := connect.Exec(sql)
	return err
}

func (i *Issu) renameColumns(connect *sql.DB) ([]*ColumnRename, error) {
	dones := []*ColumnRename{}
	for _, renameColumn := range i.columnRenames {
		version, err := i.getTableVersion(connect, renameColumn.Db, renameColumn.Table)
		if err != nil {
			return dones, err
		}
		if version == common.CK_VERSION {
			continue
		}
		if err := i.renameColumn(connect, renameColumn); err != nil {
			return dones, err
		}
		dones = append(dones, renameColumn)
	}

	return dones, nil
}

func getColumnAdds(columnAdds *ColumnAdds) []*ColumnAdd {
	adds := []*ColumnAdd{}
	for _, db := range columnAdds.Dbs {
		for _, tbl := range columnAdds.Tables {
			for _, clmn := range columnAdds.ColumnNames {
				adds = append(adds, &ColumnAdd{
					Db:           db,
					Table:        tbl,
					ColumnName:   clmn,
					ColumnType:   columnAdds.ColumnType,
					DefaultValue: columnAdds.DefaultValue,
				})
			}
		}
	}
	return adds
}

func (i *Issu) addColumns(connect *sql.DB) ([]*ColumnAdd, error) {
	dones := []*ColumnAdd{}
	for _, add := range i.columnAdds {
		version, err := i.getTableVersion(connect, add.Db, add.Table)
		if err != nil {
			return dones, err
		}
		if version == common.CK_VERSION {
			log.Infof("db(%s) table(%s) already updated", add.Db, add.Table)
			continue
		}
		if err := i.addColumn(connect, add); err != nil {
			return dones, err
		}
		dones = append(dones, add)
	}

	for _, db := range []string{"vtap_flow_port", "vtap_flow_edge_port"} {
		datasourceInfos, err := getUserDefinedDatasourceInfos(connect, db)
		if err != nil {
			return nil, err
		}
		for _, dsInfo := range datasourceInfos {
			adds, err := i.addColumnDatasource(connect, dsInfo)
			if err != nil {
				return nil, nil
			}
			dones = append(dones, adds...)
		}
	}

	return dones, nil
}

func (i *Issu) Start() error {
	for _, connect := range []*sql.DB{i.primaryConnection, i.SecondaryConnection} {
		if connect == nil {
			continue
		}
		renames, errRenames := i.renameColumns(connect)
		if errRenames != nil {
			return errRenames
		}

		adds, errAdds := i.addColumns(connect)
		if errAdds != nil {
			return errAdds
		}

		for _, cr := range renames {
			if err := i.setTableVersion(connect, cr.Db, cr.Table); err != nil {
				return err
			}
		}
		for _, cr := range adds {
			if err := i.setTableVersion(connect, cr.Db, cr.Table); err != nil {
				return err
			}
		}
	}
	return nil
}

func (i *Issu) Close() {
	for _, connect := range []*sql.DB{i.primaryConnection, i.SecondaryConnection} {
		if connect == nil {
			continue
		}
		connect.Close()
	}
}
