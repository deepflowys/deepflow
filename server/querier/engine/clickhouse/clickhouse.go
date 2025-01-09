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

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	//"github.com/k0kubun/pp"
	logging "github.com/op/go-logging"
	"github.com/xwb1989/sqlparser"
	"golang.org/x/exp/slices"

	"github.com/deepflowio/deepflow/server/querier/common"
	"github.com/deepflowio/deepflow/server/querier/config"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/client"
	chCommon "github.com/deepflowio/deepflow/server/querier/engine/clickhouse/common"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/metrics"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/tag"
	tagdescription "github.com/deepflowio/deepflow/server/querier/engine/clickhouse/tag"
	"github.com/deepflowio/deepflow/server/querier/engine/clickhouse/view"
	"github.com/deepflowio/deepflow/server/querier/parse"
)

var log = logging.MustGetLogger("clickhouse")

var DEFAULT_LIMIT = "10000"
var INVALID_PROMETHEUS_SUBQUERY_CACHE_ENTRY = "-1"
var subSqlRegexp = regexp.MustCompile(`\(SELECT\s.+?LIMIT\s.+?\)`)
var checkWithSqlRegexp = regexp.MustCompile(`WITH\s+\S+\s+AS\s+\(`)
var letterRegexp = regexp.MustCompile("^[a-zA-Z]")
var fromRegexp = regexp.MustCompile(`(?i)from\s+(\S+)`)
var whereRegexp = regexp.MustCompile(`(?i)where\s+(\S.*)`)
var visibilityRegexp = regexp.MustCompile(`(?i)regexp\s+(\S+)`)
var notRegexp = regexp.MustCompile(`(?i)(\S+)\s+not regexp\s+(\S+)`)

var Lock sync.Mutex

// Perform regular checks on show SQL and support the following formats:
// show tag {tag_name} values from {table_name} where xxx order by xxx limit xxx :{tag_name} and {table_name} can be any character
// show tags
// show tags from xxx
// show language
// show metrics
// show metrics from xxx
// show metrics functions
// show metrics on db
// show tables
// show databases
var showPatterns = []string{
	//if there are new pattern strings to match, add regular expressions directly here
	`^show\s+language$`, // 1. show language
	`^show\s+metrics\s+functions\s*(?:from\s+.*?\s*)?(?:where\s+.*?\s*)?$`,                                                // 2. show metrics functions
	`(^show\s+metrics(?: from [^\s]+)?(?: where .+)?$)|(^show\s+metrics on db$)`,                                          // 3. show metrics or show metrics on db
	`^show\s+tag\s+\S+\s+values\s+from\s+\S+(?: where .+)?(?: order by \w+)?(?: limit\s+\d+(,\s+\d+)?)?(?: offset \d+)?$`, // 4. show tag X values Y, X,Y not nil
	`^show\s+tags(?: from ([^\s]+))?(?: where .+)?(?: limit\s+\d+(,\s+\d+)?)?$`,                                           // 5. show tags ...
	`^show\s+tables(?: where .+)?$`,                                // 6. show tables
	`^show\s+databases(?: where .+)?$`,                             // 7. show databases
	`^show\s+tag-values(?: where .+)?(?: limit\s+\d+(,\s+\d+)?)?$`, // 8. show tag-values
	`^show all_enum_tags$`,
	`^show\s+enum\s+\S+\s+values`,
}
var res []*regexp.Regexp

type TargetLabelFilter struct {
	OriginFilter string
	TransFilter  string
}

type CHEngine struct {
	Model              *view.Model
	Statements         []Statement
	DB                 string
	Table              string
	DataSource         string
	AsTagMap           map[string]string
	ColumnSchemas      []*common.ColumnSchema
	View               *view.View
	Context            context.Context
	TargetLabelFilters []TargetLabelFilter
	NoPreWhere         bool
	IsDerivative       bool
	DerivativeGroupBy  []string
	ORGID              string
	Language           string
	DynamicTag         *common.Result
}

func init() {
	// init show patterns regexp
	for _, pattern := range showPatterns {
		res = append(res, regexp.MustCompile(pattern))
	}
}

func (e *CHEngine) ExecuteQuery(args *common.QuerierParams) (*common.Result, map[string]interface{}, error) {
	// 解析show开头的sql
	// show metrics/tags from <table_name> 例：show metrics/tags from l4_flow_log
	var err error
	sql := args.Sql
	e.Context = args.Context
	e.NoPreWhere = args.NoPreWhere
	e.Language = args.Language
	e.ORGID = common.DEFAULT_ORG_ID
	if args.ORGID != "" {
		e.ORGID = args.ORGID
	}
	query_uuid := args.QueryUUID // FIXME: should be queryUUID
	log.Debugf("query_uuid: %s | raw sql: %s", query_uuid, sql)
	debug_info := &client.DebugInfo{}
	// Parse withSql
	withResult, withDebug, err := e.QueryWithSql(sql, args)
	if err != nil {
		if withDebug != nil {
			debug_info.Debug = append(debug_info.Debug, *withDebug)
		}
		return nil, debug_info.Get(), err
	}
	if withResult != nil {
		debug_info.Debug = append(debug_info.Debug, *withDebug)
		return withResult, debug_info.Get(), err
	}
	// Parse slimitSql
	slimitResult, slimitDebug, err := e.QuerySlimitSql(sql, args)
	if err != nil {
		if slimitDebug != nil {
			debug_info.Debug = append(debug_info.Debug, *slimitDebug)
		}
		return nil, debug_info.Get(), err
	}
	if slimitResult != nil {
		debug_info.Debug = append(debug_info.Debug, *slimitDebug)
		return slimitResult, debug_info.Get(), err
	}
	// Parse showSql
	debug := &client.Debug{
		IP:        config.Cfg.Clickhouse.Host,
		QueryUUID: query_uuid,
	}
	// For testing purposes, ParseShowSql requires the addition of the debug parameter
	result, sqlList, isShow, err := e.ParseShowSql(sql, args, debug_info)
	if isShow {
		if err != nil {
			return nil, nil, err
		}
		if len(sqlList) == 0 {
			return result, debug_info.Get(), nil
		}
		e.DB = "flow_tag"
	} else { // Normal query, added to sqllist
		sqlList = append(sqlList, sql)
	}
	results := &common.Result{}
	chClient := client.Client{
		Host:     config.Cfg.Clickhouse.Host,
		Port:     config.Cfg.Clickhouse.Port,
		UserName: config.Cfg.Clickhouse.User,
		Password: config.Cfg.Clickhouse.Password,
		DB:       e.DB,
		Debug:    debug,
		Context:  e.Context,
	}
	ColumnSchemaMap := make(map[string]*common.ColumnSchema)
	if isShow {
		for _, ColumnSchema := range e.ColumnSchemas {
			ColumnSchemaMap[ColumnSchema.Name] = ColumnSchema
		}
	}
	parser := parse.Parser{}
	for _, sql1 := range sqlList {
		usedEngine := &CHEngine{}
		if isShow {
			showEngine := &CHEngine{DB: e.DB, DataSource: e.DataSource, Context: e.Context, ORGID: e.ORGID}
			showEngine.Init()
			parser.Engine = showEngine
			usedEngine = showEngine
		} else {
			parser.Engine = e
			usedEngine = e
		}
		err = parser.ParseSQL(sql1)
		if err != nil {
			errorMessage := fmt.Sprintf("sql: %s; parse error: %s", sql1, err.Error())
			log.Error(errorMessage)
			return nil, nil, err
		}
		// To do
		for _, stmt := range usedEngine.Statements {
			stmt.Format(usedEngine.Model)
		}
		FormatModel(usedEngine.Model)
		// 使用Model生成View
		usedEngine.View = view.NewView(usedEngine.Model)
		if !isShow {
			usedEngine.View.NoPreWhere = usedEngine.NoPreWhere
		}
		chSql := usedEngine.ToSQLString()
		log.Debug(chSql)
		callbacks := usedEngine.View.GetCallbacks()
		debug.Sql = chSql
		if !isShow {
			for _, ColumnSchema := range usedEngine.ColumnSchemas {
				ColumnSchemaMap[ColumnSchema.Name] = ColumnSchema
			}
		}
		log.Debug(ColumnSchemaMap)
		params := &client.QueryParams{
			Sql:             chSql,
			UseQueryCache:   args.UseQueryCache,
			QueryCacheTTL:   args.QueryCacheTTL,
			QueryUUID:       query_uuid,
			ColumnSchemaMap: ColumnSchemaMap,
			ORGID:           args.ORGID,
		}
		if !isShow {
			params.Callbacks = callbacks
		}
		result, err := chClient.DoQuery(params)
		if err != nil {
			log.Error(err)
			debug_info.Debug = append(debug_info.Debug, *debug)
			return nil, debug_info.Get(), err
		}
		if result != nil {
			results.Values = append(results.Values, result.Values...)
			results.Columns = result.Columns
			if !isShow {
				results.Schemas = result.Schemas
			}
			debug_info.Debug = append(debug_info.Debug, *debug)
		}
	}
	return results, debug_info.Get(), nil

}

func ShowTagTypeMetrics(tagDescriptions, result *common.Result, db, table string) {
	for _, tagValue := range tagDescriptions.Values {
		tagSlice := tagValue.([]interface{})
		name := tagSlice[0].(string)
		clientName := tagSlice[1].(string)
		serverName := tagSlice[2].(string)
		displayName := tagSlice[3].(string)
		displayNameZH := tagSlice[4].(string)
		displayNameEN := tagSlice[5].(string)
		tagType := tagSlice[6].(string)
		permissions := tagSlice[9].([]bool)
		if slices.Contains([]string{"auto_custom_tag", "time", "id"}, tagType) {
			continue
		}
		if db == chCommon.DB_NAME_FLOW_TAG {
			continue
		}
		if name == "lb_listener" || name == "pod_ingress" {
			continue
		}
		if len(tagSlice) >= 16 {
			notSupportedOperators := tagSlice[15].([]string)
			// not support select
			if slices.Contains(notSupportedOperators, "select") {
				continue
			}
		}
		if slices.Contains([]string{"l4_flow_log", "l7_flow_log", "application_map", "network_map", "vtap_flow_edge_port", "vtap_app_edge_port"}, table) {
			if serverName == clientName {
				clientNameMetric := []interface{}{
					clientName, true, displayName, displayNameZH, displayNameEN, "", "", "", metrics.METRICS_TYPE_NAME_MAP["tag"],
					"Tag", metrics.METRICS_OPERATORS, permissions, table, "", "", "",
				}
				result.Values = append(result.Values, clientNameMetric)
			} else {
				var (
					serverDisplayName   = displayName
					clientDisplayName   = displayName
					serverDisplayNameZH = chCommon.TagServerChPrefix + " " + displayName
					serverDisplayNameEN = chCommon.TagServerEnPrefix + " " + displayName
					clientDisplayNameZH = chCommon.TagClientChPrefix + " " + displayName
					clientDisplayNameEN = chCommon.TagClientEnPrefix + " " + displayName
				)
				if config.Cfg.Language == "en" {
					serverDisplayName = chCommon.TagServerEnPrefix + " " + displayName
					clientDisplayName = chCommon.TagClientEnPrefix + " " + displayName
				} else if config.Cfg.Language == "ch" {
					if letterRegexp.MatchString(serverName) {
						serverDisplayName = chCommon.TagServerChPrefix + " " + displayName
						clientDisplayName = chCommon.TagClientChPrefix + " " + displayName
					} else {
						serverDisplayName = chCommon.TagServerChPrefix + displayName
						clientDisplayName = chCommon.TagClientChPrefix + displayName
					}
				}
				serverNameMetric := []interface{}{
					serverName, true, serverDisplayName, serverDisplayNameZH, serverDisplayNameEN, "", "", "", metrics.METRICS_TYPE_NAME_MAP["tag"],
					"Tag", metrics.METRICS_OPERATORS, permissions, table, "", "", "",
				}
				clientNameMetric := []interface{}{
					clientName, true, clientDisplayName, clientDisplayNameZH, clientDisplayNameEN, "", "", "", metrics.METRICS_TYPE_NAME_MAP["tag"],
					"Tag", metrics.METRICS_OPERATORS, permissions, table, "", "", "",
				}
				result.Values = append(result.Values, serverNameMetric, clientNameMetric)
			}
		} else {
			nameMetric := []interface{}{
				name, true, displayName, displayNameZH, displayNameEN, "", "", "", metrics.METRICS_TYPE_NAME_MAP["tag"],
				"Tag", metrics.METRICS_OPERATORS, permissions, table, "", "", "",
			}
			result.Values = append(result.Values, nameMetric)
		}
	}
}

// extractFromWhere extracts the first string after 'from' and all strings after 'where'.
func ExtractFromWhereAndvisibilityFilter(s string) (table string, whereClause string, visibilityFilter string) {
	// Regex to capture the first string after 'from' and all strings after 'where'
	// Extract from part
	fromMatch := fromRegexp.FindStringSubmatch(s)
	if len(fromMatch) > 1 {
		table = fromMatch[1]
	}
	// Extract where part
	whereMatch := whereRegexp.FindStringSubmatch(s)
	if len(whereMatch) > 1 {
		whereClause = whereMatch[1]
	}
	visibilityFilterMatch := visibilityRegexp.FindStringSubmatch(s)
	if len(visibilityFilterMatch) > 1 {
		visibilityFilter = visibilityFilterMatch[1]
	}
	return
}

func MatchPattern(s string) (int, bool) {
	for i, re := range res {
		if re.MatchString(s) {
			return i + 1, true
		}
	}
	return 0, false
}
func dataVisibilityfiltering(visibilityFilterRegexp *regexp.Regexp, values []interface{}) []interface{} {
	var visibilityFilterValues []interface{}
	for _, value := range values {
		name := value.([]interface{})[0].(string)
		if !visibilityFilterRegexp.MatchString(name) {
			visibilityFilterValues = append(visibilityFilterValues, value)
		}
	}
	return visibilityFilterValues
}

func formatTagByLanguage(language string, values []interface{}) {
	for _, value := range values {
		displaynameZH := value.([]interface{})[4].(string)
		displaynameEN := value.([]interface{})[5].(string)
		descriptionZH := value.([]interface{})[11].(string)
		descriptionEN := value.([]interface{})[12].(string)
		if language == chCommon.LanguageEN {
			value.([]interface{})[3] = displaynameEN
			value.([]interface{})[10] = descriptionEN
		} else {
			value.([]interface{})[3] = displaynameZH
			value.([]interface{})[10] = descriptionZH
		}
	}
}

func formatMetricByLanguage(language string, values []interface{}) {
	for _, value := range values {
		displaynameZH := value.([]interface{})[3].(string)
		displaynameEN := value.([]interface{})[4].(string)
		unitZH := value.([]interface{})[6].(string)
		unitEN := value.([]interface{})[7].(string)
		descriptionZH := value.([]interface{})[14].(string)
		descriptionEN := value.([]interface{})[15].(string)
		if language == chCommon.LanguageEN {
			value.([]interface{})[2] = displaynameEN
			value.([]interface{})[5] = unitEN
			value.([]interface{})[13] = descriptionEN
		} else {
			value.([]interface{})[2] = displaynameZH
			value.([]interface{})[5] = unitZH
			value.([]interface{})[13] = descriptionZH
		}
	}
}

func formatEnumTagByLanguage(language string, values []interface{}) {
	for _, value := range values {
		displaynameZH := value.([]interface{})[4].(string)
		displaynameEN := value.([]interface{})[5].(string)
		descriptionZH := value.([]interface{})[11].(string)
		descriptionEN := value.([]interface{})[12].(string)
		if language == chCommon.LanguageEN {
			value.([]interface{})[3] = displaynameEN
			value.([]interface{})[10] = descriptionEN
		} else {
			value.([]interface{})[3] = displaynameZH
			value.([]interface{})[10] = descriptionZH
		}
	}
}

func (e *CHEngine) ParseShowSql(sql string, args *common.QuerierParams, DebugInfo *client.DebugInfo) (*common.Result, []string, bool, error) {
	var visibilityFilterRegexp *regexp.Regexp
	sqlSplit := strings.Fields(sql)
	// Not showSql, return
	if strings.ToLower(sqlSplit[0]) != "show" {
		return nil, []string{}, false, nil
	}
	sql = strings.Join(sqlSplit, " ")
	index, flag := MatchPattern(strings.ToLower(sql))
	if flag == false {
		err := fmt.Errorf("not support sql: '%s', please check", sql)
		return nil, []string{}, true, err
	}
	table, where, visibilityFilter := ExtractFromWhereAndvisibilityFilter(sql)
	visibilityWhere := ""
	visibilitySql := ""
	if len(visibilityFilter) > 0 {
		visibilitySql = notRegexp.ReplaceAllString(sql, "not match( $1 ,$2 )")
		sql = notRegexp.ReplaceAllString(sql, " 1=1 ")
		_, where, _ = ExtractFromWhereAndvisibilityFilter(sql)
		_, visibilityWhere, _ = ExtractFromWhereAndvisibilityFilter(visibilitySql)
		visibilityFilter = strings.Trim(visibilityFilter, "'")
		visibilityFilterRegexp = regexp.MustCompile(visibilityFilter)
	}

	switch table {
	case "vtap_app_port":
		table = "application"
	case "vtap_app_edge_port":
		table = "application_map"
	case "vtap_flow_port":
		table = "network"
	case "vtap_flow_edge_port":
		table = "network_map"
	case "vtap_acl":
		table = "traffic_policy"
	}
	// do the corresponding processing according to the matched pattern string
	switch index {
	case 1: // show language ...
		result := &common.Result{}
		result.Columns = []interface{}{"language"}
		result.Values = []interface{}{[]string{config.Cfg.Language}}
		return result, []string{}, true, nil
	case 2: // show metrics functions ...
		funcs, err := metrics.GetFunctionDescriptions()
		return funcs, []string{}, true, err
	case 3: // show metrics ...
		if e.DB == chCommon.DB_NAME_DEEPFLOW_TENANT && len(visibilityFilter) > 0 {
			where = visibilityWhere
			sql = visibilitySql
		}
		result, err := metrics.GetMetricsDescriptions(e.DB, table, where, args.QueryCacheTTL, args.ORGID, args.UseQueryCache, e.Context)
		if err != nil {
			return nil, []string{}, true, err
		}

		// tag metrics
		tagDescriptions, err := tag.GetTagDescriptions(e.DB, table, sql, args.QueryCacheTTL, e.ORGID, args.UseQueryCache, e.Context, DebugInfo)
		if err != nil {
			log.Error("Failed to get tag type metrics")
			return nil, []string{}, true, err
		}
		ShowTagTypeMetrics(tagDescriptions, result, e.DB, table)

		if len(visibilityFilter) > 0 && e.DB != chCommon.DB_NAME_DEEPFLOW_TENANT {
			result.Values = dataVisibilityfiltering(visibilityFilterRegexp, result.Values)
		}
		if args.Language != "" {
			formatMetricByLanguage(args.Language, result.Values)
		}
		return result, []string{}, true, err
	case 4: // show tag X values from Y  X, Y not nil
		result, sqlList, err := tagdescription.GetTagValues(e.DB, table, sql, args.QueryCacheTTL, args.ORGID, args.Language, args.UseQueryCache)
		e.DB = "flow_tag"
		return result, sqlList, true, err
	case 5: // show tags ...
		if e.DB == chCommon.DB_NAME_DEEPFLOW_TENANT && len(visibilityFilter) > 0 {
			sql = visibilitySql
		}
		data, err := tagdescription.GetTagDescriptions(e.DB, table, sql, args.QueryCacheTTL, args.ORGID, args.UseQueryCache, e.Context, DebugInfo)
		if len(visibilityFilter) > 0 && e.DB != chCommon.DB_NAME_DEEPFLOW_TENANT {
			data.Values = dataVisibilityfiltering(visibilityFilterRegexp, data.Values)
		}
		if args.Language != "" {
			formatTagByLanguage(args.Language, data.Values)
		}
		return data, []string{}, true, err
	case 6: // show  tables...
		if e.DB == chCommon.DB_NAME_DEEPFLOW_TENANT && len(visibilityFilter) > 0 {
			where = visibilityWhere
		}
		result := GetTables(e.DB, where, args.QueryCacheTTL, args.ORGID, args.UseQueryCache, e.Context, DebugInfo)
		if len(visibilityFilter) > 0 && e.DB != chCommon.DB_NAME_DEEPFLOW_TENANT {
			result.Values = dataVisibilityfiltering(visibilityFilterRegexp, result.Values)
		}
		return result, []string{}, true, nil
	case 7: // show  databases...
		result := GetDatabases()
		if len(visibilityFilter) > 0 {
			result.Values = dataVisibilityfiltering(visibilityFilterRegexp, result.Values)
		}
		return result, []string{}, true, nil
	case 8: // show tag-values...
		sqlList, err := tagdescription.GetTagValuesDescriptions(e.DB, sql, args.QueryCacheTTL, args.ORGID, args.UseQueryCache, e.Context)
		return nil, sqlList, true, err
	case 9:
		result, err := tagdescription.GetEnumTags(e.DB, table, sql)
		if args.Language != "" {
			formatTagByLanguage(args.Language, result.Values)
		}
		return result, []string{}, true, err
	case 10:
		sqlList, err := tagdescription.GetEnumTagAllValues(e.DB, table, sql, args.Language)
		return nil, sqlList, true, err
	}
	return nil, []string{}, true, fmt.Errorf("parse show sql error, sql: '%s' not support", sql)
}

func (e *CHEngine) QuerySlimitSql(sql string, args *common.QuerierParams) (*common.Result, *client.Debug, error) {
	sql, callbacks, columnSchemaMap, err := e.ParseSlimitSql(sql, args)
	if err != nil {
		log.Error(err)
		return nil, nil, err
	}
	if sql == "" {
		return nil, nil, nil
	}

	query_uuid := args.QueryUUID
	debug := &client.Debug{
		IP:        config.Cfg.Clickhouse.Host,
		QueryUUID: query_uuid,
	}

	debug.Sql = sql
	chClient := client.Client{
		Host:     config.Cfg.Clickhouse.Host,
		Port:     config.Cfg.Clickhouse.Port,
		UserName: config.Cfg.Clickhouse.User,
		Password: config.Cfg.Clickhouse.Password,
		DB:       e.DB,
		Debug:    debug,
		Context:  e.Context,
	}
	params := &client.QueryParams{
		Sql:             sql,
		Callbacks:       callbacks,
		QueryUUID:       query_uuid,
		ColumnSchemaMap: columnSchemaMap,
		ORGID:           args.ORGID,
	}
	rst, err := chClient.DoQuery(params)
	if err != nil {
		log.Error(err)
		return nil, debug, err
	}
	return rst, debug, err
}

func (e *CHEngine) ParseSlimitSql(sql string, args *common.QuerierParams) (string, map[string]func(*common.Result) error, map[string]*common.ColumnSchema, error) {
	if !strings.Contains(sql, "SLIMIT") && !strings.Contains(sql, "slimit") {
		return "", nil, nil, nil
	}
	newSql := strings.ReplaceAll(sql, " SLIMIT ", " slimit ")
	newSql = strings.ReplaceAll(newSql, " SORDER BY ", " sorder by ")
	newSql = strings.ReplaceAll(newSql, " ORDER BY ", " order by ")
	newSql = strings.ReplaceAll(newSql, " LIMIT ", " limit ")
	newSql = strings.ReplaceAll(newSql, " WHERE ", " where ")
	newSql = strings.ReplaceAll(newSql, " GROUP BY ", " group by ")
	newSqlSlice := []string{}

	var sorderByTag string
	var sorderByTagSql string
	if strings.Contains(newSql, " sorder by ") {
		sorderBySlice := strings.Split(newSql, " sorder by ")
		if strings.Contains(sorderBySlice[1], " order by ") {
			orderBySlice := strings.Split(sorderBySlice[1], " order by ")
			sorderByTagSql = orderBySlice[0]
			sorderByTag = strings.Trim(orderBySlice[0], " ASC")
			sorderByTag = strings.Trim(sorderByTag, " asc")
			sorderByTag = strings.Trim(sorderByTag, " DESC")
			sorderByTag = strings.Trim(sorderByTag, " desc")
			newSql = sorderBySlice[0] + " order by " + orderBySlice[1]
		} else {
			slimitSlice := strings.Split(sorderBySlice[1], " slimit ")
			sorderByTagSql = slimitSlice[0]
			sorderByTag = strings.Trim(slimitSlice[0], " ASC")
			sorderByTag = strings.Trim(sorderByTag, " asc")
			sorderByTag = strings.Trim(sorderByTag, " DESC")
			sorderByTag = strings.Trim(sorderByTag, " desc")
			newSql = sorderBySlice[0] + " slimit " + slimitSlice[1]
		}
	}
	if !strings.Contains(newSql, " where ") {
		if strings.Contains(newSql, " group by ") {
			groupSlice := strings.Split(newSql, " group by ")
			newSqlSlice = append(newSqlSlice, groupSlice[0])
			newSqlSlice = append(newSqlSlice, " where 1=1 group by ")
			newSqlSlimitSlice := strings.Split(groupSlice[1], " slimit ")
			newSqlSlice = append(newSqlSlice, groupSlice[0])
			if strings.Contains(newSqlSlimitSlice[1], " limit ") {
				newSqlLimitSlice := strings.Split(newSqlSlimitSlice[1], " limit ")
				newSqlSlice = append(newSqlSlice, fmt.Sprintf(" limit %s", newSqlLimitSlice[1]))
			}
			newSql = strings.Join(newSqlSlice, "")
		}
	} else {
		newSqlSlimitSlice := strings.Split(newSql, " slimit ")
		newSqlSlice = append(newSqlSlice, newSqlSlimitSlice[0])
		if strings.Contains(newSqlSlimitSlice[1], " limit ") {
			newSqlLimitSlice := strings.Split(newSqlSlimitSlice[1], " limit ")
			newSqlSlice = append(newSqlSlice, fmt.Sprintf(" limit %s", newSqlLimitSlice[1]))
		}
		newSql = strings.Join(newSqlSlice, "")
	}
	stmt, err := sqlparser.Parse(newSql)
	if err != nil {
		return "", nil, nil, err
	}
	innerSelectSlice := []string{}
	outerWhereLeftSlice := []string{}
	outerWhereLeftAppendSlice := []string{}

	innerGroupBySlice := []string{}
	innerSql := ""
	pStmt := stmt.(*sqlparser.Select)
	table := ""
	// From解析
	if pStmt.From != nil {
		for _, from := range pStmt.From {
			switch from := from.(type) {
			case *sqlparser.AliasedTableExpr:
				// 解析Table类型
				table = sqlparser.String(from)
			}
		}
	}
	showTagsSql := "show tags from " + table
	tags, _, _, err := e.ParseShowSql(showTagsSql, args, nil)
	if err != nil {
		return "", nil, nil, err
	} else if len(tags.Values) == 0 {
		err = errors.New("No data in show tags")
		return "", nil, nil, err
	}
	tagsSlice := []string{}
	for _, col := range tags.Values {
		colSlice := col.([]interface{})
		tagsSlice = append(tagsSlice, colSlice[0].(string))
		tagsSlice = append(tagsSlice, colSlice[1].(string))
		tagsSlice = append(tagsSlice, colSlice[2].(string))
	}
	// Select解析
	if pStmt.SelectExprs != nil {
		for _, tag := range pStmt.SelectExprs {
			item, ok := tag.(*sqlparser.AliasedExpr)
			if ok {
				as := sqlparser.String(item.As)
				colName, ok := item.Expr.(*sqlparser.ColName)
				if ok && (common.IsValueInSliceString(strings.Trim(strings.Trim(sqlparser.String(colName), "'"), "`"), tagsSlice) || strings.Contains(sqlparser.String(colName), "_id")) {
					colNameStr := strings.Trim(strings.Trim(sqlparser.String(colName), "'"), "`")
					if as != "" {
						selectTag := sqlparser.String(colName) + " AS " + as
						innerSelectSlice = append(innerSelectSlice, selectTag)
						if len(tagdescription.AUTO_CUSTOM_TAG_NAMES) != 0 && slices.Contains(tagdescription.AUTO_CUSTOM_TAG_NAMES, colNameStr) {
							tag, ok := tagdescription.GetTag(colNameStr, e.DB, table, "default")
							if ok {
								autoTagMap := tag.TagTranslatorMap
								autoTagSlice := []string{}
								for autoTagKey, _ := range autoTagMap {
									autoTagSlice = append(autoTagSlice, autoTagKey)
								}
								sort.Strings(autoTagSlice)
								for _, autoTagKey := range autoTagSlice {
									outerWhereLeftSlice = append(outerWhereLeftSlice, "`"+autoTagKey+"`")
								}
							}
						} else {
							outerWhereLeftSlice = append(outerWhereLeftSlice, as)
						}
					} else {
						innerSelectSlice = append(innerSelectSlice, sqlparser.String(colName))
						if len(tagdescription.AUTO_CUSTOM_TAG_NAMES) != 0 && slices.Contains(tagdescription.AUTO_CUSTOM_TAG_NAMES, colNameStr) {
							tag, ok := tagdescription.GetTag(colNameStr, e.DB, table, "default")
							if ok {
								autoTagMap := tag.TagTranslatorMap
								autoTagSlice := []string{}
								for autoTagKey, _ := range autoTagMap {
									autoTagSlice = append(autoTagSlice, autoTagKey)
								}
								sort.Strings(autoTagSlice)
								for _, autoTagKey := range autoTagSlice {
									outerWhereLeftSlice = append(outerWhereLeftSlice, "`"+autoTagKey+"`")
								}
							}
						} else {
							outerWhereLeftSlice = append(outerWhereLeftSlice, sqlparser.String(colName))
						}
					}
					for _, suffix := range []string{"", "_0", "_1"} {
						for _, resourceName := range []string{"auto_instance", "auto_service"} {
							resourceTypeSuffix := resourceName + "_type" + suffix
							if sqlparser.String(colName) == resourceName+suffix {
								outerWhereLeftAppendSlice = append(outerWhereLeftAppendSlice, resourceTypeSuffix)
							}
						}
					}
				}
				funcName, ok := item.Expr.(*sqlparser.FuncExpr)
				if ok && (as == sorderByTag || sqlparser.String(funcName) == sorderByTag) {
					if as != "" {
						metricTag := sqlparser.String(funcName) + " AS " + as
						innerSelectSlice = append(innerSelectSlice, metricTag)
					} else {
						innerSelectSlice = append(innerSelectSlice, sqlparser.String(funcName))
					}

				}

				if ok && strings.HasPrefix(sqlparser.String(funcName), "enum") {
					innerSelectSlice = append(innerSelectSlice, strings.ReplaceAll(sqlparser.String(funcName), "enum", "Enum"))
					outerWhereLeftSlice = append(outerWhereLeftSlice, "`"+strings.ReplaceAll(sqlparser.String(funcName), "enum", "Enum")+"`")
				}
			}
		}
	}
	outerWhereLeftSlice = append(outerWhereLeftSlice, outerWhereLeftAppendSlice...)
	// GroupBy解析
	if pStmt.GroupBy != nil {
		for _, group := range pStmt.GroupBy {
			colName, ok := group.(*sqlparser.ColName)
			if ok {
				if sqlparser.String(colName) == "toi" || strings.Contains(sqlparser.String(colName), "time") {
					continue
				} else if strings.Contains(sqlparser.String(colName), "node_type") || strings.Contains(sqlparser.String(colName), "icon_id") {
					continue
					// Inner sql remove star grouping
				} else if common.IsValueInSliceString(sqlparser.String(colName), []string{"_", "_0", "_1"}) {
					continue
				}
				groupTag := sqlparser.String(colName)
				if slices.Contains(outerWhereLeftSlice, groupTag) || (len(tagdescription.AUTO_CUSTOM_TAG_NAMES) != 0 && slices.Contains(tagdescription.AUTO_CUSTOM_TAG_NAMES, strings.Trim(groupTag, "`"))) {
					innerGroupBySlice = append(innerGroupBySlice, groupTag)
				}
			}
			funcName, ok := group.(*sqlparser.FuncExpr)
			if ok {
				if strings.HasPrefix(sqlparser.String(funcName), "time") {
					continue
				}
				groupTag := sqlparser.String(funcName)
				if slices.Contains(outerWhereLeftSlice, groupTag) {
					innerGroupBySlice = append(innerGroupBySlice, groupTag)
				}
			}
		}
	}
	innerTransSql := ""
	// No internal sql required when only star grouping
	if len(innerSelectSlice) > 0 {
		innerSelectSql := strings.Join(innerSelectSlice, ",")
		innerGroupBySql := strings.Join(innerGroupBySlice, ",")
		lowerSql := strings.ReplaceAll(sql, " WHERE ", " where ")
		lowerSql = strings.ReplaceAll(lowerSql, " GROUP BY ", " group by ")
		lowerSql = strings.ReplaceAll(lowerSql, " SLIMIT ", " slimit ")
		lowerSql = strings.ReplaceAll(lowerSql, " LIMIT ", " limit ")
		lowerSql = strings.ReplaceAll(lowerSql, " FROM ", " from ")
		if strings.Contains(lowerSql, " limit ") {
			lowerSqlLimitSlice := strings.Split(lowerSql, " limit ")
			lowerSql = lowerSqlLimitSlice[0]
		}
		if strings.Contains(lowerSql, " where ") {
			sqlSlice := strings.Split(lowerSql, " where ")
			if strings.Contains(lowerSql, " group by ") {
				whereSlice := strings.Split(sqlSlice[1], " group by ")
				whereSql := whereSlice[0]
				limitSlice := strings.Split(whereSlice[1], " slimit ")
				limitSql := limitSlice[1]
				if sorderByTag != "" {
					innerSql = "SELECT " + innerSelectSql + " FROM " + table + " WHERE " + whereSql + " GROUP BY " + innerGroupBySql + " ORDER BY " + sorderByTagSql + " LIMIT " + limitSql
				} else {
					innerSql = "SELECT " + innerSelectSql + " FROM " + table + " WHERE " + whereSql + " GROUP BY " + innerGroupBySql + " LIMIT " + limitSql
				}
			}
		} else {
			if strings.Contains(lowerSql, " group by ") {
				groupSlice := strings.Split(lowerSql, " group by ")
				limitSlice := strings.Split(groupSlice[1], " slimit ")
				limitSql := limitSlice[1]
				if sorderByTag != "" {
					innerSql = "SELECT " + innerSelectSql + " FROM " + table + " GROUP BY " + innerGroupBySql + " ORDER BY " + sorderByTagSql + " LIMIT " + limitSql
				} else {
					innerSql = "SELECT " + innerSelectSql + " FROM " + table + " GROUP BY " + innerGroupBySql + " LIMIT " + limitSql
				}
			}
		}
		innerEngine := &CHEngine{DB: e.DB, DataSource: e.DataSource, Context: e.Context, ORGID: e.ORGID}
		innerEngine.Init()
		if strings.Contains(innerSql, "Derivative") {
			innerEngine.IsDerivative = true
			innerEngine.Model.IsDerivative = true
			innerEngine.Model.DerivativeGroupBy = innerEngine.DerivativeGroupBy
		}
		innerParser := parse.Parser{Engine: innerEngine}
		err = innerParser.ParseSQL(innerSql)
		if err != nil {
			return "", nil, nil, fmt.Errorf("sql: %s; parse error: %s", innerSql, err.Error())
		}
		for _, stmt := range innerEngine.Statements {
			stmt.Format(innerEngine.Model)
		}
		FormatModel(innerEngine.Model)
		// 使用Model生成View
		innerEngine.View = view.NewView(innerEngine.Model)
		innerTransSql = innerEngine.ToSQLString()
	}
	outerEngine := &CHEngine{DB: e.DB, DataSource: e.DataSource, Context: e.Context, ORGID: e.ORGID}
	outerEngine.Init()
	if strings.Contains(newSql, "Derivative") {
		outerEngine.IsDerivative = true
		outerEngine.Model.IsDerivative = true
		outerEngine.Model.DerivativeGroupBy = outerEngine.DerivativeGroupBy
	}
	outerParser := parse.Parser{Engine: outerEngine}
	err = outerParser.ParseSQL(newSql)
	if err != nil {
		return "", nil, nil, fmt.Errorf("sql: %s; parse error: %s", innerSql, err.Error())
	}
	for _, stmt := range outerEngine.Statements {
		stmt.Format(outerEngine.Model)
	}
	FormatModel(outerEngine.Model)
	// 使用Model生成View
	outerEngine.View = view.NewView(outerEngine.Model)
	outerTransSql := outerEngine.ToSQLString()
	outerSlice := []string{}
	outerWhereLeftSql := strings.Join(outerWhereLeftSlice, ",")
	outerSql := ""
	// No internal sql required when only star grouping
	if len(innerSelectSlice) > 0 {
		if strings.Contains(outerTransSql, " PREWHERE ") {
			oldWhereSlice := strings.SplitN(outerTransSql, " PREWHERE ", 2)
			outerSlice = append(outerSlice, oldWhereSlice[0])
			if sorderByTag != "" {
				outerSlice = append(outerSlice, " PREWHERE ("+outerWhereLeftSql+") GLOBAL IN (SELECT "+outerWhereLeftSql+" FROM ("+innerTransSql+")) AND ")
			} else {
				outerSlice = append(outerSlice, " PREWHERE ("+outerWhereLeftSql+") IN ("+innerTransSql+") AND ")
			}
			outerSlice = append(outerSlice, oldWhereSlice[1])
			outerSql = strings.Join(outerSlice, "")
		} else if strings.Contains(outerTransSql, " WHERE ") {
			oldWhereSlice := strings.SplitN(outerTransSql, " WHERE ", 2)
			outerSlice = append(outerSlice, oldWhereSlice[0])
			if sorderByTag != "" {
				outerSlice = append(outerSlice, " WHERE ("+outerWhereLeftSql+") GLOBAL IN (SELECT "+outerWhereLeftSql+" FROM ("+innerTransSql+")) AND ")
			} else {
				outerSlice = append(outerSlice, " WHERE ("+outerWhereLeftSql+") IN ("+innerTransSql+") AND ")
			}
			outerSlice = append(outerSlice, oldWhereSlice[1])
			outerSql = strings.Join(outerSlice, "")
		}
	} else {
		outerSql = outerTransSql
	}
	outerSql = strings.Replace(outerSql, ") IN (", ") GLOBAL IN (", 1)

	callbacks := outerEngine.View.GetCallbacks()

	columnSchemaMap := make(map[string]*common.ColumnSchema)
	for _, ColumnSchema := range outerEngine.ColumnSchemas {
		columnSchemaMap[ColumnSchema.Name] = ColumnSchema
	}
	return outerSql, callbacks, columnSchemaMap, nil
}

func (e *CHEngine) QueryWithSql(sql string, args *common.QuerierParams) (*common.Result, *client.Debug, error) {
	sql, callbacks, columnSchemaMap, err := e.ParseWithSql(sql)
	if err != nil {
		log.Error(err)
		return nil, nil, err
	}
	if sql == "" {
		return nil, nil, nil
	}

	query_uuid := args.QueryUUID
	debug := &client.Debug{
		IP:        config.Cfg.Clickhouse.Host,
		QueryUUID: query_uuid,
	}
	debug.Sql = sql
	chClient := client.Client{
		Host:     config.Cfg.Clickhouse.Host,
		Port:     config.Cfg.Clickhouse.Port,
		UserName: config.Cfg.Clickhouse.User,
		Password: config.Cfg.Clickhouse.Password,
		DB:       e.DB,
		Debug:    debug,
		Context:  e.Context,
	}
	params := &client.QueryParams{
		Sql:             sql,
		UseQueryCache:   args.UseQueryCache,
		QueryCacheTTL:   args.QueryCacheTTL,
		Callbacks:       callbacks,
		QueryUUID:       query_uuid,
		ColumnSchemaMap: columnSchemaMap,
		ORGID:           args.ORGID,
	}
	rst, err := chClient.DoQuery(params)
	if err != nil {
		log.Error(err)
		return nil, debug, err
	}
	return rst, debug, err
}

func (e *CHEngine) ParseWithSql(sql string) (string, map[string]func(*common.Result) error, map[string]*common.ColumnSchema, error) {
	checks := checkWithSqlRegexp.FindAllStringSubmatch(sql, -1)
	if len(checks) == 0 {
		return "", nil, nil, nil
	}
	subMatches := subSqlRegexp.FindAllString(sql, -1)
	parsedSqls := []string{}
	var callbacks map[string]func(*common.Result) error
	columnSchemaMap := make(map[string]*common.ColumnSchema)
	for _, match := range subMatches {
		match = strings.TrimPrefix(match, "(")
		match = strings.TrimSuffix(match, ")")
		matchEngine := &CHEngine{DB: e.DB, DataSource: e.DataSource, Context: e.Context, ORGID: e.ORGID}
		matchEngine.Init()
		matchParser := parse.Parser{Engine: matchEngine}
		err := matchParser.ParseSQL(match)
		if err != nil {
			return "", nil, nil, err
		}
		for _, stmt := range matchEngine.Statements {
			stmt.Format(matchEngine.Model)
		}
		FormatModel(matchEngine.Model)
		// 使用Model生成View
		matchEngine.View = view.NewView(matchEngine.Model)
		if callbacks == nil {
			callbacks = matchEngine.View.GetCallbacks()
		}
		parsedSql := matchEngine.ToSQLString()
		for _, columnSchema := range matchEngine.ColumnSchemas {
			columnSchemaMap[columnSchema.Name] = columnSchema
		}
		parsedSqls = append(parsedSqls, parsedSql)
	}
	for i, parseSql := range parsedSqls {
		sql = strings.ReplaceAll(sql, subMatches[i], fmt.Sprintf("(%s)", parseSql))
	}
	return sql, callbacks, columnSchemaMap, nil
}

func (e *CHEngine) Init() {
	e.Model = view.NewModel()
	e.Model.DB = e.DB
	if e.ORGID == "" {
		e.ORGID = common.DEFAULT_ORG_ID
	}
}

func (e *CHEngine) TransSelect(tags sqlparser.SelectExprs) error {
	tagSlice := []string{}
	for _, tag := range tags {
		item, ok := tag.(*sqlparser.AliasedExpr)
		if ok {
			colName, ok := item.Expr.(*sqlparser.ColName)
			if ok {
				tagSlice = append(tagSlice, sqlparser.String(colName))
			}
			funcName, ok := item.Expr.(*sqlparser.FuncExpr)
			if ok {
				tagSlice = append(tagSlice, sqlparser.String(funcName))
			}

			// Determine whether there is a Derivative operator
			exprStr := sqlparser.String(item)
			if strings.Contains(exprStr, "Derivative") && !e.IsDerivative {
				e.IsDerivative = true
				e.Model.IsDerivative = true
				e.Model.DerivativeGroupBy = e.DerivativeGroupBy
			}
		}
	}
	// tap_port and tap_port_type must exist together in select
	if (common.IsValueInSliceString("tap_port", tagSlice) || common.IsValueInSliceString("capture_nic", tagSlice)) && !common.IsValueInSliceString("tap_port_type", tagSlice) && !common.IsValueInSliceString("capture_nic_type", tagSlice) && !common.IsValueInSliceString("enum(tap_port_type)", tagSlice) && !common.IsValueInSliceString("enum(capture_nic_type)", tagSlice) {
		return errors.New("tap_port(capture_nic) and tap_port_type(capture_nic_type) must exist together in select")
	}

	e.AsTagMap = make(map[string]string)
	for _, tag := range tags {
		err := e.parseSelect(tag)
		if err != nil {
			return err
		}
		item, ok := tag.(*sqlparser.AliasedExpr)
		if ok {
			as := chCommon.ParseAlias(item.As)
			colName, ok := item.Expr.(*sqlparser.ColName)
			if ok {
				if slices.Contains(tagdescription.AUTO_CUSTOM_TAG_NAMES, strings.Trim(sqlparser.String(colName), "`")) {
					autoTagValues := tagdescription.AUTO_CUSTOM_TAG_MAP[strings.Trim(sqlparser.String(colName), "`")]
					for _, selectTag := range tagSlice {
						//check if the group tags are not duplicated with the tags in the auto custom tags
						if slices.Contains(autoTagValues, strings.Trim(selectTag, "`")) {
							errStr := fmt.Sprintf("Cannot select tags that exist in auto custom tag : %s", selectTag)
							return errors.New(errStr)
						}
					}
					//check if the tags in the auto group tags are not duplicated with the tags in the auto custom tags
					for _, autoTagCheck := range autoTagValues {
						autoTagCheckTag := strings.Trim(autoTagCheck, "_0")
						autoTagCheckTag = strings.Trim(autoTagCheckTag, "_1")
						if slices.Contains(tagdescription.TAG_RESOURCE_TYPE_AUTO, autoTagCheckTag) {
							autoTagCheckValue := tagdescription.AUTO_CUSTOM_TAG_CHECK_MAP[autoTagCheckTag]
							for _, autoTag := range autoTagValues {
								if slices.Contains(autoTagCheckValue, autoTag) {
									errStr := fmt.Sprintf("auto custom tags cannot add tag (%s) included in auto group tag (%s) in auto custom tags", autoTag, autoTagCheck)
									return errors.New(errStr)
								}

							}
						}
					}
					if (slices.Contains(autoTagValues, "ip") && autoTagValues[len(autoTagValues)-1] != "ip") || (slices.Contains(autoTagValues, "ip_0") && autoTagValues[len(autoTagValues)-1] != "ip_0") || (slices.Contains(autoTagValues, "ip_1") && autoTagValues[len(autoTagValues)-1] != "ip_1") {
						errStr := "ip can only be the last tag in the auto custom tag"
						return errors.New(errStr)
					}

				}

				// pod_ingress/lb_listener is not supported by select
				if strings.HasPrefix(sqlparser.String(colName), "pod_ingress") || strings.HasPrefix(sqlparser.String(colName), "lb_listener") {
					errStr := fmt.Sprintf("%s is not supported by select", sqlparser.String(colName))
					return errors.New(errStr)
				} else if sqlparser.String(colName) == "tags" || sqlparser.String(colName) == "metrics" || sqlparser.String(colName) == "attributes" || sqlparser.String(colName) == "packet_batch" {
					if as != "" {
						errStr := fmt.Sprintf("%s does not support as", sqlparser.String(colName))
						return errors.New(errStr)
					}
				}
				if as != "" {
					e.AsTagMap[as] = chCommon.ParseAlias(colName)
				}
			}
			function, ok := item.Expr.(*sqlparser.FuncExpr)
			if ok {
				if as != "" {
					e.AsTagMap[as] = strings.Trim(sqlparser.String(function.Name), "`")
				}
			}
			binary, ok := item.Expr.(*sqlparser.BinaryExpr)
			if ok {
				if as != "" {
					e.AsTagMap[as] = sqlparser.String(binary)
				}
			}
			// Integer tag
			val, ok := item.Expr.(*sqlparser.SQLVal)
			if ok {
				if as != "" {
					e.AsTagMap[as] = sqlparser.String(val)
				}
			}
		}
	}
	return nil
}

func (e *CHEngine) TransPrometheusTargetIDFilter(expr view.Node) (view.Node, error) {
	// For all filter and hit target_id_list for target label, put them in cache
	isRemoteRead := false
	remoteRead := e.Context.Value("remote_read")
	if remoteRead != nil {
		isRemoteRead = remoteRead.(bool)
	}
	if isRemoteRead && len(e.TargetLabelFilters) > 0 {
		trgetOriginFilters := make([]string, len(e.TargetLabelFilters))
		trgetTransFilters := make([]string, len(e.TargetLabelFilters))
		for i, trgetFilter := range e.TargetLabelFilters {
			trgetOriginFilters[i] = trgetFilter.OriginFilter
			trgetTransFilters[i] = trgetFilter.TransFilter
		}
		targetOriginFilterStr := strings.Join(trgetOriginFilters, " AND ")
		prometheusSubqueryCache := GetPrometheusSubqueryCache()
		entryKey := common.EntryKey{ORGID: e.ORGID, Filter: targetOriginFilterStr}
		targetFilter, ok := prometheusSubqueryCache.Get(entryKey)
		if ok {
			filter := targetFilter.Filter
			filterTime := targetFilter.Time
			timeout := time.Since(filterTime)
			if filter != INVALID_PROMETHEUS_SUBQUERY_CACHE_ENTRY && timeout < time.Duration(config.Cfg.PrometheusIdSubqueryLruTimeout) {
				rightExpr := &view.Expr{Value: targetFilter.Filter}
				op := view.Operator{Type: view.AND}
				expr = &view.BinaryExpr{Left: expr, Right: rightExpr, Op: &op}
				return expr, nil
			} else if filter == INVALID_PROMETHEUS_SUBQUERY_CACHE_ENTRY {
				sql := strings.Join(trgetTransFilters, " INTERSECT ")
				targetFilter := fmt.Sprintf("toUInt64(target_id) IN (%s)", sql)
				rightExpr := &view.Expr{Value: targetFilter}
				op := view.Operator{Type: view.AND}
				expr = &view.BinaryExpr{Left: expr, Right: rightExpr, Op: &op}
				return expr, nil
			}
		}

		// lru timeout
		sql := strings.Join(trgetTransFilters, " INTERSECT ")
		chClient := client.Client{
			Host:     config.Cfg.Clickhouse.Host,
			Port:     config.Cfg.Clickhouse.Port,
			UserName: config.Cfg.Clickhouse.User,
			Password: config.Cfg.Clickhouse.Password,
			DB:       "flow_tag",
		}
		targetLabelRst, err := chClient.DoQuery(&client.QueryParams{Sql: sql, ORGID: e.ORGID})
		if err != nil {
			return expr, err
		}
		targetIDs := []string{}
		for _, v := range targetLabelRst.Values {
			targetID := v.([]interface{})[0]
			targetIDUInt64 := targetID.(uint64)
			targetIDString := fmt.Sprintf("%d", targetIDUInt64)
			targetIDs = append(targetIDs, targetIDString)
		}

		// If the target_id_list is less than a predefined, configurable length (such as 1000),
		// insert it into the cache; otherwise, use a subquery
		if len(targetIDs) < config.Cfg.MaxCacheableEntrySize {
			targetIDFilter := strings.Join(targetIDs, ",")
			targetFilter := ""
			if len(targetIDs) == 0 {
				targetFilter = "1!=1"
			} else {
				targetFilter = fmt.Sprintf("target_id IN (%s)", targetIDFilter)
			}
			rightExpr := &view.Expr{Value: targetFilter}
			op := view.Operator{Type: view.AND}
			expr = &view.BinaryExpr{Left: expr, Right: rightExpr, Op: &op}
			entryValue := common.EntryValue{Time: time.Now(), Filter: targetFilter}
			prometheusSubqueryCache.Add(entryKey, entryValue)
		} else if len(targetIDs) >= config.Cfg.MaxCacheableEntrySize {
			// When you find that you can't join the cache,
			// insert a special value into the cache so that the next time you check the cache, you will find
			entryValue := common.EntryValue{Time: time.Now(), Filter: INVALID_PROMETHEUS_SUBQUERY_CACHE_ENTRY}
			prometheusSubqueryCache.Add(entryKey, entryValue)
			targetFilter := fmt.Sprintf("toUInt64(target_id) IN (%s)", sql)
			rightExpr := &view.Expr{Value: targetFilter}
			op := view.Operator{Type: view.AND}
			expr = &view.BinaryExpr{Left: expr, Right: rightExpr, Op: &op}
		}
	}
	return expr, nil
}

func (e *CHEngine) TransWhere(node *sqlparser.Where) error {
	// 生成where的statement
	whereStmt := Where{time: e.Model.Time}
	// 解析ast树并生成view.Node结构
	// Time-first parsing
	e.parseTimeWhere(node.Expr, &whereStmt)
	expr, err := e.parseWhere(node.Expr, &whereStmt, false)
	if err != nil {
		return err
	}
	expr, err = e.TransPrometheusTargetIDFilter(expr)
	filter := view.Filters{Expr: expr}
	whereStmt.filter = &filter
	e.Statements = append(e.Statements, &whereStmt)
	return err
}

func (e *CHEngine) TransHaving(node *sqlparser.Where) error {
	// 生成having的statement
	havingStmt := Having{Where{}}
	// 解析ast树并生成view.Node结构
	// having中的metric需要在trans之前确定是否分层，所以需要提前遍历
	_, err := e.parseWhere(node.Expr, &havingStmt.Where, true)
	if err != nil {
		return err
	}
	expr, err := e.parseWhere(node.Expr, &havingStmt.Where, false)
	filter := view.Filters{Expr: expr}
	havingStmt.filter = &filter
	e.Statements = append(e.Statements, &havingStmt)
	return err
}

func (e *CHEngine) TransFrom(froms sqlparser.TableExprs) error {
	for _, from := range froms {
		switch from := from.(type) {
		case *sqlparser.AliasedTableExpr:
			// 解析Table类型
			table := strings.Trim(sqlparser.String(from), "`")
			if strings.Contains(table, "vtap_app_port") {
				table = strings.ReplaceAll(table, "vtap_app_port", "application")
			} else if strings.Contains(table, "vtap_app_edge_port") {
				table = strings.ReplaceAll(table, "vtap_app_edge_port", "application_map")
			} else if strings.Contains(table, "vtap_flow_port") {
				table = strings.ReplaceAll(table, "vtap_flow_port", "network")
			} else if strings.Contains(table, "vtap_flow_edge_port") {
				table = strings.ReplaceAll(table, "vtap_flow_edge_port", "network_map")
			} else if strings.Contains(table, "vtap_acl") {
				table = strings.ReplaceAll(table, "vtap_acl", "traffic_policy")
			}
			e.Table = table
			// get dynamic tag
			dynamicTag, err := tag.GetDynamicTagDescriptions(e.DB, e.Table, "", config.Cfg.Clickhouse.QueryCacheTTL, e.ORGID, config.Cfg.Clickhouse.UseQueryCache, context.Background(), nil)
			if err != nil {
				log.Error("Failed to get tag type dynamic metrics")
				return err
			}
			e.DynamicTag = dynamicTag
			// ext_metrics只有metrics表，使用virtual_table_name做过滤区分
			if e.DB == "ext_metrics" {
				table = "metrics"
			} else if e.DB == chCommon.DB_NAME_DEEPFLOW_ADMIN {
				table = "deepflow_server"
			} else if e.DB == chCommon.DB_NAME_DEEPFLOW_TENANT {
				table = "deepflow_collector"
			} else if e.DB == chCommon.DB_NAME_PROMETHEUS {
				whereStmt := Where{}
				metricIDFilter, err := GetMetricIDFilter(e)
				if err != nil {
					return err
				}
				filter := view.Filters{Expr: metricIDFilter}
				whereStmt.filter = &filter
				e.Statements = append(e.Statements, &whereStmt)
				table = "samples"
			}
			interval, err := chCommon.GetDatasourceInterval(e.DB, e.Table, e.DataSource, e.ORGID)
			if err != nil {
				log.Error(err)
				return err
			}
			e.Model.Time.DatasourceInterval = interval
			newDB := e.DB
			if e.ORGID != common.DEFAULT_ORG_ID && e.ORGID != "" {
				orgIDInt, err := strconv.Atoi(e.ORGID)
				if err != nil {
					log.Error(err)
					return err
				}
				if e.DB != chCommon.DB_NAME_FLOW_TAG {
					newDB = fmt.Sprintf("%04d_%s", orgIDInt, e.DB)
				}
			}
			if e.DataSource != "" {
				e.AddTable(fmt.Sprintf("%s.`%s.%s`", newDB, table, e.DataSource))
			} else {
				e.AddTable(fmt.Sprintf("%s.`%s`", newDB, table))
			}
			virtualTableFilter, ok := GetVirtualTableFilter(e.DB, e.Table)
			if ok {
				whereStmt := Where{}
				filter := view.Filters{Expr: virtualTableFilter}
				whereStmt.filter = &filter
				e.Statements = append(e.Statements, &whereStmt)
			}
		}
	}
	return nil
}

func (e *CHEngine) TransGroupBy(groups sqlparser.GroupBy) error {
	groupSlice := []string{}
	for _, group := range groups {
		colName, ok := group.(*sqlparser.ColName)
		if ok {
			groupTag := sqlparser.String(colName)
			preAsGroup, ok := e.AsTagMap[groupTag]
			if ok {
				groupSlice = append(groupSlice, preAsGroup)
			} else {
				groupSlice = append(groupSlice, groupTag)
			}
		}
		funcName, ok := group.(*sqlparser.FuncExpr)
		if ok {
			groupTag := sqlparser.String(funcName)
			preAsGroup, ok := e.AsTagMap[groupTag]
			if ok {
				groupSlice = append(groupSlice, preAsGroup)
			} else {
				groupSlice = append(groupSlice, groupTag)
			}
		}
	}
	// tap_port and tap_port_type must exist together in group
	if (common.IsValueInSliceString("tap_port", groupSlice) || common.IsValueInSliceString("capture_nic", groupSlice)) && !common.IsValueInSliceString("tap_port_type", groupSlice) && !common.IsValueInSliceString("capture_nic_type", groupSlice) && !common.IsValueInSliceString("enum(tap_port_type)", groupSlice) && !common.IsValueInSliceString("enum(capture_nic_type)", groupSlice) {
		return errors.New("tap_port(capture_nic) and tap_port_type(capture_nic_type) must exist together in group")
	}
	for _, group := range groups {
		colName, ok := group.(*sqlparser.ColName)
		if ok {
			groupTag := sqlparser.String(colName)
			if slices.Contains(tagdescription.AUTO_CUSTOM_TAG_NAMES, strings.Trim(groupTag, "`")) {
				autoTagValues := tagdescription.AUTO_CUSTOM_TAG_MAP[strings.Trim(groupTag, "`")]
				for _, sliceGroup := range groupSlice {
					if slices.Contains(autoTagValues, strings.Trim(sliceGroup, "`")) {
						errStr := fmt.Sprintf("Cannot group by tags that exist in auto custom tag : %s", groupTag)
						return errors.New(errStr)
					}
				}
			}
		}
		err := e.parseGroupBy(group)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *CHEngine) TransDerivativeGroupBy(groups sqlparser.GroupBy) error {
	groupSlice := []string{}
	for _, group := range groups {
		colName, ok := group.(*sqlparser.ColName)
		if ok {
			groupTag := sqlparser.String(colName)
			if !strings.Contains(groupTag, "time") && !strings.Contains(groupTag, "node_type") && !strings.Contains(groupTag, "icon_id") {
				groupSlice = append(groupSlice, groupTag)
			}
		}
	}
	e.DerivativeGroupBy = groupSlice
	return nil
}

func (e *CHEngine) TransOrderBy(orders sqlparser.OrderBy) error {
	for _, order := range orders {
		err := e.parseOrderBy(order)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *CHEngine) TransLimit(limit *sqlparser.Limit) error {
	e.Model.Limit.Limit = sqlparser.String(limit.Rowcount)
	if limit.Offset != nil {
		e.Model.Limit.Offset = sqlparser.String(limit.Offset)
	}
	return nil
}

// 原始sql转为clickhouse-sql
func (e *CHEngine) ToSQLString() string {
	if e.View == nil {
		for _, stmt := range e.Statements {
			stmt.Format(e.Model)
		}
		FormatLimit(e.Model)
		// 使用Model生成View
		e.View = view.NewView(e.Model)
	}
	// View生成clickhouse-sql
	chSql := e.View.ToString()
	return chSql
}

func (e *CHEngine) parseOrderBy(order *sqlparser.Order) error {
	switch expr := order.Expr.(type) {
	case *sqlparser.FuncExpr:
		e.Model.Orders.Append(
			&view.Order{
				SortBy:  sqlparser.String(expr),
				OrderBy: order.Direction,
				IsField: false,
			},
		)
	case *sqlparser.ColName:
		e.Model.Orders.Append(
			&view.Order{
				SortBy:  chCommon.ParseAlias(expr),
				OrderBy: order.Direction,
				IsField: true,
			},
		)
	}
	return nil
}

// 解析GroupBy
func (e *CHEngine) parseGroupBy(group sqlparser.Expr) error {
	//var args []string
	switch expr := group.(type) {
	// 普通字符串
	case *sqlparser.ColName, *sqlparser.SQLVal:
		groupTag := chCommon.ParseAlias(expr)
		// pod_ingress/lb_listener is not supported by group
		if strings.HasPrefix(groupTag, "pod_ingress") || strings.HasPrefix(groupTag, "lb_listener") {
			errStr := fmt.Sprintf("%s is not supported by group", groupTag)
			return errors.New(errStr)
		}
		err := e.AddGroup(groupTag)
		if err != nil {
			return err
		}
		preAsGroup, ok := e.AsTagMap[groupTag]
		if !ok {
			_, err := e.AddTag(groupTag, "")
			if err != nil {
				return err
			}
		} else {
			// pod_ingress/lb_listener is not supported by group
			if strings.HasPrefix(preAsGroup, "pod_ingress") || strings.HasPrefix(preAsGroup, "lb_listener") {
				errStr := fmt.Sprintf("%s is not supported by group", groupTag)
				return errors.New(errStr)
			}
		}
		// TODO: 特殊处理塞进group的fromat中
		whereStmt := Where{}
		notNullExpr, ok := GetNotNullFilter(groupTag, e)
		if !ok {
			return nil
		}
		filter := view.Filters{Expr: notNullExpr}
		whereStmt.filter = &filter
		e.Statements = append(e.Statements, &whereStmt)
	// func(field)
	case *sqlparser.FuncExpr:
		/* name, args, err := e.parseFunction(expr)
		if err != nil {
			return err
		}
		err = e.AddFunction(name, args, "", as)
		return err */
	// field +=*/ field
	case *sqlparser.BinaryExpr:
		/* function := expr.Left.(*sqlparser.FuncExpr)
		name, args, err := e.parseFunction(function)
		if err != nil {
			return err
		}
		math := expr.Operator
		math += sqlparser.String(expr.Right)
		e.AddFunction(name, args, math, as) */
	}
	return nil
}

// 解析Select
func (e *CHEngine) parseSelect(tag sqlparser.SelectExpr) error {
	// 解析select内容
	switch tag := tag.(type) {
	// 带as
	case *sqlparser.AliasedExpr:
		return e.parseSelectAlias(tag)
	}
	return nil
}

func (e *CHEngine) parseSelectAlias(item *sqlparser.AliasedExpr) error {
	as := chCommon.ParseAlias(item.As)
	labelType := ""
	if as != "" {
		e.ColumnSchemas = append(e.ColumnSchemas, common.NewColumnSchema(as, strings.ReplaceAll(chCommon.ParseAlias(item.Expr), "`", ""), labelType))
	} else {
		e.ColumnSchemas = append(e.ColumnSchemas, common.NewColumnSchema(strings.ReplaceAll(chCommon.ParseAlias(item.Expr), "`", ""), "", labelType))
	}
	//var args []string
	switch expr := item.Expr.(type) {
	// 普通字符串
	case *sqlparser.ParenExpr:
		binFunction, err := e.parseSelectBinaryExpr(expr)
		if err != nil {
			return err
		}
		binFunction.SetAlias(as)
		e.Statements = append(e.Statements, binFunction)
		return nil
	case *sqlparser.ColName, *sqlparser.SQLVal:
		labelType, err := e.AddTag(chCommon.ParseAlias(expr), as)
		if err != nil {
			return err
		}
		if labelType != "" {
			if as != "" {
				e.ColumnSchemas[len(e.ColumnSchemas)-1] = common.NewColumnSchema(as, strings.ReplaceAll(chCommon.ParseAlias(item.Expr), "`", ""), labelType)
			} else {
				e.ColumnSchemas[len(e.ColumnSchemas)-1] = common.NewColumnSchema(strings.ReplaceAll(chCommon.ParseAlias(item.Expr), "`", ""), "", labelType)
			}
		}
		return nil
	// func(field/tag)
	case *sqlparser.FuncExpr:
		// 二级运算符
		if common.IsValueInSliceString(sqlparser.String(expr.Name), view.MATH_FUNCTIONS) {
			if as == "" {
				as = strings.ReplaceAll(chCommon.ParseAlias(item.Expr), "`", "")
			}
			binFunction, err := e.parseSelectBinaryExpr(expr)
			if err != nil {
				return err
			}
			binFunction.SetAlias(as)
			e.Statements = append(e.Statements, binFunction)
			return nil
		}
		name, args, derivativeArgs, err := e.parseFunction(expr)
		if err != nil {
			return err
		}
		name = strings.Trim(name, "`")
		functionAs := as
		if as == "" {
			if name == view.FUNCTION_TOPK {
				argLength := len(args)
				functionAs = strings.Join(
					[]string{
						view.FUNCTION_TOPK, "_", args[argLength-1],
						"(", strings.Join(args[:argLength-1], ", "), ")",
					}, "")
			} else {
				functionAs = strings.ReplaceAll(chCommon.ParseAlias(item.Expr), "`", "")
			}
		}
		function, levelFlag, unit, err := GetAggFunc(name, args, functionAs, derivativeArgs, e)
		if err != nil {
			return err
		}
		if function != nil {
			// 通过metric判断view是否拆层
			e.SetLevelFlag(levelFlag)
			e.Statements = append(e.Statements, function)
			e.ColumnSchemas[len(e.ColumnSchemas)-1].Type = common.COLUMN_SCHEMA_TYPE_METRICS
			if unit != "" {
				e.ColumnSchemas[len(e.ColumnSchemas)-1].Unit = unit
			}
			return nil
		}
		args[0] = strings.Trim(args[0], "`")
		tagFunction, err := GetTagFunction(name, args, as, e)
		if err != nil {
			return err
		}
		if tagFunction != nil {
			// time需要被最先解析
			if name == "time" {
				tagFunction.(*Time).Trans(e.Model)
				e.Statements = append([]Statement{tagFunction}, e.Statements...)
			} else {
				e.Statements = append(e.Statements, tagFunction)
			}
			return nil
		}
		return errors.New(fmt.Sprintf("function: %s not support", sqlparser.String(expr)))
	// field +=*/ field 运算符
	case *sqlparser.BinaryExpr:
		if as == "" {
			as = strings.ReplaceAll(chCommon.ParseAlias(item.Expr), "`", "")
		}
		binFunction, err := e.parseSelectBinaryExpr(expr)
		if err != nil {
			return err
		}
		binFunction.SetAlias(as)
		e.Statements = append(e.Statements, binFunction)
		return nil
	default:
		return errors.New(fmt.Sprintf("select: %s(%T) not support", sqlparser.String(expr), expr))
	}
}

func (e *CHEngine) parseFunction(item *sqlparser.FuncExpr) (name string, args []string, derivativeArgs []string, err error) {
	for _, arg := range item.Exprs {
		argStr := sqlparser.String(arg)
		args = append(args, argStr)
		if e.IsDerivative {
			// Obtain derivative parameters
			if strings.Contains(argStr, "Derivative(") {
				derivativeArgStr := strings.TrimPrefix(argStr, "Derivative(")
				derivativeArgStr = strings.TrimSuffix(derivativeArgStr, ")")
				derivativeArgSlice := strings.Split(derivativeArgStr, ",")
				// Add default group
				if len(derivativeArgSlice) == 1 {
					derivativeArgSlice = append(derivativeArgSlice, "tag")
				}
				for i, originArg := range derivativeArgSlice {
					originArg = strings.TrimSpace(originArg)
					if e.IsDerivative && i > 0 {
						if !slices.Contains(e.DerivativeGroupBy, originArg) {
							tagTranslatorStr := originArg
							if strings.Contains(originArg, "tag") {
								tagTranslatorStr = GetPrometheusGroup(originArg, e)
							} else {
								tagItem, ok := tag.GetTag(name, e.DB, e.Table, "default")
								if ok {
									tagTranslatorStr = tagItem.TagTranslator
								}
							}
							if tagTranslatorStr == originArg {
								e.Model.AddGroup(&view.Group{Value: tagTranslatorStr, Flag: view.GROUP_FLAG_METRICS_INNTER})
							} else {
								e.Model.AddGroup(&view.Group{Value: tagTranslatorStr, Flag: view.GROUP_FLAG_METRICS_INNTER, Alias: originArg})
							}
						}
					}
					derivativeArgs = append(derivativeArgs, originArg)
				}
				args[0] = derivativeArgs[0]
			}
		}
	}
	return sqlparser.String(item.Name), args, derivativeArgs, nil
}

// 解析运算符
func (e *CHEngine) parseSelectBinaryExpr(node sqlparser.Expr) (binary Function, err error) {
	switch expr := node.(type) {
	case *sqlparser.BinaryExpr:
		if !common.IsValueInSliceString(expr.Operator, view.MATH_FUNCTIONS) {
			// TODO: 报错 不支持的math
			return nil, errors.New(fmt.Sprintf("Operator: %s not support in binary", expr.Operator))
		}
		left, err := e.parseSelectBinaryExpr(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.parseSelectBinaryExpr(expr.Right)
		if err != nil {
			return nil, err
		}
		return GetBinaryFunc(expr.Operator, []Function{left, right})
	case *sqlparser.FuncExpr:
		// 嵌套算子
		if common.IsValueInSliceString(sqlparser.String(expr.Name), view.MATH_FUNCTIONS) {
			args := []Function{}
			for _, argExpr := range expr.Exprs {
				arg, err := e.parseSelectBinaryExpr(argExpr.(*sqlparser.AliasedExpr).Expr)
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
			}
			if function, ok := metrics.METRICS_FUNCTIONS_MAP[sqlparser.String(expr.Name)]; ok {
				e.ColumnSchemas[len(e.ColumnSchemas)-1].Unit = strings.ReplaceAll(function.UnitOverwrite, "$unit", e.ColumnSchemas[len(e.ColumnSchemas)-1].Unit)
			}
			return GetBinaryFunc(sqlparser.String(expr.Name), args)
		}
		name, args, derivativeArgs, err := e.parseFunction(expr)
		name = strings.Trim(name, "`")
		if err != nil {
			return nil, err
		}
		aggfunction, levelFlag, unit, err := GetAggFunc(name, args, "", derivativeArgs, e)
		if err != nil {
			return nil, err
		}
		if aggfunction != nil {
			// 通过metric判断view是否拆层
			e.SetLevelFlag(levelFlag)
			e.ColumnSchemas[len(e.ColumnSchemas)-1].Type = common.COLUMN_SCHEMA_TYPE_METRICS
			if unit != "" && e.ColumnSchemas[len(e.ColumnSchemas)-1].Unit == "" {
				e.ColumnSchemas[len(e.ColumnSchemas)-1].Unit = unit
			}
			return aggfunction.(Function), nil
		}
		tagFunction, err := GetTagFunction(name, args, "", e)
		if err != nil {
			return nil, err
		}
		if tagFunction != nil {
			function, ok := tagFunction.(Function)
			if !ok {
				return nil, errors.New(fmt.Sprintf("tagfunction: %s not support in binary", sqlparser.String(expr)))
			}
			return function, nil
		}
		return nil, errors.New(fmt.Sprintf("function: %s not support in binary", sqlparser.String(expr)))
	case *sqlparser.ParenExpr:
		// 括号
		return e.parseSelectBinaryExpr(expr.Expr)
	case *sqlparser.SQLVal:
		return &Field{Value: sqlparser.String(expr)}, nil
	case *sqlparser.ColName:
		field := sqlparser.String(expr)
		fieldFunc, err := GetFieldFunc(field)
		if err != nil {
			return nil, err
		}
		if fieldFunc != nil {
			return fieldFunc, nil
		}
		metricStruct, ok := metrics.GetAggMetrics(field, e.DB, e.Table, e.ORGID, e.DynamicTag)
		if ok {
			return &Field{Value: metricStruct.DBField}, nil
		}
		return &Field{Value: sqlparser.String(expr)}, nil
	default:
		// TODO: 报错
		return nil, nil
	}
}

func (e *CHEngine) AddGroup(group string) error {
	stmts, err := GetGroup(group, e)
	if err != nil {
		return err
	}
	if len(stmts) != 0 {
		e.Statements = append(e.Statements, stmts...)
	}
	return nil
}

func (e *CHEngine) AddTable(table string) {
	stmt := &Table{Value: table}
	e.Statements = append(e.Statements, stmt)
}

func (e *CHEngine) AddTag(tag string, alias string) (string, error) {

	stmts, labelType, err := GetTagTranslator(tag, alias, e)

	if err != nil {
		return labelType, err
	}
	if len(stmts) != 0 {
		e.Statements = append(e.Statements, stmts...)
		return labelType, nil
	}
	stmt, err := GetMetricsTag(tag, alias, e)
	if err != nil {
		return labelType, err
	}
	if stmt != nil {
		e.Statements = append(e.Statements, stmt)
		return labelType, nil
	}
	stmt = GetDefaultTag(tag, alias)
	e.Statements = append(e.Statements, stmt)
	return labelType, nil
}

func (e *CHEngine) SetLevelFlag(flag int) {
	if flag > e.Model.MetricsLevelFlag {
		e.Model.MetricsLevelFlag = flag
	}
	e.Model.HasAggFunc = true
}

func (e *CHEngine) parseWhere(node sqlparser.Expr, w *Where, isCheck bool) (view.Node, error) {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		left, err := e.parseWhere(node.Left, w, isCheck)
		if err != nil {
			return left, err
		}
		right, err := e.parseWhere(node.Right, w, isCheck)
		if err != nil {
			return right, err
		}
		op := view.Operator{Type: view.AND}
		if !isCheck {
			if left.ToString() == "" {
				return right, nil
			} else if right.ToString() == "" {
				return left, nil
			}
		}
		return &view.BinaryExpr{Left: left, Right: right, Op: &op}, nil
	case *sqlparser.OrExpr:
		left, err := e.parseWhere(node.Left, w, isCheck)
		if err != nil {
			return left, err
		}
		right, err := e.parseWhere(node.Right, w, isCheck)
		if err != nil {
			return right, err
		}
		op := view.Operator{Type: view.OR}
		return &view.BinaryExpr{Left: left, Right: right, Op: &op}, nil
	case *sqlparser.NotExpr:
		expr, err := e.parseWhere(node.Expr, w, isCheck)
		if err != nil {
			return expr, err
		}
		op := view.Operator{Type: view.NOT}
		return &view.UnaryExpr{Op: &op, Expr: expr}, nil
	case *sqlparser.ParenExpr: // 括号
		expr, err := e.parseWhere(node.Expr, w, isCheck)
		if err != nil {
			return expr, err
		}
		return &view.Nested{Expr: expr}, nil
	case *sqlparser.ComparisonExpr:
		var comparExpr sqlparser.Expr
		switch expr := node.Left.(type) {
		case *sqlparser.ParenExpr: // 括号
			comparExpr = expr.Expr
		default:
			comparExpr = expr
		}
		switch comparExpr.(type) {
		case *sqlparser.ColName, *sqlparser.SQLVal:
			whereTag := chCommon.ParseAlias(node.Left)
			metricStruct, ok := metrics.GetMetrics(whereTag, e.DB, e.Table, e.ORGID, e.DynamicTag)
			if ok && metricStruct.Type != metrics.METRICS_TYPE_TAG {
				whereTag = metricStruct.DBField
			}
			whereValue := sqlparser.String(node.Right)
			stmt := GetWhere(whereTag, whereValue)
			return stmt.Trans(node, w, e)
		case *sqlparser.FuncExpr, *sqlparser.BinaryExpr:
			function, err := e.parseSelectBinaryExpr(comparExpr)
			if err != nil {
				return nil, err
			}
			if isCheck {
				return nil, nil
			}
			outfunc := function.Trans(e.Model)
			stmt := &WhereFunction{Function: outfunc, Value: sqlparser.String(node.Right)}
			return stmt.Trans(node, w, e)
		}
	case *sqlparser.FuncExpr:
		args := []string{}
		for _, argExpr := range node.Exprs {
			switch argExpr := argExpr.(*sqlparser.AliasedExpr).Expr.(type) {
			case *sqlparser.ColName:
				arg := sqlparser.String(argExpr)
				args = append(args, arg)
			}
		}
		whereFilter := TransWhereTagFunction(e.DB, e.Table, sqlparser.String(node.Name), args)
		if whereFilter == "" {
			return nil, nil
		}
		return &view.Expr{Value: "(" + whereFilter + ")"}, nil

	}
	return nil, errors.New(fmt.Sprintf("parse where error: %s(%T)", sqlparser.String(node), node))
}

func (e *CHEngine) parseTimeWhere(node sqlparser.Expr, w *Where) (view.Node, error) {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		left, err := e.parseTimeWhere(node.Left, w)
		if err != nil {
			return left, err
		}
		right, err := e.parseTimeWhere(node.Right, w)
		if err != nil {
			return right, err
		}
		op := view.Operator{Type: view.AND}
		return &view.BinaryExpr{Left: left, Right: right, Op: &op}, nil
	case *sqlparser.OrExpr:
		left, err := e.parseTimeWhere(node.Left, w)
		if err != nil {
			return left, err
		}
		right, err := e.parseTimeWhere(node.Right, w)
		if err != nil {
			return right, err
		}
		op := view.Operator{Type: view.OR}
		return &view.BinaryExpr{Left: left, Right: right, Op: &op}, nil
	case *sqlparser.NotExpr:
		expr, err := e.parseTimeWhere(node.Expr, w)
		if err != nil {
			return expr, err
		}
		op := view.Operator{Type: view.NOT}
		return &view.UnaryExpr{Op: &op, Expr: expr}, nil
	case *sqlparser.ComparisonExpr:
		var comparExpr sqlparser.Expr
		switch expr := node.Left.(type) {
		case *sqlparser.ParenExpr: // 括号
			comparExpr = expr.Expr
		default:
			comparExpr = expr
		}
		switch comparExpr.(type) {
		case *sqlparser.ColName, *sqlparser.SQLVal:
			whereTag := chCommon.ParseAlias(node.Left)
			if whereTag == "time" {
				whereValue := sqlparser.String(node.Right)
				stmt := GetWhere(whereTag, whereValue)
				return stmt.Trans(node, w, e)
			}
		}
	}
	return nil, nil
}

// 翻译单元,翻译结果写入view.Model
type Statement interface {
	Format(*view.Model)
}

func LoadDbDescriptions(dbDescriptions map[string]interface{}) error {
	dbData, ok := dbDescriptions["clickhouse"]
	if !ok {
		return errors.New("clickhouse not in dbDescription")
	}

	dbDataMap := dbData.(map[string]interface{})
	// 加载metric定义
	if metricData, ok := dbDataMap["metrics"]; ok {
		for db, tables := range chCommon.DB_TABLE_MAP {
			if slices.Contains([]string{chCommon.DB_NAME_DEEPFLOW_ADMIN, chCommon.DB_NAME_EXT_METRICS, chCommon.DB_NAME_DEEPFLOW_TENANT}, db) {
				continue
			}
			for _, table := range tables {
				loadMetrics, err := metrics.LoadMetrics(db, table, metricData.(map[string]interface{}))
				if err != nil {
					return err
				}
				err = metrics.MergeMetrics(db, table, loadMetrics)
				if err != nil {
					return err
				}
			}
		}
	} else {
		return errors.New("clickhouse not has metrics")
	}
	// 加载tag定义及部分tag的enum取值
	if tagData, ok := dbDataMap["tag"]; ok {
		err := tagdescription.LoadTagDescriptions(tagData.(map[string]interface{}))
		if err != nil {
			return err
		}
	} else {
		return errors.New("clickhouse not has tag")
	}
	return nil
}

func FormatModel(m *view.Model) {
	FormatInnerTime(m)
	FormatLimit(m)
}

func FormatLimit(m *view.Model) {
	if m.Limit.Limit == "" {
		defaultLimit := DEFAULT_LIMIT
		if config.Cfg != nil {
			defaultLimit = config.Cfg.Limit
		}
		m.Limit.Limit = defaultLimit
	}
}
