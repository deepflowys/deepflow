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

package model

import "context"

type Profile struct {
	AppService          string `json:"app_service" binding:"required"`
	ProfileEventType    string `json:"profile_event_type" binding:"required"`
	ProfileLanguageType string `json:"profile_language_type" binding:"required"`
	TagFilter           string `json:"tag_filter"`
	GroupBy             string `json:"group_by"`
	TimeStart           int    `json:"time_start" binding:"required"`
	TimeEnd             int    `json:"time_end" binding:"required"`
	Debug               bool   `json:"debug"`
	Context             context.Context
	OrgID               string
	MaxKernelStackDepth *int `json:"max_kernel_stack_depth"` // default: -1
}

type ProfileGrafana struct {
	Sql              string `json:"sql" binding:"required"` // profile filter
	ProfileEventType string `json:"profile_event_type" binding:"required"`
	Debug            bool   `json:"debug"`
}

type ProfileTreeNode struct {
	LocationID   int
	ParentNodeID int
	SelfValue    int
	TotalValue   int
	Depth        int
}

type Debug struct {
	IP        string `json:"ip"`
	Sql       string `json:"sql"`
	SqlCH     string `json:"sql_CH"`
	QueryTime string `json:"query_time"`
	QueryUUID string `json:"query_uuid"`
	Error     string `json:"error"`
}

type ProfileDebug struct {
	QuerierDebug []Debug `json:"querier_debug"`
	FormatTime   string  `json:"format_time"`
}

type ProfileTree struct {
	Functions      []string `json:"functions"`
	FunctionTypes  []string `json:"function_types"`
	FunctionValues Value    `json:"function_values"`
	NodeValues     Value    `json:"node_values"`
}

type Value struct {
	Columns []string `json:"columns"`
	Values  [][]int  `json:"values"`
}

type GrafanaProfileValue struct {
	Columns []string        `json:"columns"`
	Values  [][]interface{} `json:"values"`
}
