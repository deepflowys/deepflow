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

package config

type MySqlConfig struct {
	Type                   string `default:"mysql" yaml:"type"`
	Database               string `default:"deepflow" yaml:"database"`
	SchemaName             string `default:"public" yaml:"schema-name"`
	Host                   string `default:"mysql" yaml:"host"`
	Port                   uint32 `default:"30130" yaml:"port"`
	UserName               string `default:"root" yaml:"user-name"`
	UserPassword           string `default:"deepflow" yaml:"user-password"`
	TimeOut                uint32 `default:"30" yaml:"timeout"`
	DropDatabaseEnabled    bool   `default:"false" yaml:"drop-database-enabled"`
	AutoIncrementIncrement uint32 `default:"1" yaml:"auto_increment_increment"`
	ResultSetMax           uint32 `default:"100000" yaml:"result_set_max"`
}
