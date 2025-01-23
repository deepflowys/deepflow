/**
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

package common

import (
	"gorm.io/gorm"

	"github.com/deepflowio/deepflow/server/controller/db/mysql/common"
	"github.com/deepflowio/deepflow/server/controller/db/mysql/config"
)

func GetSessionWithoutName(cfg config.MySqlConfig) (*gorm.DB, error) {
	connector, err := common.GetConnector(cfg, false, cfg.TimeOut, false)
	if err != nil {
		return nil, err
	}
	return common.InitSession(cfg, connector)
}

func GetSessionWithName(cfg config.MySqlConfig) (*gorm.DB, error) {
	// set multiStatements=true in dsn only when migrating MySQL
	connector, err := common.GetConnector(cfg, true, cfg.TimeOut*2, true)
	if err != nil {
		return nil, err
	}
	return common.InitSession(cfg, connector)
}
