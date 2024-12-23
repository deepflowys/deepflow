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

package tagrecorder

import (
	"github.com/deepflowio/deepflow/server/controller/db/metadb"
	metadbmodel "github.com/deepflowio/deepflow/server/controller/db/metadb/model"
)

type ChNpbTunnel struct {
	UpdaterBase[metadbmodel.ChNpbTunnel, IDKey]
}

func NewChNpbTunnel() *ChNpbTunnel {
	updater := &ChNpbTunnel{
		UpdaterBase[metadbmodel.ChNpbTunnel, IDKey]{
			resourceTypeName: RESOURCE_TYPE_CH_NPB_TUNNEL,
		},
	}
	updater.dataGenerator = updater
	return updater
}

func (p *ChNpbTunnel) generateNewData() (map[IDKey]metadbmodel.ChNpbTunnel, bool) {
	var npbTunnels []metadbmodel.NpbTunnel
	err := metadb.DefaultDB.Unscoped().Select("id", "name").Find(&npbTunnels).Error
	if err != nil {
		log.Errorf(dbQueryResourceFailed(p.resourceTypeName, err), p.db.LogPrefixORGID)
		return nil, false
	}

	keyToItem := make(map[IDKey]metadbmodel.ChNpbTunnel)
	for _, npbTunnel := range npbTunnels {
		keyToItem[IDKey{ID: npbTunnel.ID}] = metadbmodel.ChNpbTunnel{
			ID:   npbTunnel.ID,
			Name: npbTunnel.Name,
		}
	}
	return keyToItem, true
}

func (p *ChNpbTunnel) generateKey(dbItem metadbmodel.ChNpbTunnel) IDKey {
	return IDKey{ID: dbItem.ID}
}

func (p *ChNpbTunnel) generateUpdateInfo(oldItem, newItem metadbmodel.ChNpbTunnel) (map[string]interface{}, bool) {
	updateInfo := make(map[string]interface{})
	if oldItem.Name != newItem.Name {
		updateInfo["name"] = newItem.Name
	}
	if len(updateInfo) > 0 {
		return updateInfo, true
	}
	return nil, false
}
