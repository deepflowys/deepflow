/**
 * Copyright (c) 2023 Yunshan Networks
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

package resource

import (
	ctrlrcommon "github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/config"
	"github.com/deepflowio/deepflow/server/controller/db/redis"
	"github.com/deepflowio/deepflow/server/controller/http/model"
	"github.com/deepflowio/deepflow/server/controller/http/service/resource/data"
	"github.com/deepflowio/deepflow/server/controller/http/service/resource/filter/generator"
)

type IP struct {
	ServiceGet
}

func NewIPGet(urlInfo *model.URLInfo, userInfo *model.UserInfo, redisCfg redis.Config, fpermitCfg config.FPermit) *IP {
	s := &IP{newServiceGet(ctrlrcommon.RESOURCE_TYPE_IP_EN, data.GetDataProvider(ctrlrcommon.RESOURCE_TYPE_IP_EN, &data.RequiredConfigs{Redis: redisCfg}))}
	s.generateDataContext(urlInfo, userInfo, generator.NewIP(fpermitCfg))
	return s
}
