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

package cache

import (
	cloudmodel "github.com/deepflowio/deepflow/server/controller/cloud/model"
	"github.com/deepflowio/deepflow/server/controller/db/mysql"
	. "github.com/deepflowio/deepflow/server/controller/recorder/common"
)

func (b *DiffBaseDataSet) addPodIngressRuleBackend(dbItem *mysql.PodIngressRuleBackend, seq int) {
	b.PodIngressRuleBackends[dbItem.Lcuuid] = &PodIngressRuleBackend{
		DiffBase: DiffBase{
			Sequence: seq,
			Lcuuid:   dbItem.Lcuuid,
		},
		SubDomainLcuuid: dbItem.SubDomain,
	}
	b.GetLogFunc()(addDiffBase(RESOURCE_TYPE_POD_INGRESS_RULE_BACKEND_EN, b.PodIngressRuleBackends[dbItem.Lcuuid]))
}

func (b *DiffBaseDataSet) deletePodIngressRuleBackend(lcuuid string) {
	delete(b.PodIngressRuleBackends, lcuuid)
	log.Info(deleteDiffBase(RESOURCE_TYPE_POD_INGRESS_RULE_BACKEND_EN, lcuuid))
}

type PodIngressRuleBackend struct {
	DiffBase
	SubDomainLcuuid string `json:"sub_domain_lcuuid"`
}

func (p *PodIngressRuleBackend) Update(cloudItem *cloudmodel.PodIngressRuleBackend) {
	p.SubDomainLcuuid = cloudItem.SubDomainLcuuid
	log.Info(updateDiffBase(RESOURCE_TYPE_POD_INGRESS_RULE_BACKEND_EN, p))
}
