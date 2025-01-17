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

package ingester

import (
	"fmt"
	"strconv"

	"github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/ingester/config"
	"github.com/deepflowio/deepflow/server/ingester/ingesterctl"
	"github.com/deepflowio/deepflow/server/ingester/prometheus/prometheus"
	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/debug"
	"github.com/deepflowio/deepflow/server/libs/nativetag"
)

var CleanDatabaseList = []string{
	"application_log", "deepflow_admin", "deepflow_tenant", "event", "ext_metrics",
	"flow_log", "flow_metrics", "flow_tag",
	"profile", "prometheus"}

type OrgHandler struct {
	cfg        *config.Config
	promHander *prometheus.PrometheusHandler
}

func NewOrgHandler(cfg *config.Config) *OrgHandler {
	o := &OrgHandler{
		cfg: cfg,
	}
	debug.ServerRegisterSimple(ingesterctl.CMD_ORG_SWITCH, o)
	return o
}

func (o *OrgHandler) HandleSimpleCommand(operate uint16, arg string) string {
	orgId, err := strconv.Atoi(arg)
	if err != nil || !ckdb.IsValidOrgID(uint16(orgId)) {
		debug.SetOrgId(ckdb.DEFAULT_ORG_ID)
		return "set org: default"
	}
	debug.SetOrgId(orgId)
	return "set org: " + arg
}

func (o *OrgHandler) SetPromHandler(promHandler *prometheus.PrometheusHandler) {
	o.promHander = promHandler
}

func (o *OrgHandler) DropOrg(orgId uint16) error {
	log.Info("drop org id:", orgId)
	o.dropOrgCaches(orgId)
	return o.dropOrgDatabase(orgId)
}

// FIXME: After clearing the Org data, if the same Org ID is created again later, data writing will fail. You can restart deepflow-server to solve it.
func (o *OrgHandler) dropOrgDatabase(orgId uint16) error {
	if ckdb.IsDefaultOrgID(orgId) {
		return fmt.Errorf("can not drop default org id: %d", orgId)
	}
	conns, err := common.NewCKConnections(*o.cfg.CKDB.ActualAddrs, o.cfg.CKDBAuth.Username, o.cfg.CKDBAuth.Password)
	if err != nil {
		return err
	}
	defer conns.Close()

	for _, db := range CleanDatabaseList {
		sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", ckdb.OrgDatabasePrefix(orgId)+db)
		_, err := conns.ExecParallel(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *OrgHandler) dropOrgCaches(orgId uint16) {
	if o.promHander == nil {
		return
	}
	o.promHander.DropOrg(orgId)
}

func (o *OrgHandler) UpdateNativeTag(orgId uint16, table nativetag.NativeTagTable, nativeTag *nativetag.NativeTag) error {
	if nativeTag == nil {
		return nil
	}

	conns, err := common.NewCKConnections(*o.cfg.CKDB.ActualAddrs, o.cfg.CKDBAuth.Username, o.cfg.CKDBAuth.Password)
	if err != nil {
		log.Error(err)
		return err
	}
	defer conns.Close()

	for _, conn := range conns {
		err := nativetag.CKAddNativeTag(o.cfg.CKDB.Type == ckdb.CKDBTypeByconity, conn, orgId, table, nativeTag)
		if err != nil {
			log.Error(err)
			return err
		}
	}

	droppedNativeTag := nativetag.GetDroppedNativeTag(orgId, table, nativeTag)
	// the drop operation is time-consuming and should be handled asynchronously
	go o.dropNativeTag(orgId, table, droppedNativeTag)
	return nil
}

func (o *OrgHandler) dropNativeTag(orgId uint16, table nativetag.NativeTagTable, nativeTag *nativetag.NativeTag) error {
	if nativeTag == nil || len(nativeTag.ColumnNames) == 0 {
		return nil
	}
	conns, err := common.NewCKConnections(*o.cfg.CKDB.ActualAddrs, o.cfg.CKDBAuth.Username, o.cfg.CKDBAuth.Password)
	if err != nil {
		log.Error(err)
		return err
	}
	defer conns.Close()
	for _, conn := range conns {
		err := nativetag.CKDropNativeTag(o.cfg.CKDB.Type == ckdb.CKDBTypeByconity, conn, orgId, table, nativeTag)
		if err != nil {
			log.Error(err)
		}
	}
	return nil
}
