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

package rebalance

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"

	"github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/db/metadb"
	metadbmodel "github.com/deepflowio/deepflow/server/controller/db/metadb/model"
	"github.com/deepflowio/deepflow/server/controller/grpc/statsd"
	"github.com/deepflowio/deepflow/server/controller/model"
	"github.com/deepflowio/deepflow/server/libs/logger"
	"github.com/deepflowio/deepflow/server/querier/config"
)

var log = logger.MustGetLogger("service.rebalance")

func (r *AnalyzerInfo) RebalanceAnalyzerByTraffic(db *metadb.DB, ifCheckout bool, dataDuration int) (*model.VTapRebalanceResult, error) {
	if err := r.generateRebalanceData(db, dataDuration); err != nil {
		return nil, err
	}

	response := &model.VTapRebalanceResult{}
	for _, az := range r.dbInfo.AZs {
		azVTaps, ok := r.AZToVTaps[az.Lcuuid]
		if !ok {
			continue
		}
		azAnalyzers, ok := r.AZToAnalyzers[az.Lcuuid]
		if !ok {
			continue
		}
		vtapNameToID := make(map[string]int, len(azVTaps))
		vTapIDToVTap := make(map[int]*metadbmodel.VTap, len(azVTaps))
		vTapIDToTraffic := make(map[int]int64)
		for _, vtap := range azVTaps {
			vtapNameToID[vtap.Name] = vtap.ID
			vTapIDToTraffic[vtap.ID] = 0
			vTapIDToVTap[vtap.ID] = vtap
		}
		for vtapName, traffic := range r.RegionToVTapNameToTraffic[az.Region] {
			vtapID, ok := vtapNameToID[vtapName]
			if !ok {
				continue
			}
			vTapIDToTraffic[vtapID] = traffic
		}
		if len(vTapIDToTraffic) == 0 {
			log.Warningf("no vtaps to balance, region(%s)", az.Region, db.LogPrefixORGID, db.LogPrefixName)
			continue
		}
		p := &AZInfo{
			lcuuid:          az.Lcuuid,
			vTapIDToTraffic: vTapIDToTraffic,
			vtapIDToVTap:    vTapIDToVTap,
			analyzers:       azAnalyzers,
		}
		vTapIDToChangeInfo, azVTapRebalanceResult := p.rebalanceAnalyzer(db, ifCheckout)
		if azVTapRebalanceResult != nil {
			response.TotalSwitchVTapNum += azVTapRebalanceResult.TotalSwitchVTapNum
			response.Details = append(response.Details, azVTapRebalanceResult.Details...)
		}
		if !r.onlyWeight && azVTapRebalanceResult != nil &&
			azVTapRebalanceResult.TotalSwitchVTapNum != 0 {
			for vtapID, changeInfo := range vTapIDToChangeInfo {
				if !ifCheckout && changeInfo.OldIP != changeInfo.NewIP {
					var vtapName string
					if vtap, ok := vTapIDToVTap[vtapID]; ok {
						vtapName = vtap.Name
					}
					log.Infof("az(%s) vtap(%v) analyzer ip changed: %s -> %s",
						az.Lcuuid, vtapName, changeInfo.OldIP, changeInfo.NewIP, db.LogPrefixORGID, db.LogPrefixName)
				}
			}
			for _, detail := range azVTapRebalanceResult.Details {
				log.Infof("analyzer rebalance result az(%v) ip(%v) state(%v) before_vtap_num(%v) after_vtap_num(%v), "+
					"switch_vtap_num(%v) before_vtap_weight(%v) after_vtap_weight(%v)",
					detail.AZ, detail.IP, detail.State, detail.BeforeVTapNum, detail.AfterVTapNum,
					detail.SwitchVTapNum, detail.BeforeVTapWeights, detail.AfterVTapWeights, db.LogPrefixORGID, db.LogPrefixName)
				log.Infof("analyzer rebalance result az(%v) ip(%v) before vtap traffic(%v), after vtap traffic(%v)",
					detail.AZ, detail.IP, detail.BeforeVTapTraffic, detail.AfterVTapTraffic, db.LogPrefixORGID, db.LogPrefixName)
				if len(detail.NewVTapToTraffic) > 0 {
					b, _ := json.Marshal(detail.NewVTapToTraffic)
					log.Infof("analyzer rebalance result az(%v) ip(%v) vtap(to add) name to traffic: %s",
						detail.AZ, detail.IP, string(b), db.LogPrefixORGID, db.LogPrefixName)
				}
				if len(detail.DelVTapToTraffic) > 0 {
					b, _ := json.Marshal(detail.DelVTapToTraffic)
					log.Infof("analyzer rebalance result az(%v) ip(%v) vtap(to delete) name to traffic: %s",
						detail.AZ, detail.IP, string(b), db.LogPrefixORGID, db.LogPrefixName)
				}

			}
		}

		r.updateCounter(db, vTapIDToVTap, vtapNameToID, vTapIDToChangeInfo)
	}

	allVTapNameToID := make(map[string]int, len(r.dbInfo.VTaps))
	allVTapIDToVTap := make(map[int]*metadbmodel.VTap, len(r.dbInfo.VTaps))
	for _, vtap := range r.dbInfo.VTaps {
		allVTapNameToID[vtap.Name] = vtap.ID
		allVTapIDToVTap[vtap.ID] = &vtap
	}
	vtapCounter := statsd.GetVTapCounter()
	for name := range vtapCounter.GetVtapNameCounter(db.ORGID) {
		vtapID, ok := allVTapNameToID[name]
		// set weight to 0 if vtap losed
		if !ok {
			vtapCounter.SetNull(db.ORGID, name)
			log.Infof("agent(name: %s) set null when agent losed", name, db.LogPrefixORGID)
			continue
		}
		// set weight to 0 if vtap not normal
		if vtap, ok := allVTapIDToVTap[vtapID]; ok && vtap.State != common.VTAP_STATE_NORMAL {
			vtapCounter.SetNull(db.ORGID, name)
			log.Infof("agent(name: %s) set null when agent not normal", name, db.LogPrefixORGID)
			continue
		}
	}

	if !ifCheckout && response.TotalSwitchVTapNum != 0 {
		log.Infof("analyzer rebalance vtap switch_total_num(%v)", response.TotalSwitchVTapNum, db.LogPrefixORGID, db.LogPrefixName)
	}

	return response, nil
}

func (r *AnalyzerInfo) RebalanceAnalyzerByTrafficDebug(db *metadb.DB, dataDuration int) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	if err := r.generateRebalanceData(db, dataDuration); err != nil {
		return nil, err
	}

	data = r.trafficAZDebug(data)
	data = r.trafficAnalyzerDebug(db.ORGID, data)
	return data, nil
}

func (r *AnalyzerInfo) trafficAZDebug(data map[string]interface{}) map[string]interface{} {
	var trafficResult []map[string]interface{}
	regionToName := make(map[string]string, len(r.dbInfo.Regions))
	for _, region := range r.dbInfo.Regions {
		regionToName[region.Lcuuid] = region.Name
	}
	for _, az := range r.dbInfo.AZs {
		azVTaps, ok := r.AZToVTaps[az.Lcuuid]
		if !ok {
			continue
		}
		azAnalyzers, ok := r.AZToAnalyzers[az.Lcuuid]
		if !ok {
			continue
		}
		analyzerIPToVTaps := make(map[string][]metadbmodel.VTap, len(azAnalyzers))
		for _, vtap := range azVTaps {
			analyzerIPToVTaps[vtap.AnalyzerIP] = append(analyzerIPToVTaps[vtap.AnalyzerIP], *vtap)
		}
		vTapNameToTraffic := make(map[string]int64)
		for vtapName, traffic := range r.RegionToVTapNameToTraffic[az.Region] {
			vTapNameToTraffic[vtapName] = traffic
		}

		results := make([]map[string]interface{}, len(azAnalyzers))
		azTraffic := int64(0)
		for i, analyzer := range azAnalyzers {
			results[i] = map[string]interface{}{
				"REGION":         fmt.Sprintf("%s(%s)", az.Region, regionToName[az.Region]),
				"AZ":             fmt.Sprintf("%s(%s)", az.Lcuuid, az.Name),
				"ANALYZER_IP":    analyzer.IP,
				"ANALYZER_STATE": analyzer.State,
				"AGENT_COUNT":    len(analyzerIPToVTaps[analyzer.IP]),
			}
			trafficData := int64(0)
			for _, vtap := range analyzerIPToVTaps[analyzer.IP] {
				trafficData += vTapNameToTraffic[vtap.Name]
				azTraffic += vTapNameToTraffic[vtap.Name]
			}
			results[i]["ANALYZER_TRAFFIC"] = trafficData
		}
		for _, result := range results {
			result["AZ_TRAFFIC"] = azTraffic
		}
		trafficResult = append(trafficResult, results...)
	}
	data["TRAFFIC_AZ"] = trafficResult

	return data
}

func (r *AnalyzerInfo) trafficAnalyzerDebug(orgID int, data map[string]interface{}) map[string]interface{} {
	regionToName := make(map[string]string, len(r.dbInfo.Regions))
	for _, region := range r.dbInfo.Regions {
		regionToName[region.Lcuuid] = region.Name
	}
	lcuuidToAz := make(map[string]*metadbmodel.AZ)
	for i, az := range r.dbInfo.AZs {
		lcuuidToAz[az.Lcuuid] = &r.dbInfo.AZs[i]
	}
	ipToAzAnalyzerCon := make(map[string][]*metadbmodel.AZAnalyzerConnection)
	for i, conn := range r.dbInfo.AZAnalyzerConns {
		ipToAzAnalyzerCon[conn.AnalyzerIP] = append(
			ipToAzAnalyzerCon[conn.AnalyzerIP],
			&r.dbInfo.AZAnalyzerConns[i],
		)
	}
	analyzerIPToVTaps := make(map[string][]metadbmodel.VTap, len(r.dbInfo.VTaps))
	for _, vtap := range r.dbInfo.VTaps {
		analyzerIPToVTaps[vtap.AnalyzerIP] = append(analyzerIPToVTaps[vtap.AnalyzerIP], vtap)
	}
	vtapNameToTraffic := make(map[string]int64)
	for _, item := range r.RegionToVTapNameToTraffic {
		for vtapName, traffic := range item {
			vtapNameToTraffic[vtapName] = traffic
		}
	}

	results := make([]map[string]interface{}, len(r.dbInfo.Analyzers))
	for i, analyzer := range r.dbInfo.Analyzers {
		results[i] = map[string]interface{}{
			"ANALYZER_IP":    analyzer.IP,
			"ANALYZER_STATE": analyzer.State,
			"AGENT_COUNT":    len(analyzerIPToVTaps[analyzer.IP]),
		}

		trafficData := int64(0)
		for _, vtap := range analyzerIPToVTaps[analyzer.IP] {
			trafficData += vtapNameToTraffic[vtap.Name]
		}
		results[i]["ANALYZER_TRAFFIC"] = trafficData

		azConns, ok := ipToAzAnalyzerCon[analyzer.IP]
		if ok && len(azConns) > 0 {
			if regionName, ok := regionToName[azConns[0].Region]; ok {
				results[i]["REGION"] = fmt.Sprintf("%s(%s)", azConns[0].Region, regionName)
			}
		}
		var azStr string
		for _, azConn := range azConns {
			if azConn.AZ == "ALL" {
				azStr = "ALL"
				break
			} else {
				if azStr == "" {
					azStr = fmt.Sprintf("%s(%s)", azConn.AZ, lcuuidToAz[azConn.AZ].Name)
				} else {
					azStr += fmt.Sprintf(", %s(%s)", azConn.AZ, lcuuidToAz[azConn.AZ].Name)
				}
			}
		}
		results[i]["AZ"] = azStr
	}
	data["TRAFFIC_ANALYZER"] = results
	return data
}

func (r *AnalyzerInfo) generateRebalanceData(db *metadb.DB, dataDuration int) error {
	// In automatic balancing, data is not obtained when ifCheckout = false.
	if len(r.dbInfo.Analyzers) == 0 {
		err := r.dbInfo.Get(db)
		if err != nil {
			return err
		}
	}
	info := r.dbInfo
	if len(info.VTaps) == 0 || len(info.Analyzers) == 0 {
		return nil
	}

	r.RegionToAZLcuuids = make(map[string][]string)
	r.AZToRegion = make(map[string]string, len(info.AZs))
	for _, az := range info.AZs {
		r.RegionToAZLcuuids[az.Region] = append(r.RegionToAZLcuuids[az.Region], az.Lcuuid)
		r.AZToRegion[az.Lcuuid] = az.Region
	}
	r.AZToVTaps = make(map[string][]*metadbmodel.VTap)
	for i, vtap := range info.VTaps {
		r.AZToVTaps[vtap.AZ] = append(r.AZToVTaps[vtap.AZ], &info.VTaps[i])
	}
	ipToAnalyzer := make(map[string]*metadbmodel.Analyzer)
	for i, analyzer := range info.Analyzers {
		ipToAnalyzer[analyzer.IP] = &info.Analyzers[i]
	}
	r.AZToAnalyzers = GetAZToAnalyzers(info.AZAnalyzerConns, r.RegionToAZLcuuids, ipToAnalyzer)

	if r.RegionToVTapNameToTraffic == nil {
		regionToVTapNameToTraffic, err := r.getVTapTraffic(db, dataDuration, r.RegionToAZLcuuids)
		if err != nil {
			return fmt.Errorf("get traffic data failed: %v", err)
		}
		for region, vtapNameToTraffic := range r.RegionToVTapNameToTraffic {
			log.Infof("region(%s) agent traffic: %#v", region, vtapNameToTraffic, db.LogPrefixORGID, db.LogPrefixName)
		}
		r.RegionToVTapNameToTraffic = regionToVTapNameToTraffic
	}
	r.dbInfo.AZs = r.dbInfo.AZs
	return nil
}

type AZInfo struct {
	lcuuid          string
	vTapIDToTraffic map[int]int64
	vtapIDToVTap    map[int]*metadbmodel.VTap
	analyzers       []*metadbmodel.Analyzer
}

type ChangeInfo struct {
	OldIP     string
	NewIP     string
	NewWeight float64
}

// rebalanceAnalyzer balances vtaps on the analyzer in an az
// 1. calculate the total traffic data sent by vtaps
//   - beforeTraffic is used to record the total traffic before rebalance and calculate before weight
//   - afterTraffic is used to record the total traffic after rebalance and calculate after weight, contains newly added vtap
//     (traffic is 0, virtual traffic is allocated, virtual traffic = sum of traffic of allocated vtap / number of allocated vtap)
//   - add the newly added vtap to the queue to be reallocated
//
// 2. based on the total traffic, calculate the average amount of data received on each analyzer: avg = afterTraffic / total number of normal agents
// 3. range vtaps assigned analyzer
//   - update the total traffic on the analyzer: cur = cur + vtap_traffic
//
// 4. range analyzers
//   - analyzer exception: add all vtaps on the exception analyzer to the queue to be reallocated
//   - analyzer normal
//     range vtaps on the analyzer
//     cur <= avg, continue
//     cur > avg, if agent_num = 1, remain unchanged, if agent_num > 1, add the vtap with the smallest traffic to the queue to be reallocated, and re-traverse the vtap until the conditions are not met
//
// 5. the vtap queues to be reallocated are arranged from large to small traffic volume, and range the queues
//   - range the analyzers and select the analyzer with the smallest traffic to assign to vtap
//   - until the allocation ends
//
// 6. update analyzer weights
//   - vtap weight = traffic sent by vtap / traffic sent by all vtap
//   - average weight of analyzer = sum of vtap weights / number of normal analyzers
//   - analyzer weight = sum of vtap weights on anlyzer / average weight of analyzer
//
// rebalanceAnalyzer 函数用于平衡一个可用区中数据节点上的 vtaps
// 1、计算采集器发送的总流量 traffic
//   - beforeTraffic 记录平衡前的总流量，计算平衡前的权重
//   - afterTraffic 记录平衡后的总流量，计算平衡后的权重，包含新增的采集器（流量为 0，分配虚拟流量值，虚拟流量 = 已分配采集器的流量总和 / 已分配采集器总和）
//   - 将新增的采集器加入待重新分配队列
//
// 2、根据总流量，计算每个 analyzer 上平均接收到的流量：avg = afterTraffic / 正常 analyzer 的总数
// 3、遍历已分配数据节点的采集器
//   - 更新数据节点上的总流量: cur = cur + vtap_traffic
//
// 4、遍历数据节点
//   - 数据节点异常：将异常数据节点上的所有采集器加入待重新分配队列
//   - 数据节点正常
//     遍历数据节点上的采集器
//     cur <= avg，跳过
//     cur > avg，如果 agent_num = 1，保持不变；如果 agent_num > 1，将流量最小的采集器加入待重新分配队列，重新遍历采集器直到不满足条件
//
// 5、待重新分配采集器队列按照流量从大到小排列，遍历队列
//   - 遍历数据节点，选择流量最小的数据节点分配给采集器
//   - 直到分配结束
//
// 6、更新数据节点权重
//   - 采集器权重 = 采集器发送流量 / 所有采集器发送的流量
//   - 数据节点的平均权重 = 采集器权重之和 / 正常数据节点个数
//   - 数据节点权重 = 数据节点上的采集器权重之和 / 数据节点的平均权重
func (p *AZInfo) rebalanceAnalyzer(db *metadb.DB, ifCheckout bool) (map[int]*ChangeInfo, *model.AZVTapRebalanceResult) {
	var beforeTraffic, afterTraffic int64
	for _, dataSize := range p.vTapIDToTraffic {
		afterTraffic += dataSize
	}
	beforeTraffic = afterTraffic
	var vtapWithAnalyzerSum int
	for _, vtap := range p.vtapIDToVTap {
		if vtap.AnalyzerIP != "" {
			vtapWithAnalyzerSum++
		}
	}
	var agentUnassignedTraffic int64
	if vtapWithAnalyzerSum != 0 {
		agentUnassignedTraffic = afterTraffic / int64(vtapWithAnalyzerSum)
	}
	// all vtap news, add virtual traffic
	if afterTraffic == 0 {
		agentUnassignedTraffic = 100
	}
	for id := range p.vTapIDToTraffic {
		if p.vTapIDToTraffic[id] == 0 {
			p.vTapIDToTraffic[id] = agentUnassignedTraffic
			afterTraffic += p.vTapIDToTraffic[id]
		}
	}

	type VTapInfo struct {
		VtapID  int
		Traffic int64
	}
	type Info struct {
		State        int
		SumTraffic   int64
		AfterVTapNum int
		VTapInfos    []VTapInfo
	}

	var completeAnalyzerNum int
	azVTapRebalanceResult := &model.AZVTapRebalanceResult{}
	analyzerIPToInfo := make(map[string]*Info, len(p.analyzers))
	for _, analyzer := range p.analyzers {
		if analyzer.State == common.HOST_STATE_COMPLETE {
			completeAnalyzerNum++
		}
		detail := &model.HostVTapRebalanceResult{
			IP:               analyzer.IP,
			AZ:               p.lcuuid,
			State:            analyzer.State,
			NewVTapToTraffic: make(map[string]int64),
			DelVTapToTraffic: make(map[string]int64),
		}
		analyzerIPToInfo[analyzer.IP] = &Info{State: analyzer.State}
		azVTapRebalanceResult.Details = append(azVTapRebalanceResult.Details, detail)
	}

	var allocVTaps []VTapInfo
	vTapIDToChangeInfo := make(map[int]*ChangeInfo, len(p.vtapIDToVTap))
	if len(p.vtapIDToVTap) == 0 {
		log.Warningf("no vtaps to alloc analyzer", db.LogPrefixORGID, db.LogPrefixName)
		return nil, nil
	}
	vtapaAerageTraffic := float64(afterTraffic) / float64(len(p.vtapIDToVTap))
	for _, vtap := range p.vtapIDToVTap {
		w, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(p.vTapIDToTraffic[vtap.ID])/vtapaAerageTraffic), 64)
		if vtap.AnalyzerIP == "" {
			vTapIDToChangeInfo[vtap.ID] = &ChangeInfo{OldIP: "", NewWeight: w}
			allocVTaps = append(allocVTaps, VTapInfo{VtapID: vtap.ID, Traffic: p.vTapIDToTraffic[vtap.ID]})
			continue
		}
		vTapIDToChangeInfo[vtap.ID] = &ChangeInfo{OldIP: vtap.AnalyzerIP, NewIP: vtap.AnalyzerIP, NewWeight: w}

		// the analyzer ip in getting vtap traffic data is not in the analyzer table
		if _, ok := analyzerIPToInfo[vtap.AnalyzerIP]; !ok {
			allocVTaps = append(allocVTaps, VTapInfo{VtapID: vtap.ID, Traffic: p.vTapIDToTraffic[vtap.ID]})
			log.Infof("vtap(%v) analyzer ip(%v) is not in analyzer table",
				vtap.Name, vtap.AnalyzerIP, db.LogPrefixORGID, db.LogPrefixName)
			continue
		}
		analyzerIPToInfo[vtap.AnalyzerIP].SumTraffic += p.vTapIDToTraffic[vtap.ID]
		analyzerIPToInfo[vtap.AnalyzerIP].AfterVTapNum++ // hold old vtap num
		analyzerIPToInfo[vtap.AnalyzerIP].VTapInfos = append(analyzerIPToInfo[vtap.AnalyzerIP].VTapInfos,
			VTapInfo{VtapID: vtap.ID, Traffic: p.vTapIDToTraffic[vtap.ID]},
		)

		// beforeWeight counts the actual allocated vtap weight before balancing
		beforeWeight, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(p.vTapIDToTraffic[vtap.ID])/float64(beforeTraffic)), 64)
		if beforeTraffic == 0 {
			beforeWeight = 0
		}
		for _, detail := range azVTapRebalanceResult.Details {
			if detail.IP != vtap.AnalyzerIP {
				continue
			}
			detail.BeforeVTapNum++
			detail.AfterVTapNum = detail.BeforeVTapNum
			detail.BeforeVTapWeights += beforeWeight
			detail.BeforeVTapTraffic += p.vTapIDToTraffic[vtap.ID]
		}
	}

	if completeAnalyzerNum == 0 {
		log.Warningf("no complete analyzer to rebalance vtaps, az(%v)", p.lcuuid)
		return nil, nil
	}
	avg := float64(afterTraffic) / float64(completeAnalyzerNum)
	// adjust over-allocated agents on analyzer
	for ip, info := range analyzerIPToInfo {
		if info.State == common.HOST_STATE_COMPLETE && (float64(info.SumTraffic) <= avg || len(info.VTapInfos) == 1) {
			continue
		}

		// analyzer_sum_data_size > avg && vtap_num > 1
		sort.Slice(info.VTapInfos, func(i, j int) bool {
			return info.VTapInfos[i].Traffic < info.VTapInfos[j].Traffic
		})
		for i := 0; i < len(info.VTapInfos); i++ {
			if info.State == common.HOST_STATE_COMPLETE && (float64(info.SumTraffic) <= avg || i == len(info.VTapInfos)-1) {
				break
			}
			allocVTaps = append(allocVTaps, info.VTapInfos[i])
			info.SumTraffic -= info.VTapInfos[i].Traffic
			analyzerIPToInfo[ip].AfterVTapNum--
			for _, detail := range azVTapRebalanceResult.Details {
				if ip != detail.IP {
					continue
				}
				if vtap, ok := p.vtapIDToVTap[info.VTapInfos[i].VtapID]; ok {
					detail.DelVTapToTraffic[vtap.Name] = info.VTapInfos[i].Traffic
				}
			}
		}
	}

	sort.Slice(allocVTaps, func(i, j int) bool {
		return allocVTaps[i].Traffic > allocVTaps[j].Traffic
	})
	for _, allocVTap := range allocVTaps {
		var minAnalyzerDataSize int64
		var allocIP string
		for ip, info := range analyzerIPToInfo {
			if info.State != common.HOST_STATE_COMPLETE {
				continue
			}
			minAnalyzerDataSize = info.SumTraffic
			allocIP = ip
			break
		}
		for ip, info := range analyzerIPToInfo {
			if info.State != common.HOST_STATE_COMPLETE {
				continue
			}
			if info.SumTraffic < minAnalyzerDataSize {
				minAnalyzerDataSize = info.SumTraffic
				allocIP = ip
			}
		}
		if !ifCheckout {
			db.Model(metadbmodel.VTap{}).Where("id = ?", allocVTap.VtapID).Update("analyzer_ip", allocIP)
		}
		if _, ok := analyzerIPToInfo[allocIP]; !ok {
			log.Warningf("allocate vtap(%d) failed, wanted analyzer ip(%s)", allocVTap.VtapID, allocIP, db.LogPrefixORGID, db.LogPrefixName)
			continue
		}
		analyzerIPToInfo[allocIP].SumTraffic += allocVTap.Traffic
		analyzerIPToInfo[allocIP].AfterVTapNum++
		vTapIDToChangeInfo[allocVTap.VtapID].NewIP = allocIP
		for _, detail := range azVTapRebalanceResult.Details {
			if allocIP != detail.IP {
				continue
			}
			if vtap, ok := p.vtapIDToVTap[allocVTap.VtapID]; ok {
				detail.NewVTapToTraffic[vtap.Name] += allocVTap.Traffic
			}
		}
	}

	var totalSwitchVTapNum int
	var beforeWeight, afterWeight float64
	for _, detail := range azVTapRebalanceResult.Details {
		info, ok := analyzerIPToInfo[detail.IP]
		if !ok {
			log.Errorf("can not find response data(analyzer ip: %s)", detail.IP, db.LogPrefixORGID, db.LogPrefixName)
			continue
		}
		detail.AfterVTapNum = info.AfterVTapNum
		detail.SwitchVTapNum = int(math.Abs(float64(detail.AfterVTapNum - detail.BeforeVTapNum)))
		w, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(info.SumTraffic)/float64(afterTraffic)), 64)
		detail.AfterVTapWeights = w
		detail.AfterVTapTraffic = info.SumTraffic
		w, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", detail.BeforeVTapWeights), 64)
		detail.BeforeVTapWeights = w
		totalSwitchVTapNum += detail.SwitchVTapNum
		beforeWeight += detail.BeforeVTapWeights
		afterWeight += detail.AfterVTapWeights
	}
	azVTapRebalanceResult.TotalSwitchVTapNum = totalSwitchVTapNum / 2

	avgBeforeWeight := beforeWeight / float64(completeAnalyzerNum)
	avgAfterWeight := afterWeight / float64(completeAnalyzerNum)
	for _, detail := range azVTapRebalanceResult.Details {
		if avgBeforeWeight != 0 {
			w, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", detail.BeforeVTapWeights/avgBeforeWeight), 64)
			detail.BeforeVTapWeights = w
		}
		if avgAfterWeight != 0 {
			w, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", detail.AfterVTapWeights/avgAfterWeight), 64)
			detail.AfterVTapWeights = w
		}
	}
	return vTapIDToChangeInfo, azVTapRebalanceResult
}

func (r *AnalyzerInfo) getVTapTraffic(db *metadb.DB, dataDuration int, regionToAZLcuuids map[string][]string) (map[string]map[string]int64, error) {
	ipToController := make(map[string]*metadbmodel.Controller)
	for i, controller := range r.dbInfo.Controllers {
		ipToController[controller.IP] = &r.dbInfo.Controllers[i]
	}

	regionToRegionDomainPrefix := make(map[string]string)
	for _, conn := range r.dbInfo.AZControllerConns {
		if _, ok := regionToRegionDomainPrefix[conn.Region]; ok {
			continue
		}
		if controller, ok := ipToController[conn.ControllerIP]; ok {
			regionToRegionDomainPrefix[conn.Region] = controller.RegionDomainPrefix
		}
	}
	regionToVTapNameToTraffic := make(map[string]map[string]int64)
	for region, domainPrefix := range regionToRegionDomainPrefix {
		vtapNameToTraffic, err := r.query.GetAgentDispatcher(db, domainPrefix, dataDuration)
		if err != nil {
			log.Errorf("get query data failed, region(%s), err: %s", region, err, db.LogPrefixORGID, db.LogPrefixName)
			continue
		}

		if _, ok := regionToVTapNameToTraffic[region]; !ok {
			regionToVTapNameToTraffic[region] = make(map[string]int64)
		}
		regionToVTapNameToTraffic[region] = vtapNameToTraffic
	}
	return regionToVTapNameToTraffic, nil
}

type Query struct {
	onlyWeight bool
}

func (q *Query) GetAgentDispatcher(orgDB *metadb.DB, domainPrefix string, dataDuration int) (map[string]int64, error) {
	if domainPrefix == "master-" {
		domainPrefix = ""
	}
	queryURL := fmt.Sprintf("http://%sdeepflow-server:%d/v1/query", domainPrefix, config.Cfg.ListenPort)
	values := url.Values{}
	db := "deepflow_tenant"
	now := time.Now()
	before := now.UTC().Add(time.Second * -1 * time.Duration(dataDuration))
	sql := fmt.Sprintf("SELECT `tag.host`, Sum(`metrics.tx_bytes`) AS `tx-bps` FROM deepflow_agent_collect_sender"+
		" WHERE `time`>%d AND `time`<%d GROUP BY tag.host", before.Unix(), now.Unix())
	values.Add("db", db)
	values.Add("sql", sql)

	t := time.Now()
	req, err := http.NewRequest("POST", queryURL, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(common.HEADER_KEY_X_ORG_ID, strconv.Itoa(orgDB.ORGID))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("curl (%s) failed, db (%s), sql: %s, err: %s", queryURL, db, sql, err)
	}
	defer resp.Body.Close()
	if !q.onlyWeight {
		log.Infof("curl(%s) query data time since(%v), db(%s) sql(%s)",
			queryURL, time.Since(t), db, sql, orgDB.LogPrefixORGID, orgDB.LogPrefixName)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, err
	}

	vtapNameToDataSize, err := parseBody(body)
	if err != nil {
		return nil, fmt.Errorf("parse response data failed, data: %s, err: %s", string(body), err)
	}
	return vtapNameToDataSize, nil
}

// column name
const (
	tagHost = "tag.host"
	txBPS   = "tx-bps"
)

// parseBody parse query api response body to vtapNameToTraffic(map[string]int64).
func parseBody(data []byte) (map[string]int64, error) {
	respJson, err := simplejson.NewJson(data)
	if err != nil {
		return nil, err
	}
	optStatus := respJson.Get("OPT_STATUS").MustString()
	if optStatus != "" && optStatus != "SUCCESS" {
		description := respJson.Get("DESCRIPTION").MustString()
		return nil, errors.New(description)
	}

	result := respJson.Get("result")
	columns := result.Get("columns")
	values := result.Get("values")
	vtapNameToTraffic := make(map[string]int64)
	for i := range values.MustArray() {
		value := values.GetIndex(i)
		if columns.GetIndex(0).MustString() == tagHost {
			vtapNameToTraffic[value.GetIndex(0).MustString()] = int64(value.GetIndex(1).MustFloat64())
		} else {
			vtapNameToTraffic[value.GetIndex(1).MustString()] = int64(value.GetIndex(0).MustFloat64())
		}
	}
	return vtapNameToTraffic, nil
}

func (r *AnalyzerInfo) updateCounter(db *metadb.DB, vtapIDToVTap map[int]*metadbmodel.VTap, vtapNameToID map[string]int, vtapIDToChangeInfo map[int]*ChangeInfo) {
	vtapCounter := statsd.GetVTapCounter()
	for vtapID, changeInfo := range vtapIDToChangeInfo {
		vtap, ok := vtapIDToVTap[vtapID]
		if !ok {
			log.Info("vtap(%d) not found, change info: %#v", vtapID, changeInfo, db.LogPrefixORGID, db.LogPrefixName)
			continue
		}
		isAnalyzerChanged := uint64(0)
		if !r.onlyWeight {
			if changeInfo.OldIP != changeInfo.NewIP {
				isAnalyzerChanged = uint64(1)
			}
		}
		vtapCounter.SetCounter(db, vtap.TeamID, vtap.Name, changeInfo.NewWeight, isAnalyzerChanged)
	}
}
