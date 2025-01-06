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

package common

import (
	"bytes"
	"compress/zlib"
	"crypto/tls"
	. "encoding/binary"
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	"inet.af/netaddr"

	"github.com/deepflowio/deepflow/message/agent"
	"github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/db/metadb"
	metadbmodel "github.com/deepflowio/deepflow/server/controller/db/metadb/model"
	"github.com/deepflowio/deepflow/server/libs/logger"
)

var log = logger.MustGetLogger("genesis.common")

type TeamInfo struct {
	OrgID  int
	TeamId int
}

type VifInfo struct {
	MaskLen uint32
	Address string
	Scope   string
}

type Iface struct {
	Index     int
	PeerIndex int
	MAC       string
	Name      string
	Peer      string
	IPs       []VifInfo
}

type KVMUser struct {
	UUID  string `xml:"uuid,attr"`
	Value string `xml:",innerxml"`
}

type KVMProject struct {
	UUID  string `xml:"uuid,attr"`
	Value string `xml:",innerxml"`
}

type KVMOwner struct {
	User    KVMUser    `xml:"user"`
	Project KVMProject `xml:"project"`
}
type KVMInstance struct {
	Namespace string   `xml:"xmlns nova,attr"`
	Name      string   `xml:"name"`
	Owner     KVMOwner `xml:"owner"`
}
type KVMMetaData struct {
	Instance KVMInstance `xml:"instance"`
}

type KVMMac struct {
	Value string `xml:"address,attr"`
}

type KVMSource struct {
	Value string `xml:"bridge,attr"`
}

type KVMTarget struct {
	Value string `xml:"dev,attr"`
}

type KVMModel struct {
	Value string `xml:"type,attr"`
}

type KVMAddress struct {
	AddressValue  string `xml:"type,attr"`
	DomainValue   string `xml:"domain,attr"`
	BusValue      string `xml:"bus,attr"`
	SlotValue     string `xml:"slot,attr"`
	FunctionValue string `xml:"function,attr"`
}

type KVMInterface struct {
	Type    string     `xml:"type,attr"`
	Mac     KVMMac     `xml:"mac"`
	Source  KVMSource  `xml:"source"`
	Target  KVMTarget  `xml:"target"`
	Model   KVMModel   `xml:"model"`
	Address KVMAddress `xml:"address"`
}

type KVMDevices struct {
	Interfaces []KVMInterface `xml:"interface"`
}

type KVMDomain struct {
	Type     string      `xml:"type,attr"`
	UUID     string      `xml:"uuid"`
	Name     string      `xml:"name"`
	Title    string      `xml:"title"`
	MetaData KVMMetaData `xml:"metadata"`
	Devices  KVMDevices  `xml:"devices"`
}
type ETCDomains struct {
	Domains []KVMDomain `xml:"domain"`
}

type DomStatus struct {
	Domains KVMDomain `xml:"domain"`
}

type RUNDomains struct {
	DomStatus []DomStatus `xml:"domstatus"`
}

type XMLVPC struct {
	UUID string
	Name string
}

type XMLInterface struct {
	Mac    string
	Target string
}

type XMLVM struct {
	UUID       string
	Name       string
	Label      string
	VPC        XMLVPC
	Interfaces []XMLInterface
}

var IfaceRegex = regexp.MustCompile("^(\\d+):\\s+([^@:]+)(@.*)?\\:")
var MACRegex = regexp.MustCompile("^\\s+link/\\S+\\s+(([\\dA-Za-z]{2}:){5}[\\dA-Za-z]{2}) brd.*$")
var IPRegex = regexp.MustCompile("^\\s+inet6?\\s+([\\d\\.A-Za-z:]+)/(\\d+)\\s+.*scope\\s+(global|link|host)")

func ParseIPOutput(s string) ([]Iface, error) {
	ifaces := []Iface{}
	if s == "" {
		return ifaces, nil
	}
	iface := Iface{Index: -1}
	lines := strings.Split(s, "\n")
	if len(lines) == 1 && lines[0] == "" {
		return ifaces, nil
	}
	for _, line := range lines {
		ifaceMatched := IfaceRegex.FindStringSubmatch(line)
		if ifaceMatched != nil {
			if iface.Index != -1 {
				ifaces = append(ifaces, iface)
				iface = Iface{}
			}
			index, err := strconv.Atoi(ifaceMatched[1])
			if err != nil {
				return []Iface{}, err
			}
			iface.Index = index
			iface.Name = ifaceMatched[2]
			if ifaceMatched[3] != "" {
				iface.Peer = ifaceMatched[3][1:]
			}
			if iface.Peer != "" && strings.HasPrefix(iface.Peer, "if") {
				peerIndex, err := strconv.Atoi(iface.Peer[2:])
				if err != nil {
					return []Iface{}, err
				}
				iface.PeerIndex = peerIndex
			}
		}

		macMatched := MACRegex.FindStringSubmatch(line)
		if macMatched != nil {
			iface.MAC = macMatched[1]
		} else if strings.HasPrefix(iface.Name, "tunl0") {
			iface.MAC = "00:00:00:00:00:00"
		}
		ipMatched := IPRegex.FindStringSubmatch(line)
		if ipMatched != nil {
			maskLen, err := strconv.Atoi(ipMatched[2])
			if err != nil {
				return []Iface{}, err
			}
			iface.IPs = append(iface.IPs, VifInfo{
				Address: ipMatched[1],
				Scope:   ipMatched[3],
				MaskLen: uint32(maskLen),
			})
		}
	}
	ifaces = append(ifaces, iface)
	return ifaces, nil
}

func ParseCSV(s string, fields ...string) ([]map[string]string, error) {
	ret := []map[string]string{}
	if s == "" {
		return ret, nil
	}
	sReader := strings.NewReader(s)
	csvReader := csv.NewReader(sReader)
	csvs, err := csvReader.ReadAll()
	if err != nil {
		return ret, err
	}
	for _, c := range csvs[1:] {
		item := map[string]string{}
		for i, name := range csvs[0] {
			sort.Strings(fields)
			index := sort.SearchStrings(fields, name)
			if len(fields) > 0 && !(index < len(fields) && fields[index] == name) {
				continue
			}
			item[name] = c[i]
		}
		ret = append(ret, item)
	}
	return ret, nil
}

func ParseCSVWithKey(s, key string, fields ...string) (map[string]map[string]string, error) {
	ret := map[string]map[string]string{}
	if s == "" {
		return ret, nil
	}
	sReader := strings.NewReader(s)
	csvReader := csv.NewReader(sReader)
	csvs, err := csvReader.ReadAll()
	if err != nil {
		return ret, err
	}
	for _, c := range csvs[1:] {
		item := map[string]string{}
		k := ""
		for i, name := range csvs[0] {
			if name == key {
				k = c[i]
			}
			sort.Strings(fields)
			index := sort.SearchStrings(fields, name)
			if len(fields) > 0 && !(index < len(fields) && fields[index] == name) {
				continue
			}
			item[name] = c[i]
		}
		if k != "" {
			ret[k] = item
		}
	}
	return ret, nil
}

func ParseKVString(s string) (map[string]string, error) {
	options := map[string]string{}
	trimString := strings.Trim(s, " ")
	kvSlice := strings.Split(trimString, " ")
	for _, kvString := range kvSlice {
		kv := strings.Split(kvString, "=")
		if len(kv) == 1 {
			kv = append(kv, "")
		}
		options[kv[0]] = kv[1]
	}
	return options, nil
}

func ParseBrctlShow(s string) (map[string][]string, error) {
	brs := map[string][]string{}
	lines := strings.Split(s, "\n")
	if len(lines) <= 1 {
		return brs, nil
	}
	lastBr := ""
	for _, line := range lines[1:] {
		cols := strings.Split(line, "\t")
		brName := cols[0]
		ifName := cols[len(cols)-1]
		if brName != "" {
			lastBr = brName
			brs[brName] = []string{}
		}
		if ifName != "" {
			brs[lastBr] = append(brs[lastBr], ifName)
		}
	}
	return brs, nil
}

func ParseVLANConfig(s string) (map[string]int, error) {
	configs := map[string]int{}
	lines := strings.Split(s, "\n")
	if len(lines) <= 2 {
		return configs, nil
	}
	for _, line := range lines[2:] {
		cols := strings.Split(line, "|")
		if len(cols) != 3 {
			continue
		}
		vlanID, err := strconv.Atoi(strings.Trim(cols[1], " "))
		if err != nil {
			return configs, err
		}
		configs[strings.Trim(cols[0], " ")] = vlanID
	}
	return configs, nil
}

func ParseVMStates(s string) (map[string]int, error) {
	vmToState := map[string]int{}
	lines := strings.Split(s, "\n")
	if len(lines) <= 2 {
		return vmToState, nil
	}
	for _, line := range lines[2:] {
		fields := strings.Fields(line)
		if len(fields) != 3 && len(fields) != 4 {
			continue
		}
		name := strings.Trim(fields[1], " ")
		stateStr := strings.Trim(fields[2], " ")
		var state int
		switch stateStr {
		case "running":
			state = common.VM_STATE_RUNNING
		case "shut", "关闭":
			state = common.VM_STATE_STOPPED
		default:
			state = common.VM_STATE_EXCEPTION
		}
		vmToState[name] = state
	}
	return vmToState, nil
}

func ParseVMXml(s, nameField string) ([]XMLVM, error) {
	var vms []XMLVM
	if s == "" {
		return vms, nil
	}

	// ns := "http://openstack.org/xmlns/libvirt/nova/1.0"
	var domains []KVMDomain
	var etcDomains ETCDomains
	err := xml.Unmarshal([]byte(s), &etcDomains)
	if err != nil {
		return vms, err
	}
	if len(etcDomains.Domains) != 0 {
		domains = etcDomains.Domains
	} else {
		var runDomains RUNDomains
		err := xml.Unmarshal([]byte(s), &runDomains)
		if err != nil {
			return vms, err
		}
		for _, d := range runDomains.DomStatus {
			domains = append(domains, d.Domains)
		}
	}
	for _, domain := range domains {
		var vm XMLVM
		if domain.UUID == "" {
			log.Warning("vm uuid not found in xml")
			continue
		}
		if domain.Name == "" {
			log.Warning("vm uuid not found in xml")
			continue
		}
		vm.UUID = domain.UUID
		vm.Label = domain.Name
		switch nameField {
		case "metadata":
			vm.Name = domain.MetaData.Instance.Name
		case "uuid":
			vm.Name = domain.UUID
		case "name":
			vm.Name = domain.Name
		case "title":
			vm.Name = domain.Title
		default:
			log.Warningf("invalid config vm_name_field: (%s)", nameField)
		}
		if vm.Name == "" {
			vm.Name = domain.Name
		}
		if domain.MetaData.Instance.Owner.Project.UUID != "" {
			uuid := domain.MetaData.Instance.Owner.Project.UUID
			if len(uuid) == 32 {
				uuid = strings.Join([]string{uuid[:8], uuid[8:12], uuid[12:16], uuid[16:20], uuid[20:]}, "-")
			}
			vm.VPC = XMLVPC{
				UUID: uuid,
				Name: domain.MetaData.Instance.Owner.Project.Value,
			}
		}
		if len(domain.Devices.Interfaces) != 0 {
			var interfaces []XMLInterface
			for _, item := range domain.Devices.Interfaces {
				var iface XMLInterface
				iface.Target = item.Target.Value
				iface.Mac = item.Mac.Value
				interfaces = append(interfaces, iface)
			}
			vm.Interfaces = interfaces
		}
		vms = append(vms, vm)
	}
	return vms, nil
}

func ParseCompressedInfo(cInfo []byte) (bytes.Buffer, error) {
	reader := bytes.NewReader(cInfo)
	var out bytes.Buffer
	r, err := zlib.NewReader(reader)
	if err != nil {
		return bytes.Buffer{}, err
	}
	_, err = out.ReadFrom(r)
	if err != nil {
		return bytes.Buffer{}, err
	}
	return out, nil
}

func Uint64ToMac(v uint64) net.HardwareAddr {
	bytes := [8]byte{}
	BigEndian.PutUint64(bytes[:], v)
	return net.HardwareAddr(bytes[2:])
}

func IPInRanges(ip string, ipRanges ...netaddr.IPPrefix) bool {
	ipObj, err := netaddr.ParseIP(ip)
	if err != nil {
		return false
	}
	for _, ipRange := range ipRanges {
		if ipRange.Contains(ipObj) {
			return true
		}
	}
	return false
}

func RequestGet(url string, timeout int, queryStrings map[string]string) error {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			DisableKeepAlives: true,
		},
		Timeout: time.Duration(timeout) * time.Second,
	}

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	queryData := request.URL.Query()
	for k, v := range queryStrings {
		queryData.Add(k, v)
	}
	request.URL.RawQuery = queryData.Encode()

	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("http status failed: (%d)", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	respJson, err := simplejson.NewJson(body)
	if err != nil {
		return fmt.Errorf("response body (%s) serializer to json failed: (%s)", string(body), err.Error())
	}
	optStatus := respJson.Get("OPT_STATUS").MustString()
	if optStatus != "" && optStatus != "SUCCESS" {
		description := respJson.Get("DESCRIPTION").MustString()
		return fmt.Errorf("curl (%s) failed, (%s)", request.URL.String(), description)
	}

	return nil
}

func GetTeamShortLcuuidToInfo() (map[string]TeamInfo, error) {
	teamIDToOrgID := map[string]TeamInfo{}
	orgIDs, err := metadb.GetORGIDs()
	if err != nil {
		return teamIDToOrgID, err
	}
	for _, orgID := range orgIDs {
		db, err := metadb.GetDB(orgID)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		var teams []metadbmodel.Team
		err = db.Find(&teams).Error
		if err != nil {
			log.Error(err.Error())
			continue
		}
		for _, team := range teams {
			teamIDToOrgID[team.ShortLcuuid] = TeamInfo{
				OrgID:  orgID,
				TeamId: team.TeamID,
			}
		}
	}
	return teamIDToOrgID, nil
}

func IsAgentInterestedHost(aType agent.AgentType) bool {
	types := []agent.AgentType{agent.AgentType_TT_PROCESS, agent.AgentType_TT_HOST_POD, agent.AgentType_TT_VM_POD, agent.AgentType_TT_PHYSICAL_MACHINE, agent.AgentType_TT_PUBLIC_CLOUD, agent.AgentType_TT_K8S_SIDECAR}
	for _, t := range types {
		if t == aType {
			return true
		}
	}
	return false
}
