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

package genesis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"inet.af/netaddr"

	"github.com/bitly/go-simplejson"
	tridentcommon "github.com/deepflowio/deepflow/message/common"
	cloudmodel "github.com/deepflowio/deepflow/server/controller/cloud/model"
	"github.com/deepflowio/deepflow/server/controller/common"
	genesiscommon "github.com/deepflowio/deepflow/server/controller/genesis/common"
	"github.com/deepflowio/deepflow/server/controller/genesis/config"
	"github.com/deepflowio/deepflow/server/controller/model"
	"github.com/deepflowio/deepflow/server/libs/queue"
)

type bridge struct {
	name string
	uuid string
	vlan int
}

type GenesisSyncRpcUpdater struct {
	vCtx                  context.Context
	vCancel               context.CancelFunc
	storage               *SyncStorage
	outputQueue           queue.QueueReader
	hostIPsRanges         []netaddr.IPPrefix
	localIPRanges         []netaddr.IPPrefix
	excludeIPRanges       []netaddr.IPPrefix
	multiNSMode           bool
	singleVPCMode         bool
	defaultVPCName        string
	vmNameField           string
	ignoreNICRegex        *regexp.Regexp
	genesisSyncDataByVtap map[string]GenesisSyncDataOperation
}

func NewGenesisSyncRpcUpdater(storage *SyncStorage, queue queue.QueueReader, cfg config.GenesisConfig, ctx context.Context) *GenesisSyncRpcUpdater {
	hostIPRanges := []netaddr.IPPrefix{}
	for _, h := range cfg.HostIPs {
		ipObj, err := netaddr.ParseIP(h)
		if err == nil {
			var bit uint8
			switch {
			case ipObj.Is4():
				bit = 32
			case ipObj.Is6():
				bit = 128
			}
			ipPrefix, err := ipObj.Prefix(bit)
			if err != nil {
				log.Error("host ip convert to ip prefix error: " + err.Error())
				continue
			}
			hostIPRanges = append(hostIPRanges, ipPrefix)
		} else {
			hostIPRange, err := netaddr.ParseIPPrefix(h)
			if err == nil {
				hostIPRanges = append(hostIPRanges, hostIPRange)
			} else {
				hostIPRangeSlice, err := netaddr.ParseIPRange(h)
				if err != nil {
					log.Error("parse host ip ranges error: " + err.Error())
					continue
				}
				hostIPRanges = append(hostIPRanges, hostIPRangeSlice.Prefixes()...)
			}
		}
	}

	localIPRanges := []netaddr.IPPrefix{}
	if len(cfg.LocalIPRanges) > 0 {
		for _, l := range cfg.LocalIPRanges {
			localIPRange, err := netaddr.ParseIPPrefix(l)
			if err != nil {
				localIPRangeSlice, err := netaddr.ParseIPRange(l)
				if err != nil {
					log.Error("parse local ip ranges error: " + err.Error())
					continue
				}
				localIPRanges = append(localIPRanges, localIPRangeSlice.Prefixes()...)
			} else {
				localIPRanges = append(localIPRanges, localIPRange)
			}
		}
	}

	excludeIPRanges := []netaddr.IPPrefix{}
	if len(cfg.ExcludeIPRanges) > 0 {
		for _, e := range cfg.ExcludeIPRanges {
			excludeIPRange, err := netaddr.ParseIPPrefix(e)
			if err != nil {
				excludeIPRangeSlice, err := netaddr.ParseIPRange(e)
				if err != nil {
					log.Error("parse exclude ip ranges error: " + err.Error())
				}
				excludeIPRanges = append(excludeIPRanges, excludeIPRangeSlice.Prefixes()...)
			} else {
				excludeIPRanges = append(excludeIPRanges, excludeIPRange)
			}
		}
	}

	ignoreNICRegex, err := regexp.Compile(cfg.IgnoreNICRegex)
	if err != nil {
		log.Errorf("config ignore NIC regex (%s) complie failed", cfg.IgnoreNICRegex)
	}

	vCtx, vCancel := context.WithCancel(ctx)
	return &GenesisSyncRpcUpdater{
		vCtx:                  vCtx,
		vCancel:               vCancel,
		storage:               storage,
		outputQueue:           queue,
		hostIPsRanges:         hostIPRanges,
		localIPRanges:         localIPRanges,
		excludeIPRanges:       excludeIPRanges,
		multiNSMode:           cfg.MultiNSMode,
		singleVPCMode:         cfg.SingleVPCMode,
		vmNameField:           cfg.VMNameField,
		defaultVPCName:        cfg.DefaultVPCName,
		ignoreNICRegex:        ignoreNICRegex,
		genesisSyncDataByVtap: map[string]GenesisSyncDataOperation{},
	}
}

func (v *GenesisSyncRpcUpdater) ParseVinterfaceInfo(info VIFRPCMessage, peer string, vtapID uint32, k8sClusterID, deviceType string) []model.GenesisVinterface {
	var isContainer bool
	if deviceType == genesiscommon.DEVICE_TYPE_DOCKER_HOST {
		isContainer = true
	}
	epoch := time.Now()
	VIFs := []model.GenesisVinterface{}
	ipAddrs := info.message.GetPlatformData().GetRawIpAddrs()
	if len(ipAddrs) == 0 {
		log.Errorf("get sync data (raw ip addrs) empty")
		return []model.GenesisVinterface{}
	}
	netNSs := info.message.GetPlatformData().GetRawIpNetns()
	if len(netNSs) == 0 {
		netNSs = []string{""}
		ipAddrs = ipAddrs[:1]
	}
	if len(ipAddrs) != len(netNSs) {
		log.Error("the quantities of (raw ip addrs) and (raw ip netns) do not match")
		return []model.GenesisVinterface{}
	}
	rootNSMacs := map[string]bool{}
	ifIndexToInterface := map[string]genesiscommon.Iface{}
	ifNameToInterface := map[string]genesiscommon.Iface{}
	for i, ipAddr := range ipAddrs {
		nsName := netNSs[i]
		parsedGlobalIPs, err := genesiscommon.ParseIPOutput(strings.Trim(ipAddr, " "))
		if err != nil {
			log.Errorf("parse ip output error: (%s)", err)
			return []model.GenesisVinterface{}
		}

		for _, item := range parsedGlobalIPs {
			if item.Name == "lo" {
				continue
			}
			if v.ignoreNICRegex != nil && v.ignoreNICRegex.MatchString(item.Name) {
				continue
			}
			if item.MAC == "" {
				continue
			}
			// Just take the mac of the root netns
			if i == 0 {
				rootNSMacs[item.MAC] = false
			}
			ifIndexToInterface[fmt.Sprintf("%v%v", nsName, item.Index)] = item
			ifNameToInterface[nsName+item.Name] = item
			vIF := model.GenesisVinterface{
				Name:    item.Name,
				Mac:     item.MAC,
				TapName: item.Name,
				TapMac:  item.MAC,
			}
			// ignore interfaces without ip for container nodes
			// but keep these for kvm hosts
			if isContainer && len(item.IPs) == 0 {
				continue
			}
			ipSlice := []string{}
			for _, ip := range item.IPs {
				if ip.Address == "" || ip.MaskLen == 0 {
					continue
				}
				ipSlice = append(ipSlice, fmt.Sprintf("%s/%v", ip.Address, ip.MaskLen))
			}
			vIF.IPs = strings.Join(ipSlice, ",")
			vIF.Lcuuid = common.GetUUIDByOrgID(info.orgID, vIF.Name+vIF.Mac+strconv.Itoa(int(vtapID)))
			vIF.DeviceLcuuid = common.GetUUIDByOrgID(info.orgID, vIF.Name+vIF.Mac+strconv.Itoa(int(vtapID)))
			vIF.DeviceType = deviceType
			vIF.HostIP = peer
			vIF.LastSeen = epoch
			vIF.VtapID = vtapID
			vIF.KubernetesClusterID = k8sClusterID
			vIF.TeamID = info.teamID
			VIFs = append(VIFs, vIF)
		}
	}

	deviceIDToMinMAC := map[string]uint64{}
	for _, iface := range info.message.GetPlatformData().Interfaces {
		ifaceMAC := iface.GetMac()
		ifaceDeviceID := iface.GetDeviceId()
		if iMac, ok := deviceIDToMinMAC[ifaceDeviceID]; ok {
			if ifaceMAC < iMac {
				deviceIDToMinMAC[ifaceDeviceID] = ifaceMAC
			}
		} else {
			deviceIDToMinMAC[ifaceDeviceID] = ifaceMAC
		}
	}
	deviceIDToUUID := map[string]string{}
	for key, value := range deviceIDToMinMAC {
		deviceIDToUUID[key] = common.GetUUIDByOrgID(info.orgID, key+fmt.Sprintf("%d", value))
	}
	for _, iface := range info.message.GetPlatformData().Interfaces {
		vIF := model.GenesisVinterface{
			Name: iface.GetName(),
			Mac:  genesiscommon.Uint64ToMac(iface.GetMac()).String(),
		}
		var hasNetMask bool
		var validIPs []string
		for _, addr := range iface.Ip {
			hasNetMask = strings.Contains(addr, `/`)
			var netIP netaddr.IP
			if hasNetMask {
				ipPrefix, err := netaddr.ParseIPPrefix(addr)
				if err != nil {
					log.Error(err.Error())
					continue
				}
				netIP = ipPrefix.IP()
			} else {
				ipAddr, err := netaddr.ParseIP(addr)
				if err != nil {
					log.Error(err.Error())
					continue
				}
				netIP = ipAddr
			}

			excludeFlag := false
			for _, ipRange := range v.excludeIPRanges {
				if ipRange.Contains(netIP) {
					excludeFlag = true
					break
				}
			}
			if excludeFlag {
				continue
			}
			validIPs = append(validIPs, addr)
		}
		vIF.IPs = strings.Join(validIPs, ",")
		vIF.Lcuuid = common.GetUUIDByOrgID(info.orgID, vIF.Name+vIF.Mac+vIF.IPs+strconv.Itoa(int(vtapID)))
		ifaceNSName := iface.GetNetns()
		if len(ipAddrs) == 1 {
			ifaceNSName = netNSs[0]
		}
		if gIF, ok := ifIndexToInterface[fmt.Sprintf("%v%v", ifaceNSName, iface.GetTapIndex())]; ok && isContainer {
			vIF.TapName = gIF.Name
			vIF.TapMac = gIF.MAC
		} else if gIF, ok := ifNameToInterface[ifaceNSName+iface.GetName()]; ok && !isContainer {
			vIF.TapName = gIF.Name
			vIF.TapMac = gIF.MAC
		}
		if isContainer {
			if hasNetMask {
				// 拿不到子网的时候不填写该uuid
				vIF.DeviceLcuuid = deviceIDToUUID[iface.GetDeviceId()]
			}
			vIF.DeviceName = fmt.Sprintf("namespace-%s", iface.GetDeviceId())
			vIF.DeviceType = genesiscommon.DEVICE_TYPE_DOCKER_CONTAINER
			if _, ok := rootNSMacs[vIF.Mac]; ok && v.multiNSMode {
				vIF.DeviceType = genesiscommon.DEVICE_TYPE_DOCKER_HOST
			}
		} else if deviceType == genesiscommon.DEVICE_TYPE_KVM_HOST {
			vIF.DeviceLcuuid = iface.GetDeviceId()
			vIF.DeviceName = iface.GetDeviceName()
			vIF.DeviceType = genesiscommon.DEVICE_TYPE_KVM_VM
		} else {
			// 忽略workload类型
			continue
		}
		vIF.NetnsID = iface.GetNetnsId()
		vIF.IFType = iface.GetIfType()
		vIF.HostIP = peer
		vIF.LastSeen = epoch
		vIF.VtapID = vtapID
		vIF.KubernetesClusterID = k8sClusterID
		vIF.TeamID = info.teamID
		VIFs = append(VIFs, vIF)
	}
	return VIFs
}

func (v *GenesisSyncRpcUpdater) ParseVIP(info VIFRPCMessage, vtapID uint32) []model.GenesisVIP {
	var vips []model.GenesisVIP

	ipAddrs := info.message.GetPlatformData().GetRawIpAddrs()
	if len(ipAddrs) == 0 {
		log.Errorf("get sync data (raw ip addrs) empty")
		return []model.GenesisVIP{}
	}
	for _, ipAddr := range ipAddrs {
		parsedGlobalIPs, err := genesiscommon.ParseIPOutput(strings.Trim(ipAddr, " "))
		if err != nil {
			log.Errorf("parse ip output error: (%s)", err)
			return []model.GenesisVIP{}
		}

		for _, item := range parsedGlobalIPs {
			if item.Name != "lo" {
				continue
			}
			for _, ip := range item.IPs {
				ipObj, err := netaddr.ParseIP(ip.Address)
				if err != nil {
					log.Warningf("parse lo vip (%s) field: (%s)", ip.Address, err)
					continue
				}
				if ipObj.IsLoopback() {
					continue
				}
				vips = append(vips, model.GenesisVIP{
					Lcuuid: common.GetUUIDByOrgID(info.orgID, ip.Address+strconv.Itoa(int(vtapID))),
					IP:     ip.Address,
					VtapID: vtapID,
				})
			}
		}
	}
	return vips
}

func (v *GenesisSyncRpcUpdater) ParseHostAsVmPlatformInfo(info VIFRPCMessage, peer, natIP string, vtapID uint32) GenesisSyncDataOperation {
	hostName := strings.Trim(info.message.GetPlatformData().GetRawHostname(), " \n")
	if hostName == "" {
		log.Error("get sync data (raw hostname) empty")
		return GenesisSyncDataOperation{}
	}
	ipAddrs := info.message.GetPlatformData().GetRawIpAddrs()
	if len(ipAddrs) == 0 {
		log.Error("get sync data (raw ip addrs) empty")
		return GenesisSyncDataOperation{}
	}
	interfaces, err := genesiscommon.ParseIPOutput(strings.Trim(ipAddrs[0], " "))
	if err != nil {
		log.Error(err.Error())
		return GenesisSyncDataOperation{}
	}
	vpc := model.GenesisVpc{
		Name:   v.defaultVPCName,
		Lcuuid: common.GetUUIDByOrgID(info.orgID, v.defaultVPCName),
		VtapID: vtapID,
	}
	// check if vm is behind NAT
	behindNat := peer != natIP
	log.Infof("host (%s) nat ip is (%s) peer ip is (%s), behind nat: (%t), single vpc mode: (%t)", hostName, natIP, peer, behindNat, v.singleVPCMode)
	if behindNat && !v.singleVPCMode {
		vpc = model.GenesisVpc{
			Name:   "VPC-" + peer,
			Lcuuid: common.GetUUIDByOrgID(info.orgID, "VPC-"+peer),
			VtapID: vtapID,
		}
	}
	vpcs := []model.GenesisVpc{vpc}

	vm := model.GenesisVM{
		Name:         hostName,
		Label:        hostName,
		Lcuuid:       common.GetUUIDByOrgID(info.orgID, hostName),
		VPCLcuuid:    vpc.Lcuuid,
		LaunchServer: "127.0.0.1",
		State:        common.VM_STATE_RUNNING,
		CreatedAt:    time.Now(),
		VtapID:       vtapID,
	}
	vms := []model.GenesisVM{vm}

	nameToNetwork := map[string]model.GenesisNetwork{}
	ports := []model.GenesisPort{}
	ipLastSeens := []model.GenesisIP{}
	for _, iface := range interfaces {
		if iface.MAC == "" || iface.Name == "lo" {
			log.Debugf("not found mac or netcard is loopback (%#v)", iface)
			continue
		}
		ips := iface.IPs
		firstIP := genesiscommon.VifInfo{}
		ipFlag := false
		for _, sIP := range ips {
			if sIP.Scope == "global" {
				firstIP = sIP
				ipFlag = true
				break
			}
		}
		if !ipFlag {
			continue
		}
		isExternal := false
		for _, ipItem := range ips {
			pIP, err := netaddr.ParseIP(ipItem.Address)
			if err != nil {
				log.Error(err.Error())
				return GenesisSyncDataOperation{}
			}

			localFlag := false
			for _, ipRange := range v.localIPRanges {
				if ipRange.Contains(pIP) {
					localFlag = true
					break
				}
			}
			if !localFlag {
				isExternal = true
				break
			}
		}
		networkIP, err := netaddr.ParseIP(firstIP.Address)
		if err != nil {
			log.Errorf("parse first ip (%s) err: (%s)", firstIP.Address, err.Error())
			continue
		}
		networkPrefix := netaddr.IPPrefixFrom(networkIP, uint8(firstIP.MaskLen))
		networkName := fmt.Sprintf("Network-%s", networkPrefix.Masked().String())
		network, ok := nameToNetwork[networkName]
		vType := common.VIF_TYPE_LAN
		netType := common.NETWORK_TYPE_LAN
		if isExternal {
			vType = common.VIF_TYPE_WAN
			netType = common.NETWORK_TYPE_WAN
		}
		if !ok {
			network = model.GenesisNetwork{
				Lcuuid:         common.GetUUIDByOrgID(info.orgID, networkName),
				Name:           networkName,
				SegmentationID: 1,
				VtapID:         vtapID,
				VPCLcuuid:      vpc.Lcuuid,
				External:       isExternal,
				NetType:        uint32(netType),
			}
			nameToNetwork[networkName] = network
		}
		port := model.GenesisPort{
			Lcuuid:        common.GetUUIDByOrgID(info.orgID, hostName+iface.MAC),
			Type:          uint32(vType),
			VtapID:        vtapID,
			Mac:           iface.MAC,
			DeviceLcuuid:  vm.Lcuuid,
			NetworkLcuuid: network.Lcuuid,
			VPCLcuuid:     vm.VPCLcuuid,
			DeviceType:    common.VIF_DEVICE_TYPE_VM,
		}
		ports = append(ports, port)
		for _, p := range ips {
			// ignore lin scope
			if p.Scope != "global" && p.Scope != "host" {
				continue
			}
			oIP, err := netaddr.ParseIP(p.Address)
			if err != nil {
				log.Warning(err.Error())
				continue
			}
			ipLastSeen := model.GenesisIP{
				Lcuuid:           common.GetUUIDByOrgID(info.orgID, hostName+oIP.String()+port.Lcuuid),
				VinterfaceLcuuid: port.Lcuuid,
				IP:               p.Address,
				Masklen:          p.MaskLen,
				VtapID:           vtapID,
				LastSeen:         time.Now(),
			}
			ipLastSeens = append(ipLastSeens, ipLastSeen)
		}
	}
	networks := []model.GenesisNetwork{}
	for _, n := range nameToNetwork {
		networks = append(networks, n)
	}

	return GenesisSyncDataOperation{
		VMs:         NewVMPlatformDataOperation(info.orgID, vms),
		VPCs:        NewVpcPlatformDataOperation(info.orgID, vpcs),
		Ports:       NewPortPlatformDataOperation(info.orgID, ports),
		Networks:    NewNetworkPlatformDataOperation(info.orgID, networks),
		IPlastseens: NewIPLastSeenPlatformDataOperation(info.orgID, ipLastSeens),
	}
}

func (v *GenesisSyncRpcUpdater) ParseProcessInfo(info VIFRPCMessage, vtapID uint32) []model.GenesisProcess {
	processes := []model.GenesisProcess{}
	if vtapID == 0 {
		return processes
	}

	for _, p := range info.message.GetProcessData().GetProcessEntries() {
		var osAppTagSlice []string
		for _, tag := range p.GetOsAppTags() {
			osAppTagSlice = append(osAppTagSlice, tag.GetKey()+":"+tag.GetValue())
		}
		osAppTagString := strings.Join(osAppTagSlice, ", ")
		startTime := time.Unix(int64(p.GetStartTime()), 0)
		pID := p.GetPid()
		processes = append(processes, model.GenesisProcess{
			Lcuuid:      common.GetUUIDByOrgID(info.orgID, strconv.Itoa(int(pID))+strconv.Itoa(int(vtapID))),
			PID:         pID,
			NetnsID:     p.GetNetnsId(),
			Name:        p.GetName(),
			ProcessName: p.GetProcessName(),
			CMDLine:     p.GetCmdline(),
			User:        p.GetUser(),
			ContainerID: p.GetContainerId(),
			VtapID:      vtapID,
			OSAPPTags:   osAppTagString,
			StartTime:   startTime,
		})
	}
	return processes
}

func (v *GenesisSyncRpcUpdater) ParseKVMPlatformInfo(info VIFRPCMessage, peer string, vtapID uint32) GenesisSyncDataOperation {
	rawVM := strings.Trim(info.message.GetPlatformData().GetRawAllVmXml(), " ")
	rawOVSInterface := strings.Trim(info.message.GetPlatformData().GetRawOvsInterfaces(), " ")
	rawOVSPorts := strings.Trim(info.message.GetPlatformData().GetRawOvsPorts(), " ")
	ovsMode := false
	if rawOVSPorts != "" {
		ovsMode = true
	}
	rawHostName := strings.Trim(info.message.GetPlatformData().GetRawHostname(), " \n")
	rawVMStates := strings.Trim(info.message.GetPlatformData().GetRawVmStates(), " ")
	rawBrctlShow := strings.Trim(info.message.GetPlatformData().GetRawBrctlShow(), " ")
	rawVlanConfig := strings.Trim(info.message.GetPlatformData().GetRawVlanConfig(), " ")
	tIPs := info.message.GetPlatformData().GetIps()
	if ovsMode {
		rawBrctlShow = ""
		rawVlanConfig = ""
	}

	hosts := []model.GenesisHost{
		model.GenesisHost{
			Hostname: rawHostName,
			Lcuuid:   common.GetUUIDByOrgID(info.orgID, rawHostName),
			IP:       peer,
			VtapID:   vtapID,
		},
	}

	vpcIDToName := map[string]string{}
	networkIDToNetwork := map[string]model.GenesisNetwork{}
	macToPort := map[string]map[string]string{}
	portToBridge := map[string]bridge{}
	vms := []model.GenesisVM{}
	vpcs := []model.GenesisVpc{}
	ports := []model.GenesisPort{}
	networks := []model.GenesisNetwork{}

	pCSVkeys := []string{"_uuid", "other_config", "tag"}
	nameToOvsPort, err := genesiscommon.ParseCSVWithKey(rawOVSPorts, "name", pCSVkeys...)
	if err != nil {
		log.Warning("parse csv with key failed: " + err.Error())
	}
	pCSVs := []string{"name", "external_ids"}
	nameToOvsIfs, err := genesiscommon.ParseCSV(rawOVSInterface, pCSVs...)
	if err != nil {
		log.Warning("parse csv failed: " + err.Error())
	}
	for _, nameToOvsIf := range nameToOvsIfs {
		name, ok := nameToOvsIf["name"]
		if !ok {
			continue
		}
		eIDs, err := genesiscommon.ParseKVString(nameToOvsIf["external_ids"])
		if err != nil {
			log.Warning("parse kvstring failed: " + err.Error())
		}
		mac, ok := eIDs["attached-mac"]
		if !ok {
			log.Debugf("ovs interface %s does not have external_ids:attached-mac", name)
			continue
		}
		if ovsPort, ok := nameToOvsPort[name]; ok {
			macToPort[mac] = ovsPort
		}
	}
	if !ovsMode {
		bridges, err := genesiscommon.ParseBrctlShow(rawBrctlShow)
		if err != nil {
			log.Warning("parse brctl show failed: " + err.Error())
		}
		vlanConfig, err := genesiscommon.ParseVLANConfig(rawVlanConfig)
		if err != nil {
			log.Warning("parse vlan config failed: " + err.Error())
		}
		for br, ifaces := range bridges {
			vlan := 1
			for _, iface := range ifaces {
				if v, ok := vlanConfig[iface]; ok {
					vlan = v
					break
				}
			}
			bge := bridge{
				name: br,
				uuid: common.GetUUIDByOrgID(info.orgID, fmt.Sprintf("%d,%s", vlan, br)),
				vlan: vlan,
			}
			for _, iface := range ifaces {
				portToBridge[iface] = bge
			}
		}
	}

	macToIPs := map[string][]model.GenesisIP{}
	macToIsExternalIP := map[string]bool{}
	for _, tIP := range tIPs {
		ip := model.GenesisIP{}
		ipObj := net.IP(tIP.GetIp())
		nIPObj, ok := netaddr.FromStdIP(ipObj)
		if !ok {
			log.Warningf("ip (%s) invalid", ipObj.String())
			continue
		}
		ip.IP = ipObj.String()
		tIPLastSeen := tIP.GetLastSeen()
		ip.LastSeen = time.Unix(int64(tIPLastSeen), 0).Local()
		ip.VtapID = vtapID
		macStr := genesiscommon.Uint64ToMac(tIP.GetMac()).String()
		macToIPs[macStr] = append(macToIPs[macStr], ip)
		cFlag := true
		for _, l := range v.localIPRanges {
			if l.Contains(nIPObj) {
				cFlag = false
				break
			}
		}
		macToIsExternalIP[macStr] = cFlag
	}

	ips := []model.GenesisIP{}
	vmStates, err := genesiscommon.ParseVMStates(rawVMStates)
	if err != nil {
		log.Warning("parse vm states failed: " + err.Error())
	}
	xmlVMs, err := genesiscommon.ParseVMXml(rawVM, v.vmNameField)
	if err != nil {
		log.Warning("parse vm xml failed: " + err.Error())
	}
	for _, xmlVM := range xmlVMs {
		vm := model.GenesisVM{}
		vm.VtapID = vtapID
		vm.Lcuuid = xmlVM.UUID
		vm.Name = xmlVM.Name
		vm.Label = xmlVM.Label
		vm.CreatedAt = time.Now()
		if xmlVM.VPC.Name != "" && xmlVM.VPC.UUID != "" {
			vm.VPCLcuuid = xmlVM.VPC.UUID
			vpcIDToName[xmlVM.VPC.UUID] = xmlVM.VPC.Name
		}
		vm.LaunchServer = peer
		state, ok := vmStates[xmlVM.Label]
		if ok {
			vm.State = uint32(state)
		} else {
			vm.State = uint32(common.VM_STATE_EXCEPTION)
		}
		vms = append(vms, vm)

		if vm.State != uint32(common.VM_STATE_RUNNING) {
			// 不处理非运行状态虚拟机的接口
			continue
		}

		for _, xmlIf := range xmlVM.Interfaces {
			port := model.GenesisPort{}
			network := model.GenesisNetwork{}
			mac := strings.ToLower(xmlIf.Mac)
			ifName := xmlIf.Target
			if ovsMode {
				portMap := map[string]string{}
				if mP, ok := macToPort[mac]; ok {
					portMap = mP
				} else if oP, ok := nameToOvsPort[ifName]; ok {
					portMap = oP
				} else {
					log.Debugf("vm %s interface %s mac %s not found in ovs ports", vm.Label, ifName, mac)
					continue
				}
				port.Lcuuid = portMap["_uuid"]
				options, err := genesiscommon.ParseKVString(portMap["other_config"])
				if err != nil {
					log.Warning("parse kv string failed: " + err.Error())
				}
				if nLcuuid, ok := options["net_uuid"]; ok {
					network.Lcuuid = nLcuuid
				} else {
					// tag不为数字时均默认为1
					tagInt, err := strconv.Atoi(portMap["tag"])
					if err != nil {
						tagInt = 1
					}
					network.Lcuuid = common.GetUUIDByOrgID(info.orgID, strconv.Itoa(tagInt))
				}
				if sID, ok := options["segmentation_id"]; ok {
					sIDInt, err := strconv.Atoi(sID)
					if err != nil {
						sIDInt = 1
					}
					network.SegmentationID = uint32(sIDInt)
				} else {
					tagInt, err := strconv.Atoi(portMap["tag"])
					if err != nil {
						tagInt = 1
					}
					network.SegmentationID = uint32(tagInt)
				}
			} else {
				br, ok := portToBridge[ifName]
				if !ok {
					br = bridge{
						uuid: common.GetUUIDByOrgID(info.orgID, "yunshan-temp"),
						vlan: 1,
					}
				}
				port.Lcuuid = common.GetUUIDByOrgID(info.orgID, mac)
				network.Lcuuid = br.uuid
				network.SegmentationID = uint32(br.vlan)
			}

			isExternal := false
			if isE, ok := macToIsExternalIP[mac]; ok {
				isExternal = isE
			}
			network.NetType = uint32(common.NETWORK_TYPE_LAN)
			port.Type = uint32(common.VIF_TYPE_LAN)
			if isExternal {
				network.NetType = uint32(common.NETWORK_TYPE_WAN)
				port.Type = uint32(common.VIF_TYPE_WAN)
			}
			network.External = isExternal
			network.VPCLcuuid = vm.VPCLcuuid
			network.VtapID = vtapID
			network.Name = "subnet_vni_" + strconv.Itoa(int(network.SegmentationID))
			networkIDToNetwork[network.Lcuuid] = network
			port.Mac = mac
			port.DeviceLcuuid = vm.Lcuuid
			port.NetworkLcuuid = network.Lcuuid
			port.VPCLcuuid = vm.VPCLcuuid
			port.VtapID = vtapID
			port.DeviceType = common.VIF_DEVICE_TYPE_VM
			ports = append(ports, port)
			for _, ip := range macToIPs[mac] {
				ip.VinterfaceLcuuid = port.Lcuuid
				ip.Lcuuid = common.GetUUIDByOrgID(info.orgID, ip.IP+ip.VinterfaceLcuuid)
				ips = append(ips, ip)
			}
		}
	}

	for _, n := range networkIDToNetwork {
		networks = append(networks, n)
	}
	for id, name := range vpcIDToName {
		vpc := model.GenesisVpc{}
		vpc.VtapID = vtapID
		vpc.Lcuuid = id
		vpc.Name = name
		vpcs = append(vpcs, vpc)
	}

	lldps := []model.GenesisLldp{}
	for _, l := range info.message.GetPlatformData().GetLldpInfo() {
		lldp := model.GenesisLldp{}
		lldp.Lcuuid = common.GetUUIDByOrgID(info.orgID, peer+l.GetManagementAddress()+l.GetPortId())
		lldp.HostIP = peer
		lldp.VtapID = vtapID
		lldp.HostInterface = l.GetInterface()
		lldp.SystemName = l.GetSystemName()
		lldp.ManagementAddress = l.GetManagementAddress()
		lldp.VinterfaceLcuuid = l.GetPortId()
		lldp.VinterfaceDescription = l.GetPortDescription()
		lldp.LastSeen = time.Now()
		lldps = append(lldps, lldp)
	}

	return GenesisSyncDataOperation{
		Hosts:       NewHostPlatformDataOperation(info.orgID, hosts),
		VMs:         NewVMPlatformDataOperation(info.orgID, vms),
		VPCs:        NewVpcPlatformDataOperation(info.orgID, vpcs),
		Networks:    NewNetworkPlatformDataOperation(info.orgID, networks),
		Ports:       NewPortPlatformDataOperation(info.orgID, ports),
		IPlastseens: NewIPLastSeenPlatformDataOperation(info.orgID, ips),
		Lldps:       NewLldpInfoPlatformDataOperation(info.orgID, lldps),
	}
}

func (v *GenesisSyncRpcUpdater) UnmarshalProtobuf(info VIFRPCMessage) GenesisSyncDataOperation {
	genesisSyncDataOper := GenesisSyncDataOperation{}
	vifs := v.ParseVinterfaceInfo(info, info.peer, info.vtapID, info.message.GetKubernetesClusterId(), genesiscommon.DEVICE_TYPE_KVM_HOST)
	vinterfaces := NewVinterfacePlatformDataOperation(info.orgID, vifs)
	pProcess := v.ParseProcessInfo(info, info.vtapID)
	processes := NewProcessPlatformDataOperation(info.orgID, pProcess)

	if genesiscommon.IPInRanges(info.peer, v.hostIPsRanges...) && info.message.GetPlatformData().GetPlatformEnabled() {
		genesisSyncDataOper = v.ParseKVMPlatformInfo(info, info.peer, info.vtapID)
	}

	genesisSyncDataOper.Vinterfaces = vinterfaces
	genesisSyncDataOper.Processes = processes

	return genesisSyncDataOper
}

func (v *GenesisSyncRpcUpdater) UnmarshalKubernetesProtobuf(info VIFRPCMessage) GenesisSyncDataOperation {
	genesisSyncDataOper := GenesisSyncDataOperation{}
	vifs := v.ParseVinterfaceInfo(info, info.peer, info.vtapID, info.message.GetKubernetesClusterId(), genesiscommon.DEVICE_TYPE_DOCKER_HOST)
	vinterfaces := NewVinterfacePlatformDataOperation(info.orgID, vifs)
	pProcess := v.ParseProcessInfo(info, info.vtapID)
	processes := NewProcessPlatformDataOperation(info.orgID, pProcess)

	if info.message.GetPlatformData().GetPlatformEnabled() {
		genesisSyncDataOper = v.ParseHostAsVmPlatformInfo(info, info.peer, info.message.GetNatIp(), info.vtapID)
	}

	genesisSyncDataOper.Vinterfaces = vinterfaces
	genesisSyncDataOper.Processes = processes

	return genesisSyncDataOper
}

func (v *GenesisSyncRpcUpdater) UnmarshalWorkloadProtobuf(info VIFRPCMessage, tridentType string) GenesisSyncDataOperation {
	genesisSyncDataOper := GenesisSyncDataOperation{}
	vifs := v.ParseVinterfaceInfo(info, info.peer, info.vtapID, info.message.GetKubernetesClusterId(), tridentType)
	vinterfaces := NewVinterfacePlatformDataOperation(info.orgID, vifs)
	pvips := v.ParseVIP(info, info.vtapID)
	vips := NewVIPPlatformDataOperation(info.orgID, pvips)
	pProcess := v.ParseProcessInfo(info, info.vtapID)
	processes := NewProcessPlatformDataOperation(info.orgID, pProcess)

	if info.message.GetPlatformData().GetPlatformEnabled() {
		genesisSyncDataOper = v.ParseHostAsVmPlatformInfo(info, info.peer, info.message.GetNatIp(), info.vtapID)
	}

	genesisSyncDataOper.Vinterfaces = vinterfaces
	genesisSyncDataOper.Processes = processes
	genesisSyncDataOper.VIPs = vips

	return genesisSyncDataOper
}

func (v *GenesisSyncRpcUpdater) run() {
	for {
		genesisSyncDataOper := GenesisSyncDataOperation{}
		info := v.outputQueue.Get().(VIFRPCMessage)
		if info.msgType == genesiscommon.TYPE_EXIT {
			log.Warningf("sync from (%s) vtap_id (%v) type (%v)", info.peer, info.vtapID, info.msgType)
			continue
		}

		log.Debugf("sync received (%s) vtap_id (%v) type (%v) received (%s)", info.peer, info.vtapID, info.msgType, info.message)

		vtap := fmt.Sprintf("%d%d", info.orgID, info.vtapID)
		if info.msgType == genesiscommon.TYPE_RENEW {
			if info.vtapID != 0 {
				peerInfo, ok := v.genesisSyncDataByVtap[vtap]
				if ok {
					v.storage.Renew(peerInfo)
				}
			}
		} else if info.msgType == genesiscommon.TYPE_UPDATE {
			tridentType := info.message.GetTridentType()
			switch tridentType {
			case tridentcommon.TridentType_TT_PHYSICAL_MACHINE:
				genesisSyncDataOper = v.UnmarshalWorkloadProtobuf(info, genesiscommon.DEVICE_TYPE_PHYSICAL_MACHINE)
			case tridentcommon.TridentType_TT_PUBLIC_CLOUD:
				genesisSyncDataOper = v.UnmarshalWorkloadProtobuf(info, genesiscommon.DEVICE_TYPE_PUBLIC_CLOUD)
			case tridentcommon.TridentType_TT_HOST_POD, tridentcommon.TridentType_TT_VM_POD, tridentcommon.TridentType_TT_K8S_SIDECAR:
				genesisSyncDataOper = v.UnmarshalKubernetesProtobuf(info)
			default:
				genesisSyncDataOper = v.UnmarshalProtobuf(info)
			}
			if info.vtapID != 0 {
				v.genesisSyncDataByVtap[vtap] = genesisSyncDataOper
			}
			v.storage.Update(genesisSyncDataOper, info)
		}
	}
}

func (v *GenesisSyncRpcUpdater) Start() {
	go v.run()
}

func (v *GenesisSyncRpcUpdater) Stop() {
	if v.vCancel != nil {
		v.vCancel()
	}
}

type KubernetesRpcUpdater struct {
	kCtx        context.Context
	kCancel     context.CancelFunc
	storage     *KubernetesStorage
	outputQueue queue.QueueReader
}

func NewKubernetesRpcUpdater(storage *KubernetesStorage, queue queue.QueueReader, ctx context.Context) *KubernetesRpcUpdater {
	kCtx, kCancel := context.WithCancel(ctx)
	return &KubernetesRpcUpdater{
		kCtx:        kCtx,
		kCancel:     kCancel,
		storage:     storage,
		outputQueue: queue,
	}
}

func (k *KubernetesRpcUpdater) run() {
	for {
		info := k.outputQueue.Get().(K8SRPCMessage)
		if info.msgType == genesiscommon.TYPE_EXIT {
			log.Warningf("k8s from (%s) vtap_id (%v) type (%v) exit", info.peer, info.vtapID, info.msgType)
			break
		}
		log.Debugf("k8s from %s vtap_id %v received cluster_id %s version %v", info.peer, info.vtapID, info.message.GetClusterId(), info.message.GetVersion())
		// 更新和保存内存数据
		k.storage.Add(info.orgID, KubernetesInfo{
			ORGID:     info.orgID,
			Epoch:     time.Now(),
			ClusterID: info.message.GetClusterId(),
			ErrorMSG:  info.message.GetErrorMsg(),
			Version:   info.message.GetVersion(),
			Entries:   info.message.GetEntries(),
		})
	}
}

func (k *KubernetesRpcUpdater) Start() {
	go k.run()
}

func (k *KubernetesRpcUpdater) Stop() {
	if k.kCancel != nil {
		k.kCancel()
	}
}

type PrometheusRpcUpdater struct {
	kCtx        context.Context
	kCancel     context.CancelFunc
	storage     *PrometheusStorage
	outputQueue queue.QueueReader
}

func NewPrometheuspInfoRpcUpdater(storage *PrometheusStorage, queue queue.QueueReader, ctx context.Context) *PrometheusRpcUpdater {
	pCtx, pCancel := context.WithCancel(ctx)
	return &PrometheusRpcUpdater{
		kCtx:        pCtx,
		kCancel:     pCancel,
		storage:     storage,
		outputQueue: queue,
	}
}

func (p *PrometheusRpcUpdater) ParsePrometheusEntries(info PrometheusMessage) ([]cloudmodel.PrometheusTarget, error) {
	result := []cloudmodel.PrometheusTarget{}
	entries := info.message.GetEntries()
	for index, e := range entries {
		pJobNameToHonorLabelsConfig := map[string]bool{}
		configOut, err := genesiscommon.ParseCompressedInfo(e.GetConfigCompressedInfo())
		if err != nil {
			log.Warningf("index (%d): config decompress error: %s", index+1, err.Error())
			return []cloudmodel.PrometheusTarget{}, err
		}
		cEntryJson, err := simplejson.NewJson(configOut.Bytes())
		if err != nil {
			log.Warningf("index (%d): config marshal json error: %s", index+1, err.Error())
			return []cloudmodel.PrometheusTarget{}, err
		}
		cStatus := cEntryJson.Get("status").MustString()
		if cStatus != "success" {
			errMSG := fmt.Sprintf("index (%d): config api status (%s)", index+1, cStatus)
			log.Warning(errMSG)
			return []cloudmodel.PrometheusTarget{}, errors.New(errMSG)
		}
		prometheusConfig, err := genesiscommon.ParseYMAL(cEntryJson.GetPath("data", "yaml").MustString())
		if err != nil {
			log.Warningf("index (%d): config parse yaml error: %s", index+1, err.Error())
			return []cloudmodel.PrometheusTarget{}, err
		}
		for _, scrapeConfig := range prometheusConfig.ScrapeConfigs {
			pJobNameToHonorLabelsConfig[scrapeConfig.JobName] = scrapeConfig.HonorLabels
		}

		targetOut, err := genesiscommon.ParseCompressedInfo(e.GetTargetCompressedInfo())
		if err != nil {
			log.Warningf("index (%d): target decompress error: %s", index+1, err.Error())
			return []cloudmodel.PrometheusTarget{}, err
		}
		tEntryJson, err := simplejson.NewJson(targetOut.Bytes())
		if err != nil {
			log.Warningf("index (%d): target marshal json error: %s", index+1, err.Error())
			return []cloudmodel.PrometheusTarget{}, err
		}
		tStatus := tEntryJson.Get("status").MustString()
		if tStatus != "success" {
			errMSG := fmt.Sprintf("index (%d): target api status (%s)", index+1, tStatus)
			log.Warning(errMSG)
			return []cloudmodel.PrometheusTarget{}, errors.New(errMSG)
		}
		targets := tEntryJson.GetPath("data", "activeTargets")
		for t := range targets.MustArray() {
			target := targets.GetIndex(t)
			scrapeUrl := target.Get("scrapeUrl").MustString()
			labels := target.Get("labels")
			job := labels.Get("job").MustString()
			instance := labels.Get("instance").MustString()

			labelsSlice := []string{}
			honorLabelsConfig, ok := pJobNameToHonorLabelsConfig[job]
			if !ok {
				honorLabelsConfig = true
			}
			labelsMap := labels.MustMap()
			delete(labelsMap, "job")
			delete(labelsMap, "instance")
			keys := []string{}
			for key := range labelsMap {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, k := range keys {
				v := labelsMap[k]
				vString, ok := v.(string)
				if !ok {
					vString = ""
				}
				newString := k + ":" + vString
				labelsSlice = append(labelsSlice, newString)
			}

			otherLabelsString := strings.Join(labelsSlice, ", ")

			result = append(result, cloudmodel.PrometheusTarget{
				ScrapeURL:         scrapeUrl,
				Job:               job,
				Instance:          instance,
				OtherLabels:       otherLabelsString,
				HonorLabelsConfig: honorLabelsConfig,
			})
		}
	}

	return result, nil
}

func (p *PrometheusRpcUpdater) run() {
	currentVersion := map[string]uint64{}
	for {
		info := p.outputQueue.Get().(PrometheusMessage)
		if info.msgType == genesiscommon.TYPE_EXIT {
			log.Warningf("prometheus from (%s) vtap_id (%v) type (%v) exit", info.peer, info.vtapID, info.msgType)
			break
		}
		// 更新和保存内存数据
		var err error
		var entries []cloudmodel.PrometheusTarget
		clusterID := info.message.GetClusterId()
		version := info.message.GetVersion()
		errMSG := info.message.GetErrorMsg()
		cVersion := currentVersion[clusterID]
		parseFlag := version != cVersion
		if errMSG == "" && parseFlag {
			entries, err = p.ParsePrometheusEntries(info)
			if err != nil {
				errMSG = err.Error()
			}
			currentVersion[clusterID] = version
		}

		log.Debugf("prometheus from %s vtap_id %v received cluster_id %s version %v,  error message (%s)", info.peer, info.vtapID, clusterID, version, errMSG)

		p.storage.Add(info.orgID, parseFlag, PrometheusInfo{
			ORGID:     info.orgID,
			ClusterID: clusterID,
			Entries:   entries,
			Epoch:     time.Now(),
			ErrorMSG:  errMSG,
		})
	}
}

func (p *PrometheusRpcUpdater) Start() {
	go p.run()
}

func (p *PrometheusRpcUpdater) Stop() {
	if p.kCancel != nil {
		p.kCancel()
	}
}
