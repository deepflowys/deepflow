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

package updater

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"inet.af/netaddr"

	"github.com/deepflowio/deepflow/message/agent"
	ccommon "github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/genesis/common"
	"github.com/deepflowio/deepflow/server/controller/genesis/config"
	"github.com/deepflowio/deepflow/server/controller/model"
	"github.com/deepflowio/deepflow/server/libs/logger"
)

type bridge struct {
	name string
	uuid string
	vlan int
}

type GenesisSyncRpcUpdater struct {
	hostIPsRanges   []netaddr.IPPrefix
	localIPRanges   []netaddr.IPPrefix
	excludeIPRanges []netaddr.IPPrefix
	multiNSMode     bool
	singleVPCMode   bool
	nodeIP          string
	defaultVPCName  string
	vmNameField     string
	ignoreNICRegex  *regexp.Regexp
}

func NewGenesisSyncRpcUpdater(cfg config.GenesisConfig) *GenesisSyncRpcUpdater {
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

	return &GenesisSyncRpcUpdater{
		nodeIP:          os.Getenv(ccommon.NODE_IP_KEY),
		hostIPsRanges:   hostIPRanges,
		localIPRanges:   localIPRanges,
		excludeIPRanges: excludeIPRanges,
		multiNSMode:     cfg.MultiNSMode,
		singleVPCMode:   cfg.SingleVPCMode,
		vmNameField:     cfg.VMNameField,
		defaultVPCName:  cfg.DefaultVPCName,
		ignoreNICRegex:  ignoreNICRegex,
	}
}

func (v *GenesisSyncRpcUpdater) ParseVinterfaceInfo(orgID int, teamID, vtapID uint32, peer, deviceType string, message *agent.GenesisSyncRequest) []model.GenesisVinterface {
	var isContainer bool
	if deviceType == common.DEVICE_TYPE_DOCKER_HOST {
		isContainer = true
	}
	epoch := time.Now()
	k8sClusterID := message.GetKubernetesClusterId()
	VIFs := []model.GenesisVinterface{}
	ipAddrs := message.GetPlatformData().GetRawIpAddrs()
	if len(ipAddrs) == 0 {
		log.Errorf("get sync data (raw ip addrs) empty", logger.NewORGPrefix(orgID))
		return []model.GenesisVinterface{}
	}
	netNSs := message.GetPlatformData().GetRawIpNetns()
	if len(netNSs) == 0 {
		netNSs = []string{""}
		ipAddrs = ipAddrs[:1]
	}
	if len(ipAddrs) != len(netNSs) {
		log.Error("the quantities of (raw ip addrs) and (raw ip netns) do not match", logger.NewORGPrefix(orgID))
		return []model.GenesisVinterface{}
	}
	rootNSMacs := map[string]bool{}
	ifIndexToInterface := map[string]common.Iface{}
	ifNameToInterface := map[string]common.Iface{}
	for i, ipAddr := range ipAddrs {
		nsName := netNSs[i]
		parsedGlobalIPs, err := common.ParseIPOutput(strings.Trim(ipAddr, " "))
		if err != nil {
			log.Errorf("parse ip output error: (%s)", err, logger.NewORGPrefix(orgID))
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
			vIF.Lcuuid = ccommon.GetUUIDByOrgID(orgID, vIF.Name+vIF.Mac+strconv.Itoa(int(vtapID)))
			vIF.DeviceLcuuid = ccommon.GetUUIDByOrgID(orgID, vIF.Name+vIF.Mac+strconv.Itoa(int(vtapID)))
			vIF.DeviceType = deviceType
			vIF.HostIP = peer
			vIF.LastSeen = epoch
			vIF.VtapID = vtapID
			vIF.NodeIP = v.nodeIP
			vIF.KubernetesClusterID = k8sClusterID
			vIF.TeamID = teamID
			VIFs = append(VIFs, vIF)
		}
	}

	deviceIDToMinMAC := map[string]uint64{}
	for _, iface := range message.GetPlatformData().Interfaces {
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
		deviceIDToUUID[key] = ccommon.GetUUIDByOrgID(orgID, key+fmt.Sprintf("%d", value))
	}
	for _, iface := range message.GetPlatformData().Interfaces {
		vIF := model.GenesisVinterface{
			Name: iface.GetName(),
			Mac:  common.Uint64ToMac(iface.GetMac()).String(),
		}
		var hasNetMask bool
		var validIPs []string
		for _, addr := range iface.Ip {
			hasNetMask = strings.Contains(addr, `/`)
			var netIP netaddr.IP
			if hasNetMask {
				ipPrefix, err := netaddr.ParseIPPrefix(addr)
				if err != nil {
					log.Error(err.Error(), logger.NewORGPrefix(orgID))
					continue
				}
				netIP = ipPrefix.IP()
			} else {
				ipAddr, err := netaddr.ParseIP(addr)
				if err != nil {
					log.Error(err.Error(), logger.NewORGPrefix(orgID))
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
		vIF.Lcuuid = ccommon.GetUUIDByOrgID(orgID, vIF.Name+vIF.Mac+vIF.IPs+strconv.Itoa(int(vtapID)))
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
			vIF.DeviceType = common.DEVICE_TYPE_DOCKER_CONTAINER
			if _, ok := rootNSMacs[vIF.Mac]; ok && v.multiNSMode {
				vIF.DeviceType = common.DEVICE_TYPE_DOCKER_HOST
			}
		} else if deviceType == common.DEVICE_TYPE_KVM_HOST {
			vIF.DeviceLcuuid = iface.GetDeviceId()
			vIF.DeviceName = iface.GetDeviceName()
			vIF.DeviceType = common.DEVICE_TYPE_KVM_VM
		} else {
			// 忽略workload类型
			continue
		}
		vIF.NetnsID = iface.GetNetnsId()
		vIF.IFType = iface.GetIfType()
		vIF.HostIP = peer
		vIF.LastSeen = epoch
		vIF.VtapID = vtapID
		vIF.NodeIP = v.nodeIP
		vIF.KubernetesClusterID = k8sClusterID
		vIF.TeamID = teamID
		VIFs = append(VIFs, vIF)
	}
	return VIFs
}

func (v *GenesisSyncRpcUpdater) ParseVIP(orgID int, vtapID uint32, message *agent.GenesisSyncRequest) []model.GenesisVIP {
	var vips []model.GenesisVIP

	ipAddrs := message.GetPlatformData().GetRawIpAddrs()
	if len(ipAddrs) == 0 {
		log.Errorf("get sync data (raw ip addrs) empty", logger.NewORGPrefix(orgID))
		return []model.GenesisVIP{}
	}
	for _, ipAddr := range ipAddrs {
		parsedGlobalIPs, err := common.ParseIPOutput(strings.Trim(ipAddr, " "))
		if err != nil {
			log.Errorf("parse ip output error: (%s)", err, logger.NewORGPrefix(orgID))
			return []model.GenesisVIP{}
		}

		for _, item := range parsedGlobalIPs {
			if item.Name != "lo" {
				continue
			}
			for _, ip := range item.IPs {
				ipObj, err := netaddr.ParseIP(ip.Address)
				if err != nil {
					log.Warningf("parse lo vip (%s) field: (%s)", ip.Address, err, logger.NewORGPrefix(orgID))
					continue
				}
				if ipObj.IsLoopback() {
					continue
				}
				vips = append(vips, model.GenesisVIP{
					Lcuuid: ccommon.GetUUIDByOrgID(orgID, ip.Address+strconv.Itoa(int(vtapID))),
					IP:     ip.Address,
					VtapID: vtapID,
					NodeIP: v.nodeIP,
				})
			}
		}
	}
	return vips
}

func (v *GenesisSyncRpcUpdater) ParseHostAsVmPlatformInfo(orgID int, vtapID uint32, peer string, message *agent.GenesisSyncRequest) common.GenesisSyncDataResponse {
	hostName := strings.Trim(message.GetPlatformData().GetRawHostname(), " \n")
	if hostName == "" {
		log.Error("get sync data (raw hostname) empty", logger.NewORGPrefix(orgID))
		return common.GenesisSyncDataResponse{}
	}
	ipAddrs := message.GetPlatformData().GetRawIpAddrs()
	if len(ipAddrs) == 0 {
		log.Error("get sync data (raw ip addrs) empty", logger.NewORGPrefix(orgID))
		return common.GenesisSyncDataResponse{}
	}
	interfaces, err := common.ParseIPOutput(strings.Trim(ipAddrs[0], " "))
	if err != nil {
		log.Error(err.Error(), logger.NewORGPrefix(orgID))
		return common.GenesisSyncDataResponse{}
	}
	vpc := model.GenesisVpc{
		Name:   v.defaultVPCName,
		Lcuuid: ccommon.GetUUIDByOrgID(orgID, v.defaultVPCName),
		VtapID: vtapID,
		NodeIP: v.nodeIP,
	}
	// check if vm is behind NAT
	natIP := message.GetNatIp()
	behindNat := peer != natIP
	log.Infof("host (%s) nat ip is (%s) peer ip is (%s), behind nat: (%t), single vpc mode: (%t)", hostName, natIP, peer, behindNat, v.singleVPCMode, logger.NewORGPrefix(orgID))
	if behindNat && !v.singleVPCMode {
		vpc = model.GenesisVpc{
			Name:   "VPC-" + peer,
			Lcuuid: ccommon.GetUUIDByOrgID(orgID, "VPC-"+peer),
			VtapID: vtapID,
			NodeIP: v.nodeIP,
		}
	}
	vpcs := []model.GenesisVpc{vpc}

	vm := model.GenesisVM{
		Name:         hostName,
		Label:        hostName,
		Lcuuid:       ccommon.GetUUIDByOrgID(orgID, hostName),
		VPCLcuuid:    vpc.Lcuuid,
		LaunchServer: "127.0.0.1",
		State:        ccommon.VM_STATE_RUNNING,
		CreatedAt:    time.Now(),
		VtapID:       vtapID,
		NodeIP:       v.nodeIP,
	}
	vms := []model.GenesisVM{vm}

	nameToNetwork := map[string]model.GenesisNetwork{}
	ports := []model.GenesisPort{}
	ipLastSeens := []model.GenesisIP{}
	for _, iface := range interfaces {
		if iface.MAC == "" || iface.Name == "lo" {
			log.Debugf("not found mac or netcard is loopback (%#v)", iface, logger.NewORGPrefix(orgID))
			continue
		}
		ips := iface.IPs
		firstIP := common.VifInfo{}
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
				log.Error(err.Error(), logger.NewORGPrefix(orgID))
				return common.GenesisSyncDataResponse{}
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
		networkName := fmt.Sprintf("Network-%s/%v", firstIP.Address, firstIP.MaskLen)
		network, ok := nameToNetwork[networkName]
		vType := ccommon.VIF_TYPE_LAN
		netType := ccommon.NETWORK_TYPE_LAN
		if isExternal {
			vType = ccommon.VIF_TYPE_WAN
			netType = ccommon.NETWORK_TYPE_WAN
		}
		if !ok {
			network = model.GenesisNetwork{
				Lcuuid:         ccommon.GetUUIDByOrgID(orgID, networkName),
				Name:           networkName,
				SegmentationID: 1,
				VtapID:         vtapID,
				NodeIP:         v.nodeIP,
				VPCLcuuid:      vpc.Lcuuid,
				External:       isExternal,
				NetType:        uint32(netType),
			}
			nameToNetwork[networkName] = network
		}
		port := model.GenesisPort{
			Lcuuid:        ccommon.GetUUIDByOrgID(orgID, hostName+iface.MAC),
			Type:          uint32(vType),
			VtapID:        vtapID,
			NodeIP:        v.nodeIP,
			Mac:           iface.MAC,
			DeviceLcuuid:  vm.Lcuuid,
			NetworkLcuuid: network.Lcuuid,
			VPCLcuuid:     vm.VPCLcuuid,
			DeviceType:    ccommon.VIF_DEVICE_TYPE_VM,
		}
		ports = append(ports, port)
		for _, p := range ips {
			// ignore lin scope
			if p.Scope != "global" && p.Scope != "host" {
				continue
			}
			oIP, err := netaddr.ParseIP(p.Address)
			if err != nil {
				log.Warning(err.Error(), logger.NewORGPrefix(orgID))
				continue
			}
			ipLastSeen := model.GenesisIP{
				Lcuuid:           ccommon.GetUUIDByOrgID(orgID, hostName+oIP.String()+port.Lcuuid),
				VinterfaceLcuuid: port.Lcuuid,
				IP:               p.Address,
				Masklen:          p.MaskLen,
				VtapID:           vtapID,
				NodeIP:           v.nodeIP,
				LastSeen:         time.Now(),
			}
			ipLastSeens = append(ipLastSeens, ipLastSeen)
		}
	}
	networks := []model.GenesisNetwork{}
	for _, n := range nameToNetwork {
		networks = append(networks, n)
	}

	return common.GenesisSyncDataResponse{
		VMs:         vms,
		VPCs:        vpcs,
		Ports:       ports,
		Networks:    networks,
		IPLastSeens: ipLastSeens,
	}
}

func (v *GenesisSyncRpcUpdater) ParseProcessInfo(orgID int, vtapID uint32, message *agent.GenesisSyncRequest) []model.GenesisProcess {
	processes := []model.GenesisProcess{}
	if vtapID == 0 {
		return processes
	}

	for _, p := range message.GetProcessData().GetProcessEntries() {
		var osAppTagSlice []string
		for _, tag := range p.GetOsAppTags() {
			osAppTagSlice = append(osAppTagSlice, tag.GetKey()+":"+tag.GetValue())
		}
		osAppTagString := strings.Join(osAppTagSlice, ", ")
		startTime := time.Unix(int64(p.GetStartTime()), 0)
		pID := p.GetPid()
		processes = append(processes, model.GenesisProcess{
			Lcuuid:      ccommon.GetUUIDByOrgID(orgID, strconv.Itoa(int(pID))+strconv.Itoa(int(vtapID))),
			PID:         pID,
			NetnsID:     p.GetNetnsId(),
			Name:        p.GetName(),
			ProcessName: p.GetProcessName(),
			CMDLine:     p.GetCmdline(),
			UserName:    p.GetUser(),
			ContainerID: p.GetContainerId(),
			VtapID:      vtapID,
			NodeIP:      v.nodeIP,
			OSAPPTags:   osAppTagString,
			StartTime:   startTime,
		})
	}
	return processes
}

func (v *GenesisSyncRpcUpdater) ParseKVMPlatformInfo(orgID int, vtapID uint32, peer string, message *agent.GenesisSyncRequest) common.GenesisSyncDataResponse {
	rawVM := strings.Trim(message.GetPlatformData().GetRawAllVmXml(), " ")
	rawOVSInterface := strings.Trim(message.GetPlatformData().GetRawOvsInterfaces(), " ")
	rawOVSPorts := strings.Trim(message.GetPlatformData().GetRawOvsPorts(), " ")
	ovsMode := false
	if rawOVSPorts != "" {
		ovsMode = true
	}
	rawHostName := strings.Trim(message.GetPlatformData().GetRawHostname(), " \n")
	rawVMStates := strings.Trim(message.GetPlatformData().GetRawVmStates(), " ")
	rawBrctlShow := strings.Trim(message.GetPlatformData().GetRawBrctlShow(), " ")
	rawVlanConfig := strings.Trim(message.GetPlatformData().GetRawVlanConfig(), " ")
	tIPs := message.GetPlatformData().GetIps()
	if ovsMode {
		rawBrctlShow = ""
		rawVlanConfig = ""
	}

	hosts := []model.GenesisHost{
		model.GenesisHost{
			Hostname: rawHostName,
			Lcuuid:   ccommon.GetUUIDByOrgID(orgID, rawHostName),
			IP:       peer,
			VtapID:   vtapID,
			NodeIP:   v.nodeIP,
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
	nameToOvsPort, err := common.ParseCSVWithKey(rawOVSPorts, "name", pCSVkeys...)
	if err != nil {
		log.Warning("parse csv with key failed: "+err.Error(), logger.NewORGPrefix(orgID))
	}
	pCSVs := []string{"name", "external_ids"}
	nameToOvsIfs, err := common.ParseCSV(rawOVSInterface, pCSVs...)
	if err != nil {
		log.Warning("parse csv failed: "+err.Error(), logger.NewORGPrefix(orgID))
	}
	for _, nameToOvsIf := range nameToOvsIfs {
		name, ok := nameToOvsIf["name"]
		if !ok {
			continue
		}
		eIDs, err := common.ParseKVString(nameToOvsIf["external_ids"])
		if err != nil {
			log.Warning("parse kvstring failed: "+err.Error(), logger.NewORGPrefix(orgID))
		}
		mac, ok := eIDs["attached-mac"]
		if !ok {
			log.Debugf("ovs interface %s does not have external_ids:attached-mac", name, logger.NewORGPrefix(orgID))
			continue
		}
		if ovsPort, ok := nameToOvsPort[name]; ok {
			macToPort[mac] = ovsPort
		}
	}
	if !ovsMode {
		bridges, err := common.ParseBrctlShow(rawBrctlShow)
		if err != nil {
			log.Warning("parse brctl show failed: " + err.Error())
		}
		vlanConfig, err := common.ParseVLANConfig(rawVlanConfig)
		if err != nil {
			log.Warning("parse vlan config failed: "+err.Error(), logger.NewORGPrefix(orgID))
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
				uuid: ccommon.GetUUIDByOrgID(orgID, fmt.Sprintf("%d,%s", vlan, br)),
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
			log.Warningf("ip (%s) invalid", ipObj.String(), logger.NewORGPrefix(orgID))
			continue
		}
		ip.IP = ipObj.String()
		tIPLastSeen := tIP.GetLastSeen()
		ip.LastSeen = time.Unix(int64(tIPLastSeen), 0).Local()
		ip.VtapID = vtapID
		ip.NodeIP = v.nodeIP
		macStr := common.Uint64ToMac(tIP.GetMac()).String()
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
	vmStates, err := common.ParseVMStates(rawVMStates)
	if err != nil {
		log.Warning("parse vm states failed: "+err.Error(), logger.NewORGPrefix(orgID))
	}
	xmlVMs, err := common.ParseVMXml(rawVM, v.vmNameField)
	if err != nil {
		log.Warning("parse vm xml failed: "+err.Error(), logger.NewORGPrefix(orgID))
	}
	for _, xmlVM := range xmlVMs {
		vm := model.GenesisVM{}
		vm.VtapID = vtapID
		vm.NodeIP = v.nodeIP
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
			vm.State = uint32(ccommon.VM_STATE_EXCEPTION)
		}
		vms = append(vms, vm)

		if vm.State != uint32(ccommon.VM_STATE_RUNNING) {
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
					log.Debugf("vm %s interface %s mac %s not found in ovs ports", vm.Label, ifName, mac, logger.NewORGPrefix(orgID))
					continue
				}
				port.Lcuuid = portMap["_uuid"]
				options, err := common.ParseKVString(portMap["other_config"])
				if err != nil {
					log.Warning("parse kv string failed: "+err.Error(), logger.NewORGPrefix(orgID))
				}
				if nLcuuid, ok := options["net_uuid"]; ok {
					network.Lcuuid = nLcuuid
				} else {
					// tag不为数字时均默认为1
					tagInt, err := strconv.Atoi(portMap["tag"])
					if err != nil {
						tagInt = 1
					}
					network.Lcuuid = ccommon.GetUUIDByOrgID(orgID, strconv.Itoa(tagInt))
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
						uuid: ccommon.GetUUIDByOrgID(orgID, "yunshan-temp"),
						vlan: 1,
					}
				}
				port.Lcuuid = ccommon.GetUUIDByOrgID(orgID, mac)
				network.Lcuuid = br.uuid
				network.SegmentationID = uint32(br.vlan)
			}

			isExternal := false
			if isE, ok := macToIsExternalIP[mac]; ok {
				isExternal = isE
			}
			network.NetType = uint32(ccommon.NETWORK_TYPE_LAN)
			port.Type = uint32(ccommon.VIF_TYPE_LAN)
			if isExternal {
				network.NetType = uint32(ccommon.NETWORK_TYPE_WAN)
				port.Type = uint32(ccommon.VIF_TYPE_WAN)
			}
			network.External = isExternal
			network.VPCLcuuid = vm.VPCLcuuid
			network.VtapID = vtapID
			network.NodeIP = v.nodeIP
			network.Name = "subnet_vni_" + strconv.Itoa(int(network.SegmentationID))
			networkIDToNetwork[network.Lcuuid] = network
			port.Mac = mac
			port.DeviceLcuuid = vm.Lcuuid
			port.NetworkLcuuid = network.Lcuuid
			port.VPCLcuuid = vm.VPCLcuuid
			port.VtapID = vtapID
			port.NodeIP = v.nodeIP
			port.DeviceType = ccommon.VIF_DEVICE_TYPE_VM
			ports = append(ports, port)
			for _, ip := range macToIPs[mac] {
				ip.VinterfaceLcuuid = port.Lcuuid
				ip.Lcuuid = ccommon.GetUUIDByOrgID(orgID, ip.IP+ip.VinterfaceLcuuid)
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
		vpc.NodeIP = v.nodeIP
		vpc.Lcuuid = id
		vpc.Name = name
		vpcs = append(vpcs, vpc)
	}

	lldps := []model.GenesisLldp{}
	for _, l := range message.GetPlatformData().GetLldpInfo() {
		lldp := model.GenesisLldp{}
		lldp.Lcuuid = ccommon.GetUUIDByOrgID(orgID, peer+l.GetManagementAddress()+l.GetPortId())
		lldp.HostIP = peer
		lldp.VtapID = vtapID
		lldp.NodeIP = v.nodeIP
		lldp.HostInterface = l.GetInterface()
		lldp.SystemName = l.GetSystemName()
		lldp.ManagementAddress = l.GetManagementAddress()
		lldp.VinterfaceLcuuid = l.GetPortId()
		lldp.VinterfaceDescription = l.GetPortDescription()
		lldp.LastSeen = time.Now()
		lldps = append(lldps, lldp)
	}

	return common.GenesisSyncDataResponse{
		Hosts:       hosts,
		VMs:         vms,
		VPCs:        vpcs,
		Networks:    networks,
		Ports:       ports,
		IPLastSeens: ips,
		Lldps:       lldps,
	}
}

func (v *GenesisSyncRpcUpdater) UnmarshalProtobuf(orgID int, teamID, vtapID uint32, peer string, message *agent.GenesisSyncRequest) common.GenesisSyncDataResponse {
	genesisSyncData := common.GenesisSyncDataResponse{}
	if common.IPInRanges(peer, v.hostIPsRanges...) && message.GetPlatformData().GetPlatformEnabled() {
		genesisSyncData = v.ParseKVMPlatformInfo(orgID, vtapID, peer, message)
	}

	genesisSyncData.Processes = v.ParseProcessInfo(orgID, vtapID, message)
	genesisSyncData.Vinterfaces = v.ParseVinterfaceInfo(orgID, teamID, vtapID, peer, common.DEVICE_TYPE_KVM_HOST, message)

	return genesisSyncData
}

func (v *GenesisSyncRpcUpdater) UnmarshalKubernetesProtobuf(orgID int, teamID, vtapID uint32, peer string, message *agent.GenesisSyncRequest) common.GenesisSyncDataResponse {
	genesisSyncData := common.GenesisSyncDataResponse{}
	if message.GetPlatformData().GetPlatformEnabled() {
		genesisSyncData = v.ParseHostAsVmPlatformInfo(orgID, vtapID, peer, message)
	}

	genesisSyncData.Processes = v.ParseProcessInfo(orgID, vtapID, message)
	genesisSyncData.Vinterfaces = v.ParseVinterfaceInfo(orgID, teamID, vtapID, peer, common.DEVICE_TYPE_DOCKER_HOST, message)

	return genesisSyncData
}

func (v *GenesisSyncRpcUpdater) UnmarshalWorkloadProtobuf(orgID int, teamID, vtapID uint32, peer, deviceType string, message *agent.GenesisSyncRequest) common.GenesisSyncDataResponse {
	genesisSyncData := common.GenesisSyncDataResponse{}
	if message.GetPlatformData().GetPlatformEnabled() {
		genesisSyncData = v.ParseHostAsVmPlatformInfo(orgID, vtapID, peer, message)
	}

	genesisSyncData.VIPs = v.ParseVIP(orgID, vtapID, message)
	genesisSyncData.Processes = v.ParseProcessInfo(orgID, vtapID, message)
	genesisSyncData.Vinterfaces = v.ParseVinterfaceInfo(orgID, teamID, vtapID, peer, deviceType, message)

	return genesisSyncData
}
