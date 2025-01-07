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

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	libs "github.com/deepflowio/deepflow/server/libs/kubernetes"
	"github.com/deepflowio/deepflow/server/libs/utils"
	corev1 "k8s.io/api/core/v1"
)

const (
	TIMEOUT = 60
)

type Endpoint struct {
	Host string
	Port uint16
}

func (e Endpoint) String() string {
	// if it is an IPv6 address, it needs to be enclosed in []
	if strings.Contains(e.Host, ":") {
		return fmt.Sprintf("[%s]:%d", e.Host, e.Port)
	} else {
		return fmt.Sprintf("%s:%d", e.Host, e.Port)
	}
}

type Watcher struct {
	config                        *Config
	NodePodNamesWatch             *ServerInstanceInfo
	EndpointWatch                 libs.Watcher
	clickhouseEndpointKey         string
	clickhouseEndpointTCPPortName string
	myNodeName                    string
	myPodName                     string
	clickhouseIsExternal          bool
	myClickhouseEndpoints         []Endpoint
	lastNodePodNames              map[string][]string
	lastServerEndpointsMap        map[string][]Endpoint
}

type EndpointsOnChange interface {
	EndpointsChange([]string)
}

var clickhouseEndpointsOnChanges []EndpointsOnChange

func AddClickHouseEndpointsOnChange(onChange EndpointsOnChange) {
	clickhouseEndpointsOnChanges = append(clickhouseEndpointsOnChanges, onChange)
}

func clickhouseEndpointsChange(endpoints []Endpoint) {
	addrs := []string{}
	for _, endpoint := range endpoints {
		addrs = append(addrs, endpoint.String())
	}
	log.Infof("my ClickHouse endpoints chanage to %+v", addrs)
	for i := range clickhouseEndpointsOnChanges {
		clickhouseEndpointsOnChanges[i].EndpointsChange(addrs)
	}
}

func NewWatcher(cfg *Config, myNodeName, myPodName, myPodNamespace string) (*Watcher, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		errMsg := fmt.Errorf("get cluster config failed: %v", err)
		log.Warning(errMsg)
		return nil, errMsg
	}
	kubernetesClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		errMsg := fmt.Errorf("create kubernetes client failed: %v", err)
		log.Warning(errMsg)
		return nil, errMsg
	}

	endpointsWatcher, err := libs.StartCoreV1EndpointsWatcher(context.Background(), libs.NewKubernetesWatchClient(kubernetesClient), myPodNamespace)
	if err != nil {
		errMsg := fmt.Errorf("create endpoints watcher failed: %v", err)
		log.Warning(errMsg)
		return nil, errMsg
	}

	controllers := make([]net.IP, len(cfg.ControllerIPs))
	for i, ipString := range cfg.ControllerIPs {
		controllers[i] = net.ParseIP(ipString)
		if controllers[i].To4() != nil {
			controllers[i] = controllers[i].To4()
		}
	}
	nodePodNamesWatch := NewServerInstranceInfo(controllers, int(cfg.ControllerPort), cfg.GrpcBufferSize)

	watcher := &Watcher{
		config:                        cfg,
		NodePodNamesWatch:             nodePodNamesWatch,
		EndpointWatch:                 endpointsWatcher,
		clickhouseEndpointKey:         cfg.CKDB.Host,
		clickhouseEndpointTCPPortName: cfg.CKDB.EndpointTCPPortName,
		myNodeName:                    myNodeName,
		myPodName:                     myPodName,
		clickhouseIsExternal:          cfg.CKDB.External,
		lastNodePodNames:              make(map[string][]string),
		lastServerEndpointsMap:        make(map[string][]Endpoint),
	}

	go watcher.Run()

	return watcher, nil
}

func (w *Watcher) Run() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		var endpoints []Endpoint
		for {
			es, err := w.getMyExpectedClickhouseEndpoints()
			if err == nil {
				endpoints = es
				break
			}
			log.Info(err)
			time.Sleep(10 * time.Second)
		}

		if len(endpoints) > MaxClickHouseEndpointsPerServer {
			ignores := endpoints[MaxClickHouseEndpointsPerServer:]
			endpoints = endpoints[:MaxClickHouseEndpointsPerServer]
			log.Warningf("the count of my clickhouse endpoints is exceed %d, ingnore endpoints %+v",
				MaxClickHouseEndpointsPerServer,
				ignores)
		}

		if len(w.myClickhouseEndpoints) == 0 {
			w.myClickhouseEndpoints = endpoints
		}

		if !reflect.DeepEqual(endpoints, w.myClickhouseEndpoints) {
			log.Warningf("my clickhouse endpoints change from %v to %v", w.myClickhouseEndpoints, endpoints)
			w.myClickhouseEndpoints = endpoints
			w.config.CKDB.updateActualAddrs(endpoints)
			clickhouseEndpointsChange(endpoints)
		}
	}
}

func (w *Watcher) getMyClickhouseEndpointsExternal() ([]Endpoint, error) {
	podNames, err := w.getPodNames()
	if err != nil {
		return nil, err
	}
	endpoints, err := w.getEndpoints()
	if err != nil {
		return nil, err
	}

	return getMyClickhouseEndpoints(podNames, w.myPodName, endpoints)
}

// How to get my external clickhouse endpoint:
// 1, Input a list of all 'deepflow-server' pods, and sort by name to find the 'index' of myself pod in it
// 2. Input the list of all clickhouse endpoints, and sort by IP
// 3, my corresponding 'clickhouse endpoint' is on position 'index%len'  in the 'clickhouse endpoints list'
func getMyClickhouseEndpoints(podNames []string, myName string, endpoints []Endpoint) ([]Endpoint, error) {
	myIndex := utils.IndexOf(podNames, myName)
	if myIndex < 0 {
		return nil, fmt.Errorf("can't find my pod name(%s) in pods(%v)", myName, podNames)
	}
	myEndpoints := []Endpoint{}
	if len(podNames) >= len(endpoints) {
		myEndpoints = []Endpoint{endpoints[myIndex%len(endpoints)]}
	} else {
		for i := myIndex; i < len(endpoints); i += len(podNames) {
			myEndpoints = append(myEndpoints, endpoints[i])
		}
	}
	return myEndpoints, nil
}

func (w *Watcher) getMyClickhouseEndpointsInternal() ([]Endpoint, error) {
	nodePodNames, err := w.getNodePodNames()
	if err != nil {
		return nil, err
	}
	nodeEndpoints, err := w.getNodeEndpoints()
	if err != nil {
		return nil, err
	}

	serverEndpointMap := getServerEndpointMap(nodePodNames, nodeEndpoints)
	if !serverEndpointMapEqual(w.lastServerEndpointsMap, serverEndpointMap) {
		log.Infof("the correspondence between server pod and clickhouse endpoint change from %+v to  %+v", w.lastServerEndpointsMap, serverEndpointMap)
		w.lastServerEndpointsMap = serverEndpointMap
	}
	if endpoints, ok := serverEndpointMap[w.myNodeName+w.myPodName]; ok {
		return endpoints, nil
	}

	return nil, fmt.Errorf("Can't find my clickhouse endpoint, myNodeName: %s myPodName: %s", w.myNodeName, w.myPodName)
}

func (w *Watcher) getMyExpectedClickhouseEndpoints() ([]Endpoint, error) {
	if w.clickhouseIsExternal {
		return w.getMyClickhouseEndpointsExternal()
	} else {
		return w.getMyClickhouseEndpointsInternal()
	}
}

func (w *Watcher) GetMyClickhouseEndpoints() ([]Endpoint, error) {
	if len(w.myClickhouseEndpoints) != 0 {
		return w.myClickhouseEndpoints, nil
	}
	return nil, fmt.Errorf("can't find my clickhouse endpoint now, pleate wait later...")
}

func isInEndpoints(e Endpoint, es []Endpoint) bool {
	for _, endpoint := range es {
		if e == endpoint {
			return true
		}
	}
	return false
}

func (w *Watcher) GetClickhouseEndpointsWithoutMyself() ([]Endpoint, error) {
	endpoints, err := w.getEndpoints()
	if err != nil {
		return nil, err
	}
	endpointsWithoutMyself := []Endpoint{}
	for _, e := range endpoints {
		if isInEndpoints(e, w.myClickhouseEndpoints) {
			continue
		}
		endpointsWithoutMyself = append(endpointsWithoutMyself, e)
	}
	return endpointsWithoutMyself, nil
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func nodePodNamesEqual(a, b map[string][]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if !stringsEqual(v, b[k]) {
			return false
		}
	}
	return true
}

func (w *Watcher) getNodePodNames() (map[string][]string, error) {
	nodePodNames := w.NodePodNamesWatch.GetNodePodNames()
	if len(nodePodNames) == 0 {
		return nil, fmt.Errorf("get server pod names empty")
	}

	if !nodePodNamesEqual(nodePodNames, w.lastNodePodNames) {
		log.Warningf("server node pod names change from '%v' to '%v'", w.lastNodePodNames, nodePodNames)
		w.lastNodePodNames = nodePodNames
	}

	return nodePodNames, nil
}

func (w *Watcher) getPodNames() ([]string, error) {
	nodePodNames, err := w.getNodePodNames()
	if err != nil {
		return nil, err
	}

	allPodNames := []string{}
	for _, podNames := range nodePodNames {
		allPodNames = append(allPodNames, podNames...)
	}
	sort.Slice(allPodNames, func(i, j int) bool {
		return allPodNames[i] < allPodNames[j]
	})

	return allPodNames, nil
}

func (w *Watcher) getNodeEndpoints() (map[string][]Endpoint, error) {
	for i := 0; i < TIMEOUT; i++ {
		entries := w.EndpointWatch.Entries()
		nodeEndpoints := make(map[string][]Endpoint)
		for _, v := range entries {
			e, ok := v.(*corev1.Endpoints)
			if !ok {
				continue
			}
			ep := e.GetName()
			if ep != w.clickhouseEndpointKey {
				continue
			}
			for _, v := range e.Subsets {
				port := uint16(0)
				for _, p := range v.Ports {
					if p.Name == w.clickhouseEndpointTCPPortName {
						port = uint16(p.Port)
						break
					}
				}
				if port == 0 {
					continue
				}
				for _, v := range v.Addresses {
					nodeName := ""
					if v.NodeName != nil {
						nodeName = *v.NodeName
					}
					nodeEndpoints[nodeName] = append(nodeEndpoints[nodeName], Endpoint{v.IP, port})
				}
			}
		}

		if len(nodeEndpoints) == 0 {
			time.Sleep(time.Second)
			continue
		}

		log.Debugf("get node endpoints %+v", nodeEndpoints)
		return nodeEndpoints, nil
	}
	return nil, fmt.Errorf("get endpoint(%s) empty, timeout is %d", w.clickhouseEndpointKey, TIMEOUT)
}

func (w *Watcher) getEndpoints() ([]Endpoint, error) {
	nodeEndpoints, err := w.getNodeEndpoints()
	if err != nil {
		return nil, err
	}
	return getAllEndpoints(nodeEndpoints), nil
}

func getAllEndpoints(nodeEndpointsMap map[string][]Endpoint) []Endpoint {
	allEndpoints := make([]Endpoint, 0)
	for _, endpoints := range nodeEndpointsMap {
		allEndpoints = append(allEndpoints, endpoints...)
	}

	sort.Slice(allEndpoints, func(i, j int) bool {
		return allEndpoints[i].Host < allEndpoints[j].Host
	})
	return allEndpoints
}

func getAllServers(nodePodNamesMap map[string][]string) []string {
	allServers := make([]string, 0)
	for node, podNames := range nodePodNamesMap {
		for _, podName := range podNames {
			allServers = append(allServers, node+podName)
		}
	}

	sort.Slice(allServers, func(i, j int) bool {
		return allServers[i] < allServers[j]
	})
	return allServers
}

func getUnusedEndpoints(allEndpoints, matchedEndpoints []Endpoint) []Endpoint {
	unmatchs := make([]Endpoint, 0)
	for _, v := range allEndpoints {
		match := false
		for _, ep := range matchedEndpoints {
			if v == ep {
				match = true
			}
		}
		if !match {
			unmatchs = append(unmatchs, v)
		}
	}
	sort.Slice(unmatchs, func(i, j int) bool {
		return unmatchs[i].Host < unmatchs[j].Host
	})
	return unmatchs
}

func serverEndpointMapEqual(a, b map[string][]Endpoint) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if !reflect.DeepEqual(v, b[k]) {
			return false
		}
	}
	return true
}

func getServerEndpointMap(nodePodNamesMap map[string][]string, nodeEndpointsMap map[string][]Endpoint) map[string][]Endpoint {
	serverEndpointMap := make(map[string][]Endpoint)
	unassignedServers := make([]string, 0)
	usedEndpoints := make([]Endpoint, 0)
	// 1.Prioritize allocating Endpoint on the same Node
	for nodeName, podNames := range nodePodNamesMap {
		endpoints := nodeEndpointsMap[nodeName]
		endpointsCount := len(endpoints)
		for i, podName := range podNames {
			if endpointsCount > 0 {
				endpoint := endpoints[i%endpointsCount]
				serverEndpointMap[nodeName+podName] = append(serverEndpointMap[nodeName+podName], endpoint)
				usedEndpoints = append(usedEndpoints, endpoint)
			} else {
				unassignedServers = append(unassignedServers, nodeName+podName)
			}
		}
	}

	allEndpoints := getAllEndpoints(nodeEndpointsMap)
	allEndpointsCount := len(allEndpoints)
	if allEndpointsCount == 0 {
		return serverEndpointMap
	}

	unusedEndpoints := getUnusedEndpoints(allEndpoints, usedEndpoints)
	unusedEndpointsCount := len(unusedEndpoints)

	if len(unassignedServers) == 0 && unusedEndpointsCount == 0 {
		return serverEndpointMap
	}

	// 2.Get unassigned servers and endpoints, sort them separately, and assign them in order.
	sort.Slice(unassignedServers, func(i, j int) bool {
		return unassignedServers[i] < unassignedServers[j]
	})

	for i, key := range unassignedServers {
		if unusedEndpointsCount > 0 {
			serverEndpointMap[key] = append(serverEndpointMap[key], unusedEndpoints[0])
			unusedEndpoints = unusedEndpoints[1:]
			unusedEndpointsCount = len(unusedEndpoints)
		} else {
			serverEndpointMap[key] = append(serverEndpointMap[key], allEndpoints[i%allEndpointsCount])
		}
	}

	if unusedEndpointsCount > 0 {
		allServers := getAllServers(nodePodNamesMap)
		allServersCount := len(allServers)
		if allServersCount == 0 {
			return serverEndpointMap
		}

		for i, unusedEndpoint := range unusedEndpoints {
			server := allServers[i%allServersCount]
			serverEndpointMap[server] = append(serverEndpointMap[server], unusedEndpoint)
		}
	}

	return serverEndpointMap
}
