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

package agent_config

import _ "embed"

//go:embed example.yaml
var YamlAgentGroupConfig []byte

type AgentGroupConfig struct {
	VTapGroupID                       *string       `json:"VTAP_GROUP_ID" yaml:"vtap_group_id,omitempty"`
	VTapGroupLcuuid                   *string       `json:"VTAP_GROUP_LCUUID" yaml:"vtap_group_lcuuid,omitempty"`
	MaxCollectPps                     *int          `json:"MAX_COLLECT_PPS" yaml:"max_collect_pps,omitempty"`
	MaxNpbBps                         *int64        `json:"MAX_NPB_BPS" yaml:"max_npb_bps,omitempty"` // unit: bps
	MaxCPUs                           *int          `json:"MAX_CPUS" yaml:"max_cpus,omitempty"`
	MaxMilliCPUs                      *int          `json:"MAX_MILLICPUS" yaml:"max_millicpus,omitempty"`
	MaxMemory                         *int          `json:"MAX_MEMORY" yaml:"max_memory,omitempty"` // unit: M
	SyncInterval                      *int          `json:"SYNC_INTERVAL" yaml:"sync_interval,omitempty"`
	PlatformSyncInterval              *int          `json:"PLATFORM_SYNC_INTERVAL" yaml:"platform_sync_interval,omitempty"`
	StatsInterval                     *int          `json:"STATS_INTERVAL" yaml:"stats_interval,omitempty"`
	RsyslogEnabled                    *int          `json:"RSYSLOG_ENABLED" yaml:"rsyslog_enabled,omitempty"` // 0: disabled 1:enabled
	SystemLoadCircuitBreakerThreshold *float32      `json:"SYSTEM_LOAD_CIRCUIT_BREAKER_THRESHOLD" yaml:"system_load_circuit_breaker_threshold,omitempty"`
	SystemLoadCircuitBreakerRecover   *float32      `json:"SYSTEM_LOAD_CIRCUIT_BREAKER_RECOVER" yaml:"system_load_circuit_breaker_recover,omitempty"`
	SystemLoadCircuitBreakerMetric    *string       `json:"SYSTEM_LOAD_CIRCUIT_BREAKER_METRIC" yaml:"system_load_circuit_breaker_metric,omitempty"`
	MaxTxBandwidth                    *int64        `json:"MAX_TX_BANDWIDTH" yaml:"max_tx_bandwidth,omitempty"` // unit: bps
	BandwidthProbeInterval            *int          `json:"BANDWIDTH_PROBE_INTERVAL" yaml:"bandwidth_probe_interval,omitempty"`
	TapInterfaceRegex                 *string       `json:"TAP_INTERFACE_REGEX" yaml:"tap_interface_regex,omitempty"`
	MaxEscapeSeconds                  *int          `json:"MAX_ESCAPE_SECONDS" yaml:"max_escape_seconds,omitempty"`
	Mtu                               *int          `json:"MTU" yaml:"mtu,omitempty"`
	OutputVlan                        *int          `json:"OUTPUT_VLAN" yaml:"output_vlan,omitempty"`
	CollectorSocketType               *string       `json:"COLLECTOR_SOCKET_TYPE" yaml:"collector_socket_type,omitempty"`
	CompressorSocketType              *string       `json:"COMPRESSOR_SOCKET_TYPE" yaml:"compressor_socket_type,omitempty"`
	NpbSocketType                     *string       `json:"NPB_SOCKET_TYPE" yaml:"npb_socket_type,omitempty"`
	NpbVlanMode                       *int          `json:"NPB_VLAN_MODE" yaml:"npb_vlan_mode,omitempty"`
	CollectorEnabled                  *int          `json:"COLLECTOR_ENABLED" yaml:"collector_enabled,omitempty"`             // 0: disabled 1:enabled
	VTapFlow1sEnabled                 *int          `json:"VTAP_FLOW_1S_ENABLED" yaml:"vtap_flow_1s_enabled,omitempty"`       // 0: disabled 1:enabled
	L4LogTapTypes                     []int         `json:"L4_LOG_TAP_TYPES" yaml:"l4_log_tap_types,omitempty"`               // tap type info, separate by ","
	L4LogIgnoreTapSides               []int         `json:"L4_LOG_IGNORE_TAP_SIDES" yaml:"l4_log_ignore_tap_sides,omitempty"` // separate by ","
	L7LogIgnoreTapSides               []int         `json:"L7_LOG_IGNORE_TAP_SIDES" yaml:"l7_log_ignore_tap_sides,omitempty"` // separate by ","
	NpbDedupEnabled                   *int          `json:"NPB_DEDUP_ENABLED" yaml:"npb_dedup_enabled,omitempty"`             // 0: disabled 1:enabled
	PlatformEnabled                   *int          `json:"PLATFORM_ENABLED" yaml:"platform_enabled,omitempty"`               // 0: disabled 1:enabled
	IfMacSource                       *int          `json:"IF_MAC_SOURCE" yaml:"if_mac_source,omitempty"`                     // 0: 接口MAC 1: 接口名称 2: 虚拟机MAC解析
	VMXMLPath                         *string       `json:"VM_XML_PATH" yaml:"vm_xml_path,omitempty"`
	ExtraNetnsRegex                   *string       `json:"EXTRA_NETNS_REGEX" yaml:"extra_netns_regex,omitempty"`
	NatIPEnabled                      *int          `json:"NAT_IP_ENABLED" yaml:"nat_ip_enabled,omitempty"` // 0: disabled 1:enabled
	CapturePacketSize                 *int          `json:"CAPTURE_PACKET_SIZE" yaml:"capture_packet_size,omitempty"`
	InactiveServerPortEnabled         *int          `json:"INACTIVE_SERVER_PORT_ENABLED" yaml:"inactive_server_port_enabled,omitempty"` // 0: disabled 1:enabled
	InactiveIPEnabled                 *int          `json:"INACTIVE_IP_ENABLED" yaml:"inactive_ip_enabled,omitempty"`                   // 0: disabled 1:enabled
	LogThreshold                      *int          `json:"LOG_THRESHOLD" yaml:"log_threshold,omitempty"`
	LogLevel                          *string       `json:"LOG_LEVEL" yaml:"log_level,omitempty"`
	LogRetention                      *int          `json:"LOG_RETENTION" yaml:"log_retention,omitempty"`
	HTTPLogProxyClient                *string       `json:"HTTP_LOG_PROXY_CLIENT" yaml:"http_log_proxy_client,omitempty"`
	HTTPLogTraceID                    *string       `json:"HTTP_LOG_TRACE_ID" yaml:"http_log_trace_id,omitempty"`
	L7LogPacketSize                   *int          `json:"L7_LOG_PACKET_SIZE" yaml:"l7_log_packet_size,omitempty"`
	L4LogCollectNpsThreshold          *int          `json:"L4_LOG_COLLECT_NPS_THRESHOLD" yaml:"l4_log_collect_nps_threshold,omitempty"`
	L7LogCollectNpsThreshold          *int          `json:"L7_LOG_COLLECT_NPS_THRESHOLD" yaml:"l7_log_collect_nps_threshold,omitempty"`
	L7MetricsEnabled                  *int          `json:"L7_METRICS_ENABLED" yaml:"l7_metrics_enabled,omitempty"`         // 0: disabled 1:enabled
	L7LogStoreTapTypes                []int         `json:"L7_LOG_STORE_TAP_TYPES" yaml:"l7_log_store_tap_types,omitempty"` // l7 log store tap types, separate by ","
	CaptureSocketType                 *int          `json:"CAPTURE_SOCKET_TYPE" yaml:"capture_socket_type,omitempty"`
	CaptureBpf                        *string       `json:"CAPTURE_BPF" yaml:"capture_bpf,omitempty"`
	TapMode                           *int          `json:"TAP_MODE" yaml:"tap_mode,omitempty"`
	ThreadThreshold                   *int          `json:"THREAD_THRESHOLD" yaml:"thread_threshold,omitempty"`
	ProcessThreshold                  *int          `json:"PROCESS_THRESHOLD" yaml:"process_threshold,omitempty"`
	Lcuuid                            *string       `json:"LCUUID" yaml:"-"`
	NtpEnabled                        *int          `json:"NTP_ENABLED" yaml:"ntp_enabled,omitempty"`                         // 0: disabled 1:enabled
	L4PerformanceEnabled              *int          `json:"L4_PERFORMANCE_ENABLED" yaml:"l4_performance_enabled,omitempty"`   // 0: disabled 1:enabled
	PodClusterInternalIP              *int          `json:"POD_CLUSTER_INTERNAL_IP" yaml:"pod_cluster_internal_ip,omitempty"` // 0:  1:
	Domains                           []string      `json:"DOMAINS" yaml:"domains,omitempty"`                                 // domains info, separate by ","
	DecapType                         []int         `json:"DECAP_TYPE" yaml:"decap_type,omitempty"`                           // separate by ","
	HTTPLogSpanID                     *string       `json:"HTTP_LOG_SPAN_ID" yaml:"http_log_span_id,omitempty"`
	SysFreeMemoryLimit                *int          `json:"SYS_FREE_MEMORY_LIMIT" yaml:"sys_free_memory_limit,omitempty"` // unit: %
	LogFileSize                       *int          `json:"LOG_FILE_SIZE" yaml:"log_file_size,omitempty"`                 // unit: MB
	HTTPLogXRequestID                 *string       `json:"HTTP_LOG_X_REQUEST_ID" yaml:"http_log_x_request_id,omitempty"`
	ExternalAgentHTTPProxyEnabled     *int          `json:"EXTERNAL_AGENT_HTTP_PROXY_ENABLED" yaml:"external_agent_http_proxy_enabled,omitempty"`
	ExternalAgentHTTPProxyPort        *int          `json:"EXTERNAL_AGENT_HTTP_PROXY_PORT" yaml:"external_agent_http_proxy_port,omitempty"`
	AnalyzerPort                      *int          `json:"ANALYZER_PORT" yaml:"analyzer_port,omitempty"`
	ProxyControllerPort               *int          `json:"PROXY_CONTROLLER_PORT" yaml:"proxy_controller_port,omitempty"`
	ProxyControllerIP                 *string       `json:"PROXY_CONTROLLER_IP" yaml:"proxy_controller_ip,omitempty"`
	AnalyzerIP                        *string       `json:"ANALYZER_IP" yaml:"analyzer_ip,omitempty"`
	WasmPlugins                       []string      `json:"WASM_PLUGINS" yaml:"wasm_plugins,omitempty"`
	SoPlugins                         []string      `json:"SO_PLUGINS" yaml:"so_plugins,omitempty"`
	YamlConfig                        *StaticConfig `yaml:"static_config,omitempty"`
}

type BondGroup struct {
	TapInterfaces []string `yaml:"tap-interfaces,omitempty"`
}

type StaticConfig struct {
	ProxyControllerPort                *uint16                      `yaml:"proxy-controller-port,omitempty"`
	LogLevel                           *string                      `yaml:"log-level,omitempty"`
	Profiler                           *bool                        `yaml:"profiler,omitempty"`
	AfpacketBlocksEnabled              *bool                        `yaml:"afpacket-blocks-enabled,omitempty"`
	AfpacketBlocks                     *int                         `yaml:"afpacket-blocks,omitempty"`
	AnalyzerRawPacketBlockSize         *int                         `yaml:"analyzer-raw-packet-block-size,omitempty"`
	BatchedBufferSizeLimit             *int                         `yaml:"batched-buffer-size-limit,omitempty"`
	EnableDebugStats                   *bool                        `yaml:"enable-debug-stats,omitempty"`
	AnalyzerDedupDisabled              *bool                        `yaml:"analyzer-dedup-disabled,omitempty"`
	DefaultTapType                     *uint32                      `yaml:"default-tap-type,omitempty"`
	DebugListenPort                    *uint16                      `yaml:"debug-listen-port,omitempty"`
	EnableQosBypass                    *bool                        `yaml:"enable-qos-bypass,omitempty"`
	FastPathMapSize                    *int                         `yaml:"fast-path-map-size,omitempty"`
	FirstPathLevel                     *int                         `yaml:"first-path-level,omitempty"`
	LocalDispatcherCount               *int                         `yaml:"local-dispatcher-count,omitempty"`
	PacketFanoutMode                   *int                         `yaml:"packet-fanout-mode,omitempty"`
	SrcInterfaces                      []string                     `yaml:"src-interfaces,omitempty"`
	TapInterfaceBondGroup              []BondGroup                  `yaml:"tap-interface-bond-groups,omitempty"`
	CloudGatewayTraffic                *bool                        `yaml:"cloud-gateway-traffic,omitempty"`
	MirrorTrafficPcp                   *uint16                      `yaml:"mirror-traffic-pcp,omitempty"`
	PCap                               *PCapConfig                  `yaml:"pcap,omitempty"`
	Flow                               *FlowGeneratorConfig         `yaml:"flow,omitempty"`
	FlowQueueSize                      *int                         `yaml:"flow-queue-size,omitempty"`
	QuadrupleQueueSize                 *int                         `yaml:"quadruple-queue-size,omitempty"`
	AnalyzerQueueSize                  *int                         `yaml:"analyzer-queue-size,omitempty"`
	DpdkEnabled                        *bool                        `yaml:"dpdk-enabled,omitempty"`
	LibpcapEnabled                     *bool                        `yaml:"libpcap-enabled,omitempty"`
	VhostSocketPath                    *string                      `yaml:"vhost-socket-path,omitempty"`
	XflowCollector                     *XflowCollectorConfig        `yaml:"xflow-collector,omitempty"`
	NpbPort                            *uint16                      `yaml:"npb-port,omitempty"`
	VxlanFlags                         *uint8                       `yaml:"vxlan-flags,omitempty"`
	IgnoreOverlayVlan                  *bool                        `yaml:"ignore-overlay-vlan,omitempty"`
	CollectorSenderQueueSize           *int                         `yaml:"collector-sender-queue-size,omitempty"`
	CollectorSenderQueueCount          *int                         `yaml:"collector-sender-queue-count,omitempty"`
	ToaSenderQueueSize                 *int                         `yaml:"toa-sender-queue-size,omitempty"`
	ToaLruCacheSize                    *int                         `yaml:"toa-lru-cache-size,omitempty"`
	FlowSenderQueueSize                *int                         `yaml:"flow-sender-queue-size,omitempty"`
	FlowSenderQueueCount               *int                         `yaml:"flow-sender-queue-count,omitempty"`
	SecondFlowExtraDelaySecond         *string                      `yaml:"second-flow-extra-delay-second,omitempty"`
	PacketDelay                        *string                      `yaml:"packet-delay,omitempty"`
	Triple                             *TripleMapConfig             `yaml:"triple,omitempty"`
	KubernetesPollerType               *string                      `yaml:"kubernetes-poller-type,omitempty"`
	TrimTunnelTypes                    []string                     `yaml:"trim-tunnel-types,omitempty"`
	AnalyzerIp                         *string                      `yaml:"analyzer-ip,omitempty"`
	AnalyzerPort                       *uint16                      `yaml:"analyzer-port,omitempty"`
	KubernetesNamespace                *string                      `yaml:"kubernetes-namespace,omitempty"`
	KubernetesAPIListLimit             *uint32                      `yaml:"kubernetes-api-list-limit,omitempty"`
	KubernetesAPIListInterval          *string                      `yaml:"kubernetes-api-list-interval,omitempty"`
	KubernetesResources                []KubernetesResourceConfig   `yaml:"kubernetes-resources,omitempty"`
	IngressFlavour                     *string                      `yaml:"ingress-flavour,omitempty"`
	GrpcBufferSize                     *int                         `yaml:"grpc-buffer-size,omitempty"`            // 单位：M
	L7LogSessionAggrTimeout            *string                      `yaml:"l7-log-session-aggr-timeout,omitempty"` // 单位: s
	L7LogSessionSlotCapacity           *int                         `yaml:"l7-log-session-slot-capacity,omitempty"`
	TapMacScript                       *string                      `yaml:"tap-mac-script,omitempty"`
	BpfDisabled                        *bool                        `yaml:"bpf-disabled,omitempty"`
	L7ProtocolInferenceMaxFailCount    *uint64                      `yaml:"l7-protocol-inference-max-fail-count,omitempty"`
	L7ProtocolInferenceTtl             *uint64                      `yaml:"l7-protocol-inference-ttl,omitempty"`
	OracleParseConfig                  *OracleConfig                `yaml:"oracle-parse-config,omitempty"`
	PacketSequenceBlockSize            *int                         `yaml:"packet-sequence-block-size,omitempty"`
	PacketSequenceQueueSize            *int                         `yaml:"packet-sequence-queue-size,omitempty"`
	PacketSequenceQueueCount           *int                         `yaml:"packet-sequence-queue-count,omitempty"`
	PacketSequenceFlag                 *uint8                       `yaml:"packet-sequence-flag,omitempty"`
	L7ProtocolEnabled                  []string                     `yaml:"l7-protocol-enabled,omitempty"`
	StandaloneDataFileSize             *uint64                      `yaml:"standalone-data-file-size,omitempty"`
	StandaloneDataFileDir              *string                      `yaml:"standalone-data-file-dir,omitempty"`
	LogFile                            *string                      `yaml:"log-file,omitempty"`
	ExternalAgentHttpProxyCompressed   *bool                        `yaml:"external-agent-http-proxy-compressed,omitempty"`
	FeatureFlags                       []string                     `yaml:"feature-flags,omitempty"`
	L7ProtocolPorts                    map[string]string            `yaml:"l7-protocol-ports,omitempty"`
	L7LogBlacklist                     map[string][]*L7LogBlacklist `yaml:"l7-log-blacklist,omitempty"`
	L7ProtocolAdvancedFeatures         *L7ProtocolAdvancedFeatures  `yaml:"l7-protocol-advanced-features,omitempty"`
	ConsistentTimestampInL7Metrics     *bool                        `yaml:"consistent-timestamp-in-l7-metrics,omitempty"`
	ServerPorts                        []uint16                     `yaml:"server-ports,omitempty"`
	Ebpf                               *EbpfConfig                  `yaml:"ebpf,omitempty"`
	OsAppTagExecUser                   *string                      `yaml:"os-app-tag-exec-user,omitempty"`
	OsAppTagExec                       []string                     `yaml:"os-app-tag-exec,omitempty"`
	OsProcRoot                         *string                      `yaml:"os-proc-root,omitempty"`
	OsProcSocketSyncInterval           *int                         `yaml:"os-proc-socket-sync-interval,omitempty"`
	OsProcSocketMinLifetime            *int                         `yaml:"os-proc-socket-min-lifetime,omitempty"`
	OsProcRegex                        []*OsProcRegex               `yaml:"os-proc-regex,omitempty"`
	OsProcSyncEnabled                  *bool                        `yaml:"os-proc-sync-enabled,omitempty"`
	OsProcSyncTaggedOnly               *bool                        `yaml:"os-proc-sync-tagged-only,omitempty"`
	GuardInterval                      *string                      `yaml:"guard-interval,omitempty"`
	MaxSockets                         *int                         `yaml:"max-sockets,omitempty"`
	MaxSocketsTolerateInterval         *string                      `yaml:"max-sockets-tolerate-interval,omitempty"`
	CheckCoreFileDisabled              *bool                        `yaml:"check-core-file-disabled,omitempty"`
	SoPlugins                          []string                     `yaml:"so-plugins,omitempty"`
	MemoryTrimDisabled                 *bool                        `yaml:"memory-trim-disabled,omitempty"`
	PageCacheReclaimPercentage         *int                         `yaml:"page-cache-reclaim-percentage,omitempty"`
	FastPathDisabled                   *bool                        `yaml:"fast-path-disabled,omitempty"`
	ForwardCapacity                    *uint32                      `yaml:"forward-capacity,omitempty"`
	RrtTcpTimeout                      *string                      `yaml:"rrt-tcp-timeout,omitempty"`
	RrtUdpTimeout                      *string                      `yaml:"rrt-udp-timeout,omitempty"`
	PrometheusExtraConfig              *PrometheusExtraConfig       `yaml:"prometheus-extra-config,omitempty"`
	ProcessSchedulingPriority          *int8                        `yaml:"process-scheduling-priority,omitempty"`
	CpuAffinity                        *string                      `yaml:"cpu-affinity,omitempty"`
	ExternalProfileIntegrationDisabled *bool                        `yaml:"external-profile-integration-disabled,omitempty"`
	ExternalTraceIntegrationDisabled   *bool                        `yaml:"external-trace-integration-disabled,omitempty"`
	ExternalMetricIntegrationDisabled  *bool                        `yaml:"external-metric-integration-disabled,omitempty"`
	ExternalLogIntegrationDisabled     *bool                        `yaml:"external-log-integration-disabled,omitempty"`
	NtpMaxInterval                     *string                      `yaml:"ntp-max-interval,omitempty"`
	NtpMinInterval                     *string                      `yaml:"ntp-min-interval,omitempty"`
	DispatcherQueue                    *bool                        `yaml:"dispatcher-queue,omitempty"`
	EbpfCollectorQueueSize             *int                         `yaml:"ebpf-collector-queue-size,omitempty"`
}

type XflowCollectorConfig struct {
	SflowPorts   []string `yaml:"sflow-ports,omitempty"`
	NetflowPorts []string `yaml:"netflow-ports,omitempty"`
}

type PCapConfig struct {
	QueueSize      *int    `yaml:"queue-size,omitempty"`
	QueueCount     *int    `yaml:"queue-count,omitempty"`
	FlowBufferSize *int    `yaml:"flow-buffer-size,omitempty"`
	BufferSize     *int    `yaml:"buffer-size,omitempty"`
	FlushInterval  *string `yaml:"flush-interval,omitempty"`
}

type TripleMapConfig struct {
	HashSlots *int `yaml:"hash-slots-size,omitempty"`
	Capacity  *int `yaml:"capacity,omitempty"`
}

type TcpTimeoutConfig struct {
	EstablishedTimeout *string `yaml:"established-timeout,omitempty"`
	ClosingRstTimeout  *string `yaml:"closing-rst-timeout,omitempty"`
	OthersTimeout      *string `yaml:"others-timeout,omitempty"`
	OpeningRstTimeout  *string `yaml:"opening-rst-timeout,omitempty"`
}

type FlowGeneratorConfig struct {
	TcpTimeoutConfig `yaml:",inline"`
	HashSlots        *int    `yaml:"flow-slots-size,omitempty"`
	Capacity         *int    `yaml:"flow-count-limit,omitempty"`
	FlushInterval    *string `yaml:"flush-interval,omitempty"`
	AggrQueueSize    *int    `yaml:"flow-aggr-queue-size,omitempty"`
	MemoryPoolSize   *int    `yaml:"memory-pool-size,omitempty"`

	IgnoreTorMac  *bool `yaml:"ignore-tor-mac,omitempty"`
	IgnoreL2End   *bool `yaml:"ignore-l2-end,omitempty"`
	IgnoreIdcVlan *bool `yaml:"ignore-idc-vlan,omitempty"`
}

type EbpfUprobeProcessNameRegexsConfig struct {
	GolangSymbol *string `yaml:"golang-symbol,omitempty"`
	Golang       *string `yaml:"golang,omitempty"`
	Openssl      *string `yaml:"openssl,omitempty"`
}

type EbpfKprobePortlist struct {
	PortList string `yaml:"port-list,omitempty"`
}

type OnCpuProfile struct {
	Disabled  *bool   `yaml:"disabled,omitempty"`
	Frequency *int    `yaml:"frequency,omitempty"`
	Cpu       *int    `yaml:"cpu,omitempty"`
	Regex     *string `yaml:"regex,omitempty"`
}

type OffCpuProfile struct {
	Disabled *bool   `yaml:"disabled,omitempty"`
	Regex    *string `yaml:"regex,omitempty"`
	Cpu      *int    `yaml:"cpu,omitempty"`
	MinBlock *string `yaml:"minblock,omitempty"`
}

type EbpfConfig struct {
	Disabled                           *bool                              `yaml:"disabled,omitempty"`
	GlobalEbpfPpsThreshold             *int                               `yaml:"global-ebpf-pps-threshold,omitempty"`
	UprobeProcessNameRegexs            *EbpfUprobeProcessNameRegexsConfig `yaml:"uprobe-process-name-regexs,omitempty"`
	KprobeWhitelist                    *EbpfKprobePortlist                `yaml:"kprobe-whitelist,omitempty"`
	KprobeBlacklist                    *EbpfKprobePortlist                `yaml:"kprobe-blacklist,omitempty"`
	ThreadNum                          *int                               `yaml:"thread-num,omitempty"`
	PerfPagesCount                     *int                               `yaml:"perf-pages-count,omitempty"`
	RingSize                           *int                               `yaml:"ring-size,omitempty"`
	MaxSocketEntries                   *int                               `yaml:"max-socket-entries,omitempty"`
	MaxTraceEntries                    *int                               `yaml:"max-trace-entries,omitempty"`
	SocketMapMaxReclaim                *int                               `yaml:"socket-map-max-reclaim,omitempty"`
	GoTracingTimeout                   *int                               `yaml:"go-tracing-timeout,omitempty"`
	IOEventCollectMode                 *int                               `yaml:"io-event-collect-mode,omitempty"`
	IOEventMinimalDuration             *string                            `yaml:"io-event-minimal-duration,omitempty"`
	JavaSymbolFileRefreshDeferInterval *string                            `yaml:"java-symbol-file-refresh-defer-interval,omitempty"`
	OnCpuProfile                       *OnCpuProfile                      `yaml:"on-cpu-profile,omitempty"`
	OffCpuProfile                      *OffCpuProfile                     `yaml:"off-cpu-profile,omitempty"`
	SyscallOutOfOrderReassembly        []string                           `yaml:"syscall-out-of-order-reassembly,omitempty"`
	SyscallSegmentationReassembly      []string                           `yaml:"syscall-segmentation-reassembly,omitempty"`
	SyscallOutOfOrderCacheSize         *int                               `yaml:"syscall-out-of-order-cache-size,omitempty"`
}

type OsProcRegex struct {
	MatchRegex  *string `yaml:"match-regex,omitempty"`
	MatchType   *string `yaml:"match-type,omitempty"`
	Action      *string `yaml:"action,omitempty"`
	RewriteName *string `yaml:"rewrite-name,omitempty"`
}

type PrometheusExtraConfig struct {
	Enabled     *bool    `yaml:"enabled,omitempty"`
	Labels      []string `yaml:"labels,omitempty"`
	LabelsLimit *int     `yaml:"labels-limit,omitempty"`
	ValuesLimit *int     `yaml:"values-limit,omitempty"`
}

type KubernetesResourceConfig struct {
	Name     *string `yaml:"name,omitempty"`
	Group    *string `yaml:"group,omitempty"`
	Version  *string `yaml:"version,omitempty"`
	Disabled *bool   `yaml:"disabled,omitempty"`
}

type MatchRule struct {
	Prefix       *string `yaml:"prefix,omitempty"`
	KeepSegments *int    `yaml:"keep-segments,omitempty"`
}
type HttpEndpointExtraction struct {
	Disabled   *bool       `yaml:"disabled,omitempty"`
	MatchRules []MatchRule `yaml:"match-rules,omitempty"`
}

type ExtraLogFieldsInfo struct {
	FieldName string `yaml:"field-name,omitempty"`
	// SubFieldNames []string `yaml:"sub-field-names,omitempty"` // Future version support
}

type ExtraLogFields struct {
	Http  []ExtraLogFieldsInfo `yaml:"http,omitempty"`
	Http2 []ExtraLogFieldsInfo `yaml:"http2,omitempty"`
	Grpc  []ExtraLogFieldsInfo `yaml:"grpc,omitempty"`
}

type L7LogBlacklist struct {
	FieldName string `yaml:"field-name,omitempty"`
	Operator  string `yaml:"operator,omitempty"`
	Value     string `yaml:"value,omitempty"`
}

type L7ProtocolAdvancedFeatures struct {
	HttpEndpointExtraction                 *HttpEndpointExtraction `yaml:"http-endpoint-extraction,omitempty"`
	ObfuscateEnabledProtocols              []string                `yaml:"obfuscate-enabled-protocols,omitempty"`
	ExtraLogFields                         *ExtraLogFields         `yaml:"extra-log-fields,omitempty"`
	UnconcernedDnsNxdomainResponseSuffixes []string                `yaml:"unconcerned-dns-nxdomain-response-suffixes,omitempty"`
}

type OracleConfig struct {
	IsBE              *bool `yaml:"is-be,omitempty"`
	IntCompress       *bool `yaml:"int-compress,omitempty"`
	Resp0x04ExtraByte *bool `yaml:"resp-0x04-extra-byte,omitempty"`
}
