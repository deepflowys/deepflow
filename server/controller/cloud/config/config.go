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

var CONF *CloudConfig

type QingCloudConfig struct {
	MaxRetries            uint   `default:"4" yaml:"max_retries"`
	RetryDuration         uint   `default:"60" yaml:"retry_duration"`              // unit: s
	DailyTriggerTime      string `default:"" yaml:"daily_trigger_time"`            // %H:%M 05:00
	DisableSyncLBListener bool   `default:"false" yaml:"disable_sync_lb_listener"` // disable sync for lb listener and target server
}

type FusionComputeConfig struct {
	DailyTriggerTime string `default:"" yaml:"daily_trigger_time"` // %H:%M 05:00
}

type CloudConfig struct {
	KubernetesGatherInterval uint32              `default:"30" yaml:"kubernetes_gather_interval"`
	AliyunRegionName         string              `default:"cn-beijing" yaml:"aliyun_region_name"`
	AWSRegionName            string              `default:"cn-north-1" yaml:"aws_region_name"`
	HostnameToIPFile         string              `default:"/etc/hostname_to_ip.csv" yaml:"hostname_to_ip_file"`
	DNSEnable                bool                `default:"false" yaml:"dns_enable"`
	HTTPTimeout              int                 `default:"30" yaml:"http_timeout"`
	CustomTagLenMax          int                 `default:"256" yaml:"custom_tag_len_max"`
	ProcessNameLenMax        int                 `default:"256" yaml:"process_name_len_max"`
	DebugEnabled             bool                `default:"false" yaml:"debug_enabled"`
	QingCloudConfig          QingCloudConfig     `yaml:"qingcloud_config"`
	FusionComputeConfig      FusionComputeConfig `yaml:"fusioncompute_config"`
}

func SetCloudGlobalConfig(c CloudConfig) {
	CONF = &CloudConfig{
		HostnameToIPFile:    c.HostnameToIPFile,
		DNSEnable:           c.DNSEnable,
		HTTPTimeout:         c.HTTPTimeout,
		DebugEnabled:        c.DebugEnabled,
		AWSRegionName:       c.AWSRegionName,
		CustomTagLenMax:     c.CustomTagLenMax,
		ProcessNameLenMax:   c.ProcessNameLenMax,
		QingCloudConfig:     c.QingCloudConfig,
		FusionComputeConfig: c.FusionComputeConfig,
	}
}
