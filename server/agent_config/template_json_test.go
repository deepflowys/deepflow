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

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestIgnoreSubelementComments(t *testing.T) {
	type args struct {
		yamlData []byte
		start    int
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`# type: section
global:
  # type: section
  # ---
  # name:
  #   en: Global
  limits:`),
				start: 3,
			},
			want:    5,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewLineFommatter(tt.args.yamlData)
			i := parser.ignoreSubelementComments(tt.args.start)
			if i != tt.want {
				t.Errorf("ignoreSubelementComments() = %v, want %v", i, tt.want)
			}
		})
	}
}

func TestConvSubelementCommentToSection(t *testing.T) {
	type args struct {
		yamlData []byte
		start    int
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`inputs:
  proc:
    tag_extraction:
      # type: dict
      # ---
      # upgrade_from: static_config.os-proc-regex.match-regex
      # ---
      # match_regex: ""
      process_matcher:
        - match_regex: deepflow-.*`),
				start: 4,
			},
			want: []string{
				"        match_regex_comment:",
				"          upgrade_from: static_config.os-proc-regex.match-regex",
			},
			wantErr: false,
		},
		{
			name: "case02",
			args: args{
				yamlData: []byte(`inputs:
  proc:
    tag_extraction:
      # type: dict
      # ---
      # upgrade_from: static_config.os-proc-regex.match-regex
      # ---
      # match_regex: ""
      # ---
      # type: string
      # upgrade_from: static_config.os-proc-regex.rewrite-name
      # ---
      # rewrite_name: ""
      process_matcher:
        - match_regex: deepflow-.*`),
				start: 4,
			},
			want: []string{
				"        match_regex_comment:",
				"          upgrade_from: static_config.os-proc-regex.match-regex",
				"        rewrite_name_comment:",
				"          type: string",
				"          upgrade_from: static_config.os-proc-regex.rewrite-name",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewLineFommatter(tt.args.yamlData)
			got := make([]string, 0)
			_, got, err := parser.convSubelementCommentToSection(tt.args.start, got)
			if (err != nil) != tt.wantErr {
				t.Errorf("convSubelementCommentToSection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i := 0; i < len(got); i++ {
				if got[i] != tt.want[i] {
					t.Errorf("line %d convSubelementCommentToSection() = \"%v\", want \"%v\"", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestIgnoreTodoComments(t *testing.T) {
	type args struct {
		yamlData []byte
		start    int
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`# type: section
# TODO: add more fields
global:`),
				start: 0,
			},
			want:    1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewLineFommatter(tt.args.yamlData)
			i := parser.ignoreTodoComments(tt.args.start)
			if i != tt.want {
				t.Errorf("ignoreTodoComments() = %v, want %v", i, tt.want)
			}
		})
	}
}

func TestConvCommentToSection(t *testing.T) {
	type args struct {
		yamlData []byte
		start    int
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`# type: section
# name:
#   en: Global
# ----
# TODO: add more fields
global:
  # type: section
  # --
  #
  limits:`),
				start: 0,
			},
			want: []string{
				"global_comment:",
				"  type: section",
				"  name:",
				"    en: Global",
			},
			wantErr: false,
		},
		{
			name: "case02",
			args: args{
				yamlData: []byte(`# type: section
# name:
#   en: Global
# ----
# TODO: add more fields
global:
  # type: section
  # ---
  #
  limits:`),
				start: 6,
			},
			want: []string{
				"  limits_comment:",
				"    type: section",
			},
			wantErr: false,
		},
		{
			name: "case03",
			args: args{
				yamlData: []byte(`inputs:
  proc:
    tag_extraction:
      # type: dict
      # ---
      # upgrade_from: static_config.os-proc-regex.match-regex
      # ---
      # match_regex: ""
      # ---
      # type: string
      # upgrade_from: static_config.os-proc-regex.rewrite-name
      # ---
      # rewrite_name: ""
      process_matcher:
        - match_regex: deepflow-.*`),
				start: 3,
			},
			want: []string{
				"      process_matcher_comment:",
				"        type: dict",
				"        match_regex_comment:",
				"          upgrade_from: static_config.os-proc-regex.match-regex",
				"        rewrite_name_comment:",
				"          type: string",
				"          upgrade_from: static_config.os-proc-regex.rewrite-name",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewLineFommatter(tt.args.yamlData)
			_, detailCommentlines, err := parser.convCommentToSection(tt.args.start)
			if (err != nil) != tt.wantErr {
				t.Errorf("convCommentToSection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(detailCommentlines) != len(tt.want) {
				t.Errorf("convCommentToSection() = \"%v\", want \"%v\"", detailCommentlines, tt.want)
			}
			for i := 0; i < len(detailCommentlines); i++ {
				if detailCommentlines[i] != tt.want[i] {
					t.Errorf("line %d convCommentToSection() = \"%v\", want \"%v\"", i, detailCommentlines[i], tt.want[i])
				}
			}
		})
	}
}

func TestKeyLineToKeyCommentLine(t *testing.T) {
	type args struct {
		line string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				line: "global:",
			},
			want:    "global_comment:",
			wantErr: false,
		},
		{
			name: "case02",
			args: args{
				line: "  limits:",
			},
			want:    "  limits_comment:",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewLineFommatter([]byte{})
			got, err := parser.keyLineToKeyCommentLine(tt.args.line)
			if (err != nil) != tt.wantErr {
				t.Errorf("keyLineToKeyCommentLine() error = \"%v\", wantErr \"%v\"", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("keyLineToKeyCommentLine() = \"%v\", want \"%v\"", got, tt.want)
			}
		})
	}
}

func TestLineFormat(t *testing.T) {
	type args struct {
		yamlData []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`# type: section
# name:
#   en: Global
# ----
# TODO
global:
  # type: section
  # description: |-
  #
  #   The global section contains global settings.
  limits:`),
			},
			want: []string{
				"global_comment:",
				"  type: section",
				"  name:",
				"    en: Global",
				"gloabl:",
				"  limits_comment:",
				"    type: section",
				"    description: |-",
				"      ",
				"      The global section contains global settings.",
				"  limits:",
			},
		},
		{
			name: "case02",
			args: args{
				yamlData: YamlAgentGroupConfigTemplate,
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		if tt.name != "case02" {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			fmt := NewLineFommatter(tt.args.yamlData)
			lines, err := fmt.Format()
			if (err != nil) != tt.wantErr {
				t.Errorf("stripLines() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			os.Mkdir("test_tmp", 0755)
			if err := os.WriteFile("test_tmp/stripped_lines.yaml", lines, os.ModePerm); err != nil {
				t.Fatalf("Failed to write to file: %v", err)
			}
		})
	}
}

func TestConvertTemplateYAMLToJSON(t *testing.T) {
	l7Protocols := []string{
		"HTTP", "HTTP2", "Dubbo", "gRPC", "SOFARPC", "FastCGI", "bRPC", "Tars", "Some/IP", "MySQL", "PostgreSQL",
		"Oracle", "Redis", "MongoDB", "Kafka", "MQTT", "AMQP", "OpenWire", "NATS", "Pulsar", "ZMTP", "RocketMQ",
		"DNS", "TLS", "Custom"}
	l7ProtocolsYamlBytes, err := yaml.Marshal(l7Protocols)
	if err != nil {
		t.Fatalf("Failed to marshal l7Protocols: %v", err)
	}
	var l7ProtocolsNode yaml.Node
	if err := yaml.Unmarshal(l7ProtocolsYamlBytes, &l7ProtocolsNode); err != nil {
		t.Fatalf("Failed to unmarshal l7Protocols: %v", err)
	}
	domainKeyToInfo := []map[string]interface{}{
		{
			"1": map[string]interface{}{
				"ch": "aliyun",
				"en": "aliyun",
			},
		},
		{
			"2": map[string]interface{}{
				"ch": "openstack",
				"en": "openstack",
			},
		},
	}
	domainKeyToInfoYamlBytes, err := yaml.Marshal(domainKeyToInfo)
	if err != nil {
		t.Fatalf("Failed to marshal domainKeyToInfo: %v", err)
	}
	var domainInfoNode yaml.Node
	if err := yaml.Unmarshal(domainKeyToInfoYamlBytes, &domainInfoNode); err != nil {
		t.Fatalf("Failed to unmarshal domainKeyToInfo: %v", err)
	}
	tapTypeInfo := []map[string]interface{}{
		{
			"1": map[string]interface{}{
				"ch": "test1",
				"en": "test1",
			},
		},
		{
			"2": map[string]interface{}{
				"ch": "test2",
				"en": "test2",
			},
		},
	}
	tapTypeInfoYamlBytes, err := yaml.Marshal(tapTypeInfo)
	if err != nil {
		t.Fatalf("Failed to marshal tapTypeInfo: %v", err)
	}
	var tapTypeInfoNode yaml.Node
	if err := yaml.Unmarshal(tapTypeInfoYamlBytes, &tapTypeInfoNode); err != nil {
		t.Fatalf("Failed to unmarshal tapTypeInfo: %v", err)
	}
	// fmt.Printf("tapTypeInfoNode: %#v\n", tapTypeInfoNode.Content[0])

	dynamicOptions := DynamicOptions{
		"inputs.cbpf.physical_mirror.default_capture_network_type_comment.enum_options":        tapTypeInfoNode.Content[0],
		"inputs.resources.pull_resource_from_controller.domain_filter_comment.enum_options":    domainInfoNode.Content[0],
		"inputs.ebpf.socket.preprocess.out_of_order_reassembly_protocols_comment.enum_options": l7ProtocolsNode.Content[0],
	}
	type args struct {
		yamlData    []byte
		dynamicOpts DynamicOptions
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData:    YamlAgentGroupConfigTemplate,
				dynamicOpts: dynamicOptions,
			},
			wantErr: false,
		},
		{
			name: "case02",
			args: args{
				yamlData: []byte(`inputs:
  cbpf:
    common:
      capture_mode: 0
  ebpf:
    profile: # deepflow-server for test, don't delete 241108
      unwinding:
        dwarf_regex: ^python[23].*`),
				dynamicOpts: dynamicOptions,
			},
			wantErr: false,
		},
	}
	os.Mkdir("test_tmp", 0755)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertTemplateYAMLToJSON(tt.args.dynamicOpts)
			if (err != nil) != tt.wantErr {
				t.Errorf("err: %v", err)
				t.Errorf("ParseYAMLToJson() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err = os.WriteFile(fmt.Sprintf("test_tmp/conv_tmpl_%s.json", tt.name), got, os.ModePerm); err != nil {
				t.Fatalf("Failed to write to file: %v", err)
			}
		})
	}
}

func TestGenerateKeyToComment(t *testing.T) {
	type args struct {
		yamlData []byte
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`# type: section
global:
  # type: section
  limits:
    # type: int
    max_millicpus: 1000
  # type: section
  alerts:
    # type: int
    thread_threshold: 500
# type: section
inputs:
  # type: section
  proc:
    # type: bool
    enabled: false
    # type: string
    proc_dir_path: /proc`),
			},
			want: map[string]interface{}{
				"global": map[string]interface{}{
					"type": "section",
				},
				"global.limits": map[string]interface{}{
					"type": "section",
				},
				"global.limits.max_millicpus": map[string]interface{}{
					"type": "int",
				},
				"global.alerts": map[string]interface{}{
					"type": "section",
				},
				"global.alerts.thread_threshold": map[string]interface{}{
					"type": "int",
				},
				"inputs": map[string]interface{}{
					"type": "section",
				},
				"inputs.proc": map[string]interface{}{
					"type": "section",
				},
				"inputs.proc.enabled": map[string]interface{}{
					"type": "bool",
				},
				"inputs.proc.proc_dir_path": map[string]interface{}{
					"type": "string",
				},
			},
		},
		{
			name: "case02",
			args: args{
				yamlData: []byte(`inputs:
  proc:
    tag_extraction:
      # type: dict
      # ---
      # upgrade_from: static_config.os-proc-regex.match-regex
      # ---
      # match_regex: ""
      # ---
      # type: string
      # upgrade_from: static_config.os-proc-regex.rewrite-name
      # ---
      # rewrite_name: ""
      process_matcher:
        - match_regex: deepflow-.*`),
			},
			want: map[string]interface{}{
				"inputs.proc.tag_extraction.process_matcher": map[string]interface{}{
					"type": "dict",
					"match_regex_comment": map[string]interface{}{
						"upgrade_from": "static_config.os-proc-regex.match-regex",
					},
					"rewrite_name_comment": map[string]interface{}{
						"type":         "string",
						"upgrade_from": "static_config.os-proc-regex.rewrite-name",
					},
				},
				"inputs.proc.tag_extraction.process_matcher.match_regex": map[string]interface{}{
					"upgrade_from": "static_config.os-proc-regex.match-regex",
				},
				"inputs.proc.tag_extraction.process_matcher.rewrite_name": map[string]interface{}{
					"type":         "string",
					"upgrade_from": "static_config.os-proc-regex.rewrite-name",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewTemplateFormatter(tt.args.yamlData).GenerateKeyToComment()
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateKeyToComment() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for k, v := range got {
				assert.EqualValues(t, tt.want[k], v)
			}
		})
	}
}

func TestDictValueToString(t *testing.T) {
	type args struct {
		yamlData []byte
		mapData  map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`inputs:
  proc:
    process_matcher:
      - match_regex: (python[23]|deepflow-server).* # deepflow-server for test, don't delete 241108
        match_type: ProcessName
outputs:
  flow_log:
    filters:
      l7_capture_network_types:
        - 0`),
			},
			want: map[string]interface{}{
				"inputs": map[string]interface{}{
					"proc": map[string]interface{}{
						"process_matcher": "- match_regex: (python[23]|deepflow-server).*\n  match_type: ProcessName\n",
					},
				},
				"outputs": map[string]interface{}{
					"flow_log": map[string]interface{}{
						"filters": map[string]interface{}{
							"l7_capture_network_types": []interface{}{0},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "case02",
			args: args{
				mapData: map[string]interface{}{
					"processors": map[string]interface{}{
						"request_log": map[string]interface{}{
							"filters": map[string]interface{}{
								"port_number_prefilters": map[string]interface{}{
									"HTTP":  "1-6",
									"HTTP2": "9-10",
								},
							},
						},
					},
				},
			},
			want: map[string]interface{}{
				"processors": map[string]interface{}{
					"request_log": map[string]interface{}{
						"filters": map[string]interface{}{
							"port_number_prefilters": "HTTP: 1-6\nHTTP2: 9-10",
						},
					},
				},
			},
			wantErr: false,
		},
	}
	keyToComment, _ := NewTemplateFormatter(YamlAgentGroupConfigTemplate).GenerateKeyToComment()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataFmt := NewDataFommatter()
			if err := dataFmt.LoadYAMLData(tt.args.yamlData); err != nil {
				t.Fatalf("Failed to init yaml data: %v", err)
			}
			err := dataFmt.fmtVal("", dataFmt.mapData, keyToComment, true)
			if (err != nil) != tt.wantErr {
				t.Errorf("DictValueToString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for k, v := range dataFmt.mapData {
				if ok := assert.EqualValues(t, tt.want[k], v); !ok {
					t.Errorf("key %s DictValueToString() = %v, want %v", k, v, tt.want[k])
				}
			}
		})
	}
}

func TestStringToDictValue(t *testing.T) {
	type args struct {
		yamlData []byte
		mapData  map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`inputs:
  proc:
    process_matcher: "- match_regex: (python[23]|deepflow-server).*\n  match_type: ProcessName\n"
outputs:
  flow_log:
    filters:
      l7_capture_network_types:
        - 0`),
			},
			want: map[string]interface{}{
				"inputs": map[string]interface{}{
					"proc": map[string]interface{}{
						"process_matcher": []interface{}{
							map[string]interface{}{
								"match_regex": "(python[23]|deepflow-server).*",
								"match_type":  "ProcessName",
							},
						},
					},
				},
				"outputs": map[string]interface{}{
					"flow_log": map[string]interface{}{
						"filters": map[string]interface{}{
							"l7_capture_network_types": []interface{}{0},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "case02",
			args: args{
				mapData: map[string]interface{}{
					"processors": map[string]interface{}{
						"request_log": map[string]interface{}{
							"filters": map[string]interface{}{
								"port_number_prefilters": "HTTP: 1-6\nHTTP2: 9-10",
							},
						},
					},
				},
			},
			want: map[string]interface{}{
				"processors": map[string]interface{}{
					"request_log": map[string]interface{}{
						"filters": map[string]interface{}{
							"port_number_prefilters": map[string]interface{}{
								"HTTP":  "1-6",
								"HTTP2": "9-10",
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	keyToComment, _ := NewTemplateFormatter(YamlAgentGroupConfigTemplate).GenerateKeyToComment()
	for _, tt := range tests {
		// if tt.name != "case02" {
		// 	continue
		// }
		t.Run(tt.name, func(t *testing.T) {
			dataFmt := NewDataFommatter()
			if tt.args.yamlData != nil {
				if err := dataFmt.LoadYAMLData(tt.args.yamlData); err != nil {
					t.Fatalf("Failed to init yaml data: %v", err)
				}
			} else {
				dataFmt.mapData = tt.args.mapData
			}
			err := dataFmt.fmtVal("", dataFmt.mapData, keyToComment, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("stringToDictValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for k, v := range dataFmt.mapData {
				if ok := assert.EqualValues(t, tt.want[k], v); !ok {
					t.Errorf("key %s stringToDictValue() = %v, want %v", k, v, tt.want[k])
				}
			}
		})
	}
}

func TestDataConvertToJSON(t *testing.T) {
	type args struct {
		yamlData []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`inputs:
  proc:
    process_matcher:
      - match_regex: (python[23]|deepflow-server).* # deepflow-server for test, don't delete 241108
        match_type: ProcessName
outputs:
  flow_log:
    filters:
      l7_capture_network_types:
        - 0`),
			},
			want: []byte(`{
  "inputs": {
    "proc": {
      "process_matcher": "- match_regex: (python[23]|deepflow-server).*\n  match_type: ProcessName\n"
    }
  },
  "outputs": {
    "flow_log": {
      "filters": {
        "l7_capture_network_types": [
          0
        ]
      }
    }
  }
}`),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyToComment, _ := NewTemplateFormatter(YamlAgentGroupConfigTemplate).GenerateKeyToComment()
			dataFmt := NewDataFommatter()
			if err := dataFmt.LoadYAMLData(tt.args.yamlData); err != nil {
				t.Fatalf("Failed to init yaml data: %v", err)
			}
			got, err := dataFmt.mapToJSON(keyToComment)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataConvertToJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			os.Mkdir("test_tmp", 0755)
			if err = os.WriteFile("test_tmp/data_fmt_conv.json", got, os.ModePerm); err != nil {
				t.Fatalf("Failed to write to file: %v", err)
			}
		})
	}
}

func TestConvertYAMLToJSON(t *testing.T) {
	type args struct {
		yamlData []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`inputs:
  proc:
    process_matcher:
      - match_regex: (python[23]|deepflow-server).* # deepflow-server for test, don't delete 241108
        match_type: ProcessName
outputs:
  flow_log:
    filters:
      l7_capture_network_types:
        - 0`),
			},
			want: []byte(`{
  "inputs": {
    "proc": {
      "process_matcher": "- match_regex: (python[23]|deepflow-server).*\n  match_type: ProcessName\n"
    }
  },
  "outputs": {
    "flow_log": {
      "filters": {
        "l7_capture_network_types": [
          0
        ]
      }
    }
  }
}`),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertYAMLToJSON(tt.args.yamlData)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataConvertToJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			os.Mkdir("test_tmp", 0755)
			if err = os.WriteFile("test_tmp/conv_yaml_to_json.json", got, os.ModePerm); err != nil {
				t.Fatalf("Failed to write to file: %v", err)
			}
		})
	}
}

func TestValidateYAML(t *testing.T) {
	type args struct {
		yamlData []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				yamlData: []byte(`inputs:
  proc:
    process_matcher:
      - match_regex: (python[23]|deepflow-server).* # deepflow-server for test, don't delete 241108
        match_type: ProcessName`),
			},
			wantErr: false,
		},
		{
			name: "case02",
			args: args{
				yamlData: []byte(`inputs:
                  proc:
                          PID: 123`),
			},
			wantErr: true,
		},
		{
			name: "case03",
			args: args{
				yamlData: []byte(`# type: section
global:
  limits:
    # type: int
    max_millicpus: 1000
  alerts:
    thread_threshold: 500
inputs:
  # type: section
  proc:
    enabled: false`),
			},
			wantErr: false,
		},
		{
			name: "case04",
			args: args{
				yamlData: []byte(`global:
  limits:
    max_millicpus: 1000
      alerts:
        thread_threshold: 500
inputs:
  proc:
    enabled: false`),
			},
			wantErr: true,
		},
		{
			name: "case05",
			args: args{
				yamlData: []byte(`# type: section
global:
  # type: section
  limits:
    # type: int
    max_millicpus: abc`),
			},
			wantErr: true,
		},
		{
			name: "case06",
			args: args{
				yamlData: []byte(`outputs:
  flow_log:
    filters:
      l7_capture_network_types: #test
        - 0`),
			},
			wantErr: false,
		},
		{
			name: "case07",
			args: args{
				yamlData: []byte(`outputs:
  flow_log:
    filters:
      l7_capture_network_types:
        - 0:`),
			},
			wantErr: true,
		},
		{
			name: "case08",
			args: args{
				yamlData: []byte(`global:
  limits:
    max_millicpus: 1000.00`),
			},
			wantErr: true,
		},
		{
			name: "case09",
			args: args{
				yamlData: []byte(`global:
  circuit_breakers:
    relative_sys_load:
      trigger_threshold: 1`),
			},
			wantErr: false,
		},
		{
			name: "case10",
			args: args{
				yamlData: []byte(`global:
  circuit_breakers:
    relative_sys_load:
      trigger_threshold: 1.0`),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		if tt.name != "case10" {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateYAML(tt.args.yamlData); (err != nil) != tt.wantErr {
				t.Errorf("ValidateYAML() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConvertJSONToYAMLAndValidate(t *testing.T) {
	type args struct {
		jsonData map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "case01",
			args: args{
				jsonData: map[string]interface{}{
					"global": map[string]interface{}{
						"alerts": map[string]interface{}{
							"check_core_file_disabled": true,
						},
					},
				},
			},
			want: []byte(`global:
  alerts:
    check_core_file_disabled: true
`),
			wantErr: false,
		},
		{
			name: "case02",
			args: args{
				jsonData: map[string]interface{}{
					"inputs": map[string]interface{}{
						"resources": map[string]interface{}{
							"kubernetes": map[string]interface{}{
								"api_resources": "- name: namespaces\n- name: nodes\n",
							},
						},
					},
				},
			},
			want: []byte(`inputs:
  resources:
    kubernetes:
      api_resources:
        - name: namespaces
        - name: nodes
`),
			wantErr: false,
		},
		{
			name: "case03",
			args: args{
				jsonData: map[string]interface{}{
					"inputs": map[string]interface{}{
						"ebpf": map[string]interface{}{
							"socket": map[string]interface{}{
								"preprocess": map[string]interface{}{
									"out_of_order_reassembly_protocols": []string{
										"HTTP",
										"Dubbo",
										"SOFARPC",
									},
								},
							},
						},
					},
				},
			},
			want: []byte(`inputs:
  ebpf:
    socket:
      preprocess:
        out_of_order_reassembly_protocols:
          - HTTP
          - Dubbo
          - SOFARPC
`),
			wantErr: false,
		},
		{
			name: "case04",
			args: args{
				jsonData: map[string]interface{}{
					"outputs": map[string]interface{}{
						"flow_log": map[string]interface{}{
							"filters": map[string]interface{}{
								"l7_capture_network_types": []interface{}{"a"},
							},
						},
					},
				},
			},
			want:    []byte(``),
			wantErr: true,
		},
		{
			name: "case05",
			args: args{
				jsonData: map[string]interface{}{
					"outputs": map[string]interface{}{
						"filters": map[string]interface{}{
							"l7_capture_network_types": []interface{}{"a"},
						},
					},
				},
			},
			want:    []byte(``),
			wantErr: true,
		},
		{
			name: "case06",
			args: args{
				jsonData: map[string]interface{}{
					"processors": map[string]interface{}{
						"flow_log": map[string]interface{}{
							"tunning": map[string]interface{}{
								"concurrent_flow_limit": 63000000,
							},
						},
					},
				},
			},
			want: []byte(`processors:
  flow_log:
    tunning:
      concurrent_flow_limit: 63000000
`),
			wantErr: false,
		},
		{
			name: "case07",
			args: args{
				jsonData: map[string]interface{}{
					"inputs": map[string]interface{}{
						"resources": map[string]interface{}{
							"kubernetes": map[string]interface{}{
								"kubernetes_namespace": "111",
							},
						},
					},
				},
			},
			want: []byte(`inputs:
  resources:
    kubernetes:
      kubernetes_namespace: "111"
`),
			wantErr: false,
		},
		{
			name: "case08",
			args: args{
				jsonData: map[string]interface{}{
					"inputs": map[string]interface{}{
						"cbpf": map[string]interface{}{
							"af_packet": map[string]interface{}{
								"bond_interfaces": "- slave_interfaces: [eth0, eth1]\r\n- slave_interfaces: [eth2, eth3]",
							},
						},
					},
				},
			},
			want: []byte(`inputs:
  cbpf:
    af_packet:
      bond_interfaces: "- slave_interfaces:\n  - eth0\n  - eth1\n- slave_interfaces:\n  - eth2\n  - eth3\n"
`),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		if tt.name != "case06" {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertJSONToYAMLAndValidate(tt.args.jsonData)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseJsonToYAMLAndValidate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.EqualValues(t, string(tt.want), string(got))
			os.Mkdir("test_tmp", 0755)
			if err = os.WriteFile("test_tmp/json_to_yaml.yaml", got, os.ModePerm); err != nil {
				t.Fatalf("Failed to write to file: %v", err)
			}
		})
	}
}
