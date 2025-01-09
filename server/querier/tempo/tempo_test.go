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

package tempo

import (
	//"reflect"
	"encoding/json"
	//"fmt"
	"testing"
)

func TestConvertL7TracingRespToProto(t *testing.T) {
	testData := `{"services": [{"service_uid": "-deepflow-statistics", "service_uname": "deepflow-statistics", "duration": 24753, "duration_ratio": "100.00"}], "tracing": [{"_ids": ["7169107986842648628"], "related_ids": ["0-base-7169107986842648628"], "start_time_us": 1669188027800930, "end_time_us": 1669188027825683, "duration": 24753, "selftime": 24753, "tap_side": "s-app", "l7_protocol": 20, "l7_protocol_str": "http", "endpoint": "/v1/alarm/controller-disk/", "request_type": "GET", "request_resource": "/v1/alarm/controller-disk/", "response_status": 0, "flow_id": "0", "request_id": null, "x_request_id_0": "", "trace_id": "5455e8b558250c7bfd2eed1bba623314", "span_id": "98576ec1ece19bb2", "parent_span_id": "", "req_tcp_seq": 0, "resp_tcp_seq": 0, "syscall_trace_id_request": "0", "syscall_trace_id_response": "0", "syscall_cap_seq_0": 0, "syscall_cap_seq_1": 0, "id": 0, "process_id": null, "vtap_id": 11, "service_uid": "-deepflow-statistics", "service_uname": "deepflow-statistics", "service_name": "deepflow-statistics", "service_instance_id": "", "tap_port": 0, "tap_port_name": "", "resource_from_vtap": "DF-DAILY-R0-C1", "set_parent_info": null, "auto_instance": "DF-DAILY-R0-C1", "deepflow_span_id": "98576ec1ece19bb2", "deepflow_parent_span_id": ""}]}`
	var result map[string]interface{}
	json.Unmarshal([]byte(testData), &result)
	//fmt.Println(result)
	ConvertL7TracingRespToProto(result, "test")
	//fmt.Println(proto)
}
