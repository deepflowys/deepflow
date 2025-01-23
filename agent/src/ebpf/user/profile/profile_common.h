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

#ifndef DF_USER_PROFILE_COMMON_H
#define DF_USER_PROFILE_COMMON_H

#include "../load.h"
#include "perf_profiler.h"
#include "stringifier.h"

typedef struct {
	char name[MAP_NAME_SZ];
	// Record all stack IDs in each iteration for quick retrieval.
	struct stack_ids_bitmap ids;
	// This vector table is used to remove a stack from the stack map.
	int *clear_ids;
} stack_map_t;

struct profiler_context {
	// profiler name
	const char *name;
	// log output flag string
	const char *tag;
	// Profiler type
	u8 type;
	// The name of the status map
	char state_map_name[MAP_NAME_SZ];
	// The dual-buffered reader is used to read data from the perf buffer.
	struct bpf_perf_reader *r_a;
	struct bpf_perf_reader *r_b;

	stack_map_t stack_map_a;
	stack_map_t stack_map_b;
	stack_map_t custom_stack_map_a;
	stack_map_t custom_stack_map_b;

	// The name of the stack blacklist map
	char stack_blacklist_map_name[MAP_NAME_SZ];

	// Read raw data from the eBPF perfbuf and temporarily store it.
	struct stack_trace_key_t *raw_stack_data;

	// Cache hash: obtain folded stack trace string from stack ID.
	stack_str_hash_t stack_str_hash;
	// Used for tracking data statistics and pushing.
	stack_trace_msg_hash_t msg_hash;

	/*
	 * 'cpu_aggregation_flag' is used to set whether to retrieve CPUID
	 * and include it in the aggregation of stack trace data.
	 *
	 * If valude is set to 1, CPUID will be retrieved and included in
	 * the aggregation of stack trace data. If value is set to 0,
	 * CPUID will not be retrieved and will not be included in the
	 * aggregation. Any other value is considered invalid.
	 */
	volatile u64 cpu_aggregation_flag;

	/*
	 * The profiler stop flag, with 1 indicating stop and
	 * 0 indicating running status.
	 */
	volatile u64 profiler_stop;

	/*
	 * This flag is used to enable the eBPF program to start working.
	 * with 1 indicating enable and 0 indicating disable.
	 */
	volatile u64 enable_bpf_profile;

	/*
	 * The identifier to only retrieve matched data. This flag
	 * setting will exclude the total process.
	 */
	bool only_matched_data;

	/*
	 * This setting determines whether to use time intervals.
	 * If this value is set to true, real-time intervals (in nanoseconds) are
	 * used. If this value is false, it will count the number of captured data.
	 */
	bool use_delta_time;

	/*
	 * If using sampling to obtain function call stack data, this setting is
	 * used to specify the sampling period, measured in nanoseconds.
	 */
	u64 sample_period;

	// for stack_trace_msg_hash relese
	stack_trace_msg_hash_kv *trace_msg_kvps;
	bool msg_clear_hash;

	/* profiler statistics */

	// Switching between dual buffers.
	u64 transfer_count;
	// Total iteration count for all iterations.
	u64 process_count;
	u64 stackmap_clear_failed_count;
	// perf buffer queue loss statistics.
	u64 perf_buf_lost_a_count;
	u64 perf_buf_lost_b_count;
	/*
	 * During the parsing process, it is possible for processes in procfs
	 * to be missing (processes that start and exit quickly). This variable
	 * is used to count the number of lost processes during the parsing process.
	 */
	atomic64_t process_lost_count;
	// Stack error quantity statistics obtained by eBPF.
	u64 stack_trace_err;
	// Quantity statistics of data pushed.
	u64 push_count;

	/*
	 * Record the time of the last data push
	 * (in seconds since system startup)
	 */
	u64 last_push_time;

	// Passed into callback interface as first parameter
	void *callback_ctx;
};

void process_bpf_stacktraces(struct profiler_context *ctx,
			     struct bpf_tracer *t);
void set_bpf_run_enabled(struct bpf_tracer *t, struct profiler_context *ctx,
			 u64 enable_flag);
int profiler_context_init(struct profiler_context *ctx, const char *name,
			  const char *tag, u8 type, bool enable_profiler,
			  const char *state_map_name,
			  const char *stack_map_name_a,
			  const char *stack_map_name_b,
			  const char *custom_stack_map_name_a,
			  const char *custom_stack_map_name_b,
			  const char *stack_blacklist_map_name,
			  bool only_matched, bool use_delta_time,
			  u64 sample_period, void *callback_ctx);
bool run_conditions_check(void);
int java_libs_and_tools_install(void);
void push_and_release_stack_trace_msg(struct profiler_context *ctx,
				      stack_trace_msg_hash_t * h,
				      bool is_force);
// Check if the profiler is currently running.
bool profiler_is_running(void);
void set_bpf_rt_kern(struct bpf_tracer *t, struct profiler_context *ctx);
#endif /*DF_USER_PROFILE_COMMON_H */
