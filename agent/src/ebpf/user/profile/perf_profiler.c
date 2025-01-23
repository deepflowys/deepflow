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

/*
 * TODO (@jiping)
 * There are some issues with aarch64 musl compilation, and the profiler
 * cannot be applied temporarily in scenarios where aarch64 is compiled
 * using musl.
 */
#ifndef AARCH64_MUSL
#include <sys/stat.h>
#include <math.h>
#include <signal.h>		/* kill() */
#include <bcc/perf_reader.h>
#include "../config.h"
#include "../common_utils.h"
#include "../utils.h"
#include "../mem.h"
#include "../log.h"
#include "../types.h"
#include "../vec.h"
#include "../tracer.h"
#include "../socket.h"
#include "java/collect_symbol_files.h"
#include "perf_profiler.h"
#include "../elf.h"
#include "../load.h"
#include "../../kernel/include/perf_profiler.h"
#include "../perf_reader.h"
#include "../bihash_8_8.h"
#include "stringifier.h"
#include "../table.h"
#include <regex.h>
#include "java/config.h"
#include "java/jvm_symbol_collect.h"
#include "profile_common.h"
#include "../proc.h"
#include "../unwind_tracer.h"
#include "trace_utils.h"

#include "../perf_profiler_bpf_common.c"
#include "../perf_profiler_bpf_5_2_plus.c"

#define CP_PERF_PG_NUM 16
#define ONCPU_PROFILER_NAME "oncpu"
#define PROFILER_CTX_ONCPU_IDX THREAD_PROFILER_READER_IDX
#define DEEPFLOW_AGENT_NAME "deepflow-agent"

extern int sys_cpus_count;
extern int major, minor;
struct profiler_context *g_ctx_array[PROFILER_CTX_NUM];
static struct profiler_context oncpu_ctx;

static bool g_enable_oncpu = true;

/* Used for handling updates to JAVA symbol files */
static pthread_t java_syms_update_thread;

extern char linux_release[128];
extern __thread uword thread_index;

struct bpf_tracer *profiler_tracer;

// for flame-graph test
static FILE *folded_file;
#define FOLDED_FILE_PATH "./profiler.folded"
char *flame_graph_start_time;
static char *flame_graph_end_time;

/* Continuous Profiler debug related settings. */
static pthread_mutex_t cpdbg_mutex;
static bool cpdbg_enable;
static debug_callback_t cpdbg_cb;
static bool cpdbg_use_remote;
static uint32_t cpdbg_start_time;
static uint32_t cpdbg_timeout;

static u64 get_process_lost_count(struct profiler_context *ctx)
{
	return atomic64_read(&ctx->process_lost_count);
}

static void reader_lost_cb_a(void *cookie, u64 lost)
{
	struct bpf_tracer *tracer = profiler_tracer;
	atomic64_add(&tracer->lost, lost);
	oncpu_ctx.perf_buf_lost_a_count++;
}

static void reader_lost_cb_b(void *cookie, u64 lost)
{
	struct bpf_tracer *tracer = profiler_tracer;
	atomic64_add(&tracer->lost, lost);
	oncpu_ctx.perf_buf_lost_b_count++;
}

static void reader_raw_cb(void *cookie, void *raw, int raw_size)
{
	if (unlikely(oncpu_ctx.profiler_stop == 1))
		return;

	struct reader_forward_info *fwd_info = cookie;
	if (unlikely(fwd_info->queue_id != 0)) {
		ebpf_warning("cookie(%d) error", (u64) cookie);
		return;
	}

	struct stack_trace_key_t *v;
	struct bpf_tracer *tracer = profiler_tracer;
	v = (struct stack_trace_key_t *)raw;

	int ret = VEC_OK;
	vec_add1(oncpu_ctx.raw_stack_data, *v, ret);
	if (ret != VEC_OK) {
		ebpf_warning("vec add failed\n");
	}

	atomic64_add(&tracer->recv, 1);
}

static int release_profiler(struct bpf_tracer *tracer)
{
	tracer_reader_lock(tracer);

	/* detach perf event */
	tracer_hooks_detach(tracer);

	/* free all readers */
	free_all_readers(tracer);

	print_cp_tracer_status();

	/* release object */
	release_object(tracer->obj);

	tracer_reader_unlock(tracer);

	ebpf_info("release_profiler().... finish!\n");
	return ETR_OK;
}

static inline bool is_cpdbg_timeout(void)
{
	uint32_t passed_sec;
	passed_sec = get_sys_uptime() - cpdbg_start_time;
	if (passed_sec > cpdbg_timeout) {
		cpdbg_start_time = 0;
		cpdbg_enable = false;
		cpdbg_use_remote = false;
		ebpf_info("\n\ncpdbg is finished, use time: %us.\n\n",
			  cpdbg_timeout);
		cpdbg_timeout = 0;
		return true;
	}

	return false;
}

static void print_cp_data(stack_trace_msg_t * msg)
{
	char *timestamp = gen_timestamp_str(0);
	if (timestamp == NULL)
		return;

	char *cid;
	if (strlen((char *)msg->container_id) == 0)
		cid = "null";
	else
		cid = (char *)msg->container_id;

	char buff[DEBUG_BUFF_SIZE];
	snprintf(buff, sizeof(buff),
		 "%s [cpdbg] type %d netns_id %lu container_id %s pid %u tid %u "
		 "process_name %s comm %s stime %lu u_stack_id %u k_statck_id"
		 " %u cpu %u count %lu tiemstamp %lu datalen %u data %s\n",
		 timestamp, msg->profiler_type, msg->netns_id, cid, msg->pid,
		 msg->tid, msg->process_name, msg->comm, msg->stime,
		 msg->u_stack_id, msg->k_stack_id, msg->cpu, msg->count,
		 msg->time_stamp, msg->data_len, msg->data);

	free(timestamp);

	if (cpdbg_cb != NULL) {
		cpdbg_cb(buff, strlen(buff));
	} else {
		fprintf(stdout, "%s\n", buff);
		fflush(stdout);
	}
}

void cpdbg_process(stack_trace_msg_t * msg)
{
	pthread_mutex_lock(&cpdbg_mutex);
	if (unlikely(cpdbg_enable)) {
		if (!is_cpdbg_timeout())
			print_cp_data(msg);
	}
	pthread_mutex_unlock(&cpdbg_mutex);
}

static void java_syms_update_work(void *arg)
{
	java_syms_update_main(arg);
}

static void oncpu_reader_work(void *arg)
{
	thread_index = THREAD_PROFILER_READER_IDX;
	struct bpf_tracer *t = profiler_tracer;

	for (;;) {
		if (unlikely(oncpu_ctx.profiler_stop == 1)) {
			if (oncpu_ctx.enable_bpf_profile)
				set_bpf_run_enabled(t, &oncpu_ctx, 0);

			goto exit;
		}

		/*
		 * Waiting for the regular expression to be configured
		 * and start working. Ensure the socket tracer is in
		 * the 'running' state to prevent starting the profiler
		 * before the socket tracer has completed its attach
		 * operation. The profiler's processing depends on probe
		 * interfaces provided by the socket tracer, such as process
		 * exit events. We want to ensure that everything is ready
		 * before the profiler performs address translation.
		 */
		if (unlikely(get_socket_tracer_state() != TRACER_RUNNING)) {
			if (oncpu_ctx.enable_bpf_profile)
				set_bpf_run_enabled(t, &oncpu_ctx, 0);
			sleep(1);
			continue;
		}

		if (unlikely(!oncpu_ctx.enable_bpf_profile))
			set_bpf_run_enabled(t, &oncpu_ctx, 1);

		process_bpf_stacktraces(&oncpu_ctx, t);
	}

exit:
	print_cp_tracer_status();

	print_hash_stack_str(&oncpu_ctx.stack_str_hash);
	/* free stack_str_hash */
	if (likely(oncpu_ctx.stack_str_hash.buckets != NULL)) {
		release_stack_str_hash(&oncpu_ctx.stack_str_hash);
	}

	print_hash_stack_trace_msg(&oncpu_ctx.msg_hash);
	/* free stack_str_hash */
	if (likely(oncpu_ctx.msg_hash.buckets != NULL)) {
		/* Ensure that all elements are released properly/cleanly */
		push_and_release_stack_trace_msg(&oncpu_ctx,
						 &oncpu_ctx.msg_hash, true);
		stack_trace_msg_hash_free(&oncpu_ctx.msg_hash);
	}

	/* resouce share release */
	release_symbol_caches();

	/* clear thread */
	t->perf_workers[THREAD_PROFILER_READER_IDX] = 0;
	ebpf_info(LOG_CP_TAG "perf profiler reader-thread exit.\n");

	pthread_exit(NULL);
}

static int stack_trace_map_capacity(struct bpf_tracer *tracer)
{
	/*
	 * Calculation method for stack map capacity:
	 *
	 *  `scaling_factor * (ncpus * expected_stack_count_per_cpu)`
	 *
	 * scaling_factor:
	 *   To ensure the integrity and accuracy of data, especially during stack
	 *   tracing in performance analysis or debugging tools, it's crucial to
	 *   include a margin. Stack tracing involves recording the state of program
	 *   call stacks to analyze program behavior and performance.
	 * ncpus:
	 *   This needs to calculate the total expected number of stack traces
	 *   because sampling is done per CPU.
	 * expected_stack_count_per_cpu:
	 *   Represents the expected number of stack traces on each CPU.
	 *   Here we set it as the number of samples per second.
	 *
	 * In kernel, round the given value(capacity) up to nearest power of two.
	 * ref: https://elixir.bootlin.com/linux/v4.19.313/source/kernel/bpf/stackmap.c#L122
	 */

	double scaling_factor = STACKMAP_SCALING_FACTOR;
	int ncpus = sys_cpus_count;
	int expected_stack_count_per_cpu = tracer->sample_freq;
	int capacity =
	    (int)round(scaling_factor * (ncpus * expected_stack_count_per_cpu));
	capacity = 1 << max_log2(capacity);
	if (capacity > STACKMAP_CAPACITY_THRESHOLD)
		capacity = STACKMAP_CAPACITY_THRESHOLD;

	return capacity;
}

static int create_profiler(struct bpf_tracer *tracer)
{
	int ret;

	profiler_tracer = tracer;
	int cap = stack_trace_map_capacity(tracer);
	if ((ret = maps_config(tracer, MAP_STACK_A_NAME, cap)))
		return ret;

	if ((ret = maps_config(tracer, MAP_STACK_B_NAME, cap)))
		return ret;

	if (get_dwarf_enabled() && (major > 5 || (major == 5 && minor >= 2))) {
		if ((ret = maps_config(tracer, MAP_CUSTOM_STACK_A_NAME, cap))) {
			return ret;
		}

		if ((ret = maps_config(tracer, MAP_CUSTOM_STACK_B_NAME, cap))) {
			return ret;
		}

		if ((ret =
		     maps_config(tracer, MAP_PROCESS_SHARD_LIST_NAME,
				 get_dwarf_process_map_size()))) {
			return ret;
		}

		if ((ret =
		     maps_config(tracer, MAP_UNWIND_ENTRY_SHARD_NAME,
				 get_dwarf_shard_map_size()))) {
			return ret;
		}
	}

	extended_maps_set(tracer);

	/* load ebpf perf profiler */
	if (tracer_bpf_load(tracer))
		return ETR_LOAD;

	/* clear old perf files */
	exec_command("/usr/bin/rm -rf /tmp/perf-*.map", "", NULL, 0);
	exec_command("/usr/bin/rm -rf /tmp/perf-*.log", "", NULL, 0);

	ret =
	    create_work_thread("java_update", &java_syms_update_thread,
			       (void *)java_syms_update_work, (void *)tracer);

	if (ret) {
		goto error;
	}

	if (g_enable_oncpu) {
		ebpf_info(LOG_CP_TAG "=== oncpu profiler enabled ===\n");
		tracer->enable_sample = true;
		set_bpf_run_enabled(tracer, &oncpu_ctx, 0);

		/*
		 * create reader for read eBPF-profiler data.
		 * To implement eBPF perf-profiler double buffering output,
		 * it is necessary to create two readers to correspond to
		 * the double buffering structure design.
		 */
		struct bpf_perf_reader *reader_a, *reader_b;
		reader_a =
		    create_perf_buffer_reader(tracer,
					      MAP_PERF_PROFILER_BUF_A_NAME,
					      reader_raw_cb, reader_lost_cb_a,
					      PROFILE_PG_CNT_DEF, 1,
					      PROFILER_READER_EPOLL_TIMEOUT);
		if (reader_a == NULL)
			return ETR_NORESOURCE;

		reader_b =
		    create_perf_buffer_reader(tracer,
					      MAP_PERF_PROFILER_BUF_B_NAME,
					      reader_raw_cb, reader_lost_cb_b,
					      PROFILE_PG_CNT_DEF, 1,
					      PROFILER_READER_EPOLL_TIMEOUT);
		if (reader_b == NULL) {
			free_perf_buffer_reader(reader_a);
			return ETR_NORESOURCE;
		}

		oncpu_ctx.r_a = reader_a;
		oncpu_ctx.r_b = reader_b;

		/*
		 * Start a new thread to execute the data
		 * reading of perf buffer.
		 */
		ret =
		    enable_tracer_reader_work("oncpu_reader",
					      THREAD_PROFILER_READER_IDX,
					      tracer,
					      (void *)&oncpu_reader_work);

		if (ret) {
			goto error;
		}
	} else {
		tracer->enable_sample = false;
		ebpf_info(LOG_CP_TAG "=== oncpu profiler disabled ===\n");
	}

	if (tracer_probes_init(tracer))
		return (-1);

	extended_reader_create(tracer);

	/* attach perf event */
	tracer_hooks_attach(tracer);

	return ETR_OK;

error:
	release_profiler(tracer);
	return ETR_INVAL;
}

static inline bool all_perf_workers_exit(struct bpf_tracer *t)
{
	int i;
	int count = ARRAY_SIZE(t->perf_workers);
	for (i = 0; i < count; i++) {
		if (t->perf_workers[i])
			return false;
	}

	return true;
}

static int cpdbg_sockopt_get(sockoptid_t opt, const void *conf, size_t size,
			     void **out, size_t * outsize)
{
	return 0;
}

static int cpdbg_sockopt_set(sockoptid_t opt, const void *conf, size_t size)
{
	struct cpdbg_msg *msg = (struct cpdbg_msg *)conf;
	pthread_mutex_lock(&cpdbg_mutex);
	if (msg->enable) {
		cpdbg_start_time = get_sys_uptime();
		cpdbg_timeout = msg->timeout;
	}

	if (cpdbg_enable && !msg->enable) {
		cpdbg_timeout = 0;
		cpdbg_start_time = 0;
	}

	cpdbg_enable = msg->enable;
	if (cpdbg_enable) {
		ebpf_info("cpdbg enable timeout %ds\n", cpdbg_timeout);
	} else {
		ebpf_info("cpdbg disable");
	}

	pthread_mutex_unlock(&cpdbg_mutex);

	return 0;
}

static struct tracer_sockopts cpdbg_sockopts = {.version = SOCKOPT_VERSION,
	.set_opt_min = SOCKOPT_SET_CPDBG_ADD,
	.set_opt_max = SOCKOPT_SET_CPDBG_OFF,
	.set = cpdbg_sockopt_set,
	.get_opt_min = SOCKOPT_GET_CPDBG_SHOW,
	.get_opt_max = SOCKOPT_GET_CPDBG_SHOW,
	.get = cpdbg_sockopt_get,
};

int stop_continuous_profiler(void *cb_ctx[PROFILER_CTX_NUM])
{
	if (cb_ctx) {
		memset(cb_ctx, 0, sizeof(void *) * PROFILER_CTX_NUM);
	}
	for (int i = 0; i < ARRAY_SIZE(g_ctx_array); i++) {
		if (g_ctx_array[i] == NULL)
			continue;
		g_ctx_array[i]->profiler_stop = 1;
		if (cb_ctx) {
			cb_ctx[i] = g_ctx_array[i]->callback_ctx;
		}
	}

	if (profiler_tracer == NULL)
		return 0;

	sockopt_unregister(&cpdbg_sockopts);

	// Wait for all reader threads to exit.
	while (!all_perf_workers_exit(profiler_tracer))
		sleep(1);

	if (flame_graph_end_time == NULL) {
		flame_graph_end_time = gen_file_name_by_datetime();
	}

	unwind_tracer_drop();

	release_bpf_tracer(CP_TRACER_NAME);
	profiler_tracer = NULL;

	u64 alloc_b, free_b;
	get_mem_stat(&alloc_b, &free_b);

	ebpf_info(LOG_CP_TAG "== alloc_b %lu bytes, free_b %lu bytes, "
		  "use %lu bytes ==\n", alloc_b, free_b, alloc_b - free_b);
	return 0;
}

void output_profiler_status(struct bpf_tracer *t, void *context)
{
	struct profiler_context *ctx = context;
	u64 alloc_b, free_b;
	get_mem_stat(&alloc_b, &free_b);

	u64 sample_drop_cnt = 0;
	if (!bpf_table_get_value
	    (t, ctx->state_map_name, SAMPLE_CNT_DROP,
	     (void *)&sample_drop_cnt)) {
		ebpf_warning("Get map '%s' sample_drop_cnt failed.\n",
			     ctx->state_map_name);
	}

	u64 output_err_cnt = 0;
	if (!bpf_table_get_value
	    (t, ctx->state_map_name, ERROR_IDX, (void *)&output_err_cnt)) {
		ebpf_warning("Get map '%s' output_err_cnt failed.\n",
			     ctx->state_map_name);
	}

	u64 output_count = 0;
	if (!bpf_table_get_value
	    (t, ctx->state_map_name, OUTPUT_CNT_IDX, (void *)&output_count)) {
		ebpf_warning("Get map '%s' output_cnt failed.\n",
			     ctx->state_map_name);
	}

	u64 iter_max_cnt = 0;
	if (!bpf_table_get_value
	    (t, ctx->state_map_name, SAMPLE_ITER_CNT_MAX,
	     (void *)&iter_max_cnt)) {
		ebpf_warning("Get map '%s' iter_max_cnt failed.\n",
			     ctx->state_map_name);
	}

	u64 is_rt_kern = 0;
	if (!bpf_table_get_value
	    (t, ctx->state_map_name, RT_KERN, (void *)&is_rt_kern)) {
		ebpf_warning("Get map '%s' is_rt_kern failed.\n",
			     ctx->state_map_name);
	}

	u64 is_enabled = 0;
	if (!bpf_table_get_value
	    (t, ctx->state_map_name, ENABLE_IDX, (void *)&is_enabled)) {
		ebpf_warning("Get map '%s' is_enabled failed.\n",
			     ctx->state_map_name);
	}

	ebpf_info("\n\n----------------------------\n"
		  "Profiler Name: %s\nstate_map_name: %s\n"
		  "enabled: %lu\nrecv envent:\t%lu\n"
		  "process-cnt:\t%lu\nkern_lost:\t%lu perf_buf_lost_a:\t%lu, "
		  "perf_buf_lost_b:\t%lu process_lost_count:\t%lu "
		  "stack_table_data_miss:\t%lu\n"
		  "stackmap_clear_failed_count\t%lu\n"
		  "stack_trace_err:\t%lu\ntransfer_count:\t%lu "
		  "iter_count_avg:\t%.2lf\nalloc_b:\t%lu bytes "
		  "free_b:\t%lu bytes use:\t%lu bytes\n"
		  "eBPF map status:\n"
		  " - output_cnt:\t%lu\n"
		  " - sample_drop_cnt:\t%lu\n"
		  " - output_err_cnt:\t%lu\n"
		  " - iter_max_cnt:\t%lu\n"
		  " - is_rt_kern:\t%lu\n"
		  "----------------------------\n\n",
		  ctx->name, ctx->state_map_name, is_enabled,
		  atomic64_read(&t->recv), ctx->process_count,
		  atomic64_read(&t->lost), ctx->perf_buf_lost_a_count,
		  ctx->perf_buf_lost_b_count, get_process_lost_count(ctx),
		  get_stack_table_data_miss_count(),
		  ctx->stackmap_clear_failed_count, ctx->stack_trace_err,
		  ctx->transfer_count,
		  ((double)atomic64_read(&t->recv) /
		   (double)ctx->transfer_count), alloc_b, free_b,
		  alloc_b - free_b, output_count, sample_drop_cnt,
		  output_err_cnt, iter_max_cnt, is_rt_kern);
}

void print_cp_tracer_status(void)
{
	if (profiler_tracer == NULL)
		return;
	output_profiler_status(profiler_tracer, (void *)&oncpu_ctx);
	extended_print_cp_tracer_status();
}

// Function to check if the recorded PID and its start time are correct
int check_profiler_running_pid(int pid)
{
	char path[MAX_PATH_LENGTH];
	snprintf(path, sizeof(path), "/proc/%d/root%s", pid,
		 DEEPFLOW_RUNNING_PID_PATH);
	FILE *file = fopen(path, "r");
	if (!file) {
		if (errno == ENOENT) {
			return ETR_NOTEXIST;
		}
		ebpf_warning("fopen() failed, with %s(%d)\n", strerror(errno),
			     errno);
		return ETR_IO;
	}

	pid_t recorded_pid;
	u64 recorded_start_time;
	if (fscanf(file, "%d,%lu", &recorded_pid, &recorded_start_time) != 2) {
		ebpf_warning("fscanf() failed, with %s(%d)\n", strerror(errno),
			     errno);
		fclose(file);
		return ETR_INVAL;
	}
	fclose(file);

	// Check if the process exists
	if (kill(recorded_pid, 0) == -1 && errno == ESRCH) {
		return ETR_NOTEXIST;
	}
	// Get the actual start time of the process
	u64 actual_start_time =
	    get_process_starttime_and_comm(recorded_pid, NULL, 0);
	if (actual_start_time == 0) {
		return ETR_NOTEXIST;
	}
	// Compare the recorded and actual start times
	if (recorded_start_time == actual_start_time) {
		ebpf_error("The deepflow-agent with process ID %d is already "
			   "running. You can disable the continuous profiling "
			   "feature of the deepflow-agent to skip this check.\n",
			   recorded_pid);
		return ETR_EXIST;
	} else {
		ebpf_info("Recorded PID(%d) and its startup time(%lu) do not"
			  " match(actual start time: %lu); this is an outdated"
			  " process.\n",
			  recorded_pid, recorded_start_time, actual_start_time);
	}

	return ETR_NOTEXIST;
}

int check_profiler_is_running(void)
{
	int pid = find_pid_by_name(DEEPFLOW_AGENT_NAME, getpid());
	if (pid > 0) {
		return check_profiler_running_pid(pid);
	}

	return ETR_NOTEXIST;
}

int write_profiler_running_pid(void)
{
	FILE *file = fopen(DEEPFLOW_RUNNING_PID_PATH, "w");
	if (!file) {
		ebpf_warning("fopen failed, with %s(%d)", strerror(errno),
			     errno);
		return ETR_IO;
	}

	pid_t pid = getpid();
	u64 start_time = get_process_starttime_and_comm(pid, NULL, 0);
	if (start_time == 0) {
		ebpf_warning("get_process_starttime_and_comm() failed.");
		fclose(file);
		return ETR_INVAL;
	}

	fprintf(file, "%d,%lu", pid, start_time);
	fclose(file);
	return ETR_OK;
}

void build_prog_jump_tables(struct bpf_tracer *tracer)
{
	insert_prog_to_map(tracer, MAP_CP_PROGS_JMP_PE_NAME,
			   PROG_DWARF_UNWIND_FOR_PE, PROG_DWARF_UNWIND_PE_IDX);
	insert_prog_to_map(tracer, MAP_CP_PROGS_JMP_PE_NAME,
			   PROG_ONCPU_OUTPUT_FOR_PE, PROG_ONCPU_OUTPUT_PE_IDX);
	insert_prog_to_map(tracer, MAP_CP_PROGS_JMP_PE_NAME,
			   PROG_PYTHON_UNWIND_FOR_PE,
			   PROG_PYTHON_UNWIND_PE_IDX);
	extended_prog_jump_tables(tracer);
}

/*
 * start continuous profiler
 * @freq sample frequency, Hertz. (e.g. 99 profile stack traces at 99 Hertz)
 * @java_syms_update_delay To allow Java to run for an extended period and gather
 *                    more symbol information, we delay symbol retrieval when
 *                    encountering unknown symbols. The default value is
 *                    'JAVA_SYMS_UPDATE_DELAY_DEF'.
 *                    This represents the delay in seconds.
 * @callback Profile data processing callback interface
 * @returns 0 on success, < 0 on error
 */

int start_continuous_profiler(int freq, int java_syms_update_delay,
			      tracer_callback_t callback,
			      void *cb_ctx[PROFILER_CTX_NUM])
{
	char bpf_load_buffer_name[NAME_LEN];
	void *bpf_bin_buffer;
	uword buffer_sz;

	/*
	 * To determine if the profiler is already running, at any given time, only
	 * one profiler can be active due to the persistence required for Java symbol
	 * generation, which is incompatible with multiple agents.
	 */
	if (check_profiler_is_running() != ETR_NOTEXIST)
		exit(EXIT_FAILURE);

	if (!run_conditions_check())
		exit(EXIT_FAILURE);

	if (!cb_ctx) {
		exit(EXIT_FAILURE);
	}

	memset(g_ctx_array, 0, sizeof(g_ctx_array));
	profiler_context_init(&oncpu_ctx, ONCPU_PROFILER_NAME, LOG_CP_TAG,
			      PROFILER_TYPE_ONCPU, g_enable_oncpu,
			      MAP_PROFILER_STATE_NAME, MAP_STACK_A_NAME,
			      MAP_STACK_B_NAME, MAP_CUSTOM_STACK_A_NAME,
			      MAP_CUSTOM_STACK_B_NAME, "", false, true,
			      NANOSEC_PER_SEC / freq,
			      cb_ctx[PROFILER_CTX_ONCPU_IDX]);
	g_ctx_array[PROFILER_CTX_ONCPU_IDX] = &oncpu_ctx;

	if ((java_syms_update_delay < JAVA_SYMS_UPDATE_DELAY_MIN)
	    || (java_syms_update_delay > JAVA_SYMS_UPDATE_DELAY_MAX))
		java_syms_update_delay = JAVA_SYMS_UPDATE_DELAY_DEF;
	set_java_syms_fetch_delay(java_syms_update_delay);
	ebpf_info("set java_syms_update_delay : %lu\n", java_syms_update_delay);

	/*
	 * Initialize cpdbg
	 */
	pthread_mutex_init(&cpdbg_mutex, NULL);

	if (creat_ksyms_cache())
		return (-1);

	// CPUID will not be included in the aggregation of stack trace data.
	set_profiler_cpu_aggregation(0);

	// Java agent so library generation and tools install.
	if (java_libs_and_tools_install() != 0)
		return (-1);

	enum linux_kernel_type k_type;
	if (major > 5 || (major == 5 && minor >= 2)) {
		k_type = K_TYPE_VER_5_2_PLUS;
		snprintf(bpf_load_buffer_name, NAME_LEN,
			 "continuous-profiler-5.2_plus");
		bpf_bin_buffer = (void *)perf_profiler_5_2_plus_ebpf_data;
		buffer_sz = sizeof(perf_profiler_5_2_plus_ebpf_data);
	} else {
		k_type = K_TYPE_COMM;
		snprintf(bpf_load_buffer_name, NAME_LEN,
			 "continuous-profiler-common");
		bpf_bin_buffer = (void *)perf_profiler_common_ebpf_data;
		buffer_sz = sizeof(perf_profiler_common_ebpf_data);
	}

	struct tracer_probes_conf *tps =
	    malloc(sizeof(struct tracer_probes_conf));
	if (tps == NULL) {
		ebpf_warning("malloc() error.\n");
		return -ENOMEM;
	}
	memset(tps, 0, sizeof(*tps));
	init_list_head(&tps->uprobe_syms_head);
	CP_PROFILE_SET_PROBES(tps);
	collect_extended_uprobe_syms_from_procfs(tps);

	struct bpf_tracer *tracer =
	    setup_bpf_tracer(CP_TRACER_NAME, bpf_load_buffer_name,
			     bpf_bin_buffer, buffer_sz, tps,
			     0, release_profiler, create_profiler,
			     (void *)callback, cb_ctx, freq);
	if (tracer == NULL)
		return (-1);

	if (k_type == K_TYPE_VER_5_2_PLUS) {
		if (unwind_tracer_init(tracer) != 0) {
			return -1;
		}
		build_prog_jump_tables(tracer);
	} else {
		ebpf_info
		    ("This kernel version does not support DWARF/Python unwinding.");
	}

	if (sockopt_register(&cpdbg_sockopts) != ETR_OK)
		return (-1);

	tracer->state = TRACER_RUNNING;

	if (write_profiler_running_pid() != ETR_OK)
		return (-1);

	return (0);
}

/*
 * Get running state of continuous profiler
 */
bool continuous_profiler_running()
{
	struct bpf_tracer *t = get_profiler_tracer();
	return t && t->state == TRACER_RUNNING;
}

static u64 test_add_count, stack_count;
static u64 test_hit_count, msg_ptr_zero_count;
void process_stack_trace_data_for_flame_graph(stack_trace_msg_t * msg)
{
	stack_count++;
	if (folded_file == NULL) {
		unlink(FOLDED_FILE_PATH);
		folded_file = fopen(FOLDED_FILE_PATH, "a+");
		if (folded_file == NULL)
			return;
	}

	/* Ensure that the buffer is long enough to accommodate the stack trace string. */
	int len =
	    msg->data_len + sizeof(msg->comm) + sizeof(msg->process_name) + 64;
	char str[len];
	/* profile regex match ? */
	if (msg->stime > 0)
		snprintf(str, len, "%s (%d);%s %lu\n", msg->process_name,
			 msg->pid, msg->data, msg->count);
	else
		snprintf(str, len, "%s;%s %lu\n", msg->process_name,	/*msg->pid, */
			 msg->data, msg->count);

	os_puts(folded_file, str, strlen(str), false);
}

void release_flame_graph_hash(void)
{
	u64 alloc_b, free_b;
	get_mem_stat(&alloc_b, &free_b);
	ebpf_info(LOG_CP_TAG
		  "pre alloc_b:\t%lu bytes free_b:\t%lu bytes use:\t%lu"
		  " bytes\n", alloc_b, free_b, alloc_b - free_b);
	if (folded_file)
		fclose(folded_file);

	get_mem_stat(&alloc_b, &free_b);
#ifdef DF_MEM_DEBUG
	show_mem_list();
#endif
	ebpf_info(LOG_CP_TAG
		  "after alloc_b:\t%lu bytes free_b:\t%lu bytes use:\t%lu"
		  " bytes\n", alloc_b, free_b, alloc_b - free_b);

	ebpf_info(LOG_CP_TAG
		  "<<< stack_count %lu add_count %lu hit_count %lu msg_ptr_zero"
		  "_count %lu push_count %lu >>>\n", stack_count,
		  test_add_count, test_hit_count, msg_ptr_zero_count,
		  oncpu_ctx.push_count);

	ebpf_info(LOG_CP_TAG
		  "Please use the following command to generate a flame graph:"
		  "\n\n\033[33;1mcat ./profiler.folded |./.flamegraph.pl"
		  " --countname=samples --inverted > profiler-from_%s_to_%s.svg\033[0m\n",
		  flame_graph_start_time, flame_graph_end_time);
}

int set_profiler_cpu_aggregation(int flag)
{
	if (flag != 0 && flag != 1) {
		ebpf_info(LOG_CP_TAG
			  "Set 'cpu_aggregation_flag' parameter invalid.\n");
		return (-1);
	}

	oncpu_ctx.cpu_aggregation_flag = (u64) flag;

	ebpf_info(LOG_CP_TAG
		  "Set 'cpu_aggregation_flag' successful, value %d\n", flag);
	return (0);
}

struct bpf_tracer *get_profiler_tracer(void)
{
	return profiler_tracer;
}

/*
 * Configure and enable the debugging functionality for Continuous Profiling.
 *
 * @timeout
 *   Specifying the timeout duration. If the elapsed time exceeds this
 *   duration, cpdbg will stop. The unit is in seconds.
 * @callback
 *   Callback interface, used to transfer data to the remote controller.
 *
 * @return 0 on success, and a negative value on failure.
 */
int cpdbg_set_config(int timeout, debug_callback_t cb)
{
	if (timeout < 0 || cb == NULL) {
		ebpf_warning("Invalid parameter\n");
		return -1;
	}

	pthread_mutex_lock(&cpdbg_mutex);
	if (cpdbg_enable) {
		ebpf_warning("cpdbg is already running\n");
		goto finish;
	}

	cpdbg_start_time = get_sys_uptime();
	cpdbg_timeout = timeout;
	cpdbg_enable = true;
	cpdbg_use_remote = true;
	cpdbg_cb = cb;

	ebpf_info("cpdbg enable timeout %ds\n", cpdbg_timeout);

finish:
	pthread_mutex_unlock(&cpdbg_mutex);
	return 0;
}

int enable_oncpu_profiler(void)
{
	g_enable_oncpu = true;
	ebpf_info(LOG_CP_TAG "Set oncpu profiler enable.\n");
	return 0;
}

int disable_oncpu_profiler(void)
{
	g_enable_oncpu = false;
	ebpf_info(LOG_CP_TAG "Set oncpu profiler disable.\n");
	return 0;
}

bool oncpu_profiler_enabled(void)
{
	return g_enable_oncpu;
}

void profiler_match_pid_handle(int feat, int pid, enum match_pids_act act)
{
	if (feat == FEATURE_PROFILE_ONCPU || feat == FEATURE_PROFILE_OFFCPU
	    || feat == FEATURE_PROFILE_MEMORY) {
		if (act == MATCH_PID_ADD) {
			unwind_process_exec(pid);
		} else if (act == MATCH_PID_DEL) {
			unwind_process_exit(pid);
		}
	}
}

#else /* defined AARCH64_MUSL */
#include "../tracer.h"
#include "perf_profiler.h"

int start_continuous_profiler(int freq, int java_syms_update_delay,
			      tracer_callback_t callback,
			      void *cb_ctx[PROFILER_CTX_NUM])
{
	return (-1);
}

/*
 * Get running state of continuous profiler
 */
bool continuous_profiler_running()
{
	return false;
}

int stop_continuous_profiler(void *cb_ctx[PROFILER_CTX_NUM])
{
	return (0);
}

void process_stack_trace_data_for_flame_graph(stack_trace_msg_t * val)
{
	return;
}

void release_flame_graph_hash(void)
{
	return;
}

int set_profiler_cpu_aggregation(int flag)
{
	return (-1);
}

struct bpf_tracer *get_profiler_tracer(void)
{
	return NULL;
}

void set_bpf_run_enabled(struct bpf_tracer *t, struct profiler_context *ctx,
			 u64 enable_flag)
{
}

int cpdbg_set_config(int timeout, debug_callback_t cb)
{
}

int enable_oncpu_profiler(void)
{
	return 0;
}

int disable_oncpu_profiler(void)
{
	return 0;
}

bool oncpu_profiler_enabled(void)
{
	return false;
}

void print_cp_tracer_status(void)
{
}

void profiler_match_pid_handle(int feat, int pid, enum match_pids_act act)
{
}

#endif /* AARCH64_MUSL */
