#define _GNU_SOURCE
#include "tracer.h"
#include <arpa/inet.h>
#include "libbpf/include/linux/err.h"
#include <sched.h>
#include "probe.h"
#include "table.h"
#include "common.h"
#include "socket.h"
#include "log.h"

#define TRACER_NAME	"socket-trace"
#define TRACER_ELF_NAME	"socket_trace.elf"

// eBPF Map Name
#define MAP_MEMBERS_OFFSET_NAME		"__members_offset"
#define MAP_SOCKET_INFO_NAME		"__socket_info_map"
#define MAP_TRACE_NAME			"__trace_map"
#define MAP_PERF_SOCKET_DATA_NAME	"__socket_data"
#define MAP_TRACE_UID_NAME		"__trace_uid_map"
#define MAP_TRACE_STATS_NAME		"__trace_stats_map"

// 在socket map回收时，对每条socket信息超过10秒没有收发动作就回收掉
#define SOCKET_RECLAIM_TIMEOUT_DEF  10
// 在trace map回收时，对每条trace信息超过10秒没有发生匹配动作就回收掉
#define TRACE_RECLAIM_TIMEOUT_DEF   10
static uint64_t socket_map_reclaim_count; // socket map回收数量统计
static uint64_t trace_map_reclaim_count;  // trace map回收数量统计

extern int sys_cpus_count;
extern bool *cpu_online;

static int infer_socktrace_fd;
static uint32_t conf_max_socket_entries;
static uint32_t conf_max_trace_entries;

// socket map 进行回收的最大阈值，超过这个值进行map回收。
static uint32_t conf_socket_map_max_reclaim;

extern int major, minor;
extern uint64_t sys_boot_time_ns;
static bool bpf_stats_map_collect(struct bpf_tracer *tracer,
				  struct trace_stats *stats_total);
static bool is_adapt_success(struct bpf_tracer *t);
static int socket_tracer_stop(void);
static int socket_tracer_start(void);

static void socket_tracer_set_probes(struct trace_probes_conf *tps)
{
	int index = 0, curr_idx;

	probes_set_enter_symbol(tps, "__sys_sendmsg");
	probes_set_enter_symbol(tps, "__sys_sendmmsg");
	probes_set_enter_symbol(tps, "__sys_recvmsg");
	probes_set_enter_symbol(tps, "__sys_recvmmsg");
	probes_set_enter_symbol(tps, "do_writev");
	probes_set_enter_symbol(tps, "do_readv");
	tps->probes_nr = index;

	/* tracepoints */
	index = 0;

	/*
	 * 由于在Linux 4.17+ sys_write, sys_read, sys_sendto, sys_recvfrom
	 * 接口会发生变化为了避免对内核的依赖采用tracepoints方式
	 */
	tps_set_symbol(tps, "sys_enter_write");
	tps_set_symbol(tps, "sys_enter_read");
	tps_set_symbol(tps, "sys_enter_sendto");
	tps_set_symbol(tps, "sys_enter_recvfrom");

	// exit tracepoints
	tps_set_symbol(tps, "sys_exit_socket");
	tps_set_symbol(tps, "sys_exit_read");
	tps_set_symbol(tps, "sys_exit_write");
	tps_set_symbol(tps, "sys_exit_sendto");
	tps_set_symbol(tps, "sys_exit_recvfrom");
	tps_set_symbol(tps, "sys_exit_sendmsg");
	tps_set_symbol(tps, "sys_exit_sendmmsg");
	tps_set_symbol(tps, "sys_exit_recvmsg");
	tps_set_symbol(tps, "sys_exit_recvmmsg");
	tps_set_symbol(tps, "sys_exit_writev");
	tps_set_symbol(tps, "sys_exit_readv");

	// 周期性触发用于缓存的数据的超时检查
	tps_set_symbol(tps, "sys_enter_getppid");

	// clear trace connection
	tps_set_symbol(tps, "sys_enter_close");

	tps->tps_nr = index;
}

/* ==========================================================
 * 内核结构成员偏移推断，模拟一个TCP通信tick内核，使其完成推断
 * ==========================================================
 */
static int kernel_offset_infer_server(void)
{
	int cli_fd;
	struct sockaddr_in client_addr;
	socklen_t addr_len = sizeof(client_addr);
	memset(&client_addr, 0, sizeof(struct sockaddr_in));
	int client_count = 0, cpu_online_count = 0, i;
	for (i = 0; i < sys_cpus_count; i++) {
		if (cpu_online[i])
			cpu_online_count++;
	}

next_cpu_client:
	cli_fd =
	    accept(infer_socktrace_fd, (struct sockaddr *)&client_addr,
		   &addr_len);
	if (cli_fd < 0) {
		ebpf_info("[%s] Fail to accept client request\n", __func__);
		return ETR_IO;
	}

	char buffer[16];
	int len;
	for (;;) {
		len = recv(cli_fd, buffer, sizeof(buffer), 0);
		if (len < 0) {
			continue;
		}

		if (len == 0) {
			client_count++;
			close(cli_fd);
			break;
		}

		buffer[len] = '\0';
		if (strcmp(buffer, "hello") == 0) {
			ebpf_info
			    ("kernel_offset_infer_server rcv message: \"%s\"\n",
			     buffer);
			snprintf(buffer, sizeof(buffer), "OK");
			send(cli_fd, buffer, 2, 0);
		}
	}

	if (client_count < cpu_online_count)
		goto next_cpu_client;

	close(infer_socktrace_fd);
	ebpf_info("kernel_offset_infer_server close. client_count:%d\n",
		  client_count);
	return ETR_OK;
}

static int kernel_offset_infer_client(void)
{
	int cli_fd;
	struct sockaddr_in server_addr;
	char buf[16];
	int len;

	if ((cli_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		ebpf_info("[%s] Fail client create socket\n", __func__);
		return ETR_IO;
	}

	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(OFFSET_INFER_SERVER_PORT);
	server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

	if (connect
	    (cli_fd, (struct sockaddr *)&server_addr,
	     sizeof(server_addr)) < 0) {
		ebpf_info("[%s] Fail to connect\n", __func__);
		return ETR_IO;
	}

	for (;;) {
		snprintf(buf, sizeof(buf), "hello");
		len = send(cli_fd, buf, strlen(buf), 0);
		if (len != strlen(buf))
			continue;
	      rcv_loop:
		len = recv(cli_fd, buf, sizeof(buf), 0);
		if (len > 0) {
			buf[len] = '\0';
			break;
		} else if (len == 0) {
			break;
		} else
			goto rcv_loop;
	}

	close(cli_fd);

	ebpf_info("kernel_offset_infer_client rcv message: \"%s\"\n", buf);
	return ETR_OK;
}

static int kernel_offset_infer_init(void)
{
	struct sockaddr_in srv_addr;
	memset(&srv_addr, 0, sizeof(srv_addr));

	infer_socktrace_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (infer_socktrace_fd < 0) {
		ebpf_info("[%s] Fail to create server socket\n", __func__);
		return ETR_IO;
	}

	srv_addr.sin_family = AF_INET;
	srv_addr.sin_port = htons(OFFSET_INFER_SERVER_PORT);
	srv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

	if (-1 ==
	    bind(infer_socktrace_fd, (struct sockaddr *)&srv_addr,
		 sizeof(srv_addr))) {
		ebpf_info("[%s] Fail to bind server socket\n", __func__);
		close(infer_socktrace_fd);
		return ETR_IO;
	}

	if (-1 == listen(infer_socktrace_fd, 1)) {
		ebpf_info("[%s] Server socket listen failed\n", __func__);
		close(infer_socktrace_fd);
		return ETR_IO;
	}

	return ETR_OK;
}

static int socktrace_sockopt_set(sockoptid_t opt, const void *conf, size_t size)
{
	return 0;
}

static bool bpf_offset_map_collect(struct bpf_tracer *tracer,
				   struct bpf_offset_param_array *array)
{
	int nr_cpus = libbpf_num_possible_cpus();
	struct bpf_offset_param values[nr_cpus];
	if (!bpf_table_get_value(tracer, MAP_MEMBERS_OFFSET_NAME, 0, values))
		return false;

	struct bpf_offset_param *out_val =
	    (struct bpf_offset_param *)(array + 1);

	int i;
	for (i = 0; i < array->count; i++)
		out_val[i] = values[i];

	return true;
}

static int socktrace_sockopt_get(sockoptid_t opt, const void *conf, size_t size,
				 void **out, size_t * outsize)
{
	*outsize = sizeof(struct bpf_socktrace_params) +
	    sizeof(struct bpf_offset_param) * sys_cpus_count;

	*out = calloc(1, *outsize);
	if (*out == NULL) {
		ebpf_info("%s calloc, error:%s\n", __func__, strerror(errno));
		return -1;
	}

	struct bpf_socktrace_params *params = *out;
	struct bpf_offset_param_array *array = &params->offset_array;
	array->count = sys_cpus_count;

	struct bpf_tracer *t = find_bpf_tracer(TRACER_NAME);
	if (t == NULL)
		return -1;

	params->kern_socket_map_max = conf_max_socket_entries;
	params->kern_trace_map_max = conf_max_trace_entries;

	struct trace_stats stats_total;

	if (bpf_stats_map_collect(t, &stats_total)) {
		params->kern_socket_map_used = stats_total.socket_map_count;
		params->kern_trace_map_used = stats_total.trace_map_count;
	}

	if (!bpf_offset_map_collect(t, array)) {
		free(*out);
		return -1;
	}

	return 0;
}

static struct tracer_sockopts socktrace_sockopts = {
	.version = SOCKOPT_VERSION,
	.set_opt_min = SOCKOPT_SET_SOCKTRACE_ADD,
	.set_opt_max = SOCKOPT_SET_SOCKTRACE_FLUSH,
	.set = socktrace_sockopt_set,
	.get_opt_min = SOCKOPT_GET_SOCKTRACE_SHOW,
	.get_opt_max = SOCKOPT_GET_SOCKTRACE_SHOW,
	.get = socktrace_sockopt_get,
};

// TODO : 标记上层是否需要重新确认协议准确性
// 目前上层没有实现协议再次确认的功能,对需要重新确认的包直接丢弃,这里临时设置数据包不需要重新确认
// 上层实现重新确认功能后再使用
static inline bool need_proto_reconfirm(uint16_t l7_proto)
{
	return false;
}

static void reader_raw_cb(void *t, void *raw, int raw_size)
{
	struct bpf_tracer *tracer = (struct bpf_tracer *)t;
	struct __socket_data_buffer *buf = (struct __socket_data_buffer *)raw;

	if (buf->events_num == 0)
		return;

	int i, start = 0;
	struct __socket_data *sd;

	// 确定分发到哪个队列上，通过第一个socket_data来确定
	sd = (struct __socket_data *)&buf->data[start];
	int q_idx = xxhash(sd->socket_id) % tracer->dispatch_workers_nr;
	struct queue *q = &tracer->queues[q_idx];

	if (buf->events_num > MAX_PKT_BURST) {
		ebpf_info
		    ("buf->events_num > MAX_PKT_BURST(16) error. events_num:%d\n",
		     buf->events_num);
		return;
	}

	struct socket_bpf_data *burst_data[MAX_PKT_BURST];

	/*
	 * ----------- -> memory block ptr (free_ptr)
	 *         |                /\
	 *         |                *
	 * --------------------|    *
	 *      mem_block_head |    *
	 *      >is_last-------|    *   is_last 判断是否是内存块中最后一个socket data,
	 *      >*free_ptr     | ****   如果是释放整个内存。
	 *      ---------------|----> burst enqueue
	 *                     |
	 *      socket_data    |
	 *                     |
	 * --------------------|
	 *         |
	 *         |
	 * ---------
	 */
	struct socket_bpf_data *submit_data;
	int len;
	struct mem_block_head *block_head;	// 申请内存块的指针
	void *data_buf_ptr;

	// 所有载荷的数据总大小（去掉头）
	int alloc_len = buf->len - offsetof(typeof(struct __socket_data),
					    data) * buf->events_num;
	alloc_len += sizeof(*submit_data) * buf->events_num;	// 计算长度包含要提交的数据的头
	alloc_len += sizeof(struct mem_block_head) * buf->events_num;	// 包含内存块head
	alloc_len += sizeof(sd->extra_data) * buf->events_num;	// 可能包含额外数据
	alloc_len = CACHE_LINE_ROUNDUP(alloc_len);	// 保持cache line对齐

	void *socket_data_buff = malloc(alloc_len);
	if (socket_data_buff == NULL) {
		ebpf_warning("malloc() error.\n");
		atomic64_inc(&q->heap_get_faild);
		return;
	}

	data_buf_ptr = socket_data_buff;

	for (i = 0; i < buf->events_num; i++) {
		sd = (struct __socket_data *)&buf->data[start];
		len = sd->data_len;
		block_head = (struct mem_block_head *)data_buf_ptr;
		block_head->is_last = 0;
		block_head->free_ptr = socket_data_buff;

		data_buf_ptr = block_head + 1;
		submit_data = data_buf_ptr;

		submit_data->socket_id = sd->socket_id;

		// 数据捕获时间戳，精度为微秒(us)
		submit_data->timestamp =
		    (sd->timestamp + sys_boot_time_ns) / 1000ULL;

		submit_data->tuple = sd->tuple;
		submit_data->direction = sd->direction;
		submit_data->l7_protocal_hint = sd->data_type;
		submit_data->need_reconfirm =
		    need_proto_reconfirm(sd->data_type);
		submit_data->process_id = sd->tgid;
		submit_data->thread_id = sd->pid;
		submit_data->cap_data =
		    (char *)((void **)&submit_data->cap_data + 1);
		submit_data->syscall_len = sd->syscall_len;
		submit_data->tcp_seq = sd->tcp_seq;
		submit_data->cap_seq = sd->data_seq;
		submit_data->syscall_trace_id_call = sd->thread_trace_id;
		memcpy(submit_data->process_name,
		       sd->comm, sizeof(submit_data->process_name));
		submit_data->process_name[sizeof(submit_data->process_name) -
					  1] = '\0';
		submit_data->msg_type = sd->msg_type;

		// 各种协议的统计
		if (sd->data_type >= PROTO_NUM)
			sd->data_type = PROTO_UNKNOWN;

		atomic64_inc(&tracer->proto_status[sd->data_type]);
		int offset = 0;
		if (len > 0) {
			if (sd->extra_data_count > 0) {
				*(uint32_t *) submit_data->cap_data =
				    sd->extra_data;
				offset = sizeof(sd->extra_data);
			}

			memcpy_fast(submit_data->cap_data + offset, sd->data,
				    len);
			submit_data->cap_data[len + offset] = '\0';
		}
		submit_data->syscall_len += offset;
		submit_data->cap_len = len + offset;
		burst_data[i] = submit_data;

		start +=
		    (offsetof(typeof(struct __socket_data), data) +
		     sd->data_len);

		data_buf_ptr += sizeof(*submit_data) + submit_data->cap_len;
	}

	int nr = ring_sp_enqueue_burst
	    (q->r, (void **)burst_data, buf->events_num, NULL);

	if (nr < buf->events_num) {
		int lost = buf->events_num - nr;
		ebpf_info("%s, ring_sp_enqueue lost %d.\n", __func__, lost);
		atomic64_add(&q->enqueue_lost, lost);
		if (lost == buf->events_num) {
			free(socket_data_buff);
			return;
		}
	}

	submit_data = burst_data[nr - 1];
	block_head = (struct mem_block_head *)submit_data - 1;
	block_head->is_last = 1;

	/*
	 * 通知工作线程进行dequeue，并进行数据处理。
	 */
	pthread_mutex_lock(&q->mutex);
	pthread_cond_signal(&q->cond);
	pthread_mutex_unlock(&q->mutex);

	atomic64_add(&q->enqueue_nr, nr);
}

static void reader_lost_cb(void *t, uint64_t lost)
{
	struct bpf_tracer *tracer = (struct bpf_tracer *)t;
	atomic64_add(&tracer->lost, lost);
}

static void reclaim_trace_map(struct bpf_tracer *tracer, uint32_t timeout)
{
	struct bpf_map *map =
	    bpf_object__find_map_by_name(tracer->pobj, MAP_TRACE_NAME);
	int map_fd = bpf_map__fd(map);

	uint64_t trace_key = 0, next_trace_key;
	uint32_t reclaim_count = 0;
	struct trace_info_t value;
	uint32_t uptime = get_sys_uptime();

	while (bpf_map_get_next_key(map_fd, &trace_key, &next_trace_key) == 0) {
		if (bpf_map_lookup_elem(map_fd, &next_trace_key, &value) == 0) {
			if (uptime - value.update_time > timeout) {
				bpf_map_delete_elem(map_fd, &next_trace_key);
				reclaim_count++;
			}
		}

		trace_key = next_trace_key;
	}

	trace_map_reclaim_count += reclaim_count;
	ebpf_info("[%s] trace map reclaim_count :%u\n", __func__,
		  reclaim_count);
}

static void reclaim_socket_map(struct bpf_tracer *tracer, uint32_t timeout)
{
	struct bpf_map *map =
	    bpf_object__find_map_by_name(tracer->pobj, MAP_SOCKET_INFO_NAME);
	int map_fd = bpf_map__fd(map);

	uint64_t conn_key, next_conn_key;
	uint32_t sockets_reclaim_count = 0;
	struct socket_info_t value;
	conn_key = 0;
	uint32_t uptime = get_sys_uptime();

	while (bpf_map_get_next_key(map_fd, &conn_key, &next_conn_key) == 0) {
		if (bpf_map_lookup_elem(map_fd, &next_conn_key, &value) == 0) {
			if (uptime - value.update_time > timeout) {
				bpf_map_delete_elem(map_fd, &next_conn_key);
				sockets_reclaim_count++;
			}
		}
		conn_key = next_conn_key;
	}

	socket_map_reclaim_count += sockets_reclaim_count;
	ebpf_info("[%s] sockets_reclaim_count :%u\n", __func__,
		  sockets_reclaim_count);
}

static int check_map_exceeded(void)
{
	struct bpf_tracer *t = find_bpf_tracer(TRACER_NAME);
	if (t == NULL)
		return -1;

	uint64_t kern_socket_map_used = 0, kern_trace_map_used = 0;

	struct trace_stats stats_total;

	if (bpf_stats_map_collect(t, &stats_total)) {
		kern_socket_map_used = stats_total.socket_map_count;
		kern_trace_map_used = stats_total.trace_map_count;
	}
	// 校准map的统计数量
	kern_socket_map_used -= socket_map_reclaim_count;
	kern_trace_map_used -= trace_map_reclaim_count;

	if (kern_socket_map_used >= conf_socket_map_max_reclaim) {
		ebpf_info("Current socket map used %u exceed"
			  " conf_socket_map_max_reclaim %u,reclaim map\n",
			  kern_socket_map_used, conf_socket_map_max_reclaim);
		reclaim_socket_map(t, SOCKET_RECLAIM_TIMEOUT_DEF);
	}

	if (kern_trace_map_used >=
	    (uint64_t) (conf_max_trace_entries * RECLAIM_TRACE_MAP_SCALE)) {
		ebpf_info("Current trace map used %u exceed"
			  " reclaim_map_max %u,reclaim map\n",
			  kern_trace_map_used,
			  (uint32_t) (conf_max_trace_entries *
				      RECLAIM_TRACE_MAP_SCALE));
		reclaim_trace_map(t, TRACE_RECLAIM_TIMEOUT_DEF);
	}

	return 0;
}

static int check_kern_adapt_and_state_update(void)
{
	struct bpf_tracer *t = find_bpf_tracer(TRACER_NAME);
	if (t == NULL)
		return -1;

	if (is_adapt_success(t)) {
		ebpf_info("Linux %d.%d adapt success.\n", major, minor);
		if (tracer_hooks_detach(t) == 0) {
			t->state = TRACER_STOP;
			ebpf_info("Set current state: TRACER_STOP.\n");
		}
		set_period_event_invalid("check-kern-adapt");
		t->adapt_success = true;
	}

	return 0;
}

int running_socket_tracer(l7_handle_fn handle,
			  int thread_nr,
			  uint32_t perf_pages_cnt,
			  uint32_t queue_size,
			  uint32_t max_socket_entries,
			  uint32_t max_trace_entries,
			  uint32_t socket_map_max_reclaim)
{
	int ret;
	char bpf_path[TRACER_PATH_LEN];
	char *bpf_file_name = TRACER_ELF_NAME;

	if (check_kernel_version(4, 14) != 0) {
		ebpf_warning("Currnet linux %d.%d, not support, require Linux 4.14+\n",
			     major, minor);

		return -EINVAL;
	}

	if (is_core_kernel())
		snprintf(bpf_path, TRACER_PATH_LEN,
			 "%s/linux-core/%s", ELF_PATH_PREFIX, bpf_file_name);
	else if (major == 5 && minor == 2)
		snprintf(bpf_path, TRACER_PATH_LEN,
			 "%s/linux-%d.%d/%s", ELF_PATH_PREFIX,
			 major, minor, bpf_file_name);

	else
		snprintf(bpf_path, TRACER_PATH_LEN,
			 "%s/linux-common/%s", ELF_PATH_PREFIX, bpf_file_name);

	struct trace_probes_conf *tps =
	    malloc(sizeof(struct trace_probes_conf));
	if (tps == NULL) {
		ebpf_warning("malloc() error.\n");
		return -ENOMEM;
	}
	memset(tps, 0, sizeof(*tps));
	socket_tracer_set_probes(tps);
	struct bpf_tracer *tracer =
	    create_bpf_tracer(TRACER_NAME, bpf_path, tps,
			      thread_nr, (void *)handle, perf_pages_cnt);
	if (tracer == NULL)
		return -EINVAL;

	tracer->state = TRACER_INIT;
	tracer->adapt_success = false;

	/*
	 * config perf ring-buffer reader callbak
	 */
	tracer->raw_cb = reader_raw_cb;
	tracer->lost_cb = reader_lost_cb;

	tracer->stop_handle = socket_tracer_stop;
	tracer->start_handle = socket_tracer_start;

	if ((ret =
	     maps_config(tracer, MAP_SOCKET_INFO_NAME, max_socket_entries)))
		return ret;

	conf_max_socket_entries = max_socket_entries;

	conf_socket_map_max_reclaim = socket_map_max_reclaim;

	if ((ret = maps_config(tracer, MAP_TRACE_NAME, max_trace_entries)))
		return ret;

	conf_max_trace_entries = max_trace_entries;

	if (tracer_bpf_load(tracer))
		return -EINVAL;

	if (tracer_hooks_attach(tracer))
		return -EINVAL;

	if (perf_map_init(tracer, MAP_PERF_SOCKET_DATA_NAME))
		return -EINVAL;

	uint64_t uid_base = (gettime(CLOCK_REALTIME, TIME_TYPE_NAN) / 100) &
	    0xffffffffffffffULL;
	if (uid_base == 0)
		return -EINVAL;

	uint16_t cpu;
	struct trace_uid_t t_uid[MAX_CPU_NR];
	for (cpu = 0; cpu < MAX_CPU_NR; cpu++) {
		t_uid[cpu].socket_id = (uint64_t) cpu << 56 | uid_base;
		t_uid[cpu].coroutine_trace_id = t_uid[cpu].socket_id;
		t_uid[cpu].thread_trace_id = t_uid[cpu].socket_id;
	}

	if (!bpf_table_set_value(tracer, MAP_TRACE_UID_NAME, 0, (void *)&t_uid))
		return -EINVAL;

	if ((ret = dispatch_worker(tracer, queue_size)))
		return ret;

	// use for inference struct offset.
	if (kernel_offset_infer_init() != ETR_OK)
		return -EINVAL;

	if ((ret = register_extra_waiting_op("offset-infer-server",
					     kernel_offset_infer_server,
					     EXTRA_TYPE_SERVER)))
		return ret;

	if ((ret = register_extra_waiting_op("offset-infer-client",
					     kernel_offset_infer_client,
					     EXTRA_TYPE_CLIENT)))
		return ret;

	if ((ret =
	     register_period_event_op("check-map-exceeded",
				      check_map_exceeded)))
		return ret;

	if ((ret =
	     register_period_event_op("check-kern-adapt",
				      check_kern_adapt_and_state_update)))
		return ret;

	if ((ret = sockopt_register(&socktrace_sockopts)) != ETR_OK)
		return ret;

	return 0;
}

static int socket_tracer_stop(void)
{
	int ret = -1;
	struct bpf_tracer *t = find_bpf_tracer(TRACER_NAME);
	if (t == NULL)
		return ret;

	if (t->state == TRACER_INIT) {
		ebpf_info
		    ("socket_tracer state is TRACER_INIT, not permit stop.\n");
		return -1;
	}

	if ((ret = tracer_hooks_detach(t)) == 0) {
		t->state = TRACER_STOP;
		ebpf_info("Tracer stop success, current state: TRACER_STOP\n");
	}

	//清空 ebpf map
	reclaim_socket_map(t, 0);

	return ret;
}

static int socket_tracer_start(void)
{
	int ret = -1;
	struct bpf_tracer *t = find_bpf_tracer(TRACER_NAME);
	if (t == NULL)
		return ret;

	if (t->state == TRACER_INIT) {
		ebpf_info
		    ("socket_tracer state is TRACER_INIT, not permit start.\n");
		return -1;
	}

	if ((ret = tracer_hooks_attach(t)) == 0) {
		t->state = TRACER_RUNNING;
		ebpf_info("Tracer start success, current state: TRACER_RUNNING\n");
	}

	return ret;
}

static bool bpf_stats_map_collect(struct bpf_tracer *tracer,
				  struct trace_stats *stats_total)
{
	int nr_cpus = libbpf_num_possible_cpus();
	struct trace_stats values[nr_cpus];
	memset(values, 0, sizeof(values));

	if (!bpf_table_get_value(tracer, MAP_TRACE_STATS_NAME, 0, values))
		return false;

	memset(stats_total, 0, sizeof(*stats_total));

	int i;
	for (i = 0; i < nr_cpus; i++) {
		stats_total->socket_map_count += values[i].socket_map_count;
		stats_total->trace_map_count += values[i].trace_map_count;
	}

	return true;
}

static bool is_adapt_success(struct bpf_tracer *t)
{
	bool is_success = false;
	int i;

	if (sys_cpus_count > 0) {
		struct bpf_offset_param *offset;
		struct bpf_offset_param_array *array =
		    malloc(sizeof(*array) + sizeof(*offset) * sys_cpus_count);
		if (array == NULL) {
			ebpf_warning("malloc() error.\n");
			return false;
		}

		array->count = sys_cpus_count;

		if (!bpf_offset_map_collect(t, array)) {
			free(array);
			return false;
		}

		offset = (struct bpf_offset_param *)(array + 1);
		for (i = 0; i < sys_cpus_count; i++) {
			if (!cpu_online[i])
				continue;
			if (offset[i].ready != 1) {
				is_success = false;
				break;
			} else
				is_success = true;
		}

		free(array);
	}

	return is_success;
}

struct socket_trace_stats socket_tracer_stats(void)
{
	struct socket_trace_stats stats;
	memset(&stats, 0, sizeof(stats));

	struct bpf_tracer *t = find_bpf_tracer(TRACER_NAME);
	if (t == NULL)
		return stats;

	stats.kern_lost = atomic64_read(&t->lost);
	stats.worker_num = t->dispatch_workers_nr;
	stats.perf_pages_cnt = t->perf_pages_cnt;
	stats.queue_capacity = t->queues[0].ring_size;
	stats.kern_socket_map_max = conf_max_socket_entries;
	stats.kern_trace_map_max = conf_max_trace_entries;
	stats.socket_map_max_reclaim = conf_socket_map_max_reclaim;

	struct trace_stats stats_total;

	if (bpf_stats_map_collect(t, &stats_total)) {
		stats.kern_socket_map_used = stats_total.socket_map_count;
		stats.kern_trace_map_used = stats_total.trace_map_count;
	}

	int i;
	for (i = 0; i < t->dispatch_workers_nr; i++) {
		stats.user_enqueue_lost +=
		    atomic64_read(&t->queues[i].enqueue_lost);
		stats.user_enqueue_count +=
		    atomic64_read(&t->queues[i].enqueue_nr);
		stats.user_dequeue_count +=
		    atomic64_read(&t->queues[i].dequeue_nr);
		stats.queue_burst_count +=
		    atomic64_read(&t->queues[i].burst_count);
		stats.mem_alloc_fail_count +=
		    atomic64_read(&t->queues[i].heap_get_faild);
	}

	stats.is_adapt_success = t->adapt_success;
	stats.tracer_state = t->state;

	return stats;
}

// -------------------------------------
// 协议测试
// -------------------------------------

/***********************************
DNS
***********************************/
//DNS header structure

struct DNS_HEADER {
	unsigned short id;	// identification number

	unsigned char rd:1;	// recursion desired
	unsigned char tc:1;	// truncated message
	unsigned char aa:1;	// authoritive answer
	unsigned char opcode:4;	// purpose of message
	unsigned char qr:1;	// query/response flag

	unsigned char rcode:4;	// response code
	unsigned char cd:1;	// checking disabled
	unsigned char ad:1;	// authenticated data
	unsigned char z:1;	// its z! reserved
	unsigned char ra:1;	// recursion available

	unsigned short q_count;	// number of question entries
	unsigned short ans_count;	// number of answer entries
	unsigned short auth_count;	// number of authority entries
	unsigned short add_count;	// number of resource entries
};

//Constant sized fields of query structure
struct QUESTION {
	unsigned short qtype;
	unsigned short qclass;
};

//Constant sized fields of the resource record structure
#pragma pack(push, 1)
struct R_DATA {
	unsigned short type;
	unsigned short _class;
	unsigned int ttl;
	unsigned short data_len;
};
#pragma pack(pop)

//Pointers to resource record contents
struct RES_RECORD {
	unsigned char *name;
	struct R_DATA *resource;
	unsigned char *rdata;
};

//Structure of a Query
typedef struct {
	unsigned char *name;
	struct QUESTION *ques;
} QUERY;

// ------------------------------------------

static unsigned char *read_name(unsigned char *reader, unsigned char *buffer,
				const char *buf, int *count)
{
	unsigned char *name;
	unsigned int p = 0, jumped = 0, offset;
	int i, j;

	*count = 1;
	name = (unsigned char *)buf;

	name[0] = '\0';

	//read the names in 3www6google3com format
	while (*reader != 0) {
		if (*reader >= 192) {
			offset = (*reader) * 256 + *(reader + 1) - 49152;	//49152 = 11000000 00000000 ;)
			reader = buffer + offset - 1;
			jumped = 1;	//we have jumped to another location so counting wont go up!
		} else {
			name[p++] = *reader;
		}

		reader = reader + 1;

		if (jumped == 0)
			*count = *count + 1;	//if we havent jumped to another location then we can count up
	}

	name[p] = '\0';		//string complete
	if (jumped == 1) {
		*count = *count + 1;	//number of steps we actually moved forward in the packet
	}
	//now convert 3www6google3com0 to www.google.com
	for (i = 0; i < (int)strlen((const char *)name); i++) {
		p = name[i];
		for (j = 0; j < (int)p; j++) {
			name[i] = name[i + 1];
			i = i + 1;
		}
		name[i] = '.';
	}

	name[i - 1] = '\0';	//remove the last dot

	return name;
}

void print_dns_info(const char *data, int len)
{
	//refer: https://www.binarytides.com/dns-query-code-in-c-with-winsock/
	struct DNS_HEADER *header = (struct DNS_HEADER *)data;
	unsigned char *qname = (unsigned char *)(header + 1);
	char dns_ips[10][256];
	char dns_name[10][1024];
	if (header->qr == 0) {
		printf("Query datalen %d, qcount:%d\n", len,
		       ntohs(header->q_count));
	} else {
		printf("Response datalen %d\n", len);
	}
	int q, i, j;
	char p;
	struct QUESTION *question;

	if (ntohs(header->q_count) > 10 || ntohs(header->ans_count) > 10)
		return;

	for (q = ntohs(header->q_count); q != 0; q--) {
		// now convert 3www6google3com0 to www.google.com
		for (i = 0; i < (int)strlen((const char *)qname); i++) {
			p = qname[i];
			for (j = 0; j < (int)p; j++) {
				dns_name[q][i] = qname[i + 1];
				i = i + 1;
			}
			dns_name[q][i] = '.';
		}
		dns_name[q][i - 1] = '\0';	//remove the last dot
		question = (struct QUESTION *)&qname[i + 1];
		printf("Name %s, QTYPE %s, QCLASS 0x%04x(%s)\n", dns_name[q],
		       question->qtype == 0x0100 ? "A (IPv4)" : "AAAA (IPv6)",
		       question->qclass,
		       question->qclass == 0x0100 ? "IN" : "unknown");
		qname = (unsigned char *)(question + 1);
	}

	if (header->qr == 1) {	// response
		struct RES_RECORD answers[20];	//the replies from the DNS server

		struct DNS_HEADER *dns;
		struct sockaddr_in a;
		char *buf = (char *)data;
		dns = (struct DNS_HEADER *)buf;
		int stop = 0;
		unsigned char *reader = qname;

		printf("\nThe response contains : ");
		printf("\n - %d Questions.", ntohs(dns->q_count));
		printf("\n - %d Answers.", ntohs(dns->ans_count));
		printf("\n - %d Authoritative Servers.",
		       ntohs(dns->auth_count));
		printf("\n - %d Additional records.\n\n",
		       ntohs(dns->add_count));

		//reading answers
		stop = 0;

		for (i = 0; i < ntohs(dns->ans_count); i++) {
			answers[i].name =
			    read_name((unsigned char *)reader,
				      (unsigned char *)buf, dns_name[i], &stop);
			reader = reader + stop;

			answers[i].resource = (struct R_DATA *)(reader);
			reader = reader + sizeof(struct R_DATA);

			if (ntohs(answers[i].resource->type) == 1)	//if its an ipv4 address
			{
				answers[i].rdata = (unsigned char *)
				    dns_ips[i];

				for (j = 0;
				     j < ntohs(answers[i].resource->data_len);
				     j++)
					answers[i].rdata[j] = reader[j];

				answers[i].rdata[ntohs
						 (answers[i].resource->
						  data_len)]
				    = '\0';

				reader =
				    reader +
				    ntohs(answers[i].resource->data_len);

			} else {
				answers[i].rdata =
				    read_name(reader, (unsigned char *)buf,
					      dns_ips[i], &stop);
				reader = reader + stop;
			}

		}

		printf("Answer :\n");
		for (i = 0; i < ntohs(dns->ans_count); i++) {
			printf("  - Name : %s ", answers[i].name);
			if (ntohs(answers[i].resource->type) == 1)	//IPv4 address
			{
				long *p;
				p = (long *)answers[i].rdata;
				a.sin_addr.s_addr = (*p);	//working without ntohl
				printf("has IPv4 address : %s",
				       inet_ntoa(a.sin_addr));
			}
			if (ntohs(answers[i].resource->type) == 5)	//Canonical name for an alias
			{
				printf("has alias name : %s", answers[i].rdata);
			}
			printf("\n");
		}
	}

	fflush(stdout);
}

// ------- print mysql --------
void print_mysql_info(const char *data, uint32_t len, uint8_t dir)
{
#define SOCK_DIR_SND_REQ	0
#define SOCK_DIR_SND_RES	1
#define SOCK_DIR_RCV_REQ	2
#define SOCK_DIR_RCV_RES	3
	/*
	 * MySQL Protocol
	 *    Packet Length: 33
	 *    Packet Number: 0
	 *    Request Command Query
	 *  Command: Query (3)
	 *  Statement: select user,host from mysql.user
	 */
	int i;
	for (i = 0; i < len; i++)
		printf("%c", data[i]);
	printf("\n");

	fflush(stdout);
}

// ------- print redis --------
void print_redis_info(const char *data, uint32_t len, uint8_t dir)
{
	int i;
	for (i = 0; i < len; i++)
		printf("%c", data[i]);
	printf("\n");

	fflush(stdout);
}

// ------- print dubbo --------
void print_dubbo_info(const char *data, uint32_t len, uint8_t dir)
{
	// head + body , head 16 bytes. skip head
	int i;
	for (i = 0; i < len - 16; i++)
		printf("%c", data[i + 16]);
	printf("\n");

	fflush(stdout);
}
