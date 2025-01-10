/*
 * Copyright (c) 2022 Yunshan Networks
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

#ifndef DF_USER_SOCKET_H
#define DF_USER_SOCKET_H
#include "config.h"

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

#define SYSCALL_FORK_TP_PATH "/sys/kernel/debug/tracing/events/syscalls/sys_exit_fork"
#define SYSCALL_CLONE_TP_PATH "/sys/kernel/debug/tracing/events/syscalls/sys_exit_clone"
#define FTRACE_SYSCALLS_PATH "/sys/kernel/debug/tracing/events/syscalls"
/*
 * The `__sys_recvmmsg` interface underwent a change in its parameter list starting
 * from Linux kernel version 5.0. If earlier kernel versions support the `fentry/fexit`
 * feature, attempting to load BPF programs on this interface may fail. Therefore, we
 * use this interface to check whether to load a version of the BPF bytecode binary
 * that supports `fentry/fexit`.
 */
#define TEST_KFUNC_NAME "__sys_recvmmsg"
#define TEST_KFUNC_PARAMS_NUM 6

// use for inference struct offset.
#define OFFSET_INFER_SERVER_ADDR "127.0.0.1"
#define OFFSET_INFER_SERVER_PORT 54583

#define CACHE_LINE_ROUNDUP(size) \
  (CACHE_LINE_SIZE * ((size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE))

enum linux_kernel_type {
	K_TYPE_COMM,
	K_TYPE_KYLIN,
	K_TYPE_VER_5_2_PLUS,
	K_TYPE_VER_3_10,
	K_TYPE_KFUNC,
	K_TYPE_RT,
};

enum probes_act_type {
	ACT_NONE,
	ACT_ATTACH,
	ACT_DETACH
};

struct socket_bpf_data {
	/* session info */
	uint32_t process_id;	// tgid in kernel struct task_struct
	uint32_t thread_id;	// pid in kernel struct task_struct, main thread iff pid==tgid
	uint64_t coroutine_id;	// CoroutineID, i.e., golang goroutine id
	uint8_t source;		// syscall,go_tls_uprobe,go_http2_uprobe
	uint8_t process_kname[TASK_COMM_LEN];	// comm in task_struct
	uint8_t container_id[CONTAINER_ID_SIZE]; // container id

	struct __tuple_t tuple;	// Socket五元组信息
	uint64_t socket_id;	// Socket的唯一标识，从启动时的时钟开始自增1
	uint16_t l7_protocal_hint;	// 应用数据（cap_data）的协议类型，枚举如下：1 SOCK_DATA_HTTP1, 2 SOCK_DATA_DNS, 3 ...
	// 存在一定误判性（例如标识为A协议但实际上是未知协议，或标识为多种协议），上层应用应继续深入判断
	uint8_t msg_type;	// 信息类型，值为MSG_UNKNOWN(0), MSG_REQUEST(1), MSG_RESPONSE(2)
	bool need_reconfirm;	// 是否需要上层再确认
	bool is_tls;

	/* trace info */
	uint64_t tcp_seq;	// 收发cap_data数据时TCP协议栈将会用到的TCP SEQ，可用于关联eBPF DATA与网络中的TCP Packet
	uint64_t syscall_trace_id_call;	// 应用数据的追踪ID，若应用为协程，L7代理、应用层负载均衡等类型时，可利用此值追踪一个请求或响应
	// 同一份应用数据（cap_data可能不同）接收、发送的两份cap_data会标记上相同标识

	/* data info */
	uint64_t timestamp;	// cap_data获取的时间戳
	uint8_t direction;	// 数据的收发方向，枚举如下: 1 SOCK_DIR_SND, 2 SOCK_DIR_RCV
	uint64_t syscall_len;	// 本次系统调用读、写数据的总长度
	uint32_t cap_len;	// 返回的cap_data长度
	uint64_t cap_seq;	// cap_data在Socket中的相对顺序号，从启动时的时钟开始自增1，用于数据乱序排序
	uint8_t socket_role;	// this message is created by: 0:unkonwn 1:client(connect) 2:server(accept)
	char *cap_data;		// 返回的应用数据
};

/*
 * eBPF统计
 *
 * @perf_pages_count:     perf buffer内存占用的页数量
 * @kern_lost:            perf buffer数据用户态程序来不急接收数据
 *                        造成的SockData丢失数量
 * @kern_socket_map_max:  socket追踪的hash表项最大值
 * @kern_socket_map_used: socket追踪的hash表项当前值
 * @kern_trace_map_max:  线程/协程追踪会话的hash表项最大值
 * @kern_trace_map_used: 线程/协程追踪会话的hash表项当前值
 *
 * 数据处理统计
 * 每一次系统调用向socket读/写数据，都会被eBPF获取，并把此次的
 * 数据传递给用户态接收程序
 * 约定: 把通过eBPF获取一次socket读/写的数据称作'SockData'，它包含
 * 描述头和数据两部分。
 * @worker_num：          处理数据的工作线程数量
 * @queue_capacity：      单队列容量
 * @mem_alloc_fail_count：内存申请（用于为burst-SockDatas申请一块内存）失败次数统计
 * @user_enqueue_count:   用户态程序收到内核传过来的入队列的SockData数量
 * @user_dequeue_count:   用户态程序处理的SockData数量
 * @user_enqueue_lost:    由于队列没有空闲空间使得入队列失败而造成的SockData丢失数量
 * @queue_burst_count:	  通过burst方式进行入队列和出队列，这里统计burst（16个SockData）的次数
 *
 * kerenl 适配状态
 * @is_adapt_success: 适配成功为1，否则为0
 * @tracer_state: 追踪器当前状态
 *
 * @boot_time_update_diff 这里用于记录相邻两次更新后，系统启动时间之间的差异（单位为纳秒）。
 * @probes_count How many probes now
 * @data_limit_max Maximum data length limit
 *
 * @period_push_conflict_count When the periodic push event detects that the
 *    buffer is being modified by another eBPF program, a conflict will occur.
 *    This is used to record the number of conflicts.
 * @period_push_max_delay The maximum latency time for periodic push events, in microseconds.
 * @period_push_avg_delay The average latency time for periodic push events, in microseconds.
 * @proc_exec_event_count The number of events for process execute.
 * @proc_exit_event_count The number of events for process exits.
 */
struct socket_trace_stats {

	/*
	 * eBPF统计
	 */
	uint16_t perf_pages_cnt;
	uint64_t kern_lost;
	uint32_t kern_socket_map_max;
	uint32_t kern_socket_map_used;
	uint32_t kern_trace_map_max;
	uint32_t kern_trace_map_used;
	uint32_t socket_map_max_reclaim;

	/*
	 * 数据处理统计
	 */
	uint16_t worker_num;
	uint32_t queue_capacity;
	uint64_t mem_alloc_fail_count;
	uint64_t user_enqueue_count;
	uint64_t user_dequeue_count;
	uint64_t user_enqueue_lost;
	uint64_t queue_burst_count;

	//工作线程数据处理的平均消耗时间, 单位：微妙
	//uint32_t process_cost_time;

	/*
	 * kerenl 适配状态
	 */
	bool is_adapt_success;
	uint8_t tracer_state;

	int64_t boot_time_update_diff;
	uint32_t probes_count;
	uint32_t data_limit_max;

	/*
	 * Period push events statistics.
	 */
	uint64_t period_push_conflict_count;
	uint64_t period_push_max_delay;
	uint64_t period_push_avg_delay;

	/*
	 * Process start and exit events.
	 */
	uint64_t proc_exec_event_count;
	uint64_t proc_exit_event_count;

	/*
	 * Captured packet statistics
	 */
	uint64_t rx_packets;
	uint64_t tx_packets;
	uint64_t rx_bytes;
	uint64_t tx_bytes;
	uint64_t dropped_packets;
	uint64_t kern_missed_packets;
	uint64_t invalid_packets;
};

struct bpf_offset_param_array {
	int count;
	bpf_offset_param_t offsets[0];
};

struct bpf_socktrace_params {
	uint8_t tracer_state;
	uint32_t kern_socket_map_max;
	uint32_t kern_socket_map_used;
	uint32_t kern_trace_map_max;
	uint32_t kern_trace_map_used;
	uint64_t proc_exec_event_count;
	uint64_t proc_exit_event_count;
	bool datadump_enable;
	int datadump_pid;
	uint8_t datadump_proto;
	char datadump_file_path[DATADUMP_FILE_PATH_SIZE];
	char datadump_comm[16];
	struct bpf_offset_param_array offset_array;
};

/*
 * This structure is used for registration of additional events.
 */
struct extra_event {
	struct list_head list;
	uint32_t type;
	void (*h)(void *);
};

static inline char *get_proto_name(uint16_t proto_id)
{
	switch (proto_id) {
	case PROTO_HTTP1:
		return "HTTP1";
	case PROTO_HTTP2:
		return "HTTP2";
	case PROTO_MYSQL:
		return "MySQL";
	case PROTO_DNS:
		return "DNS";
	case PROTO_REDIS:
		return "Redis";
	case PROTO_KAFKA:
		return "Kafka";
	case PROTO_MQTT:
		return "MQTT";
	case PROTO_AMQP:
		return "AMQP";
	case PROTO_OPENWIRE:
		return "OpenWire";
	case PROTO_ZMTP:
		return "ZMTP";
	case PROTO_ROCKETMQ:
		return "RocketMQ";
	case PROTO_NATS:
		return "NATS";
	case PROTO_PULSAR:
		return "Pulsar";
	case PROTO_DUBBO:
		return "Dubbo";
	case PROTO_SOFARPC:
		return "SofaRPC";
	case PROTO_SOME_IP:
		return "Some/IP";
	case PROTO_POSTGRESQL:
		return "PgSQL";
	case PROTO_ORACLE:
		return "Oracle";
	case PROTO_FASTCGI:
		return "FastCGI";
	case PROTO_BRPC:
		return "bRPC";
	case PROTO_TARS:
		return "Tars";
	case PROTO_MONGO:
		return "MongoDB";
	case PROTO_MEMCACHED:
		return "Memcached";
	case PROTO_TLS:
		return "TLS";
	case PROTO_DPDK_PKT:
		return "Pkt";
	case PROTO_CUSTOM:
		return "Custom";
	default:
		return "Unknown";
	}

	return "Unknown";
}

static inline const char *get_tracer_state_name(enum tracer_state s)
{
	switch (s) {
	case TRACER_INIT:
		return "TRACER_INIT";
	case TRACER_RUNNING:
		return "TRACER_RUNNING";
	case TRACER_STOP:
		return "TRACER_STOP";
	case TRACER_WAIT_START:
		return "TRACER_WAIT_START";
	case TRACER_START_ERR:
		return "TRACER_START_ERR";
	case TRACER_WAIT_STOP:
		return "TRACER_WAIT_STOP";
	case TRACER_STOP_ERR:
		return "TRACER_STOP_ERR";
	default:
		return "TRACER_UNKNOWN";
	}
}

#define PREFETCH_READ 0
#define PREFETCH_WRITE 1

/* *INDENT-OFF* */
#define _PREFETCH(n,size,type)				\
  if ((size) > (n)*CACHE_LINE_BYTES)			\
    __builtin_prefetch (_addr + (n)*CACHE_LINE_BYTES, 	\
            PREFETCH_##type,              		\
            /* locality */ 3);

#define PREFETCH(addr,size,type)		\
do {						\
  void * _addr = (addr);			\
	int __sz = (size);			\
  if (__sz > 2*CACHE_LINE_BYTES)		\
		__sz = 2*CACHE_LINE_BYTES;	\
  _PREFETCH (0, __sz, type);			\
  _PREFETCH (1, __sz, type);			\
} while (0)
/* *INDENT-ON* */

static inline void
prefetch_and_process_data(struct bpf_tracer *t, int nb_rx, void **datas_burst)
{
/* Configure how many socket_data ahead to prefetch, when reading socket_data */
#define PREFETCH_OFFSET   3
	int32_t j;
	struct socket_bpf_data *sd;
	struct mem_block_head *block_head;
	tracer_callback_t callback = (tracer_callback_t) t->process_fn;

	/* Prefetch first packets */
	for (j = 0; j < PREFETCH_OFFSET && j < nb_rx; j++)
		PREFETCH(datas_burst[j], 2 * CACHE_LINE_BYTES, READ);

	/*
	 * Prefetch and forward already prefetched
	 * packets.
	 */
	for (j = 0; j < nb_rx; j++) {
		if (j + PREFETCH_OFFSET < nb_rx)
			PREFETCH(datas_burst[j + PREFETCH_OFFSET],
				 2 * CACHE_LINE_BYTES, READ);
		sd = (struct socket_bpf_data *)datas_burst[j];
		block_head = (struct mem_block_head *)sd - 1;
		if (block_head->fn != NULL) {
			block_head->fn(sd);
		} else {
			int64_t boot_time = get_sysboot_time_ns();
			if (t->datadump)
				t->datadump((void *)sd, boot_time);
			/*
			 * Modify socket data time to real time,
			 * time precision is in nanosecond.
			 */
			sd->timestamp = sd->timestamp + boot_time;
			callback(NULL, sd);
		}

		if (block_head->is_last == 1)
			free(block_head->free_ptr);
	}
}

int set_data_limit_max(int limit_size);
int set_go_tracing_timeout(int timeout);
int set_io_event_collect_mode(uint32_t mode);
int set_io_event_minimal_duration(uint64_t duration);
struct socket_trace_stats socket_tracer_stats(void);
int running_socket_tracer(tracer_callback_t handle,
			  int thread_nr,
			  uint32_t perf_pages_cnt,
			  uint32_t queue_size,
			  uint32_t max_socket_entries,
			  uint32_t max_trace_entries,
			  uint32_t socket_map_max_reclaim);
int register_event_handle(uint32_t type, void (*fn)(void *));
int socket_tracer_stop(void);
int socket_tracer_start(void);
enum tracer_state get_socket_tracer_state(void);
int set_protocol_ports_bitmap(int proto_type, const char *ports);
int disable_syscall_trace_id(void);

/**
 * eBPF Probe Point Configuration
 *
 * Configure probe points. The types of probe points may include:
 * (1) kprobe/kretprobe
 * (2) tracepoint
 * During the configuration process, the kernel is automatically checked
 * to determine if it supports 'fentry/fexit'. If supported, this type
 * of probe point is preferred to improve performance. Otherwise,
 * 'kprobe/kretprobe' or 'tracepoint' types are used.
 *
 * @param tps Pointer to the structure that stores the configuration of
 * 	      all probe points.
 * @param type eBPF program type.
 * @param fn Name of the kernel probe interface.
 * @param tp_name Name of the tracepoint type probe point.
 * @param is_eixt Used to specify the position of the kernel probe
 * 		  interface. If probing at the exit of the kernel interface,
 * 		  it is set to true. Otherwise, it is set to false. This
 * 		  is not applicable for handling tracepoint type interfaces.
 */
void config_probe(struct tracer_probes_conf *tps, int type, const char *fn,
		  const char *tp_name, bool is_exit);
void uprobe_match_pid_handle(int feat, int pid, enum match_pids_act act);
#endif /* DF_USER_SOCKET_H */
