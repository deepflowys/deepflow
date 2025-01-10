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

#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <getopt.h>
#include "tracer.h"
#include "socket.h"

#define DF_BPF_NAME           "deepflow-ebpfctl"
#define DF_BPF_VERSION        "v1.0.0"
#define LINUX_VER_LEN         128
#define CMD_BUF_SZ	      256
#define TIMEOUT_DEF	      60

static char *match_str_def = ".*";
static char *comm_str_def = "";

typedef enum df_bpf_cmd_e {
	DF_BPF_CMD_ADD,
	DF_BPF_CMD_DEL,
	DF_BPF_CMD_SET,
	DF_BPF_CMD_ON,
	DF_BPF_CMD_OFF,
	DF_BPF_CMD_SHOW,
	DF_BPF_CMD_REPLACE,
	DF_BPF_CMD_FLUSH,
	DF_BPF_CMD_PRINT,
	DF_BPF_CMD_FIND,
	DF_BPF_CMD_HELP,
} df_bpf_cmd_t;

struct df_bpf_conf {
	int af;
	int verbose;
	int stats;
	int interval;
	int count;
	int timeout;
	char *match_str;
	int pid;
	int l7_proto;
	bool is_all_files;
	char *comm_str;
	bool color;
	bool only_stdout;
	char *obj;
	df_bpf_cmd_t cmd;
	int argc;
	char **argv;
};

struct df_bpf_obj {
	char *name;
	void *param;

	void (*help) (void);
	/* @conf is used to passing general config like af, verbose, ...
	 * we have obj.parse() to handle obj specific parameters. */
	int (*do_cmd) (struct df_bpf_obj * obj, df_bpf_cmd_t cmd,
		       struct df_bpf_conf * conf);
	/* the parser can be used to parse @conf into @param */
	int (*parse) (struct df_bpf_obj * obj, struct df_bpf_conf * conf);
	int (*check) (const struct df_bpf_obj * obj, df_bpf_cmd_t cmd);
};

static void tracer_help(void)
{
	fprintf(stderr, "Usage:\n" "    %s tracer show\n", DF_BPF_NAME);
}

static void socktrace_help(void)
{
	fprintf(stderr, "Usage:\n" "    %s tracer show\n", DF_BPF_NAME);
}

static void match_pids_help(void)
{
	fprintf(stderr, "Print match pids to log\n");
	fprintf(stderr, "Usage:\n" "    %s match_pids print\n", DF_BPF_NAME);
	fprintf(stderr, "For example:\n");
	fprintf(stderr, "    %s match_pids print\n", DF_BPF_NAME);
}

static void cpdbg_help(void)
{
	fprintf(stderr,
		"Continuous profiler debug: profiler data output to agent log\n");
	fprintf(stderr, "Usage:\n" "    %s cpdbg {on|off} [OPTIONS]\n",
		DF_BPF_NAME);
	fprintf(stderr, "Options:\n");
	fprintf(stderr,
		"    '-t, --timeout':      set profiler debug timout. Unit: second\n");
	fprintf(stderr, "For example:\n");
	fprintf(stderr, "    %s cpdbg on --timeout 60\n", DF_BPF_NAME);
	fprintf(stderr, "    %s cpdbg off\n", DF_BPF_NAME);
}

static void datadump_help(void)
{
	fprintf(stderr,
		"Usage:\n" "    %s datadump {on|off|ls|clear|find} [OPTIONS]\n",
		DF_BPF_NAME);
	fprintf(stderr,
		"    %s datadump set pid PID comm COMM proto PROTO_NUM\n",
		DF_BPF_NAME);
	fprintf(stderr, "PROTO_NUM:\n");
	fprintf(stderr, "    0:   PROTO_ALL\n");
	fprintf(stderr, "    1:   PROTO_OTHER\n");
	fprintf(stderr, "    20:  PROTO_HTTP1\n");
	fprintf(stderr, "    21:  PROTO_HTTP2\n");
	fprintf(stderr, "    40:  PROTO_DUBBO\n");
	fprintf(stderr, "    43:  PROTO_SOFARPC\n");
	fprintf(stderr, "    45:  PROTO_BRPC\n");
	fprintf(stderr, "    46:  PROTO_TARS\n");
	fprintf(stderr, "    47:  PROTO_SOME_IP\n");
	fprintf(stderr, "    60:  PROTO_MYSQL\n");
	fprintf(stderr, "    61:  PROTO_POSTGRESQL\n");
	fprintf(stderr, "    62:  PROTO_ORACLE\n");
	fprintf(stderr, "    80:  PROTO_REDIS\n");
	fprintf(stderr, "    81:  PROTO_MONGO\n");
	fprintf(stderr, "    82:  PROTO_MEMCACHED\n");
	fprintf(stderr, "    100: PROTO_KAFKA\n");
	fprintf(stderr, "    101: PROTO_MQTT\n");
	fprintf(stderr, "    102: PROTO_AMQP\n");
	fprintf(stderr, "    103: PROTO_OPENWIRE\n");
	fprintf(stderr, "    104: PROTO_NATS\n");
	fprintf(stderr, "    105: PROTO_PULSAR\n");
	fprintf(stderr, "    106: PROTO_ZMTP\n");
	fprintf(stderr, "    106: PROTO_ROCKETMQ\n");
	fprintf(stderr, "    120: PROTO_DNS\n");
	fprintf(stderr, "    121: PROTO_TLS\n");
	fprintf(stderr, "    127: PROTO_CUSTOM\n");
	fprintf(stderr, "    199: PROTO_DPDK_PKT\n");
	fprintf(stderr, "PID:\n");
	fprintf(stderr, "    0:   all process/thread\n");
	fprintf(stderr, "COMM:\n");
	fprintf(stderr,
		"    '':  The process name or thread name is not restricted.\n");
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "    '-O, --only-stdout':  dump to stdout only.\n");
	fprintf(stderr,
		"    '-t, --timeout':      set datadump timout. Unit: second\n");
	fprintf(stderr, "For example:\n");
	fprintf(stderr, "    %s datadump set pid 4567 comm curl proto 0\n",
		DF_BPF_NAME);
	fprintf(stderr, "    %s datadump set pid 4567 comm '' proto 20\n",
		DF_BPF_NAME);
	fprintf(stderr, "    %s datadump on --timeout 60\n", DF_BPF_NAME);
	fprintf(stderr, "    %s datadump on --only-stdout --timeout 60\n",
		DF_BPF_NAME);
	fprintf(stderr, "    %s datadump off\n", DF_BPF_NAME);
	fprintf(stderr, "    %s datadump ls\n", DF_BPF_NAME);
	fprintf(stderr, "    %s datadump clear\n", DF_BPF_NAME);
	fprintf(stderr, "    %s datadump find <Match string>\n", DF_BPF_NAME);
	fprintf(stderr,
		"    %s datadump set --pid 1234 --comm nginx --l7-proto 20\n",
		DF_BPF_NAME);
	fprintf(stderr, "    %s datadump find --match-str nginx\n",
		DF_BPF_NAME);
	fprintf(stderr, "    %s datadump find --match-str nginx --all-files\n",
		DF_BPF_NAME);
}

static int __exec_command(const char *cmd, const char *args)
{
	FILE *fp;
	int rc = 0;
	char cmd_buf[CMD_BUF_SZ * 2];
	snprintf(cmd_buf, sizeof(cmd_buf), "%s %s", cmd, args);
	fp = popen(cmd_buf, "r");
	if (NULL == fp) {
		fprintf(stderr, "%s '%s' execute error,[%s]\n",
			__func__, cmd_buf, strerror(errno));
		return -1;
	}

	char buffer[8192];
	while (fgets(buffer, sizeof(buffer), fp) != NULL) {
		fprintf(stdout, "%s", buffer);
		fflush(stdout);
	}

	rc = pclose(fp);
	if (-1 == rc) {
		fprintf(stderr, "pclose error, '%s' error:%s\n",
			cmd_buf, strerror(errno));
	} else {
		if (WIFEXITED(rc)) {
			return WEXITSTATUS(rc);
		} else if (WIFSIGNALED(rc)) {
			fprintf(stdout,
				"'%s' abnormal termination,signal number %d\n",
				cmd_buf, WTERMSIG(rc));
		} else if (WIFSTOPPED(rc)) {
			fprintf(stdout,
				"'%s' process stopped, signal number %d\n",
				cmd_buf, WSTOPSIG(rc));
		}
	}

	return -1;
}

static void tracer_dump(struct bpf_tracer_param *param)
{
	struct bpf_tracer_param *btp = param;
	struct rx_queue_info *rx_q;
	printf("%-18s %s\n", "Tracer", btp->name);	//, sizeof(btp->name), "%s", t->name);
	printf("%-18s %s\n", "Bpf buffer", btp->bpf_load_name);
	printf("%-18s %d\n", "Workers", btp->dispatch_workers_nr);
	printf("%-18s %d\n", "Perf-Pages-Count", btp->perf_pg_cnt);
	printf("%-18s %" PRIu64 "\n", "Events Lost", btp->lost);
	printf("%-18s %d\n", "Probes Count", btp->probes_count);
	printf("%-18s %s\n", "State", get_tracer_state_name(btp->state));
	printf("%-18s %d\n", "Adapt", btp->adapt_success);
	printf("%-18s %d\n", "data_limit_max", btp->data_limit_max);
	printf("\n-------------------- Queue ---------------------------\n");
	int j;
	uint64_t enqueue_nr, enqueue_lost, burst_count, heap_get_failed,
	    dequeue_nr;
	enqueue_nr = enqueue_lost = burst_count = heap_get_failed = dequeue_nr =
	    0;
	for (j = 0; j < btp->dispatch_workers_nr; j++) {
		rx_q = &btp->rx_queues[j];
		printf
		    ("worker %d for queue, de %" PRIu64 " en %" PRIu64 " lost %"
		     PRIu64 " alloc failed %" PRIu64 " burst %" PRIu64
		     " queue size %u cap %u\n", j, rx_q->dequeue_nr,
		     rx_q->enqueue_nr, rx_q->enqueue_lost,
		     rx_q->heap_get_failed, rx_q->burst_count, rx_q->queue_size,
		     rx_q->ring_capacity);
		heap_get_failed += rx_q->heap_get_failed;
		dequeue_nr += rx_q->dequeue_nr;
		enqueue_nr += rx_q->enqueue_nr;
		enqueue_lost += rx_q->enqueue_lost;
		burst_count += rx_q->burst_count;
	}

	printf
	    ("\nSUM dequeue %" PRIu64 " enqueue %" PRIu64 " lost %" PRIu64
	     " alloc failed %" PRIu64 " burst count %" PRIu64 "\n", dequeue_nr,
	     enqueue_nr, enqueue_lost, heap_get_failed, burst_count);

	fflush(stdout);

	printf("\n-------------------- Protocol ------------------------\n");
	for (j = 0; j < PROTO_NUM; j++) {
		if (btp->proto_stats[j] > 0) {
			printf("- %-10s(%d) %" PRIu64 "\n",
			       get_proto_name((uint16_t) j),
			       j, btp->proto_stats[j]);
		}
	}

	printf("\n");
}

static void offset_dump(int cpu, bpf_offset_param_t *param)
{
	printf("----------------------------------\n");
	printf("cpu: \t%d\n", cpu);
	printf("ready: \t%d\n", param->ready);
	printf("task__files_offset: \t0x%x\n", param->task__files_offset);
	printf("sock__flags_offset: \t0x%x\n", param->sock__flags_offset);
	printf("tcp_sock__copied_seq_offset: \t0x%x\n",
	       param->tcp_sock__copied_seq_offset);
	printf("tcp_sock__write_seq_offset: \t0x%x\n\n",
	       param->tcp_sock__write_seq_offset);
}

static inline int msg_send(int clt_fd,
			   const struct tracer_sock_msg *hdr,
			   const char *data, int data_len)
{
	int len, res;

	if (!hdr) {
		fprintf(stderr, "[%s] empty socket msg header\n", __func__);
		return -1;
	}

	len = sizeof(struct tracer_sock_msg);
	res = sendn(clt_fd, hdr, len, MSG_NOSIGNAL);
	if (len != res) {
		fprintf(stderr,
			"[%s] socket msg header send error -- %d/%d sent\n",
			__func__, res, len);
		return -1;
	}

	if (data && data_len) {
		res = sendn(clt_fd, data, data_len, MSG_NOSIGNAL);
		if (data_len != res) {
			fprintf(stderr,
				"[%s] socket msg body send error -- %d/%d sent\n",
				__func__, res, data_len);
			return -1;
		}
	}

	return 0;
}

static inline int msg_recv(int clt_fd, struct tracer_sock_msg_reply *reply_hdr,
			   void **out, size_t * out_len)
{
	void *msg = NULL;
	int len, res;

	if (!reply_hdr) {
		fprintf(stderr, "[%s] empty reply msg pointer\n", __func__);
		return -1;
	}

	if (out)
		*out = NULL;
	if (out_len)
		*out_len = 0;

	len = sizeof(struct tracer_sock_msg_reply);
	memset(reply_hdr, 0, len);
	res = readn(clt_fd, reply_hdr, len);
	if (len != res) {
		fprintf(stderr,
			"[%s] socket msg header recv error -- %d/%d recieved\n",
			__func__, res, len);
		return -1;
	}

	if (reply_hdr->errcode) {
		fprintf(stderr,
			"[%s] errcode set in socket msg#%d header: %s(%d)\n",
			__func__, reply_hdr->id, reply_hdr->errstr,
			reply_hdr->errcode);
		return reply_hdr->errcode;
	}

	if (reply_hdr->len > 0) {
		msg = malloc(reply_hdr->len);
		if (NULL == msg) {
			fprintf(stderr, "[%s] no memory\n", __func__);
			return -1;
		}

		res = readn(clt_fd, msg, reply_hdr->len);
		if (res != reply_hdr->len) {
			fprintf(stderr,
				"[%s] socket msg body recv error -- %d/%d recieved\n",
				__func__, res, (int)reply_hdr->len);
			free(msg);
			return -1;
		}
	}

	if (SOCKOPT_VERSION != reply_hdr->version) {
		fprintf(stderr, "[%s] socket msg version not match\n",
			__func__);
		if (reply_hdr->len > 0)
			free(msg);
		return -1;
	}

	if (out && out_len) {
		*out = msg;
		*out_len = reply_hdr->len;
	} else if (reply_hdr->len > 0) {
		free(msg);
		if (out)
			*out = NULL;
		if (out_len)
			*out_len = 0;
	}

	return 0;
}

static int sockopt_send(enum sockopt_type type, sockoptid_t cmd, const void *in,
			size_t in_len)
{
	struct tracer_sock_msg *msg;
	struct sockaddr_un clt_addr;
	int clt_fd;
	int res;
	size_t msg_len;

	memset(&clt_addr, 0, sizeof(struct sockaddr_un));
	clt_addr.sun_family = AF_UNIX;
	strncpy(clt_addr.sun_path, UNIX_DOMAIN_DEF,
		sizeof(clt_addr.sun_path) - 1);

	msg_len = sizeof(struct tracer_sock_msg);
	msg = malloc(msg_len);
	if (NULL == msg) {
		fprintf(stderr, "[%s] no memory\n", __func__);
		return -1;
	}

	clt_fd = socket(PF_UNIX, SOCK_STREAM, 0);
	res = connect(clt_fd, (struct sockaddr *)&clt_addr, sizeof(clt_addr));
	if (-1 == res) {
		fprintf(stderr, "[%s] socket msg connection error: %s\n",
			__func__, strerror(errno));
		free(msg);
		return -1;
	}

	memset(msg, 0, msg_len);
	msg->version = SOCKOPT_VERSION;
	msg->id = cmd;
	msg->type = type;
	msg->len = in_len;
	res = msg_send(clt_fd, msg, in, in_len);

	free(msg);
	msg = NULL;

	if (res) {
		close(clt_fd);
		return -1;
	}

	return clt_fd;
}

int df_bpf_getsockopt(sockoptid_t cmd, const void *in, size_t in_len,
		      void **out, size_t * out_len)
{
	struct tracer_sock_msg_reply reply_hdr;
	int clt_fd, res;

	if (NULL == out || NULL == out_len) {
		fprintf(stderr, "[%s] no pointer for info return\n", __func__);
		return -1;
	}
	*out = NULL;
	*out_len = 0;

	if ((clt_fd = sockopt_send(SOCKOPT_GET, cmd, in, in_len)) <= 0) {
		return -1;
	}
	res = msg_recv(clt_fd, &reply_hdr, out, out_len);
	if (res) {
		close(clt_fd);
		return -1;
	}

	if (reply_hdr.errcode) {
		fprintf(stderr, "[%s] Server error: %s\n", __func__,
			reply_hdr.errstr);
		close(clt_fd);
		return reply_hdr.errcode;
	}

	close(clt_fd);
	return 0;
}

int df_bpf_setsockopt(sockoptid_t cmd, const void *in, size_t in_len)
{
	struct tracer_sock_msg_reply reply_hdr;
	int clt_fd, res;
	if ((clt_fd = sockopt_send(SOCKOPT_SET, cmd, in, in_len)) <= 0) {
		return -1;
	}

	res = msg_recv(clt_fd, &reply_hdr, NULL, NULL);
	if (res) {
		close(clt_fd);
		return -1;
	}

	if (reply_hdr.errcode) {
		fprintf(stderr, "[%s] Server error: %s\n", __func__,
			reply_hdr.errstr);
		close(clt_fd);
		return reply_hdr.errcode;
	}

	close(clt_fd);
	return 0;
}

static inline void df_bpf_sockopt_msg_free(void *msg)
{
	free(msg);
	msg = NULL;
}

static inline void get_kernel_version(char *buf)
{
	int major, minor, rev, num;
	fetch_kernel_version(&major, &minor, &rev, &num);
	snprintf(buf, LINUX_VER_LEN, "Linux %d.%d.%d-%d\n",
		 major, minor, rev, num);
}

static int socktrace_do_cmd(struct df_bpf_obj *obj, df_bpf_cmd_t cmd,
			    struct df_bpf_conf *conf)
{
	struct bpf_socktrace_params *sk_trace_params = NULL;
	struct bpf_offset_param_array *array = NULL;
	size_t size, i;
	int err;
	char linux_ver_str[LINUX_VER_LEN];
	memset((void *)linux_ver_str, 0, sizeof(linux_ver_str));
	get_kernel_version(linux_ver_str);
	printf("\n\033[0;33;mLinux Version: %s\033[0m\n", linux_ver_str);

	switch (conf->cmd) {
	case DF_BPF_CMD_SHOW:
		err =
		    df_bpf_getsockopt(SOCKOPT_GET_SOCKTRACE_SHOW, NULL,
				      0, (void **)&sk_trace_params, &size);
		if (err != 0)
			return err;

		if (sk_trace_params == NULL)
			return ETR_INVAL;

		array = &sk_trace_params->offset_array;

		if (size < sizeof(*sk_trace_params)
		    || size != sizeof(*sk_trace_params) +
		    array->count * sizeof(bpf_offset_param_t)) {
			fprintf(stderr, "corrupted response.\n");
			df_bpf_sockopt_msg_free(sk_trace_params);
			return ETR_INVAL;
		}

		printf("kern_socket_map_max:\t%u\n",
		       sk_trace_params->kern_socket_map_max);
		printf("kern_socket_map_used:\t%u\n",
		       sk_trace_params->kern_socket_map_used);
		printf("kern_trace_map_max:\t%u\n",
		       sk_trace_params->kern_trace_map_max);
		printf("kern_trace_map_used:\t%u\n",
		       sk_trace_params->kern_trace_map_used);
		printf("proc_exec_event_count:\t%lu\n",
		       sk_trace_params->proc_exec_event_count);
		printf("proc_exit_event_count:\t%lu\n",
		       sk_trace_params->proc_exit_event_count);
		printf("datadump_enable:\t%s\n",
		       sk_trace_params->datadump_enable ? "true" : "false");
		printf("datadump_pid:\t%d\n", sk_trace_params->datadump_pid);
		printf("datadump_proto:\t%d\n",
		       sk_trace_params->datadump_proto);
		printf("datadump_comm:\t%s\n", sk_trace_params->datadump_comm);
		printf("datadump_file_path:\t%s\n\n",
		       sk_trace_params->datadump_file_path);

		printf("tracer_state:\t%s\n\n",
		       get_tracer_state_name(sk_trace_params->tracer_state));

		for (i = 0; i < array->count; i++) {
			if (array->offsets[i].ready != 1)
				offset_dump(i, &array->offsets[i]);
		}

		printf("CPUs: %d\n", array->count);

		offset_dump(i - 1, &array->offsets[i - 1]);

		df_bpf_sockopt_msg_free(sk_trace_params);
		return ETR_OK;
	default:
		return ETR_NOTSUPP;
	}
}

static int match_pids_do_cmd(struct df_bpf_obj *obj, df_bpf_cmd_t cmd,
			     struct df_bpf_conf *conf)
{
	switch (conf->cmd) {
	case DF_BPF_CMD_PRINT:
		if (df_bpf_setsockopt(SOCKOPT_PRINT_MATCH_PIDS, NULL, 0) == 0) {
			printf("Success.\n");
		} else {
			printf("Failed.\n");
		}
		break;

	default:
		return ETR_NOTSUPP;
	}

	return ETR_OK;
}

static int datadump_do_cmd(struct df_bpf_obj *obj, df_bpf_cmd_t cmd,
			   struct df_bpf_conf *conf)
{
	switch (conf->cmd) {
	case DF_BPF_CMD_ON:
	case DF_BPF_CMD_OFF:
	case DF_BPF_CMD_SET:
		{
			struct datadump_msg msg;
			memset(msg.comm, 0, sizeof(msg.comm));
			msg.is_params = false;
			msg.only_stdout = conf->only_stdout;
			msg.timeout = conf->timeout;
			if (conf->cmd == DF_BPF_CMD_ON
			    || conf->cmd == DF_BPF_CMD_OFF) {
				if (conf->argc != 0) {
					obj->help();
					return ETR_NOTSUPP;
				}
				if (conf->cmd == DF_BPF_CMD_ON) {
					msg.enable = true;
					if (msg.timeout == 0) {
						printf
						    ("Miss --timeout setting.\n");
						return ETR_NOTSUPP;
					}
					printf("Set datadump on, timeout %ds ",
					       msg.timeout);
				} else {
					msg.enable = false;
					printf("Set datadump off ");
				}
			} else {
				if (conf->argc == 0) {
					msg.is_params = true;
					msg.pid = conf->pid;
					msg.proto = conf->l7_proto;
					memset(msg.comm, 0, sizeof(msg.comm));
					memcpy(msg.comm, conf->comm_str,
					       sizeof(msg.comm) - 1);
					fprintf(stdout,
						" == pid %d l7-proto %d comm %s\n",
						msg.pid, msg.proto, msg.comm);
					goto conf_finish;
				}

				if (conf->argc != 6
				    || strcmp(conf->argv[0], "pid")
				    || strcmp(conf->argv[2], "comm")
				    || strcmp(conf->argv[4], "proto")) {
					obj->help();
					return ETR_NOTSUPP;
				}

				if (conf->argv[1][0] == '0'
				    && conf->argv[1][1] == '\0') {
					msg.pid = 0;
				} else {
					msg.pid = atoi(conf->argv[1]);
					if (msg.pid <= 0) {
						obj->help();
						return ETR_NOTSUPP;
					}
				}

				if (conf->argv[5][0] == '0'
				    && conf->argv[5][1] == '\0') {
					msg.proto = 0;
				} else {
					msg.proto = atoi(conf->argv[5]);
					if (msg.proto <= 0) {
						obj->help();
						return ETR_NOTSUPP;
					}
				}

				msg.is_params = true;
				memset(msg.comm, 0, sizeof(msg.comm));
				if (strlen(conf->argv[3]) > 0) {
					memcpy(msg.comm, conf->argv[3],
					       sizeof(msg.comm) - 1);
				}
			      conf_finish:
				printf("Set pid %d comm %s proto %d ", msg.pid,
				       msg.comm, msg.proto);
			}

			if (df_bpf_setsockopt(SOCKOPT_SET_DATADUMP_ADD, &msg,
					      sizeof(msg)) == 0) {
				printf("Success.\n");
			} else {
				printf("Failed.\n");
			}

			break;
		}
	case DF_BPF_CMD_SHOW:
		if (conf->argc != 0)
			fprintf(stdout, "Invalid params.\n");
		else
			__exec_command("ls -sh /var/log/datadump-*.log", "");
		break;
	case DF_BPF_CMD_FLUSH:
		if (conf->argc != 0)
			fprintf(stdout, "Invalid params.\n");
		else
			__exec_command("rm -rf /var/log/datadump-*.log", "");
		break;
	case DF_BPF_CMD_FIND:
		{
			char *match_str;
			fprintf(stdout, "conf->argc == %d conf->match_str %s\n",
				conf->argc, conf->match_str);
			if (conf->argc == 0 && conf->match_str != NULL) {
				match_str = conf->match_str;
				goto process_find;

			}

			if (conf->argc != 1) {
				fprintf(stdout, "Invalid params.\n");
				return ETR_NOTSUPP;
			}

			match_str = conf->argv[0];

		      process_find:
			{
				char cmdbuf[CMD_BUF_SZ];
				snprintf(cmdbuf, sizeof(cmdbuf),
					 "grep -n -A 1 \"%s\" %s",
					 match_str,
					 conf->
					 is_all_files ?
					 "/var/log/datadump-*.log" :
					 "$(ls -t /var/log/datadump-*.log | head -n 1)");
				printf("%s\n", cmdbuf);
				__exec_command(cmdbuf, "");
			}
		}
		break;
	default:
		return ETR_NOTSUPP;
	}
	return ETR_OK;
}

static int cpdbg_do_cmd(struct df_bpf_obj *obj, df_bpf_cmd_t cmd,
			struct df_bpf_conf *conf)
{
	switch (conf->cmd) {
	case DF_BPF_CMD_ON:
	case DF_BPF_CMD_OFF:
		{
			struct cpdbg_msg msg;
			msg.timeout = conf->timeout;
			if (conf->cmd == DF_BPF_CMD_ON
			    || conf->cmd == DF_BPF_CMD_OFF) {
				if (conf->argc != 0) {
					obj->help();
					return ETR_NOTSUPP;
				}
				if (conf->cmd == DF_BPF_CMD_ON) {
					msg.enable = true;
					if (msg.timeout == 0) {
						printf
						    ("Miss --timeout setting.\n");
						return ETR_NOTSUPP;
					}
					printf("Set cpdbg on, timeout %ds ",
					       msg.timeout);
				} else {
					msg.enable = false;
					printf("Set cpdbg off ");
				}
			} else {
				obj->help();
				return ETR_NOTSUPP;
			}

			if (df_bpf_setsockopt(SOCKOPT_SET_CPDBG_ADD, &msg,
					      sizeof(msg)) == 0) {
				printf("Success.\n");
			} else {
				printf("Failed.\n");
			}

			break;
		}
	default:
		return ETR_NOTSUPP;
	}
	return ETR_OK;
}

static int tracer_do_cmd(struct df_bpf_obj *obj, df_bpf_cmd_t cmd,
			 struct df_bpf_conf *conf)
{
	struct bpf_tracer_param_array *array;
	size_t size, i;
	int err;

	switch (conf->cmd) {
	case DF_BPF_CMD_SHOW:
		err =
		    df_bpf_getsockopt(SOCKOPT_GET_TRACER_SHOW, NULL,
				      0, (void **)&array, &size);
		if (err != 0)
			return err;

		if (size < sizeof(*array)
		    || size != sizeof(*array) +
		    array->count * sizeof(struct bpf_tracer_param)) {
			fprintf(stderr, "corrupted response.\n");
			df_bpf_sockopt_msg_free(array);
			return ETR_INVAL;
		}

		for (i = 0; i < array->count; i++)
			tracer_dump(&array->tracers[i]);

		df_bpf_sockopt_msg_free(array);
		return ETR_OK;
	default:
		return ETR_NOTSUPP;
	}
}

struct df_bpf_obj tr_obj = {
	.name = "tracer",
	.help = tracer_help,
	.do_cmd = tracer_do_cmd,
};

struct df_bpf_obj socktrace_obj = {
	.name = "socktrace",
	.help = socktrace_help,
	.do_cmd = socktrace_do_cmd,
};

struct df_bpf_obj datadump_obj = {
	.name = "datadump",
	.help = datadump_help,
	.do_cmd = datadump_do_cmd,
};

struct df_bpf_obj cpdbg_obj = {
	.name = "cpdbg",
	.help = cpdbg_help,
	.do_cmd = cpdbg_do_cmd,
};

struct df_bpf_obj match_pids_obj = {
	.name = "match_pids",
	.help = match_pids_help,
	.do_cmd = match_pids_do_cmd,
};

static void usage(void)
{
	fprintf(stderr,
		"Usage:\n"
		"    " DF_BPF_NAME " [OPTIONS] OBJECT { COMMAND | help }\n"
		"Parameters:\n"
		"    OBJECT  := { tracer socktrace datadump cpdbg match_pids}\n"
		"    COMMAND := { show list set print}\n"
		"Options:\n"
		"    -v, --verbose\n"
		"    -h, --help\n" "    -V, --version\n" "    -C, --color\n");
}

static struct df_bpf_obj *df_bpf_obj_get(const char *name)
{
	if (strcmp(name, "tracer") == 0) {
		return &tr_obj;
	} else if (strcmp(name, "socktrace") == 0) {
		return &socktrace_obj;
	} else if (strcmp(name, "datadump") == 0) {
		return &datadump_obj;
	} else if (strcmp(name, "cpdbg") == 0) {
		return &cpdbg_obj;
	} else if (strcmp(name, "match_pids") == 0) {
		return &match_pids_obj;
	}

	return NULL;
}

static int parse_args(int argc, char *argv[], struct df_bpf_conf *conf)
{
	int opt;
	struct df_bpf_obj *obj;
	struct option opts[] = {
		{"verbose", no_argument, NULL, 'v'},
		{"help", no_argument, NULL, 'h'},
		{"version", no_argument, NULL, 'V'},
		{"color", no_argument, NULL, 'C'},
		{"only-stdout", no_argument, NULL, 'O'},
		{"all-files", no_argument, NULL, 'A'},
		{"timeout", required_argument, NULL, 't'},
		{"match-str", required_argument, NULL, 'm'},
		{"pid", required_argument, NULL, 'p'},
		{"comm", required_argument, NULL, 'c'},
		{"l7-proto", required_argument, NULL, 'l'},
		{NULL, 0, NULL, 0},
	};

	memset(conf, 0, sizeof(*conf));
	conf->af = AF_UNSPEC;
	conf->only_stdout = false;
	conf->timeout = TIMEOUT_DEF;
	conf->pid = 0;
	conf->l7_proto = 0;
	conf->match_str = match_str_def;
	conf->comm_str = comm_str_def;
	conf->is_all_files = false;

	if (argc <= 1) {
		usage();
		exit(0);
	}

	while ((opt =
		getopt_long(argc, argv, "vhVCOAt:m:p:c:l:", opts,
			    NULL)) != -1) {
		switch (opt) {
		case 'v':
			conf->verbose = 1;
			break;
		case 'h':
			usage();
			exit(0);
		case 'V':
			printf(DF_BPF_NAME "-" DF_BPF_VERSION "\n");
			exit(0);
		case 'C':
			conf->color = true;
			break;
		case 'O':
			conf->only_stdout = true;
			break;
		case 'A':
			conf->is_all_files = true;
			break;
		case 't':
			conf->timeout = atoi(optarg);
			if (conf->timeout <= 0) {
				fprintf(stderr, "Invalid option: --timeout\n");
				return -1;
			}
			break;
		case 'p':
			conf->pid = atoi(optarg);
			if (conf->pid < 0) {
				fprintf(stderr,
					"Invalid option: --pid, need >= 0\n");
				return -1;
			}
			break;
		case 'l':
			conf->l7_proto = atoi(optarg);
			if (conf->l7_proto < 0) {
				fprintf(stderr,
					"Invalid option: --l7-proto, need >= 0\n");
				return -1;
			}
			break;
		case 'c':{
				if (optarg == NULL) {
					fprintf(stderr,
						"Invalid option: --comm\n");
					return -1;
				}
				int len = strlen(optarg) + 1;
				conf->comm_str = malloc(len);
				if (conf->comm_str == NULL) {
					fprintf(stderr, "malloc failed\n");
					return -1;
				}
				memcpy((void *)conf->comm_str, (void *)optarg,
				       len);
				conf->comm_str[len] = '\0';
			}
			break;
		case 'm':
			{
				if (optarg == NULL) {
					fprintf(stderr,
						"Invalid option: --match-str");
					return -1;
				}

				int len = strlen(optarg) + 1;
				conf->match_str = malloc(len);
				if (conf->match_str == NULL) {
					fprintf(stderr, "malloc failed\n");
					return -1;
				}
				memcpy((void *)conf->match_str, (void *)optarg,
				       len);
				conf->match_str[len] = '\0';
			}
			break;
		case '?':
		default:
			fprintf(stderr, "aa Invalid option: %s\n",
				argv[optind]);
			return -1;
		}
	}

	/* at least two args for: obj and cmd */
	if (optind >= argc) {
		usage();
		exit(1);
	}

	argc -= optind;
	argv += optind;

	conf->obj = argv[0];
	if (argc < 2) {
		obj = df_bpf_obj_get(conf->obj);
		if (obj && obj->help)
			obj->help();
		else
			usage();
		exit(1);
	}

	if (strcmp(argv[1], "show") == 0 || strcmp(argv[1], "list") == 0) {
		conf->cmd = DF_BPF_CMD_SHOW;
		goto show_exit;
	} else if (strcmp(argv[1], "set") == 0) {
		conf->cmd = DF_BPF_CMD_SET;
		goto show_exit;
	} else if (strcmp(argv[1], "on") == 0) {
		conf->cmd = DF_BPF_CMD_ON;
		goto show_exit;
	} else if (strcmp(argv[1], "off") == 0) {
		conf->cmd = DF_BPF_CMD_OFF;
		goto show_exit;
	} else if (strcmp(argv[1], "print") == 0) {
		conf->cmd = DF_BPF_CMD_PRINT;
    goto show_exit;
	} else if (strcmp(argv[1], "ls") == 0) {
		conf->cmd = DF_BPF_CMD_SHOW;
		goto show_exit;
	} else if (strcmp(argv[1], "clear") == 0) {
		conf->cmd = DF_BPF_CMD_FLUSH;
		goto show_exit;
	} else if (strcmp(argv[1], "find") == 0) {
		conf->cmd = DF_BPF_CMD_FIND;
		goto show_exit;
	} else if (strcmp(argv[1], "help") == 0) {
		conf->cmd = DF_BPF_CMD_HELP;
		goto show_exit;
	}
#if 0
	if (argc < 3) {
		usage();
		exit(1);
	}

	if (strcmp(argv[1], "set") == 0 && strcmp(argv[2], "delay"))
		conf->cmd = DF_BPF_CMD_SETDELAY;
	else {
		usage();
		exit(1);
	}

	conf->argc = argc - 3;
	conf->argv = argv + 3;
	return 0;
#endif
show_exit:
	conf->argc = argc - 2;
	conf->argv = argv + 2;
	return 0;
}

int main(int argc, char *argv[])
{
	char *prog;
	struct df_bpf_conf conf;
	struct df_bpf_obj *obj;
	int err;

	if ((prog = strchr(argv[0], '/')) != NULL)
		*prog++ = '\0';
	else
		prog = argv[0];

	if (parse_args(argc, argv, &conf) != 0)
		exit(1);

	if ((obj = df_bpf_obj_get(conf.obj)) == NULL) {
		fprintf(stderr, "%s: invalid object, use `-h' for help.\n",
			prog);
		exit(1);
	}

	if (conf.cmd == DF_BPF_CMD_HELP) {
		if (obj->help) {
			obj->help();
			return ETR_OK;
		}
	}

	if ((err = obj->do_cmd(obj, conf.cmd, &conf)) != ETR_OK) {
		fprintf(stderr, "%s: %s\n", prog, trace_strerror(err));
		exit(1);
	}

	exit(0);
}
