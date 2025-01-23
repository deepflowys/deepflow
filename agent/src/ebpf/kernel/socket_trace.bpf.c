/*
 * This code runs using bpf in the Linux kernel.
 * Copyright 2022- The Yunshan Networks Authors.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *
 * SPDX-License-Identifier: GPL-2.0
 */

#include <arpa/inet.h>
#include "config.h"
#include "include/socket_trace.h"
#include "include/task_struct_utils.h"

#define OFFSET_READY		1
#define OFFSET_NO_READY    	0

#define NS_PER_US		1000ULL
#define NS_PER_SEC		1000000000ULL
#define PUSH_PERIOD_TIME        10000000ULL // Push period time, 10 milliseconds.

#define PROTO_INFER_CACHE_SIZE  80

#define SUBMIT_OK		(0)
#define SUBMIT_INVALID		(-1)
#define SUBMIT_ABORT		(-2)

#define __user

/* *INDENT-OFF* */
/***********************************************************
 * map definitions
 ***********************************************************/
/*
 * 向用户态传递数据的专用map
 */
MAP_PERF_EVENT(socket_data, int, __u32, MAX_CPU, FEATURE_FLAG_SOCKET_TRACER)

/*
 * Why use two Tail Calls jmp tables ?
 *
 * struct bpf_array { ...  enum bpf_prog_type owner_prog_type}
 * 'ownership' of prog_array is claimed by the first program that
 * is going to use this map or by the first program which FD is stored
 * in the map to make sure that all callers and callees have the same
 * prog_type and JITed flag.
 *
 * Tail Calls jmp table
 * We divide the data processing and data output into two parts, and each has a -
 * different eBPF program for processing.
 * The purpose of this is to prevent the problem of the number of instructions exceeding max limit.
 *
 * 'progs_jmp_kp_map' for kprobe/uprobe (`A -> B`, both A and B are [k/u]probe program)
 * 'progs_jmp_tp_map' for tracepoint (`A -> B`, both A and B are tracepoint program)
 *
 */
MAP_PROG_ARRAY(progs_jmp_kp_map, __u32, __u32, PROG_KP_NUM, FEATURE_FLAG_SOCKET_TRACER)
MAP_PROG_ARRAY(progs_jmp_tp_map, __u32, __u32, PROG_TP_NUM, FEATURE_FLAG_SOCKET_TRACER)

/*
 * 因为ebpf栈只有512字节无法存放http数据，这里使用map做为buffer。
 */
MAP_PERARRAY(data_buf, __u32, struct __socket_data_buffer, 1, FEATURE_FLAG_SOCKET_TRACER)

/*
 * For protocol infer buffer
 */
MAP_PERARRAY(ctx_info, __u32, struct ctx_info_s, 1, FEATURE_FLAG_SOCKET_TRACER)

/*
 * 结构体成员偏移
 */
MAP_PERARRAY(members_offset, __u32, struct member_fields_offset, 1, FEATURE_FLAG_SOCKET_TRACER)

/*
 * 记录追踪各种ID值(确保唯一性, per CPU 没有使用锁）
 * 生成方法：
 *    1、先初始化一个基值（基值 = [CPU IDX: 8bit] + [ sys_boot_time ]）
 *    2、在基值的基础上递增
 * CPU IDX:          8bit      [0 - 255]个CPU。
 * sys_boot_time:    56bit     使用1970年1月1日00:00:00开始到现在纳秒时间/100
 *
 * 按照每秒钟处理 10,000,000 Requests (这是一个很大值，实际达不到)这样的一个速率，
 * 可以存储176年(如果从2022年开始)的数据而UID不会出现重复。
 * ((2^56 - 1) - sys_boot_time)/10/1000/1000/60/60/24/365 = 176 years
 */
MAP_PERARRAY(tracer_ctx_map, __u32, struct tracer_ctx_s, 1, FEATURE_FLAG_SOCKET_TRACER)

/*
 * 对各类map进行统计
 */
MAP_ARRAY(trace_stats_map, __u32, struct trace_stats, 1, FEATURE_FLAG_SOCKET_TRACER)

// key: protocol id, value: is protocol enabled, size: PROTO_NUM
MAP_ARRAY(protocol_filter, int, int, PROTO_NUM, FEATURE_FLAG_SOCKET_TRACER)

/**
 * @brief Record which protocols allow data segmentation
 * reassembly processing.
 *
 * key: protocol id, value: is protocol allowed?, size: PROTO_NUM
 */
MAP_ARRAY(allow_reasm_protos_map, int, bool, PROTO_NUM, FEATURE_FLAG_SOCKET_TRACER)

// 0: allow bitmap; 1: bypass bitmap
MAP_ARRAY(kprobe_port_bitmap, __u32, struct kprobe_port_bitmap, 2, FEATURE_FLAG_SOCKET_TRACER)

/*
 * l7-protocol-ports
 * Configuring application layer protocol ports, when performing protocol
 * inference, inference is only targeted at specified ports of Layer 7
 * protocols.
 */
MAP_ARRAY(proto_ports_bitmap, __u32, ports_bitmap_t, PROTO_NUM, FEATURE_FLAG_SOCKET_TRACER)

// write() syscall's input argument.
// Key is {tgid, pid}.
BPF_HASH(active_write_args_map, __u64, struct data_args_t, MAP_MAX_ENTRIES_DEF, FEATURE_FLAG_SOCKET_TRACER)

// read() syscall's input argument.
// Key is {tgid, pid}.
BPF_HASH(active_read_args_map, __u64, struct data_args_t, MAP_MAX_ENTRIES_DEF, FEATURE_FLAG_SOCKET_TRACER)

// socket_info_map, 这是个hash表，用于记录socket信息，
// Key is {pid + fd}. value is struct socket_info_s
BPF_HASH(socket_info_map, __u64, struct socket_info_s, MAP_MAX_ENTRIES_DEF, FEATURE_FLAG_SOCKET_TRACER)

// socket_info lifecycle is inconsistent with socket. If the role information
// is saved to the socket_info_map, it will affect the generation of syscall
// trace id. Create an independent map to save role information
// Key is {pid + fd}. value is role type
BPF_HASH(socket_role_map, __u64, __u32, MAP_MAX_ENTRIES_DEF, FEATURE_FLAG_SOCKET_TRACER);

// Key is struct trace_key_t. value is trace_info_t
BPF_HASH(trace_map, struct trace_key_t, struct trace_info_t, MAP_MAX_ENTRIES_DEF, FEATURE_FLAG_SOCKET_TRACER)

// Stores the identity used to fit the kernel, key: 0, vlaue:{tgid, pid}
MAP_ARRAY(adapt_kern_uid_map, __u32, __u64, 1, FEATURE_FLAG_SOCKET_TRACER)

#if defined(LINUX_VER_KFUNC) || defined(LINUX_VER_5_2_PLUS)
/*
 * Fast matching cache, used to speed up protocol inference.
 * Due to the limitation of the number of eBPF instruction in kernel, this feature
 * is suitable for Linux5.2+
 * key : The high 16 bits of the process-ID/thread-ID
 * value : struct proto_infer_cache_t
 * The process-ID/thread-ID range [0, 5242880], if the process value exceeds the
 * maximum value range, fast cache matching becomes invalid.
 */
MAP_ARRAY(proto_infer_cache_map, __u32, struct proto_infer_cache_t, PROTO_INFER_CACHE_SIZE, FEATURE_FLAG_SOCKET_TRACER)
#endif
/* *INDENT-ON* */

static __inline bool is_protocol_enabled(int protocol)
{
	int *enabled = protocol_filter__lookup(&protocol);
	return (enabled) ? (*enabled) : (0);
}

static __inline bool is_proto_reasm_enabled(int protocol)
{
	bool *enabled = allow_reasm_protos_map__lookup(&protocol);
	return (enabled) ? (*enabled) : false;
}

static __inline void delete_socket_info(__u64 conn_key,
					struct socket_info_s *socket_info_ptr)
{
	if (socket_info_ptr == NULL)
		return;

	__u32 k0 = 0;
	struct trace_stats *trace_stats = trace_stats_map__lookup(&k0);
	if (trace_stats == NULL)
		return;

	if (!socket_info_map__delete(&conn_key)) {
		__sync_fetch_and_add(&trace_stats->socket_map_count, -1);
	}

	socket_role_map__delete(&conn_key);
}

static __inline bool is_socket_info_valid(struct socket_info_s *sk_info)
{
	return (sk_info != NULL && sk_info->uid != 0);
}

/* *INDENT-OFF* */
static __u32 __inline get_tcp_write_seq_from_fd(int fd, void **sk,
						struct socket_info_s *socket_info_ptr)
{
	void *sock;
#ifndef LINUX_VER_KFUNC
	__u32 k0 = 0;
	struct member_fields_offset *offset =
	    members_offset__lookup(&k0);
	if (!offset)
		return 0;
	sock = get_socket_from_fd(fd, offset);
#else
	if (is_socket_info_valid(socket_info_ptr))
		sock = socket_info_ptr->sk;
	else
		sock = get_socket_from_fd(fd, NULL);
	if (sk)
		*sk = sock;
#endif
	if (sock == NULL)
		return 0;

	__u32 tcp_seq = 0;
	int seq_off;
#ifndef LINUX_VER_KFUNC
	seq_off = offset->tcp_sock__write_seq_offset;
#else
	seq_off = (int)((uintptr_t)
	    __builtin_preserve_access_index(&((struct tcp_sock *)0)->write_seq));
#endif
	bpf_probe_read_kernel(&tcp_seq, sizeof(tcp_seq), sock + seq_off);
	return tcp_seq;
}

static __u32 __inline get_tcp_read_seq_from_fd(int fd, void **sk,
					       struct socket_info_s *socket_info_ptr)
{
	void *sock;
#ifndef LINUX_VER_KFUNC
	__u32 k0 = 0;
	struct member_fields_offset *offset =
	    members_offset__lookup(&k0);
	if (!offset)
		return 0;
	sock = get_socket_from_fd(fd, offset);
#else
	if (is_socket_info_valid(socket_info_ptr))
		sock = socket_info_ptr->sk;
	else
		sock = get_socket_from_fd(fd, NULL);
	if (sk)
		*sk = sock;
#endif
	if (sock == NULL)
		return 0;

	__u32 tcp_seq = 0;
	int seq_off;
#ifndef LINUX_VER_KFUNC
	seq_off = offset->tcp_sock__copied_seq_offset;
#else
	seq_off = (int)((uintptr_t)
	    __builtin_preserve_access_index(&((struct tcp_sock *)0)->copied_seq));
#endif
	bpf_probe_read_kernel(&tcp_seq, sizeof(tcp_seq), sock + seq_off);
	return tcp_seq;
}

static bool __inline check_socket_valid(struct socket_info_s *socket_info_ptr, int fd)
{
#ifdef LINUX_VER_KFUNC
	if (is_socket_info_valid(socket_info_ptr)) {
		int sk_off = (int)((uintptr_t) __builtin_preserve_access_index(&((struct sock *)0)->sk_socket));
		void *check_socket;
		bpf_probe_read_kernel(&check_socket, sizeof(check_socket),
				      socket_info_ptr->sk + sk_off);
		if (unlikely(check_socket != socket_info_ptr->socket)) {
			__u32 tgid = (__u32) (bpf_get_current_pid_tgid() >> 32);
			__u64 conn_key = gen_conn_key_id((__u64) tgid,
							 (__u64) fd);
			delete_socket_info(conn_key, socket_info_ptr);
			return false;
		}
		return true;
	}
#endif
	return false;
}

static __u32 __inline get_tcp_write_seq(int fd, void **sk, struct socket_info_s
					*socket_info_ptr)
{
	if (check_socket_valid(socket_info_ptr, fd))
		return get_tcp_write_seq_from_fd(fd, sk, socket_info_ptr);
	else
		return get_tcp_write_seq_from_fd(fd, sk, NULL);
}

static __u32 __inline get_tcp_read_seq(int fd, void **sk, struct socket_info_s
				       *socket_info_ptr)
{
	if (check_socket_valid(socket_info_ptr, fd))
		return get_tcp_read_seq_from_fd(fd, sk, socket_info_ptr);
	else
		return get_tcp_read_seq_from_fd(fd, sk, NULL);
}

/* *INDENT-ON* */

/*
 * B : buffer
 * O : buffer offset, e.g.: infer_buf->len
 * I : &args->iov[i]
 * L_T : total_size
 * L_C : bytes_copy
 * F : first_iov
 * F_S : first_iov_size
 */
/* *INDENT-OFF* */
#define COPY_IOV(B, O, I, L_T, L_C, F, F_S) do {				\
	struct iovec iov_cpy;							\
	bpf_probe_read_user(&iov_cpy, sizeof(struct iovec), (I));		\
	if (iov_cpy.iov_base == NULL || iov_cpy.iov_len == 0) continue;		\
	if (!(F)) {								\
		F = iov_cpy.iov_base;						\
		F_S = iov_cpy.iov_len;						\
	}									\
	const int bytes_remaining = (L_T) - (L_C);				\
	__u32 iov_size =							\
		iov_cpy.iov_len <						\
			bytes_remaining ? iov_cpy.iov_len : bytes_remaining;	\
	__u32 len = (O) + (L_C);						\
	struct copy_data_s *cp = (struct copy_data_s *)((B) + len);		\
	if (len > (sizeof((B)) - sizeof(*cp)))					\
		break;								\
	if (iov_size >= sizeof(cp->data)) {					\
		bpf_probe_read_user(cp->data, sizeof(cp->data), iov_cpy.iov_base);\
		iov_size = sizeof(cp->data);					\
	} else {								\
		iov_size = iov_size & (sizeof(cp->data) - 1);			\
		bpf_probe_read_user(cp->data, iov_size + 1, iov_cpy.iov_base);	\
	}									\
	L_C = (L_C) + iov_size;							\
} while (0)
/* *INDENT-ON* */

static __inline int iovecs_copy(struct __socket_data *v,
				struct __socket_data_buffer *v_buff,
				const struct data_args_t *args,
				size_t real_len, __u32 send_len)
{
/*
 * The number of loops in eBPF is limited; tests have shown that the
 * Linux 4.14 kernel supports a maximum of 27 iterations.
 */
#define LOOP_LIMIT 27

	struct copy_data_s {
		char data[sizeof(v->data)];
	};

	int bytes_copy = 0;
	__u32 total_size = 0;

	if (real_len >= sizeof(v->data))
		total_size = sizeof(v->data);
	else
		total_size = send_len;

	char *first_iov = NULL;
	__u32 first_iov_size = 0;

#pragma unroll
	for (unsigned int i = 0;
	     i < LOOP_LIMIT && i < args->iovlen && bytes_copy < total_size;
	     ++i) {
		COPY_IOV(v_buff->data,
			 v_buff->len + offsetof(typeof(struct __socket_data),
						data), &args->iov[i],
			 total_size, bytes_copy, first_iov, first_iov_size);
	}

	return bytes_copy;
}

static __inline int infer_iovecs_copy(struct infer_data_s *infer_buf,
				      const struct data_args_t *args,
				      size_t syscall_len,
				      __u32 copy_len,
				      char **f_iov, __u32 * f_iov_len)
{
#define INFER_COPY_SZ	 32
#define INFER_LOOP_LIMIT 4
	struct copy_data_s {
		char data[INFER_COPY_SZ];
	};

	int bytes_copy = 0;
	__u32 total_size = 0;
	infer_buf->len = 0;

	if (syscall_len >= sizeof(infer_buf->data))
		total_size = sizeof(infer_buf->data);
	else
		total_size = copy_len;

	if (total_size > syscall_len)
		total_size = syscall_len;

	char *first_iov = NULL;
	__u32 first_iov_size = 0;

#pragma unroll
	for (unsigned int i = 0;
	     i < INFER_LOOP_LIMIT && i < args->iovlen
	     && bytes_copy < total_size; i++) {
		COPY_IOV(infer_buf->data, infer_buf->len, &args->iov[i],
			 total_size, bytes_copy, first_iov, first_iov_size);
	}

	*f_iov = first_iov;
	*f_iov_len = first_iov_size;

	return bytes_copy;
}

static __inline struct member_fields_offset *retrieve_ready_kern_offset(void)
{
	__u32 k0 = 0;
	struct member_fields_offset *offset = members_offset__lookup(&k0);
	if (!offset)
		return NULL;

	if (unlikely(!offset->ready))
		return NULL;

	return offset;
}

#include "uprobe_base.bpf.c"
#include "include/protocol_inference.h"
#define CONN_PERSIST_TIME_MAX_NS   100000000000ULL

static __inline struct trace_key_t get_trace_key(__u64 timeout,
						 bool is_socket_io)
{
	__u64 pid_tgid = bpf_get_current_pid_tgid();
	__u64 goid = 0;

	if (timeout) {
		goid = get_rw_goid(timeout * NS_PER_SEC, is_socket_io);
	}

	struct trace_key_t key = {};

	key.tgid = (__u32) (pid_tgid >> 32);

	if (goid) {
		key.goid = goid;
	} else {
		key.pid = (__u32) pid_tgid;
	}

	return key;
}

static __inline unsigned int __retry_get_sock_flags(void *sk, int offset)
{
	unsigned int flags = 0;
	bpf_probe_read_kernel(&flags, sizeof(flags), (void *)sk + offset);
	return flags;
}

static __inline void infer_sock_flags(void *sk,
				      struct member_fields_offset *offset)
{
	struct sock_flags_t {
		unsigned int sk_padding:1;
		unsigned int sk_kern_sock:1;
		unsigned int sk_no_check_tx:1;
		unsigned int sk_no_check_rx:1;
		unsigned int sk_userlocks:4;
		unsigned int sk_protocol:8;
		unsigned int sk_type:16;
	};

	// TAG: STRUCT_SOCK_FLAGS_OFFSET
	// Member '__sk_flags_offset' the offset in struct sock
	// 0x220 for 4.19.90-23.15.v2101.ky10.x86_64
	// 0x238 for 5.10.0-60.18.0.50.h322_1.hce2.aarch64
#ifdef LINUX_VER_KYLIN
	int sock_flags_offset_array[] =
	    { 0x1f0, 0x1f8, 0x200, 0x208, 0x210, 0x218, 0x220 };
#elif defined LINUX_VER_3_10_0
	// 0x150 for 3.10.0-957, 3.10.0-1160
	int sock_flags_offset_array[] = { 0x150 };
#elif defined LINUX_VER_5_2_PLUS
	// 0x230 for OEL7.9 Linux 5.4.17
	// 0x220 for TENCENT64.site 5.4.119-19-0008 aarch64 
	int sock_flags_offset_array[] =
	    { 0x1f0, 0x1f8, 0x200, 0x208, 0x210, 0x218, 0x220, 0x230, 0x238 };
#else
	int sock_flags_offset_array[] =
	    { 0x1f0, 0x1f8, 0x200, 0x208, 0x210, 0x218 };
#endif

	unsigned int flags = 0;
	struct sock_flags_t *sk_flags = (struct sock_flags_t *)&flags;
	int i;
#pragma unroll
	for (i = 0; i < ARRAY_SIZE(sock_flags_offset_array); i++) {
		flags = __retry_get_sock_flags(sk, sock_flags_offset_array[i]);
		/*
		 * struct sock *sk_alloc(struct net *net, int family, gfp_t priority,
		 *                    struct proto *prot, int kern)
		 *
		 *       -》sk = sk_prot_alloc(prot, priority | __GFP_ZERO, family);
		 * 在申请sock时，使用了__GFP_ZERO，为了尽量确保准确性增加一个sk_padding为0判断。
		 */
		if ((sk_flags->sk_type == SOCK_DGRAM
		     || sk_flags->sk_type == SOCK_STREAM)
		    && sk_flags->sk_kern_sock == 0 && sk_flags->sk_padding == 0) {
			offset->sock__flags_offset = sock_flags_offset_array[i];
			break;
		}
	}
}

static __inline void get_sock_flags(void *sk,
				    struct member_fields_offset *offset,
				    struct conn_info_s *conn_info)
{
	struct sock_flags_t {
		unsigned int sk_padding:1;
		unsigned int sk_kern_sock:1;
		unsigned int sk_no_check_tx:1;
		unsigned int sk_no_check_rx:1;
		unsigned int sk_userlocks:4;
		unsigned int sk_protocol:8;
		unsigned int sk_type:16;
	};

	unsigned int flags = 0;
	struct sock_flags_t *sk_flags = (struct sock_flags_t *)&flags;
	bpf_probe_read_kernel(&flags, sizeof(flags), (void *)sk +
			      offset->sock__flags_offset);

	conn_info->sk_type = sk_flags->sk_type;
}

/*
 * IPv4 connections can be handled with the v6 API by using the
 * v4-mapped-on-v6 address type;thus a program needs to
 * support only this API type to support both protocols.This
 * is handled transparently by the address handling functions
 * in the C library . IPv4 and IPv6 share the local port space.
 * When you get an IPv4 connection or packet to a IPv6 socket,
 * its source address will be mapped to v6 and it will be mapped
 * to v6. The address notation for IPv6 is a group of 8 4 -digit
 * hexadecimal numbers, separated with a ':'."::" stands for a
 * string of 0 bits.
 * Special addresses are
 *   ::1 for loopback and ::FFFF:<IPv4 address> for IPv4-mapped-on-IPv6.
 */
/*
 * Confirm whether you want to obtain IP through socket IPv4 address
 *
 * @s sock address
 * @f skc_family
 */
#define ipv4_mapped_on_ipv6_confirm(s, f, o)				\
do {									\
	char __addr[16];						\
	bpf_probe_read_kernel(__addr, 16, 				\
		(s) + o->struct_sock_ip6saddr_offset);			\
	__u32 __feature = *(__u32 *)&__addr[8];				\
	if (__feature == 0xffff0000)					\
		f = PF_INET;						\
} while(0)

static __inline int is_tcp_udp_data(void *sk,
				    struct member_fields_offset *offset,
				    struct conn_info_s *conn_info)
{
	struct skc_flags_t {
		unsigned char skc_reuse:4;
		unsigned char skc_reuseport:1;
		unsigned char skc_ipv6only:1;
		unsigned char skc_net_refcnt:1;
	};

	struct skc_flags_t skc_flags;
	bpf_probe_read_kernel(&skc_flags, sizeof(skc_flags),
			      sk + offset->struct_sock_common_ipv6only_offset);
	conn_info->skc_ipv6only = skc_flags.skc_ipv6only;
	bpf_probe_read_kernel(&conn_info->skc_family,
			      sizeof(conn_info->skc_family),
			      sk + offset->struct_sock_family_offset);
	/*
	 * Without thinking about PF_UNIX.
	 */
	switch (conn_info->skc_family) {
	case PF_INET:
		break;
	case PF_INET6:
		if (conn_info->skc_ipv6only == 0) {
			ipv4_mapped_on_ipv6_confirm(sk, conn_info->skc_family,
						    offset);
		}
		break;
	default:
		return SOCK_CHECK_TYPE_ERROR;
	}

	get_sock_flags(sk, offset, conn_info);

	if (conn_info->sk_type == SOCK_DGRAM) {
		conn_info->tuple.l4_protocol = IPPROTO_UDP;
		return SOCK_CHECK_TYPE_UDP;
	}

	if (conn_info->sk_type != SOCK_STREAM) {
		return SOCK_CHECK_TYPE_ERROR;
	}

	bpf_probe_read_kernel(&conn_info->skc_state,
			      sizeof(conn_info->skc_state),
			      (void *)sk +
			      offset->struct_sock_skc_state_offset);

	/*
	 * If the connection has not been established yet, and it is not in the
	 * ESTABLISHED or CLOSE_WAIT state, exit.
	 */
	if ((1 << conn_info->skc_state) & ~(TCPF_ESTABLISHED | TCPF_CLOSE_WAIT)) {
		return SOCK_CHECK_TYPE_ERROR;
	}

	conn_info->tuple.l4_protocol = IPPROTO_TCP;
	return SOCK_CHECK_TYPE_TCP_ES;
}

static __inline void init_conn_info(__u32 tgid, __u32 fd,
				    struct conn_info_s *conn_info, void *sk,
				    const enum traffic_direction direction,
				    ssize_t bytes_count,
				    struct member_fields_offset *offset)
{
	conn_info->correlation_id = -1;	// Currently used for Kafka and OpenWire protocol inference
	conn_info->fd = fd;
	conn_info->sk = sk;
	__u64 conn_key = gen_conn_key_id((__u64) tgid, (__u64) conn_info->fd);
	conn_info->socket_info_ptr = socket_info_map__lookup(&conn_key);
	if (is_socket_info_valid(conn_info->socket_info_ptr)) {
		conn_info->no_trace = conn_info->socket_info_ptr->no_trace;
	}
	__be16 inet_dport;
	__u16 inet_sport;
	bpf_probe_read_kernel(&inet_dport, sizeof(inet_dport),
			      sk + offset->struct_sock_dport_offset);
	bpf_probe_read_kernel(&inet_sport, sizeof(inet_sport),
			      sk + offset->struct_sock_sport_offset);
	conn_info->tuple.dport = __bpf_ntohs(inet_dport);
	conn_info->tuple.num = inet_sport;
}

/* *INDENT-OFF* */
#if !defined(LINUX_VER_KFUNC)
static __inline bool get_socket_info(struct __socket_data *v, void *sk,
				     struct conn_info_s *conn_info)
{
	if (v == NULL || sk == NULL)
		return false;

	unsigned int k0 = 0;
	struct member_fields_offset *offset = members_offset__lookup(&k0);
	if (!offset)
		return false;
	/*
	 * Without thinking about PF_UNIX.
	 */
	switch (conn_info->skc_family) {
	case PF_INET:
		bpf_probe_read_kernel(v->tuple.rcv_saddr, 4,
				      sk + offset->struct_sock_saddr_offset);
		bpf_probe_read_kernel(v->tuple.daddr, 4,
				      sk + offset->struct_sock_daddr_offset);
		v->tuple.addr_len = 4;
		break;
	case PF_INET6:
		if (sk + offset->struct_sock_ip6saddr_offset >= 0) {
			bpf_probe_read_kernel(v->tuple.rcv_saddr, 16,
					      sk +
					      offset->struct_sock_ip6saddr_offset);
		}
		if (sk + offset->struct_sock_ip6daddr_offset >= 0) {
			bpf_probe_read_kernel(v->tuple.daddr, 16,
					      sk +
					      offset->struct_sock_ip6daddr_offset);
		}
		v->tuple.addr_len = 16;
		break;
	default:
		return false;
	}

	return true;
}
#else
static __inline bool get_socket_info(struct __tuple_t *tuple, void *sk,
				     struct conn_info_s *conn_info)
{
	if (sk == NULL)
		return false;

	int saddr_off, daddr_off, ip6saddr_off, ip6daddr_off;
	/*
	 * Without thinking about PF_UNIX.
	 */
	switch (conn_info->skc_family) {
	case PF_INET:
		saddr_off = (int)((uintptr_t)
		    __builtin_preserve_access_index(&((struct sock_common *)0)->skc_rcv_saddr));
		daddr_off = (int)((uintptr_t)
		    __builtin_preserve_access_index(&((struct sock_common *)0)->skc_daddr));
		bpf_probe_read_kernel(tuple->rcv_saddr, 4, sk + saddr_off);
		bpf_probe_read_kernel(tuple->daddr, 4, sk + daddr_off);
		tuple->addr_len = 4;
		break;
	case PF_INET6:
		ip6saddr_off = (int)((uintptr_t)
		    __builtin_preserve_access_index(&((struct sock_common *)0)->skc_v6_rcv_saddr));
		ip6daddr_off = (int)((uintptr_t)
		    __builtin_preserve_access_index(&((struct sock_common *)0)->skc_v6_daddr));
		bpf_probe_read_kernel(tuple->rcv_saddr, 16, sk + ip6saddr_off);
		bpf_probe_read_kernel(tuple->daddr, 16, sk + ip6daddr_off);
		tuple->addr_len = 16;
		break;
	default:
		return false;
	}

	return true;
}
#endif
/* *INDENT-ON* */

#ifdef PROBE_CONN_SUBMIT
static __inline void connect_submit(struct pt_regs *ctx, struct conn_info_s *v,
				    int act)
{
	switch (act) {
	case CONN_ADD:
		v->type = SOCK_ADD_EVENT;
		break;
	case CONN_DEL:
		v->type = SOCK_INFO_EVENT;
		break;
	default:
		return;
	}

	bpf_perf_event_output(ctx, &NAME(socket_data),
			      BPF_F_CURRENT_CPU, v, 128);
}
#endif

static __inline int
infer_l7_class_1(struct ctx_info_s *ctx,
		 struct conn_info_s *conn_info,
		 enum traffic_direction direction,
		 const struct data_args_t *args,
		 size_t count, __u8 sk_type,
		 const struct process_data_extra *extra)
{
	if (conn_info == NULL) {
		return INFER_TERMINATE;
	}

	struct protocol_message_t inferred_protocol =
	    infer_protocol_1(ctx, args, count, conn_info, sk_type, extra);
	if (inferred_protocol.protocol == PROTO_UNKNOWN &&
	    inferred_protocol.type == MSG_UNKNOWN) {
		conn_info->protocol = PROTO_UNKNOWN;
		return INFER_CONTINUE;
	}

	conn_info->protocol = inferred_protocol.protocol;
	conn_info->message_type = inferred_protocol.type;

	return INFER_FINISH;
}

static __inline int infer_l7_class_2(struct tail_calls_context *ctx,
				     struct conn_info_s *conn_info)
{
	struct infer_data_s *infer_data;
	infer_data = (struct infer_data_s *)ctx->private_data;
	struct protocol_message_t inferred_protocol =
	    infer_protocol_2(infer_data->data, conn_info->count, conn_info);
	if (inferred_protocol.protocol == PROTO_UNKNOWN &&
	    inferred_protocol.type == MSG_UNKNOWN) {
		conn_info->protocol = PROTO_UNKNOWN;
		return INFER_TERMINATE;
	}

	conn_info->protocol = inferred_protocol.protocol;
	conn_info->message_type = inferred_protocol.type;

	return INFER_FINISH;
}

static __inline __u32 retry_get_write_seq(void *sk,
					  int offset, int snd_nxt_offset)
{
	/*
	 * Judgments based:
	 * write_seq ==  snd_nxt && snd_nxt != 0 && write_seq != 0
	 */
	__u32 snd_nxt, write_seq;

	bpf_probe_read_kernel(&write_seq, sizeof(write_seq),
			      (void *)sk + offset);
	bpf_probe_read_kernel(&snd_nxt, sizeof(snd_nxt),
			      (void *)sk + snd_nxt_offset);

	if (snd_nxt == write_seq && snd_nxt != 0 && write_seq != 0) {
		return write_seq;
	} else
		return 0;

	return 0;
}

static __inline __u32 retry_get_copied_seq(void *sk, int offset)
{
	/*
	 * Judgments based:
	 * copied_seq + 1 == rcv_wup
	 * tcp_header_len 在[20, 60]区间
	 * rcv_wup == rcv_nxt
	 * rcv_wup != 0 && rcv_nxt != 0 && copied_seq != 0
	 *
	 * struct tcp_sock {
	 *     ...
	 *     u16      tcp_header_len;     -28
	 *     ...
	 *     u64      bytes_received;     -20
	 *     ...
	 *     u32      rcv_nxt;            -4
	 *     u32      copied_seq;         0
	 *     u32      rcv_wup;            +4
	 *     u32      snd_nxt;            +8
	 *     ...
	 * }
	 *
	 * But linux 3.10.0 :
	 * struct tcp_sock {
	 *     ...
	 *     u16      tcp_header_len;     -24
	 *     ...
	 *     u64      bytes_received;     -16
	 *     ...
	 *     u32      rcv_nxt;            -4
	 *     u32      copied_seq;         0
	 *     u32      rcv_wup;            +4
	 *     u32      snd_nxt;            +8
	 *     ...
	 * }
	 */
	__u32 rcv_nxt, rcv_wup, copied_seq;
	__u16 tcp_header_len;

	bpf_probe_read_kernel(&copied_seq, sizeof(copied_seq),
			      (void *)sk + offset);
	bpf_probe_read_kernel(&rcv_nxt, sizeof(rcv_nxt),
			      (void *)sk + offset - 4);
	bpf_probe_read_kernel(&rcv_wup, sizeof(rcv_wup),
			      (void *)sk + offset + 4);
#ifdef LINUX_VER_3_10_0
	bpf_probe_read_kernel(&tcp_header_len, sizeof(tcp_header_len),
			      (void *)sk + offset - 24);
#else
	bpf_probe_read_kernel(&tcp_header_len, sizeof(tcp_header_len),
			      (void *)sk + offset - 28);
#endif
	if (!(tcp_header_len >= 20 && tcp_header_len <= 60 && copied_seq != 0))
		return 0;

	if ((copied_seq == rcv_nxt && rcv_wup == rcv_nxt)) {
		return copied_seq;
	}

	return 0;
}

static __inline void infer_tcp_seq_offset(void *sk,
					  struct member_fields_offset *offset)
{
/* *INDENT-OFF* */
	// TAG: STRUCT_TCP_SOCK_COPIED_SEQ_OFFSET
	// 成员 copied_seq 在 struct tcp_sock 中的偏移量
	// 0x644 for EulerOS 4.18.0-147
	// 0x65c for 4.19.90-23.15.v2101.ky10.x86_64
	// 0x654 for 5.10.0-60.18.0.50.h322_1.hce2.aarch64
#ifdef LINUX_VER_KYLIN
	int copied_seq_offsets[] = {
		0x514, 0x524, 0x52c, 0x534, 0x53c,
		0x544, 0x54c, 0x554, 0x55c, 0x564,
		0x56c, 0x574, 0x57c, 0x584, 0x58c,
		0x594, 0x59c, 0x5dc, 0x644, 0x65c
	};
#elif defined LINUX_VER_3_10_0
	// 0x560 for 3.10.0-957, 3.10.0-1160
	int copied_seq_offsets[] = { 0x560 };
#elif defined LINUX_VER_5_2_PLUS
	// 0x63c for OEL7.9 Linux 5.4.17
	// 0x5f4 for TENCENT64.site 5.4.119-19-0008 aarch64
	int copied_seq_offsets[] = {
		0x514, 0x51c, 0x524, 0x52c, 0x534,
		0x53c, 0x544, 0x54c, 0x554, 0x55c,
		0x564, 0x56c, 0x574, 0x57c, 0x584,
		0x58c, 0x594, 0x59c, 0x5dc, 0x644,
		0x654, 0x63c, 0x5f4
	};
#else
	// 0x65c for 4.18.0-372.9.1.15.po1.x86_64
	// 0x664 for 4.19.90-2107.6.0.0100.oe1.bclinux
	// 0x5cc for 4.19.91-21.al7.x86_64
	// 0x5dc for 4.19.91-23.al7.x86_64, 4.19.91-24.1.al7.x86_64, 4.19.91-25.6.al7.x86_64,
	//           4.19.91-26.6.al7.x86_64, 4.19.91-26.al7.x86_64, 4.19.91-27.1.al7.x86_64
	// 0x654 for 4.19.90-2107.6.0.0192.8.oe1.bclinux.x86_64
	// 0x69c for 4.19.0-91.82.65.uelc20.x86_64
	// 0x694 for 4.19.0-91.77.112.uelc20.x86_64, 4.19.0-91.82.132.uelc20.x86_64
	int copied_seq_offsets[] = {
		0x514, 0x51c, 0x524, 0x52c, 0x534,
		0x53c, 0x544, 0x54c, 0x554, 0x55c,
		0x564, 0x56c, 0x574, 0x654, 0x5dc,
		0x5cc, 0x644, 0x65c, 0x664, 0x69c,
		0x694
	};
#endif

	// TAG: STRUCT_TCP_SOCK_WRITE_SEQ_OFFSET
	// 成员 write_seq 在 struct tcp_sock 中的偏移量
	// 0x7b4 for EulerOS 4.18.0-147
	// 0x7cc for 4.19.90-23.15.v2101.ky10.x86_64
	// The 0x684 feature code interferes with the inference of write_seq in the Kylin system. It must be removed.
	// 0x7d4 for 5.10.0-60.18.0.50.h322_1.hce2.aarch64
#ifdef LINUX_VER_KYLIN
	int write_seq_offsets[] = {
		0x66c, 0x674, 0x68c, 0x694, 0x69c, 0x6a4,
		0x6ac, 0x6b4, 0x6bc, 0x6c4, 0x6cc, 0x6d4,
		0x6dc, 0x6ec, 0x6f4, 0x6fc, 0x704, 0x70c,
		0x714, 0x71c, 0x74c, 0x7b4, 0x7cc
	};
#elif defined LINUX_VER_3_10_0
	// 0x698 for 3.10.0-957, 3.10.0-1160
	int write_seq_offsets[] = { 0x698 };
#elif defined LINUX_VER_5_2_PLUS
	// 0x7bc for OEL7.9 Linux 5.4.17
	// 0x774 for TENCENT64.site 5.4.119-19-0008 aarch64
	int write_seq_offsets[] = {
		0x66c, 0x674, 0x67c, 0x684, 0x68c, 0x694,
		0x69c, 0x6a4, 0x6ac, 0x6b4, 0x6bc, 0x6c4,
		0x6cc, 0x6d4, 0x6dc, 0x6e4, 0x6ec, 0x6f4,
		0x6fc, 0x704, 0x70c, 0x714, 0x71c, 0x74c,
		0x7b4, 0x7d4, 0x7bc, 0x774
	};
#else
	// 0x7d4 for 4.19.90-2107.6.0.0100.oe1.bclinux
	// 0x7dc for 4.18.0-372.9.1.15.po1.x86_64
	// 0x73c for 4.19.91-21.al7.x86_64
	// 0x74c for 4.19.91-23.al7.x86_64, 4.19.91-24.1.al7.x86_64, 4.19.91-25.6.al7.x86_64
	//           4.19.91-26.6.al7.x86_64, 4.19.91-26.al7.x86_64, 4.19.91-27.1.al7.x86_64
	// 0x7c4 for 4.19.90-2107.6.0.0192.8.oe1.bclinux.x86_64
	// 0x80c for 4.19.0-91.82.65.uelc20.x86_64
	// 0x804 for 4.19.0-91.77.112.uelc20.x86_64, 4.19.0-91.82.132.uelc20.x86_64
	int write_seq_offsets[] = {
		0x66c, 0x674, 0x67c, 0x684, 0x68c, 0x694,
		0x69c, 0x6a4, 0x6ac, 0x6b4, 0x6bc, 0x6c4,
		0x6cc, 0x6d4, 0x6dc, 0x6e4, 0x6ec, 0x6f4,
		0x7c4, 0x73c, 0x74c, 0x7b4, 0x7d4, 0x7dc,
		0x80c, 0x804
	};
#endif
/* *INDENT-ON* */

	int i, snd_nxt_offset = 0;

	if (!offset->tcp_sock__copied_seq_offset) {
#pragma unroll
		for (i = 0; i < ARRAY_SIZE(copied_seq_offsets); i++) {
			if (retry_get_copied_seq(sk, copied_seq_offsets[i])) {
				offset->tcp_sock__copied_seq_offset =
				    copied_seq_offsets[i];
				break;
			}
		}
	}

	/*
	 * snd_nxt_offset 用于write_seq offset的判断。
	 *
	 *     u32      copied_seq;         0
	 *     u32      rcv_wup;            +4
	 *     u32      snd_nxt;            +8
	 */
	snd_nxt_offset = offset->tcp_sock__copied_seq_offset + 8;

	if (snd_nxt_offset == 8)
		return;

	if (!offset->tcp_sock__write_seq_offset) {
#pragma unroll
		for (i = 0; i < ARRAY_SIZE(write_seq_offsets); i++) {
			if (retry_get_write_seq
			    (sk, write_seq_offsets[i], snd_nxt_offset)) {
				offset->tcp_sock__write_seq_offset =
				    write_seq_offsets[i];
				break;
			}
		}
	}
}

static __inline bool check_pid_validity(void)
{
	__u32 k0 = 0;
	__u64 *adapt_uid = adapt_kern_uid_map__lookup(&k0);
	if (!adapt_uid)
		return false;

	// Only a preset uid can be adapted to the kernel
	if ((*adapt_uid) >> 32 != bpf_get_current_pid_tgid() >> 32)
		return false;

	return true;
}

static __inline int infer_offset_phase_1(int fd)
{
	__u32 k0 = 0;
	struct member_fields_offset *offset = members_offset__lookup(&k0);
	if (!offset)
		return OFFSET_NO_READY;

	if (unlikely(!offset->ready)) {
		if (!check_pid_validity())
			return OFFSET_NO_READY;

		void *infer_sk =
		    infer_and_get_socket_from_fd(fd, offset, false);
		if (infer_sk) {
			if (unlikely(!offset->sock__flags_offset))
				infer_sock_flags(infer_sk, offset);
		}
	} else {
		return OFFSET_READY;
	}

	return OFFSET_NO_READY;
}

static __inline int infer_offset_phase_2(int fd)
{
	__u32 k0 = 0;
	struct member_fields_offset *offset = members_offset__lookup(&k0);
	if (!offset)
		return OFFSET_NO_READY;

	if (unlikely(!offset->ready)) {
		if (!check_pid_validity())
			return OFFSET_NO_READY;

		if (unlikely
		    (!offset->sock__flags_offset
		     || !offset->task__files_offset))
			return OFFSET_NO_READY;

		void *sk = get_socket_from_fd(fd, offset);
		if (sk) {

			if (unlikely(!offset->tcp_sock__copied_seq_offset ||
				     !offset->tcp_sock__write_seq_offset)) {
				infer_tcp_seq_offset(sk, offset);
				if (likely
				    (offset->tcp_sock__copied_seq_offset
				     && offset->tcp_sock__write_seq_offset
				     && offset->sock__flags_offset
				     && offset->task__files_offset))
					offset->ready = 1;
			}
		}
	}

	if (!offset->ready)
		return OFFSET_NO_READY;
	return OFFSET_READY;
}

#define INFER_OFFSET_PHASE_1(f) \
do { \
	if (infer_offset_phase_1((f)) == OFFSET_NO_READY) \
		return 0; \
} while(0)

#define INFER_OFFSET_PHASE_2(f) \
do { \
	if (infer_offset_phase_2((f)) == OFFSET_NO_READY) \
		return 0; \
} while(0)

#define TRACE_MAP_ACT_NONE  0
#define TRACE_MAP_ACT_NEW   1
#define TRACE_MAP_ACT_DEL   2

static __inline void trace_process(struct socket_info_s *socket_info_ptr,
				   struct conn_info_s *conn_info,
				   __u64 socket_id, __u64 pid_tgid,
				   struct trace_info_t *trace_info_ptr,
				   struct tracer_ctx_s *tracer_ctx,
				   struct trace_stats *trace_stats,
				   __u64 * thread_trace_id,
				   __u64 time_stamp,
				   struct trace_key_t *trace_key)
{
	/*
	 * ==========================================
	 * Thread-Trace-ID (Single Redirect Trace)
	 * ==========================================
	 *
	 * Ingress              |                   | Egress
	 * ----------------------------------------------------------
	 *                   socket-a                |
	 * trace start ID ①  -> |                    |
	 *                      |                   socket-b
	 *                      - same thread ID --- |
	 *                                           | ①  -> trace end
	 *                                           |
	 *                                           |
	 * ... ...
	 *                   socket-n
	 * trace start ID ② -> |                     |
	 *                     |                    socket-m
	 *                      - same thread ID --- |
	 *                                           | ② -> trace end
	 */

	/*
	 * 同方向多个连续请求或回应的场景：
	 *
	 *              Ingress |
	 * ----------------------
	 *                   socket-n
	 *                ①  -> |
	 *                ②  -> |
	 *                ③  -> |
	 *               ......
	 *
	 *
	 *                      | Egress
	 * -----------------------------
	 *                   socket-m
	 *                      | -> ①
	 *                        ......
	 * 采用的策略是：沿用上次trace_info保存的traceID。
	 */

	__u64 pre_trace_id = 0;
	int ret;
	if (is_socket_info_valid(socket_info_ptr) &&
	    conn_info->direction == socket_info_ptr->direction) {
		if (trace_info_ptr)
			pre_trace_id = trace_info_ptr->thread_trace_id;
	}

	if (conn_info->direction == T_INGRESS) {
		struct trace_info_t trace_info = { 0 };
		*thread_trace_id = trace_info.thread_trace_id =
		    (pre_trace_id ==
		     0 ? ++tracer_ctx->thread_trace_id : pre_trace_id);
		/*
		 * For NGINX tracing, 'MSG_REQUEST' and 'MSG_RESPONSE' are used
		 * as judgment conditions. After enabling data segment reassembly,
		 * the reassembled segments are set to 'MSG_REQUEST'. Here, we need
		 * to correct it so that only the beginning of the segment data can
		 * be judged. It should be 'MSG_REASM_START', not is 'MSG_REASM_SEG'.
		 */
		if (conn_info->message_type == MSG_REQUEST &&
		    !conn_info->is_reasm_seg)
			/*
			 * Below is the processing scenario for NGINX:
			 * Save the fd for requests to the nginx frontend. The
			 * backend will query this trace information when 'socket()'
			 * is called and will set its 'sk_info.peer_fd'.
			 * The backend will not reach this point, as the request
			 * direction for the backend is outbound rather than inbound.
			 */
			trace_info.peer_fd = conn_info->fd;
		else if (conn_info->message_type == MSG_RESPONSE) {
			/*
			 * Currently, only the backend of NGINX sets the 'socket_info_ptr->peer_fd'
			 * value. This value contains the frontend fd. Essentially, this sets the
			 * 'peer_fd' to the frontend fd within the trace information.
			 */
			if (is_socket_info_valid(socket_info_ptr) &&
			    socket_info_ptr->peer_fd != 0)
				trace_info.peer_fd = socket_info_ptr->peer_fd;
		}
		trace_info.update_time = time_stamp / NS_PER_SEC;
		trace_info.socket_id = socket_id;
		ret = trace_map__update(trace_key, &trace_info);
		if (!trace_info_ptr) {
			if (ret == 0) {
				__sync_fetch_and_add
				    (&trace_stats->trace_map_count, 1);
			}
		}
	} else {		/* direction == T_EGRESS */
		if (trace_info_ptr) {
			*thread_trace_id = trace_info_ptr->thread_trace_id;

			/*
			 * Retain tracking information without deletion. Mainly address
			 * situations where MySQL 'kComStmtClose/kComStmtQuit' unilaterally
			 * sends (client only requests without response) tracking being
			 * severed.
			 *
			 * For example: (Mysql Client)
			 * 
			 * Request Type       Request TraceID   Response TraceID
			 * -----------------------------------------------------
			 * COM_STMT_EXECUTE   A                 B
			 * COM_STMT_CLOSE     B                 0
			 * COM_QUERY          B                 C
			 *
			 * Implement COM_QUERY with Request TraceID set to 'B' instead of '0'
			 * to avoid interruption of tracing.
			 */
			if (conn_info->keep_trace)
				return;

			if (!trace_map__delete(trace_key)) {
				__sync_fetch_and_add
				    (&trace_stats->trace_map_count, -1);
			}
		}
	}
}

#if defined(LINUX_VER_KFUNC) || defined(LINUX_VER_5_2_PLUS)
static __inline int
__output_data_common(void *ctx, struct tracer_ctx_s *tracer_ctx,
		     struct __socket_data_buffer *v_buff,
		     const struct data_args_t *args,
		     enum traffic_direction dir, bool vecs, int max_size,
		     bool is_close, __u32 reassembly_bytes);
#endif

static __inline int
__data_submit(struct pt_regs *ctx, struct conn_info_s *conn_info,
	      const struct data_args_t *args, const bool vecs,
	      __u32 syscall_len, struct member_fields_offset *offset,
	      __u64 time_stamp, const struct process_data_extra *extra)
{
	if (conn_info == NULL) {
		return SUBMIT_INVALID;
	}

	if (conn_info->sk == NULL || conn_info->message_type == MSG_UNKNOWN) {
		return SUBMIT_INVALID;
	}

	__u32 k0 = 0;
	struct tracer_ctx_s *tracer_ctx = tracer_ctx_map__lookup(&k0);
	if (tracer_ctx == NULL)
		return SUBMIT_INVALID;

	__u32 tgid = (__u32) (bpf_get_current_pid_tgid() >> 32);
	__u64 conn_key = gen_conn_key_id((__u64) tgid, (__u64) conn_info->fd);
	if (conn_info->message_type == MSG_CLEAR) {
		delete_socket_info(conn_key, conn_info->socket_info_ptr);
		return SUBMIT_INVALID;
	}

	__u32 tcp_seq = args->tcp_seq;
	__u64 thread_trace_id = 0;
	struct socket_info_s *sk_info;
#if defined(LINUX_VER_KFUNC) || defined(LINUX_VER_5_2_PLUS)
	__builtin_memset(&tracer_ctx->sk_info, 0, sizeof(tracer_ctx->sk_info));
	sk_info = &tracer_ctx->sk_info;
#else
	struct socket_info_s __sk_info = { 0 };
	sk_info = &__sk_info;
#endif

	if (tracer_ctx->disable_tracing)
		conn_info->no_trace = true;

	/*
	 * It is possible that these values were modified during ebpf running,
	 * so they are saved here.
	 */
	int data_max_sz = tracer_ctx->data_limit_max;

	struct trace_stats *trace_stats = trace_stats_map__lookup(&k0);
	if (trace_stats == NULL)
		return SUBMIT_INVALID;

#if defined(LINUX_VER_KFUNC) || defined(LINUX_VER_5_2_PLUS)
	struct trace_key_t trace_key = {};
	struct trace_info_t *trace_info_ptr = NULL;
	if (!conn_info->no_trace) {
		trace_key = get_trace_key(tracer_ctx->go_tracing_timeout, true);
		trace_info_ptr = trace_map__lookup(&trace_key);
	}
#else
	struct trace_key_t trace_key =
	    get_trace_key(tracer_ctx->go_tracing_timeout,
			  true);
	struct trace_info_t *trace_info_ptr = trace_map__lookup(&trace_key);
#endif
	struct socket_info_s *socket_info_ptr = conn_info->socket_info_ptr;
	// 'socket_id' used to resolve non-tracing between the same socket
	__u64 socket_id = 0;
	if (!is_socket_info_valid(socket_info_ptr)) {
		// Not use "++tracer_ctx->socket_id" here,
		// because it did not pass the verification of linux 4.14.x, 4.15.x
		socket_id = tracer_ctx->socket_id + 1;
	} else {
		socket_id = socket_info_ptr->uid;
	}

#define DNS_AAAA_TYPE_ID 0x1c
	// FIXME: By default, the Go process continuously sends A record and
	// AAAA record DNS request messages. In the current call chain tracking
	// implementation, two consecutive request messages before receiving
	// the response message will cause the link to be broken. Ignore the
	// AAAA record To ensure that the call chain will not be broken.
	if (conn_info->message_type != MSG_PRESTORE &&
	    conn_info->message_type != MSG_RECONFIRM &&
	    !conn_info->no_trace &&
	    (tracer_ctx->go_tracing_timeout != 0
	     || extra->is_go_process == false)
	    && !(conn_info->protocol == PROTO_DNS
		 && conn_info->dns_q_type == DNS_AAAA_TYPE_ID))
		trace_process(socket_info_ptr, conn_info, socket_id,
			      bpf_get_current_pid_tgid(), trace_info_ptr,
			      tracer_ctx, trace_stats, &thread_trace_id,
			      time_stamp, &trace_key);

	if (!is_socket_info_valid(socket_info_ptr)) {
		/*
		 * In the context of NGINX, the backend socket information is
		 * established during the 'socket()' system call, with 'peer_fd' and
		 * 'trace_id' set accordingly to maintain consistency.
		 */
		if (socket_info_ptr &&
		    conn_info->direction == T_EGRESS &&
		    !conn_info->no_trace && socket_info_ptr->peer_fd > 0) {
			sk_info->peer_fd = socket_info_ptr->peer_fd;
			thread_trace_id = socket_info_ptr->trace_id;
		}
#if defined(LINUX_VER_KFUNC)
		/* *INDENT-OFF* */
		sk_info->sk = args->sk;
		int sk_off = (int)((uintptr_t) __builtin_preserve_access_index(&((struct sock *)0)->sk_socket));
		bpf_probe_read_kernel(&sk_info->socket, sizeof(sk_info->socket), args->sk + sk_off);
		/* *INDENT-ON* */
#endif
		sk_info->no_trace = conn_info->no_trace;
		sk_info->uid = tracer_ctx->socket_id + 1;
		tracer_ctx->socket_id++;	// Ensure that socket_id is incremented.
		sk_info->l7_proto = conn_info->protocol;
		//Confirm whether data reassembly is required for this socket.
		if (is_proto_reasm_enabled(conn_info->protocol)) {
			sk_info->allow_reassembly = true;
			sk_info->reasm_bytes =
			    syscall_len >
			    data_max_sz ? data_max_sz : syscall_len;
		}
		sk_info->direction = conn_info->direction;
		sk_info->pre_direction = conn_info->direction;
		sk_info->role = conn_info->role;
		sk_info->update_time = time_stamp / NS_PER_SEC;
		sk_info->need_reconfirm = conn_info->need_reconfirm;
		sk_info->correlation_id = conn_info->correlation_id;
		if (conn_info->tuple.l4_protocol == IPPROTO_UDP &&
		    args->port > 0) {
			bpf_probe_read_kernel(sk_info->ipaddr,
					      sizeof(sk_info->ipaddr),
					      args->addr);
			sk_info->udp_pre_set_addr = 1;
			sk_info->port = args->port;
		}

		/*
		 * MSG_PRESTORE 目前只用于MySQL, Kafka协议推断
		 */
		if (conn_info->message_type == MSG_PRESTORE) {
			bpf_probe_read_kernel(sk_info->prev_data,
					      sizeof(sk_info->prev_data),
					      conn_info->prev_buf);
			sk_info->prev_data_len = conn_info->prev_count;
			sk_info->uid = 0;
		}

		int ret = socket_info_map__update(&conn_key, sk_info);
		if (socket_info_ptr == NULL && ret == 0) {
			__sync_fetch_and_add(&trace_stats->socket_map_count, 1);
		}
	}

	/*
	 * 对于预先存储数据或socket l7协议类型需要再次确认(适用于长链接)
	 * 的动作只建立socket_info_map项不会发送数据给用户态程序。
	 */
	if (conn_info->message_type == MSG_PRESTORE ||
	    conn_info->message_type == MSG_RECONFIRM)
		return SUBMIT_INVALID;

	struct __socket_data_buffer *v_buff =
	    bpf_map_lookup_elem(&NAME(data_buf), &k0);
	if (!v_buff)
		return SUBMIT_INVALID;

	__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, 1);
	struct __socket_data *v = (struct __socket_data *)&v_buff->data[0];

	if (v_buff->len > (sizeof(v_buff->data) - sizeof(*v))) {
		__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, -1);
		return SUBMIT_INVALID;
	}

	v = (struct __socket_data *)(v_buff->data + v_buff->len);
#ifndef LINUX_VER_KFUNC
	if (get_socket_info(v, conn_info->sk, conn_info) == false) {
		__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, -1);
		return SUBMIT_INVALID;
	}
#else
	if (get_socket_info(&v->tuple, conn_info->sk, conn_info) == false) {
		__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, -1);
		return SUBMIT_INVALID;
	}
#endif
	__u32 send_reasm_bytes = 0;
	if (is_socket_info_valid(socket_info_ptr)) {
		sk_info->uid = socket_info_ptr->uid;
		sk_info->allow_reassembly = socket_info_ptr->allow_reassembly;

		/*
		 * The kernel syscall interface determines that it is the TLS
		 * handshake protocol, and for the uprobe program, it needs to
		 * be re inferred to determine the upper layer protocol of TLS.
		 */
		if (socket_info_ptr->l7_proto == PROTO_TLS ||
		    socket_info_ptr->l7_proto == PROTO_UNKNOWN)
			socket_info_ptr->l7_proto = conn_info->protocol;

		/*
		 * Ensure that the accumulation operation of capturing the
		 * data sequence number is an atomic operation when multiple
		 * threads read/write to the socket simultaneously.
		 */
		__sync_fetch_and_add(&socket_info_ptr->seq, 1);
		sk_info->seq = socket_info_ptr->seq;
		socket_info_ptr->direction = conn_info->direction;
		socket_info_ptr->update_time = time_stamp / NS_PER_SEC;

		/*
		 * Currently, only the backend socket of NGINX sets the 'socket_info_ptr->peer_fd'
		 * value, which is the frontend fd. This handles notifying the frontend socket
		 * to use the current backend traceID when returning data.
		 */
		if (socket_info_ptr->peer_fd != 0
		    && conn_info->direction == T_INGRESS) {
			__u64 peer_conn_key = gen_conn_key_id((__u64) tgid,
							      (__u64)
							      socket_info_ptr->peer_fd);
			/*
			 * Query the socket information of the NGINX frontend and modify the
			 * traceID of the data returned by the frontend.
			 */
			struct socket_info_s *peer_socket_info_ptr =
			    socket_info_map__lookup(&peer_conn_key);
			if (is_socket_info_valid(peer_socket_info_ptr))
				peer_socket_info_ptr->trace_id =
				    thread_trace_id;
		}

		/*
		 * Below is the processing in the NGINX scenario:
		 * 1.The backend sets the 'socket_info_ptr->trace_id' during the 'socket()'
		 *   system call to ensure the traceID carried by the backend request is
		 *   consistent with the frontend request’s traceID.
		 * 2.The frontend sets the 'socket_info_ptr->trace_id' when the backend receives
		 *   a response to ensure the traceID carried by the frontend response data is
		 *   consistent with the traceID during the backend response.
		 */
		if (conn_info->direction == T_EGRESS
		    && socket_info_ptr->trace_id != 0) {
			thread_trace_id = socket_info_ptr->trace_id;
			socket_info_ptr->trace_id = 0;
		}

		if (!conn_info->is_reasm_seg)
			socket_info_ptr->reasm_bytes = 0;

		/*
		 * Below, confirm the actual size of the data to be transmitted after
		 * enabling data reassembly. The data transmission size is limited by
		 * the maximum transmission configuration value.
		 */
		if (sk_info->allow_reassembly
		    && socket_info_ptr->reasm_bytes < data_max_sz) {
			__u32 remain_bytes =
			    data_max_sz - socket_info_ptr->reasm_bytes;
			send_reasm_bytes =
			    (syscall_len >
			     remain_bytes ? remain_bytes : syscall_len);
			socket_info_ptr->reasm_bytes += send_reasm_bytes;
		}
	}

	v->tuple.l4_protocol = conn_info->tuple.l4_protocol;
	v->tuple.dport = conn_info->tuple.dport;
	v->tuple.num = conn_info->tuple.num;
	v->data_type = conn_info->protocol;
	if (conn_info->tuple.l4_protocol == IPPROTO_UDP && args->port > 0) {
		if (conn_info->skc_family == PF_INET) {
			bpf_probe_read_kernel(v->tuple.daddr, 4, args->addr);
			v->tuple.addr_len = 4;
		} else if (conn_info->skc_family == PF_INET6) {
			if (*(__u64 *) & args->addr[0] == 0 &&
			    *(__u32 *) & args->addr[8] == 0xffff0000) {
				*(__u32 *) v->tuple.daddr =
				    *(__u32 *) & args->addr[12];
				v->tuple.addr_len = 4;
			} else {
				bpf_probe_read_kernel(v->tuple.daddr, 16,
						      args->addr);
				v->tuple.addr_len = 16;
			}
		}
	}

	__u32 *socket_role = socket_role_map__lookup(&conn_key);
	v->socket_role = socket_role ? *socket_role : 0;
	v->socket_id = sk_info->uid;
	v->data_seq = sk_info->seq;
	v->tgid = tgid;
	v->is_tls = false;
	v->pid = (__u32) bpf_get_current_pid_tgid();

	// For blocking reads, there is a significant deviation between the
	// entry time of the system call and the real time of the read
	// operation. Therefore, the end time of the system call is used for
	// the read operation.
	v->timestamp = conn_info->direction == T_INGRESS ? bpf_ktime_get_ns() :
	    time_stamp;
	v->direction = conn_info->direction;
	v->syscall_len = syscall_len;
	v->msg_type = MSG_COMMON;

	// Reassembly modification type
	if (sk_info->allow_reassembly) {
		v->msg_type = MSG_REASM_START;
		if (conn_info->is_reasm_seg)
			v->msg_type = MSG_REASM_SEG;
		else
			send_reasm_bytes = 0;
	}
	v->tcp_seq = 0;

	if ((extra->source == DATA_SOURCE_GO_TLS_UPROBE ||
	     extra->source == DATA_SOURCE_OPENSSL_UPROBE) ||
	    (conn_info->tuple.l4_protocol == IPPROTO_TCP)) {
		/*
		 * If the current state is TCPF_CLOSE_WAIT, the FIN frame already has been received.
		 * However, it cannot be confirmed that it has been processed by the syscall,
		 * so use the tcp_seq value that entering the syscalls.
		 *
		 * Why not use "v->tcp_seq = args->tcp_seq;" ?
		 * This is because kernel 4.14 verify reports errors("R0 invalid mem access 'inv'").
		 */
		v->tcp_seq = tcp_seq;
	}

	v->thread_trace_id = thread_trace_id;
	bpf_get_current_comm(v->comm, sizeof(v->comm));

	if (conn_info->tuple.l4_protocol == IPPROTO_TCP &&
	    conn_info->protocol == PROTO_DNS && conn_info->prev_count == 2) {
		v->tcp_seq -= 2;
		conn_info->prev_count = 0;
	}

	if (conn_info->prev_count > 0) {
		// 注意这里没有调整v->syscall_len和v->len我们会在用户层做。
		bpf_probe_read_kernel(v->extra_data, sizeof(v->extra_data),
				      conn_info->prev_buf);
		v->extra_data_count = conn_info->prev_count;
		v->tcp_seq -= conn_info->prev_count;	// 客户端和服务端的tcp_seq匹配
	} else
		v->extra_data_count = 0;

	v->coroutine_id = trace_key.goid;
	v->source = extra->source;

#if defined(LINUX_VER_KFUNC) || defined(LINUX_VER_5_2_PLUS)
	__u32 cache_key = ((__u32) bpf_get_current_pid_tgid()) >> 16;
	if (cache_key < PROTO_INFER_CACHE_SIZE) {
		struct proto_infer_cache_t *p;
		p = proto_infer_cache_map__lookup(&cache_key);
		if (p) {
			__u16 idx = (__u16) bpf_get_current_pid_tgid();
			p->protocols[idx] = (__u8) v->data_type;
		}
	}

	return __output_data_common(ctx, tracer_ctx, v_buff, args,
				    conn_info->direction, (bool) vecs,
				    tracer_ctx->data_limit_max, false,
				    send_reasm_bytes);
#else
	struct tail_calls_context *context =
	    (struct tail_calls_context *)v->data;
	context->max_size_limit = data_max_sz;
	context->push_reassembly_bytes = send_reasm_bytes;
	context->vecs = (bool) vecs;
	context->is_close = false;
	context->dir = conn_info->direction;

	return SUBMIT_OK;
#endif
}

static __inline int trace_io_event_common(void *ctx,
					  struct member_fields_offset *offset,
					  struct data_args_t *data_args,
					  enum traffic_direction direction,
					  __u64 pid_tgid);
static __inline int process_data(struct pt_regs *ctx, __u64 id,
				 const enum traffic_direction direction,
				 struct data_args_t *args,
				 ssize_t bytes_count,
				 const struct process_data_extra *extra)
{
	if (!extra)
		return -1;

	if (!extra->vecs && args->buf == NULL)
		return -1;

	if (extra->vecs && (args->iov == NULL || args->iovlen <= 0))
		return -1;

	if (unlikely(args->fd < 0 || (int)bytes_count <= 0))
		return -1;

	/*
	 * TODO:
	 * Here you can filter the pid according to the configuration.
	 */

	__u32 k0 = 0, k1 = 1;
	struct member_fields_offset *offset = members_offset__lookup(&k0);
	if (!offset)
		return -1;

	if (unlikely(!offset->ready))
		return -1;

#if defined(LINUX_VER_KFUNC)
	void *sk = args->sk;
	if (sk == NULL)
		sk = get_socket_from_fd(args->fd, offset);
#else
	void *sk = get_socket_from_fd(args->fd, offset);
#endif
	struct conn_info_s *conn_info, __conn_info = { 0 };
	conn_info = &__conn_info;
	__u8 sock_state;
	if (!(sk != NULL &&
	      ((sock_state = is_tcp_udp_data(sk, offset, conn_info))
	       != SOCK_CHECK_TYPE_ERROR))) {
#if defined(LINUX_VER_KFUNC) || defined(LINUX_VER_5_2_PLUS)
		return trace_io_event_common(ctx, offset, args, direction, id);
#else
		return -1;
#endif
	}

	init_conn_info(id >> 32, args->fd, conn_info, sk, direction,
		       bytes_count, offset);
	if (conn_info->tuple.l4_protocol == IPPROTO_UDP
	    && conn_info->tuple.dport == 0) {
		conn_info->tuple.dport = args->port;
		if (conn_info->tuple.dport == 0 &&
		    is_socket_info_valid(conn_info->socket_info_ptr) &&
		    conn_info->socket_info_ptr->udp_pre_set_addr) {
			conn_info->tuple.dport =
			    conn_info->socket_info_ptr->port;
			args->port = conn_info->tuple.dport;
			bpf_probe_read_kernel(args->addr, sizeof(args->addr),
					      conn_info->socket_info_ptr->
					      ipaddr);
		}
	}

	conn_info->direction = direction;

	struct ctx_info_s *ctx_map = bpf_map_lookup_elem(&NAME(ctx_info), &k0);
	if (!ctx_map)
		return -1;

	struct kprobe_port_bitmap *bypass = kprobe_port_bitmap__lookup(&k1);
	if (bypass) {
		if (is_set_bitmap(bypass->bitmap, conn_info->tuple.dport) ||
		    is_set_bitmap(bypass->bitmap, conn_info->tuple.num)) {
			return -1;
		}
	}

	struct kprobe_port_bitmap *allow = kprobe_port_bitmap__lookup(&k0);
	if (allow) {
		if (is_set_bitmap(allow->bitmap, conn_info->tuple.dport) ||
		    is_set_bitmap(allow->bitmap, conn_info->tuple.num)) {
			conn_info->protocol = PROTO_CUSTOM;
		}
	}

	int act;
	act = infer_l7_class_1(ctx_map, conn_info, direction, args,
			       bytes_count, sock_state, extra);

#if !defined(LINUX_VER_KFUNC) && !defined(LINUX_VER_5_2_PLUS)
	if (act == INFER_CONTINUE) {
		ctx_map->tail_call.conn_info = __conn_info;
		ctx_map->tail_call.extra = *extra;
		ctx_map->tail_call.bytes_count = bytes_count;
		ctx_map->tail_call.offset = offset;
		ctx_map->tail_call.dir = direction;
		/* Enter the protocol inference tail call program. */
		if (extra->source == DATA_SOURCE_SYSCALL)
			bpf_tail_call(ctx, &NAME(progs_jmp_tp_map),
				      PROG_PROTO_INFER_TP_IDX);
		else
			bpf_tail_call(ctx, &NAME(progs_jmp_kp_map),
				      PROG_PROTO_INFER_KP_IDX);
	}
#endif

	if (conn_info->protocol == PROTO_CUSTOM) {
		if (conn_info->enable_reasm) {
			conn_info->is_reasm_seg = true;
		}
	}
	// When at least one of protocol or message_type is valid, 
	// data_submit can be performed, otherwise MySQL data may be lost
	if (conn_info->protocol != PROTO_UNKNOWN ||
	    conn_info->message_type != MSG_UNKNOWN) {
#if !defined(LINUX_VER_KFUNC) && !defined(LINUX_VER_5_2_PLUS)
		/*
		 * Fill in tail call context information.
		 */
		ctx_map->tail_call.conn_info = __conn_info;
		ctx_map->tail_call.extra = *extra;
		ctx_map->tail_call.bytes_count = bytes_count;
		ctx_map->tail_call.offset = offset;
		return 0;
#else
		return __data_submit(ctx, conn_info, args, extra->vecs,
				     bytes_count, offset, args->enter_ts,
				     extra);
#endif
	}

	return -1;
}

static __inline void process_syscall_data(struct pt_regs *ctx, __u64 id,
					  const enum traffic_direction
					  direction,
					  struct data_args_t *args,
					  ssize_t bytes_count)
{
	struct process_data_extra extra = {
		.vecs = false,
		.source = DATA_SOURCE_SYSCALL,
		.is_go_process = is_current_go_process(),
	};

	if (!process_data(ctx, id, direction, args, bytes_count, &extra)) {
#if !defined(LINUX_VER_KFUNC) && !defined(LINUX_VER_5_2_PLUS)
		bpf_tail_call(ctx, &NAME(progs_jmp_tp_map),
			      PROG_DATA_SUBMIT_TP_IDX);
	} else {
		bpf_tail_call(ctx, &NAME(progs_jmp_tp_map),
			      PROG_IO_EVENT_TP_IDX);
#endif
	}
}

static __inline void process_syscall_data_vecs(struct pt_regs *ctx, __u64 id,
					       const enum traffic_direction
					       direction,
					       struct data_args_t *args,
					       ssize_t bytes_count)
{
	struct process_data_extra extra = {
		.vecs = true,
		.source = DATA_SOURCE_SYSCALL,
		.is_go_process = is_current_go_process(),
	};

	if (!process_data(ctx, id, direction, args, bytes_count, &extra)) {
#if !defined(LINUX_VER_KFUNC) && !defined(LINUX_VER_5_2_PLUS)
		bpf_tail_call(ctx, &NAME(progs_jmp_tp_map),
			      PROG_DATA_SUBMIT_TP_IDX);
	} else {
		bpf_tail_call(ctx, &NAME(progs_jmp_tp_map),
			      PROG_IO_EVENT_TP_IDX);
#endif
	}
}

/***********************************************************
 * BPF syscall probe/tracepoint/kfunc function entry-points
 ***********************************************************/
#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KPROG(ksys_write) (struct pt_regs* ctx) {
	int fd = (int)PT_REGS_PARM1(ctx);
	char *buf = (char *)PT_REGS_PARM2(ctx);
#else
TP_SYSCALL_PROG(enter_write) (struct syscall_comm_enter_ctx * ctx) {
	int fd = (int)ctx->fd;
	char *buf = (char *)ctx->buf;
#endif /*NO_FTRACE_SYSCALL*/
#else
// ssize_t ksys_write(unsigned int fd, const char __user *buf, size_t count)
KFUNC_PROG(ksys_write, unsigned int fd, const char __user * buf, size_t count)
{
#endif
	__u64 id = bpf_get_current_pid_tgid();
	struct data_args_t write_args = {};
	write_args.source_fn = SYSCALL_FUNC_WRITE;
	write_args.fd = fd;
	write_args.buf = buf;
	write_args.enter_ts = bpf_ktime_get_ns();
	__u64 conn_key = gen_conn_key_id((__u64) (id >> 32), (__u64) fd);
	struct socket_info_s *socket_info_ptr =
	    socket_info_map__lookup(&conn_key);
	write_args.tcp_seq =
	    get_tcp_write_seq(fd, &write_args.sk, socket_info_ptr);
	active_write_args_map__update(&id, &write_args);

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(ksys_write) (struct pt_regs* ctx) {
	ssize_t bytes_count = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_write/format
TP_SYSCALL_PROG(exit_write) (struct syscall_comm_exit_ctx * ctx) {
	ssize_t bytes_count = ctx->ret;
#endif /*NO_FTRACE_SYSCALL*/
#else
KRETFUNC_PROG(ksys_write, unsigned int fd, const char __user * buf,
	      size_t count, ssize_t ret)
{
	ssize_t bytes_count = ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Unstash arguments, and process syscall.
	struct data_args_t *write_args = active_write_args_map__lookup(&id);
	// Don't process FD 0-2 to avoid STDIN, STDOUT, STDERR.
	if (write_args != NULL && write_args->fd > 2) {
		write_args->bytes_count = bytes_count;
		process_syscall_data((struct pt_regs *)ctx, id, T_EGRESS,
				     write_args, bytes_count);
	}

	active_write_args_map__delete(&id);
	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
// ssize_t read(int fd, void *buf, size_t count);
KPROG(ksys_read) (struct pt_regs* ctx) {
	int fd = (unsigned int)PT_REGS_PARM1(ctx);
	char *buf = (char *)PT_REGS_PARM2(ctx);
#else
// ssize_t read(int fd, void *buf, size_t count);
TP_SYSCALL_PROG(enter_read) (struct syscall_comm_enter_ctx * ctx) {
	int fd = (int)ctx->fd;
	char *buf = (char *)ctx->buf;
#endif /*NO_FTRACE_SYSCALL*/
#else
// ssize_t ksys_read(unsigned int fd, char __user *buf, size_t count)
KFUNC_PROG(ksys_read, unsigned int fd, const char __user * buf, size_t count)
{
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Stash arguments.
	struct data_args_t read_args = {};
	read_args.source_fn = SYSCALL_FUNC_READ;
	read_args.fd = fd;
	read_args.buf = buf;
	read_args.enter_ts = bpf_ktime_get_ns();
	__u64 conn_key = gen_conn_key_id((__u64) (id >> 32), (__u64) fd);
	struct socket_info_s *socket_info_ptr =
	    socket_info_map__lookup(&conn_key);
	read_args.tcp_seq =
	    get_tcp_read_seq(fd, &read_args.sk, socket_info_ptr);
	active_read_args_map__update(&id, &read_args);

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(ksys_read) (struct pt_regs* ctx) {
	ssize_t bytes_count = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_read/format
TP_SYSCALL_PROG(exit_read) (struct syscall_comm_exit_ctx * ctx) {
	ssize_t bytes_count = ctx->ret;
#endif /*NO_FTRACE_SYSCALL*/
#else
// ssize_t ksys_read(unsigned int fd, char __user *buf, size_t count)
KRETFUNC_PROG(ksys_read, unsigned int fd, const char __user * buf, size_t count,
	      ssize_t ret)
{
	size_t bytes_count = ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Unstash arguments, and process syscall.
	struct data_args_t *read_args = active_read_args_map__lookup(&id);
	// Don't process FD 0-2 to avoid STDIN, STDOUT, STDERR.
	if (read_args != NULL && read_args->fd > 2) {
		read_args->bytes_count = bytes_count;
		process_syscall_data((struct pt_regs *)ctx, id, T_INGRESS,
				     read_args, bytes_count);
	}

	active_read_args_map__delete(&id);
	return 0;
}

/*
 * The `sendto` functions are generally used in UDP protocols, but can also be used
 * in TCP after the connect function is called. `sendto()` use the datagram method
 * to transmit data.
 * In the connectionless datagram socket mode, since the local socket has not
 * established a connection with the remote machine, the destination address should
 * be specified when sending data. The sendto() function prototype is:
 *
 * `int sendto(socket s, const void *msg, int len, unsigned int flags, const
 *             struct sockaddr *to, int tolen);`
 *
 * The sendto() function has two more parameters than the send() function. The "to"
 * parameter specifies the IP address and port number information of the destination
 * machine.
 * 
 * Our current logic is as follows: network tuple information (IP, PORT) is obtained
 * by reading the corresponding fields of the kernel structure 'struct sock_common'.
 * Since the IP address and port are specified in the sendto() system calls,
 * the tuple data will not be populated into the kernel structure 'struct sock_common'.
 * As a result, we cannot obtain the tuple information. Therefore, when entering these
 * types of system calls, we need to save this information beforehand.
 */
#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
// ssize_t sendto(int sockfd, const void *buf, size_t len, int flags,
//		const struct sockaddr *dest_addr, socklen_t addrlen);
KPROG(__sys_sendto) (struct pt_regs* ctx) {
	int sockfd = (int)PT_REGS_PARM1(ctx);
	char *buf = (char *)PT_REGS_PARM2(ctx);
#else
TP_SYSCALL_PROG(enter_sendto) (struct syscall_comm_enter_ctx * ctx) {
	int sockfd = (int)ctx->fd;
	char *buf = (char *)ctx->buf;
#endif /*NO_FTRACE_SYSCALL*/
#else
//int __sys_sendto(int fd, void __user *buff, size_t len, unsigned int flags,
//                 struct sockaddr __user *addr,  int addr_len)
KFUNC_PROG(__sys_sendto, int fd, void __user * buff, size_t len,
	   unsigned int flags, struct sockaddr __user * u_addr, int addr_len)
{
	int sockfd = fd;
	char *buf = (char *)buff;
#endif
	__u64 id = bpf_get_current_pid_tgid();

	INFER_OFFSET_PHASE_1(sockfd);

	// Stash arguments.
	struct data_args_t write_args = {};
	write_args.source_fn = SYSCALL_FUNC_SENDTO;
	write_args.fd = sockfd;
	write_args.buf = buf;
	write_args.enter_ts = bpf_ktime_get_ns();
	__u64 conn_key = gen_conn_key_id((__u64) (id >> 32), (__u64) sockfd);
	struct socket_info_s *socket_info_ptr =
	    socket_info_map__lookup(&conn_key);
	write_args.tcp_seq =
	    get_tcp_write_seq(sockfd, &write_args.sk, socket_info_ptr);
	if (write_args.tcp_seq == 0) {
#ifndef LINUX_VER_KFUNC
		struct syscall_sendto_enter_ctx *sendto_ctx =
		    (struct syscall_sendto_enter_ctx *)ctx;
		struct sockaddr_in addr = { 0 };
		bpf_probe_read_user(&addr, sizeof(addr), sendto_ctx->addr);
#else
		struct sockaddr_in addr = { 0 };
		bpf_probe_read_user(&addr, sizeof(addr), u_addr);
#endif
		write_args.port = __bpf_ntohs(addr.sin_port);
		if (write_args.port > 0 && addr.sin_family == AF_INET) {
			*(__u32 *) write_args.addr =
			    __bpf_ntohl(addr.sin_addr.s_addr);
		} else if (write_args.port > 0 && addr.sin_family == AF_INET6) {
			struct sockaddr_in6 addr = { 0 };
#ifndef LINUX_VER_KFUNC
			bpf_probe_read_user(&addr, sizeof(addr),
					    sendto_ctx->addr);
#else
			bpf_probe_read_user(&addr, sizeof(addr), u_addr);
#endif
			bpf_probe_read_kernel(&write_args.addr[0], 16,
					      &addr.sin6_addr.s6_addr[0]);
		}
	}

	active_write_args_map__update(&id, &write_args);

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(__sys_sendto) (struct pt_regs* ctx) {
	ssize_t bytes_count = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_sendto/format
TP_SYSCALL_PROG(exit_sendto) (struct syscall_comm_exit_ctx * ctx) {
	ssize_t bytes_count = ctx->ret;
#endif /*NO_FTRACE_SYSCALL*/
#else
KRETFUNC_PROG(__sys_sendto, int fd, void __user * buff, size_t len,
	      unsigned int flags, struct sockaddr __user * u_addr, int addr_len,
	      int ret)
{
	ssize_t bytes_count = (int)ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Unstash arguments, and process syscall.
	struct data_args_t *write_args = active_write_args_map__lookup(&id);
	if (write_args != NULL) {
		write_args->bytes_count = bytes_count;
		process_syscall_data((struct pt_regs *)ctx, id, T_EGRESS,
				     write_args, bytes_count);
		active_write_args_map__delete(&id);
	}

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
// ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags,
//		  struct sockaddr *src_addr, socklen_t *addrlen);
KPROG(__sys_recvfrom) (struct pt_regs* ctx) {
	int sockfd = (int)PT_REGS_PARM1(ctx);
	char *buf = (char *)PT_REGS_PARM2(ctx);
	int flags = (int)PT_REGS_PARM4(ctx);
	// If flags contains MSG_PEEK, it is returned directly.
	// ref : https://linux.die.net/man/2/recvfrom
	if (flags & MSG_PEEK)
		return 0;
#else
// ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags,
//                struct sockaddr *src_addr, socklen_t *addrlen);
TP_SYSCALL_PROG(enter_recvfrom) (struct syscall_comm_enter_ctx * ctx) {
	// If flags contains MSG_PEEK, it is returned directly.
	// ref : https://linux.die.net/man/2/recvfrom
	if (ctx->flags & MSG_PEEK)
		return 0;
	int sockfd = (int)ctx->fd;
	char *buf = (char *)ctx->buf;
#endif /*NO_FTRACE_SYSCALL*/
#else
//int __sys_recvfrom(int fd, void __user *ubuf, size_t size, unsigned int flags,
//                   struct sockaddr __user *addr, int __user *addr_len)
KFUNC_PROG(__sys_recvfrom, int fd, void __user * ubuf, size_t size,
	   unsigned int flags, struct sockaddr __user * addr,
	   int __user * addr_len)
{
	if (flags & MSG_PEEK)
		return 0;
	int sockfd = fd;
	char *buf = (char *)ubuf;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Stash arguments.
	struct data_args_t read_args = {};
	read_args.source_fn = SYSCALL_FUNC_RECVFROM;
	read_args.fd = sockfd;
	read_args.buf = buf;
	read_args.enter_ts = bpf_ktime_get_ns();
	__u64 conn_key = gen_conn_key_id((__u64) (id >> 32), (__u64) sockfd);
	struct socket_info_s *socket_info_ptr =
	    socket_info_map__lookup(&conn_key);
	read_args.tcp_seq =
	    get_tcp_read_seq(sockfd, &read_args.sk, socket_info_ptr);
	active_read_args_map__update(&id, &read_args);

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_recvfrom/format
KRETPROG(__sys_recvfrom) (struct pt_regs* ctx) {
	ssize_t bytes_count = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_recvfrom/format
TP_SYSCALL_PROG(exit_recvfrom) (struct syscall_comm_exit_ctx * ctx) {
	ssize_t bytes_count = ctx->ret;
#endif /*NO_FTRACE_SYSCALL*/
#else
KRETFUNC_PROG(__sys_recvfrom, int fd, void __user * ubuf, size_t size,
	      unsigned int flags, struct sockaddr __user * addr,
	      int __user * addr_len, int ret)
{
	ssize_t bytes_count = ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Unstash arguments, and process syscall.
	struct data_args_t *read_args = active_read_args_map__lookup(&id);
	if (read_args != NULL) {
		read_args->bytes_count = bytes_count;
		process_syscall_data((struct pt_regs *)ctx, id, T_INGRESS,
				     read_args, bytes_count);
		active_read_args_map__delete(&id);
	}

	return 0;
}

#ifndef LINUX_VER_KFUNC
// ssize_t sendmsg(int sockfd, const struct msghdr *msg, int flags);
KPROG(__sys_sendmsg) (struct pt_regs * ctx) {
	int sockfd = (int)PT_REGS_PARM1(ctx);
	struct user_msghdr *msghdr_ptr =
	    (struct user_msghdr *)PT_REGS_PARM2(ctx);
#else
// long __sys_sendmsg(int fd, struct user_msghdr __user *msg, unsigned int flags,
//                    bool forbid_cmsg_compat)
KFUNC_PROG(__sys_sendmsg, int fd, struct user_msghdr __user * msg,
	   unsigned int flags, bool forbid_cmsg_compat)
{
	int sockfd = fd;
	struct user_msghdr *msghdr_ptr = msg;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	if (msghdr_ptr != NULL) {
		// Stash arguments.
		struct user_msghdr *msghdr, __msghdr;
		bpf_probe_read_user(&__msghdr, sizeof(__msghdr), msghdr_ptr);
		msghdr = &__msghdr;
		// Stash arguments.
		struct data_args_t write_args = {};
		write_args.source_fn = SYSCALL_FUNC_SENDMSG;
		write_args.fd = sockfd;
		write_args.iov = msghdr->msg_iov;
		write_args.iovlen = msghdr->msg_iovlen;
		write_args.enter_ts = bpf_ktime_get_ns();
		__u64 conn_key =
		    gen_conn_key_id((__u64) (id >> 32), (__u64) sockfd);
		struct socket_info_s *socket_info_ptr =
		    socket_info_map__lookup(&conn_key);
		write_args.tcp_seq =
		    get_tcp_write_seq(sockfd, &write_args.sk, socket_info_ptr);
		active_write_args_map__update(&id, &write_args);
	}

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(__sys_sendmsg) (struct pt_regs* ctx) {
	ssize_t bytes_count = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_sendmsg/format
TP_SYSCALL_PROG(exit_sendmsg) (struct syscall_comm_exit_ctx * ctx) {
	ssize_t bytes_count = ctx->ret;
#endif /*NO_FTRACE_SYSCALL*/
#else
KRETFUNC_PROG(__sys_sendmsg, int sockfd, const struct msghdr * msg, int flags,
	      bool forbid_cmsg_compat, long ret)
{
	ssize_t bytes_count = (ssize_t) ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Unstash arguments, and process syscall.
	struct data_args_t *write_args = active_write_args_map__lookup(&id);
	if (write_args != NULL) {
		write_args->bytes_count = bytes_count;
		process_syscall_data_vecs((struct pt_regs *)ctx, id, T_EGRESS,
					  write_args, bytes_count);
		active_write_args_map__delete(&id);
	}

	return 0;
}

#ifndef LINUX_VER_KFUNC
// int sendmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen,
//              int flags);
KPROG(__sys_sendmmsg) (struct pt_regs * ctx) {
	int sockfd = (int)PT_REGS_PARM1(ctx);
	struct mmsghdr *msgvec_ptr = (struct mmsghdr *)PT_REGS_PARM2(ctx);
	unsigned int vlen = (unsigned int)PT_REGS_PARM3(ctx);
#else
//int __sys_sendmmsg(int fd, struct mmsghdr __user *mmsg, unsigned int vlen,
//                   unsigned int flags, bool forbid_cmsg_compat)
KFUNC_PROG(__sys_sendmmsg, int fd, struct mmsghdr __user * mmsg,
	   unsigned int vlen, unsigned int flags, bool forbid_cmsg_compat)
{
	int sockfd = fd;
	struct mmsghdr *msgvec_ptr = mmsg;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	if (msgvec_ptr != NULL && vlen >= 1) {
		struct mmsghdr *msgvec, __msgvec;
		bpf_probe_read_user(&__msgvec, sizeof(__msgvec), msgvec_ptr);
		msgvec = &__msgvec;
		// Stash arguments.
		struct data_args_t write_args = {};
		write_args.source_fn = SYSCALL_FUNC_SENDMMSG;
		write_args.fd = sockfd;
		write_args.iov = msgvec[0].msg_hdr.msg_iov;
		write_args.iovlen = msgvec[0].msg_hdr.msg_iovlen;
		write_args.msg_len = (void *)msgvec_ptr + offsetof(typeof(struct mmsghdr), msg_len);	//&msgvec[0].msg_len;
		write_args.enter_ts = bpf_ktime_get_ns();
		__u64 conn_key =
		    gen_conn_key_id((__u64) (id >> 32), (__u64) sockfd);
		struct socket_info_s *socket_info_ptr =
		    socket_info_map__lookup(&conn_key);
		write_args.tcp_seq =
		    get_tcp_write_seq(sockfd, &write_args.sk, socket_info_ptr);
		active_write_args_map__update(&id, &write_args);
	}

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(__sys_sendmmsg)(struct pt_regs* ctx) {
	int num_msgs = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_sendmmsg/format
TP_SYSCALL_PROG(exit_sendmmsg) (struct syscall_comm_exit_ctx * ctx) {
	int num_msgs = ctx->ret;
#endif /* NO_FTRACE_SYSCALL */
#else
KRETFUNC_PROG(__sys_sendmmsg, int fd, struct mmsghdr __user * mmsg,
	      unsigned int vlen, unsigned int flags, bool forbid_cmsg_compat,
	      int ret)
{
	int num_msgs = ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Unstash arguments, and process syscall.
	struct data_args_t *write_args = active_write_args_map__lookup(&id);
	if (write_args != NULL && num_msgs > 0) {
		ssize_t bytes_count;
		bpf_probe_read_user(&bytes_count, sizeof(write_args->msg_len),
				    write_args->msg_len);
		process_syscall_data_vecs((struct pt_regs *)ctx, id, T_EGRESS,
					  write_args, bytes_count);
	}
	active_write_args_map__delete(&id);

	return 0;
}

// BSD recvmsg interface
// long __sys_recvmsg(int fd, struct user_msghdr __user *msg, unsigned int flags,
//                 bool forbid_cmsg_compat)
// ssize_t recvmsg(int sockfd, struct msghdr *msg, int flags);
#ifndef LINUX_VER_KFUNC
KPROG(__sys_recvmsg) (struct pt_regs * ctx) {
	int flags = (int)PT_REGS_PARM3(ctx);
	if (flags & MSG_PEEK)
		return 0;
	struct user_msghdr __msg, *msghdr =
	    (struct user_msghdr *)PT_REGS_PARM2(ctx);
	int sockfd = (int)PT_REGS_PARM1(ctx);
#else
KFUNC_PROG(__sys_recvmsg, int fd, struct user_msghdr __user * msg,
	   unsigned int flags, bool forbid_cmsg_compat)
{
	if (flags & MSG_PEEK)
		return 0;
	struct user_msghdr __msg, *msghdr = msg;
	int sockfd = fd;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	if (msghdr != NULL) {
		bpf_probe_read_user(&__msg, sizeof(__msg), (void *)msghdr);
		msghdr = &__msg;
		// Stash arguments.
		struct data_args_t read_args = {};
		read_args.source_fn = SYSCALL_FUNC_RECVMSG;
		read_args.fd = sockfd;
		read_args.iov = msghdr->msg_iov;
		read_args.iovlen = msghdr->msg_iovlen;
		read_args.enter_ts = bpf_ktime_get_ns();
		__u64 conn_key =
		    gen_conn_key_id((__u64) (id >> 32), (__u64) sockfd);
		struct socket_info_s *socket_info_ptr =
		    socket_info_map__lookup(&conn_key);
		read_args.tcp_seq =
		    get_tcp_read_seq(sockfd, &read_args.sk, socket_info_ptr);
		active_read_args_map__update(&id, &read_args);
	}

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(__sys_recvmsg) (struct pt_regs* ctx) {
	ssize_t bytes_count = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_recvmsg/format
TP_SYSCALL_PROG(exit_recvmsg) (struct syscall_comm_exit_ctx * ctx) {
	ssize_t bytes_count = ctx->ret;
#endif /* NO_FTRACE_SYSCALL */
#else
KRETFUNC_PROG(__sys_recvmsg, int fd, struct user_msghdr __user * msg,
	      unsigned int flags, bool forbid_cmsg_compat, long ret)
{
	ssize_t bytes_count = ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Unstash arguments, and process syscall.
	struct data_args_t *read_args = active_read_args_map__lookup(&id);
	if (read_args != NULL) {
		read_args->bytes_count = bytes_count;
		process_syscall_data_vecs((struct pt_regs *)ctx, id, T_INGRESS,
					  read_args, bytes_count);
		active_read_args_map__delete(&id);
	}

	return 0;
}

//int __sys_recvmmsg(int fd, struct mmsghdr __user *mmsg,
//                   unsigned int vlen, unsigned int flags,
//                   struct __kernel_timespec __user *timeout,
//                   struct old_timespec32 __user *timeout32)
#ifndef LINUX_VER_KFUNC
KPROG(__sys_recvmmsg) (struct pt_regs * ctx) {
	int flags = (int)PT_REGS_PARM4(ctx);
	if (flags & MSG_PEEK)
		return 0;
	int sockfd = (int)PT_REGS_PARM1(ctx);
	struct mmsghdr *msgvec = (struct mmsghdr *)PT_REGS_PARM2(ctx);
	unsigned int vlen = (unsigned int)PT_REGS_PARM3(ctx);
#else
KFUNC_PROG(__sys_recvmmsg, int fd, struct mmsghdr __user * mmsg,
	   unsigned int vlen, unsigned int flags,
	   struct __kernel_timespec __user * timeout,
	   struct old_timespec32 __user * timeout32)
{
	if (flags & MSG_PEEK)
		return 0;
	int sockfd = fd;
	struct mmsghdr *msgvec = mmsg;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	if (msgvec != NULL && vlen >= 1) {
		int offset;
		// Stash arguments.
		struct data_args_t read_args = {};
		read_args.source_fn = SYSCALL_FUNC_RECVMMSG;
		read_args.fd = sockfd;
		read_args.enter_ts = bpf_ktime_get_ns();

		offset = offsetof(typeof(struct mmsghdr), msg_hdr) +
		    offsetof(typeof(struct user_msghdr), msg_iov);

		bpf_probe_read_user(&read_args.iov, sizeof(read_args.iov),
				    (void *)msgvec + offset);

		offset = offsetof(typeof(struct mmsghdr), msg_hdr) +
		    offsetof(typeof(struct user_msghdr), msg_iovlen);

		bpf_probe_read_user(&read_args.iovlen, sizeof(read_args.iovlen),
				    (void *)msgvec + offset);

		read_args.msg_len =
		    (void *)msgvec + offsetof(typeof(struct mmsghdr), msg_len);
		__u64 conn_key =
		    gen_conn_key_id((__u64) (id >> 32), (__u64) sockfd);
		struct socket_info_s *socket_info_ptr =
		    socket_info_map__lookup(&conn_key);
		read_args.tcp_seq =
		    get_tcp_read_seq(sockfd, &read_args.sk, socket_info_ptr);
		active_read_args_map__update(&id, &read_args);
	}

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(__sys_recvmmsg) (struct pt_regs* ctx) {
	int num_msgs = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_recvmmsg/format
TP_SYSCALL_PROG(exit_recvmmsg) (struct syscall_comm_exit_ctx * ctx) {
	int num_msgs = ctx->ret;
#endif /* NO_FTRACE_SYSCALL */
#else
KRETFUNC_PROG(__sys_recvmmsg, int fd, struct mmsghdr __user * mmsg,
	      unsigned int vlen, unsigned int flags,
	      struct __kernel_timespec __user * timeout,
	      struct old_timespec32 __user * timeout32, int ret)
{
	int num_msgs = ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Unstash arguments, and process syscall.
	struct data_args_t *read_args = active_read_args_map__lookup(&id);
	if (read_args != NULL && num_msgs > 0) {
		ssize_t bytes_count;
		bpf_probe_read_user(&bytes_count, sizeof(read_args->msg_len),
				    read_args->msg_len);
		process_syscall_data_vecs((struct pt_regs *)ctx, id, T_INGRESS,
					  read_args, bytes_count);
	}
	active_read_args_map__delete(&id);

	return 0;
}

//static ssize_t do_writev(unsigned long fd, const struct iovec __user *vec,
//                       unsigned long vlen, rwf_t flags)
// ssize_t writev(int fd, const struct iovec *iov, int iovcnt);
#ifndef LINUX_VER_KFUNC
#ifdef LINUX_VER_3_10_0
KPROG(sys_writev) (struct pt_regs * ctx) {
#else
KPROG(do_writev) (struct pt_regs * ctx) {
#endif
	int fd = (int)PT_REGS_PARM1(ctx);
	struct iovec *iov = (struct iovec *)PT_REGS_PARM2(ctx);
	int iovlen = (int)PT_REGS_PARM3(ctx);
#else
typedef int rwf_t;
KFUNC_PROG(do_writev, unsigned long fd, const struct iovec __user * vec,
	   unsigned long vlen, rwf_t flags)
{
	struct iovec *iov = (struct iovec *)vec;
	int iovlen = (int)vlen;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Stash arguments.
	struct data_args_t write_args = {};
	write_args.source_fn = SYSCALL_FUNC_WRITEV;
	write_args.fd = (int)fd;
	write_args.iov = iov;
	write_args.iovlen = iovlen;
	write_args.enter_ts = bpf_ktime_get_ns();
	__u64 conn_key = gen_conn_key_id((__u64) (id >> 32), (__u64) fd);
	struct socket_info_s *socket_info_ptr =
	    socket_info_map__lookup(&conn_key);
	write_args.tcp_seq =
	    get_tcp_write_seq(fd, &write_args.sk, socket_info_ptr);
	active_write_args_map__update(&id, &write_args);
	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(do_writev) (struct pt_regs* ctx) {
	ssize_t bytes_count = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_writev/format
TP_SYSCALL_PROG(exit_writev) (struct syscall_comm_exit_ctx * ctx) {
	ssize_t bytes_count = ctx->ret;
#endif /* NO_FTRACE_SYSCALL */
#else
KRETFUNC_PROG(do_writev, unsigned long fd, const struct iovec __user * vec,
	      unsigned long vlen, rwf_t flags, ssize_t ret)
{
	ssize_t bytes_count = ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Unstash arguments, and process syscall.
	struct data_args_t *write_args = active_write_args_map__lookup(&id);
	if (write_args != NULL) {
		write_args->bytes_count = bytes_count;
		process_syscall_data_vecs((struct pt_regs *)ctx, id, T_EGRESS,
					  write_args, bytes_count);
	}

	active_write_args_map__delete(&id);
	return 0;
}

// ssize_t readv(int fd, const struct iovec *iov, int iovcnt);
//static ssize_t do_readv(unsigned long fd, const struct iovec __user *vec,
//                        unsigned long vlen, rwf_t flags)
#ifndef LINUX_VER_KFUNC
#ifdef LINUX_VER_3_10_0
KPROG(sys_readv) (struct pt_regs * ctx) {
#else
KPROG(do_readv) (struct pt_regs * ctx) {
#endif
	int fd = (int)PT_REGS_PARM1(ctx);
	struct iovec *iov = (struct iovec *)PT_REGS_PARM2(ctx);
	int iovlen = (int)PT_REGS_PARM3(ctx);
#else
KFUNC_PROG(do_readv, unsigned long fd, const struct iovec __user * vec,
	   unsigned long vlen, rwf_t flags)
{
	struct iovec *iov = (struct iovec *)vec;
	int iovlen = (int)vlen;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	// Stash arguments.
	struct data_args_t read_args = {};
	read_args.source_fn = SYSCALL_FUNC_READV;
	read_args.fd = (int)fd;
	read_args.iov = iov;
	read_args.iovlen = iovlen;
	read_args.enter_ts = bpf_ktime_get_ns();
	__u64 conn_key = gen_conn_key_id((__u64) (id >> 32), (__u64) fd);
	struct socket_info_s *socket_info_ptr =
	    socket_info_map__lookup(&conn_key);
	read_args.tcp_seq =
	    get_tcp_read_seq(fd, &read_args.sk, socket_info_ptr);
	active_read_args_map__update(&id, &read_args);

	return 0;
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(do_readv) (struct pt_regs* ctx) {
	ssize_t bytes_count = PT_REGS_RC(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_readv/format
TP_SYSCALL_PROG(exit_readv) (struct syscall_comm_exit_ctx * ctx) {
	ssize_t bytes_count = ctx->ret;
#endif /* NO_FTRACE_SYSCALL */
#else
KRETFUNC_PROG(do_readv, unsigned long fd, const struct iovec __user * vec,
	      unsigned long vlen, rwf_t flags, ssize_t ret)
{
	ssize_t bytes_count = ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	struct data_args_t *read_args = active_read_args_map__lookup(&id);
	if (read_args != NULL) {
		read_args->bytes_count = bytes_count;
		process_syscall_data_vecs((struct pt_regs *)ctx, id, T_INGRESS,
					  read_args, bytes_count);
	}

	active_read_args_map__delete(&id);
	return 0;
}

#ifndef LINUX_VER_KFUNC
static __inline void __push_close_event(__u64 pid_tgid, __u64 uid, __u64 seq,
					struct member_fields_offset *offset,
					struct syscall_comm_enter_ctx *ctx)
#else
static __inline void __push_close_event(__u64 pid_tgid, __u64 uid, __u64 seq,
					struct member_fields_offset *offset,
					unsigned long long *ctx)
#endif
{
	__u32 k0 = 0;
	struct tracer_ctx_s *tracer_ctx = tracer_ctx_map__lookup(&k0);
	if (tracer_ctx == NULL)
		return;
	int data_max_sz = tracer_ctx->data_limit_max;
	struct __socket_data_buffer *v_buff =
	    bpf_map_lookup_elem(&NAME(data_buf), &k0);
	if (!v_buff)
		return;

	__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, 1);
	struct __socket_data *v = (struct __socket_data *)&v_buff->data[0];
	if (v_buff->len > (sizeof(v_buff->data) - sizeof(*v))) {
		__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, -1);
		return;
	}

	v = (struct __socket_data *)(v_buff->data + v_buff->len);
	__builtin_memset(v, 0, offsetof(typeof(struct __socket_data), data));
	v->socket_id = uid;
	v->tgid = (__u32) (pid_tgid >> 32);
	v->pid = (__u32) pid_tgid;
	v->timestamp = bpf_ktime_get_ns();
	v->source = DATA_SOURCE_CLOSE;
	v->syscall_len = 0;
	v->data_seq = seq;
	v->msg_type = MSG_COMMON;
	bpf_get_current_comm(v->comm, sizeof(v->comm));

#if !defined(LINUX_VER_KFUNC) && !defined(LINUX_VER_5_2_PLUS)
	struct tail_calls_context *context =
	    (struct tail_calls_context *)v->data;
	context->max_size_limit = data_max_sz;
	context->vecs = false;
	context->is_close = true;
	context->dir = T_INGRESS;

	bpf_tail_call(ctx, &NAME(progs_jmp_tp_map), PROG_OUTPUT_DATA_TP_IDX);
#else
	__output_data_common(ctx, tracer_ctx, v_buff, NULL, T_INGRESS,
			     false, data_max_sz, true, 0);
#endif
}

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KPROG(__close_fd) (struct pt_regs* ctx) {
	int fd = (int)PT_REGS_PARM2(ctx);
#else
// /sys/kernel/debug/tracing/events/syscalls/sys_enter_close/format
TP_SYSCALL_PROG(enter_close) (struct syscall_comm_enter_ctx * ctx) {
	int fd = ctx->fd;
#endif /* NO_FTRACE_SYSCALL */
#else
#if defined(__x86_64__)
//asmlinkage long __x64_sys_close(const struct pt_regs *regs) {
//    unsigned int fd = regs->di;
KFUNC_PROG(__x64_sys_close, const struct pt_regs *regs)
{
#else
//asmlinkage long __arm64_sys_close(const struct pt_regs *regs) {
//    unsigned int fd = regs->regs[0];
KFUNC_PROG(__arm64_sys_close, const struct pt_regs *regs)
{
#endif
	int fd = (int)PT_REGS_PARM1(regs);
#endif
	//Ignore stdin, stdout and stderr
	if (fd <= 2)
		return 0;

	__u32 k0 = 0;
	struct member_fields_offset *offset = members_offset__lookup(&k0);
	if (!offset)
		return 0;

	INFER_OFFSET_PHASE_2(fd);

	__u64 id = bpf_get_current_pid_tgid();
	__u64 conn_key = gen_conn_key_id(id >> 32, (__u64) fd);
	struct socket_info_s *socket_info_ptr =
	    socket_info_map__lookup(&conn_key);
	if (socket_info_ptr == NULL) {
		socket_role_map__delete(&conn_key);
		return 0;
	}

	if (socket_info_ptr->uid)
		__sync_fetch_and_add(&socket_info_ptr->seq, 1);
	delete_socket_info(conn_key, socket_info_ptr);
	__push_close_event(id, socket_info_ptr->uid, socket_info_ptr->seq,
			   offset, ctx);
	return 0;
}

//int __sys_socket(int family, int type, int protocol)
// /sys/kernel/debug/tracing/events/syscalls/sys_exit_socket/format
#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(__sys_socket) (struct pt_regs* ctx) {
	__u64 fd = (__u64)PT_REGS_RC(ctx);
#else
TP_SYSCALL_PROG(exit_socket) (struct syscall_comm_exit_ctx * ctx) {
	__u64 fd = (__u64) ctx->ret;
#endif /* NO_FTRACE_SYSCALL */
#else
KRETFUNC_PROG(__sys_socket, int family, int type, int protocol, int ret)
{
	__u64 fd = (__u64) ret;
#endif
	__u64 id = bpf_get_current_pid_tgid();
	char comm[TASK_COMM_LEN];
	bpf_get_current_comm(comm, sizeof(comm));

	// Used in NGINX load balancing scenarios.
	if (!(comm[0] == 'n' && comm[1] == 'g' && comm[2] == 'i' &&
	      comm[3] == 'n' && comm[4] == 'x' && comm[5] == '\0'))
		return 0;

	// nginx is not a go process, disable go tracking
	struct trace_key_t key = get_trace_key(0, true);
	struct trace_info_t *trace = trace_map__lookup(&key);
	if (trace && trace->peer_fd != 0 && trace->peer_fd != (__u32) fd) {
		struct socket_info_s sk_info = { 0 };
		/*
		 * In the NGINX backend socket information, record 'peer_fd' with
		 * the value of the frontend fd, and 'trace_id' with the value of
		 * the frontend request’s 'trace_id'. The purpose of this is to
		 * ensure that the traceID of frontend requests and backend requests,
		 * as well as the traceID of frontend responses and backend responses,
		 * remain consistent.
		 */
		sk_info.peer_fd = trace->peer_fd;
		sk_info.trace_id = trace->thread_trace_id;
		__u64 conn_key = gen_conn_key_id(id >> 32, fd);
		int ret = socket_info_map__update(&conn_key, &sk_info);
		__u32 k0 = 0;
		struct trace_stats *trace_stats = trace_stats_map__lookup(&k0);
		if (trace_stats == NULL)
			return 0;
		if (ret == 0) {
			__sync_fetch_and_add(&trace_stats->socket_map_count, 1);
		}
	}

	return 0;
}

/*
 * Since the system calls `accept4` and `accept` both invoke `__sys_accept4()`, the
 * `kfunc` type should directly use `__sys_accept4()`.
 */
#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KRETPROG(__sys_accept4) (struct pt_regs* ctx) {
	int sockfd = PT_REGS_RC(ctx);
#else
TP_SYSCALL_PROG(exit_accept) (struct syscall_comm_exit_ctx * ctx) {
	int sockfd = ctx->ret;
#endif /* NO_FTRACE_SYSCALL */
#else
//int __sys_accept4(int fd, struct sockaddr __user *upeer_sockaddr,
//                  int __user *upeer_addrlen, int flags)
KRETFUNC_PROG(__sys_accept4, int fd, struct sockaddr __user * upeer_sockaddr,
	      int __user * upeer_addrlen, int flags, int ret)
{
	int sockfd = ret;
#endif
	__u64 pid_tgid = bpf_get_current_pid_tgid();
	__u32 tgid = (__u32) (pid_tgid >> 32);
	__u64 conn_key = gen_conn_key_id((__u64) tgid, (__u64) sockfd);
	__u32 role = ROLE_SERVER;
	socket_role_map__update(&conn_key, &role);
	return 0;
}

#if !defined(LINUX_VER_KFUNC) && !defined(NO_FTRACE_SYSCALL)
TP_SYSCALL_PROG(exit_accept4) (struct syscall_comm_exit_ctx * ctx) {
	int sockfd = ctx->ret;
	__u64 pid_tgid = bpf_get_current_pid_tgid();
	__u32 tgid = (__u32) (pid_tgid >> 32);
	__u64 conn_key = gen_conn_key_id((__u64) tgid, (__u64) sockfd);
	__u32 role = ROLE_SERVER;
	socket_role_map__update(&conn_key, &role);
	return 0;
}
#endif

#ifndef LINUX_VER_KFUNC
#ifdef NO_FTRACE_SYSCALL
KPROG(__sys_connect) (struct pt_regs* ctx) {
	int sockfd = (int)PT_REGS_PARM1(ctx);
#else
TP_SYSCALL_PROG(enter_connect) (struct syscall_comm_enter_ctx * ctx) {
	int sockfd = ctx->fd;
#endif /* NO_FTRACE_SYSCALL */
#else
// int __sys_connect(int fd, struct sockaddr __user *uservaddr, int addrlen)
KFUNC_PROG(__sys_connect, int fd, struct sockaddr __user * uservaddr,
	   int addrlen)
{
	int sockfd = (int)fd;
#endif
	__u64 pid_tgid = bpf_get_current_pid_tgid();
	__u32 tgid = (__u32) (pid_tgid >> 32);
	__u64 conn_key = gen_conn_key_id((__u64) tgid, (__u64) sockfd);
	__u32 role = ROLE_CLIENT;
	socket_role_map__update(&conn_key, &role);
	return 0;
}

// Store IO event information
MAP_PERARRAY(io_event_buffer, __u32, struct __io_event_buffer, 1,
	     FEATURE_FLAG_SOCKET_TRACER)

static __inline int finalize_data_output(void *ctx,
					 struct tracer_ctx_s *tracer_ctx,
					 __u64 curr_time, __u64 diff,
					 struct __socket_data_buffer *v_buff)
{
	__u32 buf_size =
	    (v_buff->len + offsetof(typeof(struct __socket_data_buffer), data))
	    & (sizeof(*v_buff) - 1);

	/*
	 * Note that when 'buf_size == 0', it indicates that the data being
	 * sent is at its maximum value (sizeof(*v_buff)), and it should
	 * be sent accordingly.
	 */
	if (buf_size < sizeof(*v_buff) && buf_size > 0) {
		/*
		 * Use 'buf_size + 1' instead of 'buf_size' to circumvent
		 * (Linux 4.14.x) length checks.
		 */
		bpf_perf_event_output(ctx, &NAME(socket_data),
				      BPF_F_CURRENT_CPU, v_buff, buf_size + 1);
	} else {
		bpf_perf_event_output(ctx, &NAME(socket_data),
				      BPF_F_CURRENT_CPU, v_buff,
				      sizeof(*v_buff));
	}

	v_buff->events_num = 0;
	v_buff->len = 0;
	if (diff > PERIODIC_PUSH_DELAY_THRESHOLD_NS) {
		tracer_ctx->last_period_timestamp =
		    tracer_ctx->period_timestamp;
		tracer_ctx->period_timestamp = curr_time;
	}

	return 0;
}

static __inline int output_data_copy(const struct data_args_t *args,
				     bool vecs,
				     struct __socket_data_buffer *v_buff,
				     struct __socket_data *v, int max_size,
				     __u32 reassembly_bytes, char *buffer)
{
	__u32 __len = v->syscall_len > max_size ? max_size : v->syscall_len;

	/*
	 * If data reassembly is enabled, the amount of data pushed must not
	 * exceed the reassembly transmission limit.
	 */
	if (reassembly_bytes > 0)
		__len = reassembly_bytes;

	/*
	 * the bitwise AND operation will set the range of possible values for
	 * the UNKNOWN_VALUE register to [0, BUFSIZE)
	 */
	__u32 len = __len & (sizeof(v->data) - 1);

	if (vecs) {
		len = iovecs_copy(v, v_buff, args, __len, len);
		return len;
	}

	if (__len >= sizeof(v->data)) {
		if (v->source != DATA_SOURCE_IO_EVENT) {
			if (unlikely
			    (bpf_probe_read_user
			     (v->data, sizeof(v->data), buffer) != 0))
				return -1;
		} else {
			if (unlikely
			    (bpf_probe_read_kernel
			     (v->data, sizeof(v->data), buffer) != 0))
				return -1;
		}

		len = sizeof(v->data);
	} else {
		/*
		 * https://elixir.bootlin.com/linux/v4.14/source/kernel/bpf/verifier.c#812
		 * __check_map_access() 触发条件检查（size <= 0）
		 * ```
		 *     if (off < 0 || size <= 0 || off + size > map->value_size)
		 * ```
		 * "invalid access to map value, value_size=10888 off=135 size=0"
		 * 使用'len + 1'代替'len'，来规避（Linux 4.14.x）这个检查。
		 */
		if (v->source != DATA_SOURCE_IO_EVENT) {
			if (unlikely(bpf_probe_read_user(v->data,
							 len + 1, buffer) != 0))
				return -1;
		} else {
			if (unlikely(bpf_probe_read_kernel(v->data,
							   len + 1,
							   buffer) != 0))
				return -1;
		}
	}

	return len;
}

#if defined(LINUX_VER_KFUNC) || defined(LINUX_VER_5_2_PLUS)
static __inline int __output_data_common(void *ctx,
					 struct tracer_ctx_s *tracer_ctx,
					 struct __socket_data_buffer *v_buff,
					 const struct data_args_t *args,
					 enum traffic_direction dir, bool vecs,
					 int max_size, bool is_close,
					 __u32 reassembly_bytes)
{
	__u32 k0 = 0;
	char *buffer = NULL;

	if (!v_buff)
		goto exit;

	if ((v_buff->len + offsetof(typeof(struct __socket_data), data)) >
	    sizeof(v_buff->data)) {
		goto exit;
	}

	struct __socket_data *v =
	    (struct __socket_data *)(v_buff->data + v_buff->len);
	if (v_buff->len > (sizeof(v_buff->data) - sizeof(*v)))
		goto exit;

	if (is_close) {
		v->data_len = 0;
		goto skip_copy;
	}

	if (args == NULL)
		goto exit;

	if (v->source == DATA_SOURCE_IO_EVENT) {
		buffer = (char *)io_event_buffer__lookup(&k0);
		if (buffer == NULL) {
			goto exit;
		}
	} else {
		buffer = (char *)args->buf;
	}

	int copy_bytes = output_data_copy(args, vecs, v_buff, v, max_size,
					  reassembly_bytes, buffer);
	if (copy_bytes < 0)
		goto exit;

	v->data_len = copy_bytes;

skip_copy:
	v_buff->len +=
	    offsetof(typeof(struct __socket_data), data) + v->data_len;
	v_buff->events_num++;
	/*
	 * If the delay of the periodic push event exceeds the threshold, it
	 * will be pushed immediately.
	 */
	__u64 curr_time = bpf_ktime_get_ns();
	__u64 diff = curr_time - tracer_ctx->last_period_timestamp;
	if (diff > PERIODIC_PUSH_DELAY_THRESHOLD_NS ||
	    v_buff->events_num >= MAX_EVENTS_BURST ||
	    ((sizeof(v_buff->data) - v_buff->len) < sizeof(*v))) {
		finalize_data_output(ctx, tracer_ctx, curr_time, diff, v_buff);
	}

exit:
	__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, -1);
	return 0;
}
#endif

/*
 * This eBPF program is specially used to transmit data to the agent. The purpose
 * of this is to solve the problem that the number of instructions exceeds the limit.
 */
static __inline int output_data_common(void *ctx)
{
	__u64 id = bpf_get_current_pid_tgid();
	enum traffic_direction dir;
	bool vecs = false;
	int max_size = 0;
	bool is_close = false;
	__u32 k0 = 0;
	char *buffer = NULL;
	__u32 reassembly_bytes = 0;

	struct tracer_ctx_s *tracer_ctx = tracer_ctx_map__lookup(&k0);
	if (tracer_ctx == NULL)
		return 0;

	struct __socket_data_buffer *v_buff =
	    bpf_map_lookup_elem(&NAME(data_buf), &k0);
	if (!v_buff)
		goto clear_args_map_2;

	struct tail_calls_context *context =
	    (struct tail_calls_context *)(v_buff->data + v_buff->len +
					  offsetof(typeof(struct __socket_data),
						   data));

	if ((v_buff->len + offsetof(typeof(struct __socket_data), data) +
	     sizeof(struct tail_calls_context)) > sizeof(v_buff->data)) {
		goto clear_args_map_2;
	}

	dir = context->dir;
	vecs = context->vecs;
	is_close = context->is_close;
	max_size = context->max_size_limit;
	reassembly_bytes = context->push_reassembly_bytes;

	struct data_args_t *args;
	if (dir == T_INGRESS)
		args = active_read_args_map__lookup(&id);
	else
		args = active_write_args_map__lookup(&id);

	struct __socket_data *v =
	    (struct __socket_data *)(v_buff->data + v_buff->len);
	if (v_buff->len > (sizeof(v_buff->data) - sizeof(*v)))
		goto clear_args_map_1;

	if (is_close) {
		v->data_len = 0;
		goto skip_copy;
	}

	if (args == NULL)
		goto clear_args_map_1;

	if (v->source == DATA_SOURCE_IO_EVENT) {
		buffer = (char *)io_event_buffer__lookup(&k0);
		if (buffer == NULL) {
			goto clear_args_map_1;
		}
	} else {
		buffer = (char *)args->buf;
	}

	int copy_bytes =
	    output_data_copy(args, vecs, v_buff, v, max_size, reassembly_bytes,
			     buffer);
	if (copy_bytes < 0)
		goto clear_args_map_1;

	v->data_len = copy_bytes;

skip_copy:
	v_buff->len +=
	    offsetof(typeof(struct __socket_data), data) + v->data_len;
	v_buff->events_num++;

	/*
	 * If the delay of the periodic push event exceeds the threshold, it
	 * will be pushed immediately.
	 */
	__u64 curr_time = bpf_ktime_get_ns();
	__u64 diff = curr_time - tracer_ctx->last_period_timestamp;
	if (diff > PERIODIC_PUSH_DELAY_THRESHOLD_NS ||
	    v_buff->events_num >= MAX_EVENTS_BURST ||
	    ((sizeof(v_buff->data) - v_buff->len) < sizeof(*v))) {
		finalize_data_output(ctx, tracer_ctx, curr_time, diff, v_buff);
	}

clear_args_map_1:
	__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, -1);
	if (dir == T_INGRESS)
		active_read_args_map__delete(&id);
	else
		active_write_args_map__delete(&id);

	return 0;

clear_args_map_2:
	__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, -1);
	active_read_args_map__delete(&id);
	active_write_args_map__delete(&id);
	return 0;
}

PROGTP(output_data) (void *ctx) {
	return output_data_common(ctx);
}

PROGKP(output_data) (void *ctx) {
	return output_data_common(ctx);
}

static __inline int data_submit(void *ctx)
{
	int ret = 0;
	__u32 k0 = 0;
	struct ctx_info_s *ctx_map = bpf_map_lookup_elem(&NAME(ctx_info), &k0);
	if (!ctx_map)
		return SUBMIT_ABORT;

	__u64 id = bpf_get_current_pid_tgid();
	struct conn_info_s *conn_info;
	struct conn_info_s __conn_info = ctx_map->tail_call.conn_info;
	conn_info = &__conn_info;
	__u64 conn_key = gen_conn_key_id(id >> 32, (__u64) conn_info->fd);
	conn_info->socket_info_ptr = socket_info_map__lookup(&conn_key);
	if (!conn_info->is_reasm_seg && conn_info->socket_info_ptr)
		conn_info->socket_info_ptr->finish_reasm = false;

	struct data_args_t *args;
	if (conn_info->direction == T_INGRESS)
		args = active_read_args_map__lookup(&id);
	else
		args = active_write_args_map__lookup(&id);

	if (args == NULL)
		return SUBMIT_ABORT;

	const bool vecs = ctx_map->tail_call.extra.vecs;
	__u32 bytes_count = ctx_map->tail_call.bytes_count;
	struct member_fields_offset *offset = ctx_map->tail_call.offset;
	__u64 enter_ts = args->enter_ts;
	const struct process_data_extra extra = ctx_map->tail_call.extra;

	ret = __data_submit(ctx, conn_info, args, vecs, bytes_count,
			    offset, enter_ts, &extra);

	return ret;
}

static __inline int __proto_infer_2(void *ctx)
{
	__u64 id = bpf_get_current_pid_tgid();
	__u32 k0 = 0;
	struct ctx_info_s *ctx_map = bpf_map_lookup_elem(&NAME(ctx_info), &k0);
	if (!ctx_map)
		goto clear_args_map_2;

	enum traffic_direction dir;
	dir = ctx_map->tail_call.dir;
	/*
	 * Use the following method to obtain `conn_info`, otherwise an error
	 * similar to "R1 invalid mem access 'inv'" will appear during the eBPF
	 * loading process.
	 */
	struct conn_info_s *conn_info, __conn_info;
	__conn_info = ctx_map->tail_call.conn_info;
	conn_info = &__conn_info;
	__u64 conn_key = gen_conn_key_id(id >> 32, (__u64) conn_info->fd);
	conn_info->socket_info_ptr = socket_info_map__lookup(&conn_key);
	int act;
	act = infer_l7_class_2(&ctx_map->tail_call, conn_info);
	if (act != INFER_FINISH) {
		/*
		 * Ignore the IO event here because it has been
		 * confirmed before protocol inference (checking
		 * whether the file type is socket).
		 */
		goto clear_args_map_1;
	}
	// When at least one of protocol or message_type is valid,
	// data_submit can be performed, otherwise MySQL data may be lost
	if (conn_info->protocol != PROTO_UNKNOWN ||
	    conn_info->message_type != MSG_UNKNOWN) {
		ctx_map->tail_call.conn_info = __conn_info;
		return 0;
	}

clear_args_map_1:
	if (dir == T_INGRESS)
		active_read_args_map__delete(&id);
	else
		active_write_args_map__delete(&id);
	return -1;

clear_args_map_2:
	active_read_args_map__delete(&id);
	active_write_args_map__delete(&id);
	return -1;
}

PROGTP(proto_infer_2) (void *ctx) {
	if (__proto_infer_2(ctx) == 0)
		bpf_tail_call(ctx, &NAME(progs_jmp_tp_map),
			      PROG_DATA_SUBMIT_TP_IDX);
	return 0;
}

PROGKP(proto_infer_2) (void *ctx) {
	if (__proto_infer_2(ctx) == 0)
		bpf_tail_call(ctx, &NAME(progs_jmp_kp_map),
			      PROG_DATA_SUBMIT_KP_IDX);
	return 0;
}

PROGTP(data_submit) (void *ctx) {
	int ret;
	ret = data_submit(ctx);
	if (ret == SUBMIT_OK) {
		bpf_tail_call(ctx, &NAME(progs_jmp_tp_map),
			      PROG_OUTPUT_DATA_TP_IDX);
	} else if (ret == SUBMIT_ABORT) {
		return 0;
	} else {
		bpf_tail_call(ctx, &NAME(progs_jmp_tp_map),
			      PROG_IO_EVENT_TP_IDX);
	}

	return 0;
}

PROGKP(data_submit) (void *ctx) {
	int ret;
	ret = data_submit(ctx);
	if (ret == SUBMIT_OK) {
		bpf_tail_call(ctx, &NAME(progs_jmp_kp_map),
			      PROG_OUTPUT_DATA_KP_IDX);
	} else if (ret == SUBMIT_ABORT) {
		return 0;
	} else {
		__u64 id = bpf_get_current_pid_tgid();
		active_read_args_map__delete(&id);
		active_write_args_map__delete(&id);
	}

	return 0;
}

static __inline bool is_regular_file(int fd,
				     struct member_fields_offset *off_ptr)
{
	struct member_fields_offset *offset = off_ptr;
	if (offset == NULL) {
		__u32 k0 = 0;
		offset = members_offset__lookup(&k0);
	}
	void *file = fd_to_file(fd, offset);
	__u32 i_mode = file_to_i_mode(file, offset);
	return S_ISREG(i_mode);
}

static __inline void set_file_metric_data(struct __io_event_buffer *buffer,
					   int fd,
					   struct member_fields_offset *off_ptr)
{
#define MAX_DIRECTORY_DEPTH 20

	struct member_fields_offset *offset = off_ptr;
	if (offset == NULL) {
		__u32 k0 = 0;
		offset = members_offset__lookup(&k0);
	}

	void *file = fd_to_file(fd, offset);
	if (file == NULL)
		return;

	bpf_probe_read_kernel(&buffer->offset, sizeof(buffer->offset),
			      file + offset->struct_file_f_pos_offset);

	void *dentry = NULL, *parent;
	bpf_probe_read_kernel(&dentry, sizeof(dentry),
			      file + offset->struct_file_dentry_offset);
	buffer->len = 0;
#pragma unroll
	for (int i = 0; i < MAX_DIRECTORY_DEPTH; i++) {
		char *name = NULL;
		bpf_probe_read_kernel(&name, sizeof(name),
				      dentry +
				      offset->struct_dentry_name_offset);
		struct __dentry_name *d_n =
		    (struct __dentry_name *)(buffer->filename + buffer->len);
		if (buffer->len + sizeof(d_n->name) > sizeof(buffer->filename))
			break;
		buffer->len +=
		    bpf_probe_read_kernel_str(d_n->name, sizeof(d_n->name), name);

		bpf_probe_read_kernel(&parent, sizeof(parent),
                              dentry + offset->struct_dentry_d_parent_offset);

		if (parent == dentry || parent == NULL)
			break;
		dentry = parent;
	}
}

static __inline int trace_io_event_common(void *ctx,
					  struct member_fields_offset *offset,
					  struct data_args_t *data_args,
					  enum traffic_direction direction,
					  __u64 pid_tgid)
{
	__u64 latency = 0;
	__u64 trace_id = 0;
	__u32 k0 = 0;
	__u32 tgid = pid_tgid >> 32;

	if (data_args->bytes_count <= 0) {
		return -1;
	}

	struct tracer_ctx_s *tracer_ctx = tracer_ctx_map__lookup(&k0);
	if (tracer_ctx == NULL) {
		return -1;
	}

	if (tracer_ctx->io_event_collect_mode == 0) {
		return -1;
	}

	__u32 timeout = tracer_ctx->go_tracing_timeout;
	struct trace_key_t trace_key = get_trace_key(timeout, false);
	struct trace_info_t *trace_info_ptr = trace_map__lookup(&trace_key);
	if (trace_info_ptr) {
		trace_id = trace_info_ptr->thread_trace_id;
	}

	if (trace_id == 0 && tracer_ctx->io_event_collect_mode == 1) {
		return -1;
	}

	int data_max_sz = tracer_ctx->data_limit_max;

	if (!is_regular_file(data_args->fd, offset)) {
		return -1;
	}

	latency = bpf_ktime_get_ns() - data_args->enter_ts;
	if (latency < tracer_ctx->io_event_minimal_duration) {
		return -1;
	}

	struct __io_event_buffer *buffer = io_event_buffer__lookup(&k0);
	if (!buffer) {
		return -1;
	}

	buffer->bytes_count = data_args->bytes_count;
	buffer->latency = latency;
	buffer->operation = direction;
	set_file_metric_data(buffer, data_args->fd, offset);
	struct __socket_data_buffer *v_buff =
	    bpf_map_lookup_elem(&NAME(data_buf), &k0);
	if (!v_buff)
		return -1;

	__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, 1);
	struct __socket_data *v = (struct __socket_data *)&v_buff->data[0];

	if (v_buff->len > (sizeof(v_buff->data) - sizeof(*v))) {
		__sync_fetch_and_add(&tracer_ctx->push_buffer_refcnt, -1);
		return -1;
	}

	v = (struct __socket_data *)(v_buff->data + v_buff->len);
	__builtin_memset(v, 0, offsetof(typeof(struct __socket_data), data));
	v->tgid = tgid;
	v->pid = (__u32) pid_tgid;
	v->coroutine_id = trace_key.goid;
	v->timestamp = data_args->enter_ts;

	v->syscall_len = sizeof(*buffer);

	v->source = DATA_SOURCE_IO_EVENT;

	v->thread_trace_id = trace_id;
	v->msg_type = MSG_COMMON;
	bpf_get_current_comm(v->comm, sizeof(v->comm));
#if !defined(LINUX_VER_KFUNC) && !defined(LINUX_VER_5_2_PLUS)
	struct tail_calls_context *context =
	    (struct tail_calls_context *)v->data;
	context->max_size_limit = data_max_sz;
	context->push_reassembly_bytes = 0;
	context->vecs = false;
	context->is_close = false;
	context->dir = direction;

	bpf_tail_call(ctx, &NAME(progs_jmp_tp_map), PROG_OUTPUT_DATA_TP_IDX);
	return 0;
#else
	return __output_data_common(ctx, tracer_ctx, v_buff, data_args,
				    direction, false, data_max_sz, false, 0);
#endif
}

PROGTP(io_event) (void *ctx) {
	__u64 id = bpf_get_current_pid_tgid();

	struct data_args_t *data_args = NULL;

	data_args = active_read_args_map__lookup(&id);
	if (data_args) {
		trace_io_event_common(ctx, NULL, data_args, T_INGRESS, id);
		active_read_args_map__delete(&id);
		return 0;
	}

	data_args = active_write_args_map__lookup(&id);
	if (data_args) {
		trace_io_event_common(ctx, NULL, data_args, T_EGRESS, id);
		active_write_args_map__delete(&id);
		return 0;
	}

	return 0;
}

static __inline int push_socket_data(struct syscall_comm_enter_ctx *ctx)
{
	__u32 k0 = 0;
	struct tracer_ctx_s *tracer_ctx = tracer_ctx_map__lookup(&k0);
	if (tracer_ctx == NULL)
		return 0;

	struct trace_stats *trace_stats = trace_stats_map__lookup(&k0);
	if (trace_stats == NULL)
		return 0;

	/*
	 * Monitor the maximum and average delay time of periodic push events.
	 */
	tracer_ctx->last_period_timestamp = tracer_ctx->period_timestamp;
	tracer_ctx->period_timestamp = bpf_ktime_get_ns();
	__u64 diff = tracer_ctx->period_timestamp -
	    tracer_ctx->last_period_timestamp;

	__sync_fetch_and_add(&trace_stats->period_event_total_time, diff);
	__sync_fetch_and_add(&trace_stats->period_event_count, 1);

	/*
	 * If a previous system call is in the process of modifying the push buffer to
	 * push data when it is interrupted by a periodic event interrupt, the interrupt
	 * handler cannot further manipulate the buffer to avoid conflicts. In such cases,
	 * we record the number of conflicts.
	 */
	if (tracer_ctx->push_buffer_refcnt != 0) {
		__sync_fetch_and_add(&trace_stats->push_conflict_count, 1);
		return 0;
	}

	struct __socket_data_buffer *v_buff =
	    bpf_map_lookup_elem(&NAME(data_buf), &k0);
	if (v_buff) {
		if (v_buff->events_num > 0) {
			__u32 buf_size =
			    (v_buff->len +
			     offsetof(typeof(struct __socket_data_buffer),
				      data))
			    & (sizeof(*v_buff) - 1);
			/* 
			 * Note that when 'buf_size == 0', it indicates that the data being
			 * sent is at its maximum value (sizeof(*v_buff)), and it should
			 * be sent accordingly.
			 */
			if (buf_size < sizeof(*v_buff) && buf_size > 0) {
				/* 
				 * Use 'buf_size + 1' instead of 'buf_size' to circumvent
				 * (Linux 4.14.x) length checks.
				 */
				bpf_perf_event_output(ctx,
						      &NAME
						      (socket_data),
						      BPF_F_CURRENT_CPU,
						      v_buff, buf_size + 1);
			} else {
				bpf_perf_event_output(ctx,
						      &NAME
						      (socket_data),
						      BPF_F_CURRENT_CPU,
						      v_buff, sizeof(*v_buff));
			}

			v_buff->events_num = 0;
			v_buff->len = 0;
			if (diff > MAX_PUSH_DELAY_TIME_NS) {
				// Indicates that a delay occurred in this data push.
				__sync_fetch_and_add(&trace_stats->period_event_max_delay, 1);
			}

		}
	}

	return 0;
}

// /sys/kernel/debug/tracing/events/syscalls/sys_enter_getppid
// Here, the tracepoint is used to periodically send the data residing in the cache but not
// yet transmitted to the user-level receiving program for processing.
#ifdef NO_FTRACE_SYSCALL
KPROG(sys_getppid) (struct pt_regs* ctx) {
#else
TP_SYSCALL_PROG(enter_getppid) (struct syscall_comm_enter_ctx * ctx) {
#endif /* NO_FTRACE_SYSCALL */
	// Only pre-specified Pid is allowed to trigger.
	if (!check_pid_validity())
		return 0;

	return push_socket_data(ctx);
}

//Refer to the eBPF programs here
#include "go_tls.bpf.c"
#include "go_http2.bpf.c"
#include "openssl.bpf.c"
