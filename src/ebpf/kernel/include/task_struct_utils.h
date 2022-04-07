#ifndef __TASK_STRUCT_UTILS_H__
#define __TASK_STRUCT_UTILS_H__

#ifndef BPF_USE_CORE
#include <linux/sched.h>
#include <math.h>
#endif

#include "utils.h"

#define NSEC_PER_SEC	1000000000L
#define USER_HZ		100

static __inline void *retry_get_socket_file_addr(struct task_struct *task,
						 int fd_num, int files_off)
{
	void *file = NULL;
	void *files, *files_ptr = (void *)task + files_off;
	bpf_probe_read(&files, sizeof(files), files_ptr);

	if (files == NULL)
		return NULL;

	struct fdtable *fdt, __fdt;
	bpf_probe_read(&fdt, sizeof(fdt),
		       files + STRUCT_FILES_STRUCT_FDT_OFFSET);
	bpf_probe_read(&__fdt, sizeof(__fdt), (void *)fdt);
	bpf_probe_read(&file, sizeof(file), __fdt.fd + fd_num);

	return file;
}

#define ARRAY_SIZE(a)    (sizeof(a) / sizeof(a[0]))

static __inline void *infer_and_get_socket_from_fd(int fd_num,
						   struct member_fields_offset
						   *offset, bool debug)
{
	struct task_struct *task = (struct task_struct *)bpf_get_current_task();
	void *file = NULL;
	void *private_data = NULL;
	struct socket *socket;
#ifdef BPF_USE_CORE
	struct file **fd = BPF_CORE_READ(task, files, fdt, fd);
	bpf_probe_read(&file, sizeof(file), fd + fd_num);
#else
	struct socket __socket;
	int i;

	int files_offset_array[17] = {
		0x790, 0xb00, 0xb10, 0xa80, 0xac0, 0xac8,
		0xad8, 0xae0, 0xaf8, 0xb20, 0xb70, 0xb80,
		0xba8, 0xbb0, 0xbb8, 0xbc0, 0xc18
	};

	if (unlikely(!offset->task__files_offset)) {
#pragma unroll
		for (i = 0; i < ARRAY_SIZE(files_offset_array); i++) {
			file =
			    retry_get_socket_file_addr(task, fd_num,
						       files_offset_array[i]);
			if (file) {
				bpf_probe_read(&private_data,
					       sizeof(private_data),
					       file +
					       STRUCT_FILES_PRIVATE_DATA_OFFSET);
				if (private_data != NULL) {
					socket = private_data;
					bpf_probe_read(&__socket,
						       sizeof(__socket),
						       (void *)socket);
					if (__socket.file == file
					    || file == __socket.wq) {
						offset->task__files_offset =
						    files_offset_array[i];
						break;
					}
				}
			}
		}
	} else
		file =
		    retry_get_socket_file_addr(task, fd_num,
					       offset->task__files_offset);
#endif
	if (file == NULL) {
		//bpf_printk("file == NULL\n");
		return NULL;
	}
#ifdef BPF_USE_CORE
	struct file *__file = file;
	private_data = BPF_CORE_READ(__file, private_data);
#else
	bpf_probe_read(&private_data, sizeof(private_data),
		       file + STRUCT_FILES_PRIVATE_DATA_OFFSET);
#endif
	if (private_data == NULL) {
		if (debug)
			bpf_printk("private_data == NULL\n");
		return NULL;
	}

	socket = private_data;
	short socket_type;
	void *check_file;
	void *sk;
#ifdef BPF_USE_CORE
	socket_type = BPF_CORE_READ(socket, type);
	check_file = BPF_CORE_READ(socket, file);
	sk = BPF_CORE_READ(socket, sk);
#else
	bpf_probe_read(&__socket, sizeof(__socket), (void *)socket);
	socket_type = __socket.type;
	if (__socket.file != file) {
		check_file = __socket.wq;	// kernel >= 5.3.0 remove '*wq'
		sk = __socket.file;
	} else {
		check_file = __socket.file;
		sk = __socket.sk;
	}
#endif
	if ((socket_type == SOCK_STREAM || socket_type == SOCK_DGRAM) &&
	    check_file == file /*&& __socket.state == SS_CONNECTED */ ) {
		return sk;
	}

	if (debug)
		bpf_printk
		    (" NULL __socket.type:%d __socket.file == file (%d)\n",
		     socket_type, check_file == file);

	return NULL;
}

static __inline void *get_socket_from_fd(int fd_num,
					 struct member_fields_offset *offset)
{
	struct task_struct *task = (struct task_struct *)bpf_get_current_task();
	void *file = NULL;
#ifdef BPF_USE_CORE
	struct file **fd = BPF_CORE_READ(task, files, fdt, fd);
	bpf_probe_read(&file, sizeof(file), fd + fd_num);
#else
	file =
	    retry_get_socket_file_addr(task, fd_num,
				       offset->task__files_offset);
#endif
	if (file == NULL)
		return NULL;
	void *private_data = NULL;
#ifdef BPF_USE_CORE
	struct file *__file = file;
	private_data = BPF_CORE_READ(__file, private_data);
#else
	bpf_probe_read(&private_data, sizeof(private_data),
		       file + STRUCT_FILES_PRIVATE_DATA_OFFSET);
#endif
	if (private_data == NULL) {
		return NULL;
	}

	struct socket *socket = private_data;
	short socket_type;
	void *check_file;
	void *sk;
#ifdef BPF_USE_CORE
	socket_type = BPF_CORE_READ(socket, type);
	check_file = BPF_CORE_READ(socket, file);
	sk = BPF_CORE_READ(socket, sk);
#else
	struct socket __socket;
	bpf_probe_read(&__socket, sizeof(__socket), (void *)socket);

	socket_type = __socket.type;
	if (__socket.file != file) {
		check_file = __socket.wq;	// kernel >= 5.3.0 remove '*wq'
		sk = __socket.file;
	} else {
		check_file = __socket.file;
		sk = __socket.sk;
	}
#endif
	if ((socket_type == SOCK_STREAM || socket_type == SOCK_DGRAM) &&
	    check_file == file /*&& __socket.state == SS_CONNECTED */ ) {
		return sk;
	}

	return NULL;
}

#endif /* __TASK_STRUCT_UTILS_H__ */
