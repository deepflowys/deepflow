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
#define _GNU_SOURCE
#include <stdio.h>
#include <stdbool.h>
#include <linux/limits.h>	/* ulimit */
#include <sys/resource.h>	/* RLIM_INFINITY */
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <dirent.h>
#include <ctype.h>		/* isdigit() */
#include <linux/types.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <inttypes.h>
#include <linux/version.h>
#include <linux/perf_event.h>
#include <linux/unistd.h>
#include <unistd.h>
#include <dirent.h>
#include <ctype.h>		/* isdigit() */
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <inttypes.h>
#include <net/if.h>
#include <linux/sockios.h>
#include <sys/ioctl.h>
#include <pthread.h>
#include "config.h"
#include "types.h"
#include "clib.h"
#include "list.h"
#include "utils.h"
#include "log.h"
#include "string.h"
#include "profile/java/config.h"

#define MAXLINE 1024

volatile uint64_t sys_boot_time_ns;	// System boot time in nanoseconds
static u64 g_sys_btime_msecs;

bool is_core_kernel(void)
{
	return (access("/sys/kernel/btf/vmlinux", F_OK) == 0);
}

int parse_num_range_disorder(const char *config_str,
			     int bytes_count, bool ** mask)
{
	if (bytes_count <= 0)
		return -1;

	int i, n, len, start, end = -1;
	bool *tmp = malloc(sizeof(bool) * PORT_NUM_MAX);
	if (tmp == NULL) {
		ebpf_warning("malloc() failed.\n");
		return -1;
	}
	memset(tmp, 0, sizeof(bool) * PORT_NUM_MAX);
	*mask = tmp;

	for (i = 0; i < bytes_count; i++) {
		if (config_str[i] == ',' || config_str[i] == '\n' ||
		    config_str[i] == ' ') {
			continue;
		}

		n = sscanf(&config_str[i], "%d%n-%d%n", &start, &len, &end,
			   &len);
		if (n <= 0 || n > 2) {
			goto failed;
		} else if (n == 1) {
			end = start;
		}

		if (start < 0 || start > end) {
			goto failed;
		}

		memset(tmp + start, 1, end - start + 1);
		i += (len - 1);
	}

	return 0;

failed:
	if (*mask != NULL) {
		free(*mask);
		*mask = NULL;
	}

	return -1;
}

int parse_num_range(const char *config_str, int bytes_count,
		    bool ** mask, int *count)
{
	int i, n, len, start, end = -1;
	bool *tmp;
	for (i = 0; i < bytes_count; i++) {
		if (config_str[i] == ',' || config_str[i] == '\n' ||
		    config_str[i] == ' ') {
			continue;
		}

		n = sscanf(&config_str[i], "%d%n-%d%n", &start, &len, &end,
			   &len);
		if (n <= 0 || n > 2) {
			goto failed;
		} else if (n == 1) {
			end = start;
		}

		if (start < 0 || start > end || start <= *count - 1) {
			goto failed;
		}

		tmp = realloc(*mask, end + 1);
		if (!tmp) {
			goto failed;
		}

		*mask = tmp;
		memset(tmp + *count, 0, start - *count);
		memset(tmp + start, 1, end - start + 1);
		*count = end + 1;
		i += (len - 1);
	}

	if (*count == 0) {
		goto failed;
	}

	return 0;
failed:
	ebpf_warning("Number range (\"%s\") error, Please make sure the range "
		     "list is in ascending order and there is no overlap in "
		     "the numbers.\n", config_str);
	if (*mask != NULL) {
		free(*mask);
		*mask = NULL;
	}

	*count = 0;
	return -1;
}

static int parse_online_cpus(const char *cpu_file, bool ** mask, int *cpu_count)
{
	int fd, len;
	char buf[1024];
	if ((fd = open(cpu_file, O_RDONLY | O_CLOEXEC)) < 0) {
		ebpf_warning("Failed to open file (%s: %d)\n", cpu_file, errno);
		return -1;
	}

	len = read(fd, buf, sizeof(buf));
	close(fd);
	if (len <= 0) {
		ebpf_warning("Failed to read file (%s: %d)\n", cpu_file, errno);
		return -1;
	}

	if (len > sizeof(buf)) {
		ebpf_warning("File is too big %s\n", cpu_file);
		return -1;
	}

	return parse_num_range(buf, len, mask, cpu_count);
}

int get_cpus_count(bool ** mask)
{
	bool *online = NULL;
	int err, n = 0;
	const char *online_cpus_file = "/sys/devices/system/cpu/online";

	err = parse_online_cpus(online_cpus_file, &online, &n);
	if (err) {
		ebpf_warning("failed to get online CPU mask: %d\n", err);
		return -1;
	}

	*mask = online;
	return n;
}

int get_num_possible_cpus(void)
{
	bool *mask = NULL;
	int err, n = 0, i, cpus = 0;
	static const char *fcpu = "/sys/devices/system/cpu/possible";

	err = parse_online_cpus(fcpu, &mask, &n);
	if (err) {
		ebpf_warning("failed to get online CPU mask: %d\n", err);
		return -1;
	}

	for (i = 0; i < n; i++) {
		if (mask[i]) {
			cpus++;
		}
	}

	free(mask);
	return cpus;
}

// 系统启动到现在的时间（以秒为单位）
uint32_t get_sys_uptime(void)
{
	struct sysinfo s_info = { 0 };
	if (sysinfo(&s_info) != 0)
		return 0;

	return (uint32_t) s_info.uptime;
}

static void exec_clear_residual_probes(const char *events_file,
				       const char *type_name)
{
	struct probe_elem {
		struct list_head list;
		char event[MAXLINE];
	};

	FILE *fp;
	char line[MAXLINE];
	char *lf;		// 字符 '\n'
	struct list_head probe_head;
	struct probe_elem *pe;

	init_list_head(&probe_head);

	if ((fp = fopen(events_file, "r")) == NULL) {
		ebpf_info("Open config file(\"%s\") failed.\n", events_file);
		return;
	}

	while ((fgets(line, MAXLINE, fp)) != NULL) {
		if ((lf = strchr(line, '\n')))
			*lf = '\0';

		pe = (struct probe_elem *)calloc(sizeof(*pe), 1);
		if (pe == NULL) {
			ebpf_warning("calloc() failed.\n");
			break;
		}
		snprintf(pe->event, sizeof(pe->event), "%s", line);
		list_add_tail(&pe->list, &probe_head);
	}

	fclose(fp);

	char *ptr;
	struct list_head *p, *n;
	char rm_event_cmd[MAXLINE];

	int kfd = open(events_file, O_WRONLY | O_APPEND, 0);

	if (kfd < 0) {
		ebpf_info("open(%s): failed %s\n", events_file,
			  strerror(errno));
		return;
	}

	list_for_each_safe(p, n, &probe_head) {
		pe = container_of(p, struct probe_elem, list);
		// Match the [K/U]probe events of "_deepflow_"
		// 匹配"_deepflow_"的[k/u]probe事件
		if (strstr(pe->event, "_deepflow_")) {
			if ((ptr = strchr(pe->event, '/'))) {
				char *s = ++ptr;
				if ((ptr = strchr(ptr, ' ')))
					*ptr = '\0';
				snprintf(rm_event_cmd, sizeof(rm_event_cmd),
					 "-:%s", s);
				if (write
				    (kfd, rm_event_cmd,
				     strlen(rm_event_cmd)) < 0) {
					if (errno == ENOENT)
						ebpf_info
						    ("clear %s error, probe entry may not exist.(%s)\n",
						     type_name, rm_event_cmd);
					else
						ebpf_info
						    ("cannot clear %s, %s (%s)\n",
						     type_name, strerror(errno),
						     rm_event_cmd);
					close(kfd);
				} else
					ebpf_info
					    ("Clear residual %s event \"%s\" success.\n",
					     type_name, rm_event_cmd);
			}
		}

		list_head_del(&pe->list);
		free(pe);
	}

	close(kfd);
}

void clear_residual_probes(void)
{
	exec_clear_residual_probes(KPROBE_EVENTS_FILE, "kprobe");
	exec_clear_residual_probes(UPROBE_EVENTS_FILE, "uprobe");
}

/* Make sure max locked memory is set to unlimited. */
int max_locked_memory_set_unlimited(void)
{
	int ret;
	struct rlimit rlim;
	errno = 0;
	if ((ret = getrlimit(RLIMIT_MEMLOCK, &rlim)) < 0) {
		ebpf_info("Call getrlimit is error(%d). %s", errno,
			  strerror(errno));
		return -1;
	}

	if (rlim.rlim_cur != RLIM_INFINITY) {
		rlim.rlim_cur = rlim.rlim_max = RLIM_INFINITY;
		if ((ret = setrlimit(RLIMIT_MEMLOCK, &rlim)) < 0) {
			ebpf_info("Call setrlimit is error. error(%d). %s",
				  errno, strerror(errno));
			return -1;
		}
	}

	return ret;
}

int max_rlim_open_files_set(int num)
{
	int ret;
	struct rlimit rlim;
	errno = 0;
	if ((ret = getrlimit(RLIMIT_NOFILE, &rlim)) < 0) {
		ebpf_info("Call getrlimit is error(%d). %s", errno,
			  strerror(errno));
		return -1;
	}

	if (rlim.rlim_cur < num) {
		rlim.rlim_cur = rlim.rlim_max = num;
		if ((ret = setrlimit(RLIMIT_NOFILE, &rlim)) < 0) {
			ebpf_info
			    ("Call setrlimit set RLIMIT_NOFILE is error. error(%d). %s",
			     errno, strerror(errno));
			return -1;
		}
	}

	memset(&rlim, 0, sizeof(rlim));
	getrlimit(RLIMIT_NOFILE, &rlim);
	ebpf_info("RLIMIT_NOFILE cur:%d, rlim_max:%d\n", rlim.rlim_cur,
		  rlim.rlim_max);

	return ret;
}

static int fs_write(const char *file_name, char *v, int mode, int len)
{
	int fd, err = 0;

	fd = open(file_name, mode);
	if (fd < 0) {
		ebpf_warning("Open debug file(\"%s\") open failed.\n",
			     file_name);
		return -1;
	}

	if ((err = write(fd, v, len)) < 0)
		ebpf_warning("Write %s to file \"%s\" failed.\n", v, file_name);

	close(fd);
	return err;
}

int sysfs_write(const char *file_name, char *v)
{
	return fs_write(file_name, v, O_WRONLY, 1);
}

static int fs_read(const char *file_name, char *v, int mode, int len)
{
	int fd, err = 0;

	fd = open(file_name, mode);
	if (fd < 0) {
		ebpf_warning("Open debug file(\"%s\") open failed.\n",
			     file_name);
		return -1;
	}

	if ((err = read(fd, v, len)) < 0)
		ebpf_warning("Read %s to file \"%s\" failed.\n", v, file_name);

	close(fd);
	return err;
}

int sysfs_read_num(const char *file_name)
{
	int ret;
	char buf[64];
	memset(buf, 0, sizeof(buf));
	ret = fs_read(file_name, (char *)buf, O_RDONLY, 1);
	if (ret > 0) {
		return atoi(buf);
	}

	return ETR_INVAL;
}

uint64_t gettime(clockid_t clk_id, int flag)
{
	struct timespec t;
	int res;
	uint64_t time = 0;
	res = clock_gettime(clk_id, &t);
	if (res < 0) {
		return 0;
	}

	if (flag == TIME_TYPE_NAN)
		time = (uint64_t) t.tv_sec * NS_IN_SEC + t.tv_nsec;
	else if (flag == TIME_TYPE_SEC)
		time = (uint64_t) t.tv_sec;

	return time;
}

/*
 * Get system boot start time (in milliseconds).
 */
u64 get_sys_btime_msecs(void)
{
	if (g_sys_btime_msecs > 0)
		goto done;

	char buff[4096];

	FILE *fp = fopen("/proc/stat", "r");
	ASSERT(fp != NULL);

	u64 sys_boot = 0;
	while (fgets(buff, sizeof(buff), fp) != NULL) {
		if (sscanf(buff, "btime %lu", &sys_boot) == 1)
			break;
	}

	fclose(fp);
	ASSERT(sys_boot > 0);

	if (g_sys_btime_msecs == 0)
		g_sys_btime_msecs = sys_boot * 1000UL;

done:
	return g_sys_btime_msecs;
}

/*
 * Get the start time (in milliseconds) of a given PID.
 */
u64 get_process_starttime(pid_t pid)
{
	char file[PATH_MAX], buff[4096];
	int fd;
	unsigned long long etime_ticks = 0;

	snprintf(file, sizeof(file), "/proc/%d/stat", pid);
	if (access(file, F_OK))
		return 0;

	fd = open(file, O_RDONLY);
	if (fd <= 2)
		return 0;

	read(fd, buff, sizeof(buff));
	close(fd);

	if (sscanf(buff, "%*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s"
		   " %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %llu ",
		   &etime_ticks) != 1) {
		return 0;
	}

	u64 sys_boot = get_sys_btime_msecs();
	u64 msecs_per_tick = 1000UL / sysconf(_SC_CLK_TCK);

	return ((etime_ticks * msecs_per_tick) + sys_boot);
}

u64 current_sys_time_secs(void)
{
	if (sys_boot_time_ns)
		return ((sys_boot_time_ns / NS_IN_SEC) +
			gettime(CLOCK_MONOTONIC, TIME_TYPE_SEC));
	else
		return (get_sys_uptime() + (get_sys_btime_msecs() / 1000));
}

/*
 * Get the start time (in milliseconds) of a given PID,
 * and fetch process comm.
 *
 * @pid processID
 * @comm_base store process name address
 * @len store process name max length
 *
 * @return process start time,
 * 	   if is 0, it indicates that an error has been encountered.
 */
u64 get_process_starttime_and_comm(pid_t pid, char *name_base, int len)
{
	char file[PATH_MAX], buff[4096];
	int fd;
	unsigned long long etime_ticks = 0;

	snprintf(file, sizeof(file), "/proc/%d/stat", pid);
	if (access(file, F_OK)) {
		ebpf_debug("file %s is not exited\n", file);
		return 0;
	}

	fd = open(file, O_RDONLY);
	if (fd <= 2) {
		ebpf_warning("open %s failed with %s(%d)\n", file,
			     strerror(errno), errno);
		return 0;
	}

	read(fd, buff, sizeof(buff));
	close(fd);

	char *start = NULL;	// process name start address;
	if (sscanf(buff, "%*s %ms %*s %*s %*s %*s %*s %*s %*s %*s %*s"
		   " %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %llu ",
		   &start, &etime_ticks) != 2) {
		ebpf_warning("sscanf() failed. pid %d buff %s\n", pid, buff);
		return 0;
	}

	if (name_base != NULL && len > 0) {
		int src_len = strlen(start);
		start[src_len - 1] = '\0';
		src_len -= 2;
		if (src_len > len)
			src_len = len;
		memset(name_base, 0, len);
		memcpy_s_inline((void *)name_base, len,
				(void *)start + 1, src_len);
	}

	free(start);

	u64 sys_boot = get_sys_btime_msecs();
	u64 msecs_per_tick = 1000UL / sysconf(_SC_CLK_TCK);

	return ((etime_ticks * msecs_per_tick) + sys_boot);
}

int fetch_kernel_version(int *major, int *minor, int *rev, int *num)
{
	struct utsname sys_info;

	// Get the real version of Ubuntu

	if (access("/proc/version_signature", R_OK) == 0) {
		FILE *f = fopen("/proc/version_signature", "r");
		if (f) {
			if (fscanf(f, "%*s %*s %d.%d.%d\n", major, minor, rev)
			    != 3) {
				fclose(f);
				*major = *minor = *rev = 0;
				return ETR_INVAL;
			}
			fclose(f);
			return ETR_OK;
		}
	}

	bool has_error = false;
	uname(&sys_info);
	int match_num = 0;
	*num = 0;
	// e.g.: 3.10.0-940.el7.centos.x86_64, 4.19.17-1.el7.x86_64
	match_num =
	    sscanf(sys_info.release, "%u.%u.%u-%u", major, minor, rev, num);
	if (match_num == 4 || match_num == 3) {
		return ETR_OK;
	} else {
		has_error = true;
	}

	// Get the real version of Debian
	// #1 SMP Debian 4.19.289-2 (2023-08-08)
	// e.g.:
	// uname -v (4.19.117.bsk.business.1 SMP Debian 4.19.117.business.1 Wed)
	// uname -v (#business SMP Debian 4.19.117.bsk.7-business Fri Sep 10 11:57:17)
	if (strstr(sys_info.version, "Debian")) {
		if ((sscanf(sys_info.version, "%*s %*s %*s %u.%u.%u-%u %*s",
			    major, minor, rev, num) != 4) &&
		    (sscanf(sys_info.version, "%*s %*s %*s %u.%u.%u.%*s %*s",
			    major, minor, rev) != 3) &&
		    (sscanf(sys_info.version, "%*s %*s %*s %*s %u.%u.%u-%u %*s",
			    major, minor, rev, num) != 4)
		    )
			has_error = true;
		else
			has_error = false;
	}

	if (has_error) {
		ebpf_warning
		    ("release %s version %s (major %d minor %d rev %d num %d)\n",
		     sys_info.release, sys_info.version, *major, *minor, *rev,
		     *num);
		return ETR_INVAL;
	}

	return ETR_OK;
}

int fetch_system_type(const char *sys_type, int type_len)
{
	int len, i, count = 0;
	char *p = NULL;
	struct utsname sys_info;
	uname(&sys_info);
	len = strlen(sys_info.release);
	for (i = len - 1; i >= 0; i--) {
		if (sys_info.release[i] == '.') {
			if (count == 0) {
				sys_info.release[i] = '\0';
			} else if (count == 1) {
				p = &sys_info.release[i + 1];
				break;
			}
			count++;
		}
	}

	if (p == NULL)
		return ETR_INVAL;

	len = strlen(p) + 1 > type_len ? type_len : strlen(p) + 1;
	memcpy((void *)sys_type, p, len);

	return ETR_OK;
}

void fetch_linux_release(const char *buf, int buf_len)
{
	struct utsname sys_info;
	uname(&sys_info);
	int len =
	    strlen(sys_info.release) + 1 >
	    buf_len ? buf_len : strlen(sys_info.release) + 1;
	memcpy((void *)buf, sys_info.release, len);
}

unsigned int fetch_kernel_version_code(void)
{
	int ret;
	int major, minor, rev, num;
	ret = fetch_kernel_version(&major, &minor, &rev, &num);
	if (ret != ETR_OK) {
		ebpf_warning("fetch_kernel_version error\n");
		return 0;
	}

	/*
	 * Calculate LINUX_VERSION_CODE based on kernel
	 * version(linux major.minor.patch), use macros
	 * `KERNEL_VERSION(a,b,c) (((a) << 16) + ((b) << 8) + (c)).`
	 * If the patch number is greater than 255, there will
	 * be a deviation. For example, Linux 4.14.275
	 * calculates 265983 with KERNEL_VERSION(4,14,275),
	 * and the backderived kernel version is 4.15.19,
	 * which is obviously wrong.
	 * The solution is to determine the value of patch
	 * and set it to 255 if it exceeds 255.
	 */
	if (rev > 255) {
		rev = 255;
	}

	return KERNEL_VERSION(major, minor, rev);
}

static bool __is_process(int pid, bool is_user)
{
	char file[PATH_MAX], buff[4096];
	int fd;
	int read_tgid = -1, read_pid = -1;

	snprintf(file, sizeof(file), "/proc/%d/status", pid);
	if (access(file, F_OK))
		return false;

	fd = open(file, O_RDONLY);
	if (fd < 0)
		return false;

	memset(buff, 0, 4096);
	if (read(fd, buff, sizeof(buff)) <= 0) {
		close(fd);
		return false;
	}
	close(fd);

	/*
	 * All kernel threads in Linux have their parent process
	 * as either 0 or 2, and not any other value.
	 */
	char *p;
	if (is_user) {
		int ppid = -1;
		p = strstr(buff, "PPid:");
		if (p == NULL)
			return false;
		sscanf(p, "PPid:\t%d", &ppid);
		if ((ppid == 0 && pid != 1) || ppid == 2 || ppid == -1)
			return false;
	}

	if ((p = strstr(buff, "Tgid:")) == NULL)
		return false;
	sscanf(p, "Tgid:\t%d", &read_tgid);

	if ((p = strstr(buff, "Pid:")) == NULL)
		return false;
	sscanf(p, "Pid:\t%d", &read_pid);

	if (read_tgid == -1 || read_pid == -1)
		return false;

	if (read_tgid != -1 && read_pid != -1 && read_tgid == read_pid)
		return true;

	return false;
}

bool is_user_process(int pid)
{
	return __is_process(pid, true);
}

bool is_process(int pid)
{
	return __is_process(pid, false);
}

char *get_timestamp_from_us(u64 microseconds)
{
#define TIME_STR_SIZE 32

	time_t seconds = microseconds / US_IN_SEC;
	long remainder_microseconds = microseconds % US_IN_SEC;

	struct tm *local_time = localtime(&seconds);

	int year = local_time->tm_year + 1900;
	int month = local_time->tm_mon + 1;
	int day = local_time->tm_mday;
	int hour = local_time->tm_hour;
	int minute = local_time->tm_min;
	int second = local_time->tm_sec;

	char *str;
	str = malloc(TIME_STR_SIZE);
	if (str == NULL) {
		ebpf_warning("malloc() failed.\n");
		return NULL;
	}

	snprintf(str, TIME_STR_SIZE, "%d-%02d-%02d %02d:%02d:%02d.%ld", year,
		 month, day, hour, minute, second, remainder_microseconds);
	return str;
}

static char *__gen_datetime_str(const char *fmt, u64 ns)
{
	const int strlen = DATADUMP_FILE_PATH_SIZE;
	time_t timep;
	char *str;
	struct tm *p;
	str = malloc(strlen);
	if (str == NULL) {
		ebpf_warning("malloc() failed.\n");
		return NULL;
	}

	long msec = 0;
	if (ns > 0) {
		timep = ns / NS_IN_SEC;
		msec = (ns % NS_IN_SEC) / NS_IN_USEC;
	} else {
		time(&timep);
		struct timeval msectime;
		gettimeofday(&msectime, NULL);
		msec = msectime.tv_usec;
	}

	p = localtime(&timep);
	snprintf(str, strlen, fmt,
		 (1900 + p->tm_year), (1 + p->tm_mon),
		 p->tm_mday, p->tm_hour, p->tm_min, p->tm_sec, msec);

	return str;
}

u32 legacy_fetch_log2_page_size(void)
{
#define LOG2_PAFE_SIZE_DEF 21

	u32 log2_page_size = 0;
	FILE *fp;
	char tmp[33] = {};

	if ((fp = fopen("/proc/meminfo", "r")) == NULL) {
		ebpf_warning("fopen file '/proc/meminfo' failed.\n");
		return LOG2_PAFE_SIZE_DEF;
	}

	while (fscanf(fp, "%32s", tmp) > 0) {
		if (strncmp("Hugepagesize:", tmp, 13) == 0) {
			u32 size;
			if (fscanf(fp, "%u", &size) > 0)
				log2_page_size = 10 + min_log2(size);
			break;
		}
	}

	fclose(fp);
	return log2_page_size;
}

char *gen_file_name_by_datetime(void)
{
	return __gen_datetime_str("%d_%02d_%02d_%02d_%02d_%02d_%ld", 0);
}

char *gen_timestamp_prefix(void)
{
	return __gen_datetime_str("%d-%02d-%02d %02d:%02d:%02d.%ld", 0);
}

char *gen_timestamp_str(u64 ns)
{
	return __gen_datetime_str("%d-%02d-%02d %02d:%02d:%02d.%ld", ns);
}

u64 get_netns_id_from_pid(pid_t pid)
{
	char netns_path[MAX_PATH_LENGTH];
	snprintf(netns_path, sizeof(netns_path), "/proc/%d/ns/net", pid);

	char target_path[MAX_PATH_LENGTH];
	ssize_t len =
	    readlink(netns_path, target_path, sizeof(target_path) - 1);
	if (len == -1) {
		return 0;
	}
	target_path[len] = '\0';

	// Extract netns_id from the target path
	char *netns_id_str_start = strstr(target_path, "[");
	if (netns_id_str_start == NULL) {
		ebpf_warning("Failed to extract netns_id.\n");
		return 0;
	}

	char *netns_id_str_end = strstr(netns_id_str_start, "]");
	if (netns_id_str_end == NULL) {
		ebpf_warning("Failed to extract netns_id.\n");
		return 0;
	}

	*netns_id_str_end = '\0';
	char *netns_id_str = netns_id_str_start + 1;

	return strtoull(netns_id_str, NULL, 10);
}

bool check_netns_enabled(void)
{
	char netns_path[MAX_PATH_LENGTH];
	snprintf(netns_path, sizeof(netns_path), "/proc/1/ns/net");

	char target_path[MAX_PATH_LENGTH];
	ssize_t len =
	    readlink(netns_path, target_path, sizeof(target_path) - 1);
	if (len == -1) {
		return false;
	}

	return true;
}

// Function to retrieve the host PID from the first line of /proc/pid/sched
// The expected format is "java (1234, #threads: 12)"
// where 1234 is the host PID (before Linux 4.1)
static int get_host_pid_from_sched(const char *path)
{
	static char *line = NULL;
	size_t size = 0;
	int host_pid = -1;

	FILE *sched_file = fopen(path, "r");
	if (sched_file == NULL) {
		ebpf_warning("Error opening file %s: %s (errno: %d)\n", path,
			     strerror(errno), errno);
		return -1;
	}
	// Read the first line of the file
	if (getline(&line, &size, sched_file) != -1) {
		// Locate the last '(' character in the line
		char *pid_start = strrchr(line, '(');
		if (pid_start != NULL) {
			// Convert the PID string to an integer
			host_pid = atoi(pid_start + 1);
		} else {
			ebpf_warning("Error parsing PID from line: %s\n", line);
		}
	} else {
		ebpf_warning("Error reading from file %s: %s (errno: %d)\n",
			     path, strerror(errno), errno);
	}

	fclose(sched_file);
	free(line);		// Free the allocated memory for line
	return host_pid;
}

// Linux kernels < 4.1 do not export NStgid field in /proc/pid/status.
// Resolve by traversing /proc/pid/sched in the container.
static int find_nspid_in_container(int host_pid)
{
	char path[300];

	// Check if we are already in the same PID namespace
	struct stat self_ns_stat, target_ns_stat;
	if (stat("/proc/self/ns/pid", &self_ns_stat) == -1) {
		ebpf_warning("stat /proc/self/ns/pid failed: %s (errno: %d)\n",
			     strerror(errno), errno);
		return -1;
	}
	snprintf(path, sizeof(path), "/proc/%d/ns/pid", host_pid);
	if (stat(path, &target_ns_stat) == -1) {
		ebpf_warning("stat %s failed: %s (errno: %d)\n", path,
			     strerror(errno), errno);
		return -1;
	}
	if (self_ns_stat.st_ino == target_ns_stat.st_ino) {
		return host_pid;
	}
	// Browse all PIDs in the namespace of the target process to find the matching PID
	snprintf(path, sizeof(path), "/proc/%d/root/proc", host_pid);
	DIR *dir = opendir(path);
	if (dir == NULL) {
		ebpf_warning("opendir %s failed: %s (errno: %d)\n", path,
			     strerror(errno), errno);
		return -1;
	}

	struct dirent *entry;
	while ((entry = readdir(dir)) != NULL) {
		if (isdigit(entry->d_name[0])) {
			// Check if /proc/<container-pid>/sched points back to <host_pid>
			snprintf(path, sizeof(path),
				 "/proc/%d/root/proc/%s/sched", host_pid,
				 entry->d_name);
			if (get_host_pid_from_sched(path) == host_pid) {
				int nspid = atoi(entry->d_name);
				if (closedir(dir) == -1) {
					ebpf_warning
					    ("closedir failed: %s (errno: %d)\n",
					     strerror(errno), errno);
				}
				return nspid;
			}
		}
	}

	if (closedir(dir) == -1) {
		ebpf_warning("closedir failed: %s (errno: %d)\n",
			     strerror(errno), errno);
	}

	ebpf_warning("No matching PID found in container for host PID %d\n",
		     host_pid);
	return -1;		// Return -1 if no matching PID is found
}

int get_nspid(int pid)
{
	int ns_pid_1, ns_pid_2;
	char status_path[MAX_PATH_LENGTH];
	snprintf(status_path, sizeof(status_path), "/proc/%d/status", pid);
	if (access(status_path, F_OK)) {
		ebpf_info("Fun %s file '%s' not exist.\n", __func__,
			  status_path);
		return ETR_NOTEXIST;
	}

	FILE *file = fopen(status_path, "r");
	if (file == NULL) {
		ebpf_info("fopen() %s error(%d)\n", status_path, errno);
		return ETR_INVAL;
	}

	char line[MAX_PATH_LENGTH];
	while (fgets(line, sizeof(line), file) != NULL) {
		if (strncmp(line, "NSpid:", 6) == 0) {
			int result = sscanf(line + 6, "\t%d\t%d\n", &ns_pid_1,
					    &ns_pid_2);
			if (result == 1) {
				ns_pid_2 = ns_pid_1;
			}
			fclose(file);
			return ns_pid_2;
		}
	}

	fclose(file);
	return find_nspid_in_container(pid);
}

int get_target_uid_and_gid(int target_pid, int *uid, int *gid)
{
	char proc_path[MAX_PATH_LENGTH];
	snprintf(proc_path, sizeof(proc_path), "/proc/%d", target_pid);
	if (access(proc_path, F_OK)) {
		ebpf_info("Fun %s file '%s' not exist.\n", __func__, proc_path);
		return ETR_NOTEXIST;
	}

	*uid = *gid = -1;
	struct stat sb;
	if (stat(proc_path, &sb) == 0) {
		*uid = sb.st_uid;
		*gid = sb.st_gid;
	} else {
		ebpf_info("stat() %s error, errno %d\n", proc_path, errno);
		return ETR_INVAL;
	}

	return ETR_OK;
}

int gen_file_from_mem(const char *mem_ptr, int write_bytes, const char *path)
{
	FILE *file_ptr = fopen(path, "wb");
	if (file_ptr == NULL) {
		ebpf_warning("Cannot open file '%s' failed. errno %d %s\n",
			     path, errno, strerror(errno));
		return ETR_INVAL;
	}

	fwrite(mem_ptr, write_bytes, 1, file_ptr);
	fclose(file_ptr);

	return ETR_OK;
}

/**
 * Function to copy a file.
 * @param src_file Path of the file to be copied.
 * @param dest_file Path where the copied file will be saved.
 * @return int 1: Copy successful; 2: Copy failed.
 */
int copy_file(const char *src_file, const char *dest_file)
{
	int ret = ETR_OK;
	FILE *src_file_ptr;	// Pointer to the source file
	FILE *dest_file_ptr;	// Pointer to the destination file
	src_file_ptr = dest_file_ptr = NULL;
	const size_t buffer_size = 4096;	// Buffer size
	char *buffer = (char *)malloc(buffer_size);	// Allocate buffer
	if (buffer == NULL) {
		ebpf_warning("malloc() failed.\n");
		return ETR_NOMEM;
	}

	size_t bytes_read;	// Actual number of bytes read

	if ((src_file_ptr = fopen(src_file, "rb")) == NULL
	    || (dest_file_ptr = fopen(dest_file, "wb")) == NULL) {
		ebpf_warning("Cannot open file, src '%s' dest '%s'\n",
			     src_file, dest_file);
		ret = ETR_INVAL;
		goto failed;
	}
	// Continuously read from src_file, place in buffer, and write buffer contents to dest_file
	while ((bytes_read = fread(buffer, 1, buffer_size, src_file_ptr)) > 0) {
		fwrite(buffer, bytes_read, 1, dest_file_ptr);
	}

failed:
	free(buffer);
	if (src_file_ptr)
		fclose(src_file_ptr);

	if (dest_file_ptr)
		fclose(dest_file_ptr);

	return ret;
}

int df_enter_ns(int pid, const char *type, int *self_fd)
{
#ifdef __NR_setns
	char path[64], selfpath[64];
	snprintf(path, sizeof(path), "/proc/%d/ns/%s", pid, type);
	snprintf(selfpath, sizeof(selfpath), "/proc/self/ns/%s", type);

	*self_fd = -1;
	struct stat oldns_stat, newns_stat;
	if (stat(selfpath, &oldns_stat) == 0 && stat(path, &newns_stat) == 0) {
		// Don't try to call setns() if we're in the same namespace already
		if (oldns_stat.st_ino != newns_stat.st_ino) {
			int newns;
			newns = open(path, O_RDONLY);
			if (newns < 0) {
				ebpf_warning("open() failed with %s(%d)\n",
					     strerror(errno), errno);
				return -1;
			}

			*self_fd = open(selfpath, O_RDONLY);
			if (*self_fd < 0) {
				ebpf_warning("open() failed with %s(%d)\n",
					     strerror(errno), errno);
				return -1;
			}
			// Some ancient Linux distributions do not have setns() function
			int result = syscall(__NR_setns, newns, 0);
			close(newns);
			if (result < 0) {
				ebpf_warning("setns(%s) failed with %s(%d)\n",
					     type, strerror(errno), errno);
				close(*self_fd);
				*self_fd = -1;
			}
			return result < 0 ? -1 : 1;
		}
	}
#endif // __NR_setns

	return 0;
}

void df_exit_ns(int fd)
{
	if (fd < 0)
		return;

	int result = syscall(__NR_setns, fd, 0);
	if (result < 0) {
		ebpf_warning("Fun %s setns error errno %d (%s)\n",
			     __func__, errno, strerror(errno));
	}

	close(fd);
}

int exec_command(const char *cmd, const char *args,
		 char *ret_buf, int ret_buf_size)
{
	FILE *fp;
	int rc = 0;
	char cmd_buf[PERF_PATH_SZ * 2];
	snprintf(cmd_buf, sizeof(cmd_buf), "%s %s", cmd, args);
	fp = popen(cmd_buf, "r");
	if (NULL == fp) {
		ebpf_warning("%s '%s' execute error,[%s]\n",
			     __func__, cmd_buf, strerror(errno));
		return -1;
	}

	if (ret_buf != NULL && ret_buf_size > 0) {
		/* Read and print the output */
		char buffer[1024];
		int write_bytes =
		    snprintf(ret_buf, ret_buf_size, "[ %s ]", cmd_buf);
		while (fgets(buffer, sizeof(buffer), fp) != NULL) {
			write_bytes +=
			    snprintf(ret_buf + write_bytes,
				     ret_buf_size - write_bytes, "%s", buffer);
			if (write_bytes >= ret_buf_size)
				break;
		}
	}

	rc = pclose(fp);
	if (-1 == rc) {
		ebpf_warning("pclose error, '%s' error:%s\n",
			     cmd_buf, strerror(errno));
	} else {
		if (WIFEXITED(rc)) {
			return WEXITSTATUS(rc);
		} else if (WIFSIGNALED(rc)) {
			ebpf_info
			    ("'%s' abnormal termination,signal number %d\n",
			     cmd_buf, WTERMSIG(rc));
		} else if (WIFSTOPPED(rc)) {
			ebpf_info("'%s' process stopped, signal number %d\n",
				  cmd_buf, WSTOPSIG(rc));
		}
	}

	return -1;
}

int fetch_container_id_from_str(char *buff, char *id, int copy_bytes)
{
	static const int cid_len = 64;
	char *p;

	if ((p = strstr(buff, ".scope")))
		*p = '\0';
	else
		p = buff + strlen(buff);

	if (strlen(buff) < cid_len)
		return -1;

	p -= cid_len;

	if (strchr(p, '.') || strchr(p, '-') || strchr(p, '/'))
		return -1;

	if (strlen(p) != cid_len)
		return -1;

	memset(id, 0, copy_bytes);
	memcpy_s_inline((void *)id, copy_bytes, (void *)p, cid_len);

	return 0;
}

int fetch_container_id(pid_t pid, char *id, int copy_bytes)
{
	char file[PATH_MAX], buff[MAXLINE];
	memset(buff, 0, sizeof(buff));
	snprintf(file, sizeof(file), "/proc/%d/cgroup", pid);
	if (access(file, F_OK))
		return -1;

	FILE *fp;
	char *lf;
	if ((fp = fopen(file, "r")) == NULL) {
		return -1;
	}

	while ((fgets(buff, sizeof(buff), fp)) != NULL) {
		if ((lf = strchr(buff, '\n')))
			*lf = '\0';
		// includes "pids" | "cpuset" | "devices" | "memory" | "cpu"
		if (strstr(buff, "pids") || strstr(buff, "cpuset")
		    || strstr(buff, "devices") || strstr(buff, "memory")
		    || strstr(buff, "cpu")) {
			break;
		}

	}

	fclose(fp);

	return fetch_container_id_from_str(buff, id, copy_bytes);
}

int generate_random_integer(int max_value)
{
	if (max_value <= 0) {
		ebpf_warning("Error: max_value must be greater than 0.\n");
		return 0;
	}

	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	srand(ts.tv_nsec);
	return (rand() % max_value);
}

u64 kallsyms_lookup_name(const char *name)
{
	static const int len = 256;
	FILE *f = fopen("/proc/kallsyms", "r");
	char func[len], buf[len];
	char symbol;
	void *addr;

	if (!f)
		return -ENOENT;

	while (!feof(f)) {
		if (!fgets(buf, sizeof(buf), f))
			break;
		if (strstr(buf, "(null)")) {
			if (sscanf(buf, "%*s %c %s", &symbol, func) != 2)
				break;
			addr = NULL;
		} else {
			if (sscanf(buf, "%p %c %s", &addr, &symbol, func) != 3)
				break;
		}
		if (!addr)
			continue;
		if (strcmp(func, name) == 0) {
			fclose(f);
			return (u64) addr;
		}
	}

	fclose(f);
	return 0;
}

static inline bool __is_same_ns(int target_pid, const char *tag)
{
	struct stat self_st, target_st;
	char path[64];
	snprintf(path, sizeof(path), "/proc/self/ns/%s", tag);
	if (stat(path, &self_st) != 0)
		return false;

	snprintf(path, sizeof(path), "/proc/%d/ns/%s", target_pid, tag);
	if (stat(path, &target_st) != 0)
		return false;

	if (self_st.st_ino == target_st.st_ino) {
		return true;
	}

	return false;
}

bool is_same_netns(int pid)
{
	return __is_same_ns(pid, "net");
}

bool is_same_mntns(int pid)
{
	return __is_same_ns(pid, "mnt");
}

// Function to get the inode number of a Unix socket from /proc/net/unix
static ino_t get_unix_socket_inode(const char *socket_path)
{
	FILE *fp = fopen("/proc/net/unix", "r");
	if (!fp) {
		perror("fopen /proc/net/unix");
		return (ino_t) - 1;
	}

	char line[PATH_MAX + 100];
	while (fgets(line, sizeof(line), fp)) {
		if (strstr(line, socket_path)) {
			ino_t inode;
			sscanf(line, "%*p: %*s %*s %*s %*s %*s %lu", &inode);
			fclose(fp);
			return inode;
		}
	}

	fclose(fp);
	return (ino_t) - 1;
}

// Function to check if a process has the specified Unix socket open
static int is_process_using_unix_socket(const char *file_path, const char *pid)
{
	char fd_dir_path[PATH_MAX];
	char target_path[PATH_MAX];
	snprintf(fd_dir_path, sizeof(fd_dir_path), "/proc/%s/fd", pid);

	DIR *dir = opendir(fd_dir_path);
	if (!dir) {
		return 0;
	}

	struct dirent *entry;
	while ((entry = readdir(dir)) != NULL) {
		if (entry->d_type == DT_LNK) {
			char link_path[PATH_MAX];
			ssize_t len;
			snprintf(link_path, sizeof(link_path), "%s/%s",
				 fd_dir_path, entry->d_name);
			if ((len =
			     readlink(link_path, target_path,
				      sizeof(target_path) - 1)) == -1) {
				continue;
			}
			target_path[len] = '\0';

			char *inode_start = strstr(target_path, "socket:[");
			if (inode_start) {
				inode_start += strlen("socket:[");
				char *inode_end = strchr(inode_start, ']');
				if (inode_end) {
					*inode_end = '\0';
					ino_t inode_number =
					    strtoul(inode_start, NULL, 10);
					ino_t target_inode =
					    get_unix_socket_inode(file_path);
					if (target_inode != (ino_t) - 1
					    && inode_number == target_inode) {
						closedir(dir);
						ebpf_info
						    ("File '%s' is opened by another process (PID: %s).\n",
						     file_path, pid);
						return 1;
					}
				}
			}
		}
	}

	closedir(dir);
	return 0;
}

// Function to check if a regular file is opened by other processes
int is_file_opened_by_other_processes(const char *filepath)
{
	struct stat file_stat;
	if (stat(filepath, &file_stat) == -1) {
		return -1;
	}

	if (!S_ISREG(file_stat.st_mode) && !S_ISSOCK(file_stat.st_mode)) {
		fprintf(stderr,
			"The specified file is neither a regular file nor a Unix socket.\n");
		return -1;
	}

	DIR *proc_dir = opendir("/proc");
	if (!proc_dir) {
		perror("opendir /proc");
		return -1;
	}

	struct dirent *proc_entry;
	while ((proc_entry = readdir(proc_dir)) != NULL) {
		if (!isdigit(proc_entry->d_name[0]))
			continue;	// Skip non-numeric entries

		if (S_ISSOCK(file_stat.st_mode)) {
			if (is_process_using_unix_socket
			    (filepath, proc_entry->d_name) == 1) {
				closedir(proc_dir);
				return 1;
			}
			continue;
		}

		char fd_dir_path[PATH_MAX];
		snprintf(fd_dir_path, sizeof(fd_dir_path), "/proc/%s/fd",
			 proc_entry->d_name);

		DIR *fd_dir = opendir(fd_dir_path);
		if (!fd_dir)
			continue;	// Skip if unable to open fd directory

		struct dirent *fd_entry;
		while ((fd_entry = readdir(fd_dir)) != NULL) {
			if (fd_entry->d_type != DT_LNK)
				continue;	// Skip non-symlink entries

			char link_path[PATH_MAX], resolved_path[PATH_MAX];
			snprintf(link_path, sizeof(link_path), "%s/%s",
				 fd_dir_path, fd_entry->d_name);

			ssize_t len = readlink(link_path, resolved_path,
					       sizeof(resolved_path) - 1);
			if (len == -1)
				continue;

			resolved_path[len] = '\0';

			struct stat link_stat;
			if (stat(resolved_path, &link_stat) == -1)
				continue;	// Skip if unable to stat the resolved path

			// Compare device and inode numbers
			if (file_stat.st_dev == link_stat.st_dev
			    && file_stat.st_ino == link_stat.st_ino) {
				if (atoi(proc_entry->d_name) != getpid()) {
					closedir(fd_dir);
					closedir(proc_dir);
					ebpf_info
					    ("File '%s' is opened by another process (PID: %s).\n",
					     filepath, proc_entry->d_name);
					return 1;	// File is opened by another process
				}
			}
		}

		closedir(fd_dir);
	}

	closedir(proc_dir);
	return 0;		// File is not opened by any other process
}

// Check if the substring starts with the main string
bool substring_starts_with(const char *haystack, const char *needle)
{
	int needle_len = strlen(needle);	// Length of the substring
	int haystack_len = strlen(haystack);	// Length of the main string

	// If the substring length is greater than the main string length, return false
	if (needle_len > haystack_len) {
		return false;
	}
	// Compare the first needle_len characters
	if (strncmp(haystack, needle, needle_len) == 0) {
		return true;	// Substring starts with the main string
	}

	return false;		// Substring does not start with the main string
}

static int find_proc_form_status_file(const char *status_path,
				      const char *process_name)
{
#define LINE_SIZE 256
#define NAME_SIZE 16
#define STATUS_PATH_SIZE 256

	FILE *status_file = fopen(status_path, "r");
	if (status_file == NULL) {
		ebpf_warning
		    ("Failed to open status file:%s, with %s(%d)\n",
		     status_file, strerror(errno), errno);
		return -1;
	}

	char line[LINE_SIZE];
	while (fgets(line, sizeof(line), status_file)) {
		if (strncmp(line, "Name:", 5) == 0) {
			char name[NAME_SIZE];
			if (sscanf(line, "Name:\t%15s", name) == 1) {
				if (strcmp(name, process_name)
				    == 0) {
					return 0;
				}
			}
			break;
		}
	}

	fclose(status_file);
	return -1;
}

int find_pid_by_name(const char *process_name, int exclude_pid)
{
	struct dirent *entry;
	DIR *proc = opendir("/proc");
	if (proc == NULL) {
		ebpf_warning("Failed to open /proc directory, with %s(%d)\n",
			     strerror(errno), errno);
		return -1;
	}

	while ((entry = readdir(proc)) != NULL) {
		if (entry->d_type == DT_DIR) {
			// Check if the directory name is a number (process ID)
			char *endptr;
			int pid = (int)strtol(entry->d_name, &endptr, 10);
			if (exclude_pid > 0 && pid == exclude_pid)
				continue;

			if (*endptr == '\0' && pid > 0) {
				char status_path[STATUS_PATH_SIZE];
				snprintf(status_path, sizeof(status_path),
					 "/proc/%d/status", pid);
				if (find_proc_form_status_file
				    (status_path, process_name) == 0)
					return pid;
			}
		}
	}

	if (closedir(proc) == -1) {
		ebpf_warning("Failed to close /proc directory, with %s(%d)\n",
			     strerror(errno), errno);
	}

	return -1;

#undef STATUS_PATH_SIZE
#undef LINE_SIZE
#undef NAME_SIZE
}

// DJB2 hash function with 32-bit output
u32 djb2_32bit(const char *str)
{
	u32 hash = 5381;
	int c;
	while ((c = *str++)) {
		hash = ((hash << 5) + hash) + c;	// hash * 33 + c
	}
	return hash;		// 32-bit output
}

#if !defined(AARCH64_MUSL) && !defined(JAVA_AGENT_ATTACH_TOOL)
int create_work_thread(const char *name, pthread_t * t, void *fn, void *arg)
{
	int ret;
	ret = pthread_create(t, NULL, fn, arg);
	if (ret) {
		ebpf_warning("worker name %s is error:%s\n",
			     name, strerror(errno));
		return ETR_INVAL;
	}

	/* set thread name */
	pthread_setname_np(*t, name);

	/*
	 * Separating threads is to automatically release
	 * resources after pthread_exit(), without being
	 * blocked or stuck.
	 */
	ret = pthread_detach(*t);
	if (ret != 0) {
		ebpf_warning("Error detaching thread, error:%s\n",
			     strerror(errno));
		return ETR_INVAL;
	} else {
		ebpf_info("thread %s, detached successful.", name);
	}

	return ETR_OK;
}
#endif /* !defined(AARCH64_MUSL) && !defined(JAVA_AGENT_ATTACH_TOOL) */

char *trim(char *str)
{
	char *end;
	while (isspace((unsigned char)*str))
		str++;
	if (*str == 0)
		return str;
	end = str + strlen(str) - 1;
	while (end > str && isspace((unsigned char)*end))
		end--;
	*(end + 1) = '\0';

	return str;
}

// Function to retrieve PCI device address and driver by NIC name
int retrieve_pci_info_by_nic(const char *nic_name, char *pci_device_address,
			     char *driver, int *numa_node)
{
	FILE *fp;
	char path[MAX_PATH_LENGTH];
	char line[MAX_PATH_LENGTH];	// Buffer for reading each line in the uevent file

	// Construct the path to the uevent file for the specified network interface
	snprintf(path, sizeof(path), "/sys/class/net/%s/device/uevent",
		 nic_name);

	// Open the uevent file to read the device information
	fp = fopen(path, "r");
	if (!fp) {
		// Log an error message if the uevent file cannot be opened
		ebpf_warning("Failed to open uevent file for %s: %s (%d)\n",
			     nic_name, strerror(errno), errno);
		return -1;
	}
	// Initialize the output variables to ensure they are empty in case no match is found
	pci_device_address[0] = '\0';
	driver[0] = '\0';

	// Read each line of the uevent file
	while (fgets(line, sizeof(line), fp)) {
		// Check for PCI_SLOT_NAME and extract the PCI address
		if (strstr(line, "PCI_SLOT_NAME") != NULL) {
			sscanf(line, "PCI_SLOT_NAME=%s", pci_device_address);
		}
		// Check for DRIVER and extract the driver name
		if (strstr(line, "DRIVER") != NULL) {
			sscanf(line, "DRIVER=%s", driver);
		}
		// If both the PCI address and driver are found, no need to continue reading
		if (pci_device_address[0] != '\0' && driver[0] != '\0') {
			break;
		}
	}

	// Close the file after reading the information
	fclose(fp);

	// Now, attempt to get the NUMA node from the PCI device
	snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/numa_node",
		 pci_device_address);
	fp = fopen(path, "r");
	if (fp) {
		// Read the NUMA node information from the file
		if (fscanf(fp, "%d", numa_node) != 1) {
			// If unable to read NUMA node, set to -1
			*numa_node = -1;
		}
		fclose(fp);
	} else {
		// If the file doesn't exist or cannot be opened, set NUMA node to -1
		*numa_node = -1;
	}

	return 0;
}

// Function to get the number of RX and TX channels for a given NIC
int get_nic_channels(const char *nic_name, int *rx_channels, int *tx_channels)
{
	char queue_path[MAX_PATH_LENGTH];
	struct dirent *entry;
	DIR *dir;

	*rx_channels = *tx_channels = 0;
	// Construct the path to the queues directory of the NIC
	snprintf(queue_path, sizeof(queue_path), "/sys/class/net/%s/queues",
		 nic_name);
	dir = opendir(queue_path);
	if (!dir) {
		ebpf_warning("Failed to open directory: %s\n", queue_path);
		return -1;
	}
	// Traverse the directory to count RX and TX channels
	while ((entry = readdir(dir)) != NULL) {
		if (strncmp(entry->d_name, "rx-", 3) == 0) {
			(*rx_channels)++;
		} else if (strncmp(entry->d_name, "tx-", 3) == 0) {
			(*tx_channels)++;
		}
	}
	closedir(dir);

	return 0;
}

// Function to get RX/TX ring size
int get_nic_ring_size(const char *nic_name, size_t * rx_sz, size_t * tx_sz)
{
	int sockfd;
	struct ifreq ifr;
	struct ethtool_ringparam ering;

	// Initialize resultsdd
	*rx_sz = *tx_sz = -1;

	// Open a socket for ioctl
	sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if (sockfd < 0) {
		ebpf_warning("Socket creation failed, with %s(%d)\n",
			     strerror(errno), errno);
		return -1;
	}
	// Prepare the ethtool request
	memset(&ifr, 0, sizeof(ifr));
	memset(&ering, 0, sizeof(ering));
	strncpy(ifr.ifr_name, nic_name, IFNAMSIZ - 1);

	ering.cmd = ETHTOOL_GRINGPARAM;
	ifr.ifr_data = (caddr_t) & ering;

	// Send the ioctl to get ring parameters
	if (ioctl(sockfd, SIOCETHTOOL, &ifr) == -1) {
		ebpf_warning("ioctl failed, with %s(%d)\n", strerror(errno),
			     errno);
		close(sockfd);
		return -1;
	}
	// Extract RX/TX ring sizes
	*rx_sz = ering.rx_pending;
	*tx_sz = ering.tx_pending;

	close(sockfd);
	return 0;
}

int set_nic_ring_size(const char *nic_name, size_t rx_sz, size_t tx_sz)
{
	int sockfd;
	struct ifreq ifr;
	struct ethtool_ringparam ering;

	// Open a socket for ioctl
	sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if (sockfd < 0) {
		ebpf_warning("Socket creation failed, with %s(%d)\n",
			     strerror(errno), errno);
		return -1;
	}
	// Prepare the ethtool request to get maximum ring sizes
	memset(&ifr, 0, sizeof(ifr));
	memset(&ering, 0, sizeof(ering));
	strncpy(ifr.ifr_name, nic_name, IFNAMSIZ - 1);

	ering.cmd = ETHTOOL_GRINGPARAM;	// Command to get ring parameters
	ifr.ifr_data = (caddr_t) & ering;

	// Get the maximum ring sizes using ioctl
	if (ioctl(sockfd, SIOCETHTOOL, &ifr) == -1) {
		ebpf_warning("Failed to get ring parameters, with %s(%d)\n",
			     strerror(errno), errno);
		close(sockfd);
		return -1;
	}
	// Log the maximum ring sizes
	ebpf_info("Max RX ring size: %u, Max TX ring size: %u\n",
		  ering.rx_max_pending, ering.tx_max_pending);

	// Set RX/TX sizes to maximum if they are 0
	if (rx_sz == 0) {
		rx_sz = ering.rx_max_pending;
	}
	if (tx_sz == 0) {
		tx_sz = ering.tx_max_pending;
	}
	// Prepare the ethtool request to set ring sizes
	ering.cmd = ETHTOOL_SRINGPARAM;	// Command to set ring parameters
	ering.rx_pending = rx_sz;
	ering.tx_pending = tx_sz;

	// Set the ring sizes using ioctl
	if (ioctl(sockfd, SIOCETHTOOL, &ifr) == -1) {
		ebpf_warning("Failed to set ring parameters, with %s(%d)\n",
			     strerror(errno), errno);
		close(sockfd);
		return -1;
	}
	// Log success
	ebpf_info
	    ("Successfully set RX ring size to %zu and TX ring size to %zu for NIC %s.\n",
	     rx_sz, tx_sz, nic_name);

	close(sockfd);
	return 0;
}

int is_promiscuous_mode(const char *nic_name)
{
	int sock;
	struct ifreq ifr;

	// Create a socket
	sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (sock < 0) {
		ebpf_warning("Failed to create socket, with %s(%d)\n",
			     strerror(errno), errno);
		return -1;
	}
	// Initialize the ifreq structure
	memset(&ifr, 0, sizeof(ifr));
	strncpy(ifr.ifr_name, nic_name, IFNAMSIZ - 1);

	// Get interface flags
	if (ioctl(sock, SIOCGIFFLAGS, &ifr) < 0) {
		ebpf_warning("Failed to get interface flags, with %s(%d)\n",
			     strerror(errno), errno);
		close(sock);
		return -1;
	}
	// Check if IFF_PROMISC is set
	int is_promiscuous = (ifr.ifr_flags & IFF_PROMISC) ? 1 : 0;

	close(sock);
	return is_promiscuous;
}

int set_promiscuous_mode(const char *nic_name)
{
	int sock;
	struct ifreq ifr;

	// Create a socket
	sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (sock < 0) {
		ebpf_warning("Failed to create socket, with %s(%d)\n",
			     strerror(errno), errno);
		return -1;
	}
	// Initialize the ifreq structure
	memset(&ifr, 0, sizeof(ifr));
	strncpy(ifr.ifr_name, nic_name, IFNAMSIZ - 1);

	// Get the current interface flags
	if (ioctl(sock, SIOCGIFFLAGS, &ifr) < 0) {
		ebpf_warning("Failed to get interface flags, with %s(%d)\n",
			     strerror(errno), errno);
		close(sock);
		return -1;
	}
	// Enable promiscuous mode by setting the IFF_PROMISC flag
	ifr.ifr_flags |= IFF_PROMISC;

	// Set the updated interface flags
	if (ioctl(sock, SIOCSIFFLAGS, &ifr) < 0) {
		ebpf_warning("Failed to set interface flags, with %s(%d)\n",
			     strerror(errno), errno);
		close(sock);
		return -1;
	}

	close(sock);
	return 0;
}
