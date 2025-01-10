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

#ifndef DF_COMMON_H
#define DF_COMMON_H
#include <stdbool.h>
#include <stdint.h>
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/utsname.h>
#include <linux/ethtool.h>
#include "types.h"
#include "clib.h"
#include "log.h"

#define __unused __attribute__((__unused__))

#define PORT_NUM_MAX	65536
#define NS_IN_SEC       1000000000ULL
#define NS_IN_MSEC      1000000ULL
#define NS_IN_USEC      1000ULL
#define US_IN_SEC	1000000ULL
#define MS_IN_SEC       1000ULL
#define TIME_TYPE_NAN   1
#define TIME_TYPE_SEC   0

#define OPEN_FILES_MAX 65536
#define KPROBE_EVENTS_FILE "/sys/kernel/debug/tracing/kprobe_events"
#define UPROBE_EVENTS_FILE "/sys/kernel/debug/tracing/uprobe_events"

#ifndef NELEMS
#define NELEMS(a) (sizeof(a) / sizeof((a)[0]))
#endif

#define MAX_PATH_LENGTH 1024
#define CONTAINER_ID_SIZE 65

extern int sysinfo(struct sysinfo *__info);

#ifndef likely
#define likely(x)  __builtin_expect((x),1)
#endif

#ifndef unlikely
#define unlikely(x)  __builtin_expect((x),0)
#endif

#ifndef offsetof
#define offsetof(TYPE, MEMBER)  __builtin_offsetof (TYPE, MEMBER)
#endif

#define CACHE_LINE_SIZE   64
#define CACHE_LINE_MASK (CACHE_LINE_SIZE-1)

#define zfree(P)		\
do {				\
	free((void *)(P));      \
	P = NULL;	        \
} while(0)

#define MAX_NIC_NAME_LEN 64

#ifndef MAX_PATH_LEN
#define MAX_PATH_LEN 256
#endif

/**
 * @struct cpu_balancer_nic
 * @brief Structure to hold Network Interface Card (NIC) information for CPU balancing.
 */
struct nic_info_s {
	char name[MAX_NIC_NAME_LEN];	/**< Network Interface Card (NIC) name. */
	char pci_device_address[MAX_PATH_LEN];
					   /**< PCI device address of the NIC. */
	char driver[MAX_PATH_LEN];	/**< Driver name associated with the NIC. */
	int rx_channels;		/**< Number of NIC receive (RX) channels. */
	int tx_channels;		/**< Number of NIC transmit (TX) channels. */
	size_t rx_ring_size;		/**< Size of the receive ring buffer. */
	size_t tx_ring_size;		/**< Size of the transmit ring buffer. */
	int promisc;			/**< Flag indicating if promiscuous mode is enabled in the NIC configuration. */
	int numa_node;			/**< NUMA node number to which the NIC is associated. */
	char *nic_cpus;			/**< 
                             * List of CPUs handling network data received by the NIC, 
                             * triggered by a physical interrupt.
                             */
	char *xdp_cpus;			/**< 
                             * List of CPUs used for XDP (eXpress Data Path) processing. 
                             * Ensures there is no overlap with nic_cpus.
                             */
};

static_always_inline void safe_buf_copy(void *dst, int dst_len,
					void *src, int src_len)
{
	if (dst == NULL || src == NULL) {
		ebpf_error("dst:%p, src:%p\n", dst, src);
		return;
	}

	int copy_count = clib_min(dst_len, src_len);
	if (copy_count <= 0) {
		ebpf_error("dst_len:%d, src_len:%d\n", dst_len, src_len);
		return;
	}

	memset(dst, 0, dst_len);
	memcpy(dst, src, copy_count);
}

#if defined(__x86_64__)
#include <emmintrin.h>
static inline void __pause(void)
{
	_mm_pause();
}
#elif defined(__aarch64__)
static inline void __pause(void)
{
	asm volatile ("yield":::"memory");
}
#else
_Pragma("GCC error \"__pause()\"");
#endif

#define CACHE_LINE_ROUNDUP(size) \
   (CACHE_LINE_SIZE * ((size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE))

#ifndef container_of
#define container_of(ptr, type, member) ({                \
	const typeof(((type *)0)->member) *__p = (ptr);   \
	(type *)( (void *)__p - offsetof(type, member) );})
#endif

#define __cache_aligned __aligned(CACHE_LINE_SIZE)

enum {
	ETR_OK = 0,
	ETR_INVAL = -1,		/* invalid parameter */
	ETR_NOMEM = -2,		/* no memory */
	ETR_EXIST = -3,		/* already exist */
	ETR_NOTEXIST = -4,	/* not exist */
	ETR_NOTGOELF = -5,	/* not go elf */
	ETR_NOPROT = -7,	/* no protocol */
	ETR_NOSYMBOL = -8,	/* no uprobe symbols */
	ETR_UPDATE_MAP_FAILD = -9,	/* update map failed */
	ETR_IDLE = -12,		/* nothing to do */
	ETR_BUSY = -13,		/* resource busy */
	ETR_NOTSUPP = -14,	/* not support */
	ETR_NORESOURCE = -15,	/* no resource */
	ETR_OVERLOAD = -16,	/* overloaded */
	ETR_NOSERV = -17,	/* no service */
	ETR_DISABLED = -18,	/* disabled */
	ETR_NOROOM = -19,	/* no room */
	ETR_NONEALCORE = -20,	/* non-eal thread lcore */
	ETR_CALLBACKFAIL = -21,	/* callbacks fail */
	ETR_IO = -22,		/* I/O error */
	ETR_MSG_FAIL = -23,	/* msg callback failed */
	ETR_MSG_DROP = -24,	/* msg callback dropped */
	ETR_SYSCALL = -26,	/* system call failed */
	ETR_PROC_FAIL = -27,	/* procfs failed */
	ETR_NOHANDLE = -28,	/* not find event handle */
	ETR_LOAD = -29,		/* bpf programe load failed */
	ETR_EPOLL = -30,	/* epoll error */

	/* positive code for non-error */
	ETR_INPROGRESS = 2,	/* in progress */
	ETR_CONTINUE = 4,
	ETR_NEWBUF = 5,
};

struct trace_err_tab {
	int errcode;
	const char *errmsg;
};

static struct trace_err_tab err_tab[] = {
	{ETR_OK, "OK"},
	{ETR_INVAL, "invalid parameter"},
	{ETR_NOMEM, "no memory"},
	{ETR_EXIST, "already exist"},
	{ETR_NOTEXIST, "not exist"},
	{ETR_NOTGOELF, "not go elf"},
	{ETR_NOPROT, "no protocol"},
	{ETR_NOSYMBOL, "no uprobe symbols"},
	{ETR_UPDATE_MAP_FAILD, "update map failed"},
	{ETR_IDLE, "nothing to do"},
	{ETR_BUSY, "resource busy"},
	{ETR_NOTSUPP, "not support"},
	{ETR_NORESOURCE, "no resource"},
	{ETR_OVERLOAD, "overloaded"},
	{ETR_NOSERV, "no service"},
	{ETR_DISABLED, "disabled"},
	{ETR_NOROOM, "no room"},
	{ETR_NONEALCORE, "non-EAL thread lcore"},
	{ETR_CALLBACKFAIL, "callback failed"},
	{ETR_IO, "I/O error"},
	{ETR_MSG_FAIL, "msg callback failed"},
	{ETR_MSG_DROP, "msg dropped"},
	{ETR_SYSCALL, "system call failed"},
	{ETR_PROC_FAIL, "procfs failed"},
	{ETR_NOHANDLE, "not find event handle"},
	{ETR_LOAD, "bpf programe load failed"},
	{ETR_EPOLL, "epoll error"},

	{ETR_INPROGRESS, "in progress"},
};

static inline const char *trace_strerror(int err)
{
	int i;

	for (i = 0; i < NELEMS(err_tab); i++) {
		if (err == err_tab[i].errcode)
			return err_tab[i].errmsg;
	}

	return "<unknow>";
}

#define RUN_ONCE(condition, f, arg) ({    \
  int __ret_warn_once = !!(condition);    \
                                    \
  if (unlikely(__ret_warn_once)) {        \
    f(arg);                         \
    condition = !(__ret_warn_once); \
  }                                       \
})

static inline int is_power_of_2(uint32_t n)
{
	return n && !(n & (n - 1));
}

// Aligns input parameter to the next power of 2
static inline uint32_t align32pow2(uint32_t x)
{
	x--;
	x |= x >> 1;
	x |= x >> 2;
	x |= x >> 4;
	x |= x >> 8;
	x |= x >> 16;

	return x + 1;
}

uint64_t gettime(clockid_t clk_id, int flag);
static inline int64_t get_sysboot_time_ns(void)
{
	int64_t real_time, monotonic_time;
	real_time = gettime(CLOCK_REALTIME, TIME_TYPE_NAN);
	monotonic_time = gettime(CLOCK_MONOTONIC, TIME_TYPE_NAN);
	return (real_time - monotonic_time);
}

bool is_core_kernel(void);
int get_cpus_count(bool ** mask);
void clear_residual_probes();
int max_locked_memory_set_unlimited(void);
int sysfs_write(const char *file_name, char *v);
int sysfs_read_num(const char *file_name);
uint32_t get_sys_uptime(void);
u64 get_sys_btime_msecs(void);
u64 get_process_starttime(pid_t pid);
int max_rlim_open_files_set(int num);
int fetch_kernel_version(int *major, int *minor, int *rev, int *num);
unsigned int fetch_kernel_version_code(void);
int get_num_possible_cpus(void);

// Check if task is the main thread based on pid.
// Ignore threads other than the main thread in uprobe to avoid repeating hooks
bool is_user_process(int pid);
bool is_process(int pid);
char *gen_file_name_by_datetime(void);
char *gen_timestamp_prefix(void);
char *gen_timestamp_str(u64 ns);
int fetch_system_type(const char *sys_type, int type_len);
void fetch_linux_release(const char *buf, int buf_len);
u64 get_process_starttime_and_comm(pid_t pid, char *name_base, int len);
u32 legacy_fetch_log2_page_size(void);
u64 get_netns_id_from_pid(pid_t pid);
bool check_netns_enabled(void);
int get_nspid(int pid);
int get_target_uid_and_gid(int target_pid, int *uid, int *gid);
int copy_file(const char *src_file, const char *dest_file);
int df_enter_ns(int pid, const char *type, int *self_fd);
void df_exit_ns(int fd);
int gen_file_from_mem(const char *mem_ptr, int write_bytes, const char *path);
int exec_command(const char *cmd, const char *args, char *ret_buf,
		 int ret_buf_size);
u64 current_sys_time_secs(void);
int fetch_container_id_from_str(char *buff, char *id, int copy_bytes);
int fetch_container_id(pid_t pid, char *id, int copy_bytes);
int parse_num_range(const char *config_str, int bytes_count,
		    bool ** mask, int *count);
int parse_num_range_disorder(const char *config_str,
			     int bytes_count, bool ** mask);
int generate_random_integer(int max_value);
bool is_same_netns(int pid);
bool is_same_mntns(int pid);
int is_file_opened_by_other_processes(const char *filename);
/**
 * @brief Find the address through kernel symbols.
 *
 * @param[in] name Kernel symbol name
 * @return 0 indicates that the kernel symbol name was not found, while
 * a non-zero value represents the address of the kernel symbol.
 */
u64 kallsyms_lookup_name(const char *name);
bool substring_starts_with(const char *haystack, const char *needle);
char *get_timestamp_from_us(u64 microseconds);
int find_pid_by_name(const char *process_name, int exclude_pid);
u32 djb2_32bit(const char *str);
#if !defined(AARCH64_MUSL) && !defined(JAVA_AGENT_ATTACH_TOOL)
int create_work_thread(const char *name, pthread_t * t, void *fn, void *arg);
#endif /* !defined(AARCH64_MUSL) && !defined(JAVA_AGENT_ATTACH_TOOL) */

/**
 * @brief Removes leading and trailing whitespace from a string.
 *
 * This function modifies the input string in place by removing all leading and 
 * trailing whitespace characters. The trimmed string starts at the first 
 * non-whitespace character and ends at the last non-whitespace character.
 *
 * @param[in,out] str A pointer to the input string to be trimmed. 
 *                    The string must be null-terminated and modifiable.
 *
 * @return char* Returns a pointer to the trimmed string. If the string is empty 
 *               or contains only whitespace, the function returns a pointer 
 *               to the null terminator (`'\0'`).
 *
 * @note This function operates directly on the input string and does not allocate
 *       any new memory. Ensure the input string is writable to avoid undefined behavior.
 *
 * @example
 * char str[] = "   Hello, World!   ";
 * char *trimmed = trim(str);
 * printf("'%s'\n", trimmed); // Output: 'Hello, World!'
 */
char *trim(char *str);

/**
 * @brief Retrieves the PCI device address and driver name for a given NIC name.
 *
 * This function queries the system to find the PCI device address, driver name,
 * and NUMA node associated with a specified network interface card (NIC) name.
 *
 * @param[in] nic_name The name of the network interface card (e.g., "eth0").
 * @param[out] pci_device_address A buffer to store the retrieved PCI device address (e.g., "0000:03:00.0").
 *                                The buffer should be preallocated by the caller.
 * @param[out] driver A buffer to store the name of the driver associated with the NIC.
 *                    The buffer should be preallocated by the caller.
 * @param[out] numa_node A pointer to an integer to store the NUMA node ID associated with the NIC.
 * 
 * @return int Returns 0 on success, or a negative error code on failure.
 *             Possible error codes:
 *             - -1: NIC not found.
 *
 * @note Ensure that the buffers `pci_device_address` and `driver` are large enough
 *       to hold the respective information to avoid buffer overflows.
 * @note This function may require root privileges or CAP_NET_ADMIN capabilities
 *       to access the necessary system files or interfaces.
 */
int retrieve_pci_info_by_nic(const char *nic_name, char *pci_device_address,
			     char *driver, int *numa_node);

/**
 * @brief Retrieves the number of RX and TX channels for a specified NIC.
 *
 * This function inspects the system's `/sys/class/net/<nic_name>/queues` directory 
 * to determine the number of receive (RX) and transmit (TX) channels configured 
 * for the given network interface card (NIC).
 *
 * @param[in] nic_name The name of the network interface card (e.g., "eth0").
 * @param[out] rx_channels A pointer to an integer where the count of RX channels will be stored.
 * @param[out] tx_channels A pointer to an integer where the count of TX channels will be stored.
 *
 * @return int Returns 0 on success, or -1 on failure.
 *             - If the directory `/sys/class/net/<nic_name>/queues` cannot be opened, 
 *               an error message is logged, and -1 is returned.
 *
 * @note The function directly modifies the values pointed to by `rx_channels` and 
 *       `tx_channels`, so ensure these pointers are valid.
 * @note Root privileges or sufficient permissions may be required to access the 
 *       `/sys/class/net/<nic_name>/queues` directory.
 *
 * @example
 * int rx = 0, tx = 0;
 * if (get_nic_channels("eth0", &rx, &tx) == 0) {
 *     printf("RX channels: %d, TX channels: %d\n", rx, tx);
 * } else {
 *     printf("Failed to retrieve NIC channel information.\n");
 * }
 */
int get_nic_channels(const char *nic_name, int *rx_channels, int *tx_channels);

/**
 * @brief Retrieves the RX and TX ring sizes for a specified network interface.
 *
 * This function queries the kernel for the receive (RX) and transmit (TX) 
 * ring buffer sizes of a given network interface card (NIC) using the 
 * `ETHTOOL_GRINGPARAM` ioctl command.
 *
 * @param[in] nic_name The name of the network interface card (e.g., "eth0").
 * @param[out] rx_sz Pointer to store the size of the RX ring buffer.
 *                   Will be set to -1 if an error occurs.
 * @param[out] tx_sz Pointer to store the size of the TX ring buffer.
 *                   Will be set to -1 if an error occurs.
 *
 * @return int Returns:
 *             - 0 on success, with `rx_sz` and `tx_sz` updated to the
 *               respective ring sizes.
 *             - -1 on failure, with `rx_sz` and `tx_sz` set to -1.
 *
 * @note This function requires sufficient privileges (e.g., root) to access 
 *       NIC parameters.
 *
 * @example
 * // Retrieve RX and TX ring sizes for "eth0"
 * size_t rx_size, tx_size;
 * if (get_nic_ring_size("eth0", &rx_size, &tx_size) == 0) {
 *     printf("RX ring size: %zu, TX ring size: %zu\n", rx_size, tx_size);
 * } else {
 *     printf("Failed to retrieve ring sizes for eth0.\n");
 * }
 */
int get_nic_ring_size(const char *nic_name, size_t * rx_sz, size_t * tx_sz);

/**
 * @brief Sets the RX and TX ring buffer sizes for a specified NIC.
 *
 * This function uses an `ioctl` system call to set the receive (RX) and transmit (TX) 
 * ring buffer sizes of a given network interface card (NIC). The sizes are set only 
 * if they are greater than 0.
 *
 * @param[in] nic_name The name of the network interface card (e.g., "eth0").
 * @param[in] rx_sz The desired RX ring buffer size. Set to 0 to leave it unchanged.
 * @param[in] tx_sz The desired TX ring buffer size. Set to 0 to leave it unchanged.
 *
 * @return int Returns 0 on success, or -1 on failure.
 *             - A failure can occur if the socket cannot be created, if the `ioctl` 
 *               call fails, or if both `rx_sz` and `tx_sz` are 0.
 *
 * @note At least one of `rx_sz` or `tx_sz` must be greater than 0; otherwise, 
 *       the function returns an error.
 * @note Requires sufficient privileges (e.g., root) to modify NIC settings.
 *
 * @example
 * // Set RX ring size to 1024 and TX ring size to 2048 for "eth0"
 * if (set_nic_ring_size("eth0", 1024, 2048) == 0) {
 *     printf("Successfully updated ring buffer sizes.\n");
 * } else {
 *     printf("Failed to update ring buffer sizes.\n");
 * }
 */
int set_nic_ring_size(const char *nic_name, size_t rx_sz, size_t tx_sz);

/**
 * @brief Checks if a specified network interface is in promiscuous mode.
 *
 * This function determines whether the given network interface card (NIC) 
 * is operating in promiscuous mode, where it can receive all packets on the 
 * network regardless of the destination address.
 *
 * @param[in] nic_name The name of the network interface card (e.g., "eth0").
 *
 * @return int Returns:
 *             - 1 if the NIC is in promiscuous mode.
 *             - 0 if the NIC is not in promiscuous mode.
 *             - -1 if an error occurs (e.g., socket creation or ioctl failure).
 *
 * @note Requires sufficient privileges (e.g., root) to query NIC flags.
 *
 * @example
 * // Check if "eth0" is in promiscuous mode
 * int result = is_promiscuous_mode("eth0");
 * if (result == 1) {
 *     printf("eth0 is in promiscuous mode.\n");
 * } else if (result == 0) {
 *     printf("eth0 is not in promiscuous mode.\n");
 * } else {
 *     printf("Failed to check promiscuous mode for eth0.\n");
 * }
 */
int is_promiscuous_mode(const char *nic_name);

/**
 * @brief Enables promiscuous mode for a specified network interface.
 *
 * This function sets the network interface card (NIC) into promiscuous mode,
 * allowing it to receive all packets on the network, regardless of their destination.
 *
 * @param[in] nic_name The name of the network interface card (e.g., "eth0").
 *
 * @return int Returns:
 *             - 0 on success, indicating that promiscuous mode was enabled.
 *             - -1 on failure, with an error message logged.
 *
 * @note This function requires sufficient privileges (e.g., root) to modify
 *       the NIC's configuration.
 *
 * @example
 * // Enable promiscuous mode for "eth0"
 * if (set_promiscuous_mode("eth0") == 0) {
 *     printf("Promiscuous mode enabled for eth0.\n");
 * } else {
 *     printf("Failed to enable promiscuous mode for eth0.\n");
 * }
 */
int set_promiscuous_mode(const char *nic_name);

#endif /* DF_COMMON_H */
