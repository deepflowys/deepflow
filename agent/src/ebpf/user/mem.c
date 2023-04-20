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

#include <sys/stat.h>
#include <malloc.h>
#include <sys/mman.h>
#include <unistd.h> // sysconf()
#include "types.h"
#include "clib.h"
#include "mem.h"
#include "log.h"

#ifndef MFD_HUGETLB
#define MFD_HUGETLB 0x0004U
#endif

#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT 26
#endif

#ifndef MFD_HUGE_SHIFT
#define MFD_HUGE_SHIFT 26
#endif

#ifndef MAP_FIXED_NOREPLACE
#define MAP_FIXED_NOREPLACE 0x100000
#endif

static clib_mem_main_t mem_main;

static uword mem_get_fd_page_size(int fd)
{
	struct stat st = { 0 };
	if (fstat(fd, &st) == -1)
		return 0;
	return st.st_blksize;
}

static clib_mem_page_sz_t mem_get_fd_log2_page_size(int fd)
{
	uword page_size = mem_get_fd_page_size(fd);
	return page_size ? min_log2(page_size) : CLIB_MEM_PAGE_SZ_UNKNOWN;
}

void clib_mem_free(void *p)
{
	void *start = p - sizeof(u64);
	u64 mem_size = *(u64 *)start;
	atomic64_add(&mem_main.clib_free_mem_bytes, mem_size);
	free(start);
}

void *clib_mem_alloc_aligned(uword size, u32 align, uword *alloc_sz)
{
	align = clib_max(CLIB_MEM_MIN_ALIGN, align);

	if (size == 0 || (align && !is_pow2(align))) {
		ebpf_warning("Invalid alignment: size %u align %u\n",
			     size, align);
		return NULL;
	}

	int extra_len = sizeof(u64); // Record the total size of memory
	size += extra_len;
	size = (size + align - 1) & ~(align - 1);
	void *ptr = malloc(size);
	if (ptr == NULL) {
		ebpf_warning("malloc error\n");
		return NULL;
	}

	if (alloc_sz != NULL)
		*alloc_sz = size - extra_len;

	*(u64 *)ptr = size;
	atomic64_add(&mem_main.clib_alloc_mem_bytes, size);

	return ptr + sizeof(u64);
}

void *clib_mem_realloc_aligned(void *p, uword size, u32 align, uword *alloc_sz)
{
	align = clib_max(CLIB_MEM_MIN_ALIGN, align);
	int extra_len = sizeof(u64);
	void *start = p - sizeof(u64);
	uword old_size = *(u64 *)start;

	if (p == NULL || size == 0 || old_size >= (size + extra_len)
	    || (align && !is_pow2(align))) {
		ebpf_warning("Invalid alignment: size %u align %u\n",
			     size, align);
		return NULL;
	}

	size = round_pow2(size, align);
	void *ptr = realloc(start, size + extra_len);
	if (ptr == NULL) {
		ebpf_warning("realloc error\n");
		*alloc_sz = 0;
		return NULL;
	}

	*alloc_sz = size;
	*(u64 *)ptr = size + extra_len;
	atomic64_add(&mem_main.clib_alloc_mem_bytes, (size + extra_len - old_size));

	return ptr + sizeof(u64);
}

uword clib_mem_vm_reserve(uword size, clib_mem_page_sz_t log2_page_sz)
{
	clib_mem_main_t *mm = &mem_main;
	uword pagesize = 1ULL << log2_page_sz;
	uword sys_page_sz = 1ULL << mm->log2_page_sz;
	uword n_bytes;
	void *base = 0, *p;

	size = round_pow2(size, pagesize);

	/*
	 * Ensure that the reserve space is in units of pages,
	 * with starting or ending addresses on page boundaries.
	 */

	/* make sure that we get reservation aligned to page_size */
	base = mmap(0, size + pagesize, PROT_NONE,
		    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

	if (base == MAP_FAILED)
		return ~0;

	/* return additional space at the end of allocation */
	p = base + size + pagesize;
	n_bytes = (uword) p & pow2_mask(log2_page_sz);
	if (n_bytes) {
		p -= n_bytes;
		munmap(p, n_bytes);
	}

	/* return additional space at the start of allocation */
	n_bytes = pagesize - sys_page_sz - n_bytes;
	if (n_bytes) {
		munmap(base, n_bytes);
		base += n_bytes;
	}

	return (uword) base + sys_page_sz;
}

void get_mem_stat(u64 *alloc_b, u64 *free_b)
{
	clib_mem_main_t *mm = &mem_main;
	*alloc_b = atomic64_read(&mm->clib_alloc_mem_bytes);
	*free_b = atomic64_read(&mm->clib_free_mem_bytes);
}

void clib_mem_init(void)
{
	int fd;
	clib_mem_main_t *mm = &mem_main;
	long sysconf_page_size;
	uword page_size;
	sysconf_page_size = sysconf(_SC_PAGESIZE);
	if (sysconf_page_size < 0) {
		ebpf_error("Could not determine the page size");
	}

	page_size = sysconf_page_size;
	mm->log2_page_sz = min_log2(page_size);
	atomic64_init(&mm->clib_alloc_mem_bytes);
	atomic64_init(&mm->clib_free_mem_bytes);

	/* fetch system hugeppage size */
	if ((fd = syscall(__NR_memfd_create, "test", MFD_HUGETLB)) != -1) {
		mm->log2_default_hugepage_sz = mem_get_fd_log2_page_size(fd);
		close(fd);
	} else {
		ebpf_error
		    ("Not fetch system hugeppage size. need linux 4.14+\n");
	}
}
