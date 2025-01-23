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

#ifndef DF_USER_STRINGIFIER_H
#define DF_USER_STRINGIFIER_H

#include "../bihash_8_8.h"

#define stack_str_hash_t	clib_bihash_8_8_t
#define stack_str_hash_init	clib_bihash_init_8_8
#define stack_str_hash_kv	clib_bihash_kv_8_8_t
#define print_hash_stack_str	print_bihash_8_8
#define stack_str_hash_search	clib_bihash_search_8_8
#define stack_str_hash_add_del	clib_bihash_add_del_8_8
#define stack_str_hash_free	clib_bihash_free_8_8
#define stack_str_hash_key_value_pair_cb	clib_bihash_foreach_key_value_pair_cb_8_8
#define stack_str_hash_foreach_key_value_pair	clib_bihash_foreach_key_value_pair_8_8

struct stack_str_hash_ext_data {
	/*
	 * It is used for quickly releasing the stack_str_hash resource.
	 */
	stack_str_hash_kv *stack_str_kvps;
	bool clear_hash;
};

#ifndef AARCH64_MUSL

u64 get_stack_table_data_miss_count(void);
int init_stack_str_hash(stack_str_hash_t *h, const char *name);
void clean_stack_strs(stack_str_hash_t *h);
void release_stack_str_hash(stack_str_hash_t *h);
char *resolve_and_gen_stack_trace_str(struct bpf_tracer *t,
				      struct stack_trace_key_t *v,
				      const char *stack_map_name,
				      const char *custom_stack_map_name,
				      const char *stack_blacklist_map_name,
				      stack_str_hash_t *h,
				      bool new_cache,
				      char *process_name, void *info_p, bool ignore_libs);
char *rewrite_java_symbol(char *sym);

#endif /* AARCH64_MUSL */
#endif /* DF_USER_STRINGIFIER_H */
