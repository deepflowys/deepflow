# Copyright (c) 2022 Yunshan Networks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ifeq ($(V),1)
        Q =
        msg =
else
        Q = @
        msg = @printf '  %-8s %s%s\n' "$(1)" "$(notdir $(2))" "$(if $(3), $(3))";
endif

ARCH ?= $(shell uname -m)
CC ?= gcc
CFLAGS ?= -std=gnu99 --static -g -O2 -ffunction-sections -fdata-sections -fPIC -fno-omit-frame-pointer -Wall -Wno-sign-compare -Wno-unused-parameter -Wno-missing-field-initializers

EXECS := test_symbol test_offset test_insns_cnt test_bihash test_vec test_fetch_container_id test_parse_range test_set_ports_bitmap test_pid_check test_match_pids test_nic_config
ifeq ($(ARCH), x86_64)
#-lbcc -lstdc++
        LDLIBS += ../libtrace.a ./libtrace_utils.a -ljattach -lbcc_bpf -lGoReSym -lbddisasm -ldwarf -lelf -lz -lpthread -lbcc -lstdc++ -ldl -lm
else ifeq ($(ARCH), aarch64)
        LDLIBS += ../libtrace.a ./libtrace_utils.a -ljattach -lGoReSym -lbcc_bpf -ldwarf -lelf -lz -lpthread -lbcc -lstdc++ -ldl -lm
endif

all: $(EXECS) libtrace_utils.a

% : %.c libtrace_utils.a
	$(call msg,TEST,$@)
	$(Q)$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)
	$(Q)./$@

libtrace_utils.a:
	$(Q)cargo rustc -p trace-utils --crate-type=staticlib
	$(Q)cp ../../../target/debug/libtrace_utils.a .

clean:
	$(Q)rm -rf $(EXECS)
	
.PHONY: all clean
