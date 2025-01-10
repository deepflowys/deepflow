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

#include "../user/types.h"
#include <bcc/perf_reader.h>
#include "../user/config.h"
#include "../user/common_utils.h"
#include "../user/utils.h"
#include "../user/mem.h"
#include "../user/log.h"
#include "../user/types.h"
#include "../user/vec.h"
#include "../user/tracer.h"
#include "../user/socket.h"
#include "../user/profile/perf_profiler.h"
#include "../user/elf.h"
#include "../user/load.h"

static const char *nic_name = "lo";

static int show_nic_info(const char *name)
{
	struct nic_info_s *nic = malloc(sizeof(struct nic_info_s));
	if (nic == NULL) {
		fprintf(stderr, "malloc() failed.\n");
		return -1;
	}

	memset(nic, 0, sizeof(*nic));
	snprintf(nic->name, sizeof(nic->name), "%s", name);
	retrieve_pci_info_by_nic(nic->name, nic->pci_device_address,
				 nic->driver, &nic->numa_node);
	get_nic_channels(nic->name, &nic->rx_channels, &nic->tx_channels);
	get_nic_ring_size(nic->name, &nic->rx_ring_size, &nic->tx_ring_size);
	nic->promisc = is_promiscuous_mode(nic->name);
	fprintf(stdout, "Device: %-8s\nAddress: %-14s\nDriver: %-8s\n"
		"Rx-Channels: %-4d\nTx-Channels: %-4d\nRX-Ring-Size: %-5ld\nTX-Ring-Size: %-5ld\n"
		"PROMISC: %-3d\nNumaNode: %d\n\n",
		nic->name, nic->pci_device_address, nic->driver,
		nic->rx_channels, nic->tx_channels, nic->rx_ring_size,
		nic->tx_ring_size, nic->promisc, nic->numa_node);
	fflush(stdout);
	free(nic);
	return ETR_OK;
}

int main(void)
{
	bpf_tracer_init(NULL, true);
	int is_promisc = is_promiscuous_mode(nic_name);
	printf("Nic %s is_promisc : %d\n", nic_name, is_promisc);
	show_nic_info(nic_name);
	if (is_promisc == 0) {
		printf("Set nic %s promisc mode.\n", nic_name);
		set_promiscuous_mode(nic_name);
	}
	int ret = set_nic_ring_size(nic_name, 4096, 4096);
	printf("ret %d\n-----------------\n", ret);
	show_nic_info(nic_name);
	return 0;
}
