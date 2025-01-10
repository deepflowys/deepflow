# libtrace

This is a library that uses eBPF technology to obtain the tracking data. It can use very simple interface calls to get any valuable data from the kernel or application.

Provides the Rust language interface.

Currently support `x86_64` and `arm64` architectures.

[Kernel version and feature support](https://github.com/deepflowio/docs/blob/main/docs/zh/02-ce-install/01-overview.md#%E8%BF%90%E8%A1%8C%E6%9D%83%E9%99%90%E5%8F%8A%E5%86%85%E6%A0%B8%E8%A6%81%E6%B1%82)

# Protocol Tracing

Deepflow-agent deploys eBPF probes (kprobe/traceponit) on Linux syscalls. When application makes network-related syscalls, deepflow-agent's eBPF probes snoop the data.

The following protocols are currently probed:

- HTTP1
- HTTP2
- DUBBO
- GRPC
- SOFARPC
- MYSQL
- POSTGRESQL
- REDIS
- KAFKA
- MQTT
- DNS
- TLS(handshake)
- MONGO
- ORACLE
- FASTCGI
- ROCKETMQ

## TLS/SSL Tracing

Currently only for Golang programs and openssl.

Use eBPF user-space probes (uprobes), set up on the Golang application's TLS API.

HTTP1 and HTTP2 protocals data are currently probed on TLS/SSL.

Trace to the Golang program is implemented by the following method:
- All the currently running Golang binary executables are found by traversing Procfs, and then the files are parsed to obtain the Golang version, trace symbol offset address, structure member offsets and other informations.
- Create probes and attach based on the parsed datas.
- Capture process execute/exit events by tracking two tracepoints(`tracepoint/sched/sched_process_fork` and `tracepoint/sched/sched_process_exit`). Update probes based on captured events.

Note: In order to avoid attaching/detaching the golang program repeatedly, it is necessary to confirm that the golang application has been running stably before DeepFlow-agent starts the attach operation. After DeepFlow-agent detects that the golang application is loaded, it delays the attach operation for 60 seconds. Openssl only supports kernel 4.17 and above.

# Implement logic

Note: The numbers in the figure below indicate the calling sequence.

## Userspace tracer startup

```mermaid
graph LR
subgraph Symbols
    style illustration-func fill:#ccff,color:#000, stroke-width:2px
    style illustration-info fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style illustration-code-block stroke-width:2px
    illustration-func(1 Function Name)
    illustration-info([Short description])
    illustration-code-block(2 Code block)
    illustration_map[(eBPF map)]
end
    style enable_ebpf_protocol-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-10-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-11-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-13-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-14-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-15-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-16-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-3-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-4-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-1-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-8-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style running_socket_tracer-9-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style bpf_tracer_finish-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style FEATUER-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style start fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style enable_ebpf_protocol fill:#ccff,color:#000, stroke-width:2px
    style FEATUER fill:#ccff,color:#000, stroke-width:2px
    style bpf_tracer_init fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-1 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-2 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-3 stroke-width:2px
    style running_socket_tracer-4 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-5 stroke-width:2px
    style running_socket_tracer-6 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-7 stroke-width:2px
    style running_socket_tracer-8 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-9 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-10 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-11 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-12 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-13 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-14 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-16 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-2-2 fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-2-3 fill:#ccff,color:#000, stroke-width:2px
    style bpf_tracer_finish fill:#ccff,color:#000, stroke-width:2px
    style bpf_tracer_init-1 stroke-width:2px
    style bpf_tracer_init-2 stroke-width:2px
    style bpf_tracer_init-3 stroke-width:2px
    start[tracer start]
    start --> enable_ebpf_protocol(1 enable_ebpf_protocol) -.- enable_ebpf_protocol-i([ebable application layer protocols]) 
    start --> FEATUER(2 set_feature_regex) -.- FEATUER-i([Uprobe OPENSSL/GOLANG filter])
    start --> bpf_tracer_init(3 bpf_tracer_init)

    start --> running_socket_tracer(4 running_socket_tracer)
    start --> bpf_tracer_finish(5 bpf_tracer_finish)-.-bpf_tracer_finish-i([Indicates that all probes have been set])
    
    bpf_tracer_init --> bpf_tracer_init-1(3.1 set bpf_jit_enable,sys_boot_time,max_locked_memory)
    bpf_tracer_init --> bpf_tracer_init-2(3.2 set log)
    bpf_tracer_init --> bpf_tracer_init-3(3.3 new thread - ctrl_main)
    bpf_tracer_init --> bpf_tracer_init-4(3.4 register kick_kern)

    running_socket_tracer --> running_socket_tracer-1(4.1 check_kernel_version)-.- running_socket_tracer-1-i([Support linux 4.14+])
    running_socket_tracer --> running_socket_tracer-2(4.2 socket_tracer_set_probes)
    running_socket_tracer-2 --> running_socket_tracer-2-1(4.2.1 Set kprobe,uprobe for attach/detach)
    running_socket_tracer-2 --> running_socket_tracer-2-2(4.2.2 collect_go_uprobe_syms_from_procfs)
    running_socket_tracer-2 --> running_socket_tracer-2-3(4.2.3 collect_ssl_uprobe_syms_from_procfs)
    running_socket_tracer --> running_socket_tracer-3(4.3 extra events list init) -.- running_socket_tracer-3-i([process exec/exit event snoop])
    running_socket_tracer --> running_socket_tracer-6(4.4 setup_bpf_tracer)

    running_socket_tracer --> running_socket_tracer-4(4.5 maps_config)-.-running_socket_tracer-4-i([set socket/trace map entries count])
    running_socket_tracer --> running_socket_tracer-5(4.6 set perf buffer reader callback) 
    running_socket_tracer --> running_socket_tracer-7(4.7 set socket_map_max_reclaim value) 
    running_socket_tracer --> running_socket_tracer-8(4.8 tracer_bpf_load)-.-running_socket_tracer-8-i([load ebpf progs and create maps])
    running_socket_tracer --> running_socket_tracer-9(4.9 tracer_probes_init)-.-running_socket_tracer-9-i([Call create_probe, prepare all probes before attach/detach])
    running_socket_tracer --> running_socket_tracer-10(4.10 update_offset_map_from_btf_vmlinux)-.-running_socket_tracer-10-i([Get the offsets from the BTF files, these offsets are used by the eBPF programs])
    running_socket_tracer-10 -.-> |store member offset to map |members_offset_map[(members_offset map)] 
    running_socket_tracer --> running_socket_tracer-11(4.11 update_proc_info_to_map) -.- running_socket_tracer-11-i([set go/ssl uprobe offsets])
    running_socket_tracer-11-.->|store uprobe offsets for the executable file|proc_info_map[(proc_info_map)]
    running_socket_tracer --> running_socket_tracer-12(4.12 update_protocol_filter_array)
    running_socket_tracer --> running_socket_tracer-13(4.13 tracer_hooks_attach) -.-running_socket_tracer-13-i([attach all probes])
    running_socket_tracer --> running_socket_tracer-14(4.14 perf_map_config) -.- running_socket_tracer-14-i([set perf buffer reader share memory])
    running_socket_tracer --> running_socket_tracer-15(4.15 set trace uid) -.- running_socket_tracer-15-i([use for socketID,traceID,capSeq])
    running_socket_tracer-15 -.-> |Prepare various UIDs to map|tracer_ctx_map[(tracer_ctx_map)]
    running_socket_tracer --> running_socket_tracer-16(4.16 dispatch_worker) -.- running_socket_tracer-16-i([userspace receive and distribute eBPF data to work queue initialization])
    running_socket_tracer --> running_socket_tracer-17(4.17 register_extra_waiting_op server/client for getting kernel struct member offset)
    running_socket_tracer --> running_socket_tracer-18(4.18 register_period_event_op kick kernel for getting kernel struct member offset)
    running_socket_tracer --> running_socket_tracer-19(4.19 new thread for process events)
```

## Kernel eBPF process

```mermaid
graph TD
subgraph Symbols
    style illustration-func fill:#ccff,color:#000, stroke-width:2px
    style illustration-info fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style illustration-code-block stroke-width:2px
    illustration-func(1 Function Name)
    illustration-info([Short description])
    illustration-code-block(2 Code block)
    illustration_map[(eBPF map)]
end
    style check_data-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style drop_msg_by_comm-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style eBPF_start fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style RET fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style process_syscall_data fill:#ccff,color:#000, stroke-width:2px
    style get_tcp_read_seq_from_fd fill:#ccff,color:#000, stroke-width:2px
    style get_tcp_write_seq_from_fd fill:#ccff,color:#000, stroke-width:2px
    style process_data fill:#ccff,color:#000, stroke-width:2px
    style trace_map__update stroke-width:2px
    style trace_map__delete stroke-width:2px
    style is_tcp_udp_data fill:#ccff,color:#000, stroke-width:2px
    style init_conn_info fill:#ccff,color:#000, stroke-width:2px
    style inferclass fill:#ccff,color:#000, stroke-width:2px
    style drop_msg_by_comm fill:#ccff,color:#000, stroke-width:2px
    style infer_protocol fill:#ccff,color:#000, stroke-width:2px
    style bpf_perf_event_output fill:#ccff,color:#000, stroke-width:2px
    style trace_process fill:#ccff,color:#000, stroke-width:2px
    style check_data stroke-width:2px
    style create_socket_info stroke-width:2px
    style set_data_from_socket_info stroke-width:2px
    style infer_message stroke-width:2px
    style IFPROTO stroke-width:2px
    style if_direction stroke-width:2px
    style burst_send_data stroke-width:2px
    style if_socket_info stroke-width:2px
    style socket_id stroke-width:2px
    style data_submit fill:#ccff,color:#000, stroke-width:2px

    eBPF_start[A syscall start]
    syscall_entry(1 syscall write/read entry hooks)
    args_map[(read/write args map)]
    syscall_exit(2 syscall write/read exit hooks)
    process_syscall_data(3 process_syscall_data)
    process_data(4 process_data)
    is_tcp_udp_data(4.1 is_tcp_udp_data)
    init_conn_info(4.2 init_conn_info)
    inferclass(4.3 infer_l7_class)
    socket_info_map[(socket_info_map)]
    socket_info_map-1[(socket_info_map)]
    infer_protocol(5 infer_protocol)
    check_data(5.1 check sk_type,sk_state)
    drop_msg_by_comm(5.2 drop_msg_by_comm)
    infer_message(5.3 Infer various protocols, e.g. HTTP/HTTP2/MySQL/DOUBBU ...)
    data_submit(6 data_submit)
    get_tcp_read_seq_from_fd(6.1 get_tcp_read_seq_from_fd)
    get_tcp_write_seq_from_fd(6.2 get_tcp_write_seq_from_fd)
    socket_id(6.3 Confirm socketID)
    tracer_ctx_map[(tracer_ctx_map)]
    trace_process(6.4 trace_process)
    eBPF_start --> syscall_entry-.->|store args to map| args_map
    args_map -.->|fetch args | syscall_exit
    syscall_exit-->process_syscall_data-->process_data
    process_data-->is_tcp_udp_data
    process_data-->init_conn_info
    process_data-->inferclass
    inferclass-->infer_protocol
    infer_protocol-->check_data-.-check_data-i([e.g. tcp, tcp status verify])
    infer_protocol-->drop_msg_by_comm-.-drop_msg_by_comm-i([drop ssh, sshd])
    infer_protocol-->infer_message
    socket_info_map -.->|fetch socket info for inference| infer_message
    infer_message --> IFPROTO{Is a valid protocol ?}
    IFPROTO ---->|No| RET[return]
    IFPROTO -->|Yes| data_submit
    data_submit-->socket_id
    data_submit-->if_direction{dir == T_INGRESS ?}
    if_direction-->|Yes|get_tcp_read_seq_from_fd
    if_direction-->|No|get_tcp_write_seq_from_fd
    data_submit-->trace_process
    subgraph trace
    trace_process-->if_direction_trace{dir == T_INGRESS ?}
    if_direction_trace-->|Yes|trace_map__update(6.4.1 Add new trace info)
    if_direction_trace-->|No|trace_map__delete(6.4.2 Determine the traceID, delete the trace_map entry)
    end
    tracer_ctx_map -.->|get uid for traceID| trace_map__update-.->|Add new traceID|trace_map[(trace_map)]
    data_submit-->if_socket_info{is socket_info exists?}
    if_socket_info-->|Yes|set_data_from_socket_info(6.6 set data from socketinfo)
    if_socket_info-->|No|create_socket_info(6.5 create socket info)-->set_data_from_socket_info
    create_socket_info-.->socket_info_map-1
    data_submit-->burst_send_data(6.7 burst send data)--> bpf_perf_event_output(bpf_perf_event_output)-.->socket_data_map[(socket_data map)]
```

## Receive data

```mermaid
graph TD
subgraph Symbols
    style illustration-thread fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style illustration-func fill:#ccff,color:#000, stroke-width:2px
    style illustration-code-block stroke-width:2px
    illustration-func(1 Function Name)
    illustration-code-block(2 Code block)
    illustration-thread>thread]
    illustration-ring{{ -- queue -- }}
end
    style perf_buffer_read fill:#ccff,color:#000, stroke-width:2px
    style start  fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style worker  fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style worker_1  fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style worker_2  fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style perf_reader_poll fill:#ccff,color:#000, stroke-width:2px
    style process_data fill:#ccff,color:#000, stroke-width:2px
    style reader_raw_cb fill:#ccff,color:#000, stroke-width:2px
    style register_events_handle fill:#ccff,color:#000, stroke-width:2px
    style dispatch_queue_index fill:#ccff,color:#000, stroke-width:2px
    style prefetch_and_process_data fill:#ccff,color:#000, stroke-width:2px
    style rust_callback stroke-width:2px
    style free stroke-width:2px

    start>thread 'perf-reader'] --- perf_buffer_read(1 perf_buffer_read)-->|1|perf_reader_poll(2 perf_reader_poll)-->|2|reader_raw_cb(3 reader_raw_cb)
    reader_raw_cb-->|3|register_events_handle(4 register_events_handle)
    reader_raw_cb-->|4|dispatch_queue_index(5 dispatch_queue_index)
    reader_raw_cb-->|5|copy_data_and_enqueue(6 Copy socket data and enqueue)
    subgraph Copy socket data and enqueue
    id0((data))-->|ring_sp_enqueue_burst|ring0{{ -- queue 0 -- }}-.-worker>worker thread-1] --- process_data(7 process_data)
    id1((data))-->|ring_sp_enqueue_burst|ring1{{ -- queue 1 -- }}-.-worker_1>worker thread-2] --- process_data
    idn((data))-->|ring_sp_enqueue_burst|ring2{{ -- queue n -- }}-.-worker_2>worker thread-n] --- process_data
    end
    copy_data_and_enqueue---|6|id0
    copy_data_and_enqueue-->|7 wakeup worker threads|process_data
    process_data-->|ring_sc_dequeue_burst|prefetch_and_process_data(8 prefetch_and_process_data)
    prefetch_and_process_data-->rust_callback(9 Call rust callback func)
    rust_callback-->free(free data)

    copy_data_and_enqueue-->|8 loop|perf_buffer_read
    free-->|loop|process_data
```

## Uprobe userspace boot

```mermaid
graph TD
subgraph Symbols
    style illustration-func fill:#ccff,color:#000, stroke-width:2px
    style illustration-info fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style illustration-code-block stroke-width:2px
    illustration-func(1 Function Name)
    illustration-info([Short description])
    illustration-code-block(2 Code block)
    illustration_map[(eBPF map)]
end
    style bcc_elf_foreach_sym-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style function_address-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style resolve_func_ret_addr-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style start fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style running_socket_tracer fill:#ccff,color:#000, stroke-width:2px
    style running_socket_tracer-2 fill:#ccff,color:#000, stroke-width:2px
    style collect_go_uprobe_syms_from_procfs fill:#ccff,color:#000, stroke-width:2px
    style proc_parse_and_register fill:#ccff,color:#000, stroke-width:2px
    style fetch_go_elf_version fill:#ccff,color:#000, stroke-width:2px
    style collect_ssl_uprobe_syms_from_procfs fill:#ccff,color:#000, stroke-width:2px
    style IFGETVER stroke-width:2px
    style collect_ssl_detail stroke-width:2px
    style RET fill:#fff,color:#000,stroke:#000,stroke-width:2px 
    style resolve_bin_file fill:#ccff,color:#000, stroke-width:2px
    style resolve_and_gen_uprobe_symbol fill:#ccff,color:#000, stroke-width:2px
    style bcc_elf_foreach_sym fill:#ccff,color:#000, stroke-width:2px
    style function_address fill:#ccff,color:#000, stroke-width:2px
    style resolve_func_ret_addr fill:#ccff,color:#000, stroke-width:2px
    style add_uprobe_symbol fill:#ccff,color:#000, stroke-width:2px
    style struct_member_offset_analyze fill:#ccff,color:#000, stroke-width:2px
    style tracer_probes_init fill:#ccff,color:#000, stroke-width:2px
    style tracer_hooks_attach fill:#ccff,color:#000, stroke-width:2px
    style update_proc_info_to_map fill:#ccff,color:#000, stroke-width:2px
    style add_uprobe_symbol-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style tracer_hooks_attach-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px
    style struct_member_offset_analyze-i fill:#cf0,color:#000,stroke:#360,stroke-width:2px

    start[tracer start]
    start --> running_socket_tracer(1 running_socket_tracer)
    running_socket_tracer --> running_socket_tracer-2(2 socket_tracer_set_probes)
    running_socket_tracer-2 --> collect_go_uprobe_syms_from_procfs(3 collect_go_uprobe_syms_from_procfs)
    running_socket_tracer-2 --> collect_ssl_uprobe_syms_from_procfs(4 collect_ssl_uprobe_syms_from_procfs)

    collect_go_uprobe_syms_from_procfs-->proc_parse_and_register(3.1 proc_parse_and_register)
    proc_parse_and_register-->fetch_go_elf_version(3.1.1 fetch_go_elf_version)
    fetch_go_elf_version-->IFGETVER{Can you get the version?}
    IFGETVER -->|No| RET[return]
    IFGETVER -->|Yes| resolve_bin_file(3.1.2 resolve_bin_file)
    resolve_bin_file-->resolve_and_gen_uprobe_symbol(3.1.2.1 resolve_and_gen_uprobe_symbol)
    resolve_and_gen_uprobe_symbol-->bcc_elf_foreach_sym(3.1.2.1.1 bcc_elf_foreach_sym)-.-bcc_elf_foreach_sym-i([Call libbcc-bpf.a function])
    resolve_and_gen_uprobe_symbol-->function_address(3.1.2.1.2 function_address)-.-function_address-i([Golang program parsing for no symbols])
    resolve_and_gen_uprobe_symbol-->resolve_func_ret_addr(3.1.2.1.3 resolve_func_ret_addr)-.-resolve_func_ret_addr-i([Parse the return address of the golang interface])

    resolve_bin_file-->add_uprobe_symbol(3.1.2.1 add_uprobe_symbol)
    add_uprobe_symbol-.-add_uprobe_symbol-i([add uprobe-symbol to conf->uprobe_syms_head])
    resolve_bin_file-->struct_member_offset_analyze(3.1.2.2 struct_member_offset_analyze)-.-struct_member_offset_analyze-i([resolve all offsets for the bin,add to proc_info_head])
    collect_ssl_uprobe_syms_from_procfs-->collect_ssl_detail(...)
    running_socket_tracer-->tracer_probes_init(5 tracer_probes_init)
    running_socket_tracer --> update_proc_info_to_map(6 update_proc_info_to_map)
    update_proc_info_to_map-.->|store uprobe offsets for the executable file|proc_info_map[(proc_info_map)]
    running_socket_tracer-->tracer_hooks_attach(7 tracer_hooks_attach)-.-tracer_hooks_attach-i([attach all uprobe probes])
```


**Explanation:**

- 3 collect_go_uprobe_syms_from_procfs
  - Find all golang binary executables from Procfs,parse and register uprobe symbols.
- 3.1.2 resolve_bin_file
  - Resolve our pre-defined all symbols in the specified file.
- 3.1.2.1 resolve_and_gen_uprobe_symbol
  - Finish parsing the given symbol in the binary and generate the uprobe_symbol.

## Probes management

For all probes we provide two APIs(socket_tracer_start()/socket_tracer_stop()) to enable/disable probes, it will attach/detach all probes.

### Uprobes monitor

```mermaid
graph LR
    style perf_buffer_read fill:#ccff,color:#000, stroke-width:2px
    style perf_reader_poll fill:#ccff,color:#000, stroke-width:2px
    style reader_raw_cb fill:#ccff,color:#000, stroke-width:2px
    style perf_buffer stroke-width:2px
    style hook fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style hook1 fill:#fff,color:#000,stroke:#000,stroke-width:2px
    style hook2 fill:#fff,color:#000,stroke:#000,stroke-width:2px
    subgraph kernel
    hook[process events snoop]
    hook1[1 hook: sched_process_fork]
    hook2[2 hook: sched_process_exit]
    hook---hook1
    hook---hook2
    end
    perf_buffer{{... perf buffer queue ...}}

    hook1-.->perf_buffer
    hook2-.->perf_buffer
    perf_buffer-.-> Buffer-Reader(Buffer-Reader)
    subgraph user
    Buffer-Reader---perf_buffer_read(3 perf_buffer_read)-->reader_event_read(4 reader_event_read)-->reader_raw_cb(5 reader_raw_cb)
    end 
```

```mermaid
graph TD
    style reader_raw_cb fill:#ccff,color:#000, stroke-width:2px
    style register_events_handle fill:#ccff,color:#000, stroke-width:2px
    style process_event fill:#ccff,color:#000, stroke-width:2px
    style go_process_exec fill:#ccff,color:#000, stroke-width:2px
    style ssl_process_exec fill:#ccff,color:#000, stroke-width:2px
    style go_process_exit fill:#ccff,color:#000, stroke-width:2px
    style ssl_process_exit fill:#ccff,color:#000, stroke-width:2px
    style add_event_to_proc_header fill:#ccff,color:#000, stroke-width:2px
    style list_add_tail stroke-width:2px
    style clear_probes_by_pid  fill:#ccff,color:#000, stroke-width:2px
    style probe_detach fill:#ccff,color:#000, stroke-width:2px
    style free_probe_from_tracer fill:#ccff,color:#000, stroke-width:2px
    reader_raw_cb(5 reader_raw_cb)
    reader_raw_cb-->register_events_handle(6 register_events_handle)
    register_events_handle-->process_event(7 process_event)
    register_events_handle-->worker_ring{{... worker ring ...}}-.-rust_extra_events_callback(8 rust extra events callback)
    process_event-->go_process_exec(9 go_process_exec)
    process_event-->ssl_process_exec(10 ssl_process_exec)
    process_event-->go_process_exit(11 go_process_exit)
    process_event-->ssl_process_exit(12 ssl_process_exit)
    go_process_exec-->add_event_to_proc_header(9.1 add_event_to_proc_header)
    add_event_to_proc_header-->list_add_tail(9.1.1 add list to proc_events_head)
    go_process_exit-->clear_probes_by_pid(11.1 clear_probes_by_pid)
    clear_probes_by_pid-->probe_detach(11.1.1 probe_detach)
    clear_probes_by_pid-->free_probe_from_tracer(11.1.2 free_probe_from_tracer)
```

```mermaid
graph LR
    style process_events_handle_main fill:#ccff,color:#000, stroke-width:2px
    style go_process_events_handle fill:#ccff,color:#000, stroke-width:2px
    style ssl_events_handle fill:#ccff,color:#000, stroke-width:2px
    style list_first_entry stroke-width:2px
    style process_execute_handle fill:#ccff,color:#000, stroke-width:2px
    style update_proc_info_to_map fill:#ccff,color:#000, stroke-width:2px
    style tracer_hooks_process fill:#ccff,color:#000, stroke-width:2px
    style tracer_uprobes_update fill:#ccff,color:#000, stroke-width:2px
    style proc_parse_and_register fill:#ccff,color:#000, stroke-width:2px
    style clear_probes_by_pid fill:#ccff,color:#000, stroke-width:2px

    subgraph process events
    start>thread 'proc-events']---process_events_handle_main(13 process_events_handle_main)
    process_events_handle_main-->|1|go_process_events_handle(14 go_process_events_handle)
    process_events_handle_main-->|9|ssl_events_handle(15 ssl_events_handle)
    list_first_entry[14.1 fetch a event]
    go_process_events_handle-->|2|process_execute_handle(14.2 process_execute_handle)
    process_execute_handle-->|4|clear_probes_by_pid(14.2.1 clear_probes_by_pid)
    process_execute_handle-->|5|proc_parse_and_register(14.2.2 proc_parse_and_register)
    process_execute_handle-->|6|tracer_uprobes_update(14.2.3 tracer_uprobes_update)
    process_execute_handle-->|7|tracer_hooks_process(14.2.4 tracer_hooks_process)
    process_execute_handle-->|8|update_proc_info_to_map(14.2.5 update_proc_info_to_map)
    list_first_entry-.->|3 get one event from list|process_execute_handle
    process_execute_handle-.->|10 loop|process_events_handle_main
    end
```

**Explanation:**

- 7 process_event 
  - According to the event_type is EVENT_TYPE_PROC_EXEC or EVENT_TYPE_PROC_EXIT to determine the final call interface. EVENT_TYPE_PROC_EXEC(call go_process_exec(), ssl_process_exec),EVENT_TYPE_PROC_EXIT(call go_process_exit(), ssl_process_exit()).
- 8 rust extra events callback
  - We provide a function that the user can register a callback interface for a specific event. e.g. Use rust function process these events.
- 9.1 add_event_to_proc_header
  - Add `struct process_event` to list-head(proc_events_head), need to set a expire time in `struct process_event`, see the description of [TLS/SSL Tracing](https://github.com/deepflowio/deepflow/tree/main/agent/src/ebpf#tlsssl-tracing) for the reason.
- 14.2.1 clear_probes_by_pid
  - Clear all probe, when process id == pid (event fetched).
- 14.2.2 proc_parse_and_register
  - Resolve symbols and register uprobe symbols.
- 14.2.3 tracer_uprobes_update
  - Update uprobes list
- 14.2.4 tracer_hooks_process
  - Execute attach probes
- 14.2.5 update_go_offsets_to_map
  - Update proc_info_map

# Tested kernel version

- 4.14.x
  - 4.14.0, 4.14.10, 4.14.11, 4.14.1, 4.14.12, 4.14.13, 4.14.14, 4.14.15, 4.14.2, 4.14.3, 4.14.4, 4.14.5, 4.14.6, 4.14.7, 4.14.8, 4.14.9
- 4.15.x
  - 4.15.0, 4.15.1, 4.15.10, 4.15.11, 4.15.12, 4.15.13, 4.15.14, 4.15.15, 4.15.2, 4.15.3, 4.15.4, 4.15.5, 4.15.6, 4.15.7, 4.15.8, 4.15.9
- 4.16.x
  - 4.16.0, 4.16.1, 4.16.10, 4.16.11, 4.16.12,4.16.13, 4.16.2, 4.16.3, 4.16.4, 4.16.5, 4.16.6, 4.16.7, 4.16.8, 4.16.9
- 4.17.x
  - 4.17.0, 4.17.1, 4.17.10, 4.17.11,4.17.12, 4.17.13, 4.17.14, 4.17.2, 4.17.3, 4.17.4, 4.17.5, 4.17.6, 4.17.8, 4.17.9 
- 4.18.x
  - 4.18.0, 4.18.1, 4.18.10,4.18.11, 4.18.12, 4.18.13, 4.18.14, 4.18.15, 4.18.16, 4.18.3, 4.18.4, 4.18.5, 4.18.6, 4.18.7, 4.18.8, 4.18.9
- 4.19.x
  - 4.19.0, 4.19.1, 4.19.10, 4.19.11, 4.19.12, 4.19.2, 4.19.3, 4.19.4, 4.19.5, 4.19.6, 4.19.7, 4.19.8, 4.19.9 
- 4.20.x
  - 4.20.0,4.20.1, 4.20.10, 4.20.11, 4.20.12, 4.20.13, 4.20.2, 4.20.3, 4.20.4, 4.20.5, 4.20.6, 4.20.7, 4.20.8
- 5.0.x
  - 5.0.0, 5.0.0,5.0.1, 5.0.10, 5.0.11, 5.0.12, 5.0.13, 5.0.2, 5.0.3, 5.0.4, 5.0.5, 5.0.6, 5.0.7, 5.0.8, 5.0.9
- 5.1.x
  - 5.1.0, 5.1.1, 5.1.1,5.1.10, 5.1.11, 5.1.12, 5.1.14, 5.1.15, 5.1.16, 5.1.2, 5.1.3, 5.1.4, 5.1.5, 5.1.6, 5.1.7, 5.1.8, 5.1.9
- 5.2.x
  - 5.2.0, 5.2.1, 5.2.1, 5.2.10, 5.2.11, 5.2.12, 5.2.13, 5.2.14, 5.2.2, 5.2.3, 5.2.4, 5.2.5, 5.2.6, 5.2.7, 5.2.8, 5.2.9
- 5.3.x
  - 5.3.0, 5.3.1, 5.3.1, 5.3.10, 5.3.11, 5.3.12, 5.3.13, 5.3.2, 5.3.4, 5.3.5, 5.3.6, 5.3.7, 5.3.8, 5.3.9
- 5.4.x
  - 5.4.0, 5.4.1, 5.4.1, 5.4.10, 5.4.11, 5.4.12, 5.4.13, 5.4.14, 5.4.15, 5.4.2, 5.4.3, 5.4.4, 5.4.5, 5.4.6, 5.4.7, 5.4.8
- 5.5.x
  - 5.5.0, 5.5.1, 5.5.10, 5.5.11, 5.5.12, 5.5.13, 5.5.2, 5.5.3, 5.5.4, 5.5.5, 5.5.6, 5.5.6, 5.5.7, 5.5.8, 5.5.9
- 5.6.x
  - 5.6.0, 5.6.1, 5.6.10, 5.6.11, 5.6.12, 5.6.13, 5.6.14, 5.6.15, 5.6.2, 5.6.3, 5.6.4, 5.6.5, 5.6.6, 5.6.7, 5.6.8, 5.6.9
- 5.7.x
  - 5.7.0, 5.7.1, 5.7.10, 5.7.11, 5.7.12, 5.7.2, 5.7.3, 5.7.4, 5.7.5, 5.7.6, 5.7.7, 5.7.8, 5.7.9
- 5.8.x
  - 5.8.0, 5.8.1, 5.8.10, 5.8.11, 5.8.12, 5.8.13, 5.8.14, 5.8.2, 5.8.3, 5.8.4, 5.8.5, 5.8.6, 5.8.7, 5.8.8, 5.8.9
- 5.9.x
  - 5.9.0, 5.9.1, 5.9.10, 5.9.11, 5.9.12, 5.9.13, 5.9.14, 5.9.2, 5.9.3, 5.9.5, 5.9.6, 5.9.7, 5.9.8, 5.9.9
- 5.10.x
  - 5.10.0, 5.10.1, 5.10.10, 5.10.11, 5.10.12, 5.10.13, 5.10.14, 5.10.15, 5.10.16, 5.10.2, 5.10.3, 5.10.4, 5.10.5, 5.10.6, 5.10.7, 5.10.8, 5.10.9
- 5.11.x
  - 5.11.0, 5.11.1, 5.11.10, 5.11.11, 5.11.12, 5.11.13, 5.11.14, 5.11.15, 5.11.16, 5.11.2, 5.11.3, 5.11.4, 5.11.5, 5.11.6, 5.11.7, 5.11.8, 5.11.9
- 5.12.x
  - 5.12.0, 5.12.1, 5.12.10, 5.12.11, 5.12.12, 5.12.13, 5.12.2, 5.12.3, 5.12.4, 5.12.9
- 5.13.x
  - 5.13.0, 5.13.1, 5.13.2, 5.13.3, 5.13.4, 5.13.5, 5.13.6, 5.13.7, 5.13.8
- 5.14.x
  - 5.14.0, 5.14.1, 5.14.2, 5.14.3, 5.14.4, 5.14.5, 5.14.6, 5.14.7, 5.14.8, 5.14.9, 5.14.9, 5.14.10, 5.14.11, 5.14.12, 5.14.13, 5.14.14, 5.14.15
- 5.15.x
  - 5.15.0, 5.15.1, 5.15.2, 5.15.3, 5.15.4, 5.15.5, 5.15.6, 5.15.7, 5.15.8, 5.15.10, 5.15.11, 5.15.12, 5.15.13
- 5.16.x
  - 5.16.0, 5.16.1, 5.16.2, 5.16.4, 5.16.5, 5.16.6, 5.16.7
