### Table of Contents

**[DeepFlow release v6.6](#v6.6)**<br/>

# Changelog

### <a id="v6.6"></a>DeepFlow release v6.6

#### New Feature

#### Bug Fix
* fix: agnet - eBPF Fix the kernel kick on CPU0 was not triggered [#8818](https://github.com/deepflowio/deepflow/pull/8818) by [yinjiping](https://github.com/yinjiping)
* fix: agent - eBPF Fix Crashes Caused by Packet Count Statistics [#8815](https://github.com/deepflowio/deepflow/pull/8815) by [yinjiping](https://github.com/yinjiping)
* fix: wan type cidr may not be tagged [#8811](https://github.com/deepflowio/deepflow/pull/8811) by [lzf575](https://github.com/lzf575)
* fix: the throttler cannot write all data to the queue at once [#8792](https://github.com/deepflowio/deepflow/pull/8792) by [lzf575](https://github.com/lzf575)
* fix: tracemap error [#8755](https://github.com/deepflowio/deepflow/pull/8755) by [xiaochaoren1](https://github.com/xiaochaoren1)
* fix: agent - eBPF Fix process event type size [#8753](https://github.com/deepflowio/deepflow/pull/8753) by [yinjiping](https://github.com/yinjiping)
* fix: querier group by icon_id error [#8749](https://github.com/deepflowio/deepflow/pull/8749) by [xiaochaoren1](https://github.com/xiaochaoren1)
* fix: agent - eBPF Fix Event Type Value [#8744](https://github.com/deepflowio/deepflow/pull/8744) by [yinjiping](https://github.com/yinjiping)
* fix: failed to create agent group config using yaml [#8732](https://github.com/deepflowio/deepflow/pull/8732) by [ZhengYa-0110](https://github.com/ZhengYa-0110)
* fix: the analyzer mode supports ebpf dpdk [#8729](https://github.com/deepflowio/deepflow/pull/8729) by [yuanchaoa](https://github.com/yuanchaoa)
* fix: recorder fails to clean 10w data at one time [#8706](https://github.com/deepflowio/deepflow/pull/8706) by [ZhengYa-0110](https://github.com/ZhengYa-0110)
* fix: agent - eBPF Correct the maximum data push delay [#8703](https://github.com/deepflowio/deepflow/pull/8703) by [yinjiping](https://github.com/yinjiping)
* fix: Configuration text [#8702](https://github.com/deepflowio/deepflow/pull/8702) by [rvql](https://github.com/rvql)
* fix: fix ip filter error [#8692](https://github.com/deepflowio/deepflow/pull/8692) by [xiaochaoren1](https://github.com/xiaochaoren1)
* fix: modify log level [#8689](https://github.com/deepflowio/deepflow/pull/8689) by [yuanchaoa](https://github.com/yuanchaoa)
* fix: invalid value when migrating agent group config [#8687](https://github.com/deepflowio/deepflow/pull/8687) by [ZhengYa-0110](https://github.com/ZhengYa-0110)
* fix: query percentile to min/max time [#8686](https://github.com/deepflowio/deepflow/pull/8686) by [taloric](https://github.com/taloric)
* fix: invalid src_interface [#8682](https://github.com/deepflowio/deepflow/pull/8682) by [yuanchaoa](https://github.com/yuanchaoa)
* fix: Modify the range of the pcp value [#8679](https://github.com/deepflowio/deepflow/pull/8679) by [yuanchaoa](https://github.com/yuanchaoa)
* fix: failed to update old version agent group config when creating new version [#8665](https://github.com/deepflowio/deepflow/pull/8665) by [ZhengYa-0110](https://github.com/ZhengYa-0110)
* fix: nil pointer may cause panic [#8671](https://github.com/deepflowio/deepflow/pull/8671) by [lzf575](https://github.com/lzf575)
* fix: agent config file.io_event.collect_mode [#8669](https://github.com/deepflowio/deepflow/pull/8669) by [askyrie](https://github.com/askyrie)
* fix: adapt pprof u64 params [#8667](https://github.com/deepflowio/deepflow/pull/8667) by [taloric](https://github.com/taloric)
* fix: agent group configuration api response unexcepted value [#8655](https://github.com/deepflowio/deepflow/pull/8655) by [ZhengYa-0110](https://github.com/ZhengYa-0110)
* fix: add alarm_label table [#8644](https://github.com/deepflowio/deepflow/pull/8644) by [SongZhen0704](https://github.com/SongZhen0704)
* fix: modify agent config example [#8649](https://github.com/deepflowio/deepflow/pull/8649) by [yuanchaoa](https://github.com/yuanchaoa)
* fix: parse 1d aggr table failed [#8646](https://github.com/deepflowio/deepflow/pull/8646) by [lzf575](https://github.com/lzf575)
* fix: errors occurred when modifying some agent group configuration [#8605](https://github.com/deepflowio/deepflow/pull/8605) by [ZhengYa-0110](https://github.com/ZhengYa-0110)
* fix: TODOs in agent config [#8597](https://github.com/deepflowio/deepflow/pull/8597) by [rvql](https://github.com/rvql)
* fix: pcap,l4_packet,spantrace data exception [#8595](https://github.com/deepflowio/deepflow/pull/8595) by [lzf575](https://github.com/lzf575)
* fix: trim-tunnel-type has not taken effect [#8578](https://github.com/deepflowio/deepflow/pull/8578) by [yuanchaoa](https://github.com/yuanchaoa)

#### Performance
* perf: add primary key for in_process table [#8624](https://github.com/deepflowio/deepflow/pull/8624) by [lzf575](https://github.com/lzf575)
* perf: modify materialized view local table without group by [#8576](https://github.com/deepflowio/deepflow/pull/8576) by [lzf575](https://github.com/lzf575)

#### NEW FEATURE
* feat: chost support subnet filter [#8878](https://github.com/deepflowio/deepflow/pull/8878) by [xiaochaoren1](https://github.com/xiaochaoren1)
* feat: agent - eBPF DPDK User-Space Packet Statistics [#8807](https://github.com/deepflowio/deepflow/pull/8807) by [yinjiping](https://github.com/yinjiping)
* feat: Enable agent after first guard check [#8751](https://github.com/deepflowio/deepflow/pull/8751) by [rvql](https://github.com/rvql)
* feat: update vtap ignore fields [#8747](https://github.com/deepflowio/deepflow/pull/8747) by [askyrie](https://github.com/askyrie)
* feat: deprecated ipvlan [#8725](https://github.com/deepflowio/deepflow/pull/8725) by [askyrie](https://github.com/askyrie)
* feat: gets by page when refreshing recorder cache [#8711](https://github.com/deepflowio/deepflow/pull/8711) by [ZhengYa-0110](https://github.com/ZhengYa-0110)
* Revert "Revert "feat: querier optimization group"" [#8708](https://github.com/deepflowio/deepflow/pull/8708) by [xiaochaoren1](https://github.com/xiaochaoren1)
* Revert "feat: querier optimization group" [#8695](https://github.com/deepflowio/deepflow/pull/8695) by [xiaochaoren1](https://github.com/xiaochaoren1)
* feat: querier optimization group [#8691](https://github.com/deepflowio/deepflow/pull/8691) by [xiaochaoren1](https://github.com/xiaochaoren1)
* feat: agent - eBPF Optimize data push logic [#8653](https://github.com/deepflowio/deepflow/pull/8653) by [yinjiping](https://github.com/yinjiping)
* feat: querier supports show enum tags by language [#8673](https://github.com/deepflowio/deepflow/pull/8673) by [xiaochaoren1](https://github.com/xiaochaoren1)
* feat: add resource association abnormal alarm [#8659](https://github.com/deepflowio/deepflow/pull/8659) by [xiaochaoren1](https://github.com/xiaochaoren1)
* feat: adds resource synchronization delay alarms [#8657](https://github.com/deepflowio/deepflow/pull/8657) by [ZhengYa-0110](https://github.com/ZhengYa-0110)
* feat: sender add compress flag [#8654](https://github.com/deepflowio/deepflow/pull/8654) by [taloric](https://github.com/taloric)
* feat: support fanout in mirror mode and analyzer mode [#8643](https://github.com/deepflowio/deepflow/pull/8643) by [yuanchaoa](https://github.com/yuanchaoa)
* feat: optimize memory [#8572](https://github.com/deepflowio/deepflow/pull/8572) by [yuanchaoa](https://github.com/yuanchaoa)
* feat: querier add version check [#8622](https://github.com/deepflowio/deepflow/pull/8622) by [xiaochaoren1](https://github.com/xiaochaoren1)
* feat: register esxi vtap support cloudtower [#8613](https://github.com/deepflowio/deepflow/pull/8613) by [askyrie](https://github.com/askyrie)
* feature: adds index to some tables [#8598](https://github.com/deepflowio/deepflow/pull/8598) by [ZhengYa-0110](https://github.com/ZhengYa-0110)
* feature: adds system alarm of recorder cleaner [#8588](https://github.com/deepflowio/deepflow/pull/8588) by [ZhengYa-0110](https://github.com/ZhengYa-0110)

#### Documentation
* docs: update agent config doc [#8661](https://github.com/deepflowio/deepflow/pull/8661) by [sharang](https://github.com/sharang)


#### Refactoring
* refactor: add logs [#8786](https://github.com/deepflowio/deepflow/pull/8786) by [yuanchaoa](https://github.com/yuanchaoa)
* refactor: Remove legacy agent config [#8656](https://github.com/deepflowio/deepflow/pull/8656) by [rvql](https://github.com/rvql)
