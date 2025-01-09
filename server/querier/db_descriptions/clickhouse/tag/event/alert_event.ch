# Name                     , DisplayName                , Description
time_str                   , 触发时间                    ,
_id                        , UID                        ,
time                       , 触发时间                    , 将 end_time 取整到秒。

region                    , 区域                     ,
az                        , 可用区                   ,
host                      , 宿主机                   , 承载虚拟机的宿主机。
chost                     , 云服务器                 , 包括虚拟机、裸金属服务器。
vpc                       , VPC                      ,
l2_vpc                    , 转发 VPC                 , MAC 地址所在的 VPC。
subnet                    , 子网                     ,
router                    , 路由器                   ,
dhcpgw                    , DHCP 网关                ,
lb                        , 负载均衡器               ,
lb_listener               , 负载均衡监听器           ,
natgw                     , NAT 网关                 ,
redis                     , Redis                    ,
rds                       , RDS                      ,
pod_cluster               , K8s 容器集群             ,
pod_ns                    , K8s 命名空间             ,
pod_node                  , K8s 容器节点             ,
pod_ingress               , K8s Ingress              ,
pod_service               , K8s 容器服务             ,
pod_group_type            , K8s 工作负载类型         ,
pod_group                 , K8s 工作负载             , 例如 Deployment、StatefulSet、Daemonset 等。
pod                       , K8s 容器 POD             ,
service                   , 服务                     , 已废弃，请使用 pod_service
auto_instance_type        , 自动实例类型              , `auto_instance`实例对应的类型。
auto_instance             , 自动实例                  , IP 对应的实例，实例为IP时，auto_instance_id显示为子网ID。
auto_service_type         , 自动服务类型               , `auto_service`实例对应的类型。
auto_service              , 自动服务                   , 在`auto_instance`基础上，将容器服务的 ClusterIP 与工作负载聚合为服务，实例为IP时，auto_service_id显示为子网ID。
gprocess                  , 进程                     ,
tap_port_host             , 采集网卡所属宿主机       , 已废弃，请使用 capture_nic_host。
tap_port_chost            , 采集网卡所属云服务器     , 已废弃，请使用 capture_nic_chost。
tap_port_pod_node         , 采集网卡所属容器节点     , 已废弃，请使用 capture_nic_pod_node。
capture_nic_host          , 采集网卡所属宿主机       ,
capture_nic_chost         , 采集网卡所属云服务器     ,
capture_nic_pod_node      , 采集网卡所属容器节点     ,
host_ip                   , 宿主机                   , 宿主机的管理 IP。
host_hostname             , 宿主机                   , 宿主机的 Hostname。
chost_ip                  , 云服务器                 , 云服务器的主 IP。
chost_hostname            , 云服务器                 , 云服务器的 Hostname。
pod_node_ip               , K8s 容器节点             , 容器节点的主 IP。
pod_node_hostname         , K8s 容器节点             , 容器节点的 Hostname。

ip                        , IP 地址                  ,
is_internet               , Internet IP 标志         , IP 地址是否为外部 Internet 地址。
province                  , 省份                     , Internet IP 地址所属的省份。

tcp_flags_bit             , TCP 标志位集合           , 当前自然分钟内所有包中 TCP 标志位的集合。

l2_end                    , 二层边界                 , 表示是否是在客户端网卡或服务端网卡处采集的流量。
l3_end                    , 三层边界                 , 表示是否是在客户端或服务端所在二层网络内采集的流量。
nat_real_ip               , NAT IP 地址              , NAT 作用前（后）的真实 IP 地址，该值从 TOA（TCP Option Address）中提取，或者根据云平台中 VIP 与 RIP 的映射信息计算。
nat_real_port             , NAT Port                 , NAT 作用前的真实端口号，该值从 TOA（TCP Option Address）中提取。

process_id                , 进程 ID                  ,
process_kname             , 内核线程名               ,

k8s.label                 , K8s Label                , K8s 自定义 Label。
k8s.annotation            , K8s Annotation           ,
k8s.env                   , K8s Env                  ,
cloud.tag                 , Cloud Tag                ,
os.app                    , OS APP                   ,

user                       , 创建人                   ,
alert_policy               , 告警策略                 ,
policy_type                , 策略类型                 ,
event_level                , 事件等级                 ,
target_tags                , 监控对象标签              , 
_target_uid                ,                          , 
_query_region              , 查询区域                 ,
