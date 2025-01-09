# Name                     , DisplayName                , Description
time                       , 时间                       ,

region                     , 区域                       ,
az                         , 可用区                     ,
host                       , 宿主机                     , 承载虚拟机的宿主机。
chost                      , 云服务器                   , 包括虚拟机、裸金属服务器。
vpc                        , VPC                        ,
subnet                     , 子网                       ,
router                     , 路由器                     ,
dhcpgw                     , DHCP 网关                  ,
lb                         , 负载均衡器                 ,
lb_listener                , 负载均衡监听器             ,
natgw                      , NAT 网关                   ,
redis                      , Redis                      ,
rds                        , RDS                        ,
pod_cluster                , K8s 容器集群               ,
pod_ns                     , K8s 命名空间               ,
pod_node                   , K8s 容器节点               ,
pod_ingress                , K8s Ingress                ,
pod_service                , K8s 容器服务               ,
pod_group_type             , K8s 工作负载类型           ,
pod_group                  , K8s 工作负载               , 例如 Deployment、StatefulSet、Daemonset 等。
pod                        , K8s 容器 POD               ,
service                    , 服务                       , 已废弃，请使用 pod_service
auto_instance_type         , 自动实例类型                , `auto_instance`实例对应的类型。
auto_instance              , 自动实例                   , IP 对应的实例，实例为IP时，auto_instance_id显示为子网ID。
auto_service_type          , 自动服务类型                , `auto_service`实例对应的类型。
auto_service               , 自动服务                   , 在`auto_instance`基础上，将容器服务的 ClusterIP 与工作负载聚合为服务，实例为IP时，auto_service_id显示为子网ID。
gprocess                   , 进程                       ,
tap_port_host              , 采集网卡所属宿主机         , 已废弃，请使用 capture_nic_host。
tap_port_chost             , 采集网卡所属云服务器       , 已废弃，请使用 capture_nic_chost。
tap_port_pod_node          , 采集网卡所属容器节点       , 已废弃，请使用 capture_nic_pod_node。
capture_nic_host           , 采集网卡所属宿主机         ,
capture_nic_chost          , 采集网卡所属云服务器       ,
capture_nic_pod_node       , 采集网卡所属容器节点       ,
host_ip                    , 宿主机                     , 宿主机的管理 IP。
host_hostname              , 宿主机                     , 宿主机的 Hostname。
chost_ip                   , 云服务器                   , 云服务器的主 IP。
chost_hostname             , 云服务器                   , 云服务器的 Hostname。
pod_node_ip                , K8s 容器节点               , 容器节点的主 IP。
pod_node_hostname          , K8s 容器节点               , 容器节点的 Hostname。

k8s.label                  , K8s Label                  ,
k8s.annotation             , K8s Annotation             ,
k8s.env                    , K8s Env                    ,
cloud.tag                  , Cloud Tag                  ,
os.app                     , OS APP                     ,

ip                         , IP 地址                    ,
is_ipv4                    , IPv4 标志                  ,
is_internet                , Internet IP 标志           , IP 地址是否为外部 Internet 地址。
protocol                   , 网络协议                   ,

tunnel_type                , 隧道类型                   ,

server_port                , 服务端口                   ,

tap                        , 采集点                     , 已废弃，请使用 capture_network_type。
capture_network_type       , 网络位置                   , 采集流量的网络位置，使用固定值（云网络）表示云内流量，其他值表示传统 IDC 流量（支持最多 254 个自定义值表示镜像分光的位置）。
vtap                       , 采集器                     ,
agent                      , 采集器                     ,
nat_source                 , NAT 源                     ,
tap_port                   , 采集位置标识               , 已废弃，请使用 capture_nic。
tap_port_name              , 采集位置名称               , 已废弃，请使用 capture_nic_name。
tap_port_type              , 采集位置类型               , 已废弃，请使用 capture_nic_type。
capture_nic                , 采集网卡标识               , 当采集位置类型为本地网卡时，此值表示采集网卡的 MAC 地址后缀（后四字节）。
capture_nic_name           , 采集网卡名称               , 当采集位置类型为本地网卡时，此值表示采集网卡的名称。
capture_nic_type           , 采集网卡类型               , 表示流量采集位置的类型，包括本地网卡（云内流量）、云网关网卡（云网关流量）、分光镜像（传统 IDC 流量）等。
tap_side                   , 路径统计位置               , 采集位置在流量路径中所处的逻辑位置，例如客户端网卡、客户端容器节点、服务端容器节点、服务端网卡等。
observation_point          , 观测点                     , 采集位置在流量路径中所处的逻辑位置，例如客户端网卡、客户端容器节点、服务端容器节点、服务端网卡等。
signal_source              , 信号源                     ,
