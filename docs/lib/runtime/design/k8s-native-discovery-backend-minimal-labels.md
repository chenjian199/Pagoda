# 用最少 Labels 的原生 Kubernetes 对象替代 Discovery CRD

本文档专门回答两个问题：

1. 在保持上层 discovery 接口不变的前提下，如何把 Kubernetes backend 的 CRD 落盘替换成原生对象落盘
2. 是否真的可以“不用 labels”，如果不能，最少需要哪些 labels

这里默认保持下面这些上层抽象**完全不改**：

- `DiscoverySpec`
- `DiscoveryInstance`
- `DiscoveryQuery`
- `DiscoveryEvent`
- `Discovery` trait 的 `register / unregister / list / list_and_watch`

也就是说，改动只发生在 kube backend 内部的“如何落盘到 kube-apiserver”。

## 1. 设计结论

采用下面这套原生对象映射：

- `Endpoint` -> `Service + EndpointSlice`
- `Model` -> `ConfigMap`
- `EventChannel` -> `Lease`

并采用“**最少 labels**”原则：

- 能靠 namespace、对象名、ownerReference、标准字段解决的，不额外打自定义 label
- 只有在下面两种情况下才使用 label：
  - Kubernetes 原生对象本身就要求的标准 label
  - 不加 label 会让 `list/watch` 退化成“扫全 namespace 全量过滤”的关键查询路径

## 2. 为什么不能完全零 labels

## 2.1 EndpointSlice 天生就需要标准 label

只要使用 `EndpointSlice` 关联 `Service`，就离不开标准 label：

- `kubernetes.io/service-name=<service>`

这是 Kubernetes 识别某个 `EndpointSlice` 属于哪个 `Service` 的标准方式。

因此，**endpoint 路径下的零 label 方案本身就不成立**。

## 2.2 不使用 labels 会让 list/watch 普遍退化

对于 `ConfigMap` 和 `Lease`，如果完全不使用 labels，那么 backend 在处理：

- `ComponentModels`
- `EndpointModels`
- `EventChannels(EventChannelQuery)`

时，通常只能：

1. 列整个 namespace 下所有 `ConfigMap` 或 `Lease`
2. 在客户端逐个解析名字 / data / annotation
3. 手动过滤

这在功能上是可行的，但代价是：

- watch 噪音更大
- 需要处理更多无关对象
- namespace 较大时成本更高
- 名字协议变得过于关键，一旦命名规则变更风险更高

所以结论不是“完全不需要 labels”，而是：

- **不需要很多 labels**
- **但需要保留一小组关键 labels**

## 3. 最少 labels 设计

## 3.1 Endpoint

### 存储对象

- `Service`
- `EndpointSlice`

### 最少 labels

`Service`：

- 可以为 `0` 个自定义 label

`EndpointSlice`：

- 必需：`kubernetes.io/service-name=<service>`
- 建议：`endpointslice.kubernetes.io/managed-by=dynamo-runtime`

### 为什么可以几乎不要自定义 labels

因为 endpoint 的定位大多可以通过以下信息完成：

- namespace
- `Service.metadata.name = service`
- `Service.spec.ports[].name = portname`
- `EndpointSlice` 的标准 service-name label
- `EndpointSlice` 名字采用确定性命名

### 名字建议

- `Service`：`<service>`
- `EndpointSlice`：`dyn-ep-<service>-<portname>-<pod-name>`

### 查询方式

- 精确查询某个 endpoint group：
  - 先按名字读 `Service`
  - 再按标准 label `kubernetes.io/service-name=<service>` 列 `EndpointSlice`
  - 再在客户端按 `portname` 过滤 slice port
- watch：
  - watch 参与 discovery 的 `EndpointSlice`
  - 从 ready 状态恢复 `DiscoveryInstance::Endpoint`

### 是否需要自己写 EndpointSlice

- 如果 endpoint membership 完全由 selector + readiness 决定，则不需要，Kubernetes 自动维护即可
- 如果要保留 Dynamo 当前这种“Pod 活着但临时摘掉某个 endpoint 实例”的能力，则需要自管 `EndpointSlice`

## 3.2 Model

### 存储对象

- `ConfigMap`

### 最少 labels

推荐只保留 1 个：

- `nvidia.com/dynamo-kind=model`

### 为什么至少建议保留这 1 个

因为如果完全没有 label，那么 backend 的 `list/watch` 很难把 discovery 专用 ConfigMap 和业务侧普通 ConfigMap 分离开。

只保留一个 `kind=model` label 的好处：

- 可以把 watch 范围限定在 discovery model 对象上
- 不需要再加 `service`、`portname` 之类 labels
- 精确过滤仍然可以靠对象名和 data 完成

### 名字建议

- base model：`dyn-model-<service>-<portname>-<instance-id>`
- lora model：`dyn-model-<service>-<portname>-<instance-id>-<suffix-hash>`

### data

- `card.json`
- `model_suffix`
- `service`
- `portname`
- `instance_id`
- `namespace`

### 查询方式

- watch 时只看 `kind=model` 的 ConfigMap
- 精确过滤时解析对象名或 `data.service` / `data.portname`
- 不依赖 `service` / `portname` labels

### 取舍

这比多 label 方案更省标签，但代价是：

- `EndpointModels` 查询不能完全由 label selector 下推
- 需要在客户端做二次过滤

## 3.3 EventChannel

### 存储对象

- `Lease`

### 最少 labels

推荐只保留 1 个：

- `nvidia.com/dynamo-kind=event-channel`

### 为什么至少建议保留这 1 个

与 `Model` 同理：

- 不加这个 label，就很难把 discovery event lease 和 namespace 中其他 lease 区分开
- 只保留一个 `kind` label，已经能显著缩小 watch 范围

### 名字建议

- `dyn-event-<service>-<portname>-<instance-id>`

### spec / annotations

`spec`：

- `holderIdentity = <instance-id>`
- `leaseDurationSeconds = <ttl>`
- `renewTime = now()`

`annotations`：

- `nvidia.com/dynamo-transport = <serialized EventTransport>`
- `nvidia.com/dynamo-service = <service>`
- `nvidia.com/dynamo-portname = <portname>`
- `nvidia.com/dynamo-topic = <topic>`

### 查询方式

- watch 时只看 `kind=event-channel` 的 Lease
- 精确过滤靠 annotation 和对象名解析
- 过期判断靠 `renewTime + leaseDurationSeconds`

## 4. 最少 labels 集合总结

推荐的最少 labels 集合如下。

### Endpoint

- `kubernetes.io/service-name=<service>`（必须，标准 label）
- `endpointslice.kubernetes.io/managed-by=dynamo-runtime`（建议，便于区分 controller）

### Model

- `nvidia.com/dynamo-kind=model`

### EventChannel

- `nvidia.com/dynamo-kind=event-channel`

也就是说，如果按最少化原则，你最终只需要：

- 1 个 endpoint 相关标准 label
- 1 个 endpointslice 管理者 label（建议）
- 1 个 model kind label
- 1 个 event kind label

而不需要把 `service`、`portname`、`instance-id`、`pod-name` 都做成 labels。

## 5. 新 backend 的内部结构

为了保持上层接口不动，建议在 kube backend 内部分成三个 store：

- `K8sEndpointStore`
- `K8sModelStore`
- `K8sEventChannelStore`

由 `KubeDiscoveryClient` 做分发。

## 5.1 register

- `DiscoverySpec::Endpoint` -> `endpoint_store.register(spec)`
- `DiscoverySpec::Model` -> `model_store.register(spec)`
- `DiscoverySpec::EventChannel` -> `event_store.register(spec)`

## 5.2 unregister

- `DiscoveryInstance::Endpoint` -> 删除或更新 `EndpointSlice`
- `DiscoveryInstance::Model` -> 删除对应 `ConfigMap`
- `DiscoveryInstance::EventChannel` -> 删除对应 `Lease`

## 5.3 list / list_and_watch

- endpoint queries -> 从 `Service + EndpointSlice` 恢复 `DiscoveryInstance::Endpoint`
- model queries -> 从 `ConfigMap` 恢复 `DiscoveryInstance::Model`
- event queries -> 从 `Lease` 恢复 `DiscoveryInstance::EventChannel`

## 6. 这种设计的优缺点

### 优点

- 上层完全不改
- 摆脱 CRD
- labels 数量非常少
- 仍能用 Kubernetes 原生对象承载三类实例
- 关键路径仍可避免全量扫描所有对象类型

### 缺点

- `Model` 和 `EventChannel` 的过滤更多依赖对象名 / data / annotation 解析
- 某些查询不能完全下推成 label selector，只能 backend 内部做二次过滤
- 实现复杂度低于“零 labels + 全量扫 namespace”，但高于“所有字段都打 labels”

## 7. 最终建议

如果你的目标是“尽量原生、尽量少 labels、但不牺牲太多可实现性”，推荐采用下面这版：

- `Endpoint`：只接受 Kubernetes 标准 label 和极少管理 label
- `Model`：只保留 `kind=model`
- `EventChannel`：只保留 `kind=event-channel`
- `service` / `portname` / `instance_id` 等信息放对象名、data、annotation 中

这基本就是“最少 labels 但仍然工程可落地”的平衡点。