# Kubernetes 原生发现并列实现代码审查说明

本文档用于帮助审查下面这组“并列存在、尚未接线”的新实现代码：它们保留上层 `Discovery` trait 和 `MetadataSnapshot` 语义不变，但把 Kubernetes 持久化对象从原来的 CRD 路径替换为原生 Kubernetes 对象。

## 1. 这份实现要解决什么问题

旧实现位于：

- `lib/runtime/src/discovery/kube.rs`
- `lib/runtime/src/discovery/kube/daemon.rs`
- `lib/runtime/src/discovery/kube/crd.rs`

旧实现的核心思路是：

1. 当前 Pod 把本地 `DiscoveryMetadata` 序列化到 `DynamoWorkerMetadata` CRD；
2. daemon 同时 watch `EndpointSlice` 与 `DynamoWorkerMetadata`；
3. daemon 把“ready Pod + 对应 CRD 元数据”拼成 `MetadataSnapshot`；
4. 上层 `list()` / `list_and_watch()` 只消费 snapshot，不直接关心 Kubernetes 对象细节。

这组新实现保持第 3、4 点不变，只替换第 1、2 点的落盘与读取来源：

- `Endpoint` 不再写 CRD，而是写 `Service + EndpointSlice`
- `Model` 不再写 CRD，而是写 `ConfigMap`
- `EventChannel` 不再写 CRD，而是写 `Lease`

也就是说，它的目标不是改上层 API，而是把“存到 K8s 的什么对象里”从 CRD 改为原生对象。

## 2. 代码入口与文件分工

本次并列实现的入口文件是：

- `lib/runtime/src/discovery/kube_native.rs`

它通过 `#[path = ...]` 明确引用了四类支撑文件：

- `lib/runtime/src/discovery/kube/native_daemon.rs`
- `lib/runtime/src/discovery/kube/native_objects.rs`
- `lib/runtime/src/discovery/kube/native_utils.rs`
- `lib/runtime/src/discovery/kube/service_registry.rs`

建议把这些文件理解成四层：

1. **入口层**：`kube_native.rs`
   - 定义 `NativeKubeDiscoveryClient`
   - 实现 `Discovery` trait
   - 负责把 register/unregister/list/list_and_watch 接到下面几层

2. **聚合层**：`native_daemon.rs`
   - 持续 watch Kubernetes 原生对象
   - 把原生对象重新聚合成 `MetadataSnapshot`
   - 让上层仍通过 snapshot 获得统一视图

3. **对象映射层**：`native_objects.rs`
   - 定义三类实例到原生对象的映射规则
   - 负责 apply/delete
   - 负责把对象反解回 `DiscoveryInstance`

4. **Endpoint 原生注册层**：`service_registry.rs`
   - 专门处理 `Service` / `EndpointSlice` 的构造与 apply/delete
   - 是 endpoint 注册路径里最基础的 K8s 原语封装

5. **运行环境辅助层**：`native_utils.rs`
   - 提供 `PodInfo`
   - 提供 `hash_pod_name`
   - 与旧 `kube/utils.rs` 平行存在，额外携带 `pod_ip`

## 3. 为什么要单独有 `kube_native.rs`

`kube_native.rs` 的作用不是简单复制旧 `kube.rs`，而是明确表达两个设计意图：

1. **这是一个并列实现，不是对旧实现的就地替换**；
2. **它的公共抽象边界仍然是 `Discovery` trait**。

因此，审查这个文件时可以重点看三件事：

### 3.1 它是否保持了旧的上层接口不变

`NativeKubeDiscoveryClient` 仍然实现：

- `instance_id()`
- `register_internal()`
- `unregister()`
- `list()`
- `list_and_watch()`

这意味着上层如果未来要切换 backend，理论上只需要在构造处选择新旧 client，而不需要改调用方的 trait 使用方式。

### 3.2 它是否仍然保留本地 `DiscoveryMetadata`

虽然它不再把 `DiscoveryMetadata` 写入 CRD，但仍然保留：

- `metadata: Arc<RwLock<DiscoveryMetadata>>`
- `metadata_watch: watch::Receiver<Arc<MetadataSnapshot>>`

原因有两个：

1. system status server 仍可继续复用本地 metadata；
2. 上层 `list()` / `list_and_watch()` 仍然基于 `MetadataSnapshot` 工作，避免把上层逻辑散落到 Kubernetes 对象细节中。

### 3.3 它是否把“写对象”和“聚合读取”解耦了

在 `kube_native.rs` 里：

- `register_internal()` / `unregister()` 只负责写入或删除原生对象；
- `list()` / `list_and_watch()` 只读 daemon 聚合出来的 snapshot；
- snapshot 的构建逻辑完全在 `native_daemon.rs`。

这个分层很重要，因为它保留了旧实现最有价值的性质：**写路径与读路径分离，watch 聚合逻辑集中在 daemon**。

## 4. 注册路径：`register_internal()` 到底做了什么

`NativeKubeDiscoveryClient::register_internal()` 的处理流程如下：

1. 根据 `DiscoverySpec` 生成带 `instance_id` 的 `DiscoveryInstance`
2. 先在本地 `DiscoveryMetadata` 中注册该实例
3. 再把该实例落盘到对应的原生 Kubernetes 对象
4. 如果原生对象持久化失败，则回滚本地 metadata

也就是说，这里仍然保留了旧实现的关键事务语义：

- **先更新内存态，再尝试持久化**
- **持久化失败时回滚内存态**

### 4.1 Endpoint 的注册

`DiscoveryInstance::Endpoint` 会调用：

- `metadata.register_endpoint(instance.clone())`
- `register_endpoint_instance(&self.kube_client, &self.pod_info, inst)`

后者最终会落到：

- `service_registry.rs` 中的 `build_service()` / `apply_service()`
- `service_registry.rs` 中的 `build_endpoint_slice()` / `apply_endpoint_slice()`

这里的设计含义是：

- `Service` 表示稳定的逻辑入口
- `EndpointSlice` 表示当前 Pod 的 ready endpoint 成员

但需要特别说明一点：**当前这份并列实现的读取侧并不会回读 `Service`**。

也就是说，在目前代码里：

- `Service` 是写入侧产出的“共享逻辑对象”
- `EndpointSlice` 才是 daemon 聚合 snapshot 时真正消费的数据源

这并不矛盾，因为两者承担的职责本来就不同：

1. `Service` 的职责更偏向 **原生 Kubernetes 发布语义**，用于给这组 endpoint 一个稳定名字，并与 Kubernetes 生态里“Service -> EndpointSlice”的常见模型保持一致；
2. `EndpointSlice` 的职责更偏向 **可读性与可聚合性**，因为它直接携带了 ready endpoint、target pod、port，以及当前实现额外写入的 annotations；
3. 对上层 `MetadataSnapshot` 来说，恢复 `DiscoveryInstance::Endpoint` 所需的信息已经都在 `EndpointSlice` 中，因此读取侧没有再去查 `Service`。

换句话说，当前代码里 `Service` 不是 snapshot 正确性的必要输入，而是一个面向 Kubernetes 原生语义和潜在外部消费者的发布对象。

这里还有一个值得审查的点：`unregister_endpoint_instance()` 现在会先删除当前 Pod 独占的 `EndpointSlice`，然后再检查同一 `Service` 下是否还存在相同 endpoint port 的其他 slice。

只有在该 endpoint 已经没有任何剩余 slice 成员时，它才会把对应 port 从共享 `Service` 中移除；如果移除后 `Service` 已经没有任何 port，则进一步删除整个 `Service`。这样才能兼顾两点：

- 不在单 Pod 反注册时误删其他 endpoint 仍在使用的共享 `Service`
- 又不会让已经失效的 endpoint port 永久残留在 `Service` 中

### 4.2 Model 的注册

`DiscoveryInstance::Model` 会调用：

- `metadata.register_model_card(instance.clone())`
- `apply_model_config_map(...)`

它把 model card 相关内容写到一个 `ConfigMap`：

- `namespace`
- `component`
- `endpoint`
- `instance_id`
- `card.json`
- 可选 `model_suffix`

### 4.3 EventChannel 的注册

`DiscoveryInstance::EventChannel` 会调用：

- `metadata.register_event_channel(instance.clone())`
- `apply_event_lease(...)`

它使用 `Lease` 来表达“事件发布者活着，而且 transport 语义有效”。

从抽象上看，`Lease` 在这里承担的是一种轻量心跳对象角色。

## 5. 反注册路径：`unregister()` 的回滚语义

`unregister()` 的结构与 `register_internal()` 基本对称：

1. 先从本地 `DiscoveryMetadata` 删除实例
2. 再删除对应 Kubernetes 原生对象
3. 如果删除失败，则把本地 metadata 回滚到删除前状态

对应关系如下：

- `Endpoint` → `unregister_endpoint_instance()`
- `Model` → `delete_model_config_map()`
- `EventChannel` → `delete_event_lease()`

这一点值得审查，因为它决定了“本地视图”和“外部可见对象”是否保持一致。

## 6. `native_daemon.rs`：为什么它是整个实现最关键的文件

如果说 `kube_native.rs` 只是一个 facade，那么 `native_daemon.rs` 才是这套并列实现能否成立的关键。

原因很简单：

> 上层并不直接读取 `Service` / `EndpointSlice` / `ConfigMap` / `Lease`；上层最终消费的仍然是 `MetadataSnapshot`。

所以，新的原生对象实现是否真的“兼容旧上层逻辑”，本质上取决于 daemon 能否把这些对象重新拼回旧语义。

### 6.1 daemon watch 了什么

`NativeDiscoveryDaemon::run()` 同时 watch 四类对象：

1. `EndpointSlice`
   - 过滤条件：`endpointslice.kubernetes.io/managed-by=dynamo-worker`
2. `Service`
   - 过滤条件：`nvidia.com/dynamo-discovery-mode=native-service`
3. `ConfigMap`
   - 过滤条件：`nvidia.com/dynamo-kind=model`
4. `Lease`
   - 过滤条件：`nvidia.com/dynamo-kind=event-channel`

这四路 reflector 共用一个 `Notify`，任意一类对象有变更，都会触发一次重新聚合。

这里也能看出修正后的 `Service` 定位：daemon **会显式 watch `Service`**，因为 `Endpoint` 的注册事实与端口声明现在由 `Service` 提供，而 `EndpointSlice` 只提供 ready 成员与 Pod 绑定关系。

### 6.2 为什么仍然保留 debounce

与旧 `kube/daemon.rs` 一样，新 daemon 也保留了 500ms 的 debounce 窗口。

原因是 Kubernetes watch 往往会在短时间内发出多条相关事件，例如：

- 先看到 `Service` / `EndpointSlice` 创建
- 后看到对应对象 annotations 或 labels 更新
- 多对象之间也可能不同时到达

如果每次触发都立刻重算 snapshot，会让 watch 噪音直接传递给上层。保留 debounce 可以把一批瞬时抖动压缩成一次 snapshot 更新。

### 6.3 聚合逻辑的核心约束

`aggregate_snapshot()` 有一个非常重要的过滤条件：

- 只有 **Pod 级别** 出现在 ready `EndpointSlice` 中的实例，才会进入最终 snapshot

这意味着即使 `ConfigMap` 或 `Lease` 还存在，只要对应 Pod 没有 ready endpoint，也不会被暴露给上层。

这与旧实现保持了同样的核心语义：

- **Kubernetes readiness 是最终可见性的门槛**

这里需要特别强调一个容易误解的点：

- 旧版 `kube/daemon.rs` 的逻辑并不是“逐 endpoint 判 ready 后再只暴露该 endpoint”；
- 它是先用 `EndpointSlice` 提取 **ready pod 集合**，再只要该 pod 有对应 `DynamoWorkerMetadata` CR，就把这个 pod 的整份 `DiscoveryMetadata` 放进 snapshot。

也就是说，旧版语义本身就是 **pod 级 ready -> 整个实例可见**。当前原生实现沿用了这一点，所以这里是“与原 Dynamo 对齐”，而不是新的语义漂移。

### 6.4 聚合时如何把对象重新拼回 `DiscoveryMetadata`

聚合步骤可以概括成：

1. 从所有 `EndpointSlice` 中提取 ready Pod → 得到 `ready_ids`
2. 为每个 ready instance 先创建一个空的 `DiscoveryMetadata`
3. 再扫描所有原生对象并补齐三类信息：
   - `endpoint_instance_from_service_and_slice()` → `register_endpoint()`
   - `model_instance_from_config_map()` → `register_model_card()`
   - `event_instance_from_lease()` → `register_event_channel()`
4. 最终把这些完整的 `DiscoveryMetadata` 收进 `MetadataSnapshot`

这一步的价值在于：上层最终看到的仍然是与旧实现同样的 `DiscoveryMetadata` 结构，而不是三套分散对象。

### 6.5 generation 为什么变成哈希

旧实现里，generation 来自 CRD metadata generation。新实现没有一个统一 CRD 可直接复用 generation，因此这里使用：

- `serde_json::to_string(metadata)`
- 再做 hash
- 生成 `i64 generation`

含义是：

- generation 不再表示某个 K8s 对象的原生 resource version
- 而是表示“该 instance 的聚合 metadata 内容签名”

这是一种近似替代：足够用于判断 snapshot 是否发生了语义变化，但不等价于 K8s 原生 revision。

这也是代码审查时值得重点确认的一点。

## 7. `native_objects.rs`：三类对象映射的细节

这个文件是“原生存储语义”的核心。建议按三部分阅读。

### 7.1 Endpoint：`Service + EndpointSlice`

#### 写入侧

- `register_endpoint_instance()`
- `unregister_endpoint_instance()`

它会：

1. 从 `TransportType` 提取 port
2. 构造 `NativeServiceRegistration`
3. 写一个按 `component` 共享的 `Service`，并把当前 endpoint 注册成一个命名 port
4. 写一个当前 Pod 独占的 `EndpointSlice`

其中 `Service` 的 annotations 会写入：

- namespace
- component

`EndpointSlice` 会写入该 endpoint 自己的 annotations，因此在修正后的聚合逻辑里：

- `Service` 负责表达“这个 component 下注册了哪些 endpoint port”
- `EndpointSlice` 负责表达“当前 Pod 对哪个 endpoint port 处于 ready 状态，以及该 endpoint 的细粒度元数据”

这样一来，Endpoint 的语义分工就更接近旧实现：

- `Service` 类似旧实现里的 `CRD`：证明这个 component 下有哪些 endpoint 已注册；
- `EndpointSlice` 类似旧实现里的 ready 视图：证明哪个 Pod 当前 ready，并把 Pod 绑定到具体 Service。

这里的“component”建议理解为 **一个逻辑组件实例类型**，而不是某个具体 Pod：

- 不同 component（例如 `planner`、`router`）应当对应不同的 `Service`；
- 同一个 component 的多个副本 Pod（例如 `planner-0`、`planner-1`）共享同一个 `Service`；
- 同一个 component 下的多个 endpoint（例如 `grpc`、`metrics`）则共享这个 `Service`，但各自占用不同命名 port。

因此，更准确的对象关系是：

- `component -> Service`
- `component.endpoint -> ServicePort(name=endpoint)`
- `component.endpoint.pod -> EndpointSlice`

其中 `EndpointSlice` 的 annotations 当前仍会写入：

- namespace
- component
- endpoint
- transport（JSON 序列化）

这些 annotations 现在更偏向冗余调试信息，而不是读取侧的唯一数据源。

#### 读取侧

- `endpoint_instance_from_service_and_slice()`

它会联合 `Service + EndpointSlice` 恢复 `DiscoveryInstance::Endpoint`：

- `Service` 提供：
   - `namespace`
   - `component`
   - 以及“该 endpoint 对应命名 port 已注册”的事实
- `EndpointSlice` 提供：
   - `endpoint`
   - `transport`
   - 与该 `Service` 的绑定关系
   - 当前 ready endpoint 成员
   - `target_ref.name = pod_name`
- `instance_id = hash_pod_name(pod_name)`

注意这里的 instance_id 推导依赖 `target_ref.name`，也就是 Pod 名称。

因此，修正后的 Endpoint 聚合语义变成：

1. `Service` 存在，并且包含该 endpoint 对应的已注册 port；
2. `EndpointSlice` 存在，且通过 `kubernetes.io/service-name` 绑定到该 `Service`；
3. `EndpointSlice` 中存在 ready Pod；
4. `EndpointSlice` 中记录的 endpoint 元数据能成功反解；

只有这四个条件同时满足，daemon 才会把该 endpoint 注册到最终 snapshot。

不过最终 snapshot 的可见性仍然是 **pod 级** 聚合：daemon 先根据 ready `EndpointSlice` 提取 ready pod，再把这些 pod 对应的 endpoint/model/event 信息收进同一个 `DiscoveryMetadata`。这与旧 CRD 路径保持一致。

### 7.2 Model：`ConfigMap`

#### 写入侧

- `apply_model_config_map()`
- `delete_model_config_map()`

ConfigMap 的主要内容放在 `data` 字段中：

- `namespace`
- `component`
- `endpoint`
- `instance_id`
- `card.json`
- `model_suffix`

并且 labels 带：

- `nvidia.com/dynamo-kind=model`

这样 daemon 可以只 watch 相关 ConfigMap。

#### 读取侧

- `model_instance_from_config_map()`

它把 `data` 反解成 `DiscoveryInstance::Model`。这一步的关键是：**Model 语义并没有被拆散，仍然能完整回到原来的 instance 抽象**。

### 7.3 EventChannel：`Lease`

#### 写入侧

- `apply_event_lease()`
- `delete_event_lease()`

`Lease` 上写入：

- label：`nvidia.com/dynamo-kind=event-channel`
- annotations：namespace/component/topic/transport
- spec.holder_identity：十六进制 `instance_id`
- spec.renew_time：当前时间
- spec.lease_duration_seconds=30`

这说明 `Lease` 同时承担两类角色：

1. 对象存在即表示 channel 存在
2. `renew_time` / duration 提供一定的活性语义

#### 读取侧

- `event_instance_from_lease()`

它从 annotations 和 spec 里恢复 `DiscoveryInstance::EventChannel`。

## 8. `service_registry.rs`：为什么单独拆出来

这个文件专注于 endpoint 原生注册，是因为 `Service + EndpointSlice` 这部分逻辑最有通用性，也最容易单独测试。

建议把它看作一个“Endpoint 原生注册 SDK”。

### 8.1 `NativeServiceRegistration` 的角色

这是构造 `Service` / `EndpointSlice` 的中间模型。

它把 endpoint 注册所需信息集中在一个结构体里：

- `service_name`
- `port_name`
- `port`
- `pod_name`
- `pod_uid`
- `pod_ip`
- `hostname`
- `protocol`
- labels / annotations

好处是：

- 调用侧不用直接拼 Kubernetes 对象
- 构造与 apply 分离
- 单测可以直接验证生成对象是否符合预期

### 8.2 `build_service()` / `build_endpoint_slice()`

这两个函数负责把中间模型翻译成原生对象：

- `build_service()` 默认构造 headless service
- `build_endpoint_slice()` 默认构造单 Pod 的 ready slice

其中几个重要设计点：

1. `EndpointSlice` 带 owner reference 指向当前 Pod
   - Pod 删除后，slice 可以随 Pod 一起被 GC

2. `EndpointSlice` 带标准 label：
   - `kubernetes.io/service-name`
   - 这样能与 `Service` 建立标准绑定关系

3. slice 名称是确定性的
   - 由 `service_name + port_name + pod_name` 派生
   - 保证幂等 apply

### 8.3 `apply_*` / `delete_*` 系列函数

这里统一使用 server-side apply：

- `apply_service()`
- `apply_endpoint_slice()`
- `delete_service()`
- `delete_endpoint_slice()`

它们的意义是把 Kubernetes 写操作收敛成一个小型基础设施层，避免上层直接拼 `PatchParams`、`Patch::Apply` 等细节。

## 9. `native_utils.rs`：与旧 `utils.rs` 的差异

这个文件很小，但有一个非常关键的差异：

- 新 `PodInfo` 比旧版多了 `pod_ip`

原因很直接：

- 原生 `EndpointSlice` 注册必须显式写入 Pod IP
- 旧 CRD 路径不需要直接把 Pod IP 塞进对象，因此旧 `utils.rs` 不需要这个字段

这里采用的读取顺序是：

1. 先读 Downward API 文件
2. 再读环境变量
3. `pod_ip` 额外允许 fallback 到本机 IP 探测

这也是为什么新实现不能直接复用旧 `kube/utils.rs`，而要单独并列出 `native_utils.rs`。

## 10. 与旧实现的对应关系

可以把新旧实现按下面方式对照：

### 10.1 入口层对照

- 旧：`kube.rs` → `KubeDiscoveryClient`
- 新：`kube_native.rs` → `NativeKubeDiscoveryClient`

### 10.2 聚合层对照

- 旧：`kube/daemon.rs`
- 新：`kube/native_daemon.rs`

两者共同点：

- 都维护 `MetadataSnapshot`
- 都通过 watch channel 驱动 `list()` / `list_and_watch()`
- 都把复杂的 Kubernetes 对象变更收敛成上层稳定视图

不同点：

- 旧：watch `EndpointSlice + DynamoWorkerMetadata`
- 新：watch `EndpointSlice + ConfigMap + Lease`

### 10.3 持久化层对照

- 旧：`kube/crd.rs`
- 新：`kube/native_objects.rs + kube/service_registry.rs`

## 11. 当前状态：这套实现“已经做了什么”和“还没有做什么”

### 11.1 已完成

1. 并列实现已经拆到独立入口文件，不再修改旧文件
2. 注册、反注册、snapshot 聚合、watch 差分逻辑都已成型
3. Endpoint / Model / EventChannel 三类实例都已经有原生对象映射
4. 仍然保留 `MetadataSnapshot` 机制，所以上层抽象没有被改穿

### 11.2 尚未完成

1. **尚未接线到现有模块树**
   - `NativeKubeDiscoveryClient` 目前存在于并列文件中，但还不是默认构造路径

2. **尚未替代旧 `mod kube` 导出**
   - 当前生产路径仍然是旧 `KubeDiscoveryClient`

3. **尚未补充切换策略**
   - 例如 feature flag、配置开关、显式 constructor 之类的入口还没加

因此，这份实现现在的定位应理解为：

> 可审查、可继续接线的并列实现，而不是已经生效的默认 backend。

## 12. 建议的审查顺序

为了减少阅读成本，建议按下面顺序审查：

1. `lib/runtime/src/discovery/kube_native.rs`
   - 看整体入口和新旧边界
2. `lib/runtime/src/discovery/kube/native_daemon.rs`
   - 看新的 snapshot 如何恢复旧语义
3. `lib/runtime/src/discovery/kube/native_objects.rs`
   - 看三类实例各自如何映射到原生对象
4. `lib/runtime/src/discovery/kube/service_registry.rs`
   - 看 endpoint 的 Service/EndpointSlice 细节
5. `lib/runtime/src/discovery/kube/native_utils.rs`
   - 看为什么需要单独的 Pod IP 能力

如果要做与旧实现的逐段对比，再对照阅读：

- `lib/runtime/src/discovery/kube.rs`
- `lib/runtime/src/discovery/kube/daemon.rs`
- `lib/runtime/src/discovery/kube/crd.rs`

## 13. 审查时最值得重点关注的问题

最后给出一份更偏审查 checklist 的问题列表：

1. `MetadataSnapshot` 的语义是否真的与旧实现保持一致
2. `generation` 用 metadata hash 替代 CR generation 是否足够稳妥
3. endpoint 的 `instance_id` 依赖 Pod 名哈希是否满足现有系统假设
4. `ConfigMap` / `Lease` 作为 model / event-channel 持久化对象是否足够表达全部元数据
5. `EndpointSlice` annotations 中保存 `transport` JSON 是否会带来大小或兼容性问题
6. `register_internal()` / `unregister()` 的回滚是否覆盖了所有失败分支
7. daemon 的 `ready_ids` 过滤是否会错误隐藏某些本该可见的 model / event 实例
8. 新 `native_utils.rs` 的 `pod_ip` 获取策略在真实 K8s 环境里是否足够可靠
9. 未来接线时，如何在不影响旧实现的前提下显式切换到 `NativeKubeDiscoveryClient`

---

如果后续需要，我建议再补一份更偏“调用链顺序图”的文档，把 `register_internal()`、daemon 聚合、`list_and_watch()` 三条链路画成时序图，会更利于做逐步 review。
