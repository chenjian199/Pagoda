# Pagoda Pagoda 组件实现确认清单

> 基于 Pagoda v1.0.1 源码梳理，用于与供应商确认 Pagoda 中各组件的实现计划。
>
> 日期：2026-05-07

---

## 一、Rust 核心运行时层（lib/）


| #   | 组件                       | Pagoda 中的 crate          | 代码量  | 职责                                                        | 实现语言 | 实现程度        | 备注                              |
| --- | ------------------------ | ------------------------ | ---- | --------------------------------------------------------- | ---- | ----------- | ------------------------------- |
| R1  | 异步运行时                    | `pagoda-runtime`         | ~5万行 | NATS 通信、组件注册/发现、生命周期管理、HTTP/gRPC server、metrics 暴露        | rust | 完整          | 核心中的核心，所有组件都依赖它                 |
| R2  | LLM 推理核心                 | `pagoda-llm`             | ~5万行 | 推理调度、KV cache 管理、disaggregated prefill/decode、pipeline 协调 | rust | 完整          | prefill/decode 分离是 Pagoda 的核心卖点 |
| R3  | KV Cache 路由              | `pagoda-kv-router`       | ~2万行 | Radix tree prefix-aware 路由，KV cache hit 判断，请求亲和性调度        | rust | 完整          | 直接影响 KV reuse 命中率和延迟            |
| R4  | 内存管理                     | `pagoda-memory`          | ~1万行 | KV cache 生命周期管理，内存分配/回收策略                                 | rust | 替代          |                                 |
| R5  | KV Block Manager CUDA 内核 | `pagoda-kvbm-kernels`    | ~1万行 | KV Block Manager 的 CUDA kernel 实现，GPU 端 KV cache block 操作 | rust | 部分（仅留连接器接口） | 需要 CUDA 开发能力                    |
| R6  | KV Block Manager 逻辑层     | `pagoda-kvbm-logical`    | ~1万行 | KV Block Manager 逻辑层，block 分配/释放/迁移策略                     | rust | 部分          |                                 |
| R7  | Token 管理                 | `pagoda-tokens`          | ~3千行 | Token 计数、吞吐统计                                             | rust | 完整          | 辅助模块                            |
| R8  | 解析器                      | `pagoda-parsers`         | ~3千行 | Tool calling、structured output、reasoning 解析               | rust | 完整          |                                 |
| R9  | Mock Engine              | `pagoda-mocker`          | ~3千行 | 测试用 mock 推理引擎                                             |      |             | 开发/测试用                          |
| R10 | OpenAI 兼容客户端             | `pagoda-async-openai`    | ~3万行 | OpenAI API 兼容的 async 客户端，支持 streaming、tool calling        |      |             | fork 自 async-openai，加了 byot 特性  |
| R11 | Python 绑定                | `pagoda-bindings/python` | ~1万行 | PyO3 绑定，让 Rust 核心能力被 Python 直接调用                          |      |             | 桥接层，决定 Python 组件能调用多少 Rust 能力   |
| R12 | 配置管理                     | `pagoda-config`          | ~2千行 | 统一配置加载、环境变量解析                                             |      |             |                                 |
| R13 | Benchmark 工具             | `pagoda-bench`           | ~3千行 | 性能基准测试框架                                                  |      |             |                                 |


**Rust 层小计：~26万行**

---

## 二、Python 推理组件层（components/）


| #   | 组件                  | Pagoda 中的包              | 代码量    | 职责                                       | 实现语言 | 实现程度 | 备注             |
| --- | ------------------- | ----------------------- | ------ | ---------------------------------------- | ---- | ---- | -------------- |
| P1  | vLLM Worker         | `pagoda.vllm`           | ~1.5万行 | vLLM 框架封装，模型加载、推理服务、KV cache 注册          |      |      | 最成熟的 worker 实现 |
| P2  | SGLang Worker       | `pagoda.sglang`         | ~5千行   | SGLang 框架封装                              |      |      |                |
| P3  | TensorRT-LLM Worker | `pagoda.trtllm`         | ~5千行   | TRT-LLM 框架封装                             |      |      |                |
| P4  | Frontend            | `pagoda.frontend`       | ~8千行   | OpenAI 兼容 API 入口，请求路由，response streaming |      |      | 对外暴露的 API 层    |
| P5  | 请求路由                | `pagoda.router`         | ~3千行   | 请求级路由策略                                  |      |      |                |
| P6  | 扩缩规划器               | `pagoda.planner`        | ~3千行   | 单 DGD 内的自动扩缩规划                           |      |      |                |
| P7  | 全局规划器               | `pagoda.global_planner` | ~3千行   | 跨 DGD 的全局资源规划                            |      |      |                |
| P8  | Profiler            | `pagoda.profiler`       | ~2千行   | 性能 profiling 和数据收集                       |      |      |                |
| P9  | 公共库                 | `pagoda.common`         | ~3千行   | 日志、配置、工具函数                               |      |      |                |


**Python 层小计：~5.3万行**

---

## 三、Go Kubernetes 编排层（deploy/）


| #   | 组件                        | Pagoda 中的模块                                | 代码量    | 职责                                                                  | 实现语言 | 实现程度 | 备注            |
| --- | ------------------------- | ------------------------------------------ | ------ | ------------------------------------------------------------------- | ---- | ---- | ------------- |
| G1  | DGD Operator              | `deploy/operator`                          | ~5万行   | DynamoGraphDeployment CRD controller、reconcile、Grove/KAI 集成、webhook |      |      | K8s 编排的核心     |
| G2  | PodCliqueSet/PodClique 生成 | `deploy/operator/internal/pagoda/graph.go` | ~3千行   | 从 DGD 生成 Grove PodCliqueSet/PodClique、注入环境变量                        |      |      | G1 的子模块，但逻辑独立 |
| G3  | Grove readiness 聚合        | `deploy/operator/internal/pagoda/grove.go` | ~1千行   | 聚合 Grove status 回写 DGD                                              |      |      | G1 的子模块       |
| G4  | Snapshot Agent            | `deploy/snapshot`                          | ~1.5万行 | Pod checkpoint/restore DaemonSet（CRIU），状态快照                         |      |      | 高级特性，优先级可降    |
| G5  | Helm Charts               | `deploy/helm`                              | ~3千行   | 部署模板、values 配置                                                      |      |      |               |
| G6  | 部署工具                      | `deploy/utils`                             | ~1千行   | 部署辅助脚本                                                              |      |      |               |


**Go 层小计：~6.5万行**

---

## 四、基础设施依赖


| #   | 组件            | Pagoda 中使用方式         | 是否必须      | Pagoda 实现方案 | 备注                     |
| --- | ------------- | -------------------- | --------- | ----------- | ---------------------- |
| I1  | NATS          | 组件间消息通信、服务注册/发现      | 是         |             | runtime 核心依赖，替代方案需评估   |
| I2  | etcd          | Pagoda 平台状态存储        | 是         |             | 与 K8s etcd 独立          |
| I3  | Grove         | PodClique/PodGang 编排 | 是（当前配置）   |             | 可用其他 gang scheduler 替代 |
| I4  | KAI Scheduler | GPU 队列/调度/PodGroup   | 是（当前配置）   |             | 可用 Volcano 等替代         |
| I5  | Multus        | 多网络接口注入              | 视 RDMA 需求 |             | GPU/RDMA 推理需要          |
| I6  | Calico        | Pod 网络 CNI           | 是         |             | K8s 标准                 |


---

## 五、CRD 资源对象


| #   | CRD                                 | API Group           | 职责         | Pagoda 是否实现 | 备注                 |
| --- | ----------------------------------- | ------------------- | ---------- | ----------- | ------------------ |
| C1  | DynamoGraphDeployment               | nvidia.com/v1alpha1 | 推理图声明式入口   |             | 用户侧最重要的 CRD        |
| C2  | DynamoComponentDeployment           | nvidia.com/v1alpha1 | 单组件部署      |             | DGD 拆解后的子对象        |
| C3  | DynamoGraphDeploymentScalingAdapter | nvidia.com/v1alpha1 | 伸缩适配器      |             | 接 HPA/KEDA/Planner |
| C4  | DynamoWorkerMetadata                | nvidia.com/v1alpha1 | Worker 元数据 |             |                    |
| C5  | DynamoModel                         | nvidia.com/v1alpha1 | 模型定义       |             |                    |


---

## 六、关键设计决策确认


| #   | 决策点                | Pagoda 的选择              | Pagoda 的选择 | 影响                       |
| --- | ------------------ | ----------------------- | ---------- | ------------------------ |
| D1  | 核心运行时语言            | Rust                    |            | 决定性能天花板和开发成本             |
| D2  | Python 绑定方式        | PyO3                    |            | 决定 Python 层能调用多少 Rust 能力 |
| D3  | 组件间通信              | NATS                    |            | 决定延迟模型和部署依赖              |
| D4  | 编排层语言              | Go + kubebuilder        |            | 决定 K8s 生态兼容性             |
| D5  | Gang Scheduling    | Grove + KAI             |            | 决定多节点推理调度能力              |
| D6  | KV Cache 路由实现      | Rust Radix tree         |            | 决定 prefix-aware 调度精度和性能  |
| D7  | Prefill/Decode 分离  | 支持                      |            | 核心架构决策，影响推理吞吐模型          |
| D8  | 多后端框架支持            | vLLM + SGLang + TRT-LLM |            | 决定用户覆盖面                  |
| D9  | Checkpoint/Restore | CRIU Snapshot Agent     |            | 高级特性，可降优先级               |


---

## 填写说明

- **实现语言**：Pagoda 中该组件用什么语言实现，如与 Pagoda 不同请说明原因
- **实现程度**：
  - `完整` — 功能与 Pagoda 等价
  - `部分` — 实现核心功能，非核心功能暂不实现（请注明哪些做了哪些没做）
  - `替代` — 用不同方案实现同等功能（请注明替代方案）
  - `不实现` — Pagoda 不需要该能力（请注明原因）
  - `待定` — 需要进一步评估

---

*文档生成时间：2026-05-07*
*基于源码版本：Pagoda v1.0.1*