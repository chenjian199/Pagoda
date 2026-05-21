# 分布式推理框架模块总览

本文档用于理解和分析本项目的一级模块分工与功能实现。

## 总体分层

从职责上看，rust功能模块大致可以分成 6 组：

1. **运行时与业务主线**：`runtime`、`llm`
2. **通用基础能力**：`config`、`protocols`、`tokens`、`parsers`、`memory`
3. **KV 路由与 KV Block Manager 体系**：`kv-router`、`kvbm-*`
4. **事件与传输基础设施**：`velo-common`、`velo-events`、`velo-transports`
5. **测试、模拟与压测辅助**：`mocker`、`bench`
6. **语言绑定与独立服务**：`bindings`、`gpu_memory_service`

---

## 一级模块说明

### runtime

- Rust crate：`runtime`
- 作用：Pagoda 的通用运行时底座。
- 主要职责：提供分布式运行时、组件/端点抽象、服务发现、网络 pipeline、传输集成、通用路由与执行框架。
- 使用场景：实例如何注册/发现、组件如何通信、上层能力建立在什么运行时抽象上。

### llm

- Rust crate：`llm`
- 作用：面向 LLM 场景的高层业务库。
- 主要职责：封装模型运行时能力、KV 路由接入、模型卡与运行时配置、推理相关协议适配，以及与 block manager / memory / router 的集成。
- 使用场景：生成式推理主流程、模型实例管理、KV cache 路由或 LLM 特有能力。

### config

- Rust crate：`config`
- 作用：统一配置与环境变量读取的基础库。
- 主要职责：为其他模块提供配置获取、配置约定和环境变量解析能力。
- 使用场景：运行时行为由哪些环境变量或配置项控制。

### tokens

- Rust crate：`tokens`
- 作用：Token 处理相关的基础工具。
- 主要职责：提供 token 管理、token 序列辅助能力，以及被 KV 路由、推理调度等模块复用的 token 级工具。
- 使用场景：处理 prompt token、sequence hash、token 粒度处理有关的通用逻辑。

### protocols

- Rust crate：`protocols`
- 作用：跨模块复用的协议类型定义。
- 主要职责：提供 OpenAI 兼容推理 API 等协议对象、请求响应类型，以及跨运行时/服务之间共享的数据结构。
- 使用场景：当前后端之间对齐接口模型、事件格式或 API 类型。

### parsers

- Rust crate：`parsers`
- 作用：工具调用与 reasoning 相关解析库。
- 主要职责：封装 tool calling、reasoning 输出等上层文本/结构化结果的解析逻辑。
- 使用场景：将模型输出解释成结构化动作、工具调用或 reasoning 结果。

### memory

- Rust crate：`memory`
- 作用：Pagoda 的内存管理基础库。
- 主要职责：封装 pinned memory、NUMA 感知分配、设备内存辅助能力，以及上层 block manager / LLM 模块依赖的底层内存机制。
- 使用场景：关注 NUMA、CUDA 内存、host pinned memory 或内存布局/分配策略。

### kv-router

- Rust crate：`kv-router`
- 作用：KV cache 感知路由核心。
- 主要职责：提供基于 radix tree 的 KV overlap 索引、调度器、路由策略、worker 选择器，以及 KV 事件与 hash 计算辅助能力。
- 使用场景：请求为什么被路由到某个 worker、KV overlap 怎么算、调度如何排队。

### kvbm-common

- Rust crate：`kvbm-common`
- 作用：KV Block Manager 体系的公共基础模块。
- 主要职责：承载 `kvbm` 家族共享的数据类型、公共约定和基础能力。
- 使用场景：`kvbm-kernels`、`kvbm-logical`、`kvbm-physical`的公共依赖层。

### kvbm-kernels

- Rust crate：`kvbm-kernels`
- 作用：KV Block Manager 的 GPU kernel 层。
- 主要职责：提供 KV cache block 在不同内存布局之间转换、拷贝与批处理相关的 CUDA kernel 和底层加速逻辑。
- 使用场景：关注 KV block 的设备端布局转换、批量拷贝、kernel 性能。

### kvbm-logical

- Rust crate：`kvbm-logical`
- 作用：KV Block 的逻辑生命周期管理层。
- 主要职责：通过类型状态机、注册表、池化机制管理 block 的分配、暂存、注册、弱引用和复用。
- 使用场景： KV block 在“可写/完成/注册/可回收”之间如何流转的逻辑中心。

### kvbm-physical

- Rust crate：`kvbm-physical`
- 作用：KV Block 的物理布局与传输层。
- 主要职责：负责 block 到物理内存的映射、布局描述、跨介质/跨节点传输管理，以及与 RDMA/NIXL 之类能力集成。
- 使用场景： KV block 如何真正落到 GPU/Host/远端存储并被传输。

### velo-common

- Rust crate：`velo-common`
- 作用：Velo 分布式通信栈的公共类型层。
- 主要职责：定义实例身份、worker 地址、peer 信息、传输键等通用身份与寻址对象。
- 使用场景：如果你要理解底层传输系统如何识别实例、表达地址和注册 peer，这里是入口。

### velo-events

- Rust crate：`velo-events`
- 作用：轻量级事件/前置条件系统。
- 主要职责：提供事件创建、等待、合并、poison 传播等能力，用于协调异步任务和构建依赖图。
- 使用场景：当系统需要表达“某几个条件全部完成后再继续”这类异步依赖关系时，会用到这里。

### velo-transports

- Rust crate：`velo-transports`
- 作用：统一多种传输协议的传输抽象层。
- 主要职责：封装 TCP、HTTP、NATS、gRPC、UCX 等传输实现，并提供统一的 `Transport` 抽象、peer 路由和优雅关闭机制。
- 使用场景：关注底层 active message 通信、跨实例数据传输、传输后端切换时，重点看这里。

### mocker

- Rust crate：`pagoda-mocker`
- 作用：不依赖真实 GPU 的 LLM 模拟器。
- 主要职责：模拟调度器、KV cache 行为、负载回放与测试流程，为功能测试、trace replay 和压测提供近似真实的行为环境。
- 使用场景：当你要做无 GPU 的调试、复现实验或压测回放时，通常从这里入手。

### bench

- Rust crate：`pagoda-bench`
- 作用：基准测试与轻量 HTTP 压测工具集。
- 主要职责：提供端点性能测试、KV indexer 基准、交互式/多轮基准等辅助程序。
- 使用场景：需要快速对某类能力做定量性能验证时，可以看这里的 bench 工具。

### bindings

- 作用：语言绑定与对外接入层目录。
- 目录定位：这里不是单一 crate，而是一组绑定子项目的集合，当前主要包含：
  - `bindings/c`：C 绑定
  - `bindings/python`：Python 绑定
  - `bindings/kvbm`：面向 KVBM 的绑定/包装
- 使用场景：如果你要从 Rust 以外语言访问 Pagoda 能力，或者给外部系统暴露接口，通常会进入这里。

### gpu_memory_service

- 作用：独立的 GPU Memory Service（GMS）实现。
- 目录定位：这是一个偏独立服务形态的 Python 模块，不是当前 Rust workspace 的核心 crate 之一。
- 主要职责：把 GPU 内存所有权从使用进程里剥离出来，支持多进程零拷贝共享、跨进程保活、以及更快的模型复用与恢复。
- 使用场景：如果你关注模型权重在 GPU 上的共享、进程崩溃后的显存保留、或独立显存服务化，这里是重点。

