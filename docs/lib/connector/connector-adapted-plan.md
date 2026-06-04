---
# SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0
#
# API-compatible implementation plan based on the public interfaces and behavioral
# contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
# Implementation plan written by PAGODA.
title: Pagoda KV Connector Adaptation Plan
---

# Pagoda KV Connector Adaptation Plan

本文档给出 Pagoda 在接入 vLLM 后端时，围绕 KV connector、LMCache、KVBM、NIXL 与未来 Ascend 后端的第一阶段适配方案。

本文档不是 KVBM 重新实现方案。第一阶段目标是让 Pagoda 保留清晰、轻量、可扩展的 KV connector 配置与编排层，并以 LMCache 作为默认外部 KV cache 接入路径。

## 设计目标

Pagoda 第一阶段的边界是：

```text
Pagoda 第一阶段只做 KV connector orchestration，
不做 KV block memory management。
```

也就是说：

- vLLM 继续管理执行期 GPU paged KV buffer。
- Pagoda 负责表达、校验和生成 vLLM `kv_transfer_config`。
- LMCache 负责外部 KV cache 的查询、load、save、L1/L2 后端与跨实例复用。
- KVBM 的 allocator、block table、tier manager、lifecycle state machine、NIXL layer 不进入第一阶段主路径。
- KVBM、AscendStore、Mooncake 等后端只保留轻量扩展入口。

## 非目标

第一阶段明确不做：

- 自研 KVBM。
- 自研多级 KV memory manager。
- 自研 NIXL 抽象层。
- 自研 KV block allocator。
- 自研 KV block lifecycle state machine。
- 在 Pagoda 内部接管 vLLM GPU KV block。
- 复制 LMCache 的缓存管理能力。
- 将 NIXL 作为所有后端的唯一传输抽象。

## 当前代码现状

当前仓库中 KV connector 相关代码分布如下：

| 区域 | 当前职责 | 第一阶段处理方式 |
| --- | --- | --- |
| `components/src/dynamo/vllm/args.py` | 解析 vLLM 后端参数，校验 `kv_transfer_config`，包含 NIXL/KVBM 判断 | 收敛到轻量 `kv_connector` 配置层 |
| `components/src/dynamo/vllm/backend_args.py` | Dynamo vLLM wrapper 参数定义 | 增加 Pagoda 高层 KV connector 参数 |
| `components/src/dynamo/vllm/main.py` | 创建 vLLM engine，设置 metrics，按 KVBM connector 注入 consolidator endpoints | KVBM 逻辑改为 experimental gated |
| `components/src/dynamo/vllm/llm_engine.py` | 统一 vLLM engine，prefill/decode 传递 `kv_transfer_params` | 保留行为，文案和校验改成通用 connector 语义 |
| `lib/bindings/kvbm` | Dynamo KVBM Python package | 从默认 Pagoda 路径隔离，保留为 experimental |
| `lib/kvbm-*` | KVBM Rust crates | 从默认构建路径隔离，保留为 experimental 或后续删除 |
| `lib/memory` | Dynamo memory 相关 crate | 第一阶段不作为 Pagoda KV connector 主路径 |
| `examples/backends/vllm/*lmcache*` | LMCache 示例 | 提升为 Pagoda 官方示例 |
| `examples/backends/vllm/*kvbm*` | KVBM 示例 | 移到 experimental / legacy 区域 |
| `container/deps/vllm/install_vllm.sh` | 安装 vLLM 与 LMCache | 保留 LMCache 安装；KVBM 不作为默认依赖 |

## 推荐模块边界

新增轻量模块：

```text
components/src/dynamo/vllm/kv_connector/
  __init__.py
  config.py
  builder.py
  validation.py
  lmcache.py
```

各文件职责：

| 文件 | 职责 |
| --- | --- |
| `config.py` | 定义 `KvConnectorKind`、`ServingMode`、`KvStorageBackend`、`KvTransferBackend`、`LMCacheConfig` |
| `builder.py` | 根据 Pagoda 高层配置生成 vLLM `kv_transfer_config` |
| `validation.py` | 校验 aggregated / disaggregated / LMCache / transfer backend 组合是否合法 |
| `lmcache.py` | 保存 LMCache connector 名称、默认 `kv_role`、L1/L2 backend 参数转换 |

不要把第一阶段模块命名为 `memory`。`memory` 会暗示 Pagoda 自己管理 KV block 分配、迁移和生命周期；第一阶段实际只做 connector 配置与编排。

## 高层配置模型

推荐抽象：

```text
KvConnectorKind
  ├── None
  ├── LMCache
  ├── KVBM
  ├── AscendStore
  └── Custom

ServingMode
  ├── Aggregated
  └── Disaggregated

KvStorageBackend
  ├── Memory
  ├── LocalDisk
  ├── POSIX
  ├── GDS
  ├── ObjectStore
  ├── AzureBlob
  ├── Mooncake
  ├── Redis
  ├── NIXL
  └── Custom

KvTransferBackend
  ├── Native
  ├── NIXL
  ├── Mooncake
  ├── HIXL
  ├── HCCL
  ├── RDMA
  └── Custom
```

第一阶段只正式支持：

```text
KvConnectorKind.None
KvConnectorKind.LMCache
KvConnectorKind.Custom
```

`KVBM`、`AscendStore`、`Mooncake` 可以保留枚举和配置位置，但应标记为 experimental 或 future。

## vLLM 参数层改造

### `backend_args.py`

增加 Pagoda 高层参数：

```text
--kv-connector-kind none|lmcache|custom
--kv-storage-backend memory|posix|object-store|mooncake|custom
--kv-transfer-backend native|nixl|mooncake|custom
--lmcache-l1-size-gb
--lmcache-l2-backend
--lmcache-server-url
--lmcache-config
--allow-experimental-kvbm
```

第一阶段默认行为：

| 模式 | 默认行为 |
| --- | --- |
| 未配置 connector | 不设置 `kv_transfer_config` |
| `--kv-connector-kind lmcache` + aggregated | 生成 `LMCacheConnectorV1` / `kv_both` |
| `--kv-connector-kind custom` | 透传用户提供的 `--kv-transfer-config` |
| `--kv-connector-kind kvbm` | 默认拒绝，除非 `--allow-experimental-kvbm` |

### `args.py`

将当前散落的 connector 辅助函数迁移到 `kv_connector` 模块：

```text
_uses_nixl_connector()
_uses_dynamo_connector()
_connector_to_kv_transfer_json()
```

改为更通用的实现：

```text
uses_connector(engine_config, connector_name)
iter_connector_entries(engine_config)
build_kv_transfer_config(pagoda_config)
validate_kv_connector_config(pagoda_config, engine_config)
```

`NixlConnector` 只作为 transfer backend 的一个可能实现，不应成为 disaggregated 的唯一默认语义。

## LMCache 配置生成

Aggregated LMCache 的目标 vLLM 配置：

```json
{
  "kv_connector": "LMCacheConnectorV1",
  "kv_role": "kv_both"
}
```

如果目标 vLLM 版本使用 `LMCacheMPConnector`，则由 `lmcache.py` 根据版本或显式参数选择 connector 名称。文档和错误信息必须说明 connector 名称与 vLLM / LMCache 版本相关。

建议生成逻辑：

```text
if kv_connector_kind == none:
  不写 kv_transfer_config

if kv_connector_kind == lmcache and serving_mode == aggregated:
  写 LMCacheConnectorV1 + kv_both

if kv_connector_kind == lmcache and serving_mode == disaggregated:
  要求显式 transfer backend，例如 nixl/mooncake/custom

if kv_connector_kind == custom:
  用户必须提供 --kv-transfer-config
```

## Disaggregated serving 处理

`llm_engine.py` 中 prefill/decode 传递 `kv_transfer_params` 的行为应保留。这属于 vLLM connector 的外部行为契约，而不是 KVBM 专属逻辑。

需要调整的是文案和校验：

- 将 “vLLM's NixlConnector handles ...” 改成 “the configured vLLM KV connector handles ...”。
- prefill worker 仍要求有 transfer-capable connector。
- 第一阶段不默认给 disaggregated LMCache 自动补 NIXL。
- 如果使用 disaggregated LMCache，应显式声明 transfer backend。

推荐错误信息语义：

```text
Disaggregated serving requires a transfer-capable KV connector configuration.
For LMCache, configure a transfer backend such as NIXL, Mooncake, or a custom
backend supported by the target vLLM version.
```

## KVBM 隔离策略

### Python binding

`lib/bindings/kvbm` 不进入 Pagoda 默认 wheel。处理方式：

- 从默认 Python package 构建路径中移除。
- 保留为 `experimental-kvbm` extra 或单独构建任务。
- 只有显式启用时才安装。

### Rust crates

当前 workspace 包含：

```text
lib/memory
lib/kvbm-common
lib/kvbm-config
lib/kvbm-engine
lib/kvbm-kernels
lib/kvbm-logical
lib/kvbm-physical
```

第一阶段建议先隔离，不立即硬删：

1. 从默认 workspace members 移出，或放到 experimental workspace/profile。
2. CI 默认不构建 `kvbm-*`。
3. 确认主路径无依赖后，再决定删除、迁移或保留为可选子项目。

### vLLM KVBM consolidator

`main.py` 中 KVBM consolidator endpoint 逻辑应加 feature gate：

```text
if allow_experimental_kvbm and uses_connector(DynamoConnector):
  try import kvbm and configure consolidator endpoints
else:
  consolidator_endpoints = None
```

默认 LMCache 路径不应 import `kvbm`，也不应因为未安装 `kvbm` 产生 warning。

## 容器与依赖

需要调整的文件：

```text
container/deps/vllm/install_vllm.sh
container/deps/requirements.vllm.txt
container/templates/frontend.Dockerfile
container/templates/trtllm_runtime.Dockerfile
container/templates/args.Dockerfile
pyproject.toml
```

建议策略：

| 依赖 | 第一阶段默认 |
| --- | --- |
| vLLM | 保留 |
| LMCache | vLLM runtime 镜像默认安装，或提供 `pagoda[vllm-lmcache]` extra |
| NIXL | 仅 NVIDIA disaggregated transfer profile 需要 |
| KVBM | 不默认安装，仅 experimental profile |

`install_vllm.sh` 已包含 LMCache 安装逻辑，可保留。需要避免 KVBM/NIXL 在 Pagoda 默认路径中表现为必需依赖。

## 示例与部署文件

提升为 Pagoda 官方示例：

```text
examples/backends/vllm/launch/agg_lmcache.sh
examples/backends/vllm/launch/agg_lmcache_multiproc.sh
examples/backends/vllm/launch/disagg_lmcache.sh
```

建议新增：

```text
examples/backends/vllm/launch/pagoda_agg_lmcache.sh
examples/backends/vllm/deploy/pagoda_agg_lmcache.yaml
```

降级为 experimental / legacy：

```text
examples/backends/vllm/launch/agg_kvbm.sh
examples/backends/vllm/launch/disagg_kvbm.sh
examples/backends/vllm/launch/disagg_kvbm_router.sh
examples/backends/vllm/launch/disagg_kvbm_2p2d.sh
examples/backends/vllm/deploy/*kvbm*.yaml
examples/backends/vllm/deploy/v1beta1/*kvbm*.yaml
```

## 测试计划

新增单元测试：

```text
components/src/dynamo/vllm/tests/test_kv_connector_config.py
```

覆盖：

- `none` 不生成 `kv_transfer_config`。
- `lmcache` + aggregated 生成 `LMCacheConnectorV1`。
- `lmcache` + disaggregated + 无 transfer backend 报错。
- `custom` 透传用户 `--kv-transfer-config`。
- `DynamoConnector` 默认拒绝，experimental flag 下允许。
- NIXL side-channel 只在实际使用 `NixlConnector` 时设置。

保留 LMCache 集成测试标记：

```bash
pytest -m lmcache
```

KVBM 测试默认跳过：

```bash
pytest -m kvbm
```

仅在显式开启时运行：

```bash
PAGODA_ENABLE_KVBM_EXPERIMENTAL=1 pytest -m kvbm
```

## 分阶段实施顺序

### Phase 1：LMCache-only connector orchestration

1. 新增 `components/src/dynamo/vllm/kv_connector/`。
2. 将 connector 生成、遍历和校验逻辑从 `args.py` 收敛到新模块。
3. 在 `backend_args.py` 中增加 Pagoda 高层 KV connector 参数。
4. `args.py` 根据高层参数生成 vLLM `kv_transfer_config`。
5. `llm_engine.py` 保留 `kv_transfer_params` 行为，改成通用 connector 文案。
6. `main.py` 默认不 import KVBM，KVBM consolidator 受 experimental flag 控制。
7. 新增 `test_kv_connector_config.py`，更新现有 vLLM unit tests。
8. 将 LMCache 示例设为官方推荐路径。

### Phase 2：Disaggregated serving 扩展

1. 建模 prefill / decode worker 的 transfer backend 配置。
2. 支持 LMCache + NIXL / Mooncake / custom transfer backend 组合。
3. 明确 router 传递 transfer metadata 的契约。
4. 增加 disaggregated LMCache 集成测试。

### Phase 3：多后端 connector 扩展

1. 可选恢复 KVBM 适配，但仍保持 feature gate。
2. 增加 AscendStore / Mooncake 配置后端。
3. 增加 `KvTransferBackend` 与 `KvStorageBackend` 的跨硬件实现。
4. 接入 KV cache metrics 与 KV-aware routing 扩展。

## 风险与约束

### vLLM connector API 版本敏感

vLLM KV connector 的接口名称、参数和 connector 名称随版本变化。Pagoda 配置层必须将 connector 名称和 JSON 字段集中管理，避免散落硬编码。

### 不要混淆执行层和缓存层

vLLM GPU paged KV buffer 是执行层。LMCache / KVBM / AscendStore 是外部缓存或管理层。外部 KV 必须 load 回 vLLM GPU paged KV buffer 后才能参与 attention。

### 不要保留 KVBM memory 假象

如果第一阶段不实现 KVBM，就不要在主路径保留大块 `memory` / `kvbm` 模块，否则会造成职责混乱。

### 不要写死 NIXL

NIXL 属于 NVIDIA 生态。为了未来适配 Ascend，应把传输层抽象为 `KvTransferBackend`，而不是直接写死为 `NIXL`。

### 注意 `request_finished()` 的资源释放语义

如果 connector 异步保存或传输未完成，不得提前释放 vLLM KV block，否则可能导致保存错误或脏数据。Pagoda 第一阶段不直接实现该回调，但配置层必须知道 connector 的释放语义由目标 vLLM connector 负责。

## 验收标准

第一阶段完成时，应满足：

- LMCache aggregated 模式可以通过 Pagoda 高层参数启动。
- 未启用 KVBM 时，不 import `kvbm`，不要求 KVBM wheel。
- 未使用 NIXL 时，不设置 NIXL side-channel 环境变量。
- vLLM prefill/decode 的 `kv_transfer_params` 行为保持兼容。
- 默认容器包含 vLLM + LMCache 路径，不包含 KVBM 必需依赖。
- KVBM 示例和测试默认不进入主路径，但可通过 experimental flag 运行。
- 单元测试覆盖 connector 配置生成、校验和 experimental gating。

## 推荐结论

Pagoda 第一阶段应以 LMCache 接入为主线：

```text
Pagoda Runtime / Router
  ↓
KvConnector configuration layer
  ↓
LMCache connector / LMCache server
  ↓
vLLM GPU paged KV buffer
```

KVBM 不应作为第一阶段默认 memory 模块保留。正确做法是保留扩展口、隔离实现层、降低默认依赖，并把 Pagoda 的职责限定为 KV connector 编排和配置生成。
