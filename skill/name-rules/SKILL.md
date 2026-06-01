---
# SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
name: name-rules
description: Pagoda 项目命名规约 Skill。用于新增、重命名或迁移 Rust 模块、crate、常量、变量、字符串、服务发现字段、端口名和 timeline/NVTX 相关宏时，强制应用 Pagoda 命名规则。
---

# Pagoda 命名规约 Skill

## 使用场景

当任务涉及以下任一内容时，必须使用本 Skill：

- 新增或修改 Rust crate、模块、文件、结构体、枚举、trait、函数、变量、常量、feature、环境变量名。
- 将上游 Dynamo 命名迁移到 Pagoda 命名。
- 修改服务发现、服务注册、Endpoint、PortName、Namespace、ServiceGroup 相关代码。
- 修改模型实例元数据或拓扑字段。
- 修改 NVTX、timeline、性能标注、trace 事件相关代码和宏。

## 核心规则

### 1. 服务命名采用三段式

统一使用贴近 Kubernetes 原生语义的三段式服务标识：

1. `Namespace`
2. `ServiceGroup`
3. `PortName`

禁止继续使用旧式 `Namespace / ServiceGroup / Endpoint` 作为服务寻址模型。若历史代码中仍存在 `Endpoint` 语义，需要判断其实际含义：

- 表示网络端点对象时，可以保留 `Endpoint`。
- 表示服务暴露端口名称时，必须迁移为 `PortName`。
- 表示三段式服务定位中的第三段时，必须迁移为 `PortName`。

### 2. Dynamo 命名迁移为 Pagoda

所有属于项目命名空间的 `dynamo` 前缀或标识必须迁移为 `pagoda`，并保持大小写风格一致：

| 原命名 | 新命名 |
| --- | --- |
| `dynamo` | `pagoda` |
| `Dynamo` | `Pagoda` |
| `DYNAMO` | `PAGODA` |
| `dynamo_*` | `pagoda_*` |
| `DYN_*` | `PAG_*` |
| `dynamo-xxx` | `pagoda-xxx` |
| `dynamo_xxx` | `pagoda_xxx` |

迁移范围包括但不限于：

- crate 名、package 名、feature 名。
- 模块名、文件名、函数名、变量名、常量名、类型名。
- 环境变量名、配置键、指标名、日志 target、协议字段。
- 文档标题、示例命令、注释中的项目专有命名。

第三方依赖、上游兼容协议、外部 API 契约中必须保留的名称，不要盲目替换；需要在修改说明中标注保留原因。

### 3. 域名/标注前缀迁移

项目自有的 Kubernetes 标签、注解 key 及其它字面值中的上游域名必须迁移为 Pagoda 自有域名：

| 原命名 | 新命名 |
| --- | --- |
| `nvidia.com` | `bedicloud.com` |
| `nvidia.com/dynamo` | `bedicloud.com/pagoda` |
| `nvidia.com/dynamo-xxx` | `bedicloud.com/pagoda-xxx` |

要求：

- 先匹配更具体的 `nvidia.com/dynamo` 前缀迁移为 `bedicloud.com/pagoda`，再处理剩余的 `nvidia.com` → `bedicloud.com`。
- 标注后缀（如 `-kind`、`-namespace`、`-servicegroup`、`-portname`、`-topic`、`-transport`、`-discovery-mode`）保持不变。
- 若标注 key 属于必须与上游兼容的外部契约，需在修改说明中标注保留原因，不得盲目替换。

### 4. 模型实例必须包含拓扑字段

服务发现模型实例需要包含拓扑信息字段：

```rust
topo_json: serde_json::Value
```

要求：

- 字段名固定为 `topo_json`。
- 类型固定为 `serde_json::Value`。
- 该字段用于承载动态拓扑信息，避免过早固化结构。
- 若涉及序列化/反序列化，必须确认字段不会被遗漏。

### 5. NVTX 命名迁移为 Timeline 命名

项目侧语义统一使用 `timeline`，不再使用 `nvtx` 作为业务模块命名。

迁移规则：

- `nvtx` 模块迁移为 `timeline` 模块。
- `Nvtx` 类型前缀迁移为 `Timeline`。
- `NVTX` 常量或特性前缀迁移为 `TIMELINE`。
- timeline 事件标注宏统一使用 `pagoda_timeline_` 前缀。

涉及的宏命名必须满足：

```text
pagoda_timeline_*
```

如果底层仍调用 NVIDIA NVTX API，可以在实现层保留 NVTX 适配命名，但对外 API、模块边界和项目语义必须暴露为 `timeline`。

## 执行流程

执行命名相关修改时，按以下顺序检查：

1. 识别是否存在旧 `dynamo` / `Dynamo` / `DYNAMO` 项目命名。
2. 判断该命名是否属于项目自身命名空间，避免误改第三方依赖或外部协议。
3. 按大小写风格迁移为 `pagoda` / `Pagoda` / `PAGODA`。
4. 检查域名/标注前缀，将 `nvidia.com/dynamo` 迁移为 `bedicloud.com/pagoda`，剩余 `nvidia.com` 迁移为 `bedicloud.com`。
5. 检查服务发现命名是否符合 `Namespace / ServiceGroup / PortName`。
6. 检查模型实例是否保留 `topo_json: serde_json::Value`。
7. 检查 NVTX 相关对外命名是否迁移为 timeline 语义。
8. 运行搜索确认没有不应保留的旧命名残留。

## 验收清单

提交前必须确认：

- 新增文件包含 Pagoda SPDX 文件头。
- 项目自有命名不再新增 `dynamo` 前缀。
- 项目自有 k8s 标注域名使用 `bedicloud.com`，`nvidia.com` 已迁移完毕。
- crate/package 命名使用 `pagoda-*`。
- Rust crate 导入名使用 `pagoda_*`。
- 服务发现三段式使用 `Namespace / ServiceGroup / PortName`。
- 模型实例包含 `topo_json: serde_json::Value`。
- timeline 对外 API 不再暴露业务语义上的 `nvtx` 命名。
- 保留的 `dynamo` 或 `nvtx` 命名必须有兼容性或第三方边界原因。
