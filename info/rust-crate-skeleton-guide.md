# Rust Crate 骨架创建步骤指南

> 以 `pagoda-runtime` 为例，记录从零建立 Rust library crate 骨架的完整流程。

## 1. 使用 Cargo 初始化 crate

```bash
cd /home/gpt/CJ_Workspace/Pagoda/lib

# --lib 表示创建库类型（入口是 src/lib.rs，而非 main.rs）
# --name 指定 crate 名称，可以与目录名不同
cargo init --lib runtime --name pagoda-runtime
```

生成的结构：

```
lib/runtime/
├── Cargo.toml      # [package] name = "pagoda-runtime"
└── src/
    └── lib.rs      # 库入口
```

参数说明：

| 参数 | 作用 |
|------|------|
| `--lib` | 创建库类型，入口为 `src/lib.rs` |
| `--bin`（默认） | 创建可执行类型，入口为 `src/main.rs` |
| `--name pagoda-runtime` | crate 名称，不指定则默认取目录名 |

## 2. 添加依赖

使用 `cargo add` 逐个添加（自动获取最新兼容版本并写入 `Cargo.toml`）：

```bash
cd lib/runtime

# ── 异步运行时 ──
cargo add tokio --features full
cargo add tokio-util --features full
cargo add tokio-stream

# ── 序列化 ──
cargo add serde --features derive
cargo add serde_json

# ── 配置 ──
cargo add config

# ── 网络 ──
cargo add async-nats
cargo add hyper --features full
cargo add axum

# ── Kubernetes 发现 ──
cargo add kube --features runtime,derive
cargo add k8s-openapi --features latest

# ── etcd ──
cargo add etcd-client

# ── CPU 计算隔离 ──
cargo add rayon
cargo add tokio-rayon

# ── 可观测性 ──
cargo add tracing
cargo add tracing-subscriber --features env-filter
cargo add opentelemetry
cargo add prometheus

# ── 错误处理 ──
cargo add anyhow
cargo add thiserror

# ── 并发工具 ──
cargo add arc-swap
cargo add dashmap

# ── 代码生成 ──
cargo add derive_builder
cargo add educe

# ── 其他工具 ──
cargo add uuid --features v4
cargo add regex
cargo add rand
cargo add once_cell
cargo add slug
```

添加可选依赖（需通过 feature 启用）：

```bash
cargo add cudarc --optional
cargo add console-subscriber --optional
```

手动编辑 `Cargo.toml` 中的 `[features]` 段：

```toml
[features]
default = []
integration = []                          # 集成测试 flag
tokio-console = ["dep:console-subscriber"]
timeline = []                             # 性能标注（替代旧版 nvtx）
cuda = ["dep:cudarc"]
```

### cargo add 常用参数

| 参数 | 示例 | 说明 |
|------|------|------|
| `--features` | `cargo add tokio --features full` | 启用指定 feature |
| `--optional` | `cargo add cudarc --optional` | 声明为可选依赖 |
| `--dev` | `cargo add mockall --dev` | 仅用于测试 |
| `--build` | `cargo add cc --build` | 仅用于 build.rs |
| `--rename` | `cargo add foo --rename bar` | 重命名依赖 |

## 3. 创建模块目录结构

```bash
cd src

# 创建各层目录（-p 递归创建父目录）
mkdir -p config
mkdir -p servicegroup
mkdir -p pipeline/nodes/sources
mkdir -p pipeline/nodes/sinks
mkdir -p pipeline/network/codec
mkdir -p pipeline/network/tcp
mkdir -p pipeline/network/ingress
mkdir -p pipeline/network/egress
mkdir -p discovery/kube
mkdir -p transports/etcd
mkdir -p transports/event_plane
mkdir -p compute
mkdir -p metrics
mkdir -p protocols
mkdir -p utils/tasks
```

## 4. 创建骨架文件

### 4.1 批量创建空文件

```bash
# 顶层文件
touch prelude.rs worker.rs runtime.rs error.rs
touch config.rs distributed.rs traits.rs
touch engine.rs engine_routes.rs pipeline.rs
touch logging.rs system_status_server.rs health_check.rs system_health.rs
touch service.rs local_portname_registry.rs runnable.rs slug.rs timeline.rs

# 子模块入口文件（新式写法用 xxx.rs，旧式用 xxx/mod.rs）
touch servicegroup.rs transports.rs metrics.rs protocols.rs utils.rs

# 需要 mod.rs 的深层目录
touch pipeline/nodes/mod.rs
touch pipeline/nodes/sources/mod.rs pipeline/nodes/sinks/mod.rs
touch pipeline/network/mod.rs pipeline/network/codec/mod.rs
touch pipeline/network/tcp/mod.rs pipeline/network/ingress/mod.rs
touch pipeline/network/egress/mod.rs
touch discovery/mod.rs discovery/kube/mod.rs
touch transports/etcd/mod.rs transports/event_plane/mod.rs
touch compute/mod.rs
touch utils/tasks/mod.rs

# 各子模块具体文件
touch config/environment_names.rs
touch servicegroup/{client,namespace,portname,registry,service,servicegroup_impl}.rs
touch pipeline/{context,error,registry}.rs
# ... 以此类推
```

### 4.2 为每个文件填充骨架内容

每个 `.rs` 文件的基本结构：

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! 模块简述（一句话说明职责）。

use crate::xxx;  // 必要的内部引用

/// 核心类型/trait 的文档注释。
pub struct Foo {
    // 字段使用 todo!() 或占位类型
}

impl Foo {
    pub fn new() -> Self {
        todo!()
    }
}
```

## 5. 在父模块中声明子模块

这是 Rust 模块系统的关键步骤。子模块必须在父模块中用 `pub mod` 声明才会被编译器识别。

### 5.1 顶层 lib.rs

```rust
// ── 进程入口层 ──
pub mod worker;
pub mod runtime;
pub mod config;

// ── 分布式运行时层 ──
pub mod distributed;
pub mod traits;

// ── 服务模型层 ──
pub mod servicegroup;

// ── 引擎与管道层 ──
pub mod engine;
pub mod engine_routes;
pub mod pipeline;

// ... 以此类推

// ── Re-export 常用类型到 crate 根 ──
pub use worker::Worker;
pub use error::PagodaError;
pub use pipeline::error::PipelineError;
```

### 5.2 子模块入口

**新式写法** — `pipeline.rs` + `pipeline/` 目录共存：

```rust
// src/pipeline.rs — 等价于旧式 src/pipeline/mod.rs
pub mod context;
pub mod error;
pub mod registry;
pub mod nodes;
pub mod network;
```

**旧式写法** — 深层嵌套使用 `mod.rs`：

```rust
// src/pipeline/network/mod.rs
pub mod codec;
pub mod tcp;
pub mod ingress;
pub mod egress;
pub mod manager;
```

### 5.3 新式 vs 旧式对比

```
新式（2018 edition 推荐）：       旧式：
src/                              src/
├── pipeline.rs    ← 模块入口     ├── pipeline/
├── pipeline/                     │   ├── mod.rs    ← 模块入口
│   ├── context.rs                │   ├── context.rs
│   ├── error.rs                  │   ├── error.rs
│   └── network/                  │   └── network/
│       └── mod.rs                │       └── mod.rs
```

两种写法可以混用，本项目采用的策略：
- **第一层子模块**（如 `pipeline`、`servicegroup`）→ 新式
- **深层嵌套**（如 `pipeline/network/`）→ 旧式 `mod.rs`

## 6. 验证编译

```bash
cd lib/runtime

# 快速检查语法和类型（不生成产物，速度快）
cargo check

# 完整编译
cargo build

# 运行测试
cargo test

# 查看文档
cargo doc --open
```

### 常见编译错误与解决

| 错误 | 原因 | 解决 |
|------|------|------|
| `file not found for module` | 缺少 `pub mod xxx;` 声明或文件路径不对 | 在父模块中添加声明 |
| `unresolved import` | 引用了未声明的模块或未定义的类型 | 检查 `mod` 声明链和 `use` 路径 |
| `not found in this scope` | 骨架阶段类型未定义 | 添加占位 struct/enum 或 `todo!()` |
| `duplicate module` | 同时存在 `foo.rs` 和 `foo/mod.rs` | 只保留一种写法 |

## 7. 实用命令速查

```bash
# 初始化
cargo init --lib <dir> --name <crate-name>
cargo new --lib <name>          # new = init + 创建目录

# 依赖管理
cargo add <crate>               # 添加依赖
cargo add <crate> --features x  # 带 feature
cargo add <crate> --optional    # 可选依赖
cargo remove <crate>            # 移除依赖
cargo update                    # 更新 Cargo.lock

# 检查与构建
cargo check                     # 仅类型检查
cargo build                     # Debug 构建
cargo build --release           # Release 构建
cargo clippy                    # Lint 检查

# 查看
cargo tree                      # 依赖树
cargo tree -d                   # 重复依赖
cargo doc --open                # 生成并打开文档

# 测试
cargo test                      # 运行所有测试
cargo test --lib                # 仅库测试
cargo test -- --nocapture       # 显示 println 输出

# 格式化
cargo fmt                       # 格式化代码
cargo fmt -- --check            # 仅检查不修改
```
