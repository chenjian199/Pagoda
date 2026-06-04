---
# SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
name: rewrite
description: Pagoda API 兼容重实现 Skill。用于对 lib/<crate>/ 中的 Rust 源码进行基于 public API 与行为契约的重写、测试整理、中文化和零回归验证。
---

# API 兼容重实现约束

## 0. 目标版权归类

本规则的目标是让重写后的文件尽量符合以下归类：

```text
API-compatible implementation based on the public interfaces and behavioral
contracts of NVIDIA Dynamo.
Implementation rewritten by PAGODA.
```

推荐文件头：

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.
```

如果文件含 `async-openai` 派生协议声明、类型树、字段树、enum variants、serde 表达或测试表达，则必须升级为 Pagoda + NVIDIA + Himanshu Neema，许可证使用 `Apache-2.0 AND MIT`。

## 1. 接口契约兼容

功能一致与契约一致是本重写任务的第一性要求。任何实现多样化、中文化、测试重写和版权归类目标，都不能削弱或改变对下游可观察行为的兼容性。

公开 API 名称、签名、类型形状、serde wire contract 与可观察行为应与 Dynamo 对齐，使下游调用者不感知差异。

必须保持一致的范围包括但不限于：公开类型与函数签名、字段名与字段类型、enum variants、serde tag / rename / default / skip 规则、输入输出 JSON 形态、默认值、错误字符串、反序列化错误行为、panic/assert 可观察文本，以及调用者可依赖的返回值和状态变化。

允许不同的范围仅限内部实现表达：辅助函数拆分、局部变量命名、内部数据结构、算法组织、注释表达和测试代码写法可以由 Pagoda 重新设计，但不得改变外部契约。

`lib-copy` 或 Dynamo 源码只作为 public API surface 与 observable behavior 的参考，不作为源码改写底稿。

## 2. 实现独立重写

在契约约束下，内部数据结构、算法路径、模块组织与辅助函数由 Pagoda 重新设计，避免与 Dynamo / lib-copy 的实现表达同形同构。

禁止通过变量改名、语句重排、注释翻译、局部替换等方式把 Dynamo 源码伪装成重写实现。若实际采用了这种方式，该文件应改用 `Derived from NVIDIA Dynamo / Modified by PAGODA` 文件头。

## 3. 代码风格统一中文化

- 模块/函数文档头：`## 设计意图` / `## 外部契约` / `## 实现要点`
- 节段分隔：`// === SECTION: 名称 ===`
- 注释使用中文表达；专有名词、类型名、结构体名、变量名、函数名、API 标识符、URL、错误信息字符串、日志字符串等不宜中文化的内容保留原文。

## 4. 测试模块职责

- 覆盖 Pagoda 重写实现的主要路径、边界条件与错误路径。
- 基于 public API 与 observable behavior 重新设计回归测试，验证与 Dynamo 行为契约兼容。
- `lib-copy` / Dynamo 原测试可以作为兼容性 oracle 使用，用于确认 Pagoda 重写实现没有偏离原接口行为；这是契约一致性的强校验手段。
- 提交到 Pagoda 代码中的测试应优先重写为等价行为覆盖：保留被验证的行为点、输入输出关系、边界条件和错误场景，但重新组织测试结构、测试命名、断言表达、测试注释和非必要测试数据。
- 如果某些原测试覆盖的是难以从 public API 文档重新推导的兼容性边界，可以保留等价场景，但应将测试表达改写为 Pagoda 自有形式，并在必要时补充说明该场景验证的外部契约。
- 如果确实保留或改写了 Dynamo / lib-copy 测试结构，该文件应改用 `Derived from NVIDIA Dynamo / Modified by PAGODA` 文件头。
- 测试注释统一格式：`## 测试过程` / `## 意义`。

## 5. 测试模块组织

每个文件最多保留一个统一的 `mod tests`。新增测试应并入同一个测试模块，不再保留 `supplemental_tests` 等额外测试模块。

## 6. discovery 模块例外

`discovery` 模块的偏离是有意为之，不在等价性检查范围内。

## 7. 来源判断升级规则

重写过程中应逐文件判断来源：

- 若只对齐 Dynamo public API 与行为契约，且实现、注释、测试表达均由 Pagoda 重新编写，使用 API 兼容重实现文件头。
- 若直接复制、翻译、改写、重构 Dynamo 源码、注释、测试结构或代码组织，使用 `Derived from NVIDIA Dynamo / Modified by PAGODA` 文件头。
- 若包含或改写 `async-openai` 派生协议声明、字段、enum variants、serde 形状或测试表达，使用 Pagoda + NVIDIA + Himanshu Neema 文件头，并将许可证标为 `Apache-2.0 AND MIT`。
- 边界不清时采用更保守的 attribution。