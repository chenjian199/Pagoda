# SKILL.md：Pagoda 版权与许可证标注规则

## 1. 适用范围

本规则用于指导 Pagoda 项目中源文件、模块、crate 的版权头、许可证声明和上游来源标注。

尤其适用于以下场景：

- 参考 NVIDIA Dynamo 项目实现 Pagoda 模块；
- 与 Dynamo 的 public API、结构体、字段、函数签名、文件名或行为契约保持兼容；
- 从 Dynamo 直接复制、改写、重构部分代码；
- 使用或派生自 `async-openai` 的协议类型；
- 判断某个文件应只标 Pagoda，还是也应保留 NVIDIA / async-openai 来源。

本规则不是正式法律意见，但作为 Pagoda 项目内的工程合规规则使用。

---

## 2. 总原则

判断一个文件的版权头时，必须先判断这个文件的来源。

核心问题是：

```text
这个文件是 Pagoda 完全原创，
还是基于 Dynamo / async-openai 的接口、结构、声明或代码表达？
```

不要只看函数体是否重写，也要看：

- 文件名是否沿用上游；
- 模块路径是否沿用上游；
- public struct / enum 名称是否沿用上游；
- 字段名、字段类型、字段顺序是否沿用上游；
- 函数名、函数签名、trait 名称是否沿用上游；
- enum variants 是否沿用上游；
- serde tag / rename / default 等协议表达是否沿用上游；
- 测试结构、注释、文档是否复制或改写自上游；
- 实现逻辑是否只是改写表达，而非独立设计。

如果一个文件只是函数体重写，但文件结构、公开 API、类型树、字段树、协议契约基本对齐 Dynamo，则不应简单视为完全原创文件。

---

## 3. 文件分类规则

### 3.1 Pagoda 完全原创文件

满足以下条件时，可视为 Pagoda 完全原创文件：

- 文件不是从 Dynamo 或 async-openai 复制、改写而来；
- public API 没有刻意对齐 Dynamo；
- 结构体、字段、函数签名、enum variants 不是从 Dynamo 逐项复现；
- 实现逻辑、代码组织、注释和测试都是 Pagoda 自己设计；
- 没有保留上游代码表达。

这类文件只写 Pagoda 版权头：

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0
```

对应 crate 如果只包含这类文件或纯 Apache-2.0 来源文件：

```toml
license = "Apache-2.0"
```

---

### 3.2 只参考 Dynamo 思想或行为，不复现 API 声明面

如果只是参考 Dynamo 的设计思想、模块职责、行为目标，但没有逐项复现它的文件名、结构体、字段、函数签名和代码表达，可以按 Pagoda 原创处理。

文件头：

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0
```

建议在模块 README、NOTICE 或 THIRD_PARTY_NOTICES 中加入说明：

```text
This module is a Pagoda-native implementation inspired by NVIDIA Dynamo.
It does not copy NVIDIA Dynamo source code unless explicitly stated in file headers.
NVIDIA Dynamo is available at https://github.com/ai-dynamo/dynamo and is licensed under the Apache License, Version 2.0.
```

---

### 3.3 API 兼容重实现：声明面对齐 Dynamo，但函数体重写

如果文件满足以下特征：

- 文件名与 Dynamo 一致；
- 模块路径与 Dynamo 一致；
- public struct / enum / trait 名称与 Dynamo 一致；
- 字段名、字段类型、函数名、函数签名与 Dynamo 一致；
- public API surface 与 Dynamo 基本对齐；
- 目标是保持下游调用者不感知差异；
- 但函数体实现由 Pagoda 重新编写，未直接复制 Dynamo 实现代码；

则该文件属于：

```text
API-compatible implementation based on Dynamo public interfaces and behavioral contracts.
```

这种文件不建议只写 Pagoda。推荐写 Pagoda + NVIDIA，并明确说明是 API 兼容重实现，而不是直接复制源码。

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

含义：

- Pagoda 拥有自己重写实现的版权；
- NVIDIA attribution 用于说明 public interface / behavioral contract 来源；
- 许可证仍为 Apache-2.0；
- 该表述比 `Derived from` 更准确，因为它强调接口兼容重实现，而不是直接复制函数体。

---

### 3.4 直接复制、改写或重构 Dynamo 源码

如果文件包含以下情况之一：

- 直接复制了 Dynamo 源码；
- 在 Dynamo 原文件基础上删改；
- 翻译、改写了 Dynamo 注释或文档；
- 保留了 Dynamo 的实现结构和代码表达；
- 测试用例、测试数据、断言结构来自 Dynamo；
- 只是通过变量改名、函数重排、局部重写来降低相似度；

则该文件应视为基于 Dynamo 的派生文件。

推荐文件头：

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// Derived from NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Modified by PAGODA.
```

---

### 3.5 含 async-openai 派生内容的文件

如果文件中还包含或改写了 `async-openai` 的协议类型、结构体、字段、enum variants、serde 表达或其它源码内容，则必须同时保留 `async-openai` 的 MIT 来源。

适用场景包括：

- 文件来自 Dynamo 的 `lib/protocols`，而 Dynamo 对应文件本身基于 `async-openai`；
- 文件直接复制或改写了 `async-openai` 类型声明；
- 保留了 `async-openai` 的结构体字段、enum variants 或 serde 形状；
- Cargo.toml 中对应 crate 标注为 `Apache-2.0 AND MIT`。

推荐文件头：

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-FileCopyrightText: Copyright (c) 2022 Himanshu Neema
// SPDX-License-Identifier: Apache-2.0 AND MIT
//
// Based on NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
//
// Portions are based on async-openai (https://github.com/64bit/async-openai)
// by Himanshu Neema.
// Implementation rewritten or modified by PAGODA.
```

对应 `Cargo.toml`：

```toml
license = "Apache-2.0 AND MIT"
```

如果该 crate 内只有部分文件包含 MIT 来源，也应确保：

- 文件级 SPDX 正确；
- crate 级 license 不低估实际许可证组合；
- MIT license 文本或 attribution 能在项目中找到；
- `ATTRIBUTIONS-Rust.md` / `THIRD_PARTY_NOTICES.md` 中保留对应来源。

---

## 4. Cargo.toml 许可证规则

### 4.1 纯 Pagoda / Dynamo Apache-2.0 来源

如果 crate 只包含：

- Pagoda 自己写的 Apache-2.0 文件；
- Dynamo Apache-2.0 派生或 API 兼容重实现文件；

则 `Cargo.toml` 使用：

```toml
license = "Apache-2.0"
```

### 4.2 含 async-openai MIT 来源

如果 crate 中包含 `async-openai` 派生内容，或保留了 MIT 来源类型，则使用：

```toml
license = "Apache-2.0 AND MIT"
```

不要只写：

```toml
license = "Apache-2.0"
```

---

## 5. 文件头选择表

| 文件情况 | 文件头 |
| --- | --- |
| Pagoda 完全原创 | Pagoda + Apache-2.0 |
| 只参考 Dynamo 思想，不对齐声明面 | Pagoda + Apache-2.0；可在 README/NOTICE 说明参考来源 |
| 文件名/API/字段/函数签名基本对齐 Dynamo，但实现重写 | Pagoda + NVIDIA + Apache-2.0；说明 API-compatible implementation |
| 直接复制/改写/重构 Dynamo 源码 | Pagoda + NVIDIA + Apache-2.0；说明 Derived from / Modified by |
| 含 async-openai 派生内容 | Pagoda + NVIDIA + Himanshu Neema + Apache-2.0 AND MIT |
| 完全新加测试，未复制上游测试 | Pagoda + Apache-2.0 |
| 改写 Dynamo 测试或保留其测试结构 | Pagoda + NVIDIA + Apache-2.0 |
| 改写 async-openai 测试或协议声明 | Pagoda + NVIDIA + Himanshu Neema + Apache-2.0 AND MIT |

---

## 6. 判断是否需要 NVIDIA attribution

需要 NVIDIA attribution 的典型情况：

```text
文件名、模块路径、public API、结构体字段、trait、函数签名与 Dynamo 基本一致；
或者文件是从 Dynamo 代码改写、翻译、重构而来；
或者测试、注释、文档结构来源于 Dynamo。
```

不需要 NVIDIA attribution 的典型情况：

```text
只是了解 Dynamo 的设计思想；
只是实现同类功能；
没有复制或逐项复现 Dynamo 的声明结构；
API、字段、文件结构、测试结构均为 Pagoda 自己设计。
```

边界不清时，按保守原则处理：

```text
如果文件的声明面高度对齐 Dynamo，则保留 NVIDIA attribution。
```

---

## 7. “API 兼容重实现”和“派生文件”的区别

### API 兼容重实现

特点：

- 目标是保持 public API 兼容；
- 类型名、字段名、函数签名可能对齐；
- 实现逻辑由 Pagoda 独立重写；
- 没有复制原文件函数体；
- 没有复制注释、测试和文档表达。

推荐表述：

```text
API-compatible implementation based on the public interfaces and behavioral
contracts of NVIDIA Dynamo.
Implementation rewritten by PAGODA.
```

### 派生文件

特点：

- 直接复制或改写了 Dynamo 源文件；
- 保留了明显的实现表达；
- 保留了注释、测试结构或代码组织；
- 只是局部替换命名、重排逻辑或改写函数体。

推荐表述：

```text
Derived from NVIDIA Dynamo.
Modified by PAGODA.
```

---

## 8. 统一推荐文件头

### 8.1 Pagoda 原创文件

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0
```

### 8.2 Dynamo API 兼容重实现文件

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.
```

### 8.3 Dynamo 派生文件

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// Derived from NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Modified by PAGODA.
```

### 8.4 Dynamo + async-openai 派生文件

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-FileCopyrightText: Copyright (c) 2022 Himanshu Neema
// SPDX-License-Identifier: Apache-2.0 AND MIT
//
// Based on NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
//
// Portions are based on async-openai (https://github.com/64bit/async-openai)
// by Himanshu Neema.
// Implementation rewritten or modified by PAGODA.
```

---

## 9. 禁止做法

- 不要把 API 兼容重实现伪装成完全原创。
- 不要把所有文件都机械标成 `Derived from`。
- 不要遗漏 MIT 来源。
- 不要只在 `lib.rs` 或 `Cargo.toml` 中统一声明上游来源，而忽略具体文件头。
- 不要删除上游版权声明。
- 不要把 MIT 来源错误改成纯 Apache-2.0。
- 不要把完全新写文件错误标成上游派生。

---

## 10. README / NOTICE 推荐说明

如果某个模块大量进行 Dynamo API 兼容重实现，可以在模块 README 或 NOTICE 中加入：

```text
This module contains Pagoda-native implementations designed for API compatibility
with selected public interfaces and behavioral contracts of NVIDIA Dynamo.

Unless explicitly stated in individual file headers, the implementation code is
rewritten by PAGODA.

NVIDIA Dynamo is available at https://github.com/ai-dynamo/dynamo and is licensed
under the Apache License, Version 2.0.
```

如果涉及 async-openai：

```text
Some protocol types are based on async-openai
(https://github.com/64bit/async-openai) by Himanshu Neema, licensed under the MIT
License. Files containing async-openai-derived content are marked with
`SPDX-License-Identifier: Apache-2.0 AND MIT`.
```

---

## 11. 代码生成 / 重写任务中的执行规则

在让 AI 或代码生成工具重写 Dynamo 相关模块时，必须遵守：

1. 先判断目标文件属于哪一类来源；
2. 根据分类选择正确文件头；
3. 如果 public API 与 Dynamo 对齐，即使函数体重写，也保留 NVIDIA attribution；
4. 如果文件含 async-openai 派生声明，保留 MIT attribution；
5. 不删除上游版权声明；
6. 不把 MIT 来源错误改成纯 Apache-2.0；
7. 不把完全新写文件错误标成上游派生；
8. 在不确定时，采用更保守的 attribution。

---

## 12. 最终决策流程

处理每个文件时，按下面顺序判断：

```text
1. 是否含 async-openai 派生内容？
   是 -> 使用 Pagoda + NVIDIA + Himanshu Neema，Apache-2.0 AND MIT。

2. 是否直接复制/改写/重构 Dynamo 源码？
   是 -> 使用 Pagoda + NVIDIA，Apache-2.0，标 Derived from / Modified by。

3. 是否 public API、字段、函数签名、文件结构高度对齐 Dynamo？
   是 -> 使用 Pagoda + NVIDIA，Apache-2.0，标 API-compatible implementation。

4. 是否只是参考思想、功能目标或行为设计？
   是 -> 使用 Pagoda，Apache-2.0，可在 README/NOTICE 说明参考来源。

5. 是否完全原创？
   是 -> 使用 Pagoda，Apache-2.0。
```

---

## 13. 一句话规则

```text
函数体重写不等于完全原创。
如果文件的 public API、结构体、字段、函数签名和文件结构高度对齐 Dynamo，
就按 API 兼容重实现处理，保留 NVIDIA attribution。

只有完全没有复制或复现 Dynamo 声明结构的文件，才只写 Pagoda。
```
