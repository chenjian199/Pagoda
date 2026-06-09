---
# SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
name: dyn2pgd-check
description: Pagoda 项目代码审计标准（5条契约规则）。用于对任意源码文件进行系统化检查，确保注释国际化、代码清洁、历史术语淘汰、前缀标准化等各项契约均已满足。
---

# Pagoda 代码审计标准 Skill

## 使用场景

当需要对 Pagoda 项目内任意源码目录进行系统性审计时使用本 Skill：

- 新完成的模块（如 `lib/tokenizers`, `lib/tokens`）的最终验收。
- 遗留代码的现代化改造（移除 `dyn-` 前缀、更新旧术语等）。
- 跨多个文件的契约一致性检查（SPDX 头、注释语言、命名规范）。
- 代码提交前的完整性审核。

## 5 条核心检查规则

### 规则 1：注释国际化 —— 仅中文，翻译而非删除

**检查对象**：所有源码文件中的注释（//、/**/、///、//!）

**规则定义**：
- **允许**：纯中文注释、代码标识符、URL、代码片段
- **禁止**：英文 sentence-level 注释（≥3 个连续英文单词）
- **禁止**：中英混合表述（如 "这是 test case"）

**核心原则**：
- **严禁直接删除英文注释**。所有英文 sentence-level 注释必须翻译为等义的中文注释，
  不得因"看不懂"或"太长"而整段移除。
- 翻译应保留原注释的结构语义（如"说明返回值""说明参数""说明边界条件"）。
- 代码标识符、URL、包名/crate 名、markdown 链接引用等不翻译。

**检查清单**：
```
1. grep -E '[a-zA-Z]{3,}' <file> | grep -i '//'  找所有含英文单词的注释行
2. 排除以下不算英文：
   - URL (http://, https://, file://)
   - 代码标识符 (struct Foo, fn bar, mod baz)
   - 包名/crate名 (async-openai, tokio)
   - 数学/专业术语单字母缩写 (T, U, D)
3. 对确认的英文注释行 → 翻译成等义中文，不得删除
4. 翻译后代码行为（如断言消息、log 消息、行内注释）必须保持原有功能不变
```

**示例修复**：
```rust
// 错误（英文注释未翻译）
/// Test the async operation flow.

// 正确（翻译为中文本地化注释）
/// 测试异步操作流程。

// 错误（直接删掉了英文注释，丢失了文档）
// （原本有一行 /// Returns the parsed token count. 被整行删除）

// 正确（翻译保留语义）
/// 返回已解析的 token 数量。
```

---

### 规则 2：代码清洁 —— 移除注释掉的死代码

**检查对象**：所有被注释掉（//、/* */）的代码行

**规则定义**：
- **允许**：临时调试注释（加日期标记或TODO标签）
- **允许**：模式示例（带清晰的"示例"标记）
- **禁止**：无标签的永久性死代码块（>10 行）
- **禁止**：版本控制混乱的代码（if 0、disabled、old impl）

**检查清单**：
```
1. 查找模式: ^[[:space:]]*(//|/\*)[[:space:]]*[a-zA-Z0-9]
2. 对每个匹配行判断：
   - 是否有明确的 TODO/FIXME/DEBUG 标签？
   - 是否有日期或 issue 引用？
   - 是否超过 3 行未使用？
3. 如无标签且>3行 → 删除
```

**示例修复**：
```rust
// 错误
// let result = do_something();
// if result.is_ok() {
//   println!("ok");
// }

// 正确
// TODO: 考虑后续优化流程 (2026-06-01)
// let fast_path = ...;
```

---

### 规则 3：历史术语淘汰 —— 移除过时前缀和缩写

**检查对象**：标识符名、注释、文档、版本描述中的旧术语

**规则定义**：

#### 3a. 前缀标准化
- **禁止**：`dyn-*`、`dynamo-*`（NVIDIA 遗留）
- **禁止**：其他非 Pagoda 前缀
- **必须**：`pagoda-*`（Cargo.toml package name）

#### 3b. 术语更新
- 禁止：`comp` → 应为 `servicegroup` （缩写 `sg`）
- 禁止：`ep` → 应为 `portname` （缩写 `pn`）
- 禁止：`dyn` 缩写 → 应为 `pgd`（缩写 `pgd`）
- 禁止：三段式 key 描述 `{ns}/{comp}/{ep}` → 四段式 `{ns}/{sg}/{pn}/{id}`

#### 3c. 项目自我认知
- **禁止**：`lib-copy` 相关描述（过时的参考架构说法）
- **禁止**：纯 `NVIDIA CORPORATION` 旧版权头（无 Pagoda 版权行）
- **允许**：API 兼容重实现文件使用双版权头（Pagoda + NVIDIA）并附来源说明
- **必须**：将其描述为 Pagoda 独立项目

**检查清单**：
```
1. grep -E 'dyn-|dynamo-|comp[^a-z]|ep[^a-z]|\{.*comp.*\}|lib-copy' <file>
2. 每行判断是否需要替换
3. 常见替换对：
   - s/dynamo-/pagoda-/g
   - s/dyn([^a-z])/pgd\1/g
   - s/servicegroup.*comp/servicegroup/g (变量名对齐)
   - s/\{ns\}\/\{comp\}\/\{ep\}/\{ns\}\/\{sg\}\/\{pn\}\{id\}/g
4. 版权头单独判定：
   - 若为 API-compatible 双版权头（Pagoda + NVIDIA + 来源说明）→ 合规
   - 若仅有 NVIDIA 旧版权头（无 Pagoda 版权行）→ 违规
```

**示例修复**：
```rust
// 错误
/// 使用 `{ns}/{comp}/{ep}/{instance_id:x}` key 格式。

// 正确
/// 使用 `{ns}/{sg}/{pn}/{instance_id:x}` key 格式。
```

---

### 规则 4：文件头标准化 —— SPDX 和版权

**检查对象**：每个源码文件的前 10 行

**规则定义**：
- **必须**：所有新建源码文件都有 SPDX 头，且首行非空即为版权头区域
- **必须**：至少包含一行 Pagoda 版权（`Copyright (c) 2026-2028 PAGODA`）
- **必须**：`SPDX-License-Identifier: Apache-2.0`
- **必须**：SPDX 头是文件首段非空内容
- **允许**：API 兼容重实现头（Pagoda + NVIDIA + API-compatible 来源说明）
- **禁止**：纯 NVIDIA 旧版权头（无 Pagoda 行），以及仅保留 `2024-2026` / `2025-2026` 的旧模板
- **例外**：自动生成文件（Cargo.lock）、保留上游原始版权的代码

**检查清单**：
```
1. 查看文件前 10 行
2. 确认存在 SPDX-FileCopyrightText
3. 确认至少有一行 Pagoda 版权（2026-2028）
4. 确认 SPDX-License-Identifier: Apache-2.0
5. 若出现 NVIDIA 行，检查是否同时满足：
   - 存在 Pagoda 版权行
   - 存在 API-compatible 来源说明段
   - 不是纯 NVIDIA 旧模板
```

**文件格式对应**：
```
Rust/C/C++/JS/TS:
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

Rust/C/C++/JS/TS（API-compatible 双版权头，允许）:
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

TOML/YAML/Python/Shell:
# SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
```

---

### 规则 5：架构自我认知 —— lib-copy 术语淘汰

**检查对象**：代码注释、文档、模块说明中对项目结构的描述

**规则定义**：
- **禁止**：`lib-copy` 作为参考点或"金标准"的描述
- **禁止**：将当前代码视为"副本"的表述
- **必须**：将 Pagoda crate 描述为自有项目、独立实现
- **可选**：保留来源说明（如"基于 async-openai"），但要明确当前是 Pagoda 分支

**检查清单**：
```
1. grep -E 'lib-copy|参考自|副本|金标准' <file>
2. 每处判断：
   - 是否在阐述依赖关系？→ 改为"基于 X，已为 Pagoda 适配"
   - 是否暗示代码不独立？→ 改为"实现了 X，扩展了..."
   - 是否涉及遗留文档？→ 更新为当前结构描述
```

**示例修复**：
```rust
// 错误
//! lib-copy 中的 streaming 类型副本。
//! 参考了上游 async-openai 的设计。

// 正确
//! Pagoda 推理服务的 streaming 类型实现。
//! 基于上游 async-openai，进行了服务层扩展（continuous_usage_stats 等）。
```

---

## 使用流程

### 单个文件快速审核
```bash
# 步骤 1: 检查英文混合
grep -E '[a-zA-Z]{3,}' <file> | grep -v 'http' | grep -v '//'

# 步骤 2: 检查死代码
grep -E '^[[:space:]]*//' <file> | wc -l

# 步骤 3: 检查旧术语
grep -E 'dyn|comp|ep|lib-copy' <file>

# 步骤 4: 检查 SPDX
head -5 <file> | grep 'SPDX'

# 步骤 4.1: 检查是否为“纯 NVIDIA 旧头”（命中则需修复）
head -10 <file> | grep -E 'NVIDIA' && ! head -10 <file> | grep -E 'PAGODA'

# 步骤 5: 检查 lib-copy 术语
grep -E 'lib-copy|副本|金标准' <file>
```

### 目录级批量审核
```bash
# 扫描整个 lib/runtime/src/
for file in $(find lib/runtime/src -name '*.rs'); do
  echo "=== Checking $file ==="
  # 应用上述 5 条规则
done
```

---

## 验收标准

通过本 Skill 审核的代码应满足：

- 0 行英文 sentence 注释
- 0 个无标签的死代码块（>3 行）
- 0 个 `dyn-`/`dynamo-` 前缀
- 0 个 `comp`/`ep` 术语（应为 `sg`/`pn`）
- 所有文件头都有正确 SPDX
- 0 个 `lib-copy` 相关描述
- `cargo check -p <crate>` 无编译错误
- `cargo test -p <crate>` 所有测试通过

---

## 参考约束文件

- [CLAUDE.md](../../CLAUDE.md) —— 文件头 SPDX 规范
- [standard.md](../../standard.md) —— Pagoda 项目标准
- [docs/lib/runtime/](../../docs/lib/runtime/) —— 各模块设计文档
