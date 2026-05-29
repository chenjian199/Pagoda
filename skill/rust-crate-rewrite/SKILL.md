---
# SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
name: rust-crate-rewrite
description: Rust crate 精细化重写 Skill。用于对 lib/<crate>/ 中的 Rust 源码进行面向 lib-copy/<crate>/ 金标准的结构化重写、公共 API 对齐、测试保留、中文文档化和零回归验证。
---

# Rust Crate 精细化重写 Skill

## 使用场景

当任务涉及以下任一内容时，必须使用本 Skill：

- 对 `lib/<crate>/` 下 Rust crate 进行精细化重写。
- 以 `lib-copy/<crate>/` 作为只读金标准进行对齐。
- 调整 Rust 文件头部模块文档、SECTION 分区、测试矩阵或中文注释。
- 重构私有实现，同时要求公共 API 与金标准保持一致。
- 检查测试是否缺失、公共 API 是否漂移、构建测试是否回归。

## 基线约束

- 工作副本：`lib/<crate>/`
- 只读金标准：`lib-copy/<crate>/`
- 禁止编辑：`lib-copy/<crate>/`
- 验证命令：`cargo test -p <crate> --lib`
- 验收要求：`failed = 0`

## 七条硬规则

### 1. 三段头部

每个 Rust 文件开头的 `//!` 模块文档块必须包含以下中文三节：

- `## 设计意图`
- `## 外部契约`
- `## 实现要点`

要求：

- 三节标题必须出现在文件顶部模块级文档中。
- 内容必须描述当前文件的真实职责，不允许模板化空话。
- 如果文件包含 SPDX 头，SPDX 头在最前，模块文档紧随其后。

### 2. SECTION 分区

使用如下格式划分文件内部结构：

```rust
// === SECTION: 中文标签 ===
```

要求：

- 冒号必须使用 ASCII `:`，禁止使用全角 `：`。
- 文件超过 100 行时，至少 2 个 SECTION。
- 文件超过 500 行时，至少 4 个 SECTION。
- 文件超过 1000 行时，至少 5 个 SECTION。
- SECTION 标签必须是中文，并能准确描述代码区域职责。

### 3. 测试名子集

`lib-copy` 中的全部真实 `fn test_*` 测试函数，必须同名出现在 `lib` 中。

要求：

- 允许新增测试。
- 禁止删除、改名或弱化 `lib-copy` 已有测试。
- `/* */` 注释块中的测试不算真实测试。
- `#[ignore]` 测试必须写明原因，例如 `#[ignore] // reason: requires external broker`。

### 4. 测试矩阵

如果文件包含 `#[cfg(test)] mod tests`，测试模块内必须包含 `## 测试矩阵` 中文表。

表格要求：

- 列出每个测试名。
- 列出每个测试覆盖的维度。
- 来自金标准的旧测试必须标注 `(lib-copy)`。

示例：

```rust
//! ## 测试矩阵
//!
//! | 测试名 | 覆盖维度 |
//! | --- | --- |
//! | `test_parse_bool_true` | truthy 分支 `(lib-copy)` |
//! | `test_parse_bool_invalid` | 非法输入保持 false |
```

### 5. `pub_diff = 0`

公共 API 表面必须与 `lib-copy` 严格一致。

公开接口不仅要保持签名一致，还必须保持功能语义和实现目的一致。

要求：

- 顶层 `pub` 集合不可增减。
- 缩进层级中的公开项集合不可增减。
- 字段可见性、方法签名、类型名、常量名必须保持一致。
- 每个公开函数、公开类型、公开 trait、公开常量的用途必须与 `lib-copy` 保持一致。
- 公开接口的输入含义、输出含义、副作用、错误语义和边界行为必须保持一致。
- 不允许把公开接口改造成服务于不同业务目的的新语义，即使函数签名没有变化也不允许。
- 若不得不新增公开项，优先降级为 `pub(crate)` 或私有。

允许：

- 内部多态重构，例如 `Arc<Enum>` 改为 `Arc<dyn Trait>`。
- 前提是字段可见性、字段名、方法签名和外部行为 100% 一致。

### 6. 内部多样化

允许对私有实现进行多样化重构。

允许：

- 重命名私有项。
- 抽取私有 helper。
- 内联私有 helper。
- 调整私有控制流。

禁止：

- 修改并发原语类型，例如作为契约的 `parking_lot::Mutex`。
- 修改错误消息文案。
- 修改协议字面值。
- 修改外部可观察行为。

### 7. 零回归

重写后必须满足：

- `cargo build -p <crate> --lib` 通过。
- `cargo test -p <crate> --lib` 中 `failed = 0`。
- `passed` 数量大于或等于基线。

## 注释语言

所有人工注释必须使用中文：

- `//`
- `///`
- `//!`

保留不翻译：

- SPDX 文件头。
- 代码字面值。
- 错误消息文案。
- 标识符。
- 第三方协议固定术语。

文档子节标题可以翻译：

| 原标题 | 推荐标题 |
| --- | --- |
| `# Examples` | `# 示例` |
| `# Arguments` | `# 参数` |
| `# Returns` | `# 返回` |

引用代码符号必须使用反引号，例如 `parse_bool`。

## 工作流

执行重写任务时，按以下顺序处理：

1. 读取 `lib-copy/<crate>/FILE.rs`。
2. 记录金标准中的 `pub` 集合、公开接口目的、测试名集合和硬契约。
3. 硬契约包括错误文案、协议字面值、并发原语类型和外部可见签名。
4. 改写 `lib/<crate>/FILE.rs`。
5. 补齐三段头部模块文档。
6. 添加或整理 SECTION 分区。
7. 汉化人工注释。
8. 添加或更新测试矩阵。
9. 可选新增 diversification 测试。
10. 校验 `pub_diff = 0`。
11. 校验测试名子集。
12. 运行构建和测试。
13. 若出现新增 `pub` 项，降级为 `pub(crate)` 或私有。

## 审计脚本

可使用以下脚本进行初步结构审计：

```bash
#!/bin/bash
# 用法: bash audit.sh <src-dir>
# 例: bash audit.sh runtime/src
cd "$(git rev-parse --show-toplevel)"
SRC="${1:-runtime/src}"
LIB="lib/$SRC"
COPY="lib-copy/$SRC"

for f in $(find "$LIB" -type f -name '*.rs' | sed "s|^$LIB/||" | sort); do
  [ -f "$COPY/$f" ] || { echo "MISSING-IN-COPY: $f"; continue; }

  L=$(wc -l < "$LIB/$f")
  H=$(grep -c '设计意图' "$LIB/$f")
  S=$(grep -cE 'SECTION[:：]' "$LIB/$f")
  M=$(grep -c '测试矩阵' "$LIB/$f")
  T=$(grep -cE '#\[(tokio::)?test\]' "$LIB/$f")
  CT=$(grep -cE '#\[(tokio::)?test\]' "$COPY/$f")
  PT=$(diff <(grep -E '^pub( |\()' "$COPY/$f" | sort -u) \
            <(grep -E '^pub( |\()' "$LIB/$f"  | sort -u) | wc -l)

  fail=""
  [ "$H" -eq 0 ] 2>/dev/null && fail="$fail no-header"
  [ "$L" -gt 100 ] 2>/dev/null && [ "$S" -eq 0 ] 2>/dev/null && fail="$fail no-section"
  [ "$T" -gt 0 ] 2>/dev/null && [ "$M" -eq 0 ] 2>/dev/null && fail="$fail no-matrix"
  [ "$T" -lt "$CT" ] 2>/dev/null && fail="$fail tests-missing($T<$CT)"
  [ "$PT" -gt 0 ] 2>/dev/null && fail="$fail pub-top-diff($PT)"
  [ -n "$fail" ] && echo "FAIL $f:$fail"
done

echo "===DONE==="
```

## 判定要点

避免误判时遵循以下规则：

- `pub(super) const X` 位于私有子模块内时，外部不可见，通常合规。
- `Arc<Enum>` 改为 `Arc<dyn Trait>`，且字段不可名、方法签名不变时，通常合规。
- `grep ^pub` 行数差可能来自 rustfmt 折行，应使用 `sort -u` 比较集合。
- 有意结构性重构必须列入白名单，并说明为什么不会改变公共 API。
- `lib-copy` 测试位于 `/* */` 注释块内时，不算真实测试。
- 需要外部 broker、Kubernetes、Etcd 等环境的测试，应使用 `#[ignore]` 并注明原因。
- Rust 2024 edition 中调用 `set_var` 需要使用 `unsafe { ... }` 包裹。
- 涉及全局环境变量的测试必须使用本测试专属变量名，避免并发干扰。

## 验收清单

完成任务前必须确认：

- [ ] `bash audit.sh <src-dir>` 输出 `===DONE===` 且无 `FAIL`。
- [ ] `cargo build -p <crate> --lib` 通过。
- [ ] `cargo test -p <crate> --lib` 中 `failed = 0`。
- [ ] `cargo test -p <crate> --lib` 中 `passed >= 基线`。
- [ ] 已列出本次合规白名单，或确认无白名单。
- [ ] 未编辑 `lib-copy/<crate>/`。
- [ ] 未修改错误消息文案和协议字面值。
- [ ] 未破坏公共 API 表面。
- [ ] 公开接口的功能语义和实现目的与 `lib-copy` 保持一致。
