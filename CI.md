# Pagoda CI / CD 说明文档

本文档描述 Pagoda 项目的持续集成（CI）与持续发布（CD）流程，帮助团队成员理解每条流水线的作用、触发时机、本地复现方法及常见问题排查。

---

## 目录

1. [CI 文件总览](#1-ci-文件总览)
2. [主 CI 流水线（ci.yml）](#2-主-ci-流水线)
3. [安全审计（audit.yml）](#3-安全审计)
4. [发布流水线（release.yml）](#4-发布流水线)
5. [本地复现 CI](#5-本地复现-ci)
6. [Branch Protection 配置](#6-branch-protection-配置)
7. [环境变量与 Secrets](#7-环境变量与-secrets)
8. [工具安装](#8-工具安装)
9. [常见问题](#9-常见问题)
10. [维护说明](#10-维护说明)

---

## 1. CI 文件总览

```
Pagoda/
├── .github/
│   ├── workflows/
│   │   ├── ci.yml          # 主 CI：格式、lint、构建、测试、覆盖率
│   │   ├── audit.yml       # 安全审计：CVE 扫描、许可证合规
│   │   └── release.yml     # 发布：tag 触发，生成 SBOM 并创建 Release
│   └── PULL_REQUEST_TEMPLATE.md   # PR 提交模板
│
├── rust-toolchain.toml     # 固定 Rust stable 工具链（含 rustfmt/clippy/llvm-tools）
├── rustfmt.toml            # 代码格式规范（max_width=100，import 分组等）
├── deny.toml               # cargo-deny 配置（许可证白名单、漏洞策略、禁止 crate 列表）
├── Makefile                # 本地开发快捷命令（make ci / make test / make fmt …）
└── .cargo/config.toml      # Cargo 别名与构建配置
```

---

## 2. 主 CI 流水线

**文件**：`.github/workflows/ci.yml`

**触发条件**：
- `push` 到 `main` 或 `develop` 分支（忽略纯文档变更）
- PR 目标为 `main` 或 `develop`

**并发控制**：同一 ref 触发多次时自动取消旧的运行，节省 Actions 用量。

### 2.1 Job 列表与依赖关系

```
fmt ──┐
       ├──► build (dev) ──┐
clippy ┘   build (rel)    ├──► test-unit ──► coverage
                           ├──► test-integration
                           └──► test-etcd

doc     (独立运行)
msrv    (独立运行，验证 Rust 1.82 最低版本)
deny    (独立运行，依赖检查)
license-headers  (独立运行)

ci-success (汇总 Job，Branch Protection 仅需设置此一条)
```

### 2.2 各 Job 详解

| Job | 功能 | 失败原因 & 修复 |
|-----|------|----------------|
| `fmt` | `cargo fmt --check` | 运行 `make fmt` 后提交 |
| `clippy` | 静态分析，`-D warnings` | 修复所有 `warning` |
| `build` | debug + release 双 profile 编译 | 检查编译错误 |
| `test-unit` | 内联单元测试，无外部依赖 | `make test-unit` 本地复现 |
| `test-integration` | NATS 集成测试 | `make infra-up && make test-integration` |
| `test-etcd` | etcd 集成测试 | `make infra-up && make test-etcd` |
| `doc` | `cargo doc`，`RUSTDOCFLAGS=-D warnings` | 修复文档注释中的悬空链接 |
| `msrv` | `cargo check` on Rust 1.82 | 避免使用 1.82 后才稳定的 API |
| `coverage` | `cargo llvm-cov`，上传 Codecov | 纯信息，不阻塞 merge |
| `deny` | 依赖许可证 + CVE 检查 | 更新 `deny.toml` 或升级依赖 |
| `license-headers` | 检查 `.rs` 文件 SPDX 头 | 在文件开头加版权头（见下方格式） |
| `ci-success` | 汇总所有必须 Job 结果 | Branch Protection 的唯一检查点 |

### 2.3 许可证头格式

每个 `.rs` 文件必须以以下两行开头（由 `license-headers` Job 强制检查）：

```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
```

---

## 3. 安全审计

**文件**：`.github/workflows/audit.yml`

**触发条件**：
- 每周一 UTC 02:00 自动运行
- `main` 分支变更 `Cargo.lock` 或 `Cargo.toml` 时
- 手动触发（Actions → Run workflow）

| Job | 工具 | 说明 |
|-----|------|------|
| `audit` | `rustsec/audit-check` | 对照 RustSec 漏洞数据库扫描依赖 |
| `deny-licenses` | `cargo-deny` | 确认所有依赖许可证合规 |
| `udeps` | `cargo-udeps` (nightly) | 检测未使用的依赖声明 |
| `report` | GitHub Issue | 审计失败且为定时触发时，自动创建 Issue |

**如何处理审计失败**：
1. 查看 `audit` Job 的日志，找到漏洞的 Advisory ID（如 `RUSTSEC-2024-XXXX`）
2. 升级受影响依赖：`cargo update <crate-name>`
3. 如无法立即升级（等待上游修复），在 `deny.toml` 的 `[advisories].ignore` 中添加豁免条目，**必须附带原因和预计处理日期**

---

## 4. 发布流水线

**文件**：`.github/workflows/release.yml`

**触发条件**：推送语义化版本 tag，格式为 `v<MAJOR>.<MINOR>.<PATCH>` 或 `v<MAJOR>.<MINOR>.<PATCH>-<pre>`

```bash
# 正式发布
git tag v0.2.0 -m "Release v0.2.0"
git push origin v0.2.0

# 预发布
git tag v0.2.0-beta.1 -m "Beta release"
git push origin v0.2.0-beta.1
```

**发布前必做**：
1. 更新 `lib/runtime/Cargo.toml` 中的 `version` 字段，与 tag 一致
2. 确认 `main` 分支 CI 全绿
3. 确认 CHANGELOG（可借助 git log 自动生成）

**发布产物**：
| 产物 | 说明 |
|------|------|
| GitHub Release | 含 changelog、SBOM |
| `sbom-vX.Y.Z.json` | CycloneDX 格式软件物料清单 |
| GitHub Pages | API 文档（cargo doc 输出） |

---

## 5. 本地复现 CI

所有 CI 步骤均可通过 `make` 命令在本地运行：

```bash
# 一键本地 CI（等同于主要 CI Jobs，不含集成测试）
make ci

# 完整 CI（含集成测试，需先启动基础设施）
make infra-up
make ci-full

# 单独运行各步骤
make fmt-check     # 格式检查
make lint          # Clippy
make build         # 编译
make test-unit     # 单元测试
make doc           # 文档构建
make license-check # 许可证头检查
make audit         # 安全审计
make deps-check    # 依赖检查（cargo-deny）
make coverage      # 覆盖率报告（HTML，自动打开）
```

### 5.1 启动本地测试基础设施

```bash
# 启动 NATS + etcd
make infra-up

# 验证
curl -s http://localhost:2379/health | python3 -m json.tool
nc -z localhost 4222 && echo "NATS OK"

# 停止
make infra-down
```

---

## 6. Branch Protection 配置

在 GitHub 仓库设置 → **Branches** → **Branch protection rules** 中，对 `main` 和 `develop` 应设置：

| 设置项 | 推荐值 |
|--------|--------|
| Require status checks to pass | ✅ 启用 |
| Required status checks | `CI Success`（汇总 Job） |
| Require branches to be up to date | ✅ 启用 |
| Require pull request reviews | ✅ 至少 1 人审核 |
| Dismiss stale reviews | ✅ 新 push 后作废旧审核 |
| Require signed commits | 可选（推荐启用） |
| Restrict force pushes | ✅ 禁止 force push |

> **为什么只设置 `CI Success` 一条**：`ci-success` Job 通过 `needs` 依赖所有其他 Job，任何一个失败它都会失败。这样 Branch Protection 只需维护一个检查点，不需要随每次添加新 Job 而手动更新保护规则。

---

## 7. 环境变量与 Secrets

### GitHub Actions Secrets（需在仓库 Settings → Secrets 中配置）

| Secret | 用途 | 必须 |
|--------|------|------|
| `CODECOV_TOKEN` | 上传覆盖率报告到 Codecov | 可选 |
| `GITHUB_TOKEN` | 自动由 Actions 注入，无需手动配置 | 自动 |

### CI 使用的环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PROTOC` | `/usr/bin/protoc` | protobuf 编译器路径（etcd-client 构建需要） |
| `PGD_NATS_SERVER` | `nats://localhost:4222` | NATS 服务器地址（集成测试用） |
| `ETCD_ENDPOINTS` | `http://localhost:2379` | etcd 地址（etcd 集成测试用） |
| `RUST_BACKTRACE` | `1` | 测试失败时打印完整 backtrace |
| `CARGO_TERM_COLOR` | `always` | CI 日志中保留 ANSI 颜色 |

---

## 8. 工具安装

### 必需工具

```bash
# Rust 工具链（rustup 会自动读取 rust-toolchain.toml）
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update

# protoc（etcd-client 构建时需要）
# Ubuntu/Debian:
sudo apt-get install protobuf-compiler
# macOS:
brew install protobuf
```

### 可选工具（CI 中自动安装，本地按需安装）

```bash
# 代码覆盖率
cargo install cargo-llvm-cov --locked

# 安全审计
cargo install cargo-audit --locked

# 依赖检查
cargo install cargo-deny --locked

# 未使用依赖检测（需要 nightly）
cargo +nightly install cargo-udeps --locked

# SBOM 生成
cargo install cargo-cyclonedx --locked
```

---

## 9. 常见问题

### Q: `fmt` Job 失败，提示格式不符合

**原因**：本地 `rustfmt` 版本与 CI 不一致，或提交前未运行格式化。

**解决**：
```bash
make fmt
git add -A && git commit --amend --no-edit
```

---

### Q: `clippy` Job 报告 `error[E0...]`（变成编译错误）

**原因**：`-D warnings` 将所有 warning 提升为 error。

**解决**：本地运行 `make lint`，逐一修复 clippy 提示。

---

### Q: `license-headers` Job 报告缺少版权头

**原因**：新建的 `.rs` 文件未添加 SPDX 头。

**解决**：在文件开头添加：
```rust
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
```

可在编辑器中配置模板，或在 Makefile 中添加 `add-header` 目标自动补全。

---

### Q: `msrv` Job 失败，提示 API 在 1.82 不存在

**原因**：使用了在 Rust 1.82 之后才稳定的 API。

**解决**：
- 查看 [Rust API 稳定版本](https://doc.rust-lang.org/stable/std/) 确认 API 的最低稳定版本
- 使用替代 API，或将 `Cargo.toml` 中的 `rust-version` 适当提高（需团队讨论）

---

### Q: `audit` Job 报告 CVE

**解决方案**（按优先级）：
1. **升级依赖**：`cargo update <affected-crate>`
2. **等待上游修复**：在 `deny.toml` 中临时忽略（添加原因和日期）
3. **替换依赖**：改用未受影响的替代库

---

### Q: `test-integration` Job 一直等待 NATS

**原因**：NATS 服务容器健康检查失败，服务未正常启动。

**排查**：查看 Job 日志中"Wait for NATS"步骤的输出，确认 `nc -z localhost 4222` 是否超时。

---

### Q: `deny` Job 报告许可证不符合

**解决**：
1. 确认依赖使用的许可证（`cargo license` 可列出全部）
2. 如果许可证兼容 Apache-2.0，在 `deny.toml` 的 `[licenses].allow` 中添加
3. 如果不兼容（如 GPL），替换该依赖

---

## 10. 维护说明

### 升级 Actions 版本

定期（每季度）将 `.github/workflows/` 中的 Actions 版本升级到最新：

```bash
# 推荐使用 Dependabot 自动管理
# 在 .github/dependabot.yml 中配置（见下方示例）
```

```yaml
# .github/dependabot.yml（推荐添加）
version: 2
updates:
  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "monthly"
  - package-ecosystem: "cargo"
    directory: "/"
    schedule:
      interval: "weekly"
```

### 新增 Crate 时

1. 确认 `deny.toml` 中的许可证白名单包含新 crate 的许可证
2. 运行 `make deps-check` 验证
3. 若 crate 引入新的外部服务依赖，在 `ci.yml` 对应 Job 中添加 `services` 配置

### 添加新 CI Job

1. 在 `ci.yml` 中添加 Job 定义
2. 将新 Job 加入 `ci-success` 的 `needs` 列表
3. 在本文档第 2.2 节的 Job 列表中补充说明
4. 在 `Makefile` 中添加对应的本地命令

---

*最后更新：2026-05-21*
*维护：PAGODA CORPORATION & AFFILIATES*
