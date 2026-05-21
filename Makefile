# SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Pagoda 本地开发辅助命令
#
# 用法：make <target>
# 示例：make ci       # 本地运行完整 CI 检查
#        make test     # 运行单元测试
#        make fmt      # 格式化代码
#
# 依赖：cargo, protoc, docker（集成测试）

.PHONY: help fmt fmt-check lint build build-release test test-all \
        test-unit test-integration test-etcd doc doc-open coverage \
        clean ci audit deps-check license-check

# 默认目标：显示帮助
.DEFAULT_GOAL := help

# ── 环境变量 ──────────────────────────────────────────────────────────
PROTOC         ?= $(shell which protoc 2>/dev/null || echo "/usr/bin/protoc")
CARGO          := PROTOC=$(PROTOC) cargo
NATS_URL       ?= nats://localhost:4222
ETCD_ENDPOINTS ?= http://localhost:2379

# ── 颜色输出 ─────────────────────────────────────────────────────────
BOLD  := \033[1m
GREEN := \033[32m
YELLOW:= \033[33m
RESET := \033[0m

# ─────────────────────────────────────────────────────────────────────
# help：列出所有可用目标
# ─────────────────────────────────────────────────────────────────────
help: ## 显示帮助信息
	@echo ""
	@echo "$(BOLD)Pagoda 开发辅助命令$(RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ \
		{ printf "  $(GREEN)%-22s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""

# ─────────────────────────────────────────────────────────────────────
# 代码格式
# ─────────────────────────────────────────────────────────────────────
fmt: ## 格式化所有代码（in-place）
	$(CARGO) fmt --all

fmt-check: ## 检查格式（不修改，CI 使用）
	$(CARGO) fmt --all --check

# ─────────────────────────────────────────────────────────────────────
# 静态分析
# ─────────────────────────────────────────────────────────────────────
lint: ## 运行 clippy（默认 features，-D warnings）
	$(CARGO) clippy --all-targets -- \
		-D warnings \
		-D clippy::dbg_macro \
		-D clippy::todo

lint-all: ## 运行 clippy（所有 features）
	$(CARGO) clippy --all-targets --all-features -- \
		-D warnings

# ─────────────────────────────────────────────────────────────────────
# 构建
# ─────────────────────────────────────────────────────────────────────
build: ## 编译（debug）
	$(CARGO) build

build-release: ## 编译（release）
	$(CARGO) build --release

build-all: ## 编译（all features，debug）
	$(CARGO) build --all-features

check: ## 快速语法检查（cargo check，比 build 快）
	$(CARGO) check --all-features

# ─────────────────────────────────────────────────────────────────────
# 测试
# ─────────────────────────────────────────────────────────────────────
test: test-unit ## 运行单元测试（默认目标）

test-unit: ## 运行单元测试（无外部依赖）
	$(CARGO) test --lib

test-unit-all: ## 运行单元测试（所有 non-infra features）
	$(CARGO) test --lib --features compute-validation,tcp-low-latency

test-integration: ## 运行集成测试（需要本地 NATS，见 docker-compose.yml）
	PGD_NATS_SERVER=$(NATS_URL) \
	$(CARGO) test --features integration -- --test-threads=1

test-etcd: ## 运行 etcd 集成测试（需要本地 etcd）
	ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) \
	$(CARGO) test --features testing-etcd -- --test-threads=1

test-all: test-unit test-integration test-etcd ## 运行所有测试

# ─────────────────────────────────────────────────────────────────────
# 文档
# ─────────────────────────────────────────────────────────────────────
doc: ## 生成 API 文档
	RUSTDOCFLAGS="-D warnings" \
	$(CARGO) doc --no-deps --all-features

doc-open: ## 生成并在浏览器打开 API 文档
	$(CARGO) doc --no-deps --all-features --open

# ─────────────────────────────────────────────────────────────────────
# 代码覆盖率（需要 cargo-llvm-cov）
# ─────────────────────────────────────────────────────────────────────
coverage: ## 生成覆盖率报告（HTML，在浏览器打开）
	@command -v cargo-llvm-cov >/dev/null 2>&1 || \
		(echo "Installing cargo-llvm-cov..." && cargo install cargo-llvm-cov --locked)
	$(CARGO) llvm-cov --lib --html --open

coverage-lcov: ## 生成 LCOV 格式覆盖率（上传 Codecov 用）
	$(CARGO) llvm-cov --lib --lcov --output-path lcov.info
	@echo "LCOV 报告已写入 lcov.info"

# ─────────────────────────────────────────────────────────────────────
# 安全审计
# ─────────────────────────────────────────────────────────────────────
audit: ## 运行安全审计（需要 cargo-audit）
	@command -v cargo-audit >/dev/null 2>&1 || \
		(echo "Installing cargo-audit..." && cargo install cargo-audit --locked)
	cargo audit

# ─────────────────────────────────────────────────────────────────────
# 依赖检查
# ─────────────────────────────────────────────────────────────────────
deps-check: ## 检查依赖许可证和漏洞（需要 cargo-deny）
	@command -v cargo-deny >/dev/null 2>&1 || \
		(echo "Installing cargo-deny..." && cargo install cargo-deny --locked)
	cargo deny check

# ─────────────────────────────────────────────────────────────────────
# 许可证头检查
# ─────────────────────────────────────────────────────────────────────
license-check: ## 检查所有 .rs 文件是否包含 SPDX 许可证头
	@echo "Checking SPDX license headers..."
	@MISSING=0; \
	for file in $$(find lib/ -name "*.rs" -not -path "*/target/*"); do \
		if ! head -2 "$$file" | grep -q "SPDX-FileCopyrightText"; then \
			echo "  MISSING: $$file"; \
			MISSING=$$((MISSING + 1)); \
		fi; \
	done; \
	if [ "$$MISSING" -gt 0 ]; then \
		echo ""; \
		echo "$(YELLOW)ERROR: $$MISSING file(s) missing SPDX header$(RESET)"; \
		exit 1; \
	fi; \
	echo "$(GREEN)All files have SPDX headers ✓$(RESET)"

# ─────────────────────────────────────────────────────────────────────
# 本地 CI（模拟 GitHub Actions 主要步骤）
# ─────────────────────────────────────────────────────────────────────
ci: fmt-check lint build test-unit doc license-check ## 本地运行完整 CI 检查
	@echo ""
	@echo "$(GREEN)$(BOLD)本地 CI 全部通过 ✓$(RESET)"

ci-full: fmt-check lint-all build-all test-all doc license-check audit ## 运行完整 CI（含集成测试和审计）
	@echo ""
	@echo "$(GREEN)$(BOLD)完整 CI 全部通过 ✓$(RESET)"

# ─────────────────────────────────────────────────────────────────────
# 基础设施（本地开发服务）
# ─────────────────────────────────────────────────────────────────────
infra-up: ## 启动本地开发基础设施（NATS + etcd）
	@command -v docker >/dev/null 2>&1 || (echo "ERROR: docker not found" && exit 1)
	docker run -d --name pagoda-nats -p 4222:4222 nats:2.10-alpine 2>/dev/null || \
		echo "NATS already running"
	docker run -d --name pagoda-etcd \
		-p 2379:2379 \
		-e ALLOW_NONE_AUTHENTICATION=yes \
		-e ETCD_ADVERTISE_CLIENT_URLS=http://0.0.0.0:2379 \
		-e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 \
		bitnami/etcd:3.5 2>/dev/null || echo "etcd already running"
	@echo "$(GREEN)基础设施已启动$(RESET)"
	@echo "  NATS:  nats://localhost:4222"
	@echo "  etcd:  http://localhost:2379"

infra-down: ## 停止本地开发基础设施
	docker stop pagoda-nats pagoda-etcd 2>/dev/null || true
	docker rm   pagoda-nats pagoda-etcd 2>/dev/null || true
	@echo "基础设施已停止"

# ─────────────────────────────────────────────────────────────────────
# 清理
# ─────────────────────────────────────────────────────────────────────
clean: ## 清理编译产物
	$(CARGO) clean

clean-all: clean ## 深度清理（含 cargo-llvm-cov 产物）
	rm -rf lcov.info coverage-html/
