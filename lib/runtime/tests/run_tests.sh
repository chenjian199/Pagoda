#!/usr/bin/env bash
# =============================================================================
# Pagoda Runtime 集成测试运行脚本
#
# 用法:
#   ./run_tests.sh                  # PR 级别（默认，跳过 ignored）
#   ./run_tests.sh nightly          # Nightly（NATS + etcd）
#   ./run_tests.sh release          # Release（K8s + soak）
#   ./run_tests.sh all              # 全部测试（需要完整环境）
#   ./run_tests.sh <test_name>      # 运行单个测试文件
#
# 环境变量:
#   NATS_SERVER         NATS broker 地址（默认 nats://127.0.0.1:4222）
#   ETCD_ENDPOINTS      etcd 集群地址（默认 http://127.0.0.1:2379）
#   POD_IP              K8s Pod IP（K8s 测试需要）
#   PGD_SOAK_RUN_DURATION  soak 测试时长（默认 5s）
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
pass()  { echo -e "${GREEN}[PASS]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail()  { echo -e "${RED}[FAIL]${NC} $*"; }

# 默认配置
THREADS="${TEST_THREADS:-10}"
SOAK_DURATION="${PGD_SOAK_RUN_DURATION:-5}"
TIMEOUT="${TEST_TIMEOUT:-600}"

# 切换到项目目录
cd "$PROJECT_DIR"

# -----------------------------------------------------------------------------
# PR 级别测试（无外部依赖）
# -----------------------------------------------------------------------------
run_pr_tests() {
    info "运行 PR 级别集成测试（跳过 #[ignore]）..."
    cargo test -p pagoda-runtime --tests -- --test-threads="$THREADS" 2>&1
    local ret=$?
    if [ $ret -eq 0 ]; then
        pass "PR 级别测试全部通过"
    else
        fail "PR 级别测试存在失败 (exit=$ret)"
    fi
    return $ret
}

# -----------------------------------------------------------------------------
# Nightly 测试（NATS + etcd）
# -----------------------------------------------------------------------------
run_nightly_tests() {
    local nats_url="${NATS_SERVER:-nats://127.0.0.1:4222}"
    local etcd_url="${ETCD_ENDPOINTS:-http://127.0.0.1:2379}"
    local failed=0

    # --- NATS 测试 ---
    info "检查 NATS broker: $nats_url ..."
    if curl -s --max-time 2 "$nats_url" >/dev/null 2>&1 || nats-server --version >/dev/null 2>&1; then
        info "NATS 可用，运行 NATS 集成测试..."
        NATS_SERVER="$nats_url" cargo test -p pagoda-runtime \
            --test request_plane \
            --test event_plane \
            --test storage \
            -- --test-threads="$THREADS" --include-ignored 2>&1
        if [ $? -eq 0 ]; then
            pass "NATS 测试通过"
        else
            warn "NATS 测试存在失败（部分测试可能需要 JetStream 或 Docker）"
            ((failed++))
        fi
    else
        warn "NATS broker 不可用，跳过 NATS 测试"
        warn "  启动 NATS: nats-server -p 4222 -js &"
    fi

    # --- etcd 测试 ---
    info "检查 etcd 集群: $etcd_url ..."
    if curl -s --max-time 2 "${etcd_url}/version" >/dev/null 2>&1; then
        info "etcd 可用，运行 etcd 集成测试..."
        ETCD_ENDPOINTS="$etcd_url" cargo test -p pagoda-runtime \
            --test discovery \
            --features testing-etcd \
            -- --test-threads="$THREADS" --include-ignored 2>&1
        if [ $? -eq 0 ]; then
            pass "etcd 测试通过"
        else
            fail "etcd 测试失败"
            ((failed++))
        fi
    else
        warn "etcd 集群不可用，跳过 etcd 测试"
        warn "  启动 etcd: etcd --listen-client-urls http://0.0.0.0:2379 &"
    fi

    return $failed
}

# -----------------------------------------------------------------------------
# Release 测试（K8s + soak）
# -----------------------------------------------------------------------------
run_release_tests() {
    local pod_ip="${POD_IP:-127.0.0.1}"
    local failed=0

    # --- K8s 测试 ---
    info "检查 Kubernetes 集群..."
    if kubectl cluster-info >/dev/null 2>&1; then
        info "K8s 可用，运行 K8s 集成测试..."
        warn "K8s 测试需要在 Pod 内运行，或设置 POD_IP/POD_NAME/POD_UID 环境变量"
        POD_IP="$pod_ip" cargo test -p pagoda-runtime \
            --test discovery \
            --features integration-kube \
            -- --test-threads=1 --include-ignored kube:: 2>&1
        if [ $? -eq 0 ]; then
            pass "K8s 测试通过"
        else
            warn "K8s 测试失败（可能需要集群内运行 + 预创建 namespace）"
            ((failed++))
        fi
    else
        warn "K8s 集群不可用，跳过 K8s 测试"
    fi

    # --- Soak 测试 ---
    info "运行 soak 测试 (duration=${SOAK_DURATION}s)..."
    PGD_SOAK_RUN_DURATION="$SOAK_DURATION" cargo test -p pagoda-runtime \
        --test soak \
        -- --test-threads=1 --include-ignored 2>&1
    if [ $? -eq 0 ]; then
        pass "Soak 测试通过"
    else
        warn "部分 soak 测试失败或超时"
        ((failed++))
    fi

    return $failed
}

# -----------------------------------------------------------------------------
# 单文件测试
# -----------------------------------------------------------------------------
run_single_test() {
    local test_name="$1"
    # 移除可能的 .rs 后缀和路径前缀
    test_name="${test_name%.rs}"
    test_name="${test_name##*/}"

    info "运行单个测试文件: $test_name ..."
    cargo test -p pagoda-runtime \
        --test "$test_name" \
        -- --test-threads="$THREADS" 2>&1
    local ret=$?
    if [ $ret -eq 0 ]; then
        pass "$test_name 测试通过"
    else
        fail "$test_name 测试失败 (exit=$ret)"
    fi
    return $ret
}

# -----------------------------------------------------------------------------
# 主入口
# -----------------------------------------------------------------------------
main() {
    echo ""
    echo "=============================================="
    echo "  Pagoda Runtime 集成测试"
    echo "=============================================="
    echo "  项目目录: $PROJECT_DIR"
    echo "  测试线程: $THREADS"
    echo "----------------------------------------------"
    echo ""

    local mode="${1:-pr}"

    case "$mode" in
        pr)
            run_pr_tests
            ;;
        nightly)
            run_pr_tests || true
            echo ""
            run_nightly_tests
            ;;
        release)
            run_pr_tests || true
            echo ""
            run_release_tests
            ;;
        all)
            run_pr_tests || true
            echo ""
            run_nightly_tests || true
            echo ""
            run_release_tests || true
            ;;
        help|--help|-h)
            echo "用法: $0 [pr|nightly|release|all|<test_name>]"
            echo ""
            echo "  pr           PR 级别测试（默认，无外部依赖）"
            echo "  nightly      Nightly 测试（需要 NATS + etcd）"
            echo "  release      Release 测试（需要 K8s + soak）"
            echo "  all          全部测试"
            echo "  <test_name>  运行单个测试文件"
            echo ""
            echo "示例:"
            echo "  $0                          # PR 级别"
            echo "  $0 nightly                  # Nightly (NATS+etcd)"
            echo "  $0 component_routing        # 单个测试"
            echo "  NATS_SERVER=nats://10.0.0.1:4222 $0 nightly"
            ;;
        *)
            run_single_test "$mode"
            ;;
    esac

    echo ""
    echo "----------------------------------------------"
    echo "  测试完成"
    echo "=============================================="
}

main "$@"
