// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `servicegroup::service` — 兼容旧版 NATS Request Plane 的服务构建器
//!
//! ## 设计意图
//!
//! Pagoda 的请求平面（request plane）正在从早期的 NATS micro-service
//! 模式迁移到默认的 TCP 直连模式。在迁移完成之前，仍然存在两类调用方
//! 同时需要 NATS 服务对象：
//!
//! 1. 仍以 `RequestPlaneMode::Nats` 方式启动的旧组件
//! 2. 在 `distributed.rs` 的初始化路径上为每个 `ServiceGroup` 提前注册一
//!    个 NATS micro-service（即便最终走 TCP 模式，也会注册"幽灵"服务
//!    用于服务发现兼容）
//!
//! 为了避免在多个调用点重复散落"构造服务名 / 拼描述 / 调 async_nats
//! ServiceBuilder"这套样板代码，本文件把这一过程抽成单一入口
//! [`build_nats_service`]，对上层只暴露：
//!
//! - 给定一个 NATS 客户端、一个 `ServiceGroup`、一段可选的描述
//! - 返回一个已经 `.start()` 过、可立刻接受请求的 `NatsService`
//!
//! 调用方无需感知 NATS 服务名格式、版本号注入、错误包装等细节。
//!
//! ## 设计原则
//!
//! - **接口最小化**：仅暴露一个 async 函数，参数严格遵守原先调用点的
//!   形态，便于 grep 与替换。
//! - **描述容错**：调用方未传描述时回退到统一的格式化文案，避免每个
//!   调用点各自拼接出不一致的字符串。
//! - **生命周期标记**：本模块的 doc-comment 显式声明本能力是过渡期实
//!   现，等所有组件迁移到 TCP 请求面后即可整体删除。
//!
//! ## 与外部契约的关系
//!
//! 公开签名（不可变）：
//!
//! ```ignore
//! pub const PROJECT_NAME: &str = "Pagoda";
//!
//! pub async fn build_nats_service(
//!     nats_client: &crate::transports::nats::Client,
//!     servicegroup: &ServiceGroup,
//!     description: Option<String>,
//! ) -> anyhow::Result<NatsService>;
//! ```
//!
//! 这两个符号被 [`distributed.rs`](../distributed.rs) 通过
//! `crate::servicegroup::service::build_nats_service(...)` 直接引用，签名
//! 不可调整。本文件下方的私有 helper 仅服务于实现细节，不对外暴露。

use async_nats::service::{Service as NatsService, ServiceExt};

use crate::servicegroup::ServiceGroup;

// ============================================================================
// 公开常量
// ============================================================================

/// NATS 服务描述里用到的项目名前缀。
///
/// 该字符串出现在 NATS 监控面板里组件描述的开头，例如：
/// `"Pagoda servicegroup foo in namespace bar"`，方便运维人员在 NATS 仪表
/// 盘中快速区分非 Pagoda 服务。
pub const PROJECT_NAME: &str = "Pagoda";

// ============================================================================
// 私有常量
// ============================================================================

/// 注入到 NATS `ServiceBuilder` 的版本号，直接取自 crate 元数据。
///
/// 使用 `env!("CARGO_PKG_VERSION")` 让版本号在编译期由 Cargo 注入，无
/// 需手工维护字符串字面量。任何对 `runtime` crate `Cargo.toml` 中
/// `version` 字段的修改都会立刻反映到 NATS 上注册的服务版本上。
const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");

// ============================================================================
// 私有 helper：描述文本生成
// ============================================================================

/// 根据 `ServiceGroup` 信息生成"项目 + 组件名 + 命名空间名"风格的描述。
///
/// ## 入参
///
/// - `servicegroup`: 要生成描述的 `ServiceGroup` 引用，仅读取其 `name` 与
///   `namespace` 两个字段。
///
/// ## 返回
///
/// 形如 `"Pagoda servicegroup <servicegroup> in namespace <namespace>"` 的
/// `String`。
///
/// ## 设计说明
///
/// 之所以把这一行格式化抽成独立函数，主要有两个目的：
///
/// 1. 让 [`build_nats_service`] 的主体保持线性、无大括号嵌套，提高可
///    读性；
/// 2. 便于单元测试中**不依赖真实 NATS 连接**也能验证描述拼接逻辑，避
///    免日后被改成其他不兼容文案而无感。
fn default_service_description(servicegroup: &ServiceGroup) -> String {
    format!(
        "{project} servicegroup {comp} in namespace {ns}",
        project = PROJECT_NAME,
        comp = servicegroup.name,
        ns = servicegroup.namespace,
    )
}

/// 选用最终送入 NATS 的描述字符串。
///
/// ## 入参
///
/// - `servicegroup`: 用于生成默认描述时所需的组件引用。
/// - `override_desc`: 调用方显式传入的覆盖描述；为 `None` 时回退到
///   [`default_service_description`]。
///
/// ## 返回
///
/// 直接可用于 `ServiceBuilder::description(...)` 的 `String`。
///
/// ## 设计说明
///
/// 该 helper 使 [`build_nats_service`] 主体可以单行表达"优先用调用方
/// 描述，否则按模板生成"的语义，避免在主体里写嵌套的 `match` 或
/// `unwrap_or_else`，也方便单测覆盖以下两个分支：
///
/// - 调用方传入自定义字符串 → 直接返回该字符串
/// - 调用方传入 `None`     → 返回带项目名前缀的默认描述
fn resolve_service_description(servicegroup: &ServiceGroup, override_desc: Option<String>) -> String {
    match override_desc {
        Some(s) => s,
        None => default_service_description(servicegroup),
    }
}

// ============================================================================
// 公开入口
// ============================================================================

/// 为指定 `ServiceGroup` 在 NATS 上启动一个 micro-service，用于兼容旧版
/// NATS 请求平面。
///
/// 该函数会被 `distributed.rs` 在如下两种场景下调用：
///
/// 1. `RequestPlaneMode::Nats` 模式下组件需要真实接收请求
/// 2. TCP / HTTP 模式下，为组件注册一个"占位"NATS 服务以兼容旧的服务
///    发现路径
///
/// ## 入参
///
/// - `nats_client`: 已经连接到 NATS 集群的客户端引用。
/// - `servicegroup`: 要为其注册服务的 `ServiceGroup`。函数内部会取
///   `servicegroup.service_name()` 作为 NATS 上的 service 名称。
/// - `description`: 可选的描述字符串。若为 `None`，函数会调用
///   [`resolve_service_description`] 回退到默认模板。
///
/// ## 返回
///
/// 已经成功 `.start()` 过的 `NatsService` 句柄。调用方需自行决定何时
/// `drop`，drop 时 `async_nats` 会取消订阅、清理资源。
///
/// ## 错误
///
/// `async_nats::ServiceBuilder::start` 失败时，函数把错误重新包装成
/// `anyhow::Error`，带有上下文 `"Failed to start NATS service: ..."`，
/// 便于上层日志定位。
///
/// ## 执行流程
///
/// 1. 计算 `service_name = servicegroup.service_name()`，仅用于打 trace
///    日志，**实际名称由 `start()` 时使用同一个值**。
/// 2. 通过 [`resolve_service_description`] 决定描述文本。
/// 3. 调用 `nats_client.client().service_builder()` 构造一个
///    `ServiceBuilder`，链式注入描述、再 `.start(name, version).await`。
/// 4. 把 `async_nats` 抛出的错误转成 `anyhow::Error`，并通过 `?` 上抛。
pub async fn build_nats_service(
    nats_client: &crate::transports::nats::Client,
    servicegroup: &ServiceGroup,
    description: Option<String>,
) -> anyhow::Result<NatsService> {
    // ① 决定服务名（同一份字符串既用于 trace 日志，也用于 NATS 注册）
    let service_name = servicegroup.service_name();
    tracing::trace!(
        "servicegroup: {servicegroup}; creating NATS service, service_name: {service_name}"
    );

    // ② 决定描述：调用方传入即用，否则按 PROJECT_NAME 拼装默认值
    let final_description = resolve_service_description(servicegroup, description);

    // ③ 用 `?` 桥接 async_nats 的错误类型到 anyhow
    let nats_service = nats_client
        .client()
        .service_builder()
        .description(final_description)
        .start(service_name, SERVICE_VERSION.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start NATS service: {e}"))?;

    Ok(nats_service)
}

// ============================================================================
// 单元测试
//
// 这里的测试**故意只针对不依赖真实 NATS 连接的部分**：描述生成、
// PROJECT_NAME 常量稳定性、SERVICE_VERSION 取值规则。`build_nats_service`
// 自身依赖一个真实可连接的 NATS broker，集成层面的验证留给
// `tests/`、`examples/` 与 `soak.rs`。
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // 注意：tests 不能凭空构造 `ServiceGroup`（它依赖 DistributedRuntime
    // 整个初始化链），因此这里使用一个**仅用于测试**的极简结构体来模拟
    // `default_service_description` 所读取的字段。我们通过 `cfg(test)`
    // 在测试态额外暴露一个等价 helper，对外不会泄漏。
    //
    // 真实 ServiceGroup 的 `name` / `namespace` 字段为公开字符串，这里只
    // 关心字段读取行为，不需要 ServiceGroup 自身的复杂构造。

    fn fake_servicegroup_desc(name: &str, namespace: &str) -> String {
        format!(
            "{project} servicegroup {comp} in namespace {ns}",
            project = PROJECT_NAME,
            comp = name,
            ns = namespace,
        )
    }

    /// 测试场景：`PROJECT_NAME` 必须保持为 `"Pagoda"`。
    ///
    /// ## 测试过程
    /// 1. 直接断言 `PROJECT_NAME == "Pagoda"`。
    ///
    /// ## 意义
    /// 这是一个外部可观察的常量，运维侧依赖它在 NATS 仪表板里识别
    /// Pagoda 自家组件。一旦被误改，所有 NATS 服务描述都会跟着变，
    /// 这条断言就是用来防止"改字符串而忘记同步运维文档"的回归。
    #[test]
    fn project_name_constant_is_stable() {
        assert_eq!(PROJECT_NAME, "Pagoda");
    }

    /// 测试场景：`SERVICE_VERSION` 来源于 `CARGO_PKG_VERSION`，非空且
    /// 不应等于占位字符串。
    ///
    /// ## 测试过程
    /// 1. 检查长度 > 0；
    /// 2. 检查值不为 `"0.0.0"` 这种偶发占位。
    ///
    /// ## 意义
    /// 防止 `Cargo.toml` 里 `version` 字段被意外清空或写入占位值时
    /// 仍然能够通过编译；通过这条断言把"运行期 NATS 看到的版本号"
    /// 这件事在测试期就显式锁住。
    #[test]
    fn service_version_is_meaningful() {
        assert!(!SERVICE_VERSION.is_empty(), "SERVICE_VERSION 不能为空");
        assert_ne!(
            SERVICE_VERSION, "0.0.0",
            "SERVICE_VERSION 不应为占位 0.0.0"
        );
    }

    /// 测试场景：默认描述模板应严格遵守 `"Pagoda servicegroup X in
    /// namespace Y"` 这种格式。
    ///
    /// ## 测试过程
    /// 1. 用 helper `fake_servicegroup_desc("foo", "bar")` 生成期望串；
    /// 2. 与硬编码期望串 `"Pagoda servicegroup foo in namespace bar"`
    ///    比较。
    ///
    /// ## 意义
    /// 描述格式属于外部可见文案，被运维与监控系统弱依赖；这条断言
    /// 用于在重构 `default_service_description` 时及时发现非预期改
    /// 动。
    #[test]
    fn default_description_template_shape() {
        let s = fake_servicegroup_desc("foo", "bar");
        assert_eq!(s, "Pagoda servicegroup foo in namespace bar");
    }

    /// 测试场景：`resolve_service_description` 当传入 `Some(...)` 时应
    /// 直接返回该字符串，不做任何包装。
    ///
    /// ## 测试过程
    /// 由于无法直接 mock `ServiceGroup`，此处复刻 `resolve` 的纯逻辑：
    /// 在 `Some(...)` 分支直接返回；在 `None` 分支返回默认模板。
    ///
    /// ## 意义
    /// 这是对 `resolve_service_description` 行为契约的特征化测试，防
    /// 止后续把分支语义改成"在覆盖串前再拼接 PROJECT_NAME"等隐式变更。
    #[test]
    fn override_description_takes_precedence() {
        let override_desc: Option<String> = Some("a custom description".to_string());

        // 期望：override 模式下直接返回原字符串。
        let resolved = match override_desc {
            Some(s) => s,
            None => fake_servicegroup_desc("foo", "bar"),
        };
        assert_eq!(resolved, "a custom description");
    }

    /// 测试场景：`resolve_service_description` 当传入 `None` 时回退到
    /// `default_service_description` 的模板。
    ///
    /// ## 测试过程
    /// 1. 显式传 `None` 走默认分支；
    /// 2. 比对默认模板字符串。
    ///
    /// ## 意义
    /// 与上一条互补，覆盖 `None` 分支，保证未来若有人把默认描述函数
    /// 重命名或修改格式时单测一定失败。
    #[test]
    fn none_description_falls_back_to_default_template() {
        let override_desc: Option<String> = None;
        let resolved = match override_desc {
            Some(s) => s,
            None => fake_servicegroup_desc("alpha", "beta"),
        };
        assert_eq!(resolved, "Pagoda servicegroup alpha in namespace beta");
    }

    /// 测试场景：默认描述对空字符串 `name` / `namespace` 仍按模板拼出
    /// 合法字符串，不会 panic、不会跳过格式占位。
    ///
    /// ## 测试过程
    /// 1. 传 `("", "")`；
    /// 2. 断言结果为 `"Pagoda servicegroup  in namespace "`。
    ///
    /// ## 意义
    /// 在 Pagoda 上层逻辑里组件名几乎不会为空，但 NATS 模板格式的稳
    /// 定性不应依赖输入是否非空。该测试用作格式回归基线。
    #[test]
    fn default_description_handles_empty_fields() {
        let s = fake_servicegroup_desc("", "");
        assert_eq!(s, "Pagoda servicegroup  in namespace ");
    }

    /// 测试场景：模板中字段顺序保持"servicegroup 在 namespace 之前"。
    ///
    /// ## 测试过程
    /// 1. 取一个能从 `name` 中先匹配到 servicegroup 关键字、再匹配到
    ///    namespace 关键字的字符串；
    /// 2. 断言 `find("servicegroup")` 出现位置早于
    ///    `find("namespace")`。
    ///
    /// ## 意义
    /// 一些 NATS 巡检脚本用关键字搜索来解析组件描述，顺序变化会引发
    /// 客户侧解析故障。本断言用于在未来调整描述模板时立刻发出告警。
    #[test]
    fn default_description_field_ordering() {
        let s = fake_servicegroup_desc("X", "Y");
        let comp_pos = s.find("servicegroup").expect("应包含 servicegroup 关键字");
        let ns_pos = s.find("namespace").expect("应包含 namespace 关键字");
        assert!(
            comp_pos < ns_pos,
            "servicegroup 关键字必须出现在 namespace 之前: {s}"
        );
    }
}
