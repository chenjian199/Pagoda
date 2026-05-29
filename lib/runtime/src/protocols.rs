// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Dynamo 协议层：通用消息与端点标识
//!
//! ## 设计意图
//!
//! 本模块汇总了所有 dynamo 子系统都会用到的“最小通用类型”：
//! - [`LeaseId`]：etcd 租约 id 的别名，让接口签名带出语义；
//! - [`Component`]：二段命名 (namespace, name) 的轻量值对象；
//! - [`EndpointId`]：三段命名 (namespace, component, name) 的端点标识，
//!   附带 `dyn://` URL 形式。
//!
//! 子模块 [`annotated`] / [`maybe_error`] 各自承载流式增量与错误投影。
//!
//! ## 外部契约
//!
//! - [`ENDPOINT_SCHEME`]：`"dyn://"` 字面量；
//! - [`EndpointId`] 支持 `From<&str>` / `FromStr` / `Display`，以及与
//!   `Vec<&str>` 和 `[&str; 3]` 的双向 `PartialEq`（用于测试断言）；
//! - [`EndpointId::as_url`]：`dyn://ns.cp.ep` 风格 URL；
//! - 默认值 `(NS, C, E)` 通过模块私有常量定义。
//!
//! ## 实现要点
//!
//! - **解析算法**：核心是 [`parse_endpoint_segments`]，把字符串拆成
//!   `Vec<&str>` 后用切片 pattern match 处理 0/1/2/≥3 段四种情况；
//!   多余段通过 `std::iter::once + chain + collect + join("_")` 折叠进
//!   `name`，避免手写 push_str 循环；
//! - **分隔符策略**：同时识别 `.` 和 `/`，并允许用户在首尾混入空白；
//! - **`Display` 与 `as_url` 对称**：前者用 `/`、后者用 `.`，两者都用
//!   `[..].join(...)` 表达，凸显“同结构、不同分隔符”的对称关系。

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[path = "protocols/annotated.rs"]
pub mod annotated;
#[path = "protocols/maybe_error.rs"]
pub mod maybe_error;

/// etcd 租约 id 类型别名。
pub type LeaseId = i64;

// === 默认占位符 + Scheme ====================================================

/// 调用方未提供 namespace 时使用的占位符。
const DEFAULT_NAMESPACE: &str = "NS";

/// 调用方未提供 component 时使用的占位符。
const DEFAULT_COMPONENT: &str = "C";

/// 调用方未提供 endpoint name 时使用的占位符。
const DEFAULT_ENDPOINT: &str = "E";

/// 端点 URL 的固定前缀。严格意义上 `://` 不是 scheme 的一部分，
/// 但与前缀合并在一起可以减少字符串拼接次数。
pub const ENDPOINT_SCHEME: &str = "dyn://";

// === STRUCT: Component ======================================================

/// (namespace, name) 二段命名的轻量组件标识。
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Component {
    pub name: String,
    pub namespace: String,
}

// === STRUCT: EndpointId =====================================================

/// 三段命名的端点标识 (namespace / component / name)。
///
/// 字符串形式支持 `/` 或 `.` 分隔，可选 `dyn://` 前缀。例如：
/// `"dyn://ns/component/endpoint"` 与 `"ns.component.endpoint"` 等价。
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct EndpointId {
    pub namespace: String,
    pub component: String,
    pub name: String,
}

// === 解析器 =================================================================

/// 把任意输入字符串解析为最多 3 段的 `Vec<&str>`：
/// 1. 去掉可选的 [`ENDPOINT_SCHEME`] 前缀；
/// 2. 去掉首尾的空白 / `/` / `.`；
/// 3. 在 `.` 或 `/` 上切分；
/// 4. 丢弃空段。
///
/// 调用方根据返回切片长度决定如何映射到三个字段。
fn parse_endpoint_segments(input: &str) -> Vec<&str> {
    input
        .strip_prefix(ENDPOINT_SCHEME)
        .unwrap_or(input)
        .trim_matches([' ', '/', '.'])
        .split(['.', '/'])
        .filter(|segment| !segment.is_empty())
        .collect()
}

// === IMPL: 显示 / 默认值 ====================================================

impl fmt::Display for EndpointId {
    /// 以 `namespace/component/name` 形式渲染（用 `/` 分隔，与 URL 形式
    /// 的 `.` 区分开）。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rendered = [
            self.namespace.as_str(),
            self.component.as_str(),
            self.name.as_str(),
        ]
        .join("/");
        f.write_str(&rendered)
    }
}

impl Default for EndpointId {
    /// 三段都使用模块私有占位符 `(NS, C, E)`。
    fn default() -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.to_string(),
            component: DEFAULT_COMPONENT.to_string(),
            name: DEFAULT_ENDPOINT.to_string(),
        }
    }
}

// === IMPL: PartialEq 兼容 Vec/数组 ==========================================

impl PartialEq<Vec<&str>> for EndpointId {
    /// 与 `vec!["ns", "cp", "ep"]` 形式比较：要求长度恰好为 3。
    fn eq(&self, other: &Vec<&str>) -> bool {
        matches!(other.as_slice(), [ns, cp, ep]
            if self.namespace == *ns && self.component == *cp && self.name == *ep)
    }
}

impl PartialEq<[&str; 3]> for EndpointId {
    /// 与定长数组 `["ns", "cp", "ep"]` 比较。
    fn eq(&self, other: &[&str; 3]) -> bool {
        let [ns, cp, ep] = other;
        self.namespace == *ns && self.component == *cp && self.name == *ep
    }
}

impl PartialEq<EndpointId> for [&str; 3] {
    fn eq(&self, other: &EndpointId) -> bool {
        other == self
    }
}

impl PartialEq<EndpointId> for Vec<&str> {
    fn eq(&self, other: &EndpointId) -> bool {
        other == self
    }
}

// === IMPL: 解析 =============================================================

impl From<&str> for EndpointId {
    /// 从字符串构造 [`EndpointId`]。
    ///
    /// 解析规则：
    /// 1. 去掉可选 `dyn://` 前缀；
    /// 2. 去掉首尾空白 / 斜杠 / 点号；
    /// 3. 按 `.` 或 `/` 分段；
    /// 4. 缺失段用 [`Default`] 占位符补齐；
    /// 5. 多余段（第 4 段及之后）用 `_` 拼接折叠进 `name`。
    ///
    /// # 示例
    ///
    /// - `"component"` → `["NS", "component", "E"]`
    /// - `"namespace.component"` → `["namespace", "component", "E"]`
    /// - `"namespace.component.endpoint"` → `["namespace", "component", "endpoint"]`
    /// - `"namespace.component.endpoint.other.parts"`
    ///   → `["namespace", "component", "endpoint_other_parts"]`
    ///
    /// ```
    /// use dynamo_runtime::protocols::EndpointId;
    ///
    /// let endpoint = EndpointId::from("namespace/component/endpoint");
    /// assert_eq!(endpoint.namespace, "namespace");
    /// assert_eq!(endpoint.component, "component");
    /// assert_eq!(endpoint.name, "endpoint");
    /// ```
    fn from(s: &str) -> Self {
        let segments = parse_endpoint_segments(s);

        match segments.as_slice() {
            [] => Self::default(),
            [component] => Self {
                namespace: DEFAULT_NAMESPACE.to_string(),
                component: (*component).to_string(),
                name: DEFAULT_ENDPOINT.to_string(),
            },
            [namespace, component] => Self {
                namespace: (*namespace).to_string(),
                component: (*component).to_string(),
                name: DEFAULT_ENDPOINT.to_string(),
            },
            [namespace, component, head, tail @ ..] => Self {
                namespace: (*namespace).to_string(),
                component: (*component).to_string(),
                name: std::iter::once(*head)
                    .chain(tail.iter().copied())
                    .collect::<Vec<_>>()
                    .join("_"),
            },
        }
    }
}

impl FromStr for EndpointId {
    type Err = core::convert::Infallible;

    /// 通过标准 `.parse::<T>()` 模式构造，直接委托 [`From<&str>`]。
    /// 解析过程不会失败。
    ///
    /// ```
    /// use std::str::FromStr;
    /// use dynamo_runtime::protocols::EndpointId;
    ///
    /// let endpoint: EndpointId = "namespace/component/endpoint".parse().unwrap();
    /// assert_eq!(endpoint.namespace, "namespace");
    /// assert_eq!(endpoint.component, "component");
    /// assert_eq!(endpoint.name, "endpoint");
    ///
    /// let endpoint: EndpointId = "dyn://namespace/component/endpoint".parse().unwrap();
    /// assert_eq!(endpoint.name, "endpoint");
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

// === IMPL: URL 形式 =========================================================

impl EndpointId {
    /// 渲染为 `dyn://namespace.component.name` 形式的 URL。
    ///
    /// 与 [`Display`](fmt::Display) 对称：后者用 `/`，本方法用 `.`。
    pub fn as_url(&self) -> String {
        let suffix = [
            self.namespace.as_str(),
            self.component.as_str(),
            self.name.as_str(),
        ]
        .join(".");
        format!("{ENDPOINT_SCHEME}{suffix}")
    }
}

// === 单元测试 ================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// ## 测试过程
    /// 三段斜杠分隔输入走 `From<&str>`，断言三个字段。
    /// ## 意义
    /// 守护“正常路径”的基础解析。
    #[test]
    fn test_valid_endpoint_from() {
        let input = "namespace1/component1/endpoint1";
        let endpoint = EndpointId::from(input);

        assert_eq!(endpoint.namespace, "namespace1");
        assert_eq!(endpoint.component, "component1");
        assert_eq!(endpoint.name, "endpoint1");
    }

    /// ## 测试过程
    /// 同样输入走 `FromStr::from_str`，应返回 Infallible Ok。
    /// ## 意义
    /// 守护 `.parse::<EndpointId>()` 的等价路径。
    #[test]
    fn test_valid_endpoint_from_str() {
        let input = "namespace2/component2/endpoint2";
        let endpoint = EndpointId::from_str(input).unwrap();

        assert_eq!(endpoint.namespace, "namespace2");
        assert_eq!(endpoint.component, "component2");
        assert_eq!(endpoint.name, "endpoint2");
    }

    /// ## 测试过程
    /// 使用 `.parse()` 调用风格，验证类型推断与解析结果。
    /// ## 意义
    /// 验证 trait 推断不破。
    #[test]
    fn test_valid_endpoint_parse() {
        let input = "namespace3/component3/endpoint3";
        let endpoint: EndpointId = input.parse().unwrap();

        assert_eq!(endpoint.namespace, "namespace3");
        assert_eq!(endpoint.component, "component3");
        assert_eq!(endpoint.name, "endpoint3");
    }

    /// ## 测试过程
    /// 单段 `"component"` 输入：缺失 namespace 与 endpoint name。
    /// ## 意义
    /// 守护“缺段补默认”路径。
    #[test]
    fn test_endpoint_from() {
        let result = EndpointId::from("component");
        assert_eq!(
            result,
            vec![DEFAULT_NAMESPACE, "component", DEFAULT_ENDPOINT]
        );
    }

    /// ## 测试过程
    /// 点号三段输入。
    /// ## 意义
    /// 守护 `.` 分隔语义。
    #[test]
    fn test_namespace_component_endpoint() {
        let result = EndpointId::from("namespace.component.endpoint");
        assert_eq!(result, vec!["namespace", "component", "endpoint"]);
    }

    /// ## 测试过程
    /// 斜杠两段输入。
    /// ## 意义
    /// 守护“缺第三段 → 默认 endpoint 占位”。
    #[test]
    fn test_forward_slash_separator() {
        let result = EndpointId::from("namespace/component");
        assert_eq!(result, vec!["namespace", "component", DEFAULT_ENDPOINT]);
    }

    /// ## 测试过程
    /// 5 段输入，验证多余段被 `_` 折叠到 `name`。
    /// ## 意义
    /// 守护“超长输入不丢字段”的设计意图。
    #[test]
    fn test_multiple_parts() {
        let result = EndpointId::from("namespace.component.endpoint.other.parts");
        assert_eq!(
            result,
            vec!["namespace", "component", "endpoint_other_parts"]
        );
    }

    /// ## 测试过程
    /// 同时混用 `/` 与 `.`，并刻意走 `.into()` 写法。
    /// ## 意义
    /// 兼作 `From<&str>` 的 `.into()` 使用示例。
    #[test]
    fn test_mixed_separators() {
        let result: EndpointId = "namespace/component.endpoint".into();
        assert_eq!(result, vec!["namespace", "component", "endpoint"]);
    }

    /// ## 测试过程
    /// 空串与纯空白串两种输入，断言都回退到三段默认值。
    /// ## 意义
    /// 守护“极端输入也不 panic、有合理回退”的鲁棒性。
    #[test]
    fn test_empty_string() {
        let result = EndpointId::from("");
        assert_eq!(
            result,
            vec![DEFAULT_NAMESPACE, DEFAULT_COMPONENT, DEFAULT_ENDPOINT]
        );

        let result = EndpointId::from("   ");
        assert_eq!(
            result,
            vec![DEFAULT_NAMESPACE, DEFAULT_COMPONENT, DEFAULT_ENDPOINT]
        );
    }

    /// ## 测试过程
    /// 带 `dyn://` 前缀的输入解析后再 `as_url`，验证去前缀与重编码闭环。
    /// ## 意义
    /// 守护“parse → as_url”往返一致性。
    #[test]
    fn test_parse_with_scheme_and_url_roundtrip() {
        let input = "dyn://ns/cp/ep";
        let endpoint: EndpointId = input.parse().unwrap();
        assert_eq!(endpoint, vec!["ns", "cp", "ep"]);
        assert_eq!(endpoint.as_url(), "dyn://ns.cp.ep");
    }

    /// ## 测试过程
    /// 直接调 `EndpointId::default()`，断言三个字段。
    /// ## 意义
    /// 锁定模块私有常量的默认值字面量。
    #[test]
    fn test_default_endpoint_id_values() {
        let endpoint = EndpointId::default();

        assert_eq!(endpoint.namespace, DEFAULT_NAMESPACE);
        assert_eq!(endpoint.component, DEFAULT_COMPONENT);
        assert_eq!(endpoint.name, DEFAULT_ENDPOINT);
    }

    /// ## 测试过程
    /// 手动构造 `EndpointId`，验证 `to_string()` 输出三段斜杠路径。
    /// ## 意义
    /// 守护 `Display` 与解析方向独立。
    #[test]
    fn test_display_formats_three_part_path() {
        let endpoint = EndpointId {
            namespace: "ns".to_string(),
            component: "worker".to_string(),
            name: "generate".to_string(),
        };

        assert_eq!(endpoint.to_string(), "ns/worker/generate");
    }

    /// ## 测试过程
    /// 与长度不为 3 的 `Vec<&str>` 比较，断言不相等。
    /// ## 意义
    /// 守护 `PartialEq<Vec<&str>>` 的元数校验。
    #[test]
    fn test_partial_eq_vec_rejects_wrong_arity() {
        let endpoint = EndpointId::from("ns/component/name");

        assert_ne!(endpoint, vec!["ns", "component"]);
    }

    /// ## 测试过程
    /// 输入带 scheme 且第 4/5 段非空，验证 scheme 被去掉、多余段被折叠。
    /// ## 意义
    /// 把“前缀剥离 + 多段折叠”这两条路径在一个测试里联合校验。
    #[test]
    fn test_from_trims_prefix_and_collapses_extra_parts() {
        let endpoint = EndpointId::from("dyn://ns/component/name/extra.parts");

        assert_eq!(endpoint, vec!["ns", "component", "name_extra_parts"]);
    }
}
