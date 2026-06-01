// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `pagoda_config` —— 通用配置解析工具集
//!
//! ## 设计意图
//! 为整个 pagoda 工作区提供"环境变量 / 配置文件中的布尔字符串"统一解析能力，
//! 让上层 crate 不再各自实现 "1/true/yes/on" ↔ "0/false/no/off" 的判定。
//! - 不引入任何运行时依赖（仅 `anyhow` for 错误传播）；
//! - 区分"宽松判定"（`is_*` / `env_is_*` 返回 `bool`，对未知值视为 false）
//!   与"严格解析"（`parse_bool` / `env_parse_bool` 对未知值返回 `Err`）；
//! - 大小写不敏感（全部走 `to_lowercase`）。
//!
//! ## 外部契约
//! - 6 个 `pub fn`，签名与可见性必须严格一致：
//!   - `pub fn is_truthy(val: &str) -> bool`
//!   - `pub fn is_falsey(val: &str) -> bool`
//!   - `pub fn parse_bool(val: &str) -> anyhow::Result<bool>`
//!   - `pub fn env_is_truthy(env: &str) -> bool`
//!   - `pub fn env_is_falsey(env: &str) -> bool`
//!   - `pub fn env_parse_bool(env: &str) -> anyhow::Result<Option<bool>>`
//! - "truthy 字面集" = `{1, true, on, yes}`；"falsey 字面集" = `{0, false, off, no}`。
//!   集合外的值在严格 API 中触发 `Err`；在宽松 API 中视为 false。
//! - `parse_bool("")` 必须返回 `Err`（空串既非 truthy 也非 falsey）。
//! - `env_parse_bool` 在变量未设置时返回 `Ok(None)`，在设置为非法值时返回 `Err`。
//! - 错误消息格式 `"Invalid boolean value: '{val}'. Expected one of: true/false, 1/0, on/off, yes/no"`
//!   是契约的一部分，下游可能以此 grep 匹配。
//!
//! ## 实现要点
//! - `matches!` 宏配合 `to_lowercase()` 做字面常量匹配，避免分配 `HashSet`。
//! - 6 个 API 在 2×3 维度上对称（truthy/falsey 一组、宽松/严格一组、
//!   字符串/环境变量一组），测试矩阵以此遍历。
//! - 不在 crate 顶层 re-export 任何 `integrations` 类型。

// === SECTION: 字面集合判定（基础层）===

/// 判定字符串是否为「真」字面。
///
/// 用于评估环境变量或其它由用户设置、需按布尔语义解释的字符串配置项。
///
/// 真字面集合（大小写不敏感）：`"1"`、`"true"`、`"on"`、`"yes"`。
///
/// 集合外的值（包括空串、未知词）一律返回 `false`。
/// 若需要对非法值抛错而非静默 false，请改用 [`parse_bool`]。
pub fn is_truthy(val: &str) -> bool {
    matches!(val.to_lowercase().as_str(), "1" | "true" | "on" | "yes")
}

/// 判定字符串是否为「假」字面（[`is_truthy`] 的对偶函数）。
///
/// 假字面集合（大小写不敏感）：`"0"`、`"false"`、`"off"`、`"no"`。
///
/// 集合外的值一律返回 `false`。
/// 若需要对非法值抛错而非静默 false，请改用 [`parse_bool`]。
pub fn is_falsey(val: &str) -> bool {
    matches!(val.to_lowercase().as_str(), "0" | "false" | "off" | "no")
}

// === SECTION: 严格解析（带错误传播）===

/// 严格地把字符串解析为布尔值；非法输入返回 `Err`。
///
/// 用于「用户必须正确填写」的强约束场景（与宽松判定的 `is_*` 形成互补）。
///
/// # 参数
/// * `val` —— 待解析的字符串
///
/// # 返回
/// * `Ok(true)` —— 真字面（大小写不敏感）：`"1"`、`"true"`、`"on"`、`"yes"`
/// * `Ok(false)` —— 假字面（大小写不敏感）：`"0"`、`"false"`、`"off"`、`"no"`
/// * `Err(_)` —— 任何其它输入（含空串）
///
/// # 示例
/// ```ignore
/// assert_eq!(parse_bool("true")?, true);
/// assert_eq!(parse_bool("0")?, false);
/// assert!(parse_bool("maybe").is_err());
/// ```
pub fn parse_bool(val: &str) -> anyhow::Result<bool> {
    if is_truthy(val) {
        Ok(true)
    } else if is_falsey(val) {
        Ok(false)
    } else {
        anyhow::bail!(
            "Invalid boolean value: '{}'. Expected one of: true/false, 1/0, on/off, yes/no",
            val
        )
    }
}

// === SECTION: 环境变量包装（宽松层）===

/// 判定指定环境变量的取值是否为「真」字面。
///
/// 当环境变量未设置或取值非法时返回 `false`。若需区分「未设置 / 合法 / 非法」
/// 三种状态，请改用 [`env_parse_bool`]。
pub fn env_is_truthy(env: &str) -> bool {
    match std::env::var(env) {
        Ok(val) => is_truthy(val.as_str()),
        Err(_) => false,
    }
}

/// 判定指定环境变量的取值是否为「假」字面。
///
/// 当环境变量未设置或取值非法时返回 `false`。若需区分「未设置 / 合法 / 非法」
/// 三种状态，请改用 [`env_parse_bool`]。
pub fn env_is_falsey(env: &str) -> bool {
    match std::env::var(env) {
        Ok(val) => is_falsey(val.as_str()),
        Err(_) => false,
    }
}

// === SECTION: 环境变量解析（严格层 + 三态返回）===

/// 严格地把环境变量解析为布尔值；以 `Option` 区分「未设置 / 合法 / 非法」。
///
/// # 参数
/// * `env` —— 环境变量名
///
/// # 返回
/// * `Ok(Some(true))` —— 已设置且为真字面
/// * `Ok(Some(false))` —— 已设置且为假字面
/// * `Ok(None)` —— 未设置
/// * `Err(_)` —— 已设置但取值非法（透传 [`parse_bool`] 的错误文案）
///
/// # 示例
/// ```ignore
/// match env_parse_bool("MY_FLAG")? {
///     Some(true) => println!("enabled"),
///     Some(false) => println!("disabled"),
///     None => println!("not configured"),
/// }
/// ```
pub fn env_parse_bool(env: &str) -> anyhow::Result<Option<bool>> {
    match std::env::var(env) {
        Ok(val) => parse_bool(&val).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => anyhow::bail!("Failed to read environment variable {}: {}", env, e),
    }
}

// === SECTION: tests ===

#[cfg(test)]
mod tests {
    //! ## 测试矩阵
    //!
    //! | 测试名 | 覆盖维度 |
    //! |---|---|
    //! | `test_is_truthy` | `is_truthy` 全字面 + 大小写 + 反例|
    //! | `test_is_falsey` | `is_falsey` 全字面 + 大小写 + 反例|
    //! | `test_parse_bool` | `parse_bool` 三类输入：truthy/falsey/Err|
    //! | `test_env_is_truthy_not_set` | `env_is_truthy` 变量未设置 → false|
    //! | `test_env_is_falsey_not_set` | `env_is_falsey` 变量未设置 → false|
    //! | `test_env_parse_bool_not_set` | `env_parse_bool` 未设置 → `Ok(None)`|
    //! | `test_is_truthy_falsey_disjoint` | 两字面集合互不重叠（自洽性）|
    //! | `test_parse_bool_error_message_contract` | 错误消息文案契约（grep 友好）|
    //! | `test_parse_bool_mixed_case_roundtrip` | 严格 API 也支持大小写混合 |
    //! | `test_env_apis_roundtrip` | 设置后宽松/严格三 API 行为一致 |
    //! | `test_env_parse_bool_invalid_value_errors` | 已设置但非法 → `Err` |
    //! | `test_env_parse_bool_invalid_passes_through_message` | 透传 parse_bool 错误文案 |
    //!
    //! ## 备注
    //! 涉及 `std::env::set_var` 的用例使用**互不相同的变量名**避免并发干扰，
    //! 无需 `serial_test`。`set_var` 在 Rust 2024 edition 起为 `unsafe`，
    //! 故新增用例显式包裹 `unsafe { ... }`。

    use super::*;

    #[test]
    fn test_is_truthy() {
        assert!(is_truthy("1"));
        assert!(is_truthy("true"));
        assert!(is_truthy("True"));
        assert!(is_truthy("TRUE"));
        assert!(is_truthy("on"));
        assert!(is_truthy("ON"));
        assert!(is_truthy("yes"));
        assert!(is_truthy("YES"));

        assert!(!is_truthy("0"));
        assert!(!is_truthy("false"));
        assert!(!is_truthy("off"));
        assert!(!is_truthy("no"));
        assert!(!is_truthy(""));
        assert!(!is_truthy("random"));
    }

    #[test]
    fn test_is_falsey() {
        assert!(is_falsey("0"));
        assert!(is_falsey("false"));
        assert!(is_falsey("False"));
        assert!(is_falsey("FALSE"));
        assert!(is_falsey("off"));
        assert!(is_falsey("OFF"));
        assert!(is_falsey("no"));
        assert!(is_falsey("NO"));

        assert!(!is_falsey("1"));
        assert!(!is_falsey("true"));
        assert!(!is_falsey("on"));
        assert!(!is_falsey("yes"));
        assert!(!is_falsey(""));
        assert!(!is_falsey("random"));
    }

    #[test]
    fn test_env_is_truthy_not_set() {
        // 使用一个绝不会被定义的变量名，验证未设置时返回 false
        assert!(!env_is_truthy("DEFINITELY_NOT_SET_VAR_12345"));
    }

    #[test]
    fn test_env_is_falsey_not_set() {
        // 使用一个绝不会被定义的变量名，验证未设置时返回 false
        assert!(!env_is_falsey("DEFINITELY_NOT_SET_VAR_12345"));
    }

    #[test]
    fn test_parse_bool() {
        // 真字面
        assert!(parse_bool("1").unwrap());
        assert!(parse_bool("true").unwrap());
        assert!(parse_bool("TRUE").unwrap());
        assert!(parse_bool("on").unwrap());
        assert!(parse_bool("yes").unwrap());

        // 假字面
        assert!(!parse_bool("0").unwrap());
        assert!(!parse_bool("false").unwrap());
        assert!(!parse_bool("FALSE").unwrap());
        assert!(!parse_bool("off").unwrap());
        assert!(!parse_bool("no").unwrap());

        // 非法输入（含空串）
        assert!(parse_bool("").is_err());
        assert!(parse_bool("maybe").is_err());
        assert!(parse_bool("2").is_err());
        assert!(parse_bool("random").is_err());
    }

    #[test]
    fn test_env_parse_bool_not_set() {
        // 未设置时应返回 Ok(None)，与「已设置但非法」的 Err 严格区分
        assert_eq!(
            env_parse_bool("DEFINITELY_NOT_SET_VAR_12345").unwrap(),
            None
        );
    }

    /// truthy 与 falsey 字面集合必须两两互不重叠：避免 `parse_bool` 出现歧义分支。
    #[test]
    fn test_is_truthy_falsey_disjoint() {
        for v in ["1", "true", "on", "yes", "TRUE", "Yes"] {
            assert!(is_truthy(v), "{v} 应为 truthy");
            assert!(!is_falsey(v), "{v} 不应同时是 falsey");
        }
        for v in ["0", "false", "off", "no", "FALSE", "No"] {
            assert!(is_falsey(v), "{v} 应为 falsey");
            assert!(!is_truthy(v), "{v} 不应同时是 truthy");
        }
    }

    /// 错误消息文案是契约：必须包含原始输入和"Expected one of"提示。
    #[test]
    fn test_parse_bool_error_message_contract() {
        let err = parse_bool("nope").unwrap_err().to_string();
        assert!(err.contains("'nope'"), "错误消息应包含原始值：{err}");
        assert!(
            err.contains("Expected one of"),
            "错误消息应包含 Expected one of 提示：{err}"
        );
        assert!(
            err.contains("true/false") && err.contains("1/0"),
            "错误消息应列出 truthy/falsey 字面：{err}"
        );
    }

    /// 大小写混合也应正确解析。
    #[test]
    fn test_parse_bool_mixed_case_roundtrip() {
        assert!(parse_bool("TrUe").unwrap());
        assert!(!parse_bool("OfF").unwrap());
        assert!(parse_bool("Yes").unwrap());
        assert!(!parse_bool("nO").unwrap());
    }

    /// 设置环境变量后，宽松 / 严格 API 行为应一致。使用本测试专属变量名。
    #[test]
    fn test_env_apis_roundtrip() {
        let name = "PAGODA_CFG_TEST_ROUNDTRIP_VAR";
        unsafe { std::env::set_var(name, "yes") };
        assert!(env_is_truthy(name));
        assert!(!env_is_falsey(name));
        assert_eq!(env_parse_bool(name).unwrap(), Some(true));

        unsafe { std::env::set_var(name, "off") };
        assert!(!env_is_truthy(name));
        assert!(env_is_falsey(name));
        assert_eq!(env_parse_bool(name).unwrap(), Some(false));

        unsafe { std::env::remove_var(name) };
    }

    /// 已设置但非法 → 宽松 API 静默 false，严格 API 必须 Err。
    #[test]
    fn test_env_parse_bool_invalid_value_errors() {
        let name = "PAGODA_CFG_TEST_INVALID_VAR";
        unsafe { std::env::set_var(name, "garbage-not-a-bool") };
        assert!(!env_is_truthy(name));
        assert!(!env_is_falsey(name));
        assert!(env_parse_bool(name).is_err());
        unsafe { std::env::remove_var(name) };
    }

    /// env_parse_bool 应透传 parse_bool 的错误（保留原始值与提示文案）。
    #[test]
    fn test_env_parse_bool_invalid_passes_through_message() {
        let name = "PAGODA_CFG_TEST_PASSTHROUGH_VAR";
        unsafe { std::env::set_var(name, "perhaps") };
        let err = env_parse_bool(name).unwrap_err().to_string();
        assert!(err.contains("'perhaps'"), "应透传原始值：{err}");
        unsafe { std::env::remove_var(name) };
    }
}
