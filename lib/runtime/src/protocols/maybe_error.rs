// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `MaybeError`：可选携带错误的统一接口
//!
//! ## 设计意图
//!
//! 项目中“响应/事件/注解”这类容器经常需要在同一个类型里同时表达成功和错误，
//! 例如 [`Annotated`](super::annotated::Annotated)。为了避免每个容器都各自
//! 发明一套 `is_err()` / `into_result()` 风格的接口，本模块抽出
//! [`MaybeError`] trait，把“可不可能含错误”这个能力下沉到一个公共契约。
//!
//! ## 外部契约
//!
//! 实现者必须提供两个核心方法：
//! - [`MaybeError::from_err`]：从任意 `std::error::Error` 构造自身错误态；
//! - [`MaybeError::err`]：把当前状态投影为 [`PagodaError`] 的可选值。
//!
//! 在此之上提供两个默认方法 [`is_ok`](MaybeError::is_ok) /
//! [`is_err`](MaybeError::is_err)，**完全用 `err()` 表达**，保证默认实现
//! 与实现者重写的 `err()` 始终自洽（即不允许 `is_err()` 的判定与 `err()`
//! 不一致）。
//!
//! ## 实现要点
//!
//! - `from_err` 用 `impl std::error::Error + 'static` 接口，**不**通过泛型
//!   参数，避免调用方写一长串 turbofish；
//! - `is_ok` / `is_err` 互为反义，简单委托给 `err().is_none()` /
//!   `err().is_some()`，避免实现者两边各重写一遍走偏。

use crate::error::PagodaError;

// === TRAIT ==================================================================

/// 表示“可能携带错误”的容器统一接口。
///
/// 该 trait 让实现者同时承载成功值与错误信息，并通过 [`PagodaError`] 暴露
/// 结构化错误。详见模块级文档的契约说明。
///
/// # 示例
///
/// ```rust,ignore
/// use pagoda_runtime::protocols::maybe_error::MaybeError;
/// use pagoda_runtime::error::PagodaError;
///
/// struct MyResponse {
///     data: Option<String>,
///     error: Option<PagodaError>,
/// }
///
/// impl MaybeError for MyResponse {
///     fn from_err(err: impl std::error::Error + 'static) -> Self {
///         MyResponse {
///             data: None,
///             error: Some(PagodaError::from(
///                 Box::new(err) as Box<dyn std::error::Error + 'static>
///             )),
///         }
///     }
///
///     fn err(&self) -> Option<PagodaError> {
///         self.error.clone()
///     }
/// }
/// ```
pub trait MaybeError {
    /// 从任意 `std::error::Error` 构造一个“错误态”的容器实例。
    ///
    /// 推荐做法是先把 `err` 装箱成 `Box<dyn Error>` 再交给
    /// `PagodaError::from`，以兼容下游统一序列化路径。
    fn from_err(err: impl std::error::Error + 'static) -> Self;

    /// 投影当前实例的错误视图。
    ///
    /// - 成功态返回 `None`；
    /// - 错误态返回 `Some(PagodaError)`。
    fn err(&self) -> Option<PagodaError>;

    /// 是否表示成功状态。默认实现：`err().is_none()`。
    fn is_ok(&self) -> bool {
        self.err().is_none()
    }

    /// 是否表示错误状态。默认实现：`err().is_some()`。
    fn is_err(&self) -> bool {
        self.err().is_some()
    }
}

// === 单元测试 ================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试用容器：只保存一个可选错误。
    struct TestError {
        error: Option<PagodaError>,
    }

    impl MaybeError for TestError {
        fn from_err(err: impl std::error::Error + 'static) -> Self {
            let boxed: Box<dyn std::error::Error + 'static> = Box::new(err);
            TestError {
                error: Some(PagodaError::from(boxed)),
            }
        }

        fn err(&self) -> Option<PagodaError> {
            self.error.clone()
        }
    }

    /// ## 测试过程
    /// 用 `PagodaError::msg` 构造一个错误，再借助 `from_err` 放入容器，
    /// 断言 `err()` 文本、`is_ok()` / `is_err()` 行为符合契约。
    /// ## 意义
    /// 锁定默认方法与 `err()` 的等价性。
    #[test]
    fn test_maybe_error_default_implementations() {
        let pagoda_err = PagodaError::msg("Test error");
        let err = TestError::from_err(pagoda_err);
        assert!(err.err().unwrap().to_string().contains("Test error"));
        assert!(!err.is_ok());
        assert!(err.is_err());
    }

    /// ## 测试过程
    /// 用 `std::io::Error::other` 构造非 PagodaError 错误，验证 `from_err`
    /// 能透传文本。
    /// ## 意义
    /// 确保任何 `std::error::Error` 都能无损接入。
    #[test]
    fn test_from_std_error() {
        let std_err = std::io::Error::other("io failure");
        let test_err = TestError::from_err(std_err);

        assert!(test_err.is_err());
        assert!(test_err.err().unwrap().to_string().contains("io failure"));
    }

    /// ## 测试过程
    /// 直接构造一个 `error: None` 的容器，验证三个查询接口在“无错误”
    /// 状态下的返回值。
    /// ## 意义
    /// 验证成功态分支，避免默认方法出现“错配”。
    #[test]
    fn test_not_error() {
        let test = TestError { error: None };
        assert!(test.is_ok());
        assert!(!test.is_err());
        assert!(test.err().is_none());
    }

    /// ## 测试过程
    /// 同时构造成功容器与失败容器，逐一比对 `is_ok` / `is_err` 是否始终
    /// 与 `err().is_some()` 互补。
    /// ## 意义
    /// 守护“默认方法 = `err()` 的语法糖”这一不变量。
    #[test]
    fn test_default_helpers_follow_err_contract() {
        let success = TestError { error: None };
        let failure = TestError {
            error: Some(PagodaError::msg("boom")),
        };

        assert!(success.is_ok());
        assert!(!success.is_err());
        assert!(!failure.is_ok());
        assert!(failure.is_err());
    }
}
