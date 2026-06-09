// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # 工具调用响应类型
//!
//! ## 设计意图
//! 定义工具调用解析结果的公共数据结构，作为各方言解析器的统一输出载体。
//!
//! ## 外部契约
//! - [`ToolCallType`]：序列化为 snake_case，目前仅含 `Function` 变体。
//! - [`CalledFunction`]：字段 `name`、`arguments` 均为 `String`，可序列化/反序列化。
//! - [`ToolCallResponse`]：字段 `id`、`tp`（序列化键名为 `type`）、`function`。

// 注：以下 pyo3 派生属性在上游用于可选的 Python 绑定，当前 crate 未启用该特性，故保留为注释占位。
// #[cfg_attr(feature = "pyo3_macros", pyo3::pyclass(eq, eq_int))]
// #[cfg_attr(feature = "pyo3_macros", pyo3(get_all))]
#[derive(Clone, Debug, serde::Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallType {
    Function,
}

// #[cfg_attr(feature = "pyo3_macros", pyo3::pyclass)]
// #[cfg_attr(feature = "pyo3_macros", pyo3(get_all))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CalledFunction {
    pub name: String,
    pub arguments: String,
}

// #[cfg_attr(feature = "pyo3_macros", pyo3::pyclass)]
// #[cfg_attr(feature = "pyo3_macros", pyo3(get_all))]
#[derive(Clone, Debug, serde::Serialize)]
pub struct ToolCallResponse {
    pub id: String,
    #[serde(rename = "type")]
    pub tp: ToolCallType,
    pub function: CalledFunction,
}
