// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA. All rights reserved.
// SPDX-FileCopyrightText: Copyright (c) 2022 Himanshu Neema
// SPDX-License-Identifier: Apache-2.0 AND MIT
//
// Based on async-openai (https://github.com/64bit/async-openai) by Himanshu Neema.
// Original async-openai Copyright (c) 2022 Himanshu Neema.
// async-openai portions licensed under the MIT License; see ATTRIBUTIONS-Rust.md.
//
// Pagoda modifications Copyright (c) 2026-2028 PAGODA.
// Pagoda modifications licensed under the Apache License, Version 2.0.

//! 兼容 OpenAI 的推理 API 协议类型定义。
//!
//! ## 设计意图
//! 在上游 `async-openai` 之上提供一层声明式协议类型门面：默认重新导出上游，仅对需要
//! 放宽输入校验或扩展字段的最小类型子树进行本地自有，从而既复用上游又不被其合并节奏阻塞。
//!
//! ## 外部契约
//! 本 crate 为多种推理 API 协议提供类型：
//! - **OpenAI Chat Completions 与 Completions**（上游 `async-openai` 重新导出 + 扩展）
//! - **OpenAI Responses API**（上游 `async-openai` 重新导出）
//! - **Anthropic Messages API**（完全自定义）
//!
//! ## 实现要点
//! 推理服务扩展（推理内容、停止原因、多模态）在本地定义并文档化；
//! 业务逻辑与 HTTP 传输不属于本 crate 职责。
#![allow(deprecated)]
#![allow(warnings)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod error;
pub mod types;
