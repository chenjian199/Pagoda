// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # `component::component` 子模块占位文件
//!
//! ## 设计意图
//!
//! 在早期版本中，`Component` 类型上曾经直接 `impl EventPublisher` /
//! `impl EventSubscriber`，把"组件可以发布/订阅事件"作为 `Component`
//! 本身的固有方法暴露给上层。这种做法把"组件命名 / 注册"与"事件平面"
//! 这两个原本应当解耦的关注点强绑在同一个类型上，会带来两个副作用：
//!
//! - 任何对事件协议的修改都必须穿过 `Component` 类型本体
//! - 上层若想单独使用 `Component`，会被迫拖入事件平面相关依赖
//!
//! 因此在迁移到统一事件平面（`transports::event_plane`）的过程中，这部
//! 分 `impl` 已经被移除。事件发布与订阅的能力改为通过
//! `EventPublisher::for_component(&component, topic)` 与
//! `EventSubscriber::for_component(&component, topic)` 这两个外部入口
//! 获得，组件类型本身保持只负责命名与注册职责。
//!
//! ## 本文件作用
//!
//! 本文件刻意保留为一个**只包含说明性注释的占位文件**，不引入任何符
//! 号、不声明任何类型或函数，原因有三：
//!
//! 1. **维持源码树结构稳定** —— 其他子模块（`namespace.rs`、
//!    `endpoint.rs`、`registry.rs`、`service.rs`、`client.rs`）都以
//!    "模块名 + `.rs`"的形式组织，删除该文件会令模块布局看起来不一致。
//! 2. **记录历史决策** —— 后续读者在浏览源码时能在最直观的位置看到
//!    "事件平面的 `impl` 为何不在 `Component` 上"的解释。
//! 3. **留出后续扩展点** —— 如果将来还需要在 `Component` 类型本体上
//!    挂载与发现 / 事件 / 配额等无关、仅服务于"组件"自身语义的小工
//!    具，可以继续在此文件中追加，而不必先创建新的模块层级。
//!
//! 如需查找事件相关的 trait 与 helper，请改阅：
//!
//! - `crate::transports::event_plane::EventPublisher`
//! - `crate::transports::event_plane::EventSubscriber`
//!
//! 它们提供了与组件解耦的统一接入方式。
