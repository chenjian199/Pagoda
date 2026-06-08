// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-License-Identifier: Apache-2.0
//
// API-compatible implementation based on the public interfaces and behavioral
// contracts of NVIDIA Dynamo (https://github.com/ai-dynamo/dynamo).
// Implementation rewritten by PAGODA.

//! # 性能模型（prefill / decode 时延预测）
//!
//! ## 设计意图
//! 为 mocker 提供 token 级时序预测。支持三种模型：
//! 1. `Polynomial`：内置多项式公式（默认，向后兼容）；
//! 2. `Interpolated`：基于 profiler 数据的网格插值（从 NPZ 文件加载）；
//! 3. `Aiconfigurator`：经 Python 回调直接调用 AIC SDK。
//!
//! ## 外部契约
//! - 枚举 [`PerfModel`] 的三个变体、各自字段类型，以及 [`PrefillInterpolator`] /
//!   [`DecodeInterpolator`] / [`AicCallback`] 三个 trait 的方法签名保持稳定。
//! - [`PerfModel::predict_prefill_time`] 与 [`PerfModel::predict_decode_time`] 的入参面、
//!   各变体所用公式与系数、`max` 下限（prefill 下限 0.0、decode 下限 1.0、`batch_size==0`
//!   时 decode 返回 0.0）必须与上游一致。
//! - [`PerfModel::from_npz`] 期望的 NPZ 数组名（`prefill_isl`、`prefill_ttft_ms`、
//!   `decode_active_kv_tokens`、`decode_context_length`、`decode_itl`）、维度校验与错误串保持一致。
//!
//! ## 实现要点
//! - prefill 用「整批新 token 数」`batch_size * (isl - prefix)`，建模 GPU 并行处理。
//! - decode 多项式以活跃 KV 占比为自变量；插值用 `(active_kv_tokens, context_length)` 两轴。
//! - 具体插值器类型用包装结构隐藏，对外只暴露 trait object。

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use ndarray::{Array1, Array2};
use ndarray_interp::InterpolateError;
use ndarray_interp::interp1d::{Interp1DBuilder, Linear};
use ndarray_interp::interp2d::{Bilinear, Interp2DBuilder};

// === SECTION: 插值与回调 trait ===

/// prefill 时延的一维插值抽象。
pub trait PrefillInterpolator: Send + Sync {
    fn interp(&self, x: f64) -> Result<f64, InterpolateError>;
}

/// decode 时延的二维插值抽象。
pub trait DecodeInterpolator: Send + Sync {
    fn interp(&self, x: f64, y: f64) -> Result<f64, InterpolateError>;
}

/// 直接调用 AIC SDK 的回调抽象。实现方经 PyO3 GIL 调用 Python AIC SDK。
pub trait AicCallback: Send + Sync {
    /// 预测 prefill 时延（毫秒）。参数：`(batch_size, effective_isl, prefix)`。
    fn predict_prefill(&self, batch_size: usize, effective_isl: usize, prefix: usize) -> f64;

    /// 预测 decode（生成）时延（毫秒）。参数：`(batch_size, isl, osl)`。
    fn predict_decode(&self, batch_size: usize, isl: usize, osl: usize) -> f64;
}

// === SECTION: 具体插值器包装 ===

/// 把具体的一维插值器适配为 [`PrefillInterpolator`]。
struct PrefillInterp1D {
    inner: ndarray_interp::interp1d::Interp1D<
        ndarray::OwnedRepr<f64>,
        ndarray::OwnedRepr<f64>,
        ndarray::Ix1,
        Linear,
    >,
}

impl PrefillInterpolator for PrefillInterp1D {
    fn interp(&self, x: f64) -> Result<f64, InterpolateError> {
        self.inner.interp_scalar(x)
    }
}

/// 把具体的二维插值器适配为 [`DecodeInterpolator`]。
struct DecodeInterp2D {
    inner: ndarray_interp::interp2d::Interp2D<
        ndarray::OwnedRepr<f64>,
        ndarray::OwnedRepr<f64>,
        ndarray::OwnedRepr<f64>,
        ndarray::Ix2,
        Bilinear,
    >,
}

impl DecodeInterpolator for DecodeInterp2D {
    fn interp(&self, x: f64, y: f64) -> Result<f64, InterpolateError> {
        self.inner.interp_scalar(x, y)
    }
}

// === SECTION: PerfModel 枚举 ===

/// 用于预测 prefill 与 decode 时序的性能模型。
#[derive(Default)]
pub enum PerfModel {
    /// 默认的多项式模型，使用内置公式。
    #[default]
    Polynomial,
    /// 基于 profiler 数据的插值模型。decode 两轴为 `(active_kv_tokens, context_length)`。
    Interpolated {
        prefill_interp: Arc<dyn PrefillInterpolator>,
        decode_interp: Arc<dyn DecodeInterpolator>,
    },
    /// 经 Python 回调调用 AI Configurator SDK。prefill 传入 `(batch_size, effective_isl, prefix)`。
    Aiconfigurator { callback: Arc<dyn AicCallback> },
}

impl Clone for PerfModel {
    fn clone(&self) -> Self {
        match self {
            PerfModel::Polynomial => PerfModel::Polynomial,
            PerfModel::Interpolated {
                prefill_interp,
                decode_interp,
            } => PerfModel::Interpolated {
                prefill_interp: Arc::clone(prefill_interp),
                decode_interp: Arc::clone(decode_interp),
            },
            PerfModel::Aiconfigurator { callback } => PerfModel::Aiconfigurator {
                callback: Arc::clone(callback),
            },
        }
    }
}

impl std::fmt::Debug for PerfModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PerfModel::Polynomial => write!(f, "PerfModel::Polynomial"),
            PerfModel::Interpolated { .. } => write!(f, "PerfModel::Interpolated {{ .. }}"),
            PerfModel::Aiconfigurator { .. } => write!(f, "PerfModel::Aiconfigurator"),
        }
    }
}

// === SECTION: 加载与构造 ===

impl PerfModel {
    /// 从 NPZ 文件加载插值性能模型。
    ///
    /// NPZ 中期望的数组：
    /// - `prefill_isl`：一维，输入序列长度；
    /// - `prefill_ttft_ms`：一维，首 token 时延（毫秒）；
    /// - `decode_active_kv_tokens`：一维，活跃 KV token 数；
    /// - `decode_context_length`：一维，上下文长度；
    /// - `decode_itl`：二维，token 间时延（毫秒）。
    pub fn from_npz(path: &Path) -> Result<Self> {
        use ndarray_npy::NpzReader;
        use std::fs::File;

        tracing::info!("Loading performance model from NPZ file: {:?}", path);

        let file =
            File::open(path).with_context(|| format!("Failed to open NPZ file: {:?}", path))?;
        let mut npz = NpzReader::new(file)
            .with_context(|| format!("Failed to create NPZ reader for: {:?}", path))?;

        // prefill 轴。
        let prefill_isl: Array1<f64> = npz
            .by_name("prefill_isl")
            .with_context(|| "Failed to load prefill_isl from NPZ")?;
        let prefill_ttft_ms: Array1<f64> = npz
            .by_name("prefill_ttft_ms")
            .with_context(|| "Failed to load prefill_ttft_ms from NPZ")?;

        // decode 轴与网格。
        let decode_active_kv_tokens: Array1<f64> = npz
            .by_name("decode_active_kv_tokens")
            .with_context(|| "Failed to load decode_active_kv_tokens from NPZ")?;
        let decode_context_length: Array1<f64> = npz
            .by_name("decode_context_length")
            .with_context(|| "Failed to load decode_context_length from NPZ")?;
        let decode_itl: Array2<f64> = npz
            .by_name("decode_itl")
            .with_context(|| "Failed to load decode_itl from NPZ")?;

        // 维度校验：prefill 两轴等长。
        if prefill_isl.len() != prefill_ttft_ms.len() {
            anyhow::bail!(
                "Prefill array length mismatch: isl={}, ttft={}",
                prefill_isl.len(),
                prefill_ttft_ms.len()
            );
        }
        // 维度校验：decode 网格与两轴匹配。
        if decode_itl.nrows() != decode_active_kv_tokens.len()
            || decode_itl.ncols() != decode_context_length.len()
        {
            anyhow::bail!(
                "Decode array dimension mismatch: itl shape=({}, {}), active_kv={}, context={}",
                decode_itl.nrows(),
                decode_itl.ncols(),
                decode_active_kv_tokens.len(),
                decode_context_length.len()
            );
        }

        tracing::info!(
            "Loaded performance model: prefill_points={}, decode_grid={}x{}",
            prefill_isl.len(),
            decode_itl.nrows(),
            decode_itl.ncols()
        );

        // 加载期一次性构建插值器（允许外推）。
        let prefill_inner = Interp1DBuilder::new(prefill_ttft_ms)
            .x(prefill_isl)
            .strategy(Linear::new().extrapolate(true))
            .build()
            .with_context(|| "Failed to build prefill interpolator")?;
        let decode_inner = Interp2DBuilder::new(decode_itl)
            .x(decode_active_kv_tokens)
            .y(decode_context_length)
            .strategy(Bilinear::new().extrapolate(true))
            .build()
            .with_context(|| "Failed to build decode interpolator")?;

        Ok(PerfModel::Interpolated {
            prefill_interp: Arc::new(PrefillInterp1D {
                inner: prefill_inner,
            }),
            decode_interp: Arc::new(DecodeInterp2D {
                inner: decode_inner,
            }),
        })
    }

    /// 由回调构造 Aiconfigurator 性能模型。
    pub fn from_aic_callback(callback: Arc<dyn AicCallback>) -> Self {
        PerfModel::Aiconfigurator { callback }
    }

    // === SECTION: 预测 ===

    /// 预测 prefill 时延（毫秒）。
    ///
    /// 调用方始终传入全部参数，各变体按需取用：
    /// - Polynomial / Interpolated：用整批新 token 数 `batch_size * (isl - prefix)`，
    ///   建模 GPU 并行处理；
    /// - Aiconfigurator：把 `(batch_size, isl - prefix, prefix)` 交给 AIC SDK。
    pub fn predict_prefill_time(&self, batch_size: usize, isl: usize, prefix: usize) -> f64 {
        let new_tokens_per_req = isl.saturating_sub(prefix);
        let time = match self {
            PerfModel::Polynomial => {
                let tokens = (batch_size * new_tokens_per_req) as f64;
                4.209989e-07 * tokens.powi(2) + 1.518344e-02 * tokens + 1.650142e+01
            }
            PerfModel::Interpolated { prefill_interp, .. } => {
                let tokens = (batch_size * new_tokens_per_req) as f64;
                prefill_interp.interp(tokens).unwrap_or(0.0)
            }
            PerfModel::Aiconfigurator { callback } => {
                callback.predict_prefill(batch_size, new_tokens_per_req, prefix)
            }
        };
        time.max(0.0)
    }

    /// 预测 decode 时延（毫秒）。
    ///
    /// 调用方始终传入全部参数，各变体按需取用：
    /// - Polynomial：以活跃 KV 占比 `(active_kv_tokens / total_kv_tokens)` 为自变量；
    /// - Interpolated：用 `(active_kv_tokens, context_length)`；
    /// - Aiconfigurator：用 `(batch_size, context_length)`。
    pub fn predict_decode_time(
        &self,
        batch_size: usize,
        active_kv_tokens: usize,
        context_length: usize,
        total_kv_tokens: usize,
    ) -> f64 {
        if batch_size == 0 {
            return 0.0;
        }
        let time = match self {
            PerfModel::Polynomial => {
                let active_perc = if total_kv_tokens > 0 {
                    active_kv_tokens as f64 / total_kv_tokens as f64
                } else {
                    tracing::warn!("Total KV tokens is 0, using 1.0 as capacity");
                    1.0
                };
                -25.74 * active_perc.powi(2) + 54.01 * active_perc + 5.74
            }
            PerfModel::Interpolated { decode_interp, .. } => decode_interp
                .interp(active_kv_tokens as f64, context_length as f64)
                .unwrap_or(0.0),
            PerfModel::Aiconfigurator { callback } => {
                callback.predict_decode(batch_size, context_length, 2)
            }
        };
        // 产出 token 的 decode 步不应塌缩到同一时间戳，故下限为 1.0 ms。
        let result = time.max(1.0);
        tracing::trace!(
            "Decode time prediction: batch_size={batch_size}, active_kv_tokens={active_kv_tokens}, context_length={context_length}, time={result:.2}ms"
        );
        result
    }
}
