// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Codec implementations for network framing.

pub mod two_part;
pub mod zero_copy_decoder;

pub use two_part::TwoPartCodec;
pub use zero_copy_decoder::ZeroCopyTcpDecoder;
