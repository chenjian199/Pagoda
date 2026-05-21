// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Slug 生成工具：生成 URL/文件名安全的短标识符。

/// 将字符串转换为 URL/文件名安全的 slug。
///
/// 替换规则：非 `[a-z0-9]` 字符替换为 `-`，连续 `-` 合并，首尾 `-` 去除。
pub fn slugify(input: &str) -> String {
    let lowered = input.to_lowercase();
    let mut result = String::with_capacity(lowered.len());
    let mut prev_dash = false;

    for ch in lowered.chars() {
        if ch.is_ascii_alphanumeric() {
            result.push(ch);
            prev_dash = false;
        } else if !prev_dash {
            result.push('-');
            prev_dash = true;
        }
    }

    result.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slugify_basic() {
        assert_eq!(slugify("Hello World"), "hello-world");
        assert_eq!(slugify("llm.worker.generate"), "llm-worker-generate");
        assert_eq!(slugify("---abc---"), "abc");
    }
}
