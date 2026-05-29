// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! URL / NATS 友好的 Slug 字符串类型
//!
//! ## 设计意图
//! 在 NATS subject、URL path 等场景下，只有有限的字符集是安全的。`Slug`
//! 用强类型把"已规范化、字符集合法"的字符串与普通 `String` 区分开，避免
//! 在跨组件传递时反复做字符校验。同时提供两种构造路径：
//! * `slugify` —— 宽松转换：把所有非法字符替换为占位符，保留连字符；
//! * `slugify_unique` —— 在 `slugify` 基础上附加 8 字符 blake3 短哈希，
//!   用于区分大小写不同但小写化后冲突的输入。
//!
//! ## 外部契约
//! - 公开类型：`Slug`（`Clone + Debug + Eq + PartialEq + Default + Serialize + Deserialize`）；
//!   公开错误：`InvalidSlugError`（实现 `std::error::Error + Display`）。
//! - `Slug` 的公开方法 `from_string` / `slugify` / `slugify_unique` 的签名、
//!   返回值以及对外字符集语义不变；`Display`、`AsRef<str>`、`PartialEq<str>`
//!   的行为不变。
//! - 公开 `TryFrom<&str>` / `TryFrom<String>`：仅接受 `[a-z0-9-_]` 字符集，
//!   遇到首个非法字符返回 `InvalidSlugError(char)`。
//! - serde 反序列化必须支持 `&str` 与 `String` 两条路径，并把内部 `TryFrom`
//!   校验失败的 `InvalidSlugError` 映射为 `de::Error::custom`。
//! - 规范化规则：所有 `slugify*` 路径都会先小写化、再过滤字符、最后
//!   `trim_start_matches('_')`；`slugify_unique` 的合法字符集**不包含** `-`。
//!
//! ## 实现要点
//! - 字符过滤改用 `flat_map`，以单一管道把"分类 + 替换"组合为函数式流水线，
//!   避免在 `map` 闭包里嵌套 `if/else`；行为与原版逐字符 `map` 等价。
//! - 抽出私有助手 `short_hash`：集中 blake3 → 取末 8 字符的子串逻辑，
//!   `slugify_unique` 直接复用，降低字符串切片下标计算的重复出现。
//! - 公共构造路径仍走 `Self::new`，把"剥离前导占位符"作为唯一收尾步骤，
//!   保证所有入口的最终形态严格一致。

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;

/// 用于替换所有非法字符的占位符（同时也是被剥离的前导字符）。
const REPLACEMENT_CHAR: char = '_';

// === SECTION: 类型定义 ===

/// URL / NATS 友好的字符串类型；只允许 `a-z`、`0-9`、`-`、`_`。
#[derive(Serialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct Slug(String);

// === SECTION: 私有助手 ===

/// 判定字符是否属于"标准 slug 字符集"：小写字母、数字、`-`、`_`。
#[inline]
fn is_slug_char(character: char) -> bool {
    character.is_ascii_lowercase()
        || character.is_ascii_digit()
        || character == '-'
        || character == '_'
}

/// 判定字符是否属于"唯一 slug 字符集"：小写字母、数字、`_`（不含 `-`）。
#[inline]
fn is_unique_slug_char(character: char) -> bool {
    character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
}

/// 用给定的字符集判定函数对小写化后的字符流做"保留或替换"过滤。
///
/// 行为：对每个字符——若 `keep(c)` 为真则保留 `c`，否则替换为 [`REPLACEMENT_CHAR`]。
/// 实现使用 `flat_map` 作为单一函数式管道，避免显式 `if/else` 嵌套。
fn sanitize_with<F>(source: &str, keep: F) -> String
where
    F: Fn(char) -> bool,
{
    source
        .to_lowercase()
        .chars()
        .flat_map(|character| {
            let replacement = if keep(character) {
                character
            } else {
                REPLACEMENT_CHAR
            };
            std::iter::once(replacement)
        })
        .collect()
}

/// 计算输入字符串的 blake3 哈希，并返回十六进制表示的末 8 个字符。
///
/// 仅作为 `slugify_unique` 的内部消歧后缀使用，与历史实现行为等价
/// （`hash.to_string()[(len-8)..]`）。
fn short_hash(source: &str) -> String {
    let hash = blake3::hash(source.as_bytes()).to_string();
    let suffix_start = hash.len() - 8;
    hash[suffix_start..].to_string()
}

// === SECTION: 构造方法 ===

impl Slug {
    /// 内部构造：剥离前导占位字符后包装为 `Slug`。
    ///
    /// 中文说明：
    /// 1. 通过 `trim_start_matches` 去掉前导的 `_`，避免 slug 以占位字符开头。
    /// 2. 把处理后的字符串包装成 `Slug` 返回。
    fn new(s: String) -> Slug {
        let trimmed = s.trim_start_matches(REPLACEMENT_CHAR);
        let normalized = trimmed.to_string();

        Self(normalized)
    }

    /// 由任意可借用为字符串的输入构造 `Slug`，等价于调用 [`Slug::slugify`]。
    ///
    /// 中文说明：
    /// 1. 借出底层字符串切片。
    /// 2. 委托给 `slugify` 完成合法化与规范化。
    pub fn from_string(s: impl AsRef<str>) -> Slug {
        let source = s.as_ref();
        let slug = Self::slugify(source);

        slug
    }

    /// 把字符串转换为合法 slug：非 `[a-z0-9-_]` 字符一律替换为 `_`。
    ///
    /// 中文说明：
    /// 1. 用 `sanitize_with` 配合 `is_slug_char` 做小写化 + 字符过滤的函数式流水线。
    /// 2. 调用 `Self::new` 去掉前导占位字符并返回最终 slug。
    pub fn slugify(s: &str) -> Slug {
        let sanitized = sanitize_with(s, is_slug_char);

        Self::new(sanitized)
    }

    /// 与 [`Slug::slugify`] 类似，但额外追加 4 字节（8 个十六进制字符）的短哈希后缀，
    /// 用于在大小写差异导致冲突时仍能区分原始输入。
    ///
    /// 中文说明：
    /// 1. 用 `sanitize_with` + `is_unique_slug_char`（**不**含 `-`）做字符过滤。
    /// 2. 通过 `short_hash` 取 blake3 摘要末 8 字符作为后缀。
    /// 3. `format!` 拼接 `主体_后缀`，交由 `Self::new` 完成统一收尾。
    pub fn slugify_unique(s: &str) -> Slug {
        let sanitized = sanitize_with(s, is_unique_slug_char);
        let suffix = short_hash(s);
        let unique_slug = format!("{sanitized}_{suffix}");

        Self::new(unique_slug)
    }
}

// === SECTION: 显示 / 借用 / 相等 ===

impl fmt::Display for Slug {
    /// 中文说明：直接把内部字符串写入 formatter，供日志 / 拼接 / `to_string` 复用。
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let value = &self.0;
        f.write_str(value)
    }
}

impl AsRef<str> for Slug {
    /// 中文说明：返回内部字符串的只读引用，不做复制。
    fn as_ref(&self) -> &str {
        let value = &self.0;
        value
    }
}

impl PartialEq<str> for Slug {
    /// 中文说明：取出内部字符串视图并与目标 `&str` 做逐字节比较。
    fn eq(&self, other: &str) -> bool {
        let value = self.0.as_str();
        value == other
    }
}

// === SECTION: 错误类型 ===

/// 当字符串包含非法字符而无法直接转换为 [`Slug`] 时返回的错误。
#[derive(Debug)]
pub struct InvalidSlugError(char);

impl fmt::Display for InvalidSlugError {
    /// 中文说明：携带首个非法字符与允许字符范围说明，便于上层定位非法输入。
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let invalid_char = self.0;

        write!(
            f,
            "Invalid char '{}'. String can only contain a-z, 0-9, - and _.",
            invalid_char
        )
    }
}

impl std::error::Error for InvalidSlugError {}

// === SECTION: TryFrom 校验入口 ===

impl TryFrom<&str> for Slug {
    type Error = InvalidSlugError;

    /// 中文说明：把借用字符串复制为 `String` 后委托给 `TryFrom<String>`。
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let owned = s.to_string();
        let slug = owned.try_into();

        slug
    }
}

impl TryFrom<String> for Slug {
    type Error = InvalidSlugError;

    /// 中文说明：
    /// 1. 用 `is_slug_char` 找出首个非法字符。
    /// 2. 命中则返回 `InvalidSlugError`；否则把原 `String` 原样包装为 `Slug`。
    fn try_from(s: String) -> Result<Self, Self::Error> {
        let invalid_char = s.chars().find(|character| !is_slug_char(*character));

        if let Some(character) = invalid_char {
            return Err(InvalidSlugError(character));
        }

        Ok(Self(s))
    }
}

// === SECTION: serde 反序列化 ===

impl<'de> Deserialize<'de> for Slug {
    /// 中文说明：通过专用 `SlugVisitor` 适配 `&str` / `String` 两种反序列化路径。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SlugVisitor;

        impl Visitor<'_> for SlugVisitor {
            type Value = Slug;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                let message =
                    "a valid slug string containing only characters a-z, 0-9, - and _.";

                formatter.write_str(message)
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let slug = Slug::try_from(v);

                slug.map_err(de::Error::custom)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let slug = Slug::try_from(v.as_ref());

                slug.map_err(de::Error::custom)
            }
        }

        let visitor = SlugVisitor;
        deserializer.deserialize_string(visitor)
    }
}

// === SECTION: 单元测试 ===

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 覆盖 `Slug` 的全部对外可观察行为：内部 `new` 的前导剥离、`from_string` /
    //! `slugify` / `slugify_unique` 的规范化与短哈希拼接、`Display` /
    //! `AsRef<str>` / `PartialEq<str>` 的等价性、`TryFrom` 校验与
    //! `InvalidSlugError::Display`、serde 在 `&str` / `String` / JSON 三条路径
    //! 上的反序列化，以及 `Default` + 序列化往返。
    //!
    //! ## 意义
    //! 这些断言把"宽松规范化（保留 `-`）"与"唯一规范化（不含 `-`，附加 8 字符
    //! blake3 后缀）"两条契约钉死。本次重构把字符过滤改为 `flat_map` 函数式
    //! 流水线、并抽出 `short_hash` 私有助手；任何一条断言失败都意味着
    //! 对外可观察行为发生漂移。

    use super::*;
    use serde::Deserialize as _;
    use serde::de::value::{Error as ValueError, StrDeserializer, StringDeserializer};

    #[test]
    fn test_new_trims_only_leading_replacement_chars() {
        let slug = Slug::new("___hello_world__".to_string());
        assert_eq!(slug.as_ref(), "hello_world__");

        let already_clean = Slug::new("hello_world".to_string());
        assert_eq!(already_clean.as_ref(), "hello_world");

        let only_replacements = Slug::new("____".to_string());
        assert_eq!(only_replacements.as_ref(), "");
    }

    #[test]
    fn test_from_string_slugify_display_as_ref_and_partial_eq() {
        let slug = Slug::from_string("__Hello World-42!");

        assert_eq!(slug.as_ref(), "hello_world-42_");
        assert_eq!(slug.to_string(), "hello_world-42_");
        assert!(<Slug as PartialEq<str>>::eq(&slug, "hello_world-42_"));
        assert!(!<Slug as PartialEq<str>>::eq(&slug, "different"));

        let slugified = Slug::slugify("A-b_C 9!");
        assert_eq!(slugified.as_ref(), "a-b_c_9_");

        let leading_underscore = Slug::slugify("___valid_name");
        assert_eq!(leading_underscore.as_ref(), "valid_name");
    }

    #[test]
    fn test_slugify_unique_is_deterministic_and_disambiguates() {
        let input = "Hello-World";
        let hash = blake3::hash(input.as_bytes()).to_string();
        let expected = format!("hello_world_{}", &hash[(hash.len() - 8)..]);

        let slug1 = Slug::slugify_unique(input);
        let slug2 = Slug::slugify_unique(input);
        assert_eq!(slug1.as_ref(), expected);
        assert_eq!(slug1, slug2);

        let case1 = Slug::slugify_unique("Hello");
        let case2 = Slug::slugify_unique("HELLO");
        assert_ne!(case1, case2);
        assert!(case1.as_ref().starts_with("hello_"));
        assert!(case2.as_ref().starts_with("hello_"));

        let trimmed = Slug::slugify_unique("-hello");
        assert!(trimmed.as_ref().starts_with("hello_"));
        assert!(!trimmed.as_ref().starts_with('_'));
    }

    #[test]
    fn test_try_from_variants_and_invalid_slug_error_display() {
        let from_str = Slug::try_from("abc-123_slug").unwrap();
        let from_string = Slug::try_from("abc-123_slug".to_string()).unwrap();
        assert_eq!(from_str, from_string);
        assert_eq!(from_str.as_ref(), "abc-123_slug");

        let uppercase_err = Slug::try_from("Abc").unwrap_err();
        assert_eq!(
            uppercase_err.to_string(),
            "Invalid char 'A'. String can only contain a-z, 0-9, - and _."
        );

        let punctuation_err = Slug::try_from("abc!def".to_string()).unwrap_err();
        assert_eq!(
            punctuation_err.to_string(),
            "Invalid char '!'. String can only contain a-z, 0-9, - and _."
        );

        let manual_err = InvalidSlugError('!');
        assert_eq!(
            manual_err.to_string(),
            "Invalid char '!'. String can only contain a-z, 0-9, - and _."
        );
    }

    #[test]
    fn test_deserialize_from_str_and_string_deserializers() {
        let from_borrowed = Slug::deserialize(StrDeserializer::<ValueError>::new("abc-123"))
            .unwrap();
        assert_eq!(from_borrowed.as_ref(), "abc-123");

        let from_owned = Slug::deserialize(StringDeserializer::<ValueError>::new(
            "def_456".to_string(),
        ))
        .unwrap();
        assert_eq!(from_owned.as_ref(), "def_456");

        let from_json: Slug = serde_json::from_str("\"ghi-789\"").unwrap();
        assert_eq!(from_json.as_ref(), "ghi-789");
    }

    #[test]
    fn test_deserialize_errors_for_invalid_and_non_string_inputs() {
        let invalid_err = serde_json::from_str::<Slug>("\"Bad-Slug\"")
            .unwrap_err()
            .to_string();
        assert!(invalid_err.contains("Invalid char 'B'"));

        let type_err = serde_json::from_str::<Slug>("123").unwrap_err().to_string();
        assert!(type_err.contains("a valid slug string containing only characters a-z, 0-9, - and _."));
    }

    #[test]
    fn test_default_and_serialize_round_trip() {
        let default_slug = Slug::default();
        assert_eq!(default_slug.as_ref(), "");

        let serialized = serde_json::to_string(&default_slug).unwrap();
        assert_eq!(serialized, "\"\"");

        let round_tripped: Slug = serde_json::from_str(&serialized).unwrap();
        assert_eq!(round_tripped, default_slug);
    }
}
