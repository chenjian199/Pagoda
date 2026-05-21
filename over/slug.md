# `slug` 模块设计文档

**源码位置**：`lib/runtime/src/slug.rs`（143 行）

---

## 一、设计背景

Pagoda 使用 NATS 作为传输层，NATS 的 subject（主题）类似 `pagoda_backend.generate-694d988806b92e39`，其命名规则要求只包含字母、数字、点（`.`）、连字符（`-`）和下划线（`_`）。同样，Kubernetes 资源名称、HTTP URL path segment 也有类似约束。

若代码直接使用任意用户输入的字符串作为 ServiceGroup 名、命名空间名或 PortName 名，特殊字符（空格、斜杠、Unicode 字符等）会破坏 NATS subject 的解析、HTTP URL 路由，或产生 NATS 无法订阅的 subject。

`slug` 模块定义了 `Slug` 类型，在**创建时**就强制验证或转换字符串的合法性，使后续代码可以安全地将 `Slug` 用于任何需要安全标识符的场景，无需在每次使用时重复校验。

---

## 二、合法字符集与设计选择

`Slug` 的合法字符集是：小写字母 `a-z`、数字 `0-9`、连字符 `-`、下划线 `_`。

注意**不包括大写字母**：`slugify` 会将所有大写字母转为小写。这是一个刻意的选择——NATS subject 和 Kubernetes 资源名称区分大小写，但大小写混合的标识符在分布式系统中容易产生"明明看起来一样"的混淆问题（如 `MyServiceGroup` 和 `myservicegroup` 被当作不同的服务）。统一小写消除了这类歧义。

---

## 三、`Slug` 结构体

```rust
#[derive(Serialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct Slug(String);

impl Slug {
    fn new(s: String) -> Slug
    // 私有构造辅助：去除前导 `REPLACEMENT_CHAR`，再封装成 `Slug`

    pub fn from_string(s: impl AsRef<str>) -> Slug
    // 对外便捷入口，内部直接委托给 `Slug::slugify`

    pub fn slugify(s: &str) -> Slug
    // 宽松模式：保留 `[a-z0-9_-]`，其余字符替换为 `REPLACEMENT_CHAR`

    pub fn slugify_unique(s: &str) -> Slug
    // 唯一化模式：仅保留 `[a-z0-9_]`，并追加原始字符串的 blake3 哈希后缀
}

```

**为什么是 newtype 而非 type alias**：`type Slug = String` 只是别名，不会阻止将普通 `String` 传入需要 `Slug` 的函数。`Slug(String)` 是独立类型，编译器在类型层面阻止未经验证的字符串直接使用——只有经过 `slugify`、`from_string` 或通过 `TryFrom` 校验的字符串才能成为 `Slug`。这将运行时错误转化为编译期类型错误。

`Default` 实现返回空字符串 `Slug("")`，用于数据结构初始化场景（如 `#[derive(Default)]` 的父结构体）。

---

## 四、三种创建方式与它们的权衡

### `slugify(s)` — 宽松创建，自动转换

```rust
pub fn slugify(s: &str) -> Slug {
    let out = s.to_lowercase().chars().map(|c| {
        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' {
            c
        } else {
            '_'  // REPLACEMENT_CHAR
        }
    }).collect::<String>();
    Slug::new(out)  // 去除开头的 '_'
}
```

非法字符替换为 `_`（`REPLACEMENT_CHAR`）。`Slug::new` 去除开头的 `_`，因为以下划线开头的标识符在某些命名约定中有特殊含义（如 "内部/私有"），用于公开的 ServiceGroup 名容易引起混淆。

`slugify` 用于代码知道字符串可能包含非法字符但希望静默处理（不报错）的场景，例如将用户输入的模型名称转化为 NATS subject 的一部分。

### `slugify_unique(s)` — 带哈希后缀的唯一化

```rust
pub fn slugify_unique(s: &str) -> Slug {
    let out = s.to_lowercase().chars().map(|c| {
        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' { c } else { '_' }
    }).collect::<String>();
    let hash = blake3::hash(s.as_bytes()).to_string();
    let out = format!("{out}_{}", &hash[(hash.len() - 8)..]);
    Slug::new(out)
}
```

**为什么需要哈希后缀**：不同的字符串可能 slugify 为相同结果，例如 `"My Model"` 和 `"my_model"` 都变成 `my_model`，若两个不同的 Worker 使用这两个名称注册，会产生冲突。`slugify_unique` 在末尾追加 8 字符的 blake3 哈希（原始字符串的哈希，非 slugified 结果的哈希），确保原始字符串不同则后缀不同。

**为什么是 blake3**：blake3 是目前性能最高的密码哈希算法之一，且已是 Pagoda 的依赖。8 字符（32 bit）的哈希后缀在正常使用场景（不超过几百个同名 ServiceGroup）中碰撞概率极低（约 1/4,000,000,000）。

注意 `slugify_unique` 的合法字符集中**不包含 `-`**（`slugify` 包含），因为后缀使用 `_` 连接，排除 `-` 使 slug 结构更清晰（人类阅读时能识别 `_xxxx` 是哈希后缀）。

### `TryFrom<&str>` / `TryFrom<String>` — 严格创建，失败时报错

```rust
impl TryFrom<&str> for Slug {
    type Error = InvalidSlugError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.to_string().try_into()
    }
}

impl TryFrom<String> for Slug {
    type Error = InvalidSlugError;
    fn try_from(s: String) -> Result<Self, Self::Error>
    // 查找第一个非法字符；全部合法则返回 `Ok(Slug(s))`，否则返回 `Err(InvalidSlugError(c))`
}
```

当代码确信字符串已是合法 slug（如从配置文件读取的值），但需要转换为 `Slug` 类型时使用。`TryFrom<&str>` 本身不重复实现校验逻辑，而是先转成 `String` 再委托给 `TryFrom<String>`；真正的验证路径会扫描字符串，寻找第一个不属于 `[a-z0-9_-]` 的字符。非法字符不会静默替换，而是立即返回包含该字符的 `InvalidSlugError`，便于调试时快速定位配置或输入中的问题字符。

---

## 五、`InvalidSlugError` — 非法字符错误类型

```rust
#[derive(Debug)]
pub struct InvalidSlugError(char);

impl fmt::Display for InvalidSlugError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
    // 输出明确错误消息：指出具体非法字符，并说明允许字符集仅为 `a-z`、`0-9`、`-`、`_`
}

impl std::error::Error for InvalidSlugError {}
```

`InvalidSlugError` 只保存一个字符，而不是整个原始字符串，这是一个有意的设计：调用方通常最需要知道的是“哪一个字符不合法”，而不是把整段原文再拷贝一遍。其 `Display` 实现会生成面向用户和配置作者都能直接理解的错误文本，因此它既能被 `TryFrom` 直接返回，也能被 `Deserialize` 通过 `de::Error::custom` 原样包装进反序列化错误链。

---

## 六、`Deserialize` 的自定义实现

```rust
impl<'de> Deserialize<'de> for Slug {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> { ... }
}
```

Pagoda 的配置文件（TOML、JSON）和 NATS 消息可能包含 `Slug` 字段。若使用 `#[derive(Deserialize)]`，serde 会直接将字符串放入 `Slug(s)` 而跳过验证。

自定义 `Deserializer` 实现了 `Visitor`，在 `visit_str` 和 `visit_string` 中调用 `Slug::try_from`，将验证错误转化为 serde 的反序列化错误（`de::Error::custom`）。这样当配置文件中的 `slug` 字段包含非法字符时，反序列化直接失败，产生带有明确错误信息的 `DeserializeError`，而非在后续使用时才出错。

---

## 七、`Display` / `AsRef<str>` / `PartialEq<str>` — 输出、借用与便捷比较

```rust
impl fmt::Display for Slug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Slug {
    fn as_ref(&self) -> &str { &self.0 }
}

impl PartialEq<str> for Slug {
    fn eq(&self, other: &str) -> bool { self.0 == other }
}
```

`Display` 让 `Slug` 在日志、错误信息、格式化输出中表现得像普通字符串，调用方可以直接使用 `{}` 打印，而不必手动访问内部字段。`AsRef<str>` 则使 `Slug` 可以零成本传入接受 `&str` 的函数（如 `nats_client.publish(subject, ...)`），无需显式提取内部 `String`。`PartialEq<str>` 进一步补上了常见的人机工学能力：调用方可以直接写 `slug == "generate"` 做比较，而不必先调用 `.as_ref()` 或构造临时 `String`。
