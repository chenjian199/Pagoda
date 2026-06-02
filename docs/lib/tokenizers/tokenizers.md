# Tokenizers 模块设计文档

## 1.模块要解决的问题

`tokenizers` 模块位于 Pagoda 推理链路的文本边界，负责把用户输入文本转换为模型可处理的 token ids，并把模型生成的 token ids 还原为可输出文本。

它解决的是这条路径：

```text
用户文本
  ↓ encode
token ids
  ↓ 模型推理
生成 token ids
  ↓ decode / stream decode
输出文本
```

当前实现关注三个核心问题：

- 不同 tokenizer 后端如何被统一调用；
- 普通解码和流式解码如何正确处理 token 边界；
- 生成过程中如何检测 stop token 和 stop sequence。

本模块不负责模型推理、KV cache 管理、Router 选路或 token block hashing。

---

## 2.设计核心：用统一接口隔离后端差异

不同模型可能使用不同 tokenizer 文件，不同 tokenizer 后端的调用方式也不同。当前实现通过统一 trait 和 wrapper 把这些差异隔离起来。

整体结构是：

```text
HuggingFaceTokenizer
TikTokenTokenizer
FastTokenizer
  ↓
Encoder / Decoder / Tokenizer trait
  ↓
Tokenizer wrapper
  ↓
Runtime / Worker / 生成逻辑统一调用
```

当前实现的 tokenizer 后端包括：

| 后端 | 用途 |
| --- | --- |
| `HuggingFaceTokenizer` | 处理 HuggingFace `tokenizer.json` |
| `TikTokenTokenizer` | 处理 `.model` / `.tiktoken` BPE 文件 |
| `FastTokenizer` | 使用 fastokens 加速编码，使用 HuggingFace 解码 |

其中，具体 tokenizer 实例由加载的 tokenizer 文件决定。

---

## 3.统一数据表示

### 3.1 TokenIdType

模块统一使用 `u32` 表示 token id：

```rust
pub type TokenIdType = u32;
```

所有后端最终都需要把编码结果转换为这一统一类型。

### 3.2 TokenizerType

当前实现中存在 tokenizer 类型枚举：

```rust
pub enum TokenizerType {
    HuggingFace(String),
    TikToken(String),
}
```

它用于表示 tokenizer 类型及其对应路径或标识字符串。

### 3.3 Encoding

不同后端返回的原始 encoding 类型不同，因此模块使用 `Encoding` 统一承载结果：

```rust
pub enum Encoding {
    Hf(Box<tokenizers::tokenizer::Encoding>),
    Sp(Vec<TokenIdType>),
}
```

其中：

| 变体 | 含义 |
| --- | --- |
| `Hf` | HuggingFace tokenizer 的原始 encoding |
| `Sp` | 普通 token id 向量，主要用于 TikToken / FastTokenizer |

上层通过统一方法获取 token ids：

```rust
pub fn token_ids(&self) -> &[u32]
```

`Encoding` 的 hash 也基于 token id 序列计算，而不是基于后端内部结构。

---

## 4.统一行为抽象

当前实现使用三个 trait 定义 tokenizer 能力。

### 4.1 Encoder：文本到 token ids

```rust
pub trait Encoder: Send + Sync {
    fn encode(&self, input: &str) -> Result<Encoding>;
    fn encode_batch(&self, inputs: &[&str]) -> Result<Vec<Encoding>>;
}
```

`Encoder` 负责单条文本和批量文本编码。

### 4.2 Decoder：token ids 到文本

```rust
pub trait Decoder: Send + Sync {
    fn decode(
        &self,
        token_ids: &[TokenIdType],
        skip_special_tokens: bool,
    ) -> Result<DecodeResult>;
}
```

`Decoder` 负责把 token ids 还原为文本，并通过 `DecodeResult` 表达解码是否完整。

### 4.3 Tokenizer：同时具备编码和解码

```rust
pub trait Tokenizer: Encoder + Decoder {}
```

实现该 trait 的后端都可以被统一包装和调用。

---

## 5.Tokenizer Wrapper：统一运行入口

模块提供统一包装类型：

```rust
pub struct Tokenizer(Arc<dyn traits::Tokenizer>);
```

它的作用是：

- 持有任意实现了 `Tokenizer` trait 的后端；
- 通过 `Arc` 支持共享；
- 屏蔽具体后端类型；
- 作为上层模块的统一调用入口。

主要接口：

```rust
pub fn from_file(file_path: &str) -> Result<Tokenizer>;

pub fn decode_stream(
    &self,
    prompt_token_ids: &[TokenIdType],
    skip_special_tokens: bool,
) -> DecodeStream;
```

---

## 6.文件加载路径

当前实现通过文件后缀选择 tokenizer 后端：

```rust
pub fn create_tokenizer_from_file(
    file_path: &str,
) -> Result<Arc<dyn traits::Tokenizer>>
```

加载规则如下：

| 文件后缀 | 后端 |
| --- | --- |
| `.json` | `HuggingFaceTokenizer::from_file` |
| `.model` | `TikTokenTokenizer::from_file_auto` |
| `.tiktoken` | `TikTokenTokenizer::from_file_auto` |

加载流程：

```text
Tokenizer::from_file
  ↓
create_tokenizer_from_file
  ↓
根据后缀选择后端
  ↓
创建具体 tokenizer
  ↓
包装为 Tokenizer
```

不支持的后缀会返回错误。

---

## 7.后端实现策略

### 7.1 HuggingFaceTokenizer

`.json` 文件加载为 `HuggingFaceTokenizer`。它处理 HuggingFace tokenizer 文件，并且也被 `FastTokenizer` 用作解码后端。

### 7.2 FastTokenizer

`FastTokenizer` 是混合实现：

```rust
pub struct FastTokenizer {
    fast_encoder: fastokens::Tokenizer,
    hf_decoder: HuggingFaceTokenizer,
}
```

设计原因是：

- `fastokens` 提供更快的 BPE 编码；
- `fastokens` 当前只用于编码；
- 解码仍然交给 HuggingFace tokenizer；
- 同一个 `tokenizer.json` 会同时加载 fast encoder 和 HuggingFace decoder。

流程：

```text
文本
  ↓ fastokens encode
Encoding::Sp(token_ids)

token ids
  ↓ HuggingFace decode
DecodeResult
```

批量编码使用并行方式执行。

### 7.3 TikTokenTokenizer

`TikTokenTokenizer` 基于 `tiktoken_rs::CoreBPE`：

```rust
pub struct TikTokenTokenizer {
    bpe: CoreBPE,
    special_token_ids: HashSet<u32>,
}
```

它负责：

- 解析 `.model` / `.tiktoken` 文件；
- 构建 BPE 编码器；
- 加载 special tokens；
- 编码文本；
- 解码 token ids；
- 处理 UTF-8 partial 解码。

---

## 8.TikToken 自动加载

`TikTokenTokenizer::from_file_auto` 会从 tiktoken 文件所在目录读取配置并自动构建后端。

流程如下：

```text
tiktoken 文件
  ↓
定位父目录
  ↓
读取 config.json 中的 model_type
  ↓
根据 model_type 选择 BPE pattern
  ↓
解析 tiktoken BPE 文件
  ↓
读取 tokenizer_config.json 中的 special tokens
  ↓
构造 CoreBPE
```

当前支持的 `model_type`：

```text
kimi
kimi_k2
kimi_k25
deepseek_v3
```

`tiktoken` 文件格式为：

```text
base64_token rank
```

每一行解析为一个 BPE token bytes 到 rank 的映射。

---

## 9.Special Tokens 处理

TikToken 后端维护：

```rust
special_token_ids: HashSet<u32>
```

用于在解码时支持：

```rust
skip_special_tokens
```

special token 加载顺序：

```text
tokenizer_config.json added_tokens_decoder
  ↓ 如果不存在
生成默认 reserved tokens
  ↓ 如果存在缺口
补齐默认 reserved tokens
```

默认 reserved token 数量为：

```rust
const DEFAULT_NUM_RESERVED_SPECIAL_TOKENS: u32 = 256;
```

默认 token 名称使用绝对 token id：

```text
<|reserved_token_{id}|>
```

这样可以避免 reserved token 被拆成多个普通 BPE token。

---

## 10.DecodeResult：为流式解码保留状态信息

流式生成时，一个 token 不一定能形成完整文本。例如 UTF-8 多字节字符、中文、emoji 或 byte fallback 都可能需要多个 token 才能完整解码。

因此当前实现不直接返回 `String`，而是返回：

```rust
pub enum DecodeResult {
    Complete(String),
    Partial(String),
}
```

含义：

| 状态 | 含义 |
| --- | --- |
| `Complete` | 文本完整，可以输出 |
| `Partial` | 文本尾部可能不完整，需要等待后续 token |

`DecodeResult::from_decoded` 会根据文本是否以 `U+FFFD` 结尾判断是否 partial。

---

## 11.DecodeStream：token 流到文本流

`DecodeStream` 用于把逐 token 生成结果转换成可输出文本片段。

它维护：

```rust
tokenizer: Arc<dyn traits::Tokenizer>
skip_special_tokens: bool
all_token_ids: Vec<u32>
prefix_offset: usize
read_offset: usize
```

创建时会保留 prompt token ids，并从 prompt 尾部回看最多 5 个 token，以便新增 token 的解码能够参考上下文。

核心接口：

```rust
pub fn step(&mut self, id: u32) -> Result<Option<String>>
```

处理流程：

```text
追加新 token
  ↓
解码旧 prefix
  ↓
解码 prefix + 新 token
  ↓
比较两者差异
  ↓
完整新增文本 -> Some(text)
  ↓
暂时不完整 -> None
```

`None` 不表示错误，只表示当前 token 还不能安全输出文本。

---

## 12.Sequence：维护增长中的 token 序列

`Sequence` 用于维护一条持续增长的 token 序列。

它支持：

- 追加文本；
- 追加 token id；
- 查询当前 token ids；
- 解码完整文本；
- 清空状态。

主要接口包括：

```rust
pub fn append_text(&mut self, input: &str) -> Result<()>;
pub fn append_token_id(&mut self, token_id: TokenIdType) -> Result<String>;
pub fn token_ids(&self) -> &[TokenIdType];
pub fn text(&self) -> Result<String>;
```

`append_token_id` 会尝试返回新增文本。如果当前 token 仍处于 partial 解码状态，则返回空字符串。

---

## 13.StopSequenceDecoder：生成停止判断

`StopSequenceDecoder` 用于边接收 token，边执行解码和停止条件判断。

整体流程：

```text
生成 token id
  ↓
Sequence.append_token_id
  ↓
得到新增文本
  ↓
检查 stop token id
  ↓
检查 stop sequence
  ↓
返回输出状态
```

输出状态：

```rust
pub enum SequenceDecoderOutput {
    Text(String),
    Held,
    Stopped,
    StoppedWithText(String),
}
```

含义：

| 输出 | 说明 |
| --- | --- |
| `Text` | 正常输出文本 |
| `Held` | 文本可能是 stop sequence 前缀，暂时保留 |
| `Stopped` | 命中 hidden stop，不返回 stop 内容 |
| `StoppedWithText` | 命中 visible stop，并返回文本 |

支持通过 builder 添加：

- visible stop token id；
- hidden stop token id；
- visible stop sequence；
- hidden stop sequence。

当前实际匹配重点是 hidden stop sequence 和 token id stop。

---

## 14.主要使用路径

### 14.1 请求编码

```text
text
  ↓
Tokenizer.encode
  ↓
Encoding
  ↓
token_ids()
```

### 14.2 普通解码

```text
token ids
  ↓
Tokenizer.decode
  ↓
DecodeResult
```

### 14.3 流式解码

```text
prompt token ids
  ↓
Tokenizer.decode_stream
  ↓
DecodeStream.step(new_token)
  ↓
Some(text) / None
```

### 14.4 带停止条件的生成解码

```text
Tokenizer
  ↓
StopSequenceDecoderBuilder
  ↓
append_token_id
  ↓
Text / Held / Stopped / StoppedWithText
```

---

## 15.与其他模块的关系

| 模块 | 关系 |
| --- | --- |
| Runtime | 使用 tokenizer 编码请求文本、解码流式输出 |
| Tokens | 使用 tokenizer 输出的 token ids 计算后续 token hash |
| Worker | 接收 tokenizer 生成的 token ids 并执行模型推理 |
| Frontend | 使用解码结果组织流式响应和 stop 处理 |

---

## 16.测试关注点

当前实现的测试重点包括：

- FastTokenizer 编码和解码；
- FastTokenizer 与 HuggingFace 编码一致；
- FastTokenizer 批量编码；
- FastTokenizer 与 DecodeStream 集成；
- TikToken 文件解析；
- TikToken 手动加载和自动加载；
- special token 跳过；
- tokenizer_config 中 added tokens 加载；
- reserved token 默认生成；
- Kimi 系列 pattern 检测；
- unknown model_type 报错；
- 不完整 UTF-8 byte 不报错；
- 多 token 拼成中文字符；
- 多 token 拼成 emoji；
- 合法 `U+FFFD` token 不被误判为 partial；
- reserved token 使用绝对 ID 命名，避免 token 膨胀。

---

## 17.总结

当前 `tokenizers` 模块的设计主线是：

```text
多 tokenizer 后端
  ↓
统一 Encoder / Decoder / Tokenizer trait
  ↓
Tokenizer wrapper 屏蔽具体实现
  ↓
Encoding 统一 token id 访问
  ↓
DecodeResult 区分 Complete / Partial
  ↓
DecodeStream 支持增量解码
  ↓
Sequence 管理增长中的 token 序列
  ↓
StopSequenceDecoder 处理停止条件
```

它的核心价值是：

> 让 Runtime、Worker 和生成逻辑通过统一接口完成编码、解码、流式输出和停止判断，而不直接依赖具体 tokenizer 后端。
