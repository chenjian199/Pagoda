# Tokens Hashing 设计文档：从序列到链式关系

## 1. 概要

`tokens` 模块用于为 LLM 推理系统中的 token 序列和 token block 生成稳定、轻量、可比较的哈希标识。

它的核心思想是：

> 将 token block 的内容、位置和父子关系压缩编码到简单的整数值中，而不是依赖复杂结构体、指针、堆分配或引用计数。

本模块主要设计三类哈希：

| 类型 | 底层大小 | 表达的信息 |
| --- | ---: | --- |
| `SequenceHash` | `u64` | 序列内容身份 |
| `PositionalSequenceHash` | `u128` | 序列内容身份 + block 位置 |
| `PositionalLineageHash` | `u128` | 序列内容身份 + block 位置 + 父子链式关系 |

这样设计可以带来：

- O(1) 比较；
- 按值复制；
- 无需堆分配；
- 无需 `Arc` 引用计数；
- 线程间传递更简单；
- 更适合作为缓存 key；
- 更适合 KV cache、prefix cache 和分布式缓存协调。

---

## 2. 背景问题：Token Block 会形成树

### 2.1 LLM 推理天然会产生 Prefix Tree

多个用户请求经常共享相同前缀。

例如：

```text
System Prompt
├── User A: Hello
│   ├── Assistant: Hi!
│   └── Assistant: Hello!
└── User B: Hi there
    └── Assistant: Hey!
```

如果按 token block 来看，可以理解为：

```text
Block 0: System Prompt
├── Block 1: User A
│   ├── Block 2: Assistant A1
│   └── Block 2: Assistant A2
└── Block 1: User B
    └── Block 2: Assistant B1
```

这里有几个重要特点：

- `System Prompt` 被多个请求共享；
- `User A` 和 `User B` 是不同分支；
- 后续 assistant 回复继续形成子节点；
- 整体结构不是一条线，而是一棵前缀树。

### 2.2 为什么普通内容 Hash 不够

普通 hash 只能回答：

```text
这两个 block 的 token 内容是否相同？
```

但不能回答：

```text
1.它们是不是在同一个位置？
2.它们前面的 prefix 是否相同？
3.它们的父 block 是谁？
4.它们是不是叶子节点？
5.它们能不能被安全淘汰？
```

对比：

| 问题 | 普通内容 Hash | 带位置 Hash | 带链式 Hash |
| --- | --- | --- | --- |
| token 内容是否一样 | 可以 | 可以 | 可以 |
| 是否在同一个 block 位置 | 不知道 | 可以 | 可以 |
| 父 block 是谁 | 不知道 | 不知道 | 可以 |
| 是否适合叶子淘汰 | 不方便 | 不方便 | 可以 |

如果没有位置和父子关系信息，KV cache 系统很难正确地：

- 复用共享 prefix；
- 避免错误复用不同上下文下的 block；
- 优先淘汰叶子节点；
- 保留高价值共享前缀。

---

## 3. 为什么不用传统结构体

一种直接做法是：

```rust
struct BlockInfo {
    content_hash: u64,
    position: u64,
    parent: Option<Arc<BlockInfo>>,
}
```

这种方式虽然直观，但在高并发推理系统里有明显问题：

| 问题 | 说明 |
| --- | --- |
| 堆分配 | 每个 block 都需要单独分配对象 |
| 引用计数 | `Arc` 会带来原子引用计数开销 |
| 指针追踪 | 查找父节点时需要不断跟随指针 |
| Cache miss | 父子节点不一定连续存储，CPU cache 不友好 |
| 分布式不友好 | 指针关系无法直接跨节点传递 |
| 并发复杂 | 生命周期和引用关系更难维护 |

因此 Pagoda 的 `tokens` 模块采用值编码方式：

> 不用指针表达关系，而是把关系编码到 `u64` 或 `u128` 里。

---

## 4. 设计原则：把关系压缩到值类型中

Pagoda `tokens` 模块的 hash 类型应满足：

- 是简单整数值；
- 可以复制；
- 可以比较；
- 可以作为 HashMap / DashMap 的 key；
- 可以低成本序列化；
- 不依赖堆对象；
- 不依赖父节点指针；
- 不依赖引用计数；
- 能够表达 token block 的位置和父子关系。

核心思想：

```text
传统方式:
BlockInfo -> parent pointer -> parent BlockInfo

Pagoda 方式:
PositionalLineageHash = position + current fragment + parent fragment
```

---

## 5. Hash 类型演进

### 5.1 `SequenceHash`：序列内容身份

`SequenceHash` 是最基础的哈希。

它是一个 `u64`，用于表示：

> 从序列开头到当前 block 为止的完整 prefix 身份。

概念计算方式：

```text
local_hash[0] = hash(block_0_tokens)
sequence_hash[0] = hash(local_hash[0])

local_hash[1] = hash(block_1_tokens)
sequence_hash[1] = hash(sequence_hash[0], local_hash[1])

local_hash[2] = hash(block_2_tokens)
sequence_hash[2] = hash(sequence_hash[1], local_hash[2])
```

也就是说：

```text
sequence_hash[i]
```

不是只表示当前 block，而是表示：

```text
Block 0 -> Block 1 -> ... -> Block i
```

这条完整路径。

#### 例子

```text
请求 A:
Block 0 = SYS
Block 1 = UserA

请求 B:
Block 0 = SYS
Block 1 = UserA
```

它们到 `Block 1` 为止的 prefix 一样，所以 `SequenceHash` 一样。

但如果：

```text
请求 C:
Block 0 = OtherSYS
Block 1 = UserA
```

虽然 `UserA` 的 block 内容可能一样，但前缀不同，所以 `SequenceHash` 不一样。

#### 能力

`SequenceHash` 可以回答：

```text
这条 prefix 路径是否相同？
```

#### 限制

它不能直接回答：

```text
当前 block 在第几层？
当前 block 的父节点是谁？
```

---

### 5.2 `LocalBlockHash`：当前 block 自己的内容指纹

`LocalBlockHash` 表示：

> 当前 block 自己的 token 内容 hash。

它只看当前 block，不看前缀，不看位置。

例如：

```text
Block A = [5, 6, 7, 8]
Block B = [5, 6, 7, 8]
```

那么：

```text
local_hash(A) == local_hash(B)
```

即使 A 在 position 0，B 在 position 5，只要内容相同，local hash 就相同。

它的作用是：

```text
判断当前 block 自己的 token 内容是否相同。
```

但它不能用于判断 KV cache 是否一定可复用，因为 KV cache 还依赖上下文。

---

### 5.3 `PositionalSequenceHash`：内容身份 + 位置

`PositionalSequenceHash` 是一个 `u128`，用于表达：

```text
完整 prefix 身份 + 当前 block 所在位置 + 当前 block 局部内容信息
```

可以理解为：

```text
PositionalSequenceHash =
    position
  + local_block_hash fragment
  + sequence_hash
```

逻辑布局：

```text
u128
├── 高 64 bits
│   ├── mode
│   ├── position
│   └── local block hash fragment
└── 低 64 bits
    └── SequenceHash
```

#### position 是什么？

这里的 `position` 不是内存位置，也不是树中唯一编号。

它表示：

> 当前 block 是整条 token 序列中的第几个 block。

如果把 block 组织成 prefix tree，那么它等价于：

> 当前节点在树中的层数 / depth。

例如：

```text
Block 0 -> position 0 -> 树第 0 层
Block 1 -> position 1 -> 树第 1 层
Block 2 -> position 2 -> 树第 2 层
```

同一层可以有多个节点，所以 position 不能单独唯一标识一个 block。

#### 编码模式

position 越大，需要的 bit 越多，留给 local hash 的 bit 越少。

| Mode | Position Bits | Local Hash Bits | 最大 Position |
| --- | ---: | ---: | ---: |
| `00` | 8 | 54 | 255 |
| `01` | 16 | 46 | 65,535 |
| `10` | 24 | 38 | 16,777,215 |
| `11` | 31 | 31 | 2,147,483,647 |

#### 能力

`PositionalSequenceHash` 可以回答：

```text
这是不是同一个 prefix，并且在同一个 block 位置？
```

#### 限制

它仍然不能直接回答：

```text
它的父节点是谁？
```

因为 hash 是单向的，无法从当前 `SequenceHash` 反推出父 `SequenceHash`。

---

### 5.4 `PositionalLineageHash`：内容身份 + 位置 + 父子关系

`PositionalLineageHash` 是本模块最重要的设计。

它也是 `u128`，但它编码的是：

```text
position
+ parent hash fragment
+ current hash fragment
```

逻辑布局：

```text
u128
├── mode
├── position
├── parent hash fragment
└── current hash fragment
```

它可以回答：

```text
当前 block 在第几层？
当前 block 的身份片段是什么？
父 block 的身份片段是什么？
```

#### 编码模式

| Mode | Position Bits | Parent Bits | Current Bits | 最大 Position |
| --- | ---: | ---: | ---: | ---: |
| `00` | 8 | 59 | 59 | 255 |
| `01` | 16 | 55 | 55 | 65,535 |
| `10` | 24 | 51 | 51 | 16,777,215 |

#### current fragment 是什么？

`current_hash_fragment` 是当前 block 的 `SequenceHash` 截断片段。

例如：

```text
current_sequence_hash = 0xABCDEF1234567890
current_fragment = 取其中若干 bit
```

因为 `u128` 里还要放 position 和 parent fragment，所以不能完整保存所有信息，只保存足够用于匹配的片段。

#### parent fragment 是什么？

`parent_hash_fragment` 是父 block 的 `current_hash_fragment`。

例如：

```text
Block A:
position = 0
current = frag(A)
parent = none

Block B:
position = 1
current = frag(B)
parent = frag(A)

Block C:
position = 2
current = frag(C)
parent = frag(B)
```

这样就可以不靠指针找到父节点。

---

## 6. Lineage Hash 如何找父节点

假设有一棵 prefix tree：

```text
Position 0:
A current = 0xAAA

Position 1:
B parent = 0xAAA, current = 0xBBB
C parent = 0xAAA, current = 0xCCC

Position 2:
D parent = 0xBBB, current = 0xDDD
```

如果现在拿到 D：

```text
D.position = 2
D.parent_fragment = 0xBBB
```

查父节点的过程是：

```text
1. D 在 position 2
2. 父节点一定在 position 1
3. 去 position 1 的节点里找 current_fragment == 0xBBB
4. 找到 B
```

所以：

```text
D 的父节点是 B
```

这个过程不需要：

- parent 指针；
- `Arc`;
- 引用计数；
- 额外 parent map；
- 堆分配对象。

只需要：

```text
position + fragment 整数比较
```

---

## 7. 跨 Mode 边界对齐

### 7.1 问题

当 position 从 255 增加到 256 时，编码模式会变化。

```text
position 255 使用 Mode 0:
current fragment 可以有 59 bits

position 256 使用 Mode 1:
parent fragment 只能有 55 bits
```

问题是：

```text
position 256 的 parent_fragment
需要匹配 position 255 的 current_fragment
```

如果父节点保存了 59 bits，而子节点只能保存 55 bits，就无法直接匹配。

### 7.2 解决方案

在边界位置提前截断。

也就是说：

```text
position 255 虽然可以保存 59 bits，
但因为它的子节点 position 256 只能保存 55 bits，
所以 position 255 的 current fragment 提前截断成 55 bits。
```

这样：

```text
position 255 current_fragment = 55 bits
position 256 parent_fragment = 55 bits
```

两边就能匹配。

### 7.3 需要重点测试的边界

- `255 -> 256`
- `65,535 -> 65,536`

这些位置会发生 mode 切换，必须保证父子关系不会断裂。

---

## 8. PositionalHash Trait

为了让不同带 position 的 hash 类型都能被统一索引，可以定义一个公共 trait：

```rust
pub trait PositionalHash {
    fn position(&self) -> u64;
}
```

`PositionalSequenceHash` 和 `PositionalLineageHash` 都应该实现这个 trait。

这样同一个位置索引结构可以支持不同 key 类型。

---

## 9. PositionalRadixTree：按位置分层的稀疏索引

`PositionalRadixTree` 用于存储带 position 的 hash key。

它的结构不是传统字符 radix tree，而是：

```text
第一层：position
第二层：hash -> value
```

结构示意：

```text
Position 0
  └── HashMap<Hash, Value>

Position 1
  └── HashMap<Hash, Value>

Position 2
  └── HashMap<Hash, Value>

Position N
  └── HashMap<Hash, Value>
```

### 好处

| 好处 | 说明 |
| --- | --- |
| 按层查找 | 父节点一定在 `position - 1` |
| 稀疏分配 | 只有有 block 的 position 才分配 map |
| 查找快 | 同一 position 内可以 O(1) hash 查找 |
| 并发友好 | 不同 position 可分别加锁或并发访问 |
| 适合 prefix tree | position 正好对应树的层数 |

### 示例

```text
tree = {
  0: {
    hash_SYS -> Block SYS
  },
  1: {
    hash_UserA -> Block UserA,
    hash_UserB -> Block UserB
  },
  2: {
    hash_AnswerA -> Block AnswerA,
    hash_AnswerB -> Block AnswerB
  }
}
```

---

## 10. 使用场景

### 10.1 Prefix Cache / KV Cache 复用

假设有两个请求：

```text
请求 A:
SYS -> UserA -> Question1

请求 B:
SYS -> UserA -> Question2
```

它们共享：

```text
SYS -> UserA
```

因此可以复用前两个 block 对应的 KV cache。

`SequenceHash` 和 `PositionalSequenceHash` 可以帮助判断 prefix 是否相同。

---

### 10.2 防止错误复用

假设：

```text
请求 A:
SYS1 -> BlockX

请求 B:
SYS2 -> BlockX
```

虽然 `BlockX` 内容相同：

```text
local_hash(BlockX) 相同
```

但前缀不同：

```text
sequence_hash(BlockX) 不同
```

所以它们的 KV cache 不应直接复用。

---

### 10.3 Lineage-Aware Eviction

当缓存空间不足时，应优先淘汰叶子节点。

例如：

```text
SYS
├── UserA
│   ├── AnswerA
│   └── AnswerC
└── UserB
    └── AnswerB
```

优先淘汰：

```text
AnswerA
AnswerC
AnswerB
```

不要优先淘汰：

```text
SYS
UserA
UserB
```

因为它们可能是共享前缀，被多个后续节点依赖。

使用 `PositionalLineageHash` 可以判断：

```text
某个 block 有没有子节点？
某个 block 是不是叶子？
某个 block 的父节点是谁？
```

---

### 10.4 分布式缓存协调

因为 `u128` 只有 16 字节，所以很适合跨节点传输。

可以用于：

- Router 查询某个 Worker 是否有指定 prefix cache；
- Worker 上报自己持有的 KV block；
- Cache coordinator 维护全局 block 状态；
- 外部 Redis / RocksDB / memcached 作为 cache key；
- Event Plane 传播 cache add/remove/update 事件。

---

## 11. API 设计草案

### 11.1 `SequenceHash`

```rust
pub struct SequenceHash(u64);
```

建议能力：

```rust
impl SequenceHash {
    pub fn from_block(tokens: &[TokenId]) -> Self;

    pub fn extend(self, local_block_hash: BlockHash) -> Self;

    pub fn as_u64(&self) -> u64;
}
```

---

### 11.2 `PositionalSequenceHash`

```rust
pub struct PositionalSequenceHash(u128);
```

建议能力：

```rust
impl PositionalSequenceHash {
    pub fn new(
        sequence_hash: SequenceHash,
        position: u64,
        local_block_hash: BlockHash,
    ) -> Self;

    pub fn sequence_hash(&self) -> SequenceHash;

    pub fn position(&self) -> u64;

    pub fn local_block_hash(&self) -> BlockHash;

    pub fn mode(&self) -> u8;

    pub fn as_u128(&self) -> u128;
}
```

---

### 11.3 `PositionalLineageHash`

```rust
pub struct PositionalLineageHash(u128);
```

建议能力：

```rust
impl PositionalLineageHash {
    pub fn new(
        current_seq_hash: SequenceHash,
        parent_seq_hash: Option<SequenceHash>,
        position: u64,
    ) -> Self;

    pub fn position(&self) -> u64;

    pub fn current_hash_fragment(&self) -> u64;

    pub fn parent_hash_fragment(&self) -> u64;

    pub fn mode(&self) -> u8;

    pub fn as_u128(&self) -> u128;
}
```

---

### 11.4 `PositionalRadixTree`

```rust
pub struct PositionalRadixTree<V, K = PositionalSequenceHash>
where
    K: PositionalHash + Hash + Eq + Clone,
{
    // position -> hash map
}
```

建议能力：

```rust
impl<V, K> PositionalRadixTree<V, K>
where
    K: PositionalHash + Hash + Eq + Clone,
{
    pub fn new() -> Self;

    pub fn insert(&self, key: K, value: V);

    pub fn get(&self, key: &K) -> Option<V>;

    pub fn remove(&self, key: &K) -> Option<V>;

    pub fn position(&self, position: u64) -> Option<PositionMap<K, V>>;

    pub fn len(&self) -> usize;
}
```

对于 `PositionalLineageHash`，可以扩展：

```rust
impl<V> PositionalRadixTree<V, PositionalLineageHash> {
    pub fn find_parent(&self, key: &PositionalLineageHash) -> Option<V>;

    pub fn find_children(&self, key: &PositionalLineageHash) -> Vec<V>;

    pub fn is_leaf(&self, key: &PositionalLineageHash) -> bool;
}
```

---

## 12. 与 Pagoda 其他模块的关系

### 12.1 与 Runtime

Runtime 负责请求生命周期、流式返回、取消和状态管理。

`tokens` 模块可以为 Runtime 请求上下文提供：

- prefix hash；
- block position；
- cache hint；
- lineage key。

Runtime 不负责具体 KV cache 淘汰策略。

---

### 12.2 与 Router / KV Router

Router 可以使用 `tokens` 提供的 hash 来判断：

- 请求 prefix 是否已被某个 Worker 缓存；
- 哪个 Worker 拥有更长的 prefix 命中；
- 是否应该优先路由到某个缓存命中更高的 Worker；
- prefill / decode 分离时如何选择目标实例。

---

### 12.3 与 Worker

Worker 负责真实模型执行和 KV cache 写入。

Worker 可以使用 `tokens` 模块生成或消费：

- block key；
- prefix key；
- lineage key；
- cache 上报信息。

---

### 12.4 与 Memory / KVBM

KVBM 负责真实 KV block 的资源管理。

`tokens` 模块提供：

- KV block 的身份 key；
- 父子关系；
- 叶子判断能力；
- position 分层索引。

KVBM 使用这些信息决定：

- 哪些 block 可以复用；
- 哪些 block 可以淘汰；
- 如何维护 prefix tree。

---

## 13. 第一阶段实现范围

Pagoda `tokens` 模块第一阶段建议实现：

| 编号 | 能力 |
| --- | --- |
| TOK-P0-001 | `SequenceHash` |
| TOK-P0-002 | `LocalBlockHash` / `BlockHash` |
| TOK-P0-003 | `PositionalSequenceHash` |
| TOK-P0-004 | `PositionalLineageHash` |
| TOK-P0-005 | `PositionalHash` trait |
| TOK-P0-006 | `PositionalRadixTree` |
| TOK-P0-007 | position / mode 编码和解码 |
| TOK-P0-008 | parent/current fragment 提取 |
| TOK-P0-009 | mode 边界对齐 |
| TOK-P0-010 | 父节点查找 |
| TOK-P0-011 | 子节点查找 |
| TOK-P0-012 | 叶子节点判断 |
| TOK-P0-013 | 单元测试和边界测试 |

---

## 14. 测试要求

### 14.1 基础 Hash 测试

- 相同 token block 生成相同 local hash；
- 不同 token block 尽量生成不同 local hash；
- 相同 prefix 生成相同 sequence hash；
- 不同 prefix 生成不同 sequence hash；
- 相同 block 接在不同 prefix 后，sequence hash 不同。

### 14.2 Position 测试

- position 可正确编码；
- position 可正确解码；
- 不同 position 的 hash 不会误判为同一个；
- position 0、1、255、256、65535、65536 都应覆盖。

### 14.3 Lineage 测试

- position 0 根节点处理正确；
- 子节点 parent fragment 等于父节点 current fragment；
- 可从子节点找到父节点；
- 可从父节点找到子节点；
- 可判断叶子节点；
- mode 边界处父子关系不断裂。

### 14.4 RadixTree 测试

- 插入后可查询；
- 删除后不可查询；
- 不同 position 独立存储；
- 同一 position 下多个 key 可共存；
- 空 position 不应创建无意义数据；
- 并发插入和查询行为正确。

---

## 15. 未来方向

### 15.1 压缩血缘链

对于非常长的序列，可以考虑保存更多祖先信息，形成类似 skip-list 的 lineage hash。

这样可以更快跳转到更远的祖先节点。

### 15.2 分布式 Cache Directory

后续可以建设全局 cache directory：

```text
lineage_hash -> worker_id / memory_tier / ref_count / last_access
```

用于支持 KV-aware routing 和跨 Worker 缓存复用。

### 15.3 外部缓存集成

这些 hash 可以作为外部缓存 key，用于：

- Redis；
- RocksDB；
- memcached；
- 本地磁盘 cache；
- 分布式 cache index。

### 15.4 冲突校验增强

hash 理论上存在碰撞风险。

后续可在 debug 或高可靠模式中增加：

- block token 数量；
- tokenizer version；
- model id；
- config hash；
- 二级 checksum；
- 原始 token 片段校验。

---

## 16. 总结

Pagoda `tokens` 模块的核心价值是：

> 用简单的值类型表达 token block 的内容、位置和父子血缘关系，为 KV cache 复用、prefix tree 索引、叶子淘汰和分布式缓存协调提供基础能力。

三类 hash 的关系可以总结为：

| 类型 | 解决的问题 |
| --- | --- |
| `SequenceHash` | 当前 block 对应的完整 prefix 是谁 |
| `PositionalSequenceHash` | 当前 prefix 在第几个 block 位置 |
| `PositionalLineageHash` | 当前 block 是谁，以及它的父 block 是谁 |

最关键的理解是：

```text
local_hash
= 当前 block 自己的内容指纹

sequence_hash
= 从开头到当前 block 的完整路径指纹

position
= 当前 block 在序列中的 block index，也就是 prefix tree 的层数

parent_fragment
= 父 block 的身份片段

current_fragment
= 当前 block 的身份片段
```

最终，Pagoda 可以在不使用指针和复杂对象的情况下，高效完成：

- prefix cache 查询；
- block 去重；
- 父子关系回溯；
- 叶子节点识别；
- lineage-aware eviction；
- 分布式 cache key 传递。
