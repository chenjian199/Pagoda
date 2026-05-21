# `pipeline` 模块设计还原文档

这份文档的目标不是做 API 索引，也不是简单复述源码注释，而是尽量用“**像写代码之前的设计文档**”的方式，把当前 `pipeline` 模块的设计意图、分层、关键抽象、结构体字段、核心函数和典型调用链重新还原出来。  

你可以把它当成一份“读源码前的地图”：

- 先看这份文档，建立整体心智模型
- 再回到 `pipeline` 源码，一个结构体一个函数去对照

源码范围：

- `lib/runtime/src/pipeline.rs`
- `lib/runtime/src/pipeline/context.rs`
- `lib/runtime/src/pipeline/registry.rs`
- `lib/runtime/src/pipeline/error.rs`
- `lib/runtime/src/pipeline/nodes.rs`
- `lib/runtime/src/pipeline/nodes/`
- `lib/runtime/src/pipeline/network.rs`
- `lib/runtime/src/pipeline/network/`

---

## 一、整体设计意图

`pipeline` 模块想解决的不是某一个孤立问题，而是一整组彼此耦合的问题：

1. **一次请求如何表示**
2. **请求在本地如何经过一串处理节点**
3. **如何把本地调用扩展成远程调用**
4. **如何让流式响应、取消、上下文、错误传播在整个过程中都保持一致**

如果没有这套模块，系统通常会退化成下面这种形态：

- 请求就是裸数据，没有统一上下文
- 节点之间直接互相调用，没有可组合图结构
- 本地调用和远程调用走两套完全不同的接口
- 流式响应只是某个 transport 的细节，而不是系统级语义

`pipeline` 的设计目标，就是把这些问题统一到一套抽象下：

- **请求**统一表示为带上下文的输入
- **处理链**统一表示为 `Source -> Edge -> Sink` 图
- **本地执行**和**远程执行**统一表示为 `AsyncEngine::generate`
- **取消、阶段标签、元数据**统一由 `Context` / `Controller` / `Registry` 承载
- **流式响应**统一由 `EngineStream` / `ResponseStream` 语义表达

从这个角度看，`pipeline` 实际上是一个把：

- 数据语义
- 图结构
- 分布式 transport

三者粘在一起的中间层。

---

## 二、先建立一个完整心智模型

先不要陷进具体代码，先把整个模块看成一个运行时系统。

一个典型请求，在 `pipeline` 里的完整旅程大概是这样：

1. 调用方发起一个 `generate(request)`
2. 请求被包装成 `Context<T>`，获得 request id、取消状态、阶段标签和 registry
3. 请求从一个入口节点进入图
4. 图中的中间节点逐步变换它
5. 图尾节点调用真实业务引擎
6. 响应再沿着图返回到最初的等待方
7. 如果图尾不是本地引擎，而是远端 worker，那么中间会经过网络层：
   - 前端先注册响应流
   - 把连接信息一起随请求发给远端
   - 远端恢复上下文，执行业务逻辑
   - 再按约定把响应流回推给前端

所以整个模块天然分成三件事：

- **请求是什么**
- **请求如何在图里流动**
- **请求如何跨节点流动**

这也是整个源码的自然分层。

---

## 三、模块分层与文件关系

### 第一层：请求语义层

这层回答的问题是：

- 一次请求是什么
- 它有哪些伴随信息
- 它如何被取消
- 它如何携带额外元数据

核心文件：

- `pipeline.rs`
- `context.rs`
- `registry.rs`
- `error.rs`

核心对象：

- `Context<T>`
- `StreamContext`
- `Controller`
- `Registry`
- `PipelineError`
- `PipelineIO`

### 第二层：本地图执行层

这层回答的问题是：

- 请求如何在单进程内走过一条处理链
- 节点如何组合
- 图入口和图出口怎么实现

核心文件：

- `nodes.rs`
- `nodes/sources.rs`
- `nodes/sources/base.rs`
- `nodes/sources/common.rs`
- `nodes/sinks.rs`
- `nodes/sinks/base.rs`
- `nodes/sinks/pipeline.rs`
- `nodes/sinks/segment.rs`

核心对象：

- `Source<T>`
- `Sink<T>`
- `Edge<T>`
- `Frontend<In, Out>`
- `ServiceFrontend`
- `SegmentSource`
- `SinkEdge<Resp>`
- `ServiceBackend`
- `SegmentSink`
- `Operator<...>`
- `PipelineOperator<...>`
- `PipelineNode<In, Out>`

### 第三层：分布式传输层

这层回答的问题是：

- 如何把一次本地 `generate()` 变成跨进程/跨机器调用
- 如何建立响应流
- 如何在 transport 之间保持统一语义

核心文件：

- `network.rs`
- `network/egress/addressed_router.rs`
- `network/ingress/push_handler.rs`
- `network/tcp.rs`
- `network/tcp/server.rs`
- `network/tcp/client.rs`

核心对象：

- `ConnectionInfo`
- `StreamOptions`
- `RegisteredStream`
- `PendingConnections`
- `ResponseService`
- `StreamSender`
- `StreamReceiver`
- `Egress`
- `Ingress`
- `PushWorkHandler`
- `AddressedPushRouter`

### 第四层：传输选择与生命周期管理层

这层回答的问题是：

- HTTP/TCP/NATS 怎么统一选择
- server/client 谁负责创建
- 多 worker 同进程时怎么共享服务端资源

核心文件：

- `network/manager.rs`
- `network/ingress/unified_server.rs`
- `network/egress/unified_client.rs`

核心对象：

- `NetworkManager`
- `RequestPlaneServer`
- `RequestPlaneClient`

---

## 四、顶层入口 `pipeline.rs`

`pipeline.rs` 的作用是给整套设计定义“公共语言”。

它本身没有复杂业务逻辑，但非常关键，因为所有子模块都建立在它导出的类型和约束之上。

### 4.1 类型别名

这里最重要的一组别名是：

- `SingleIn<T> = Context<T>`
- `ManyIn<T> = Context<DataStream<T>>`
- `SingleOut<T> = EngineUnary<T>`
- `ManyOut<T> = EngineStream<T>`

它们的设计意义是：

- 输入默认都要带 `Context`
- 输出默认都要保留 context 能力
- 单值和流值在类型系统里是可区分的

进一步又定义了：

- `ServiceEngine<T, U>`
- `UnaryEngine<T, U>`
- `ClientStreamingEngine<T, U>`
- `ServerStreamingEngine<T, U>`
- `BidirectionalStreamingEngine<T, U>`

这些别名让整个模块在表达“这是一个单请求多响应引擎”时非常清楚，而不是到处铺开长 trait bound。

如果进一步展开，它们并不只是“把长签名缩短”的语法糖，而是在给整套 pipeline 规定一组稳定的交互语义：

- `SingleIn<T>` 的重点不是“单值”，而是“单个请求对象”。
  只要一个值进入 pipeline，它就必须先被包进 `Context<T>`，这样 request id、取消信号、阶段标签、registry 引用等运行时能力从入口处就被固定下来。pipeline 不接受“裸请求”；任何节点都默认可以假设自己拿到的是一个可追踪、可取消、可传播元数据的请求。

- `ManyIn<T>` 的重点不是“很多个带 context 的 T”，而是“一个带统一 context 的输入流”。
  这里用的是 `Context<DataStream<T>>`，而不是 `DataStream<Context<T>>`。这说明设计上把 client-streaming 看成“一个请求会话里持续到达的一串输入片段”，而不是“很多个彼此独立的小请求”。这样取消、超时、链路追踪、资源清理都能在整条输入流维度上统一处理。

- `SingleOut<T>` 和 `ManyOut<T>` 的重点不是返回值的容器形状，而是“输出仍然保留 request context 能力”。
  `EngineUnary<T>` / `EngineStream<T>` 都还能把 context 暴露出来，所以响应不是脱离请求语境的裸数据。这样无论结果是单值还是流，后续都还能继续拿同一个 request id 做日志关联、错误归因、流注册、取消传播和下游传输。

- `Single*` / `Many*` 的区分，本质上是在类型系统里显式编码“基数(cardinality)”。
  某个节点到底消费一个值还是一个流、产出一个值还是一个流，不再靠文档约定或命名习惯推断，而是直接体现在签名里。这样图连接是否合法、某个 operator 是否能接在某个 source/sink 之后，很多时候在编译期就能看出来。

在这组基础别名之上，`ServiceEngine<T, U>` 和四种 `*StreamingEngine` 别名表达的是另一层设计意图：把常见服务交互模式统一投影到同一个 `AsyncEngine` 抽象里。

- `ServiceEngine<T, U>` 是最底层、最中性的名字。
  它只是说“这里有一个 `AsyncEngine<T, U, Error>`”，并不提前假设输入输出是单值还是流。这个别名的作用是先把错误类型和服务语义固定下来，让 pipeline 内部大多数位置都用同一套 engine 语言说话。

- `UnaryEngine<T, U>` 对应最普通的 request-response 形状。
  它表达的是“收到一个带 context 的请求，返回一个仍可关联同一 context 的单值响应”。这类引擎最适合表示普通同步 RPC、单次变换、校验/改写类节点。

- `ClientStreamingEngine<T, U>` 对应“输入是流、输出是单值”的聚合型交互。
  它强调实现者面对的是一整个输入会话，通常会消费完整条输入流，再在结束时给出一个最终结果；当然也允许中途提前结束。这种形状适合上传、批量聚合、收集后一次性归约的场景。

- `ServerStreamingEngine<T, U>` 对应“输入是单值、输出是流”的展开型交互。
  一个请求启动一次处理，但结果会持续地产生多段输出。推理 token 流、SSE、分段检索结果、渐进式计算都天然落在这个模型里。它也是当前 pipeline 最核心的服务形状，因为它能同时覆盖“只回一个结果”和“持续回多段结果”两类场景。

- `BidirectionalStreamingEngine<T, U>` 对应“输入输出都是流”的全双工交互。
  这里设计上不强制输入元素和输出元素一一对应，也不强制严格顺序关系；它只表达双方都可以在同一 request context 下持续交换数据。这个抽象给实时会话、代理转发、增量交互式推理预留了空间。

因此，这组别名的真正价值有三点：

- 它把“单值 / 流式”从实现细节提升成了公共协议语言；
- 它把本地节点、远程 transport、上层服务定义压到同一套交互形状上；
- 它让调用方、图构建器、网络层和业务引擎都能只通过类型签名就看懂一条服务链的行为边界。

### 4.2 `AsyncTransportEngine<T, U>`

定义：

```rust
pub trait AsyncTransportEngine<T: Data + PipelineIO, U: Data + PipelineIO>:
    AsyncEngine<T, U, Error> + Send + Sync + 'static
{
}
```

如果只看语法，它确实只是一个标记 trait。  
但在设计上，它表达的是一条非常重要的边界：

- `AsyncEngine<T, U, Error>` 只说明“这个对象能收一个 `T`，返回一个 `U`”
- `AsyncTransportEngine<T, U>` 则进一步说明“这个 engine 可以承担跨进程/跨机器传输职责”

这个区别的意义在于，pipeline 并不想让任何普通业务 engine 都被当成 transport 使用。

换句话说，`AsyncTransportEngine` 不是在增加能力，而是在给一类特殊职责命名：

- 它的输入输出仍然遵守 pipeline 的上下文约束
- 但它内部会做额外的网络工作，例如编码、发包、注册响应流、等待连接、恢复字节流

因此它解决的不是“怎么做业务处理”，而是“怎么把一次 `AsyncEngine::generate()` 延长到网络另一端”。

从分层角度看，它还有两个很关键的作用：

- 对上，`Egress` 可以只依赖这个抽象，而不关心底下是 HTTP、TCP、NATS 还是未来别的 request plane；
- 对下，具体 transport 实现可以自由演化，只要继续满足 `AsyncEngine` 语义和 pipeline 的 `PipelineIO` 约束。

所以它真正统一的不是协议细节，而是**远程调用在上层看来仍然像一个普通 engine** 这件事。

### 4.3 `PipelineIO`

`PipelineIO` 的核心问题是：**什么样的对象才允许在 pipeline 里流动？**

这里看似只是一个 trait 约束集合，实际上是在给整张图定义“可连接对象”的最低公共协议。

它要求一个类型同时满足三件事：

- 必须是 `sealed::Connectable`
- 必须实现 `AsyncEngineContextProvider`
- 必须提供稳定的 `id()`

这三条分别对应三种设计约束：

#### 第一层约束：它必须是图里认可的“连接形状”

`sealed::Connectable` 只对少数几类类型开放：

- `Context<T>`
- `EngineUnary<T>`
- `EngineStream<T>`

这等于是在说，pipeline 不是一个“任意类型都能过边”的泛型数据流框架。  
它只允许三类标准形状通过连接点：

- 带上下文的单值输入
- 带上下文的单值输出
- 带上下文的流式输出

这样做的好处是，图连接的合法性不会无限开放；节点之间连的不是“任何 T”，而是被 runtime 明确认可的请求/响应载体。

#### 第二层约束：它必须能暴露请求上下文

无论当前对象是请求 `Context<T>`，还是响应 `EngineUnary<T>` / `EngineStream<T>`，后续节点都必须能统一拿到：

- request id
- 停止/终止状态
- 父子取消链

所以 `PipelineIO` 强制要求实现 `AsyncEngineContextProvider`。  
这意味着图中的任何连接点都不需要猜“这里还能不能取 context”；答案永远是“可以”。

#### 第三层约束：它必须能给出稳定身份

`id()` 的要求听上去很小，但它是整个系统能成立的关键之一。

因为 pipeline 中很多机制都建立在 request id 之上：

- `Frontend` 用它把响应配回最初等待者
- 网络层用它匹配已注册流和真正到来的连接
- 指标、日志、追踪都用它关联同一条请求链

所以 `PipelineIO` 的本质不是“方便拿个字符串 id”，而是把**可追踪性**写进了所有流经图的对象契约里。

因此，`PipelineIO` 真正解决的是：

- 图里流动的对象不能只是“值”
- 它们还必须是“可连接、可追踪、可取消、可配对”的运行时对象

这也是为什么 `pipeline` 不是一套纯函数式变换框架，而是一套带请求生命周期语义的运行时图模型。

---

## 五、请求语义层

## 5.1 `Context<T>`

`Context<T>` 是整个 `pipeline` 设计的根。

如果只记一句话：

- **`Context<T>` = 业务载荷 + 请求生命周期控制 + 请求级元数据**

`Context<T>` 最重要的设计意图，不是“给一个值多包一层壳”，而是把“一次请求”从一开始就提升为运行时一级对象。

如果没有它，系统会很快退化成下面这种样子：

- 业务 payload 单独传
- request id 额外传
- 取消 token 额外传
- 阶段信息靠日志字符串零散拼接
- 某些中间态信息被塞进临时参数或 thread-local

这样一来，请求只要在中途变形一次、跨模块一次、跨机器一次，就很容易丢身份、丢取消语义、丢附加信息。

`Context<T>` 的作用就是把这些伴随能力从“外围约定”变成“请求本体的一部分”。

### 字段

- `current: T`
- `controller: Arc<Controller>`
- `registry: Registry`
- `stages: Vec<String>`

### 这些字段为什么要绑在一起

#### `current: T`

这是“这一阶段真正被处理的业务值”。

它会随着 pipeline 向前推进而不断变化。  
一个请求进入系统时可能是原始输入，经过若干节点后可能变成：

- 结构化后的内部请求对象
- 下游服务需要的请求格式
- 某种中间协议对象

因此 `Context<T>` 设计上允许 payload 变形，但不允许请求身份断裂。

#### `controller: Arc<Controller>`

这是请求生命周期的统一控制平面。

它承载的不是业务信息，而是运行时控制能力：

- request id
- stop / kill
- `stop_generating`
- 父子取消传播

之所以必须是 `Arc`，是因为同一请求的控制语义天然会被多方同时观察或触发：

- 图中的多个节点
- 响应流消费者
- 网络发送/接收任务
- 子请求或派生流

如果这里不是共享引用，那么“同一请求的取消状态”就会分裂成多个局部副本，整个链路就无法保持一致。

#### `registry: Registry`

这是请求级附加存储。

它解决的是一种非常常见但不适合放进业务 payload 的信息：

- 某阶段提取出的辅助结果
- 路由或鉴权附加信息
- 只想在少数后续阶段读取的上下游协作数据

它之所以直接内嵌在 `Context<T>` 里，而不是一开始就做成 `Arc<Registry>`，反映的是 `Context<T>` 的默认语义仍然是“请求沿图移动”而不是“很多地方同时持有整个上下文可变引用”。  
真正需要共享时，会在 `StreamContext` 这一层再转成 `Arc<Registry>`。

#### `stages: Vec<String>`

这是请求走过的阶段轨迹。

它看起来像个调试辅助字段，但设计上很有价值，因为 pipeline 不只是“算出结果”，还要回答：

- 这个请求经过了哪些逻辑段
- 现在卡在哪一层
- 是本地图阶段出了问题，还是网络段出了问题

因此 `stages` 不是额外锦上添花，而是在为“跨阶段可观察性”预留结构化入口。

### 关键函数真正表达的设计

#### `Context::new(current)`

它表达的是“从一个裸业务值，生成一次新的请求会话”。

创建时同时初始化：

- 新的 `Controller`
- 新的空 `Registry`
- 空阶段轨迹

这说明一次请求的生命周期是从 `Context::new` 这一刻才真正开始的。

#### `Context::with_id(current, id)`

它解决的是“请求跨网络边界后，如何恢复原有身份”。

远端 worker 收到的往往已经不是本地内存对象，而是解码出来的业务值。  
这时如果直接 `Context::new`，就会得到一个新的 request id，前后链路会断开。  
所以网络场景需要 `with_id` 来显式恢复同一个请求身份。

#### `Context::with_controller(current, controller)`

它表达的是“上下文身份可以被显式继承，而不是只能重新生成”。

这给更高级的场景留出了空间，例如：

- 外部系统已经决定了请求控制策略
- 某个节点想把已有控制器绑定到新 payload 上

#### `Context::rejoin(current, context)`

`rejoin` 的重点在于：**换 payload，不换请求**。

这个函数体现了 pipeline 的一个根本假设：

- 业务对象可以在阶段之间不断变化
- 但 request id、取消链、registry、stages 必须继续沿用

它把“业务变形”和“请求身份延续”拆开了。

#### `Context::transfer(new_current)`

这是整个上下文迁移设计里最核心的原语。

它把当前上下文拆成两件事：

- 旧的 payload
- 带新 payload 的同一个上下文壳

`map`、`try_map`、请求解包、网络重组、本地节点变换，本质上都建立在这个能力之上。  
也正因为有 `transfer`，pipeline 才能在保持 request identity 的同时自由做类型级变换。

#### `Context::into_parts()`

它是一个极简但很重要的设计信号：  
上下文可以被拆成“业务内容”和“纯上下文壳”。

这对网络层尤其关键，因为网络层经常需要：

- 单独序列化业务 payload
- 但继续保留或传播原 context 的控制信息

#### `map` / `try_map`

它们让“保留上下文的 payload 变换”成为标准操作，而不是每个节点手工重写一遍。

这类 API 的价值不在语法便利，而在于把一种高频需求固定成统一模式：

- payload 可以变
- context 不应丢
- 错误路径也要保持这个约束

#### `insert` / `insert_unique` / `get` / `clone_unique` / `take_unique`

这些函数表面是在转发 `Registry`，但设计上是在告诉调用者：

- 请求级附加数据应该跟着 `Context` 走
- 而不是散落在外部 side table 或全局状态里

### 设计意义总结

`Context<T>` 真正统一的是三件原本很容易被拆散的东西：

- 当前这一步要处理的业务值
- 整条请求链共享的生命周期控制
- 只属于这一次请求的附加状态

因此它不是“包装器”，而是 pipeline 里“请求”的正式定义。

### 相关 trait 实现的设计意义

`Context<T>` 下面还实现了一组看起来偏 Rust 语法层面的 trait：

- `Debug`
- `Deref`
- `DerefMut`
- `From<T>`
- `IntoContext<U>`
- `AsyncEngineContextProvider`

这些实现的目的，是让 `Context<T>` 同时具备两种身份：

- 对业务代码来说，它尽量像一个普通的 `T`
- 对 pipeline runtime 来说，它又必须始终是一个带 request context 的请求对象

#### `Debug`

`Debug` 只打印 `Context` 的 request id：

```rust
f.debug_struct("Context")
    .field("id", &self.controller.id())
    .finish()
```

这个设计避免了两个问题：

- 不要求业务 payload `T` 也必须实现 `Debug`
- 不把可能很大或敏感的请求体直接打进日志

所以它保留了最重要的追踪信息，也就是 request id，同时避免把 `Context<T>` 的日志输出和业务类型强绑定。

#### `Deref` / `DerefMut`

`Deref` 和 `DerefMut` 让 `Context<T>` 在很多场景下可以像 `T` 一样被读取或修改：

- `Deref` 返回 `&self.current`
- `DerefMut` 返回 `&mut self.current`

它们的设计意义是降低业务代码使用 `Context<T>` 的摩擦。  
节点如果只是想读写当前请求体，不需要每次都显式拆开 context。

但这里有一个关键点：  
通过 `Deref` / `DerefMut` 操作的只是 `current`，不是整个上下文。request id、controller、registry、stages 仍然留在 `Context<T>` 里，不会因为业务代码访问 payload 而丢失。

#### `From<T> for Context<T>`

`From<T>` 表达的是“裸业务值进入 pipeline 时，可以默认创建一个新的请求上下文”。

所以调用方可以写：

```rust
let ctx: Context<MyRequest> = request.into();
```

这相当于调用 `Context::new(request)`。  
它让入口侧把普通请求提升成 pipeline 请求对象时更自然，也明确了一个边界：裸 `T` 只有被包成 `Context<T>` 后，才真正进入 pipeline 的请求语义。

#### `IntoContext<U>`

`IntoContext<U>` 解决的是“如果 payload 本身能从 `T` 转成 `U`，那么整个 `Context<T>` 也应该能转成 `Context<U>`”。

它的实现是：

```rust
self.map(|current| current.into())
```

也就是只转换 `current`，保留原来的 controller、registry 和 stages。

这和 `map()` 的设计是一致的：  
请求体可以变形，但请求身份和生命周期不能因为类型转换而断掉。

#### `AsyncEngineContextProvider`

`AsyncEngineContextProvider` 是让 `Context<T>` 接入 engine / pipeline 统一上下文系统的关键。

它返回的是：

```rust
self.controller.clone()
```

也就是说，任何拿到 `Context<T>` 的节点或 engine，都可以通过统一接口取到：

- request id
- stop / kill 状态
- 取消控制能力
- 父子 context 链接能力

这也是为什么 `PipelineIO` 要求实现 `AsyncEngineContextProvider`。  
pipeline 图里流动的对象不只是数据，还必须能暴露请求级控制上下文。

综合来看，这组 trait 实现共同完成了一件事：

- `Context<T>` 对业务侧足够轻便，像 `T` 一样好用；
- `Context<T>` 对 runtime 侧足够严格，始终保留 request context 能力；
- payload 的读写、转换、日志、engine 调用都围绕同一个请求身份展开。

---

## 5.2 `StreamContext`

`StreamContext` 是流式输出版本的上下文视图。

它要解决的问题很具体：

- 当输出变成 `ResponseStream` 之后，响应项已经不再是“一个独占的 `Context<T>`”
- 但整条响应流仍然属于同一个请求，仍然需要同一套控制和元数据

所以 `StreamContext` 本质上是：**把 `Context<T>` 中与具体 payload 无关的那部分上下文抽出来，变成可被整条流共享的视图**。

### 字段

- `controller: Arc<Controller>`
- `registry: Arc<Registry>`
- `stages: Vec<String>`

### 为什么它和 `Context<T>` 不同

`Context<T>` 的语义更接近“某一刻正在被处理的请求对象”。  
而 `StreamContext` 的语义更接近“这整条响应流都要共享的请求会话”。

这就是为什么它：

- 去掉了 `current: T`
- 保留了 `controller`
- 把 `registry` 升级成 `Arc<Registry>`

因为一旦进入流式阶段，请求上下文往往要被多个响应项、多个消费者、多个异步任务并发引用。  
这时如果还保持 `Context<T>` 那种单一 payload 所有权模型，就会很别扭。

### 关键函数背后的意图

- `new(controller, registry)`：把单请求上下文转成可供整条流共享的上下文视图
- `get` / `clone_unique`：让流消费者仍然可以读取请求级附加信息
- `registry()`：显式暴露共享 registry，方便更复杂的流处理逻辑复用
- `stages()` / `add_stage()`：让流式阶段继续记账，而不是在“返回 ResponseStream”那一刻丢掉阶段轨迹

### 核心意义

`StreamContext` 解决的是：

- 请求级控制必须贯穿到响应流末端
- 但上下文不应该再绑死在某一个响应 item 上

所以它是 pipeline 从“单个请求对象”过渡到“请求所属整条流”的关键抽象。

### 相关 trait 实现的设计意义

`StreamContext` 后面实现了三类能力：

- `AsyncEngineContext`
- `AsyncEngineContextProvider`
- `From<Context<T>>`

它们的共同目标是：**让响应流虽然不再持有单个 payload，但仍然完整保留请求级控制语义。**

#### `AsyncEngineContext`

`StreamContext` 自己实现了 `AsyncEngineContext`，但每个方法都委托给内部的 `controller`：

```rust
fn stop(&self) {
    self.controller.stop();
}

fn is_stopped(&self) -> bool {
    self.controller.is_stopped()
}
```

这说明 `StreamContext` 并没有重新发明一套流自己的生命周期状态。  
它仍然复用原请求的 `Controller`。

这样做非常重要，因为响应流阶段和原请求阶段必须共享同一个取消语义：

- 调用方取消请求时，响应流也应该停止
- 响应流发送失败时，也可以反过来触发同一个 controller 的停止状态
- 子流或派生任务通过 `link_child()` 仍然能挂到同一条取消链上

所以 `StreamContext` 实现 `AsyncEngineContext` 的意义，不是让它变成一个新的 context，而是让“整条响应流”继续代表同一个 request context。

#### `AsyncEngineContextProvider`

`StreamContext` 也实现了 `AsyncEngineContextProvider`：

```rust
fn context(&self) -> Arc<dyn AsyncEngineContext> {
    self.controller.clone()
}
```

这和 `Context<T>` 的做法一致：  
外部不需要知道当前拿到的是单请求上下文还是流上下文，只要通过统一接口就能拿到请求级控制对象。

这对 `EngineStream<T>` / `ResponseStream<T>` 很关键。  
因为响应流在 pipeline 里也属于 `PipelineIO`，后续节点、网络层、日志和指标系统仍然需要从它身上拿到 request id 和取消状态。

#### `From<Context<T>> for StreamContext`

这段实现是 `Context<T>` 到 `StreamContext` 的关键转换点：

```rust
impl<T: Send + Sync + 'static> From<Context<T>> for StreamContext {
    fn from(value: Context<T>) -> Self {
        StreamContext::new(value.controller, value.registry)
    }
}
```

它表达的是：

- 丢掉 `current: T`
- 继承原来的 `controller`
- 继承原来的 `registry`
- 转成一份可供响应流共享的上下文视图

这正好对应从“请求处理阶段”进入“响应流阶段”的语义变化。

比如一个 `Context<ChatRequest>` 进入推理 engine 后，返回的是一条 token 流：

```text
Context<ChatRequest>
  current = ChatRequest(...)
  controller = req-123
  registry = ...

转换为：

StreamContext
  controller = req-123
  registry = Arc<Registry>
```

后续每个 token chunk 都不再单独拥有一个完整 `Context<ChatRequest>`，但它们都共享这个 `StreamContext`。

因此，这个转换的设计意义是：

- 请求 payload 已经完成了它的输入职责
- 响应流接下来只需要共享请求身份、取消控制和请求级附加状态
- pipeline 可以自然地从 `SingleIn<T>` 过渡到 `ManyOut<U>`

---

## 5.3 `Controller`

虽然 `Controller` 的完整实现细节不在这份文档里逐行展开，但从 `Context` 和 `StreamContext` 的使用方式看，它是整个请求生命周期的状态机。

从实现上看，它内部是：

- 一个稳定的 `id`
- 一个 watch 状态机
- 一组 child context 引用

这说明它不是简单 token，而是一个**可传播的请求控制中心**。

### `watch::Sender` / `watch::Receiver` 在这里做什么

`Controller` 里这两个字段是：

```rust
tx: Sender<State>,
rx: Receiver<State>,
```

它们来自：

```rust
use tokio::sync::watch::{Receiver, Sender, channel};
```

这里用的不是普通 `mpsc` 消息队列，而是 Tokio 的 `watch` channel。

`watch` channel 的语义可以理解成：

- 它只保存“最新状态”
- `Sender` 负责更新这个状态
- `Receiver` 负责读取当前状态，或者等待状态发生变化
- 可以有多个 receiver 同时观察同一个状态

这正好适合表示请求生命周期，因为请求生命周期不是一串业务消息，而是一个会变化的状态：

```rust
enum State {
    Live,
    Stopped,
    Killed,
}
```

#### 初始化时发生了什么

`Controller::new(id)` 里有这一行：

```rust
let (tx, rx) = channel(State::Live);
```

意思是创建一个初始状态为 `Live` 的 watch channel。

此时：

- `tx` 是状态发布端
- `rx` 是状态观察端
- 当前请求状态是 `Live`

所以一个新请求刚创建出来时，默认就是“还活着、没有停止、没有被 kill”。

#### `tx` 的作用：发布状态变化

当代码调用：

```rust
stop()
stop_generating()
kill()
```

最终都会通过 `tx.send(...)` 更新状态。

例如：

```rust
let _ = self.tx.send(State::Stopped);
```

或者：

```rust
let _ = self.tx.send(State::Killed);
```

这表示 `Controller` 在告诉所有观察者：

- 这个请求不再是 `Live`
- 它已经进入 `Stopped` 或 `Killed`

这里的 `tx` 不是在发送业务数据，也不是在发送 token。  
它只是在广播“请求生命周期状态变了”。

#### `rx` 的作用：读取或等待状态

`rx` 用在两类场景。

第一类是同步检查当前状态：

```rust
fn is_stopped(&self) -> bool {
    *self.rx.borrow() != State::Live
}

fn is_killed(&self) -> bool {
    *self.rx.borrow() == State::Killed
}
```

这里的 `borrow()` 是直接查看 watch channel 当前保存的最新状态。

所以：

- 如果当前还是 `Live`，`is_stopped()` 返回 `false`
- 如果当前已经是 `Stopped` 或 `Killed`，`is_stopped()` 返回 `true`
- 只有当前是 `Killed`，`is_killed()` 才返回 `true`

第二类是异步等待状态变化：

```rust
async fn stopped(&self) {
    let mut rx = self.rx.clone();
    loop {
        if *rx.borrow_and_update() != State::Live || rx.changed().await.is_err() {
            return;
        }
    }
}
```

这个函数的意思是：

1. 先 clone 一个 receiver
2. 看当前状态是不是已经不是 `Live`
3. 如果已经停止，就直接返回
4. 如果还活着，就 `await rx.changed()`
5. 等有人通过 `tx.send(...)` 改状态后，再重新检查

所以 `stopped().await` 可以被某个 task 用来挂起等待：

```text
只要请求还 Live，就继续等；
一旦请求进入 Stopped 或 Killed，就醒过来。
```

`killed().await` 也是类似逻辑，只是它等的是状态变成 `Killed`。

#### 为什么这里要 clone `rx`

在 `stopped()` / `killed()` 里会写：

```rust
let mut rx = self.rx.clone();
```

这是因为 `watch::Receiver` 可以被 clone。  
每个等待者都可以拿到自己的 receiver 句柄，各自独立等待同一个状态源。

这对 pipeline 很重要，因为同一个请求可能有多个异步任务都在关心取消状态：

- 生成 token 的任务
- 网络发送响应的任务
- 下游子请求
- 健康检查或清理任务

它们都不应该互相抢同一个消息。  
这也是为什么这里不用 `mpsc`：`mpsc` 更像“消息被某个消费者取走”，而 `watch` 更像“很多观察者都能看到同一个最新状态”。

#### 为什么不用 `oneshot`

`oneshot` 只能表达“一次性完成”。  
但这里的状态至少有三种：

- `Live`
- `Stopped`
- `Killed`

并且代码既需要同步查询当前状态，也需要异步等待状态变化。  
`watch` 同时满足这两点：

- 可以随时 `borrow()` 当前状态
- 可以 `changed().await` 等待后续变化

所以 `watch` 比 `oneshot` 更适合做请求生命周期状态机。

#### 一个完整例子

假设一个请求正在流式生成 token：

```text
State = Live
```

这时可能有两个 task：

- task A：模型推理，持续生成 token
- task B：网络层，把 token 发给客户端

它们都可以通过 `controller.context()` 拿到同一个请求控制对象，并观察同一个 watch 状态。

如果客户端断开连接，网络层调用：

```rust
controller.stop_generating();
```

内部会做：

```rust
self.tx.send(State::Stopped);
```

于是：

- `is_stopped()` 之后会返回 `true`
- 正在 `stopped().await` 的 task 会醒来
- 后续生成逻辑可以停止继续产出 token

如果发生更强的终止，比如 runtime 要强制中断，则调用：

```rust
controller.kill();
```

内部会做：

```rust
self.tx.send(State::Killed);
```

于是：

- `is_stopped()` 返回 `true`
- `is_killed()` 返回 `true`
- 正在 `killed().await` 的 task 会醒来

所以这两个 watch 端点的核心意义是：

- `tx`：由控制方发布请求生命周期变化
- `rx`：由任意观察方读取或等待生命周期变化

这让一次请求的取消/终止状态可以在多个节点、多个流、多个异步任务之间保持一致。

### 它负责什么

- 生成和持有 request id
- 暴露 `is_stopped` / `is_killed`
- 提供 `stop()`、`kill()`、`stop_generating()`
- 支撑 `link_child()` 建立父子取消链
- 实现 `AsyncEngineContext`

### 为什么要把这些能力集中在一个对象里

因为请求生命周期如果分散在多个辅助对象中，会立刻出现一致性问题：

- 日志里用一个 id
- 网络层取消用另一个 token
- 子流再自己维护第三种结束状态

最终结果就是“这几个东西名义上都代表同一请求，但状态不同步”。

`Controller` 的设计，就是把这些本该属于一次请求的控制语义收口到同一个地方。

### `stop`、`kill`、`stop_generating` 为什么要区分

这不是命名细节，而是在区分不同层级的终止语义：

- `stop_generating` 更偏向“别再继续产出后续结果了”
- `stop` 更偏向“这条请求链应当尽快停下”
- `kill` 则是更强的强制终止语义

如果只有一种取消信号，系统就很难表达“优雅收尾”和“立即中断”的区别。

### `link_child()` 的设计意义

这是分布式和流式场景里很关键的一点。  
一个请求在运行过程中可能派生出：

- 下游子请求
- 额外后台任务
- 流式发送/接收子上下文

如果父请求已经取消，这些子任务继续运行往往只会浪费资源，甚至造成幽灵流和悬挂连接。  
所以 `Controller` 通过 child 链接把取消传播显式建模出来。

### 设计意义

`Controller` 让“请求控制”从某个具体节点里的局部实现，提升成了整条 pipeline 链共享的基础设施。

---

## 5.4 `Registry`

`Registry` 是一个类型擦除的请求级 KV 仓库。

它想解决的是一个非常现实的问题：

- 有些信息确实属于“这一次请求”
- 但又不适合塞进 `T`
- 因为 `T` 是业务语义，`Registry` 里的东西更像运行时协作数据

例如：

- 某阶段提取出的辅助对象
- 某类只在后续少数节点才需要的中间结果
- 不方便污染公开请求结构体的内部协作信息

### 字段

- `shared_storage: HashMap<String, Arc<dyn Any + Send + Sync>>`
- `unique_storage: HashMap<String, Box<dyn Any + Send + Sync>>`

### 为什么它不是一个统一的 `HashMap<String, Box<dyn Any>>`

因为请求级附加数据在使用方式上天然分成两类，而这两类语义差异非常大。

#### `shared`

它表达的是：

- 多个阶段都可能读取
- 读的时候不应该夺走所有权
- 大家看到的是同一份共享对象

所以它使用 `Arc<dyn Any + Send + Sync>`，并通过：

- `insert_shared`
- `get_shared`
- `contains_shared`

来表达“共享读取”的意图。

#### `unique`

它表达的是另一种语义：

- 这份数据属于请求
- 但通常只会被后续某一步真正消费一次
- 允许先 clone 看一眼，但最终所有权可以被拿走

所以它使用 `Box<dyn Any + Send + Sync>`，并通过：

- `insert_unique`
- `take_unique`
- `clone_unique`
- `contains_unique`

来表达“单次消费或延迟转移”的意图。

这两类如果混在同一张表里，就会出现很糟糕的问题：

- 调用方不知道一个值拿出来后还能不能继续被别人用
- 代码里看不出它应该被共享，还是应该被消费掉
- 某些本该只取一次的数据会被误读为共享配置

所以 `Registry` 的重点不只是“类型擦除存储”，而是把**请求级附加状态的所有权语义** 也编码进接口里。

### 设计意图总结

`Registry` 让 pipeline 可以携带“不是业务 payload、但又属于这次请求”的信息，而且不会为了这些附加信息污染每一层的业务类型定义。

---

## 5.5 `PipelineError`

`PipelineError` 是 pipeline 自己的错误边界。

它的设计重点，不只是“列出一堆错误枚举”，而是在给 pipeline 划出自己的故障边界。

因为这个模块同时涉及：

- 本地图装配
- 请求/响应配对
- 流生命周期
- 编解码
- transport 连接
- 外部依赖

如果所有错误都直接塌缩成 `anyhow::Error`，那么你很难回答下面这些关键问题：

- 是图没接好，还是远端没连上
- 是业务引擎报错，还是 pipeline 自己的协议层报错
- 是正常断流导致的 detached，还是代码逻辑真的错了

### 它覆盖的问题类型

下面按错误所属的系统边界展开。  
这样看会更清楚：`PipelineError` 不是简单按底层库罗列错误，而是在标记“失败发生在 pipeline 的哪一层”。

#### 图装配边界

这一类错误回答的是：**本地图或分段图有没有被正确装配起来。**

##### `EdgeAlreadySet`

这个错误表达的是：**图的连接边只能设置一次。**

对应场景通常是：

- 某个 `Source` 已经连过一个下游；
- 又有人试图再次调用 `set_edge`；
- 这会让拓扑变得不确定。

它的设计意义是保护 pipeline 的图装配语义。  
很多节点里的边使用 `OnceLock`，这说明图连接是“构建期确定、运行期只读”的。  
如果允许重复设置边，那么同一个 source 到底应该把数据发给谁就不再清楚，运行期行为也会很难推理。

所以 `EdgeAlreadySet` 本质上是一个拓扑完整性错误。

##### `NoEdge`

这个错误表达的是：**某个 source 想发送数据，但它还没有连接下游。**

典型场景是：

- `Frontend.generate()` 已经把请求推入图；
- `Source::on_next()` 想把请求写到 edge；
- 但 `edge.get()` 发现没有设置过边。

这不是业务错误，而是图没有装配完整。  
它把“请求处理失败”和“pipeline 拓扑没有连好”区分开来。

如果没有 `NoEdge`，这类问题可能会表现成空指针式 panic 或更模糊的 generate 失败。  
单独建模后，调用方能更快定位到“图构建阶段漏了 link”。

##### `NoNetworkEdge`

这个错误和 `NoEdge` 类似，但语义更偏网络/分段边界。

它通常出现在 `SegmentSink` 这类可以“先建图、后 attach engine”的对象里：

- 图结构已经存在；
- 运行期请求到达了 segment sink；
- 但真正的 network edge、egress port 或 engine 还没有绑定。

它的设计意义是表达“网络分段尾部尚未接入真实执行端”。  
这和普通 `NoEdge` 的区别在于，`NoNetworkEdge` 更强调分布式/网络拼接场景里的延迟绑定失败。

#### 异步生命周期边界

这一类错误回答的是：**原本应该配对的请求方、响应方、流端点，是否在异步生命周期中提前断开了。**

##### `DetachedStreamReceiver`

这个错误表达的是：**请求发出去了，但最初等待响应的接收方已经不在了。**

在本地图里，`Frontend.generate()` 会创建一个 `oneshot`：

- sender 存进 `sinks` 表；
- receiver 由调用 `generate()` 的任务等待。

当响应回到 `Frontend::on_data()` 时，它会按 request id 找 sender，把响应发回给等待者。

如果这时找不到对应 sender，或者 receiver 已经被 drop，就说明请求/响应配对关系断开了。  
可能原因包括：

- 调用方 task 已经取消；
- generate future 被 drop；
- pipeline 内部配对逻辑有 bug；
- 响应回来得太晚，原等待者已经不存在。

这个错误的设计意义是把“响应无法交还给原始请求方”明确标出来。  
它不是远端业务失败，而是本地请求/响应回路的生命周期断裂。

##### `DetachedStreamSender`

这个错误表达的是另一侧断裂：**等待响应的一方还在等，但负责送回响应的 sender 已经没了。**

典型场景是：

- `Frontend.generate()` 创建了 `oneshot` 并等待 receiver；
- 但响应路径没有成功把数据送回来；
- sender 侧提前 drop，导致 receiver await 失败。

它和 `DetachedStreamReceiver` 是一对生命周期错误：

- `DetachedStreamReceiver`：响应想送回去，但接收者不在了；
- `DetachedStreamSender`：调用方还在等，但发送者不在了。

这两个错误一起表达的是：pipeline 的请求/响应配对不只是类型问题，也是异步任务生命周期问题。

#### 协议编码边界

这一类错误回答的是：**pipeline 对象和网络字节之间的转换是否成功。**

##### `SerializationError(String)`

这个错误表达的是：**pipeline 对象转成字节失败。**

远程调用必须把控制头、请求体、响应 item 等对象序列化后才能跨 transport 发送。  
如果序列化失败，说明本地对象无法变成协议层可传输的 bytes。

它的设计意义是把“编码前失败”从 transport 发送失败里分离出来。  
这样排查时可以知道：

- 不是连接断了；
- 不是远端拒绝；
- 而是本地对象本身无法被编码。

##### `DeserializationError(String)`

这个错误表达的是：**收到的字节无法恢复成预期的 pipeline 对象。**

典型场景：

- 远端收到 request payload，但无法解析 `RequestControlMessage`；
- 前端收到 response bytes，但无法还原成响应 item；
- 消息格式和当前协议预期不一致。

它和 `SerializationError` 是协议边界的两面：

- serialization 失败发生在发出前；
- deserialization 失败发生在收到后。

单独建模的意义是让系统能区分“我发不出去”和“我读不懂对方发来的内容”。

##### `TwoPartCodec`

这个错误表达的是：**pipeline 的 two-part 编解码协议失败。**

当前远程请求经常被编码成两段：

- header / control message
- data / payload

`TwoPartCodec` 出错说明消息帧本身不符合协议预期。  
这和 `SerdeJsonError` 不同：

- `TwoPartCodec` 更偏帧结构、大小、I/O、校验；
- `SerdeJsonError` 更偏 JSON 内容解析。

它内部还有更细的 `TwoPartCodecError`：

- `Io`：底层读写失败；
- `MessageTooLarge`：消息超过允许大小；
- `InvalidMessage`：帧结构非法；
- `ChecksumMismatch`：校验失败。

所以这个错误主要用于定位“字节帧协议层”的失败。

##### `SerdeJsonError`

这个错误表达的是：**JSON 序列化/反序列化失败。**

很多控制消息和请求/响应包装层使用 JSON。  
如果 JSON 解析失败，说明 bytes 已经到了 JSON 层，但内容不符合目标 Rust 类型的结构。

它和 `SerializationError` / `DeserializationError` 有重叠语义，但这里保留底层 `serde_json::Error`，可以让调用方看到更具体的 JSON 解析原因。

#### 远程控制与连接边界

这一类错误回答的是：**远程调用在控制平面、地址语义或 streaming 连接建立阶段是否失败。**

##### `ControlPlaneRequestError(String)`

这个错误表达的是：**向控制平面发请求失败。**

在分布式场景里，pipeline 不只是传业务流，还要和控制平面或 request plane 协作，例如：

- 发现目标 portname；
- 发送 work request；
- 触发某种控制面路由或管理请求。

如果这一步失败，业务 engine 甚至可能还没有被调用。  
所以它需要独立于 `GenerateError` 和普通 connection error。

它的设计意义是告诉调用方：失败发生在“请求进入远端执行链路之前”的控制面阶段。

##### `ConnectionFailed(String)`

这个错误表达的是：**streaming 连接建立失败。**

它通常不是“业务请求处理失败”，而是响应平面或数据平面没有接起来。  
例如：

- 前端已经注册了响应流；
- 远端尝试根据 `ConnectionInfo` 回连；
- 但 socket、subject 或 transport 连接无法建立。

单独建模它，是因为远程 streaming 调用里“请求发出”和“响应流建立”是两个不同阶段。  
`ConnectionFailed` 明确指向后者。

##### `InvalidPortnameFormat`

这个错误表达的是：**portname 地址格式不符合约定。**

错误信息里写明了期望格式：

```text
namespace/servicegroup/portname
```

它的设计意义是把“路由地址语义错误”和“路由不到目标”区分开。  
如果格式本身就是错的，就不应该继续走 discovery、网络请求或 portname lookup。

这类错误通常应该尽早返回，因为它属于调用方传入的 portname 标识不合法。

#### 业务执行边界

这一类错误回答的是：**请求是否已经进入真实业务 engine，并在业务执行阶段失败。**

##### `GenerateError(Error)`

这个错误表达的是：**真实业务 engine 的 `generate()` 调用失败。**

它和上面的图连接、编解码、transport 错误不同。  
到这里时，说明 pipeline 已经成功把请求送到了某个 engine，但 engine 自己在处理过程中返回了错误。

它的设计意义是保留一条清晰边界：

- pipeline 基础设施失败；
- 业务 engine 执行失败。

`GenerateError(Error)` 把后者包装起来，让上层仍然能通过 `PipelineError` 统一处理，但不会把业务错误误判成图或网络错误。

#### NATS / 消息系统依赖边界

这一类错误回答的是：**基于 NATS 的 request plane、work queue 或消息流依赖是否失败。**

##### `NatsConnectError`

这个错误表达的是：**连接 NATS 服务失败。**

这是更底层的依赖可用性问题。  
如果连接都无法建立，那么 stream、consumer、publish、batch 等后续操作都不会发生。

单独建模它，能让上层区分“NATS 服务不可达”和“NATS 内部某个操作失败”。

##### `NatsRequestError`

这个错误表达的是：**通过 NATS 发起 request/response 请求失败。**

它对应 NATS JetStream context 的 request 操作。  
设计上它保留了底层 NATS 的错误类型，使 pipeline 能告诉上层：失败发生在 NATS request 这一步，而不是普通 publish、subscribe 或 stream 管理。

##### `NatsGetStreamError`

这个错误表达的是：**获取已有 NATS stream 失败。**

这通常属于 NATS 资源发现或资源访问问题。  
它独立出来的意义是区分：

- stream 不存在或无法获取；
- stream 创建失败；
- stream 中 consumer 操作失败。

这些都属于 NATS，但排查方向完全不同。

##### `NatsCreateStreamError`

这个错误表达的是：**创建 NATS stream 失败。**

如果 pipeline 需要某个 stream 承载请求或响应，但创建失败，那么后续消费者、发布者都无法正常工作。

它和 `NatsGetStreamError` 分开，是为了区分“找不到/拿不到已有资源”和“尝试创建资源失败”。

##### `NatsConsumerError`

这个错误表达的是：**创建或访问 NATS consumer 失败。**

consumer 是从 stream 中消费消息的对象。  
在基于 NATS 的 request plane 或 work queue 里，consumer 失败通常意味着 worker 无法从队列里拿到任务。

它的设计意义是把“消息流存在，但消费端不可用”单独暴露出来。

##### `NatsBatchError`

这个错误表达的是：**批量拉取 NATS 消息失败。**

它比 `NatsConsumerError` 更具体：consumer 可能已经存在，但 pull batch 时失败。

这对排查很有用，因为它说明故障发生在实际 dequeue / batch pull 阶段，而不是 stream 或 consumer 的创建阶段。

##### `NatsPublishError`

这个错误表达的是：**向 NATS 发布消息失败。**

它通常出现在请求投递、响应投递或控制消息发送时。  
独立出来的意义是明确：对象可能已经成功序列化了，但底层 broker 没有接受这次 publish。

##### `NatsSubscriberError`

这个错误表达的是：**订阅 NATS subject 失败。**

它对应 subscribe 阶段。  
在响应流或控制消息依赖 subscription 的场景里，订阅失败意味着当前节点无法接收后续消息。

它的设计意义是把“接收路径建立失败”从“发送路径失败”中拆出来。

##### `NatsError`

这是一个更泛化的 NATS 错误包装：

```rust
Box<dyn std::error::Error + Send + Sync>
```

它的设计意义类似 `Generic`，但范围限定在 NATS 相关错误。  
当某个 NATS 错误还没有被细分成具体变体，或者来自更通用的 NATS API 时，可以先通过它进入统一的 `PipelineError`。

#### 运行时外部依赖边界

这一类错误回答的是：**pipeline 依赖的本机环境、指标系统、KV 存储是否可用。**

##### `LocalIpAddressError`

这个错误表达的是：**获取本机 IP 地址失败。**

这听起来不像 pipeline 核心逻辑，但在分布式 transport 里很重要。  
因为节点可能需要把自己的地址写进 `ConnectionInfo`，让远端知道该回连到哪里。

如果本地 IP 无法确定，那么响应平面可能根本没法生成可用的回连信息。  
所以它被纳入 pipeline 错误边界。

##### `PrometheusError`

这个错误表达的是：**指标注册或指标操作失败。**

pipeline 的远程入口、work handler、request plane 都会挂指标。  
虽然指标不是业务路径本身，但在当前实现中它属于 portname 运行时的一部分。

单独建模它，可以让初始化或 metrics wiring 的失败不要被误认为业务 generate 失败。

##### `KeyValueError(String, String)`

这个错误表达的是：**NATS KV 操作失败。**

两个字段通常可以理解为：

- 具体错误描述；
- bucket 名称。

它的设计意义是让 KV 存储失败带上 bucket 上下文。  
否则只知道“KV 出错”，但不知道是哪个 bucket 出错，排查分布式配置或服务发现问题会很困难。

#### 容量与降级边界

这一类错误回答的是：**服务当前是否还能接收新请求。**

##### `ServiceOverloaded(String)`

这个错误表达的是：**服务当前过载，暂时无法接收新请求。**

它和普通连接失败或业务失败不同。  
过载通常是一个可恢复状态：

- 当前所有实例都忙；
- 队列满；
- 调度层认为没有可用 worker；
- 后续重试可能成功。

单独建模它的意义是给上层提供更准确的处理策略。  
例如上层可以选择：

- 返回 503；
- 做重试；
- 换 worker；
- 触发限流或扩容。

#### 演化兜底边界

这一类错误回答的是：**当前故障是否还没有被建模成足够具体的 pipeline 错误。**

##### `Generic(String)`

这是一个兜底错误。

它的存在通常说明：

- 某个错误场景还没有被抽象成更精确的 pipeline 错误；
- 或者当前调用点只需要传出一段人类可读的错误描述；
- 或者代码还处在演化阶段，暂时不想为了一个低频场景扩展枚举。

从设计上看，它不是最理想的错误类型。  
如果某个 `Generic` 场景在系统里越来越重要，就应该把它升级成更具体的错误变体。  
但在 pipeline 这种同时连接图、网络、transport、外部依赖的模块里，保留一个兜底出口可以避免临时错误被硬塞进不合适的分类。

### 这些错误分类背后的整体设计

按类型分类后，可以看到这些错误大体覆盖了 pipeline 的几条关键边界：

- **图装配边界**：`EdgeAlreadySet`、`NoEdge`、`NoNetworkEdge`
- **异步生命周期边界**：`DetachedStreamReceiver`、`DetachedStreamSender`
- **协议编码边界**：`SerializationError`、`DeserializationError`、`TwoPartCodec`、`SerdeJsonError`
- **远程控制/连接边界**：`ControlPlaneRequestError`、`ConnectionFailed`、`InvalidPortnameFormat`
- **业务执行边界**：`GenerateError`
- **外部依赖边界**：NATS、KV、IP、Prometheus
- **容量和降级边界**：`ServiceOverloaded`
- **演化兜底边界**：`Generic`、`NatsError`

这样设计的好处是，调用方看到错误时，不只是知道“失败了”，还能大致知道失败发生在 pipeline 的哪一层。

### `PipelineErrorExt`

它解决的是一个常见摩擦点：  
很多地方为了方便会先把错误提升成 `anyhow::Error`，但上层有时又希望拿回 pipeline 级语义。

因此它提供：

- `try_into_pipeline_error`
- `either_pipeline_error`

来做“尽量恢复结构化错误”的桥接。

### 设计意义

`PipelineError` 的真正价值在于：

- 让 pipeline 既能作为独立子系统表达自己的故障模型
- 又不会把所有底层错误信息抹平成一团不可区分的字符串

所以它不是“错误枚举表”，而是 pipeline 的运行时失败分类系统。

---

## 六、本地图执行层

本地图执行层最容易让人困惑的地方是：它看起来像一组 `Source` / `Sink` / `Edge` 的低级接口，但真正想表达的是一个“**请求正向走、响应反向回**”的双向图。

先把它想成一条本地 RPC 链路：

```text
调用方
  │ generate(request)
  ▼
Frontend
  │ 正向请求路径：In
  ▼
PipelineNode / PipelineOperator / ...
  │
  ▼
ServiceBackend
  │ 调用真实业务 engine
  ▼
真实 engine
  │ 产出响应 Out
  ▼
ServiceBackend
  │ 反向响应路径：Out
  ▼
PipelineOperator / ...
  │
  ▼
Frontend
  │ 根据 request id 找回最初等待者
  ▼
调用方拿到 response
```

这里最关键的一点是：  
**pipeline 内部不是简单函数嵌套调用，而是一个显式连接的图；但图入口 `Frontend` 又把它包装回普通 `AsyncEngine::generate()` 的形状。**

这就是第六节所有抽象的核心目的：

- `Source` / `Sink` / `Edge`：定义图里怎么连线、怎么推数据；
- `Frontend`：把外部 `generate()` 调用转换成图里的正向请求，并等待反向响应；
- `ServiceBackend` / `SegmentSink`：在图尾调用真实 engine，然后把响应推回去；
- `PipelineNode`：处理只需要单向变换的简单节点；
- `Operator` / `PipelineOperator`：处理需要同时理解请求和响应的双向节点。

如果只看单个 trait，会觉得抽象很多；但把它们放在一起看，它们是在解决一个问题：

- 如何把一个普通 engine 调用拆成可组合的图；
- 如何让图中每段都能独立插入处理逻辑；
- 如何在最后仍然让调用方看到一个普通的 `generate()`。

### 为什么图里会有很多阶段

最简单的情况确实不需要很多阶段。  
如果只是“前端接收请求，交给后端 engine，拿到响应”，最小图可以非常短：

```text
Frontend
  │ request
  ▼
ServiceBackend
  │ engine.generate(request)
  ▼
Frontend
```

这条路径已经能完成一次请求响应。

第六节之所以把图设计成可以有很多阶段，不是因为每个请求都必须经过很多节点，而是因为真实系统里经常需要在“前端”和“后端引擎”之间插入额外处理。

这些处理可能包括：

- 请求校验：检查参数、模型名、portname 是否合法；
- 请求归一化：把外部 API 请求转成内部 engine 请求；
- 路由选择：根据模型、worker、负载或地址决定送到哪里；
- 指标和 tracing：记录每个阶段的耗时和 request id；
- batching：把多个请求合并后再交给后端；
- 协议适配：把下游响应格式转回上游期望的格式；
- 网络分段：当后端不在本进程时，把本地调用转成远程调用。

所以更复杂的图可能长这样：

```text
Frontend
  │ RawRequest
  ▼
ValidateNode
  │ ValidRequest
  ▼
NormalizeNode
  │ InternalRequest
  ▼
RoutingOperator
  │ RoutedRequest
  ▼
ServiceBackend / Egress
  │ ResponseStream
  ▼
PostprocessOperator
  │ ExternalResponseStream
  ▼
Frontend
```

如果没有这套图模型，这些逻辑很容易全部堆进 `Frontend` 或 `ServiceBackend`：

```text
Frontend {
  validate
  normalize
  route
  metrics
  network
  call_backend
  postprocess
}
```

这样会带来几个问题：

- 每个阶段很难单独测试；
- 想替换路由或预处理逻辑时会影响整个入口；
- 本地调用和远程调用容易走出两套接口；
- 响应后处理很难和请求预处理成对维护。

pipeline 的做法是把这些处理拆成节点：

- 每个节点只负责一段语义；
- 节点之间用 `Source` / `Edge` / `Sink` 连接；
- 最终仍然通过 `Frontend` 包装成 `generate()`。

因此，“很多阶段”不是必需路径，而是可组合能力。  
简单场景可以只有 `Frontend -> Backend`，复杂场景才把中间阶段接起来。

## 6.1 `Source<T>` / `Sink<T>` / `Edge<T>`

这是整个图模型的最小原语。  
但它们的价值不在“拆成三个 trait/struct”，而在于把 pipeline 的本地图执行语义刻意压成了一种非常受控的形状。

这套形状的核心假设是：

- 节点负责定义自己对输入的处理方式
- 边只负责连接
- 数据不应该被任意对象随手塞进图里

也就是说，`pipeline` 不是一个“任何地方都能拿到某个节点然后随便调用”的调用网，而是一个显式连线的图。

可以用一个最小例子理解：

```text
Source<A> -- Edge<A> --> Sink<A>
```

这句话表示：

- source 能发出 `A`
- sink 能接收 `A`
- edge 把二者接起来

它和直接写：

```rust
sink.on_data(a)
```

的区别在于：pipeline 希望“谁能把数据送给谁”是图装配结果，而不是任意代码随手调用的结果。

### `Source<T>`

关键函数：

- `on_next(data, Token)`
- `set_edge(edge, Token)`
- `link(sink)`

`Source<T>` 表达的是“这个对象能把 `T` 继续推进给下游”。

它的重要之处在于，推进动作被标准化了：

- 不是每个节点自己发明一套 `push` / `emit` / `forward`
- 而是统一收敛成 `on_next`

`link(sink)` 则把图构建固定成一种非常直接的语法糖：

- 创建一条 `Edge`
- 把这条边设到 source 上
- 返回 sink 以便继续链式构图

因此 `Source<T>` 既是“运行期发数据的接口”，也是“构建期声明下游连接的接口”。

也可以把 `Source<T>` 理解成“这个节点在某个方向上的输出端口”。  
比如：

- `Frontend` 在请求方向上是 `Source<In>`
- `ServiceBackend` 在响应方向上是 `Source<Resp>`
- `PipelineNode<In, Out>` 在处理完输入后是 `Source<Out>`

同一个结构体可能在某个方向是 source，在另一个方向又是 sink。  
这正是第六节容易绕的地方：`Source` / `Sink` 不是给“对象整体”贴永久身份，而是在描述它在某条路径、某个数据类型上的角色。

### `Sink<T>`

关键函数：

- `on_data(data, Token)`

它表达的是“这个对象愿意承接上游送来的 `T` 并继续处理”。

设计上它只关心一件事：  
当一份数据真实到达这里时，该怎么处理。

换句话说：

- `Source` 更偏向拓扑中的发送端语义
- `Sink` 更偏向拓扑中的承接端语义

同样，`Sink<T>` 可以理解成“这个节点在某个方向上的输入端口”。  
例如：

- `ServiceBackend` 在请求方向上是 `Sink<Req>`
- `Frontend` 在响应方向上是 `Sink<Out>`
- `PipelineNode<In, Out>` 在请求方向上是 `Sink<In>`

所以本地图其实不是“对象 A 调对象 B”那么简单，而是很多节点按端口连接：

```text
请求方向：Source<Req> -> Sink<Req>
响应方向：Source<Resp> -> Sink<Resp>
```

### `Edge<T>`

字段：

- `downstream: Arc<dyn Sink<T>>`

关键函数：

- `new(downstream)`
- `write(data)`

`Edge<T>` 被设计得很薄，这是刻意的。  
它不做业务逻辑，不做转换，不持有复杂状态，只表达一件事：

- “把这份 `T` 交给哪个下游 sink”

这样做的好处是，图拓扑和节点逻辑被拆得很开：

- 节点负责语义
- 边负责连接

如果边本身也塞进一堆业务逻辑，图结构就会变得很难推理。

`Edge<T>` 里只有一个：

```rust
downstream: Arc<dyn Sink<T>>
```

这说明它只记录“下游是谁”。  
真正的数据处理仍然发生在：

```rust
downstream.on_data(data, Token)
```

因此 edge 是一个“拓扑对象”，不是“处理对象”。  
这能让图的结构和每个节点的业务逻辑保持分离。

### 为什么要有 `private::Token`

这个设计非常关键，因为它实际上是在防止“模块外绕过拓扑直接驱动节点”。

如果外部任何代码都能随手调用 `on_next` / `on_data`：

- 图的连接关系就不再可信
- 某些节点可能被绕开
- 某些请求可能凭空出现在图中间

`private::Token` 的作用就是把这些底层接口锁回模块内部，只允许通过合法的图装配和官方入口触发。  
所以它不是语法技巧，而是在维护图模型的完整性。

可以把 `Token` 理解成“内部调用许可证”。  
外部用户能做的是：

```rust
source.link(sink)
frontend.generate(request)
```

而不是绕过图直接调用：

```rust
sink.on_data(...)
source.on_next(...)
```

这样 pipeline 才能保证数据流动符合它自己构建出来的拓扑。

---

## 6.2 `Frontend<In, Out>`

`Frontend` 是图入口的核心基础实现。

### 字段

- `edge: OnceLock<Edge<In>>`
- `sinks: Arc<Mutex<HashMap<String, oneshot::Sender<Out>>>>`

### 设计意图

它是本地图执行层最关键的桥，因为它要同时把两套世界接起来：

- 图内部的 `Source -> Edge -> Sink` 推进语义
- 图外部更熟悉的 `AsyncEngine::generate` 调用语义

如果没有 `Frontend`，调用方就得显式操作图内部连线和回包路径，这会把整套图模型直接暴露给上层业务。

所以它必须同时解决三个问题：

1. 如何把请求推进到图内
2. 如何在响应回来的时候把它交还给原调用方
3. 如何把这件事包装成标准 `generate(request) -> response`

这也是为什么它同时实现：

- `Source<In>`
- `Sink<Out>`
- `AsyncEngine<In, Out, Error>`

### 字段意义

#### `edge`

作用：

- 保存请求正向路径的唯一下游连接

为什么用 `OnceLock`：

- 图连接通常发生在构建阶段
- 一旦连好，运行期只读
- 不允许重复设置

#### `sinks`

作用：

- 用 request id 暂存“当前有哪些调用方正在等待各自的响应”

结构：

- `HashMap<String, oneshot::Sender<Out>>`

先注意一个容易误解的点：这里字段名叫 `sinks`，但它**不是** pipeline 图里的 `Sink` 节点集合，也不是下游列表。  
它更准确地说是 `Frontend` 内部的一张“未完成请求表”：

```text
request id -> 负责唤醒某个 generate() 调用的 oneshot sender
```

也就是说，每次外部调用：

```rust
frontend.generate(request).await
```

`Frontend` 都会先创建一对 `oneshot`：

```text
tx: oneshot::Sender<Out>
rx: oneshot::Receiver<Out>
```

然后：

- `tx` 被放进 `self.sinks`，key 是 `request.id()`
- `rx` 留在当前 `generate()` 调用里 await
- 请求继续沿 `edge` 进入 pipeline 图
- 等未来响应从反向路径回到 `Frontend::on_data()` 时，再用响应里的 request id 找回 `tx`
- `on_data()` 通过 `tx.send(data)` 唤醒最初那个等待中的 `generate()`

所以这张表保存的不是最终响应数据，而是“未来响应回来时应该送到哪里”的一次性投递口。

它体现的是 `Frontend` 的另一半职责：  
图入口不能只会把请求送出去，还必须能在图尾把响应重新接回来并配回原等待者。

因此请求路径和响应路径在 `Frontend` 里是同一个对象的两面：

- 正向上，它像一个 source
- 反向上，它像一个 response multiplexer / demultiplexer

字段本身的出处在：

- `lib/runtime/src/pipeline/nodes/sources.rs:10-12`

具体读写这张表的实现不在 `sources.rs` 主文件里，而是在拆出去的实现文件：

- 初始化空表：`lib/runtime/src/pipeline/nodes/sources/base.rs:8-14`
- 写入 `request.id() -> tx`：`lib/runtime/src/pipeline/nodes/sources/base.rs:58-68`
- 按 `ctx.id()` 取出并删除：`lib/runtime/src/pipeline/nodes/sources/base.rs:35-55`

### `generate()` 为什么能成立

`generate()` 能被包装出来，并不是因为图天然就是函数调用，而是因为 `Frontend` 在中间额外做了一层 request/response 配对。

具体来说：

1. 请求进入时，以 request id 建立一个一次性的等待槽位
2. 请求被送进图内
3. 图尾某处把响应送回 `Frontend` 时，再用同一个 request id 找回最初的等待者
4. 最终调用方看起来就像直接 await 了一个 engine

所以 `Frontend` 本质上是在把“异步图中的往返路径”折叠成“单个 `generate` 调用”。

可以把 `Frontend::generate()` 想成下面这个伪流程：

```text
generate(request):
  1. 创建一个 oneshot channel
  2. 用 request.id() 把 sender 存进 sinks 表
  3. 把 request 沿正向 edge 送进图
  4. 等待 receiver

on_data(response):
  1. 从 response.context() 取 request id
  2. 用 request id 到 sinks 表里找到 sender
  3. 把 response 发给当初那个 generate() 调用
```

也就是说，`Frontend` 让本来分离的两条路径重新配对：

```text
正向：generate(request) -> on_next(request) -> 下游
反向：上游响应 -> on_data(response) -> 唤醒 generate()
```

这里为什么一定要用 request id？

因为同一个 `Frontend` 可能同时处理很多个请求。  
请求 A、B、C 都从同一个入口出去，响应回来时顺序可能不是 A、B、C。  
如果没有 request id 做 key，`Frontend` 就不知道某个响应应该交还给哪个等待中的 `generate()`。

可以把并发时的 `sinks` 表想成这样：

```text
sinks = {
  "req-A" -> tx_A,
  "req-B" -> tx_B,
  "req-C" -> tx_C,
}
```

如果 `req-B` 的响应最先回来，`on_data()` 会用响应上下文里的 `"req-B"` 找到 `tx_B`，只唤醒请求 B 对应的那一次 `generate()`。  
请求 A、C 仍然留在表里继续等自己的响应。

因此 `sinks: HashMap<String, oneshot::Sender<Out>>` 的设计意义是：

- `String`：request id
- `oneshot::Sender<Out>`：这个请求最终响应的交付位置

它相当于是 `Frontend` 里的“未完成请求表”。

响应送达后使用的是 `remove(ctx.id())`，不是 `get(ctx.id())`。  
这表示一个请求只会被配对并完成一次；完成后表项立即删除，避免同一个 request id 被重复回包，也避免完成的请求继续占着内存。

### 关键函数

下面这些方法的定义并不都在 `sources.rs` 里。  
`sources.rs` 只定义结构体；`Frontend` 的基础实现主要在 `sources/base.rs`，`ServiceFrontend` / `SegmentSource` 的转发实现主要在 `sources/common.rs`。

#### `Default::default()`  

出处：`lib/runtime/src/pipeline/nodes/sources/base.rs:8-14`

初始化：

- 空 `edge`
- 空 `sinks`

也就是这里：

```rust
Self {
    edge: OnceLock::new(),
    sinks: Arc::new(Mutex::new(HashMap::new())),
}
```

#### `Source<In>::on_next`  

出处：`lib/runtime/src/pipeline/nodes/sources/base.rs:17-25`

行为：

- 取出 `edge`
- 若未设置，则报 `PipelineError::NoEdge`
- 否则把数据写给下游

这是 `Frontend` 在“请求正向路径”上的输出动作。  
外部调用 `generate()` 之后，请求真正进入图，就是靠这里的 `edge.write(data).await`。

#### `Source<In>::set_edge`  

出处：`lib/runtime/src/pipeline/nodes/sources/base.rs:27-32`

行为：

- 只允许设置一次
- 重复设置报 `EdgeAlreadySet`

这一步通常发生在构建 pipeline 图时：把 `Frontend` 的正向输出边接到下游节点。  
运行期只读这个 edge，不会反复改连接关系。

#### `Sink<Out>::on_data`  

出处：`lib/runtime/src/pipeline/nodes/sources/base.rs:35-55`

行为：

1. 从响应里取上下文
2. 拿到 request id
3. 在 `sinks` 中找对应 `oneshot::Sender`
4. 找到后把响应发回去
5. 找不到或发送失败则触发相关错误并 stop

关键代码含义是：

```rust
let tx = sinks
    .remove(ctx.id())
    .ok_or(PipelineError::DetachedStreamReceiver)?;

tx.send(data)
```

`remove(ctx.id())` 说明 `Frontend` 根据响应自己的 context id 找到当初保存的 sender。  
`tx.send(data)` 则把这个响应对象交回给正在 await 的 `generate()`。

这是整个本地图执行层最关键的回路之一。  
它证明了图虽然是由一堆 `Source` / `Sink` 组成的，但对最外层调用者来说仍然可以表现为普通 RPC。

#### `AsyncEngine::generate`  

出处：`lib/runtime/src/pipeline/nodes/sources/base.rs:58-68`

行为：

1. 创建 `oneshot::channel`
2. 把 sender 按 request id 存进 `sinks`
3. 调用 `on_next(request, Token)` 推进请求
4. 等待 receiver 得到响应

关键代码含义是：

```rust
let (tx, rx) = oneshot::channel::<Out>();
sinks.insert(request.id().to_string(), tx);
self.on_next(request, private::Token {}).await?;
Ok(rx.await.map_err(|_| PipelineError::DetachedStreamSender)?)
```

这段代码把一次外部 `generate()` 调用拆成了两半：

- 前半段：登记 `tx`，再把 request 推进图
- 后半段：等待 `rx`，直到未来某个 `on_data()` 用同一个 request id 把 response 发回来

这里最重要的设计意图是：  
**图的内部执行机制不需要和外部调用语义长得一样，但 `Frontend` 可以把两者桥接起来。**

更具体地说，`generate()` 的返回不是来自直接调用某个下游函数，而是来自未来某个时间点的反向路径回包。

这也是为什么这里用 `oneshot`：

- 对一次 `generate()` 调用来说，最终只需要交付一个 `Out`
- 这个 `Out` 可能本身是 `ManyOut<T>`，也就是一条响应流
- 但“把这条响应流对象交给调用方”这件事只发生一次

所以 `oneshot` 用来交付“最终响应对象”，不是用来传输响应流里的每个 item。

#### `ServiceFrontend` / `SegmentSource` 的方法从哪里来

`ServiceFrontend` 和 `SegmentSource` 自己没有重新写一套完整逻辑。  
它们都只有一个字段：

- `inner: Frontend<In, Out>`

出处：`lib/runtime/src/pipeline/nodes/sources.rs:15-22`

它们的方法由 `impl_frontend!` 宏统一生成，出处是：

- `new()`：`lib/runtime/src/pipeline/nodes/sources/common.rs:8-16`
- `Source<In>::on_next()` / `set_edge()` 转发到 `inner`：`lib/runtime/src/pipeline/nodes/sources/common.rs:18-27`
- `Sink<Out>::on_data()` 转发到 `inner`：`lib/runtime/src/pipeline/nodes/sources/common.rs:29-36`
- `AsyncEngine::generate()` 转发到 `inner`：`lib/runtime/src/pipeline/nodes/sources/common.rs:38-45`

所以读源码时可以按这个顺序看：

1. 先看 `sources.rs` 里的结构体字段，知道对象长什么样
2. 再看 `sources/base.rs`，理解 `Frontend` 真正怎么工作
3. 最后看 `sources/common.rs`，理解 `ServiceFrontend` / `SegmentSource` 只是包了一层 `inner`

### 设计意义总结

`Frontend` 本质上是一个“图入口 + 请求/响应配对器 + `AsyncEngine` 适配层”。

如果没有 `Frontend`，调用方就必须自己知道：

- 请求要往哪个 source 推；
- 响应会从哪个 sink 回来；
- 如何按 request id 配对；
- 如何处理等待方提前取消。

`Frontend` 把这些复杂性收起来，使上层只需要看到熟悉的：

```rust
engine.generate(request).await
```

---

## 6.3 `ServiceFrontend` 和 `SegmentSource`

这两个对象是 `Frontend` 的薄包装。

### 字段

- `inner: Frontend<In, Out>`

### 作用

两者几乎共享同一套实现，只是站位不同：

- `ServiceFrontend`
  - 更偏向“本地服务入口”
- `SegmentSource`
  - 更偏向“网络或分段管道入口”

### 实现方式

它们的大部分行为通过 `nodes/sources/common.rs` 的宏统一实现：

- `new()`
- `Source`
- `Sink`
- `AsyncEngine`

### 设计意义

这里的关键设计不是“省代码”，而是“保留语义名字”。

因为从机制上看，这两类入口都在做同一件事：

- 接收一个 pipeline 请求
- 记住谁在等响应
- 把请求推进给下游

但从架构位置看，它们表示的却是不同边界：

- `ServiceFrontend` 更像本地服务对外暴露的入口
- `SegmentSource` 更像某个分段图或远程落地段的起点

如果只保留一个通用名字，读代码时就很难一眼看出“这段入口是本地服务语义，还是网络拼接语义”。  
因此这里保留了不同类型名来承载架构角色。

可以简单区分为：

```text
ServiceFrontend:
  面向“这是一个本地服务入口”
  常用于把一条本地 pipeline 暴露成 ServiceEngine

SegmentSource:
  面向“这是一个分段入口”
  常用于网络 ingress 或子 pipeline 的起点
```

它们的实现相同，是因为底层机制确实相同；  
它们的名字不同，是因为架构语义不同。

---

## 6.4 `SinkEdge<Resp>`

`SinkEdge` 是响应方向的最小 `Source`。

### 字段

- `edge: OnceLock<Edge<Resp>>`

字段出处：

- `lib/runtime/src/pipeline/nodes/sinks.rs:13-15`

### 作用

它解决的问题虽然简单，但不可少：

- 正向请求路径天然有 `Frontend` 作为入口
- 反向响应路径也需要一个同样明确的“往上游送”的连接面

`SinkEdge` 就是在提供这个极简出口。

所以它实现的是：

- `Source<Resp>`

这里的名字容易绕：`SinkEdge` 不是一个 `Sink<T>` 实现，也不是接收请求的地方。  
它其实是被“sink-like 节点”内部持有的响应回送边。

更直白地说：

```text
ServiceBackend / SegmentSink:
  请求方向上：它们是 Sink<Req>，负责接住请求
  响应方向上：它们需要把 Resp 继续发回上游

SinkEdge<Resp>:
  就是它们内部专门负责“把 Resp 发回上游”的小组件
```

因此 `SinkEdge` 只关心响应方向，不关心真实业务 engine，也不关心请求怎么来的。

### 核心行为

- `on_next`：往上游写响应
- `set_edge`：设置唯一响应边

方法出处：

- `Default::default()`：`lib/runtime/src/pipeline/nodes/sinks/base.rs:7-13`
- `Source<Resp>::on_next()`：`lib/runtime/src/pipeline/nodes/sinks/base.rs:15-23`
- `Source<Resp>::set_edge()`：`lib/runtime/src/pipeline/nodes/sinks/base.rs:25-30`

`Default::default()` 创建的是一个空的响应边槽位：

```rust
Self {
    edge: OnceLock::new(),
}
```

`on_next()` 的行为和 `Frontend` 正向发请求时很像，只是方向换成了响应回流：

```rust
self.edge
    .get()
    .ok_or(PipelineError::NoEdge)?
    .write(data)
    .await
```

这段代码表示：

1. 先取出响应方向的 edge
2. 如果还没连响应边，就返回 `NoEdge`
3. 如果已经连好，就把 `Resp` 写给上游

`set_edge()` 也和 `Frontend` 的 `set_edge()` 一样，只允许设置一次：

```rust
self.edge
    .set(edge)
    .map_err(|_| PipelineError::EdgeAlreadySet)?;
```

这说明响应路径也是构建期连线、运行期只读，不是在每次请求时动态决定往哪里回。

本质上它是响应路径上的“出口插座”。  
它的存在说明一件事：在这套图模型里，响应不是凭空回去的，而是和请求一样，也要沿着明确的连接面流动。

为什么它叫 `SinkEdge` 会有点绕？  
可以这样理解：

- 它常被 sink-like 节点持有，比如 `ServiceBackend` / `SegmentSink`
- 这些节点在请求方向上是 `Sink<Req>`
- 但当它们拿到响应后，又需要变成响应方向上的 `Source<Resp>`

所以 `SinkEdge` 是“请求 sink 节点内部用来把响应发回去的 edge 持有者”。

例如 `ServiceBackend`：

```text
请求方向：
Frontend --Req--> ServiceBackend

响应方向：
ServiceBackend --Resp--> Frontend
```

`SinkEdge<Resp>` 就负责第二条箭头。

把它和 `Frontend` 放在一起看，会更清楚：

```text
请求方向：
Frontend.edge
  负责把 Req 从入口送到下游

响应方向：
ServiceBackend.inner: SinkEdge<Resp>
  负责把 Resp 从图尾送回上游
```

所以 `Frontend.edge` 和 `SinkEdge.edge` 是一对相反方向的连接面。  
前者负责请求进入图，后者负责响应离开图尾并回流。

---

## 6.5 `ServiceBackend<Req, Resp>`

这是图尾的本地执行节点。

### 字段

- `engine: ServiceEngine<Req, Resp>`
- `inner: SinkEdge<Resp>`

字段出处：

- `lib/runtime/src/pipeline/nodes/sinks.rs:17-20`

### 设计意图

图不能永远只在内部节点之间转来转去，它最终必须落到一个真实处理者上。  
`ServiceBackend` 就是在承担这层“图世界”和“真实业务引擎世界”之间的边界。

所以它同时承担两个角色：

- `Sink<Req>`：作为图尾接住请求
- `Source<Resp>`：把真实引擎的响应重新送回响应路径

### 关键函数

#### `from_engine(engine)`

出处：`lib/runtime/src/pipeline/nodes/sinks/pipeline.rs:7-14`

作用：

- 用真实引擎构造 backend

构造时做两件事：

```rust
Arc::new(Self {
    engine,
    inner: SinkEdge::default(),
})
```

- `engine`：保存真正执行业务逻辑的 engine
- `inner`：创建一个空的响应回送边，后面通过 `set_edge()` 接回上游

所以 `ServiceBackend` 创建出来时，请求执行端已经确定了；但响应往哪里回，还要靠 pipeline 构建时继续连边。

#### `Sink<Req>::on_data`

出处：`lib/runtime/src/pipeline/nodes/sinks/pipeline.rs:16-22`

行为：

1. 调用真实 `engine.generate(data)`
2. 得到响应
3. 调用 `on_next` 把响应继续送到上游

核心代码是：

```rust
let stream = self.engine.generate(data).await?;
self.on_next(stream, Token).await
```

这里有一个很重要的点：  
`engine.generate(data).await?` 的结果没有直接返回给最外层调用者，而是交给 `self.on_next(...)`。

原因是 `ServiceBackend` 位于图尾。  
它收到请求后确实会调用真实 engine，但响应仍然要回到 pipeline 的响应路径里，再一路回到 `Frontend::on_data()`，最后由 `Frontend` 根据 request id 唤醒最初的 `generate()`。

所以这里的执行链可以写成：

```text
ServiceBackend::on_data(req)
  -> engine.generate(req).await
  -> ServiceBackend::on_next(resp)
  -> SinkEdge::on_next(resp)
  -> response edge.write(resp)
  -> 上游节点 / Frontend::on_data(resp)
```

#### `Source<Resp>`

出处：`lib/runtime/src/pipeline/nodes/sinks/pipeline.rs:24-33`

行为：

- 直接委托给 `inner: SinkEdge<Resp>`

也就是：

```rust
self.inner.on_next(data, Token).await
self.inner.set_edge(edge, Token)
```

这说明 `ServiceBackend` 自己不直接保存响应边，而是把响应方向的 `Source<Resp>` 能力交给内部的 `SinkEdge` 实现。

因此它的两个角色是分开的：

```text
ServiceBackend.engine:
  负责真正处理 Req -> Resp

ServiceBackend.inner:
  负责把 Resp 沿响应路径送回去
```

### 核心意义

`ServiceBackend` 最重要的价值在于：  
业务引擎不用理解整张图的结构，只要继续实现标准 `AsyncEngine`；而图也不用知道业务引擎内部如何运行，只要在尾部调用它即可。

因此它是本地图执行层真正把“图模型”闭合起来的尾节点。

一个最小本地图可以这样理解：

```text
ServiceFrontend<Req, Resp>
  --Req-->
ServiceBackend<Req, Resp>
  --Resp-->
ServiceFrontend<Req, Resp>
```

真实执行发生在 `ServiceBackend`：

```rust
let stream = self.engine.generate(data).await?;
self.on_next(stream, Token).await
```

这两行对应两个阶段：

1. 请求正向到达图尾，调用真实 engine；
2. engine 产出的响应不直接返回给调用方，而是沿响应路径 `on_next` 回推。

所以 `ServiceBackend` 是一个双角色节点：

- 收请求时，它是 `Sink<Req>`
- 发响应时，它是 `Source<Resp>`

这也是为什么 `ServiceBackend` 文件在 `nodes/sinks/` 下面，但它同时实现了 `Source<Resp>`。  
这里的 “sink” 指的是它在请求方向上的位置：它是正向请求路径的末端。  
一旦真实 engine 产出响应，它又必须切换成响应方向的 source，把响应往回送。

这就是本地图执行层“请求正向、响应反向”的核心闭环。

---

## 6.6 `SegmentSink<Req, Resp>`

这是分段或网络场景下的尾节点。

### 字段

- `engine: OnceLock<ServiceEngine<Req, Resp>>`
- `inner: SinkEdge<Resp>`

字段出处：

- `lib/runtime/src/pipeline/nodes/sinks.rs:22-26`

### 和 `ServiceBackend` 的差别

- `ServiceBackend` 构造时就带 engine
- `SegmentSink` 可以先建图，后绑定 engine

这个差别看起来只是初始化时机不同，但背后对应的是两种很不同的部署/装配模式：

- 本地服务图通常在构建时就知道真实 backend 是谁
- 分段图或网络拼接图往往需要先把拓扑留出来，稍后再接上真实执行端或网络出口

### 关键函数

#### `new()`

出处：`lib/runtime/src/pipeline/nodes/sinks/segment.rs:7-10`

作用：

- 创建空的 `SegmentSink`

它只是返回 `Arc<Self::default()>`。  
真正的默认结构在：

- `lib/runtime/src/pipeline/nodes/sinks/segment.rs:19-26`

默认值是：

```rust
Self {
    engine: OnceLock::new(),
    inner: SinkEdge::default(),
}
```

这表示 `SegmentSink` 创建时：

- 真实 engine 还没有绑定
- 响应回送边也还是空的
- 但这个节点已经可以先放进 pipeline 拓扑里

#### `attach(engine)`

出处：`lib/runtime/src/pipeline/nodes/sinks/segment.rs:12-16`

作用：

- 绑定真实引擎

约束：

- 只允许绑定一次

核心代码是：

```rust
self.engine
    .set(engine)
    .map_err(|_| PipelineError::EdgeAlreadySet)
```

这里用 `OnceLock<ServiceEngine<Req, Resp>>` 的原因和 `edge: OnceLock<_>` 类似：  
这个执行端可以晚一点绑定，但绑定后就不应该被请求过程中随意替换。

如果重复 attach，会返回 `EdgeAlreadySet`。  
虽然错误名里叫 edge，但这里表达的是同一种“一次性绑定失败”的语义。

#### `Sink<Req>::on_data`

出处：`lib/runtime/src/pipeline/nodes/sinks/segment.rs:28-39`

行为：

1. 取出已绑定的 engine
2. 若没有则报 `NoNetworkEdge`
3. 调用 `generate`
4. 把响应经 `inner` 发回去

核心代码是：

```rust
let stream = self
    .engine
    .get()
    .ok_or(PipelineError::NoNetworkEdge)?
    .generate(data)
    .await?;
self.on_next(stream, Token).await
```

和 `ServiceBackend::on_data()` 相比，它多了一步：

```rust
self.engine.get().ok_or(PipelineError::NoNetworkEdge)?
```

也就是说，`SegmentSink` 收到请求时，必须先确认执行端已经 attach。  
如果还没 attach，请求无法继续落地执行，所以返回 `NoNetworkEdge`。

#### `Source<Resp>`

出处：`lib/runtime/src/pipeline/nodes/sinks/segment.rs:41-50`

行为：

- 和 `ServiceBackend` 一样，直接委托给 `inner: SinkEdge<Resp>`

也就是 `SegmentSink` 自己也不直接保存响应边，而是通过内部 `SinkEdge` 实现响应回送。

### 设计意义

`SegmentSink` 让图装配和执行端绑定这两件事可以解耦。  
这对分布式场景尤其重要，因为很多时候：

- 图的拓扑先确定
- 但远程执行端、network edge、或者真正 attach 的 engine 要稍后才能拿到

所以它服务的是“延迟绑定”的需求，而不是单纯再造一个 backend。

可以把 `ServiceBackend` 和 `SegmentSink` 的差别记成：

```text
ServiceBackend:
  创建时就知道 engine 是谁

SegmentSink:
  先占住图尾位置
  后续再 attach(engine)
```

这种延迟绑定在网络场景里很自然。  
比如一个 segment 先被创建出来，等某个远程 transport、egress、或者本地 engine 准备好后，再把它 attach 进去。  
如果请求在 attach 之前就到了，`SegmentSink` 会返回 `NoNetworkEdge`，明确告诉调用方“分段尾部还没接上”。

把 `SegmentSource` 和 `SegmentSink` 放在一起看，可以这样理解：

```text
SegmentSource:
  分段入口
  把请求推进某个 segment 内部

SegmentSink:
  分段尾部
  等真实 engine / 远端执行端 attach 后，负责落地执行并把响应回送
```

因此 `SegmentSink` 不是网络传输本身。  
它是在分段拓扑里预留出的“尾部承接点”：请求到这里以后，要么找到已 attach 的 engine 执行，要么明确报 `NoNetworkEdge`。

---

## 6.7 `Operator<UpIn, UpOut, DownIn, DownOut>`

`Operator` 是比 `PipelineNode` 更强的节点抽象。

### 它要解决什么问题

有些节点不是简单地把输入 `A` 变成输出 `B`。

它可能需要：

- 把上游请求 `UpIn` 变成下游请求 `DownIn`
- 调用下游引擎
- 再把下游响应 `DownOut` 变回上游响应 `UpOut`

这意味着它同时参与：

- 正向请求路径
- 反向响应路径

### 核心函数

- `generate(req, next)`
- `into_operator()`

### 设计意义

`Operator` 的关键不在“多了几个泛型参数”，而在它把一个很难表达的能力单独抽了出来：

- 节点不仅能改写请求
- 还能利用同一次调用的上下文参与响应改写

这与单向 map 型节点差别非常大。  
很多中间层逻辑天然需要这种双向感知能力，例如：

- 请求预处理后再把响应恢复成上游语义
- 在正向路径记录信息，反向路径再据此补全响应
- 某些代理/适配层同时理解上下游两端协议

所以 `Operator` 不是“更重的 `PipelineNode`”，而是在表达**双向耦合变换** 这类更高阶的节点能力。

一个典型例子是协议适配器：

```text
上游请求：OpenAIChatRequest
下游请求：InternalGenerateRequest

下游响应：InternalTokenChunk
上游响应：OpenAIChatChunk
```

这个节点不能只做：

```text
OpenAIChatRequest -> InternalGenerateRequest
```

它还必须在响应回来时做：

```text
InternalTokenChunk -> OpenAIChatChunk
```

这就是 `Operator<UpIn, UpOut, DownIn, DownOut>` 四个类型参数的意义：

- `UpIn`：上游交给 operator 的请求类型
- `DownIn`：operator 交给下游的请求类型
- `DownOut`：下游返回给 operator 的响应类型
- `UpOut`：operator 返回给上游的响应类型

它的 `generate(req, next)` 也正是在表达这个过程：

```text
1. 收到 UpIn
2. 转成 DownIn
3. 调用 next.generate(DownIn)
4. 拿到 DownOut
5. 转成 UpOut
```

所以 `Operator` 本质上是“包在下游 engine 外面的一层双向适配器”。

---

## 6.8 `PipelineOperator`

`PipelineOperator` 把一个 `Operator` 包装成真正可插进图里的节点。

如果说 `Operator` 是“我知道怎么把一组上下游协议互相转换”的业务逻辑，那么 `PipelineOperator` 就是“把这段业务逻辑接到 pipeline 图上的机械结构”。

它要解决的问题不是简单的：

```text
收到 A -> 变成 B -> 往后传
```

而是下面这种成对过程：

```text
正向请求：
上游给我 UpIn
  -> 我把它变成 DownIn
  -> 我调用下游那段图

反向响应：
下游返回 DownOut
  -> 我把它变成 UpOut
  -> 我再把 UpOut 送回上游
```

所以 `PipelineOperator` 的思想核心是：  
**它不是一条边上的 map，而是包住下游调用的一层双向适配器。**

源码里对这个设计有一段总说明，出处是：

- `lib/runtime/src/pipeline/nodes.rs:16-25`

那段注释说得很关键：`PipelineOperator` 同时参与请求路径和响应路径，因此它实际上有“两组 source/sink 面”。  
为了不把这两组面混在一起，代码提供了：

- `forward_edge()`：给请求正向路径使用
- `backward_edge()`：给响应反向路径使用

### 字段

- `operator: Arc<dyn Operator<...>>`
- `downstream: Arc<sources::Frontend<DownIn, DownOut>>`
- `upstream: sinks::SinkEdge<UpOut>`

字段出处：

- `lib/runtime/src/pipeline/nodes.rs:152-170`

### 字段意义

#### `operator`

- 真正的业务逻辑

更准确地说，`operator` 保存的是用户实现的双向转换策略：

```rust
operator: Arc<dyn Operator<UpIn, UpOut, DownIn, DownOut>>
```

出处：`lib/runtime/src/pipeline/nodes.rs:160-161`

它不直接关心图里的 `Source` / `Sink` 怎么连。  
它只关心一件事：

```text
给我 UpIn 和一个 next engine，
我负责调用 next，
最后返回 UpOut。
```

也就是 `Operator::generate(req, next)` 的语义，出处：

- `lib/runtime/src/pipeline/nodes.rs:95-120`

这也是 `Operator` 为什么需要四个类型参数：

```text
UpIn:
  上游进来的请求

DownIn:
  发给下游的请求

DownOut:
  下游返回的响应

UpOut:
  返回给上游的响应
```

这四个类型不是为了复杂而复杂，而是在类型层面把“上游协议”和“下游协议”分开。

#### `downstream`

- 这是一个内部 `Frontend`
- 它负责代表“下游那一段引擎”

这里之所以用内部 `Frontend`，是因为 `Operator` 自己也需要一种方式，把“调用下游并等待响应”继续包装成统一的 engine 语义。  
这说明 `PipelineOperator` 内部其实又嵌了一层局部请求/响应桥。

字段出处：

- `lib/runtime/src/pipeline/nodes.rs:163-165`

这是 6.8 最容易迷糊的地方：为什么 operator 里面又放了一个 `Frontend`？

原因是 `Operator::generate()` 的签名要求传进去一个 `next`：

```rust
next: Arc<dyn AsyncEngine<DownIn, DownOut, Error>>
```

也就是说，从 `Operator` 的视角看，下游不是“一堆 Source/Sink/Edge”，而是一个可以这样调用的 engine：

```text
next.generate(DownIn).await -> DownOut
```

但真实的 pipeline 下游仍然是一张图。  
所以 `PipelineOperator` 需要用一个内部 `Frontend<DownIn, DownOut>` 把“下游那段图”重新包装成 `AsyncEngine<DownIn, DownOut, Error>`。

这和最外层 `ServiceFrontend` 的作用很像，只是范围更小：

```text
外层 Frontend:
  把整张 pipeline 包装成 generate()

PipelineOperator.downstream:
  把 operator 后面的那段子图包装成 generate()
```

因此 `downstream` 不是“真正的下游节点”，而是 operator 眼中的“下游子图入口”。  
operator 调用它，就等价于调用 operator 后面接着的那段 pipeline。

#### `upstream`

- 这是一个 `SinkEdge`
- 它负责把最终响应继续回送给上游

字段出处：

- `lib/runtime/src/pipeline/nodes.rs:167-169`

`upstream` 的方向和 `downstream` 相反。  
`downstream` 是 operator 往下游发请求并等待响应的桥；`upstream` 是 operator 处理完响应后，把 `UpOut` 往上游送回去的边。

也就是说：

```text
downstream:
  面向下游
  类型是 DownIn -> DownOut
  用 Frontend 包成 AsyncEngine

upstream:
  面向上游
  类型是 UpOut
  用 SinkEdge 把响应写回上游
```

为什么 `upstream` 不是另一个 `Frontend`？

因为这里不需要“发一个请求再等待响应”。  
当 `operator.generate(...)` 已经拿到 `UpOut` 后，只需要把这个响应继续沿反向边送回上游。  
这个动作正好是 `SinkEdge<UpOut>` 的职责：作为响应方向的 `Source<UpOut>`，调用 `on_next(UpOut)` 往上游写。

### 关键函数

#### `new(operator)`

出处：`lib/runtime/src/pipeline/nodes.rs:179-186`

作用：

- 创建一个完整 `PipelineOperator`

内部自动创建：

- 下游 `Frontend`
- 上游 `SinkEdge`

核心代码是：

```rust
Arc::new(PipelineOperator {
    operator,
    downstream: Arc::new(sources::Frontend::default()),
    upstream: sinks::SinkEdge::default(),
})
```

这三件东西正好对应三个角色：

- `operator`：负责 UpIn/DownIn/DownOut/UpOut 的业务转换
- `downstream`：把下游子图包装成 `next.generate(...)`
- `upstream`：把最终 `UpOut` 回送给上游

#### `forward_edge()`

出处：`lib/runtime/src/pipeline/nodes.rs:188-195`

返回：

- `PipelineOperatorForwardEdge`

作用：

- 这是请求正向路径上的连接点

它返回的不是 `PipelineOperator` 自己，而是一个边视图：

```rust
Arc::new(PipelineOperatorForwardEdge {
    parent: self.clone(),
})
```

这个 `parent` 很重要：  
`PipelineOperatorForwardEdge` 自己不保存 operator/downstream/upstream，它只是拿着父 `PipelineOperator`，把正向路径上需要暴露的 `Sink<UpIn>` / `Source<DownIn>` 能力转发给父对象内部结构。

#### `backward_edge()`

出处：`lib/runtime/src/pipeline/nodes.rs:197-204`

返回：

- `PipelineOperatorBackwardEdge`

作用：

- 这是响应反向路径上的连接点

它同样只是一个边视图：

```rust
Arc::new(PipelineOperatorBackwardEdge {
    parent: self.clone(),
})
```

它存在的目的，是把响应路径上的 `Sink<DownOut>` / `Source<UpOut>` 从正向路径中分离出来。

#### `AsyncEngine::generate(req)`

出处：`lib/runtime/src/pipeline/nodes.rs:207-219`

行为：

- 调用 `operator.generate(req, self.downstream.clone())`

核心代码是：

```rust
self.operator.generate(req, self.downstream.clone()).await
```

这行代码说明 `PipelineOperator` 自己也表现成一个 `AsyncEngine<UpIn, UpOut, Error>`。  
上游看它时，只觉得它是：

```text
generate(UpIn) -> UpOut
```

但它内部实际上做的是：

```text
operator.generate(UpIn, downstream_frontend)
```

所以 `PipelineOperator` 是在把复杂的双向图接线，重新包装成上游可调用的 engine 形状。

### 接口流程和连线图

先看一张总图。  
这张图只表达接口角色和线的方向，不表达每个函数调用的细节：

```text
                              PipelineOperator
┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  请求正向面                                                                  │
│                                                                              │
│  上游请求 UpIn                                                               │
│       │                                                                      │
│       ▼                                                                      │
│  forward_edge: PipelineOperatorForwardEdge                                   │
│       │                                                                      │
│       │  Sink<UpIn>                                                          │
│       │  - on_data(UpIn)                                                     │
│       │  - 调 parent.generate(UpIn)                                          │
│       │                                                                      │
│       ▼                                                                      │
│  operator: Operator<UpIn, UpOut, DownIn, DownOut>                             │
│       │                                                                      │
│       │  operator.generate(UpIn, downstream_frontend)                         │
│       │                                                                      │
│       ▼                                                                      │
│  downstream: Frontend<DownIn, DownOut>                                        │
│       │                                                                      │
│       │  Source<DownIn>                                                      │
│       │  - on_next(DownIn)                                                   │
│       │  - 把 DownIn 送入 operator 后面的下游子图                             │
│       ▼                                                                      │
│  下游 pipeline / backend                                                     │
│                                                                              │
│  ───────────────────────────── 分界线 ─────────────────────────────          │
│                                                                              │
│  响应反向面                                                                  │
│                                                                              │
│  下游响应 DownOut                                                            │
│       │                                                                      │
│       ▼                                                                      │
│  backward_edge: PipelineOperatorBackwardEdge                                 │
│       │                                                                      │
│       │  Sink<DownOut>                                                       │
│       │  - on_data(DownOut)                                                  │
│       │  - 转交给 downstream.on_data(DownOut)                                │
│       │                                                                      │
│       ▼                                                                      │
│  downstream: Frontend<DownIn, DownOut>                                        │
│       │                                                                      │
│       │  Sink<DownOut>                                                       │
│       │  - 用 request id 找到等待中的 downstream.generate()                  │
│       │  - 把 DownOut 交还给 operator.generate(...)                          │
│       │                                                                      │
│       ▼                                                                      │
│  operator 继续执行                                                           │
│       │                                                                      │
│       │  把 DownOut 转成 UpOut                                                │
│       ▼                                                                      │
│  upstream: SinkEdge<UpOut>                                                   │
│       │                                                                      │
│       │  Source<UpOut>                                                       │
│       │  - on_next(UpOut)                                                    │
│       │  - 把 UpOut 沿响应路径送回上游                                       │
│       ▼                                                                      │
│  上游响应 UpOut                                                              │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

这张图里最关键的是三组接口：

| 结构 | 对外暴露的接口 | 它接什么 | 它送什么 | 真实委托给谁 |
|---|---|---|---|---|
| `PipelineOperatorForwardEdge` | `Sink<UpIn>` | 上游请求 `UpIn` | 最终触发 `UpOut` 回上游 | `parent.generate()` 和 `parent.upstream` |
| `PipelineOperatorForwardEdge` | `Source<DownIn>` | operator 产生的 `DownIn` | 下游请求 `DownIn` | `parent.downstream` |
| `PipelineOperatorBackwardEdge` | `Sink<DownOut>` | 下游响应 `DownOut` | 唤醒内部 `downstream.generate()` | `parent.downstream` |
| `PipelineOperatorBackwardEdge` | `Source<UpOut>` | operator 产生的 `UpOut` | 上游响应 `UpOut` | `parent.upstream` |
| `downstream: Frontend<DownIn, DownOut>` | `AsyncEngine<DownIn, DownOut>` | operator 发起的下游请求 | 下游响应 | 自己的 `sinks` 表和下游 edge |
| `upstream: SinkEdge<UpOut>` | `Source<UpOut>` | 已经算好的上游响应 | 上游响应路径 | 自己保存的 response edge |

再看一次完整调用的时序线图：

```text
上游节点
  │
  │ UpIn
  ▼
forward_edge.on_data(UpIn)
  │
  │ parent.generate(UpIn)
  ▼
PipelineOperator.generate(UpIn)
  │
  │ operator.generate(UpIn, downstream_frontend)
  ▼
Operator
  │
  │ 1. UpIn -> DownIn
  │
  │ 2. downstream_frontend.generate(DownIn)
  ▼
downstream Frontend
  │
  │ 3. 记录 request id -> oneshot sender
  │
  │ 4. DownIn
  ▼
下游子图 / backend
  │
  │ 5. DownOut
  ▼
backward_edge.on_data(DownOut)
  │
  │ 6. downstream_frontend.on_data(DownOut)
  ▼
downstream Frontend
  │
  │ 7. 用 request id 唤醒第 2 步的 generate()
  │
  │ 8. DownOut 回到 Operator
  ▼
Operator
  │
  │ 9. DownOut -> UpOut
  ▼
forward_edge.on_data(...) 继续
  │
  │ 10. parent.upstream.on_next(UpOut)
  ▼
upstream SinkEdge
  │
  │ 11. UpOut
  ▼
上游响应路径
```

注意第 2 步和第 6 步是一对：  
`downstream_frontend.generate(DownIn)` 先登记等待者，`downstream_frontend.on_data(DownOut)` 后面再用同一个 request id 唤醒它。

也注意第 10 步：  
`UpOut` 已经由 operator 算出来了，所以这里不需要再通过一个 `Frontend` 等待什么，只要用 `SinkEdge` 发回上游即可。

### 两个边对象的意义

#### `PipelineOperatorForwardEdge`

身份：

- `Sink<UpIn>`
- `Source<DownIn>`

意义：

- 接上游请求
- 再把转换后的请求送给下游

结构定义出处：

- `lib/runtime/src/pipeline/nodes.rs:130-139`

它有两个实现。

第一，它是 `Sink<UpIn>`，出处：

- `lib/runtime/src/pipeline/nodes.rs:222-235`

核心代码是：

```rust
let stream = self.parent.generate(data).await?;
self.parent.upstream.on_next(stream, private::Token).await
```

这段代码看起来短，但含义很重：

1. 正向请求 `UpIn` 从上游到了 operator
2. 调用 `parent.generate(data)`，也就是执行完整的 `operator.generate(...)`
3. 这个过程内部会调用下游，并把 `DownOut` 转成 `UpOut`
4. 拿到最终 `UpOut` 后，通过 `parent.upstream.on_next(...)` 往上游响应路径送回去

所以 `ForwardEdge` 的 `on_data(UpIn)` 不是简单“收请求然后立即发 DownIn”。  
真正的 `DownIn` 生成和下游调用发生在 `operator.generate(...)` 里。

第二，它是 `Source<DownIn>`，出处：

- `lib/runtime/src/pipeline/nodes.rs:237-253`

对应代码是：

```rust
self.parent.downstream.on_next(data, token).await
self.parent.downstream.set_edge(edge, token)
```

这个实现服务于图装配：  
当外部要把 operator 的正向输出接到下游节点时，实际接的是 `parent.downstream` 这个内部 `Frontend` 的正向 edge。

换句话说：

```text
forward_edge.set_edge(...)
  实际是在设置 downstream Frontend 的 edge

forward_edge.on_next(DownIn)
  实际是在让 downstream Frontend 把 DownIn 发给下游
```

这就是为什么 `ForwardEdge` 同时是 `Sink<UpIn>` 和 `Source<DownIn>`：  
它站在 operator 的正向请求面上，一边接上游 `UpIn`，一边暴露下游 `DownIn` 的连接面。

#### `PipelineOperatorBackwardEdge`

身份：

- `Sink<DownOut>`
- `Source<UpOut>`

意义：

- 接下游响应
- 再把转换后的响应送回上游

结构定义出处：

- `lib/runtime/src/pipeline/nodes.rs:141-150`

它也有两个实现。

第一，它是 `Sink<DownOut>`，出处：

- `lib/runtime/src/pipeline/nodes.rs:255-267`

核心代码是：

```rust
self.parent.downstream.on_data(data, token).await
```

这一步非常关键：  
下游响应 `DownOut` 回来时，不是直接交给 operator，也不是直接送上游，而是先交给 `parent.downstream.on_data(...)`。

为什么？

因为 `downstream` 是内部 `Frontend<DownIn, DownOut>`。  
前面 `operator.generate(...)` 调用 `downstream.generate(DownIn)` 时，`downstream` 已经在自己的 `sinks` 表里登记了这个 request id 对应的 `oneshot::Sender`。  
现在 `DownOut` 回来，必须进入 `downstream.on_data(DownOut)`，才能唤醒那个正在 await 的 `downstream.generate(...)`。

这一步完成之后，`operator.generate(...)` 才能拿到 `DownOut`，继续把它转换成 `UpOut`。

第二，它是 `Source<UpOut>`，出处：

- `lib/runtime/src/pipeline/nodes.rs:269-285`

对应代码是：

```rust
self.parent.upstream.on_next(data, token).await
self.parent.upstream.set_edge(edge, token)
```

这个实现服务于响应路径装配：  
当外部要把 operator 的响应输出接回上游时，实际接的是 `parent.upstream` 这个 `SinkEdge<UpOut>`。

所以：

```text
backward_edge.set_edge(...)
  实际是在设置 upstream SinkEdge 的 edge

backward_edge.on_next(UpOut)
  实际是在把 UpOut 发回上游响应路径
```

`BackwardEdge` 站在 operator 的响应面上，一边接下游 `DownOut`，一边暴露上游 `UpOut` 的连接面。

### 一次完整请求在 `PipelineOperator` 内部怎么走

可以按 request id 的等待关系理解，而不是只按函数调用栈理解。

```text
1. 上游请求 UpIn 到达 forward_edge.on_data(UpIn)

2. forward_edge 调 parent.generate(UpIn)

3. parent.generate 调 operator.generate(UpIn, downstream_frontend)

4. operator 内部把 UpIn 转成 DownIn

5. operator 调 downstream_frontend.generate(DownIn)
   - downstream_frontend 创建 oneshot
   - 按 request id 把 sender 存进自己的 sinks 表
   - 通过 downstream_frontend.edge 把 DownIn 发给下游
   - 然后 await receiver

6. 下游处理完成，返回 DownOut 到 backward_edge.on_data(DownOut)

7. backward_edge 调 downstream_frontend.on_data(DownOut)
   - downstream_frontend 用 request id 找到第 5 步的 sender
   - 唤醒正在 await 的 downstream_frontend.generate(...)

8. operator.generate 拿到 DownOut

9. operator 把 DownOut 转成 UpOut

10. forward_edge.on_data 拿到 parent.generate 返回的 UpOut

11. forward_edge 调 parent.upstream.on_next(UpOut)

12. UpOut 沿响应路径继续回到上游
```

这也是为什么 `downstream` 必须是一个 `Frontend`：  
它不是单纯保存一条边，而是要在 operator 调用下游时管理“发出去的 DownIn”和“未来回来的 DownOut”之间的配对。

而 `upstream` 只需要是 `SinkEdge`：  
因为 `UpOut` 已经算出来了，剩下只是沿响应边发回去，不需要再做 request/response 配对。

### 为什么设计成两个边而不是一个对象全包

因为 `PipelineOperator` 不是一个单向通过点，而是一个真正的双面节点：

- 正向面承接上游请求，面向下游输出改写后的请求
- 反向面承接下游响应，面向上游输出改写后的响应

如果把这两面硬塞进一个连接对象里，图装配时会非常混乱，因为调用方无法清楚表达：

- 现在连的是请求路径，还是响应路径
- 当前这一侧到底消耗什么、产出什么

拆成 `forward_edge()` 和 `backward_edge()` 之后，这个双面结构被显式化了。  
这不是为了代码好看，而是在让“节点同时参与正向和反向路径”这一事实直观可见。

还可以从类型上理解：

```text
ForwardEdge:
  输入 UpIn
  输出 DownIn

BackwardEdge:
  输入 DownOut
  输出 UpOut
```

如果强行合成一个对象，它就会同时暴露四种类型和四种方向：

```text
Sink<UpIn>
Source<DownIn>
Sink<DownOut>
Source<UpOut>
```

这样虽然理论上可行，但读图和连线都会很难判断当前拿到的这个对象到底代表哪一面。

拆开以后，连线语义更直接：

```text
请求路径只拿 forward_edge()
响应路径只拿 backward_edge()
```

这和 `Frontend` / `ServiceBackend` 的设计是同一种思想：  
同一个逻辑节点可以同时参与两个方向，但具体连接面要拆清楚。

### 设计意义总结

`PipelineOperator` 让一个抽象的双向 `Operator` 真正变成图里的可连接节点。  
它是 pipeline 能表达复杂中间层、适配层、代理层逻辑的核心基础设施。

这里最容易混的是 `PipelineOperator` 内部为什么又有一个 `Frontend`。

原因是：`Operator` 的逻辑需要调用“下游那一整段图”，而下游图在 pipeline 里仍然是异步请求/响应模型。  
所以 `PipelineOperator` 内部放了一个：

```rust
downstream: Arc<sources::Frontend<DownIn, DownOut>>
```

这个内部 `Frontend` 代表“从 operator 往下游看的那段 engine”。

于是调用关系变成：

```text
上游请求 UpIn
  ▼
PipelineOperator.forward_edge.on_data(UpIn)
  ▼
PipelineOperator.generate(UpIn)
  ▼
operator.generate(UpIn, downstream_frontend)
  │
  ├─ operator 把 UpIn 转成 DownIn
  ├─ operator 调 downstream_frontend.generate(DownIn)
  ├─ 下游图返回 DownOut
  └─ operator 把 DownOut 转成 UpOut
  ▼
PipelineOperator.upstream.on_next(UpOut)
  ▼
响应回到上游
```

也就是说，`PipelineOperator` 不是直接把输入转一下就发走。  
它会把“调用下游并拿回响应”这个完整过程交给 `Operator` 控制。

这就是它比 `PipelineNode` 强的地方。

可以把最终心智模型压缩成三句话：

```text
operator:
  负责业务上的双向转换

downstream Frontend:
  负责把 operator 后面的子图包装成 next.generate()

forward_edge / backward_edge:
  负责把这个双向 operator 拆成请求路径和响应路径上可连接的两个面
```

因此 `PipelineOperator` 的复杂度主要不是来自 Rust 泛型，而是来自它要同时维持两种视角：

- 对 `Operator` 来说：下游像一个普通 `AsyncEngine`
- 对 pipeline 图来说：operator 又必须暴露成可连线的 `Source` / `Sink`

`PipelineOperator` 就是这两种视角之间的适配层。

---

## 6.9 `PipelineNode<In, Out>`

`PipelineNode` 是最简单的单向节点。

### 字段

- `edge: OnceLock<Edge<Out>>`
- `map_fn: NodeFn<In, Out>`

### 设计意图

它解决的是另一类更常见、也更轻量的需求：

- 这个节点只想把 `In` 变成 `Out`
- 不关心下游引擎如何执行
- 也不关心响应回来的时候发生什么

换句话说，它故意只覆盖“单向局部变换”。

### 关键函数

#### `new(map_fn)`

作用：

- 创建一个带映射函数的节点

#### `Source<Out>::on_next`

作用：

- 把结果推给下游

#### `Sink<In>::on_data`

行为：

1. 用 `map_fn` 把 `In` 变成 `Out`
2. 调用 `on_next` 送到下游

### 设计意义

`PipelineNode` 和 `Operator` 的关系，不是“功能弱一点 / 强一点”这么简单，而是分别服务两类不同复杂度的节点：

- `PipelineNode`：单向、局部、直接
- `Operator`：双向、成对、可感知下游调用结果

把这两个抽象分开，是为了避免所有节点都被迫背上最重的双向接口。  
否则哪怕只是一个简单 map，也得实现整套 request/response 成对逻辑，使用成本会非常高。

所以 `PipelineNode` 的意义，是给图模型保留一个足够轻的基础积木。

例如一个很简单的前处理节点：

```text
SingleIn<RawRequest> -> SingleIn<ParsedRequest>
```

它只需要：

```rust
map_fn(raw_ctx) -> parsed_ctx
```

然后把结果继续 `on_next` 给下游。  
它不需要知道下游响应是什么，也不需要改写响应。

所以 `PipelineNode` 的模型是：

```text
收到 In
  ▼
map_fn(In) -> Out
  ▼
送出 Out
```

而 `Operator` 的模型是：

```text
收到 UpIn
  ▼
转成 DownIn
  ▼
调用下游
  ▼
拿到 DownOut
  ▼
转成 UpOut
```

如果只是单向转换，用 `PipelineNode` 更清楚；  
如果需要包住下游调用并影响响应，用 `Operator`。

### 第六节整体再总结

把第六节所有对象合起来，可以得到这张心智图：

```text
外部调用方
  │
  │ generate(In)
  ▼
Frontend
  │
  │ Source<In>
  ▼
Edge<In>
  │
  ▼
Sink<In> 的节点
  │
  │ 可能是 PipelineNode：只做 In -> Out
  │ 也可能是 PipelineOperator：做 UpIn/DownIn/DownOut/UpOut 双向适配
  ▼
ServiceBackend / SegmentSink
  │
  │ 调真实 engine.generate(...)
  ▼
真实业务 engine
  │
  │ 返回 Out
  ▼
ServiceBackend / SegmentSink 作为 Source<Out>
  │
  ▼
响应路径上的 Edge<Out>
  │
  ▼
Frontend 作为 Sink<Out>
  │
  │ 根据 request id 唤醒对应 oneshot receiver
  ▼
外部调用方拿到 Out
```

所以第六节的设计意图可以压缩成一句话：

**它把一次普通 `AsyncEngine::generate()` 调用拆成一张可插节点、可双向变换、可本地闭环的图，同时又通过 `Frontend` 把这张图重新包装成普通 engine 接口。**

---

## 七、分布式传输层

## 7.1 整体设计意图

分布式传输层要解决的，不是单纯“把 bytes 发到远端”。

它真正要保住的是这样一条语义承诺：

- 本地调用 `AsyncEngine::generate(request)`
- 远端执行真实逻辑
- 调用方仍然觉得自己拿回的是同一个 request 的响应流

而这条承诺里隐藏着一整组难题：

- 远端怎么知道响应应该回哪里
- 调用方怎么知道“响应流已建立”而不是“只是请求发出去了”
- 取消、终止、异常结束怎么和业务数据分离
- 流式响应如何在 transport 边界继续保持 request context

因此这层不是“给本地 engine 外面包一层 socket”，而是在把远程执行重新组织成两条协作平面：

- **请求平面（request plane）**：把请求头和请求体送到远端
- **响应平面（response plane）**：把远端产生的响应流再送回前端

这也是为什么网络层里会额外出现一整套看似“不是业务协议”的对象：

- `RequestControlMessage`
- `ConnectionInfo`
- `StreamOptions`
- `RegisteredStream`
- `ResponseStreamPrologue`
- `ControlMessage`

它们不是附属小工具，而是在补齐“远程调用要继续长得像本地 engine”所缺的运行时语义。

---

## 7.2 先建立完整心智模型

先把第六节的图模型和第七节的网络模型接起来看。  
第六节讲的是**进程内 pipeline 图怎么通过 `Source` / `Sink` / `Edge` 连接**；第七节讲的是**当图的某一段不在本进程时，如何把这条连接跨网络延长到远端**。

所以 `Ingress` / `Egress` 不应该理解成又一套替代 `Frontend` / `Backend` 的图节点体系。  
它们更像网络边界上的适配层：

```text
第六节：本地图层
  Frontend / ServiceFrontend
  PipelineNode / PipelineOperator
  ServiceBackend / SegmentSink
  SegmentSource

第七节：网络边界层
  Egress:
    当前进程的出口
    把一次本地 generate() 变成远程 request-plane 调用

  Ingress:
    远端进程的入口
    把网络收到的 bytes / control message 恢复成本地 SegmentSource.generate()
```

也就是说，`Egress` 和 `Ingress` 是夹在两张本地图之间的“跨进程桥”：

```text
调用方所在进程
────────────────────────────────────────────────────────

ServiceFrontend / Frontend
  │
  │ 本地图内的请求路径
  ▼
PipelineNode / PipelineOperator / ...
  │
  │ 如果下游在远端，这里看到的仍然像一个 AsyncEngine
  ▼
Egress<Req, Resp>
  │
  │ request plane：编码控制头 + 请求体，发到远端
  ▼
========================= 网络边界 =========================
  ▲
  │ response plane：远端响应流回到本地，继续表现为 Resp
  │

远端执行进程
────────────────────────────────────────────────────────

Ingress<Req, Resp>
  │
  │ 解码控制头 + 请求体，重建 Context
  ▼
SegmentSource<Req, Resp>
  │
  │ 远端本地图内的请求路径
  ▼
PipelineNode / PipelineOperator / ...
  │
  ▼
ServiceBackend / SegmentSink
  │
  │ 远端本地图内的响应路径
  ▼
SegmentSource<Req, Resp>
  │
  │ 结果再通过网络响应平面发回调用方所在进程
  ▼
========================= 网络边界 =========================
```

从这个图可以看出层次关系：

1. `Frontend` / `ServiceFrontend` / `SegmentSource` 是 **pipeline 图入口**，负责把 `generate()` 折叠成本地图里的请求/响应配对。
2. `ServiceBackend` / `SegmentSink` 是 **pipeline 图尾部**，负责落到真实 engine，并把响应沿反向路径送回去。
3. `Egress` 是 **本地侧网络出口适配器**，让上游看起来还在调用一个普通 `AsyncEngine`，但实际请求会被送到远端。
4. `Ingress` 是 **远端侧网络入口适配器**，把网络请求恢复成对远端 `SegmentSource` 的调用。
5. `RequestControlMessage`、`ConnectionInfo`、`StreamProvider` 这些对象，是为了让跨网络后的响应仍然能按 request id、连接信息和流生命周期正确配回去。

对应源码里，`Egress` 和 `Ingress` 的位置也体现了这层关系：

- `Egress` 持有 `transport_engine: Arc<dyn AsyncTransportEngine<Req, Resp>>`，出处：`lib/runtime/src/pipeline/network.rs:245-258`
- `Ingress` 持有 `segment: OnceLock<Arc<SegmentSource<Req, Resp>>>`，出处：`lib/runtime/src/pipeline/network.rs:288-308`
- `Ingress::for_engine()` 会构造一个最小远端本地图：`SegmentSource -> ServiceBackend -> SegmentSource`，出处：`lib/runtime/src/pipeline/network.rs:340-350`

所以如果把本地和远端合起来看，一条最小远程链路可以理解成：

```text
本地调用方
  │
  ▼
本地 Frontend / 上游 pipeline
  │
  ▼
Egress
  │
  ▼
网络 request plane
  │
  ▼
Ingress
  │
  ▼
远端 SegmentSource
  │
  ▼
远端 ServiceBackend / SegmentSink
  │
  ▼
远端 SegmentSource 接住响应回流
  │
  ▼
网络 response plane
  │
  ▼
本地调用方拿到响应
```

这里最容易混的是：`Egress` / `Ingress` 的命名是站在网络边界上看的，不是站在业务前后端上看的。

```text
Egress:
  数据从当前进程流出去

Ingress:
  数据流入当前进程
```

因此，同一条远程调用在发送方那侧叫 `egress`，在接收方那侧叫 `ingress`。  
它们和第六节图模型的关系是：**网络层负责跨进程搬运，搬运前后仍然要落回 `AsyncEngine` / `SegmentSource` / `ServiceBackend` 这套本地图语义。**

一个典型远程调用过程是：

1. 前端 `Egress` 想把一个 `SingleIn<T>` 发到远端
2. 它先向某个 `ResponseService` 注册“我等会儿要收一个响应流”
3. 注册结果里带一个 `ConnectionInfo`
4. 前端把 `ConnectionInfo` 塞进 `RequestControlMessage`
5. 前端把控制消息和真正请求体一起编码发给远端
6. 后端 `Ingress` 收到请求后，解出控制消息和请求体
7. 后端重建 `Context`
8. 后端把请求丢进本地 `SegmentSource`
9. 后端根据 `connection_info` 建立响应流
10. 后端把结果一条条发回去
11. 前端等待之前注册得到的 `StreamProvider`
12. 一旦配对成功，前端拿到真正的 `StreamReceiver`
13. 前端把字节流反序列化成 `ResponseStream<U>`

这个流程里最关键的设计点有三个：

#### 第一，响应通道必须先准备好

如果前端先发请求，再临时去想“远端等下把响应往哪里回”，就会出现窗口期：

- 远端可能已经开始吐数据
- 但前端还没有一个稳定的响应接收面

所以当前设计选择是：**先注册响应流，再发送请求**。

#### 第二，请求头里必须带控制平面信息

远端只拿到业务 payload 是不够的，它还必须知道：

- 这次请求是谁
- 输入/输出是单值还是流
- 响应流该如何回连
- 指标和追踪该如何关联

因此会额外有 `RequestControlMessage` 跟业务请求一起编码发送。

#### 第三，`generate()` 的成功时机不能等同于“请求已发出”

对 server-streaming 而言，请求发出成功，并不等于调用已经成功。  
真正更有意义的时刻是：

- 远端已经接住请求
- 响应流也已经建立成功

`ResponseStreamPrologue` 就是在解决这个时机判定问题。

因此，这整层虽然看起来对象很多，但背后其实都在围绕同一个目标协作：  
**把分布式往返链路重新折叠回上层熟悉的 engine 调用语义。**

这个过程中：

- `network.rs` 负责协议和抽象
- 具体 socket / HTTP / NATS 细节在子模块里实现

---

## 7.3 `Codable`

定义：

```rust
pub trait Codable: PipelineIO + Serialize + for<'de> Deserialize<'de> {}
```

作用：

- 这是一个约束打包 trait

它表达的不是“任何可序列化对象都能走网络”，而是：

- 这个对象已经是合法的 `PipelineIO`
- 同时它也能被 transport 层可靠编码/解码

所以 `Codable` 实际上在做一次交集约束：

- pipeline 世界认可它
- 网络世界也认可它

这能避免出现一种尴尬情况：  
某个类型在本地图里合法，但一到远程路径就发现根本没法编码；或者反过来，某个可序列化类型并不具备 request context 能力，却被误拿来接进 pipeline。

---

## 7.4 `WorkQueueConsumer`

定义：

```rust
async fn dequeue(&self) -> Result<Bytes, String>
```

作用：

- 抽象“从请求平面或工作队列拿到一坨原始字节”

它存在的设计意义是把“怎么收包”和“收到后怎么恢复 pipeline 请求”拆开。

这样上层逻辑只需要假设：

- 最终会拿到一段 `Bytes`
- 后续的解码、上下文恢复、推入 pipeline 都走统一逻辑

因此它更像 transport 接入的抽象预留点，而不是业务核心对象本身。

---

## 7.5 `StreamType`

枚举值：

- `Request`
- `Response`

作用：

- 标记一条流到底属于请求方向还是响应方向

这个枚举看似简单，但它把“流的方向性”从隐含约定变成了显式协议字段。

对于某些 transport 来说，请求流和响应流可能对应：

- 不同 subject
- 不同连接建立方式
- 不同 socket 角色

所以这里不能只靠调用方脑补“我现在拿到的是哪种流”，而必须显式编码。

---

## 7.6 `ControlMessage`

枚举值：

- `Stop`
- `Kill`
- `Sentinel`

作用：

- 这是控制平面的消息，不是业务数据

为什么要单独设计它，而不是把这类标记混进业务响应流里？  
因为停止、终止、结束都属于 transport / lifecycle 语义，不属于业务 payload 语义。

把两者混在一起会带来两个问题：

- 业务层必须理解本不该关心的网络控制帧
- transport 层又没法在不理解业务类型的前提下处理这些控制动作

因此 `ControlMessage` 负责承载那些“必须跨 transport 传递、但不应污染业务对象”的信号。

---

## 7.7 `ResponseStreamPrologue`

字段：

- `error: Option<String>`

作用：

- 这是响应流的第一条控制消息

它解决的是一个非常容易被忽略但极其关键的问题：

- 对于 streaming RPC，`generate()` 到底在什么时刻返回成功？

如果仅以“请求已经被 request plane 成功写出”为准，那么：

- 远端可能还没建立 response stream
- 后端 `generate` 甚至可能已经失败

这样前端拿到的会是一个其实永远收不到数据的“假成功”。

`ResponseStreamPrologue` 就是在补这个缺口：

- `error = None` 表示“响应流已建立，可以把它当作真正的返回值”
- `error = Some(...)` 表示“请求虽然发过去了，但这次远程调用并没有成功进入可返回流的状态”

所以它本质上是在给 streaming 版 `generate()` 补一个可靠的成功判定点。

---

## 7.8 `StreamProvider<T>`

从 7.8 开始，这一组对象不要再按“单个结构体有什么字段”来孤立理解。  
它们其实是在补齐一个更大的问题：

> 本地 `AsyncEngine::generate()` 只需要一次函数调用就能拿到响应对象；  
> 远程调用必须先把请求送到另一端，还要提前约定响应流从哪里回来。

所以 7.8 到 8.3 的主线是：

```text
前端侧准备响应回流通道
  ├─ StreamOptions：声明我要什么流
  ├─ ResponseService::register：向响应平面注册
  ├─ RegisteredStream：同时拿到给远端看的 connection_info，和本地等待的 stream_provider
  └─ StreamProvider：等待真正的 StreamReceiver / StreamSender 建立

前端侧发送请求
  ├─ RequestControlMessage：把 request id、交互形态、connection_info 写进控制头
  ├─ TwoPartMessage：控制头 + 业务请求体
  └─ AddressedPushRouter / Egress：把这次调用推出当前进程

后端侧接收请求
  ├─ PushWorkHandler：server 层统一入口
  ├─ Ingress：恢复 Context，并调用本地 SegmentSource
  ├─ StreamSender：把响应写回前端注册好的回流通道
  └─ NetworkStreamWrapper：把响应 item 和流结束信号编码回网络协议
```

也就是说，这一段讲的不是“多了一堆网络结构体”，而是在解释：  
**一次本地函数形态的 `generate()`，跨过网络后，需要哪些显式协议和生命周期对象才能重新闭合。**

定义：

```rust
type StreamProvider<T> = oneshot::Receiver<Result<T, String>>;
```

作用：

- 这是“等 transport 层完成连接配对后，再把真正流端点交给调用方”的等待器

这里的设计很克制：

- 连接建立这件事只发生一次
- 成功后只需要交付一次真正的流端点

所以 `oneshot` 恰好贴合它的语义，不会引入额外状态机复杂度。

`T` 可能是：

- `StreamSender`
- `StreamReceiver`

因此 `StreamProvider<T>` 的真正意义不是“某种 receiver 类型别名”，而是在把：

- “连接注册时刻”
- “真正流端点可用时刻”

明确区分开来。

它和前面 `Frontend.sinks` 里的 `oneshot` 有相似思想，但用途不同：

```text
Frontend.sinks 里的 oneshot:
  等一次 pipeline 响应对象回到 Frontend

StreamProvider<T>:
  等 transport 层把真正的流端点建立好
```

前者是在本地图内配对请求和响应；后者是在网络边界上配对“注册过的流”和“后来真正连上的流”。  
这就是 7.8 之后开始进入网络层的关键变化：很多本地图里隐含的关系，到了网络层都要显式登记、等待和恢复。

源码出处：

- `lib/runtime/src/pipeline/network.rs:75`

---

## 7.9 `RegisteredStream<T>`

字段：

- `connection_info: ConnectionInfo`
- `stream_provider: StreamProvider<T>`

作用：

- 它是一次流注册的返回句柄

字段意义：

#### `connection_info`

- 给远端看的
- 告诉远端如何回连或匹配这条流

#### `stream_provider`

- 给本地调用方自己等的
- 等真正的流对象建立完成

设计意义：

- 把“给远端的信息”和“给本地等待的信息”绑定在一起

注意：

- 注释提到它未来可能想做成带 RAII 清理能力的对象
- 但当前实现没有真正的 `Drop` 清理逻辑

这其实反映了一个很清晰的设计意图：  
一次“注册流”的动作天然会产出两份不同受众的数据：

- 一份要发给远端，让远端知道如何连接
- 一份留给本地，让本地在未来等真正连接完成

`RegisteredStream<T>` 就是把这两份信息捆成一个注册结果，避免它们在调用链里被拆散或错配。

这一点非常关键。  
如果只返回 `connection_info`，前端就知道该告诉远端怎么回连，但本地没人等真正的流建立。  
如果只返回 `stream_provider`，前端可以等流，但远端不知道应该连到哪里。

所以 `RegisteredStream<T>` 其实是一个“双面注册凭证”：

```text
connection_info:
  跨网络发送给远端
  让远端知道响应应该写到哪里

stream_provider:
  留在本地前端
  等远端真的建立响应流后拿到 StreamReceiver
```

在 `AddressedPushRouter` 里，这个关系会被拆开：

```text
let (connection_info, response_stream_provider) =
    pending_response_stream.into_parts();
```

也就是：

- `connection_info` 被放进 `RequestControlMessage`
- `response_stream_provider` 留在前端，稍后 await

这就是远程调用链路里最重要的一次“分叉”：一半信息随请求出站，另一半信息留在本地等待响应流回来。

源码出处：

- 定义：`lib/runtime/src/pipeline/network.rs:85-94`
- 使用：`lib/runtime/src/pipeline/network/egress/addressed_router.rs:181-190`

---

## 7.10 `PendingConnections`

字段：

- `send_stream: Option<RegisteredStream<StreamSender>>`
- `recv_stream: Option<RegisteredStream<StreamReceiver>>`

作用：

- 一次注册可能只需要发、只需要收，或者双向都需要

设计意义：

- 兼容不同 RPC 形态

例如：

- `SingleIn -> ManyOut` 主要只需要 `recv_stream`
- 将来更复杂的双向流可能两边都需要

因此 `PendingConnections` 不是在“额外包一层 Option”，而是在给注册结果保留足够的交互形状空间。

把它放到远程调用链里看，它代表的是“这次调用需要的所有待连接流端点”。  
当前成熟路径主要是 `SingleIn -> ManyOut`，所以前端只期待：

```text
(send_stream = None, recv_stream = Some(...))
```

对应到 `AddressedPushRouter` 里的校验：

```text
match pending_connections.into_parts() {
  (None, Some(recv_stream)) => recv_stream,
  _ => panic!("Invalid data plane registration for a SingleIn/ManyOut transport"),
}
```

这里不是随便 panic，而是在维护一个交互形态不变量：  
`SingleIn -> ManyOut` 的请求体是一次性随 request plane 发出去的，不需要额外 request stream；响应是流式回来的，所以必须有 response stream。

如果将来支持双向 streaming，这里就会变成：

```text
send_stream = Some(...)
recv_stream = Some(...)
```

所以 `PendingConnections` 是在为不同 RPC 形态预留统一返回结构，而不是只服务当前一种实现。

源码出处：

- 定义：`lib/runtime/src/pipeline/network.rs:97-112`
- 当前 `SingleIn -> ManyOut` 校验：`lib/runtime/src/pipeline/network/egress/addressed_router.rs:181-187`

---

## 7.11 `ResponseService`

定义：

```rust
async fn register(&self, options: StreamOptions) -> PendingConnections
```

作用：

- 抽象“谁负责注册响应流”

它的真正作用是把“响应平面资源分配”单独抽出来。

也就是说，前端不需要知道下面细节：

- 是分配了 TCP 监听端点
- 还是注册了某个 subject
- 还是在某个共享 server 里登记了待连接记录

上层只需要表达：“基于这个 request context，我要注册一组怎样的流。”

当前典型实现：

- `TcpStreamServer`

从分层上看，`ResponseService` 是“响应平面”的抽象入口。  
它不负责发送业务请求；它只负责在请求发出之前，先把响应回流需要的资源占好。

也就是说，远程调用不是直接：

```text
send request
wait response
```

而是：

```text
register response path
send request with connection_info
wait registered stream to become connected
read response stream
```

这个顺序不能反过来。  
如果先发请求，再注册响应流，远端可能已经开始回包，但前端还没有任何地方可以接。  
所以 `ResponseService::register()` 是远程调用的前置动作，它让“响应怎么回来”先于“请求怎么出去”被确定下来。

源码出处：

- trait 定义：`lib/runtime/src/pipeline/network.rs:115-120`
- 前端注册调用：`lib/runtime/src/pipeline/network/egress/addressed_router.rs:169-190`

---

## 7.12 `StreamSender`

字段：

- `tx: mpsc::Sender<TwoPartMessage>`
- `prologue: Option<ResponseStreamPrologue>`

作用：

- 网络层的发送端封装

### 字段意义

#### `tx`

- 真正发消息的通道
- 发送的是 `TwoPartMessage`

为什么不是直接 `Bytes`：

- 因为当前协议里 header / data 往往分段编码

#### `prologue`

- 用来保证第一条 prologue 只发送一次

### 关键函数

#### `send(data)`

- 发送普通数据 payload

#### `send_control(control)`

- 发送控制消息

#### `send_prologue(error)`

- 发送响应流启动消息

行为：

- `take()` 掉内部 `prologue`
- 再编码并发出去
- 如果重复调用，直接 panic

### 设计意义

`StreamSender` 并不是一个普通字节发送器。  
它内部还知道“这一条流的第一条消息必须先是 prologue”。

这说明发送端并不只是 transport primitive，而是已经带了一层协议状态。

因此它做的事情包括三层：

- 发送业务数据
- 发送控制消息
- 在流开始时完成一次显式握手

这让响应流的生命周期起点变得清晰可判定，而不是靠双方隐式约定。

`StreamSender` 出现在后端响应回流侧。  
后端 `Ingress` 调用本地 `segment.generate(request)` 后，会得到一个 `ManyOut<U>` 响应流，但这个响应流还只是本地 Rust stream。  
要把它送回前端，必须逐项编码并写入远端提前注册好的回流通道。

这里的关键不是“能 send bytes”，而是 `send_prologue()`：

```text
后端 generate 成功:
  send_prologue(None)
  然后逐项 send(response item)

后端 generate 失败:
  send_prologue(Some(error))
  前端 generate 不能假装成功
```

因此 prologue 是远程 streaming generate 的成功边界。  
前端只有在收到这个握手后，才能认为这次远程调用真的建立了响应流。

源码出处：

- 定义和方法：`lib/runtime/src/pipeline/network.rs:146-188`
- 后端发送 prologue：`lib/runtime/src/pipeline/network/ingress/push_handler.rs:259-286`

---

## 7.13 `StreamReceiver`

字段：

- `rx: mpsc::Receiver<Bytes>`

作用：

- 网络层接收端封装

当前实现确实很薄，但单独设类型仍然有价值，因为这层抽象把：

- 纯 transport `mpsc::Receiver<Bytes>`
- pipeline 语义里的“响应流接收端”

区分开了。

一旦后续需要补：

- 指标
- 生命周期钩子
- 结束态检查
- 调试信息

都可以继续往 `StreamReceiver` 上长，而不必回头大面积改接口。

`StreamReceiver` 是前端拿到的响应流入口。  
它和 `StreamSender` 是一对：

```text
后端:
  StreamSender.send(bytes)

前端:
  StreamReceiver.rx 接收 bytes
```

在 `AddressedPushRouter` 中，前端并不是一开始就拥有 `StreamReceiver`。  
它先拿到的是 `StreamProvider<StreamReceiver>`，然后在请求发出后 await：

```text
let response_stream = response_stream_provider.await?
```

这说明响应流的建立是异步的：  
前端先注册、再发请求、再等后端按 `connection_info` 连接回来。`StreamReceiver` 是这个回连完成后的实际接收端。

源码出处：

- 定义：`lib/runtime/src/pipeline/network.rs:190-192`
- 前端 await：`lib/runtime/src/pipeline/network/egress/addressed_router.rs:262-268`

---

## 7.14 `ConnectionInfo`

字段：

- `transport: String`
- `info: String`

作用：

- 这是跨 transport 的类型擦除连接描述

### 字段意义

#### `transport`

- 指明应该交给哪个 transport 解释

例如：

- `tcp_server`

#### `info`

- transport 专属连接细节
- 存成 JSON 字符串

### 为什么不是大 enum

如果用一个统一大 enum：

- 所有 transport 都得集中注册到一个地方
- 新增 transport 时耦合更高

现在这种做法：

- 耦合更低
- transport 更容易扩展

代价：

- `info` 需要二次序列化

这其实正是注释里提到的“为了类型擦除”。

这里的关键设计取舍是：

- 如果做成统一大 enum，编译期类型更强，但 transport 扩展性差
- 如果做成 `transport + info` 的擦除描述，运行时需要二次序列化，但 transport 模块之间耦合更低

当前实现明显更偏向后者。  
原因是 `pipeline` 想把 transport 选择这件事留在网络层，而不把所有具体 transport 细节回渗到公共协议层。

`ConnectionInfo` 是 `RegisteredStream` 能跨网络生效的原因。  
它把“前端本地注册好的响应流”翻译成远端能理解的连接描述。

一次远程调用里它的流向是：

```text
ResponseService::register(StreamOptions)
  -> RegisteredStream { connection_info, stream_provider }
  -> connection_info 放进 RequestControlMessage
  -> RequestControlMessage 随请求发到 Ingress
  -> Ingress 根据 connection_info 创建 response publisher
```

这也解释了为什么它不能只是本地对象引用。  
远端进程拿不到前端内存里的 receiver，只能拿到一段可序列化的连接描述，再交给对应 transport 去解释。

源码出处：

- 定义：`lib/runtime/src/pipeline/network.rs:194-207`
- 放入控制消息：`lib/runtime/src/pipeline/network/egress/addressed_router.rs:196-202`
- 后端使用：`lib/runtime/src/pipeline/network/ingress/push_handler.rs:225-241`

---

## 7.15 `StreamOptions`

字段：

- `context: Arc<dyn AsyncEngineContext>`
- `enable_request_stream: bool`
- `enable_response_stream: bool`
- `send_buffer_count: usize`
- `recv_buffer_count: usize`

作用：

- 声明“我要注册什么样的流”

### 字段意义

#### `context`

- 流关联的请求上下文
- 里面最核心的信息就是 request id 和生命周期控制

#### `enable_request_stream`

- 需要注册请求方向的流

注释已明确：

- 当前实现还没有完全成熟

#### `enable_response_stream`

- 需要注册响应方向的流

#### `send_buffer_count`

- 发送缓冲大小

#### `recv_buffer_count`

- 接收缓冲大小

### 为什么用 builder

因为这里本质上是在声明一组流资源需求，而不同 RPC 形态的参数组合差异很大。

例如：

- `SingleIn -> ManyOut`
- 双向流
- 只读流
- 只写流

这些场景既不适合靠位置参数硬传，也不适合要求调用方手动填全所有字段。  
builder 的作用是把“声明一组连接需求”这件事写得更像配置，而不是像在拼构造函数。

`StreamOptions` 是注册响应平面之前的需求声明。  
它把调用方想要的交互形态转成 response service 能理解的资源需求：

```text
context:
  这组流属于哪个 request id

enable_request_stream:
  是否需要额外请求流

enable_response_stream:
  是否需要响应回流

send_buffer_count / recv_buffer_count:
  transport 内部通道缓冲策略
```

当前 `AddressedPushRouter` 的路径里，构造的是：

```text
enable_request_stream(false)
enable_response_stream(true)
```

这正好对应 `SingleIn -> ManyOut`：

- 请求是单次输入，直接放进请求消息体
- 响应是多项输出，需要单独响应流

所以 `StreamOptions` 是把类型层面的 `SingleIn / ManyOut`，落实成网络层的“我要注册哪些流资源”。

源码出处：

- 定义：`lib/runtime/src/pipeline/network.rs:209-243`
- 当前构造：`lib/runtime/src/pipeline/network/egress/addressed_router.rs:169-175`

---

## 7.16 `Egress<Req, Resp>`

字段：

- `transport_engine: Arc<dyn AsyncTransportEngine<Req, Resp>>`

作用：

- 这是前端出口适配器

### 设计意图

上游 pipeline 不想知道：

- 现在调用的是本地 engine
- 还是远端 worker

`Egress` 做的事情就是：

- 把“远程 transport 调用”包装成普通 `AsyncEngine`

### 核心函数

#### `AsyncEngine::generate(request)`

行为：

- 直接委托给 `transport_engine.generate(request)`

### 一句话总结

- `Egress` 的意义就是：**把 RPC 伪装成本地 `AsyncEngine` 调用。**

但这层“伪装”不是掩耳盗铃，而是一种明确的系统设计选择：

- 上层 pipeline 关心的是请求/响应语义
- transport 细节应该沉到更低层

因此 `Egress` 是远程透明性的入口。  
它保证图里的节点不需要因为“这次请求其实去远端了”就换一套编程模型。

这里要区分两层：

```text
Egress:
  对 pipeline 图暴露成 AsyncEngine
  让上游觉得“我只是 generate 了一个 engine”

AddressedPushRouter / transport_engine:
  真正执行远程发送、响应流注册、编码、等待回流
```

所以 `Egress` 是语义适配层，不是远程调用细节的主实现。  
它的价值是把“远端 worker”伪装成一个普通 engine，让第六节的图模型可以照常使用：

```text
Frontend -> PipelineNode / Operator -> Egress
```

从图的角度看，`Egress` 可以站在类似 `ServiceBackend` 的位置：  
请求正向到达它时，它不调用本地业务 engine，而是调用 `transport_engine.generate(request)` 去远端拿响应。

源码出处：

- 定义和 `generate()`：`lib/runtime/src/pipeline/network.rs:245-258`

---

## 7.17 `RequestType` / `ResponseType`

枚举值：

- `SingleIn` / `ManyIn`
- `SingleOut` / `ManyOut`

作用：

- 把 RPC 形态写进控制消息

远端之所以需要这些信息，是因为只收到一包 bytes 并不能自动推断：

- 这是 unary 还是 streaming 输入
- 返回应该是一项结果还是一条结果流

所以这里本质上是在把“交互形状”显式编码进远程协议头里。  
这和前面在 `SingleIn` / `ManyOut` 等类型别名里做的事是呼应的：

- 本地用类型系统表达交互形状
- 远程用控制消息显式表达交互形状

当前实现中，最成熟路径主要围绕：

- `SingleIn -> ManyOut`

但协议设计上已经为更多形态留了空间。

它们和 `StreamOptions` 是同一件事的两个位置：

```text
StreamOptions:
  前端本地注册流资源时用

RequestType / ResponseType:
  远端收到请求后理解交互形态时用
```

本地图里，`SingleIn<T>` / `ManyOut<U>` 是 Rust 类型。  
跨网络后，类型系统不会跟着 bytes 自动传过去，所以需要 `RequestType` / `ResponseType` 把交互形态显式写进控制头。

这也是远程调用和本地调用最大的不同之一：  
本地可以靠类型和函数签名表达的信息，跨网络后都必须变成协议字段。

源码出处：

- `lib/runtime/src/pipeline/network.rs:261-273`

---

## 7.18 `RequestControlMessage`

字段：

- `id: String`
- `request_type: RequestType`
- `response_type: ResponseType`
- `connection_info: ConnectionInfo`
- `frontend_send_ts_ns: Option<u64>`

作用：

- 这是随请求一起发送的控制头

### 字段意义

#### `id`

- 请求 id

作用：

- 上下文关联
- 回包匹配

#### `request_type`

- 输入流形态

#### `response_type`

- 输出流形态

#### `connection_info`

- 告诉远端响应应该怎么回连

#### `frontend_send_ts_ns`

- 前端发送时间

作用：

- 便于拆解网络延迟指标

### 为什么它很关键

如果没有这个控制头，远端只拿到业务数据，不知道：

- 这次请求是谁
- 应该怎么回流
- 回的是单值还是流
- 指标怎么打

所以它其实就是：

- **远程调用的控制平面头部**

从设计角度看，它等价于把本地一次 `generate()` 所隐含的关键信息显式化了：

- 谁在发起这次请求
- 远端收到后应该按什么交互模型处理
- 响应建立后应该往哪里回
- 这次请求的时序指标从哪里开始记

也就是说，`RequestControlMessage` 不是“附带一点 metadata”，而是远程调用得以成立的最小控制协议。

可以把它理解成“跨网络版本的 `Context + 调用形态 + 回流地址`”。

本地 `Frontend.generate(request)` 里隐含了很多信息：

```text
request.id():
  用来配对请求和响应

In / Out 类型:
  表示调用形态

Frontend.sinks:
  保存响应应该回到哪个等待者
```

到了远程调用，这些隐含关系必须写进 `RequestControlMessage`：

```text
id:
  远端用它重建 Context，也用于指标和响应关联

request_type / response_type:
  告诉远端交互形态

connection_info:
  告诉远端响应流应该写回哪里

frontend_send_ts_ns:
  让后端能计算前端发出到后端收到之间的网络 transit 时间
```

所以 `RequestControlMessage` 是前端 egress 和后端 ingress 之间的最小契约。  
没有它，后端只能拿到业务 bytes，却不知道这次远程调用应该如何恢复成本地 pipeline 请求。

源码出处：

- 定义：`lib/runtime/src/pipeline/network.rs:275-286`
- 前端构造：`lib/runtime/src/pipeline/network/egress/addressed_router.rs:196-202`
- 后端解析：`lib/runtime/src/pipeline/network/ingress/push_handler.rs:171-212`

---

## 7.19 `Ingress<Req, Resp>`

字段：

- `segment: OnceLock<Arc<SegmentSource<Req, Resp>>>`
- `metrics: OnceLock<Arc<WorkHandlerMetrics>>`
- `portname_health_check_notifier: OnceLock<Arc<tokio::sync::Notify>>`

作用：

- 这是后端入口适配器

### 设计意图

网络层收到的是字节，不是 pipeline 类型。  
`Ingress` 的作用就是把网络请求落回本地 pipeline。

### 字段意义

#### `segment`

- 绑定到本地某个 `SegmentSource`

为什么是 `OnceLock`：

- 入口图通常只绑定一次

#### `metrics`

- portname 级别的请求处理指标

#### `portname_health_check_notifier`

- 响应流结束时通知健康检查/保活逻辑重置计时器

### 关键函数

#### `new()`

- 创建空壳 `Ingress`

#### `attach(segment)`

- 绑定本地 pipeline 入口

#### `add_metrics(...)`

- 初始化 metrics

#### `link(segment)` / `for_pipeline(segment)`

- 语义化辅助构造函数

#### `for_engine(engine)`

作用：

- 自动拼出最简单的本地回路

即：

- `SegmentSource -> ServiceBackend -> SegmentSource`

再包成 `Ingress`

### 一句话总结

- `Ingress` 的角色就是：**把远程请求落到本地 pipeline 上。**

更具体地说，`Ingress` 做的是从“网络视角”到“本地图视角”的翻译：

- 网络视角里，拿到的是 bytes 和控制头
- 本地图视角里，需要的是一个 `Context<Req>` 和一个已经连好的 `SegmentSource`

所以它是远程透明性的另一半。  
`Egress` 负责把远程调用伪装成本地 engine，`Ingress` 负责把网络请求恢复成本地图入口。

把它和第六节的图模型接起来看，`Ingress` 自己不是普通图节点，它更像网络层到本地图的入口桥：

```text
网络 payload
  -> PushWorkHandler::handle_payload
  -> 解析 RequestControlMessage
  -> 恢复 Context<Req>
  -> Ingress.segment.generate(request)
  -> SegmentSource 把请求推进本地 segment
```

`Ingress` 持有的不是任意 engine，而是：

```rust
segment: OnceLock<Arc<SegmentSource<Req, Resp>>>
```

这点很重要。  
远端收到请求后，不是绕过第六节的 pipeline 图模型直接调用业务函数，而是重新落到 `SegmentSource`，继续用同一套 `Frontend -> Edge -> Sink` 机制执行。

`for_engine(engine)` 是最小落地图的快捷构造：

```text
SegmentSource
  --Req-->
ServiceBackend
  --Resp-->
SegmentSource
```

也就是说，即便远端只是包一个本地 engine，也仍然通过第六节那套请求正向、响应反向的闭环来执行。  
这就是 `Ingress` 和 `SegmentSource` 的层次关系：

```text
Ingress:
  网络入口，负责 bytes/control/context/metrics/health

SegmentSource:
  pipeline segment 入口，负责把请求推进本地图并等待响应回流
```

源码出处：

- 字段与构造：`lib/runtime/src/pipeline/network.rs:288-350`
- 后端调用 `segment.generate()`：`lib/runtime/src/pipeline/network/ingress/push_handler.rs:243-249`

---

## 7.20 `PushWorkHandler`

方法：

- `handle_payload(payload)`
- `add_metrics(...)`
- `set_portname_health_check_notifier(...)`

作用：

- 给网络/队列接入层调用的统一处理接口

它的价值在于把“server 层职责”压得很窄。

server 层不需要理解：

- 请求头格式
- Context 如何恢复
- 流如何建立
- 响应如何回推

它只需要：

- 收到一包 payload
- 交给 `PushWorkHandler`

后面的一切统一由 pipeline 网络入口来完成。  
因此这是一个非常典型的分层隔离点。

`PushWorkHandler` 是为了把 server 实现和 pipeline 网络入口解耦。  
HTTP/TCP/NATS server 最终都可以收下一段 payload，但它们不应该都各自实现：

```text
TwoPartCodec 解码
RequestControlMessage 解析
Context 恢复
response stream 创建
segment.generate()
NetworkStreamWrapper 编码
```

这些逻辑属于 pipeline 网络入口，不属于 server accept loop。  
因此 server 只依赖一个很窄的接口：

```text
handle_payload(Bytes)
```

从层次上看：

```text
RequestPlaneServer / transport portname
  负责监听、接收、路由 payload

PushWorkHandler
  负责把 payload 交给 pipeline ingress 处理

Ingress<SingleIn<T>, ManyOut<U>>
  实现 PushWorkHandler，完成协议恢复和本地图调用
```

所以 `PushWorkHandler` 是“transport server 层”和“pipeline ingress 层”的分界线。

源码出处：

- trait 定义：`lib/runtime/src/pipeline/network.rs:359-378`
- `Ingress` 实现：`lib/runtime/src/pipeline/network/ingress/push_handler.rs:128-364`

---

## 7.21 `NetworkStreamWrapper<U>`

字段：

- `data: Option<U>`
- `complete_final: bool`

作用：

- 这是响应流项的临时网络包装层

### 字段意义

#### `data`

- 真正的响应项
- `None` 表示这一帧不是业务数据

#### `complete_final`

- 标记这是不是“正常结束”的最后信号

### 为什么需要它

因为在纯 TCP/JSON 字节流里，连接关闭不一定能区分：

- 正常结束
- 异常中断

所以这里额外包一层结束标记。

### 当前状态

- 注释已经明确说明：这是过渡方案
- 未来希望换成更自然的结束检测机制

这个类型非常能体现当前实现的工程现实：

- 理想上，底层 transport 应该能更自然地表达流结束
- 但在当前协议和实现约束下，还需要一个显式结束帧来区分“正常完成”和“中途断掉”

所以它是一个补洞型抽象，而不是最终理想形态。

它和 `ResponseStreamPrologue` 分别解决响应流生命周期的两个端点：

```text
ResponseStreamPrologue:
  响应流开始时，告诉前端“这条远程响应流是否成功建立”

NetworkStreamWrapper:
  响应流进行中和结束时，告诉前端“这一帧是数据，还是正常结束标记”
```

后端发送逻辑是：

```text
for resp in stream:
  send(NetworkStreamWrapper { data: Some(resp), complete_final: false })

stream 正常结束:
  send(NetworkStreamWrapper { data: None, complete_final: true })
```

前端接收逻辑则反过来：

```text
data = Some(item):
  交给 ResponseStream 调用方

data = None 且 complete_final = true:
  正常结束

连接关闭但没有 complete_final:
  视为异常断流
```

所以它虽然是过渡方案，但承担的是一个真实协议职责：  
把“Rust stream 正常结束”翻译成网络字节流里可观察的结束帧。

源码出处：

- 类型定义：`lib/runtime/src/pipeline/network.rs:423-430`
- 后端包装发送：`lib/runtime/src/pipeline/network/ingress/push_handler.rs:291-357`
- 前端解析：`lib/runtime/src/pipeline/network/egress/addressed_router.rs:270-331`

---

## 八、`network` 子模块完整设计分析

第七节把远程调用需要的协议对象拆开讲了。  
第八节要把它们重新合起来看，并且覆盖 `network` 子模块下所有实现文件。

## 8.0 第八节如何接上前面的 pipeline、Ingress 和 Egress

进入第八节之前，要先把它和前面几节的关系摆正。  
第六节讲的是本地图执行模型，第七节讲的是远程调用所需的公共协议对象，第八节讲的是这些协议对象如何被 `network/` 子模块里的具体实现串成一条真正能跑的远程请求链。

从第六节看，pipeline 图的核心不变量是：

```text
Frontend / SegmentSource:
  对外表现为 AsyncEngine::generate()
  对内用 Source -> Edge -> Sink 推进请求
  再用响应路径把 ManyOut 回到等待者

ServiceBackend / PipelineOperator / SegmentSink:
  都是在这套图模型里承担不同边界的节点
```

第八节的 `network` 子模块并没有替换这套模型。  
它做的是把这套模型跨进程复原：

```text
前端本地图:
  generate(SingleIn<T>)
    -> Egress / PushRouter / AddressedPushRouter
    -> 远程发送

后端本地图:
  Ingress::handle_payload(bytes)
    -> 恢复 Context<T>
    -> SegmentSource.generate(...)
    -> 继续走本地图 Source / Edge / Sink
```

所以 `network` 和 pipeline 图之间不是松散的“调用工具”关系，而是有很强的语义耦合：

- `network` 必须保留 `Context` 的 id、取消控制和 stream context，否则远端响应无法回到同一条请求语义上。
- `network` 必须知道 `SingleIn` / `ManyOut` 这类交互形态，否则跨网络后类型系统表达的输入输出形状会丢失。
- `network` 必须把远端 worker 包装回 `AsyncEngine`，否则上层 pipeline 图就会因为“本地/远端”分裂成两套编程模型。
- `network` 必须把后端 payload 落回 `SegmentSource`，否则远端执行会绕过第六节的图连接、响应回流和 operator 机制。

这也是 `Egress` 和 `Ingress` 在第七节里被单独讲过的原因。  
它们是 pipeline 图和第八节具体网络实现之间的两个语义接头。

```text
Egress:
  面向前端 pipeline 图
  把远程调用包装成 AsyncEngine
  让上游节点仍然只看到 generate()

Ingress:
  面向后端 pipeline 图
  把网络 payload 恢复成 Context<Req>
  再交给 SegmentSource.generate()
```

因此从“接入点”看，前后端是对称的：

```text
前端接入点:
  pipeline 图里某个节点需要远程 worker
  -> Egress / PushRouter 仍然暴露 AsyncEngine
  -> AddressedPushRouter 负责真正跨网络调用

后端接入点:
  request-plane server 收到 bytes
  -> PushWorkHandler 统一接住 transport payload
  -> Ingress 恢复 Context 并调用 SegmentSource.generate()
```

这两个接入点共同保证了一件事：  
跨网络以后，调用链没有脱离 pipeline 的原始语义。前端仍然认为自己在调用一个 `AsyncEngine`，后端仍然把请求放进 `SegmentSource` 所代表的本地图入口。

但 `Egress` / `Ingress` 本身还不是完整实现。  
它们更像边界适配器，真正把请求发出去、把响应流接回来、把 portname 注册到 server、把 worker 从 discovery 里选出来的，是第八节要讲的 `network/` 子模块：

```text
Egress / PushRouter:
  对上接 pipeline 图
  对下接 AddressedPushRouter 和 RequestPlaneClient

AddressedPushRouter:
  接住“已知目标地址的一次 generate”
  协调 request plane 和 response plane

RequestPlaneServer:
  把 Ingress 注册成远端 portname handler

PushWorkHandler:
  把 HTTP/TCP/NATS server 收到的 bytes 交回 Ingress

TcpStreamServer / TcpClient:
  让 ManyOut 响应流从后端连回前端
```

可以把第八节理解成第六、七节的落地层：

```text
第六节:
  本地 pipeline 如何运行

第七节:
  远程调用需要哪些公共协议对象

第八节:
  network 子模块如何把这些对象组织成
  discovery -> request plane -> ingress -> local pipeline -> response plane -> ManyOut
```

因此，第八节分析 `network` 时，重点不是重复“哪个文件定义哪个 struct”，而是看每个文件如何守住这条跨模块链路中的一个边界。

从耦合角度看，这里有三类耦合需要分清：

```text
语义耦合:
  network 必须理解 Context id、SingleIn/ManyOut、ResponseStream 这些 pipeline 语义
  否则远程调用无法伪装成本地 generate()

协议耦合:
  AddressedPushRouter 和 PushWorkHandler 必须共享 RequestControlMessage / TwoPartCodec / NetworkStreamWrapper
  否则前端编码和后端解码无法对齐

资源耦合:
  NetworkManager、RequestPlaneServer、TcpStreamServer 必须和 servicegroup/discovery 生命周期配合
  否则 portname 注册、实际端口发布和 response CallHome 会错位
```

这三类耦合决定了第八节的组织方式：  
先看文件布局，再看 request plane / response plane，再看 codec、ingress、egress、tcp、manager，最后回到完整调用链和演进边界。

这一层的代码不要只按“HTTP/TCP/NATS 三种 transport”理解。  
更准确的理解方式是：它把一次远程 pipeline 调用拆成了两条平面。

```text
request plane:
  前端把控制头 + 请求体推到远端 worker
  可以走 HTTP、TCP、NATS

response plane:
  远端 worker 把 ManyOut 响应流连回前端
  当前实现固定使用 tcp::server::TcpStreamServer + tcp::client::TcpClient 的 CallHome 模型

长期资源总控：
  NetworkManager
  负责创建和复用 request plane server/client
```

所以第八节不是第七节的重复，而是回答：  
**这些零散协议对象在真正请求链路里分别被谁串起来，哪些文件各自守住哪条边界。**

---

## 8.1 `network` 文件布局和职责地图

源码入口：

- `lib/runtime/src/pipeline/network.rs`
- `lib/runtime/src/pipeline/network/`

顶层 `network.rs` 负责定义跨子模块共享的协议对象和两个 pipeline 适配器：

- `StreamSender` / `StreamReceiver`
- `ConnectionInfo`
- `StreamOptions`
- `Egress<Req, Resp>`
- `Ingress<Req, Resp>`
- `PushWorkHandler`
- `RequestControlMessage`
- `NetworkStreamWrapper<U>`

子目录则把具体传输实现拆开：

```text
network.rs:
  公共协议类型、Ingress/Egress 适配器、PushWorkHandler trait

network/codec.rs:
  TCP request-plane framing、TCP ack framing、TwoPartCodec re-export

network/codec/two_part.rs:
  控制头 + 业务体的二段消息协议

network/codec/zero_copy_decoder.rs:
  Shared TCP request-plane 服务端的零拷贝读路径

network/ingress.rs:
  ingress 子模块 re-export

network/ingress/unified_server.rs:
  RequestPlaneServer trait

network/ingress/http_portname.rs:
  HTTP/2 request-plane server

network/ingress/shared_tcp_portname.rs:
  TCP request-plane server

network/ingress/nats_server.rs:
  NATS request-plane multiplex server

network/ingress/push_portname.rs:
  NATS 单 portname 消费循环

network/ingress/push_handler.rs:
  Ingress<SingleIn<T>, ManyOut<U>> 的 PushWorkHandler 实现

network/egress.rs:
  egress 子模块 re-export

network/egress/unified_client.rs:
  RequestPlaneClient trait

network/egress/http_router.rs:
  HTTP/2 request-plane client

network/egress/tcp_client.rs:
  TCP request-plane client 和连接池

network/egress/nats_client.rs:
  NATS request-plane client

network/egress/push_router.rs:
  worker discovery、负载选择和故障下线

network/egress/addressed_router.rs:
  已知目标地址后的单次远程调用主桥

network/tcp.rs:
  response plane 的 TCP ConnectionInfo 和 CallHomeHandshake

network/tcp/server.rs:
  response plane 的 TcpStreamServer

network/tcp/client.rs:
  response plane 的 TcpClient::create_response_stream

network/tcp/test_utils.rs:
  TCP 单元测试辅助工具
```

这份布局体现了一个核心设计选择：

- request plane 是可替换的
- response plane 当前还是固定 TCP CallHome
- pipeline 语义通过 `Ingress` / `Egress` / `PushRouter` / `AddressedPushRouter` 和底层 transport 解耦

也就是说，`network` 不是简单的“网络工具包”。  
它是把第六节本地图执行模型远程化的一整套协议层和运行时资源层。

---

## 8.2 request plane 和 response plane 为什么分开

一次远程 `SingleIn<T> -> ManyOut<U>` 调用不是普通 unary RPC。

请求方向只有一包：

```text
RequestControlMessage + request body
```

但响应方向是一条流：

```text
response item 1
response item 2
response item 3
...
complete_final
```

所以当前设计把两件事分开：

```text
request plane:
  负责把请求推到远端
  返回的只是 ack

response plane:
  负责把真正的 ManyOut<U> 流从远端写回前端
```

这也是为什么 `AddressedPushRouter` 同时持有两个对象：

```text
req_client:
  Arc<dyn RequestPlaneClient>
  可以是 HTTP/TCP/NATS

resp_transport:
  Arc<tcp::server::TcpStreamServer>
  当前固定负责响应流注册和接收
```

这个拆分带来两个重要结果。

第一，request plane 可以演进。  
HTTP、TCP、NATS 都只要实现 `RequestPlaneClient` / `RequestPlaneServer`，上层 `AddressedPushRouter` 和 `PushRouter` 不需要知道具体 transport。

第二，response plane 暂时没有同等抽象。  
`push_handler.rs` 里仍然直接调用：

```text
tcp::client::TcpClient::create_response_stream(...)
```

这说明当前代码已经把请求面抽象出来了，但响应面仍然是 TCP CallHome 专用实现。  
文档阅读时要注意这个不对称：它不是概念错误，而是当前实现阶段的边界。

完整时序可以这样看：

```text
frontend:
  PushRouter 选择 worker
  AddressedPushRouter 注册 response plane
  AddressedPushRouter 构造 RequestControlMessage
  RequestPlaneClient 发送 request plane payload

backend:
  RequestPlaneServer 收到 payload
  PushWorkHandler 解码 payload
  Ingress 恢复 Context
  SegmentSource.generate() 执行本地图
  TcpClient CallHome 到 frontend 的 TcpStreamServer

frontend:
  TcpStreamServer 收到 CallHome
  StreamReceiver 交付给 AddressedPushRouter
  AddressedPushRouter 解包 NetworkStreamWrapper
  返回 ManyOut<U>
```

---

## 8.3 `codec/`：两套协议分别服务不同层

`codec` 子模块最容易被误读成“所有网络消息共用一套 codec”。  
实际不是。

它里面有两套不同层级的协议：

```text
TcpRequestMessage / TcpResponseMessage:
  TCP request plane 使用
  解决“一个共享 TCP server 如何路由到某个 portname”

TwoPartMessage / TwoPartCodec:
  pipeline workload 使用
  解决“控制头和业务体如何放进同一个 payload”
```

### `codec.rs`

`codec.rs` 本身也是一个门面模块。  
它一方面定义 TCP request plane 的 request/ack framing，另一方面把更下层的 workload framing 和热路径 decoder 暴露出来：

```text
pub use two_part::{TwoPartCodec, TwoPartMessage, TwoPartMessageType}
pub use zero_copy_decoder::{TcpRequestMessageZeroCopy, ZeroCopyTcpDecoder}
```

所以这个文件里的“codec”不是单一协议名，而是把三类编码边界收在同一个模块入口下：

```text
TcpRequestCodec / TcpResponseCodec:
  TCP request plane 的 framed stream 协议

TwoPartCodec:
  pipeline workload 的 control/data 二段协议

ZeroCopyTcpDecoder:
  SharedTcpServer 读路径上的 TcpRequestMessage 优化解码器
```

`TcpRequestMessage` 的 wire format 是：

```text
portname_path_len: u16
portname_path: UTF-8
headers_len: u16
headers: JSON HashMap<String, String>
payload_len: u32
payload: bytes
```

从实体边界看，`TcpRequestMessage` 不是业务请求对象，而是 TCP request plane 的 envelope：

```text
portname_path:
  共享 TCP server 上的 portname 路由键

headers:
  request-plane metadata，例如 trace header、frontend send timestamp

payload:
  已经编码好的 workload bytes，通常是 TwoPartMessage
```

也就是说，它故意把“怎么找到 portname”和“portname 收到后如何理解业务请求”拆开。  
`portname_path` 和 `headers` 属于 transport/request-plane 层；`payload` 里才进入 pipeline remote-call 协议。

`TcpRequestMessage::new(portname_path, payload)` 是最小构造器。  
它创建一个没有 headers 的 request envelope，适合测试、旧路径，或者不需要额外 metadata 的调用。

`TcpRequestMessage::with_headers(portname_path, headers, payload)` 则显式暴露 metadata 注入点。  
这和 `AddressedPushRouter` 发送前准备 trace headers 的行为对应：router 不需要理解 TCP framing，只需要把 headers 交给 request-plane client；具体 TCP client 再把这些 headers 放进 `TcpRequestMessage`。

`TcpRequestMessage::encode()` 是一次性编码路径，设计上做了三件事：

1. 先把 `portname_path` 当 UTF-8 bytes 计算长度，并限制在 `u16::MAX` 内。
2. 把 `headers` 序列化成 JSON `HashMap<String, String>`，同样限制在 `u16::MAX` 内。
3. 把 `payload` 长度限制在 `u32::MAX` 内，然后按固定顺序写入 `BytesMut`，最后 `freeze()` 成 `Bytes`。

这些长度字段的大小选择体现了协议假设：

```text
portname_path:
  小字段，用 u16 足够表达 portname routing key

headers:
  小字段，用 u16 约束 metadata 不膨胀

payload:
  主体字段，用 u32 承载真实 workload bytes
```

这里的错误类型也有语义区分：  
编码阶段的超长 portname、headers、payload 都是 `InvalidInput`，表示调用方试图构造一个协议无法承载的 request；headers JSON 序列化失败同样属于输入对象不合法。

`TcpRequestMessage::decode(bytes)` 是兼容性的整包解码路径。  
它按 wire format 顺序推进 offset：先读 portname 长度，再校验完整 portname bytes；然后读 headers 长度并解析 JSON；最后读 payload 长度并用 `Bytes::slice` 返回 payload。

这个函数的设计重点不是 streaming，而是“给定完整 bytes 后恢复 envelope”。  
所以它遇到截断数据会直接返回 `UnexpectedEof`，遇到 portname 非 UTF-8 或 headers 非 JSON 会返回 `InvalidData`。  
payload 部分使用 slice，是因为大 payload 不应该在恢复 envelope 时再复制一份。

这里的 `payload` 本身通常又是一包 `TwoPartCodec` 编出来的 bytes。  
所以 TCP request plane 里经常是“协议套协议”：

```text
TcpRequestMessage.payload
  = TwoPartMessage(RequestControlMessage, business_request)
```

`TcpRequestCodec` 则是面向 `tokio_util::codec::FramedRead/FramedWrite` 的流式版本。  
它和 `TcpRequestMessage::encode/decode` 使用同一套 wire format，但职责不同：

```text
TcpRequestMessage::encode/decode:
  整包 bytes 和结构体之间互转

TcpRequestCodec:
  在 TCP byte stream 上处理半包、粘包、最大消息大小和 buffer 消费
```

`TcpRequestCodec::decode()` 不会在数据不足时消费 buffer。  
它先 peek portname/header/payload 三段长度，算出 `total_len`，只有完整消息到齐后才 `advance` 和 `split_to`。  
这使它可以安全处理 TCP 的 partial frame：一次 `read` 不完整时返回 `Ok(None)`，等待后续 bytes。

`TcpRequestCodec::encode()` 的设计则更适合 framed writer：它直接写入调用方提供的 `BytesMut`，同时用 `max_message_size` 做总长度保护。  
因此 `TcpRequestCodec::new(Some(max))` 是 request-plane 入口处的背压和防御边界之一，避免 portname/path/header 合法但总 frame 过大的请求进入后续处理。

`TcpRequestCodec` 派生 `Default` 和 `Clone` 也有工程含义。  
`Default` 等价于不设置 `max_message_size`，方便测试和调用方先获得无上限 codec；`Clone` 则让 framed 组件或连接管理代码可以复制 codec 配置，而不是共享一个带 buffer 状态的对象。

这里要注意 `max_message_size` 的错误语义：

```text
decode 侧超过 max:
  InvalidData
  因为远端发来的 frame 数据不符合本端接收策略

encode 侧超过 max:
  InvalidInput
  因为本端调用方正在构造一个不允许发送的 frame
```

这一区分对日志、指标和错误分类有意义。  
同样是“太大”，接收方向说明 peer 或网络输入不可接受，发送方向说明本地请求对象不可接受。

`TcpRequestCodec` 的实现还体现了一个拷贝边界：portname path 必须变成 `String`，所以要做 UTF-8 校验和分配；payload 则通过 `split_to(...).freeze()` 交出 `Bytes`，避免复制主体数据。  
这也是后面 `ZeroCopyTcpDecoder` 继续优化 server 热路径的原因：大块 payload 才是 request plane 的主要成本。

`TcpResponseMessage` 则简单很多：

```text
length: u32
data: bytes
```

它只承载 request plane ack 或错误文本，不承载真正的 streaming response。  
真正响应流在 response plane 的 `StreamReceiver` 上回来。

`TcpResponseMessage::new(data)` 表达“带 ack/error body 的 response”，`TcpResponseMessage::empty()` 表达成功但没有 body 的 ack。  
这和 request plane 的语义一致：response 只说明请求是否被 transport/server 接收，不代表业务推理结果。

`TcpResponseMessage::encode/decode` 的结构比 request 简单：一个 `u32` 长度加 data bytes。  
decode 同样只在 data 完整时返回成功，并用 `Bytes::slice` 避免复制 response body。

`TcpResponseCodec` 是它的 framed-stream 版本，负责从 TCP stream 中处理 response 半包和最大消息大小。  
在当前架构里，这条 response codec 仍属于 request plane ack 路径；真正的 ManyOut token/result stream 走的是 response plane 的 TCP stream server，而不是这里的 `TcpResponseMessage`。

`TcpRequestMessage` 和 `TcpResponseMessage` 都派生 `PartialEq/Eq`。  
这主要服务协议回归测试：测试可以直接断言 encode/decode 后的 envelope 完全一致，而不需要逐字段比较，也避免遗漏 headers、空 payload、空 ack 这类边界。

`codec.rs` 的测试矩阵也说明了这组 API 的契约：

```text
request/response encode-decode:
  整包往返必须保持结构和值不变

empty payload / empty response:
  空主体是合法 frame，不是 EOF

large payload:
  主体较大时仍按长度字段和 Bytes 语义处理

truncated decode:
  整包 decode 遇到截断必须失败，framed decode 遇到半包先返回 None

unicode portname:
  portname_path 是 UTF-8 字符串，不限 ASCII

max_message_size:
  encode 侧应拒绝超过本地策略的 frame

FramedRead/FramedWrite + Cursor:
  不依赖真实 TCP 也能测试多帧串联和 partial frame 行为
```

这组测试没有把 `with_headers` 单独作为 round-trip 用例展开，但 wire format 和 request codec 都把 headers 纳入 JSON 段，因此 headers 是 envelope 协议的一部分，不是 transport 实现外部的附加状态。

设计意义：

- `TcpRequestMessage` 负责 TCP 共享 server 的 portname multiplexing
- `TcpResponseMessage` 负责 request plane ack
- `TwoPartMessage` 负责 pipeline 远程调用协议

如果把这三者混在一起理解，就会误以为 TCP client 的 response 是业务 response。  
实际上 TCP request-plane client 收到的 response 只是“请求已被接收/排队”的 ack。

### `codec/two_part.rs`

`TwoPartCodec` 的格式是：

```text
header_len: u64
body_len: u64
checksum: u64
header bytes
body bytes
```

它把一条消息拆成两个语义部分：

```text
header:
  RequestControlMessage 或 ControlMessage

data:
  业务请求体或业务响应体
```

`TwoPartMessageType` 再把组合情况显式化：

- `HeaderOnly`
- `DataOnly`
- `HeaderAndData`
- `Empty`

`TwoPartCodec::encode_message(msg)` 和 `decode_message(data)` 是整包便利方法。  
它们内部都会 clone 一个 codec，再复用 `tokio_util::codec::Encoder/Decoder` 的实现：

```text
encode_message:
  TwoPartMessage -> BytesMut -> freeze() -> Bytes

decode_message:
  Bytes -> BytesMut -> decode(...) -> TwoPartMessage
```

也就是说，真正的协议逻辑只有一份，既可用于整包 API，也可用于 framed stream。  
`decode_message` 如果没有解出完整消息，会返回 `InvalidMessage("No message decoded")`；这和 stream decoder 的 `Ok(None)` 不同，因为整包 API 的调用方已经承诺传入的是一段完整 bytes。

`TwoPartCodec::decode(src)` 的流程是：

```text
1. 至少需要 24 bytes 才能读 fixed header
2. 用 cursor 读取 header_len、body_len、checksum
3. checked_add 计算 total_len = 24 + header_len + body_len
4. 检查 total_len 是否超过 max_message_size
5. 如果 src 还没有完整 total_len，返回 Ok(None)
6. advance(24) 跳过 fixed header
7. debug/test 构建下校验 checksum
8. split_to(header_len).freeze() 得到 header
9. split_to(body_len).freeze() 得到 data
```

这里用 cursor 读取 `u64` 的原因是先 peek fixed header，而不是立刻消费 `src`。  
`get_u64()` 会推进 cursor 自己的位置，但 cursor 只是 `&src[..]` 的临时视图，不会改变外层 `BytesMut`。  
只有确认完整消息已经到齐之后，代码才对真正的 `src` 调用 `advance(24)`。

`checked_add` 是安全边界。  
`header_len` 和 `body_len` 都来自 wire input，不能信任；如果直接做 `24 + header_len + body_len`，恶意长度可能导致整数溢出，让一个超大 frame 变成看似很小的 `total_len`。  
因此溢出时直接转成 `MessageTooLarge`。

`max_message_size` 是策略边界。  
即使长度相加没有溢出，只要 `total_len` 超过配置上限，decode 就拒绝这条消息。  
这避免 framed reader 在后续路径里接受过大的 control/data payload。

半包处理靠 `Ok(None)` 表达。  
当 `src.len() < total_len` 时，decoder 不消费任何数据，等待下一次读到更多 bytes。  
这就是它能在 TCP stream 上处理 partial frame 的原因。

checksum 是 debug/test 下的诊断检查：

```text
checksum == 0:
  视为 dummy checksum，跳过

checksum != 0:
  对 header bytes + body bytes 重新计算 xxh3_64
  不一致则返回 ChecksumMismatch
```

release 构建里这段校验不会编译进去。  
这样 debug/test 能尽早发现编码错位、内存内容损坏或测试构造错误，release 热路径则避免每条消息都 hash 一遍。

`advance`、`split_to` 和 `freeze` 的配合体现了解码后的所有权转移：

```text
advance(24):
  fixed header 已经读完且不再需要，直接跳过

split_to(header_len):
  header 是有语义的数据，要从 src 里切出来

split_to(body_len):
  body 同样切出来，剩余 src 保留给后续 frame

freeze():
  BytesMut -> Bytes，后续作为不可变 payload 传递
```

`TwoPartCodec::encode(item, dst)` 是 decode 的反向过程：

```text
1. 取 item.header.len() 和 item.data.len()
2. checked_add 计算 total_len
3. 检查 max_message_size
4. 写 header_len: u64
5. 写 body_len: u64
6. debug/test 下计算 header+data 的 xxh3_64 checksum
7. release 下写 dummy checksum 0
8. 依次写 header bytes 和 data bytes
```

encode 同样用 `checked_add` 和 `max_message_size`，因为本地调用方也可能构造出协议无法接受的大消息。  
debug/test 下计算 checksum 时，会临时拼出 `header + data` 用于 hashing；release 下写 0，和 decode 侧 “dummy checksum 跳过校验” 配套。

在远程请求主链路里，后端 `push_handler` 要求收到的是 `HeaderAndData`：

```text
header = RequestControlMessage JSON
data   = T 的 JSON
```

在 TCP response plane 的控制消息里，经常使用 `HeaderOnly`：

```text
header = ControlMessage / ResponseStreamPrologue / CallHomeHandshake
data   = empty
```

### `codec/zero_copy_decoder.rs`

`ZeroCopyTcpDecoder` 专门服务 `SharedTcpServer` 的读路径。  
它不是新的协议，而是 `TcpRequestMessage` wire format 的高性能读取方式。

这里的 “zero-copy” 要限定理解：  
它不是说 TCP socket 到用户态完全没有拷贝，而是说 request-plane server 在已经把 bytes 读进进程之后，不再为了重建 `TcpRequestMessage` 而复制大块 payload。

它的设计目标是：

- 复用内部 `BytesMut`
- 先原地解析长度字段
- 等完整消息到齐后 `split_to(total_len)`
- 用 `Bytes::slice` 返回 payload

`ZeroCopyTcpDecoder` 持有两个核心字段：

```text
read_buffer: BytesMut
  每条连接复用的读缓冲区
  会按需要增长，但不会在每条消息后缩小

max_message_size: usize
  当前连接允许接收的最大 frame
  默认来自 PGD_TCP_MAX_MESSAGE_SIZE，否则 32MB
```

默认初始 buffer capacity 是 256KB。  
这个选择反映了一个热路径假设：多数请求不需要每次从很小 buffer 反复扩容，但也不为每条连接一开始就分配最大消息大小。

`new()` 只是使用默认初始容量；`with_capacity(capacity)` 允许测试或特定调用方控制初始 buffer。  
`Default` 等价于 `new()`，方便把 decoder 放进连接处理结构里。

`read_message(reader)` 是核心函数。  
它不是一次性读完整消息再解析，而是按 wire format 分阶段确保所需 bytes 到齐：

```text
1. 至少读到 2 bytes，解析 path_len
2. 确保 path bytes + headers_len 到齐
3. 解析 headers_len
4. 确保 headers bytes + payload_len 到齐
5. 解析 payload_len
6. 计算 total_len = 2 + path_len + 2 + headers_len + 4 + payload_len
7. 检查 total_len <= max_message_size
8. 继续 read_buf，直到整条消息都在 read_buffer 里
9. split_to(total_len).freeze() 切出完整消息
10. 返回 TcpRequestMessageZeroCopy
```

这种分阶段读取有两个意义。

第一，它避免提前分配目标结构。  
portname path 长度、headers 长度、payload 长度都直接从 `read_buffer` 的固定 offset 读取：

```text
path_len:
  read_buffer[0..2]

headers_len:
  read_buffer[2 + path_len .. 2 + path_len + 2]

payload_len:
  read_buffer[2 + path_len + 2 + headers_len .. +4]
```

这些解析都是对 buffer 的索引读取，不会构造临时 `String`、`HashMap` 或 payload `Vec`。

第二，它能自然处理 TCP 半包。  
每个阶段如果 buffer 不够，就继续 `reader.read_buf(&mut read_buffer).await`；如果对端关闭连接，则根据当前阶段返回 `UnexpectedEof`。  
这比“先读一个大 Vec 再解析”更适合长连接上的连续 frame。

这里也有几个防护边界：

```text
path_len == 0 或 path_len > 1024:
  InvalidData
  portname path 为空或异常膨胀时直接拒绝

total_len > max_message_size:
  InvalidData
  以整条 frame 的大小为准，而不是只看 payload_len

reader 返回 0 且消息不完整:
  UnexpectedEof
  区分正常无数据和半条消息中断
```

`split_to(total_len).freeze()` 是这个实现的关键点。  
`split_to(total_len)` 从复用的 `read_buffer` 前面切出一条完整消息；剩余 bytes 仍留在 `read_buffer` 里，可能已经是下一条 frame 的开头。  
`freeze()` 把切出的 `BytesMut` 转成不可变 `Bytes`，后续 `TcpRequestMessageZeroCopy` 持有这整条消息的共享底层内存。

所以这里的所有权变化是：

```text
read_buffer:
  连接级复用 buffer
  负责累计 socket 读到的 bytes

message_bytes:
  从 read_buffer 前端切出来的一条完整 frame
  freeze 后成为 Bytes

TcpRequestMessageZeroCopy.raw:
  持有整条 frame 的 Bytes
  后续访问字段都从 raw 上切片
```

`TcpRequestMessageZeroCopy` 是解码后的零拷贝视图。  
它不急着把 wire format 转成普通 `TcpRequestMessage`，而是保存完整 raw bytes：

```text
raw:
  [path_len][path][headers_len][headers][payload_len][payload]
```

然后通过访问器按需解析：

```text
portname_path():
  返回 &str
  只做 UTF-8 校验，成功时借用 raw 内的 path bytes

portname_path_bytes():
  返回 &[u8]
  完全不做字符串分配

headers_bytes():
  返回 raw 内 headers JSON 的 &[u8]

headers():
  需要把 JSON 解析成 HashMap<String, String>
  这是按需分配，不是 read_message 阶段的成本

payload():
  返回 raw.slice(payload_start..)
  这是 Bytes 的零拷贝切片，clone 也只是引用计数和 offset/len

total_size():
  返回 raw.len()

raw_bytes():
  暴露整条 frame，主要用于调试
```

这意味着 `portname_path()` 仍可能因为 UTF-8 非法而失败，`headers()` 仍会产生 JSON 解析和 `HashMap` 分配。  
真正被重点优化的是 payload：它通常是最大的一段，也是后续要交给 `PushWorkHandler` / `TwoPartCodec` 的主体数据。

和 `TcpRequestCodec` 相比，差异可以这样理解：

```text
TcpRequestCodec:
  tokio_util::codec::Decoder
  解出普通 TcpRequestMessage
  portname_path 是 String，headers 是 HashMap

ZeroCopyTcpDecoder:
  自己驱动 AsyncRead::read_buf
  解出 TcpRequestMessageZeroCopy
  尽量延迟 String/HashMap 构造，payload 通过 Bytes::slice 共享 raw
```

所以 `ZeroCopyTcpDecoder` 更适合 `SharedTcpServer` 这种高并发接收端。  
server 的第一职责是快速路由、投递和 ack；如果每条请求都复制 payload 或提前解析全部 metadata，高并发下成本会集中在内存带宽和分配器上。

测试也体现了它的契约：

```text
basic:
  普通 portname + payload 能恢复

large_payload:
  大 payload 不需要复制成 Vec 才能访问

total_size_limit:
  max 限制按整条 frame 计算，包含 path/header/length 字段开销

with_headers:
  headers_bytes 保留 raw JSON，headers() 可按需解析 HashMap

empty_vs_populated_headers:
  同一个 decoder 连续读不同 headers 形态时 offset 计算不能漂移
```

这样 `SharedTcpServer` 把请求投递到 worker pool 时，不需要复制大 payload。  
对高并发、较大请求体的 request plane 来说，这是很关键的热路径优化。

源码出处：

- `lib/runtime/src/pipeline/network/codec.rs`
- `lib/runtime/src/pipeline/network/codec/two_part.rs`
- `lib/runtime/src/pipeline/network/codec/zero_copy_decoder.rs`

---

## 8.4 `ingress/`：request plane server 和 payload 处理边界

`ingress` 子模块可以分成两层：

```text
server 层:
  HTTP/TCP/NATS 负责收请求、查 portname、ack、并发控制、生命周期

handler 层:
  PushWorkHandler 负责把 payload 恢复成 pipeline 调用
```

这个分层非常重要。  
server 层不应该理解 `RequestControlMessage`、`SegmentSource`、`NetworkStreamWrapper`。  
它只需要把收到的 bytes 交给：

```text
Arc<dyn PushWorkHandler>
```

### `ingress.rs`

这个文件只负责声明子模块。  
它的意义不是业务逻辑，而是把 ingress 相关实现聚合到一个 namespace 下。

当前 ingress namespace 包含六类文件：

```text
unified_server.rs:
  request-plane server trait

shared_tcp_portname.rs:
  TCP request-plane server

http_portname.rs:
  HTTP/2 request-plane server

nats_server.rs:
  NATS 多 portname server 适配层

push_portname.rs:
  NATS 单 portname 消费循环

push_handler.rs:
  payload -> 本地 pipeline -> response plane 的落地器
```

这个入口文件本身没有控制流，但它给读者一个边界：  
`ingress/` 下既有 transport server，也有真正理解 pipeline 远程调用协议的 handler。  
前者在第 8.4 节讲，后者单独放到第 8.5 节。

### `ingress/unified_server.rs`

`RequestPlaneServer` 是 HTTP/TCP/NATS 服务端的统一接口。

核心方法：

- `register_portname(...)`
- `unregister_portname(...)`
- `address()`
- `transport_name()`
- `is_healthy()`

它表达的不是“某个具体服务器怎么收包”，而是 runtime 需要的最小服务端能力：

```text
可以注册 portname handler
可以注销 portname
可以给 discovery/日志提供地址
可以报告 transport 名称和健康状态
```

这让 `NetworkManager` 可以只返回：

```text
Arc<dyn RequestPlaneServer>
```

调用方不需要知道当前是 HTTP、TCP 还是 NATS。

这个 trait 有几个设计点。

第一，它要求实现 `Send + Sync`。  
request-plane server 会被包进 `Arc<dyn RequestPlaneServer>`，在 portname 注册、取消任务、网络 manager 和 runtime 之间共享，所以不能依赖单线程所有权。

第二，`register_portname` 的参数不是 transport 细节，而是 runtime 语义：

```text
portname_name:
  上层逻辑 portname 名，例如 generate / health / load_lora

service_handler:
  Arc<dyn PushWorkHandler>
  server 层收到请求后最终调用的 handler

instance_id:
  当前 portname instance 的唯一标识
  TCP/NATS 会把它放进路由 key，HTTP 当前不额外拼 instance id

namespace / servicegroup_name:
  用于 tracing、metrics、service group、discovery 语义

system_health:
  portname Ready / NotReady 状态的共享健康视图
```

第三，这个 trait 没规定“路由 key 必须长什么样”。  
不同 transport 的实现会把同一组入参映射成不同的底层路由形态：

```text
TCP:
  {instance_id:x}/{portname_name}

HTTP:
  {PGD_HTTP_RPC_ROOT_PATH}/{portname_name}

NATS:
  {portname_name}-{instance_id:x}
```

这样上层只说“注册 portname”，具体如何避免 subject/path/portname 冲突由 transport 自己处理。

`address()` 返回的是 transport-specific 的对外地址。  
它主要用于日志、discovery 和客户端构造；不是健康检查本身。  
`transport_name()` 是轻量标签，用于日志和策略分支。  
`is_healthy()` 当前在几个实现里都偏轻量，更多表示 server 对象处于可用状态，而不是做一次真实网络探测。

### `ingress/shared_tcp_portname.rs`

`SharedTcpServer` 是 TCP request plane 服务端。

它解决的问题是：  
同一个进程通常只有一个 discovery instance id，但可能注册多个 portname；测试或特殊多 worker 场景里，也可能有多个 instance id 共享同一个 TCP server。  
无论是哪种情况，对外都只希望暴露一个 TCP request-plane 端口，而不是每个 portname / worker 都绑定一个端口。

因此它内部有：

- `handlers: DashMap<String, Arc<PortnameHandler>>`
- worker pool
- bounded work queue
- per-portname inflight counter
- portname health 状态

TCP 路由 key 当前是：

```text
{instance_id:x}/{portname_name}
```

如果是最常见的“一进程一个 instance”场景，这个 key 里的 `instance_id` 看起来有点冗余，但它有两个作用：

```text
同一 instance 多 portname:
  a1/generate
  a1/health
  a1/load_lora

同一进程多 worker / 测试模拟多 instance:
  a1/generate
  b2/generate
```

这样可以避免同名 portname 在共享 TCP server 上互相覆盖，也和 discovery/NATS 里 portname instance 的命名习惯保持一致。

`SharedTcpServer` 的字段可以按职责拆开看：

```text
handlers:
  portname_path -> portnameHandler
  request 到达后用 portname_path 做路由

bind_addr:
  配置期望绑定的地址，可能包含端口 0

actual_addr:
  bind_and_start 后真实绑定的地址
  端口 0 时由 OS 分配，所以必须运行后再记录

cancellation_token:
  控制 accept loop 和 worker dispatcher 退出

work_tx:
  read loop 到 worker dispatcher 的有界 work queue
```

`PortnameHandler` 是注册到 `handlers` 里的 portname 状态：

```text
service_handler:
  Arc<dyn PushWorkHandler>
  真正把 payload 交给 pipeline ingress 的处理器

instance_id / namespace / servicegroup_name / portname_name:
  日志、trace、metrics、health 和路由语义所需的身份字段

system_health:
  register 后设 Ready，unregister 时设 NotReady

inflight:
  当前 portname 已入队但尚未处理完成的请求数

notify:
  unregister 等待 inflight 清零时使用
```

`WorkItem` 是从 TCP read loop 投递到 worker pool 的最小工作单元：

```text
service_handler:
  后续要调用的 PushWorkHandler

payload:
  ZeroCopyTcpDecoder 返回的 Bytes
  clone 成本低，不复制大 payload

headers:
  TCP request envelope 里的 request-plane metadata
  用于 trace context 和前端发送时间戳

inflight / notify:
  worker 完成后递减并唤醒 unregister 等待者

instance_id / namespace / servicegroup_name / portname_name:
  构造 span、日志和指标标签
```

这三个结构体的关系可以按生命周期理解：

```text
SharedTcpServer:
  server 级对象，生命周期最长
  负责持有 portname 路由表、监听地址、取消信号和 work queue sender

PortnameHandler:
  portname 注册态对象，生命周期跟 portname registration 绑定
  一个 portname_path 对应一个 PortnameHandler
  保存 handler、身份字段、health、inflight 和 notify

WorkItem:
  单次请求对象，生命周期最短
  从某个 PortnameHandler 拷贝/clone 出处理本次请求所需的字段
  入队后交给 worker dispatcher 执行
```

一次 TCP request 在这三者之间的流转是：

```text
SharedTcpServer.accept_loop:
  接收连接，启动 handle_connection

read_loop:
  解码 TcpRequestMessage
  取 portname_path
  handlers.get(portname_path) -> PortnameHandler

PortnameHandler:
  inflight += 1
  clone service_handler / notify / identity fields
  payload 和 headers 组成 WorkItem

work_tx:
  WorkItem 入有界队列

worker dispatcher:
  取出 WorkItem
  acquire semaphore permit
  spawn handle_work_item(work_item)

handle_work_item:
  调 service_handler.handle_payload(payload)
  完成后 inflight -= 1，并 notify unregister 等待者
```

所以 `WorkItem` 不是新的业务实体，也不是新的 portname。  
它只是“把某次请求从 socket 读循环搬到异步执行池”时需要携带的一包上下文。

这里不能只把 `PortnameHandler` 直接丢给 worker pool，原因是单次请求还包含：

```text
payload:
  每个 request 不同

headers:
  每个 request 不同，包含 traceparent、x-request-id、x-frontend-send-ts-ns 等

inflight / notify:
  虽然来自 PortnameHandler，但 worker 完成时必须能独立递减和唤醒

identity fields:
  用于本次请求的 span 和指标标签
```

也不能让 `read_loop` 直接 `await service_handler.handle_payload(payload)`。  
如果这样做，一条 TCP 连接上的读循环会被业务处理阻塞：

```text
request A:
  read_loop 调 handle_payload(A) 并等待

request B:
  即使 socket 上已经到达，也要等 A 的业务处理结束后才能 decode / route / ack
```

这会带来几个问题：

```text
连接级 head-of-line blocking:
  慢请求会阻塞同一连接上的后续请求读取和 ack

accept/read 热路径被业务耗时污染:
  TCP server 的主要职责是收包、路由、入队、ack
  不应该被模型执行、pipeline 调用或 response plane 初始化拖住

无法做统一背压:
  如果无限 spawn handle_payload，过载时内存和 task 数会失控
  如果同步 await handle_payload，又会牺牲吞吐和 ack 延迟

graceful shutdown 难以精确:
  unregister 需要知道已经接收并入队的请求是否完成
  per-portname inflight 必须覆盖“排队中 + 执行中”的请求
```

因此当前设计把 TCP ingress 拆成两段：

```text
socket/read path:
  尽快 decode
  查 handler
  inflight += 1
  WorkItem 入队
  入队成功后 ACK

worker path:
  受 semaphore 限制并发
  执行 handle_payload
  记录 transit/span
  inflight -= 1
```

这个设计处理的是高并发 request-plane 入口的典型问题：网络收包速度可能短时间高于业务处理速度。  
bounded queue 负责吸收有限突发，semaphore 负责限制真实并发，ACK 时机负责把“已经成功排队”和“还没被接收处理”区分开。

这里的 worker pool 不等同于 discovery 里的 worker instance。  
它只是 `SharedTcpServer` 内部的并发执行池，用来把 socket read loop 和 `handle_payload` 解耦。

`new(bind_addr, cancellation_token)` 做三件事：

```text
1. 读取 PGD_TCP_WORKER_POOL_SIZE，默认 1500
2. 读取 PGD_TCP_WORK_QUEUE_SIZE，默认 6000
3. 创建 bounded mpsc work queue，并启动 worker dispatcher
```

work queue 是有界的，这一点很重要。  
如果后端处理跟不上，`work_tx.send(work_item).await` 会形成背压；如果 channel 已关闭，则 request plane 会返回错误 response，而不是假装 ack 成功。

`start_worker_pool` 的名字里有 pool，但实现上是一个 dispatcher 加 semaphore：

```text
一个 receiver:
  从 work_rx 串行接收 WorkItem

一个 semaphore:
  限制同时执行的 handle_work_item 数量

每个 WorkItem:
  acquire permit 后 spawn task
  task 完成时 drop permit
```

这样避免了多个 worker task 争抢同一个 receiver 时的额外锁竞争，同时仍然把并发度限制在 `PGD_TCP_WORKER_POOL_SIZE`。

`handle_work_item` 是 request plane server 到 pipeline ingress 的最后一步。  
它先根据 headers 里的 `x-frontend-send-ts-ns` 计算 frontend -> backend request-plane transit：

```text
frontend:
  AddressedPushRouter 在真正发送前写 x-frontend-send-ts-ns

backend:
  SharedTcpServer worker 读取该 header
  用当前 SystemTime 做差
  记录 WORK_HANDLER_NETWORK_TRANSIT_SECONDS
```

然后它用 TCP headers 构造 tracing span，再调用：

```text
service_handler.handle_payload(payload)
```

这里的 `service_handler` 通常是 `Ingress` 实现的 `PushWorkHandler`。  
也就是说，`SharedTcpServer` 本身不理解 `RequestControlMessage`、`TwoPartMessage` 里的业务语义；它只负责把 request envelope 解开、路由到 handler、再把 payload 交给 ingress。

无论 `handle_payload` 成功还是失败，`handle_work_item` 最后都会：

```text
inflight.fetch_sub(1)
notify.notify_one()
```

这保证了 unregister / graceful shutdown 不会因为失败请求而永远等待。

`bind_and_start()` 是推荐启动路径：

```text
1. TcpListener::bind(bind_addr)
2. 读取 listener.local_addr() 得到 actual_addr
3. 写入 self.actual_addr
4. spawn accept_loop(listener)
5. 返回 actual_addr
```

这解释了为什么同时有 `bind_addr` 和 `actual_addr`。  
当配置端口为 0 时，只有 bind 完成后才知道 OS 分配的真实端口；discovery 和日志应该使用 `actual_addr`。

`accept_loop` 只做连接级调度：

```text
listener.accept()
  -> clone handlers / work_tx
  -> spawn handle_connection(stream, handlers, work_tx)

cancellation_token.cancelled()
  -> 退出 accept loop
```

每条 TCP 连接进入 `handle_connection` 后会被拆成 read half 和 write half。  
读循环在当前 task 中运行，写循环单独 spawn：

```text
read_loop:
  从连接上持续解码 TcpRequestMessageZeroCopy
  路由、入队、产生 ack/error response

write_loop:
  从 response channel 读取已经编码好的 TcpResponseMessage bytes
  write_all + flush
```

这里之所以叫 `read_loop`，不是因为“一次业务请求会被重复处理”，而是因为 TCP request plane 使用的是连接上的流式 framing：

```text
TCP connection:
  一个长期存在的字节流

TcpRequestMessage:
  字节流里的一帧 request message

TcpResponseMessage:
  对这一帧 request message 的一帧 ack / error
```

也就是说，`RequestPlaneClient::send_request(...)` 对调用方来说是一次性请求：

```text
send_request(payload)
  -> 写入一帧 TcpRequestMessage
  -> 等待一帧 TcpResponseMessage ack
  -> 返回 ack bytes
```

但这不等于底层 TCP 连接也是一次性的。  
`egress/tcp_client.rs` 里有连接池，每个 `TcpConnection` 有自己的 writer task 和 reader task；同一条连接可以连续发送多条 request frame，并按 FIFO 顺序等待多条 ack frame。

所以两个独立进程之间的 TCP request plane 更像这样：

```text
frontend process:
  TcpRequestClient 维护到 backend 的连接池
  每次 send_request 只是在某条连接上写一帧 request

backend process:
  SharedTcpServer 的 accept_loop 长期监听新连接
  每条已建立连接由 read_loop 持续读取多帧 request
  直到对端关闭、协议错误或 server shutdown
```

这和 HTTP 请求模型不一样。  
HTTP/2 也可能复用底层连接，但复用和 framing 由 HTTP stack 管；这里的 raw TCP transport 必须自己用 `TcpRequestCodec` / `ZeroCopyTcpDecoder` 识别“下一帧 request 从哪里开始、到哪里结束”，所以服务端自然需要一个循环不断从同一条字节流中取出下一帧。

如果没有这个循环，server 处理完第一帧 request 后就会退出连接处理任务，client 侧连接池里的这条连接也就无法复用，后续 request 要么失败，要么只能重新建连。  
这会增加连接建立成本，也会破坏 `TcpConnection` 里 FIFO request/ack 配对的设计。

这种拆分允许 read loop 在处理下一条 request 时，把 ack/error 交给 write task 发送，不需要把读写逻辑混在同一段控制流里。

`read_loop` 是 `SharedTcpServer` 的 request-plane 热路径：

```text
1. 创建 ZeroCopyTcpDecoder
2. read_message() 读取完整 TcpRequestMessageZeroCopy
3. portname_path() 得到路由 key
4. headers() 解析 request-plane metadata
5. payload() 取零拷贝 Bytes
6. handlers.get(portname_path) 查 PortnameHandler
7. inflight += 1
8. 构造 WorkItem
9. work_tx.send(work_item).await
10. 入队成功后发送空 TcpResponseMessage ack
```

这里有几条错误路径：

```text
read_message 返回 UnexpectedEof:
  对端关闭连接，退出 read loop

read_message 返回其他错误:
  发送 "Read error: ..." response，然后返回错误

portname_path 非 UTF-8:
  发送 "Invalid portname path"，继续读下一条消息

handlers 找不到 portname:
  发送 "Unknown portname: ..."，继续读下一条消息

work queue send 失败:
  发送 "Server overloaded: ..."，递减 inflight 并唤醒 notify
```

ACK 的时机是这个设计里最关键的不变量：

```text
只有 work_tx.send(work_item).await 成功后，才发送空 ACK
```

因此 TCP request-plane ack 表达的是：

```text
目标 server 已经收到请求，并且请求已经成功进入本地处理队列
```

它不表示业务生成已经完成，也不表示 response plane 已经开始回流。  
业务结果仍然走 `RequestControlMessage.connection_info` 指定的 response plane。

`register_portname` 有两个层次。  
内部方法接收完整 `portname_path`；`RequestPlaneServer` trait 实现则把上层传入的 `portname_name` 和 `instance_id` 组合成：

```text
format!("{instance_id:x}/{portname_name}")
```

内部注册顺序也有意安排：

```text
1. 构造 PortnameHandler
2. 先插入 handlers
3. 再把 portname health 设置为 Ready
```

这样 health 变 Ready 时，handler 已经可以被路由命中，避免服务发现或健康检查看到 Ready 但请求还找不到 handler。

`unregister_portname` 则反过来：

```text
1. 从 handlers 移除 portname_path
2. health 设置为 NotReady
3. 如果 inflight > 0，等待 notify 直到 inflight 清零
```

trait 层的 `unregister_portname(portname_name)` 会删除所有以 `/{portname_name}` 结尾的 key。  
这是为了兼容“同一 TCP server 内多个 instance id 注册同名 portname”的场景：

```text
a1/generate
b2/generate
```

调用 unregister `generate` 时，两者都会被清理。

`address()` 返回的是对外暴露给 discovery / 日志的 TCP 地址：

```text
tcp://ip:port
```

如果已经 bind，则使用 `actual_addr`；否则退回 `bind_addr`。  
`transport_name()` 固定返回 `"tcp"`；`is_healthy()` 当前是轻量判断，主要表达 server 对象已创建且可作为 request-plane server 使用。

### `ingress/http_portname.rs`

`SharedHttpServer` 是 HTTP/2 request plane 服务端。  
它的角色和 `SharedTcpServer` 对齐：

- 一个 server 多 portname
- `DashMap` 路由到 portname handler
- `register_portname` 后设置 health ready
- `unregister_portname` 时等待 inflight
- 收到请求后异步 spawn `handle_payload`
- 立即返回 `202 Accepted`

它内部的字段和 TCP server 类似，但没有独立 work queue：

```text
handlers:
  portname path -> PortnameHandler

bind_addr:
  配置期望绑定的 HTTP 地址

actual_addr:
  bind 后的真实地址，端口 0 时由 OS 分配

cancellation_token:
  控制 accept loop 和 per-connection task 退出
```

`PortnameHandler` 保存每个 HTTP portname 的运行时状态：

```text
service_handler:
  请求 body 最终交给的 PushWorkHandler

instance_id / namespace / servicegroup_name / portname_name:
  构造 tracing span 和健康状态所需的身份字段

system_health:
  register 后 Ready，unregister 时 NotReady

inflight / notify:
  HTTP handler 已接受但 handle_payload 尚未完成的请求数和等待通知
```

HTTP 路由形状是：

```text
{PGD_HTTP_RPC_ROOT_PATH}/{portname}
```

默认 root 是：

```text
/v1/rpc
```

`bind_and_start()` 会把 root path 拼成 Axum catch-all 路由：

```text
format!("{}/{{*portname}}", rpc_root_path)
```

这意味着 `/v1/rpc/generate`、`/v1/rpc/health`、`/v1/rpc/load_lora` 都可以落到同一个 `handle_shared_request`，再由 `portname_path` 查 `handlers`。

HTTP server 的网络栈是：

```text
TcpListener
  -> accept loop
  -> TokioIo
  -> Axum Router.into_service()
  -> TowerToHyperService
  -> hyper_util Http2Builder::serve_connection
```

`TraceLayer::new_for_http()` 提供通用 HTTP tracing，而 `handle_shared_request` 里还会从 headers 解析 Pagoda 关心的 trace 字段：

```text
traceparent
tracestate
x-request-id
x-pagoda-request-id
```

这些字段被放进 `handle_payload` span，后续跨到 `PushWorkHandler` 时仍然能关联到原始 HTTP request。

HTTP 的请求处理流程是：

```text
1. Axum 从 path 提取 portname_path
2. handlers.get(portname_path)
3. 找不到则返回 404 Portname not found
4. 找到后 inflight += 1
5. clone handler 状态
6. spawn task 调 service_handler.handle_payload(body)
7. 立即返回 202 Accepted
8. task 完成后 inflight -= 1，并 notify
```

HTTP 的 ack 语义和 TCP/NATS 一样：  
它只表示 request plane 已接收请求，不表示业务响应完成。

不过 HTTP 与 TCP 的错误反馈形态不同。  
TCP read loop 可以在同一条 TCP request/ack 协议里返回 error response；HTTP 这里在 handler 已找到后直接返回 `202 Accepted`，后续 `handle_payload` 失败只写 warn 日志，不再改变已经发出的 HTTP ack。  
只有 portname path 找不到时，HTTP handler 会同步返回 `404`。

`register_portname` 也采用“先可路由，后 Ready”的顺序：

```text
1. 构造 PortnameHandler
2. insert handlers
3. system_health.set_portname_health_status(portname_name, Ready)
```

trait 层 HTTP 实现把 `portname_name` 同时作为 route subject 和健康状态名。  
这和 TCP 的 `{instance_id:x}/{portname_name}` 不同：HTTP 这里没有在 route key 里拼 instance id，因此更接近“一个 HTTP server 下 portname name 唯一”的模型。

`unregister_portname(subject, portname_name)` 会先从 `handlers` 移除 portname，再设置 NotReady，并等待该 portname 的 inflight 请求清零。  
此外 `wait_for_inflight()` 可以遍历所有 handler，等待整个 HTTP server 下所有 portname 的 inflight 清零。

`address()` 返回：

```text
http://ip:port
```

如果已经 bind，就用 `actual_addr`；否则用配置的 `bind_addr`。  
`is_healthy()` 当前恒为 true，源码里也留了 TODO，未来可以检查 listener 是否仍活跃。  
`VERSION` 常量只是暴露 crate 版本，目前不参与 request-plane 路由。

### `ingress/nats_server.rs` 和 `ingress/push_portname.rs`

`NatsMultiplexedServer` 把 NATS 服务端也适配成 `RequestPlaneServer`。

它注册 portname 时会：

1. 根据 namespace/servicegroup 找到 NATS service group
2. 构造带 instance id 的 portname 名称
3. 创建 `PushPortname`
4. spawn 一个 portname 消费循环
5. 保存 portname cancellation token 和 join handle

`NatsMultiplexedServer` 的字段含义是：

```text
nats_client:
  NATS 连接对象
  当前主要用于持有连接生命周期

servicegroup_registry:
  从 namespace/servicegroup 找到对应 NATS service group

handlers:
  portname_name -> PortnameTask
  用于 unregister 时取消对应消费循环

cancellation_token:
  server 级取消 token
```

`PortnameTask` 是每个 NATS portname 的运行时句柄：

```text
cancel_token:
  取消 PushPortname::start 的 select loop

join_handle:
  等待 portname task 退出，捕获 panic

_portname_name:
  保留 portname 名，主要用于可读性/调试
```

NATS 注册时先把 runtime 的 namespace/servicegroup 映射成 NATS service group 名：

```text
service_name_raw = "{namespace}_{servicegroup_name}"
service_name = Slug::slugify(service_name_raw)
registry.services[service_name].group(service_name)
```

如果 registry 里找不到对应 service，会直接返回错误。  
这说明 NATS server 注册 portname 的前提是 servicegroup service group 已经先存在。

真正的 NATS portname 名会带上 instance id：

```text
{portname_name}-{instance_id:x}
```

例如：

```text
generate-a1
load_lora-a1
```

这个格式和其他 NATS 路由/发现代码里的 instance subject 习惯一致，用来区分同一个 portname 的不同实例。

注册过程中还有几条错误边界：

```text
service_group.portname(...) 失败:
  无法创建 NATS service portname

PushPortname::builder().build() 失败:
  必要字段缺失或 builder 状态不完整

PushPortname::start(...) 返回 Err:
  portname task 记录 error 日志
```

`register_portname` 在 spawn portname task 后会 sleep 10ms。  
这是为了减少竞态：如果 discovery 先公布 portname，而 NATS portname 消费循环还没真正开始，前端可能立刻发请求但服务端还没 ready。

`PushPortname` 是 NATS 单 portname 的消费循环。  
它做的事情和 HTTP/TCP server 层一致：

- `portname.next()` 等请求
- 先 respond 空 ack
- spawn task 调 `handle_payload`
- 维护 inflight
- shutdown 时可等待 inflight 完成

`PushPortname` 用 builder 创建，字段很少：

```text
service_handler:
  NATS request payload 最终交给的 PushWorkHandler

cancellation_token:
  控制 portname 消费循环退出

graceful_shutdown:
  默认 true
  退出时是否等待 inflight 清零
```

`start(...)` 的入口参数补足了 portname 身份：

```text
portname:
  async_nats service portname，提供 next()/respond()/stop()

namespace / servicegroup_name / portname_name / instance_id:
  span、日志、健康状态和 service identity

system_health:
  start 时设 Ready，退出循环后设 NotReady
```

主循环使用 `tokio::select!`，并带 `biased`：

```text
req = portname.next():
  收到一个 NATS request

cancellation_token.cancelled():
  调 portname.stop().await，然后退出循环
```

收到 request 后，NATS 路径会先调用：

```text
req.respond(Ok("".into())).await
```

也就是先返回空 ack，再 spawn task 调 `handle_payload`。  
如果 respond 失败，只记录 warn，因为这通常表示请求方已经关闭或超时；当前实现仍会继续处理 payload。

每条 request 的处理 task 会：

```text
1. inflight += 1
2. 从 NATS headers 构造 handle_payload span
3. 调 service_handler.handle_payload(req.message.payload)
4. 记录成功或失败日志
5. inflight -= 1
6. notify.notify_one()
```

如果 `portname.next()` 返回 `None`，循环退出。  
退出后先把 portname health 设置为 NotReady，再根据 `graceful_shutdown` 决定是否等待 inflight 清零：

```text
graceful_shutdown = true:
  等 inflight 全部完成

graceful_shutdown = false:
  直接跳过等待
```

`NatsMultiplexedServer::unregister_portname` 会从 `handlers` 删除对应 task，cancel token，然后 await join handle。  
如果 task panic，unregister 不会 panic，而是记录 warn 后继续完成注销。

`address()` 当前返回固定字符串：

```text
nats://connected
```

这不是一个可拨号的完整服务地址，而是占位式状态描述。  
原因是 `async_nats::Client` 没有直接暴露这里想要的 server URL。  
`is_healthy()` 当前也恒为 true，含义接近“持有 NATS client 且假设可用”。

从设计角度看，HTTP/TCP/NATS 三个服务端实现虽然 I/O 方式不同，但都收敛到同一个边界：

```text
transport-specific server
  -> PushWorkHandler::handle_payload(Bytes)
```

更细地说，三者的 ack 时机并不完全一样：

```text
TCP:
  work item 成功进入 bounded queue 后 ack

HTTP:
  portname handler 命中并 spawn handle_payload 后返回 202

NATS:
  收到 request 后先 respond 空 ack，再 spawn handle_payload
```

这些差异都仍属于 request-plane ack。  
它们共同不表示业务响应完成；真实响应仍由 `push_handler.rs` 通过 response plane 写回。

源码出处：

- `lib/runtime/src/pipeline/network/ingress.rs`
- `lib/runtime/src/pipeline/network/ingress/unified_server.rs`
- `lib/runtime/src/pipeline/network/ingress/shared_tcp_portname.rs`
- `lib/runtime/src/pipeline/network/ingress/http_portname.rs`
- `lib/runtime/src/pipeline/network/ingress/nats_server.rs`
- `lib/runtime/src/pipeline/network/ingress/push_portname.rs`

---

## 8.5 `ingress/push_handler.rs`：payload 到本地图的落地器

`push_handler.rs` 是后端入站路径里最关键的文件。  
它为：

```rust
Ingress<SingleIn<T>, ManyOut<U>>
```

实现：

```rust
PushWorkHandler
```

它的职责不是监听网络，而是把 server 层交进来的 payload 恢复成一次本地 pipeline 调用。

这个文件里有三类实体：

```text
WorkHandlerMetrics:
  work handler 级别的请求数、耗时、inflight、bytes、错误和取消指标

RequestMetricsGuard:
  RAII guard，保证所有返回路径都会递减 inflight 并记录耗时

PushWorkHandler impl:
  真正的 payload 解码、本地图调用和 response plane 写回
```

`WorkHandlerMetrics` 把 portname 的 metrics factory 固化成一组具体 Prometheus 指标：

```text
request_counter:
  handle_payload 被调用的请求总数

request_duration:
  handle_payload 从入口到退出的总耗时

inflight_requests:
  当前正在处理的 payload 数

request_bytes:
  收到的 request payload bytes

response_bytes:
  写回 response plane 的 response bytes

error_counter:
  按 error_type 维度计数

cancellation_total:
  response stream / client 取消相关计数
```

`WorkHandlerMetrics::from_portname(portname, metrics_labels)` 的意义是把指标标签绑定到具体 portname。  
server 层只知道 `Arc<dyn PushWorkHandler>`，但 handler 层能通过 portname 的 metrics API 创建带 namespace/servicegroup/portname 语义的指标。

`RequestMetricsGuard` 只保存三样东西：

```text
inflight_requests
request_duration
start_time
```

它的 `Drop` 做两件事：

```text
inflight_requests.dec()
request_duration.observe(elapsed)
```

这样即使 `handle_payload` 在解码失败、反序列化失败、response stream 创建失败、generate 失败、publish 失败等路径提前返回，也不会让 inflight gauge 泄漏，耗时也仍会被记录。

`add_metrics(...)` 和 `set_portname_health_check_notifier(...)` 是 `PushWorkHandler` trait 需要的附加能力。  
前者委托给 `Ingress::add_metrics`；后者把 health check manager 的 `Notify` 写进 `portname_health_check_notifier`。  
这个 notifier 使用一次性设置语义，重复设置会返回：

```text
Portname health check notifier already set
```

核心流程：

1. 记录 work handler metrics
2. 用 `TwoPartCodec` 解码 payload
3. 要求消息必须是 `HeaderAndData`
4. 从 header 解析 `RequestControlMessage`
5. 从 data 反序列化业务请求 `T`
6. 用控制头里的 id 重建 `Context<T>`
7. 根据 `connection_info` 创建 TCP response publisher
8. 调用 `segment.generate(request)`
9. 发送 `ResponseStreamPrologue`
10. 逐项读取业务响应流
11. 包装成 `NetworkStreamWrapper<U>`
12. 发送正常结束帧 `complete_final = true`
13. 通知 health check timer

`handle_payload(payload)` 入口会先记录两个时间：

```text
t2_wallclock_ns:
  backend 收到 payload 后的 wall-clock 时间
  用于和 control message 里的 frontend_send_ts_ns 做 request-plane transit 估算

start_time:
  本地 Instant
  用于 work handler 总耗时和 TTFR 指标
```

然后如果 metrics 已配置，会立即：

```text
request_counter.inc()
inflight_requests.inc()
request_bytes.inc_by(payload.len())
创建 RequestMetricsGuard
```

接下来用：

```text
TwoPartCodec::default().decode_message(payload)
```

把 server 层交来的 raw bytes 恢复成 `TwoPartMessage`。  
这里要求消息必须是：

```text
HeaderAndData(header, data)
```

因为远程请求主链路需要：

```text
header:
  RequestControlMessage JSON

data:
  业务请求 T 的 JSON
```

如果不是 `HeaderAndData`，会计入 `INVALID_MESSAGE`，并返回 `PipelineError::Generic`。  
如果 header 不能反序列化成 `RequestControlMessage`，会计入 `DESERIALIZATION`，同时把原始 JSON 字符串放进错误消息，便于定位控制头漂移。  
业务请求 `T` 的反序列化失败则直接向上传播 serde 错误转换后的 pipeline error。

`RequestControlMessage` 解析成功后有两个关键用途。

第一，用 control header 的 id 重建请求上下文：

```text
Context::with_id(request, control_msg.id)
```

这保证后端本地图执行时仍使用前端 request id，响应流、日志和取消语义都能继续挂在同一个上下文上。

第二，用 `connection_info` 创建 response plane publisher：

```text
tcp::client::TcpClient::create_response_stream(
  request.context(),
  control_msg.connection_info,
  cancellation_total_metric
)
```

这里也是当前实现的边界：  
request plane 已经抽象为 HTTP/TCP/NATS，但 response plane publisher 仍固定是 TCP client。  
如果创建 response stream 失败，会计入 `RESPONSE_STREAM`，并返回 `PipelineError::Generic`。

`frontend_send_ts_ns` 当前也有一条指标路径：

```text
control_msg.frontend_send_ts_ns:
  如果存在，则记录 T2 - T1 到 WORK_HANDLER_NETWORK_TRANSIT_SECONDS
```

不过前面 `AddressedPushRouter` 当前主要把发送时间写进 transport header。  
因此这里是控制头字段路径，和 `SharedTcpServer` 从 headers 里读 `x-frontend-send-ts-ns` 的路径并存。

调用本地图执行时：

```text
self.segment.get().expect("segment not set").generate(request).await
```

如果 `segment` 没设置会直接 panic，这属于 pipeline 构建期不变量失败；如果 `generate` 返回错误，则计入 `GENERATE`，包装成 `PipelineError::GenerateError`。

`generate` 成功后，handler 会先发送 response stream prologue：

```text
publisher.send_prologue(None).await
```

这告诉前端 response plane：后端已经成功创建业务响应流，可以开始接收后续 item。  
此时会记录：

```text
WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS
```

注意这里的 “first response” 更接近“后端成功拿到 response stream 并发送 prologue”，不一定等同于第一个业务 token bytes。

如果 `generate` 失败，handler 会发送：

```text
publisher.send_prologue(Some(error_string)).await
```

也就是把失败原因通过 response plane 的 prologue 带回前端。  
debug 构建下日志会包含更详细的 debug backtrace，release 下只打印错误字符串。

业务响应流发送阶段是一个循环：

```text
while let Some(resp) = stream.next().await {
  NetworkStreamWrapper {
    data: Some(resp),
    complete_final: false,
  }
  serde_json::to_vec(...)
  publisher.send(...)
}
```

每个业务 item 都先包成 `NetworkStreamWrapper<U>`。  
这个 wrapper 是前端区分“有一条业务数据”和“流正常结束”的协议对象。

如果发送业务响应失败，代码会把 `send_complete_final` 设为 false，并根据 context 状态区分：

```text
context.is_stopped():
  可能是前端已经取消或连接关闭
  记录 warn

context 还没 stop:
  视为异常 publish failure
  记录 error，并调用 context.stop_generating()
```

无论是哪种 publish 失败，都会计入 `PUBLISH_RESPONSE`。  
源码里也明确提示：这个 metric 可能因为正常取消场景而偏大。

如果业务流自然结束，并且前面没有 publish 失败，则发送 final frame：

```text
NetworkStreamWrapper::<U> {
  data: None,
  complete_final: true,
}
```

这就是前端 `AddressedPushRouter` 中 `complete_final` 判断的来源。  
如果 final frame 发送失败，会计入 `PUBLISH_FINAL`。

最后，如果 portname health check notifier 已设置，正常发送 final 后会：

```text
notifier.notify_one()
```

这不是业务响应的一部分，而是告诉 health check manager：这个 portname 刚刚完成了一次真实流，可以推迟下一次 canary health check。

它和 server 层的关系是：

```text
SharedTcpServer / SharedHttpServer / PushPortname:
  只负责 transport 收包、ack、排队、并发和生命周期

PushWorkHandler implementation:
  负责协议恢复、本地图调用和响应流写回
```

它和 response plane 的关系是：

```text
RequestControlMessage.connection_info
  -> TcpClient::create_response_stream(...)
  -> StreamSender
  -> send_prologue()
  -> send(NetworkStreamWrapper bytes)
```

这里也暴露了一个当前实现边界：  
虽然 request plane 已经支持 HTTP/TCP/NATS，但响应流创建处仍然写死 TCP：

```text
tcp::client::TcpClient::create_response_stream(...)
```

所以 `PushWorkHandler` 是“transport server 抽象”和“TCP response plane 固定实现”相遇的地方。

源码出处：

- `lib/runtime/src/pipeline/network/ingress/push_handler.rs`

---

## 8.6 `egress/`：发现、选路、发送和 ack

`egress` 子模块也分两层：

```text
request client 层:
  HTTP/TCP/NATS 统一成 RequestPlaneClient

routing 层:
  PushRouter 选择 worker
  AddressedPushRouter 执行已知地址的一次远程调用
```

### `egress.rs`

和 `ingress.rs` 类似，它只负责声明子模块。  
实际逻辑在各个具体文件中。

当前 egress namespace 可以按两类文件看：

```text
unified_client.rs:
  request-plane client trait 与 ClientStats

http_router.rs / tcp_client.rs / nats_client.rs:
  三种 request-plane client 实现

push_router.rs:
  worker discovery、路由策略、故障反馈和负载跟踪

addressed_router.rs:
  已知目标地址后的远程调用编排
```

也就是说，`egress/` 不是单纯“发网络包”的目录。  
它从“选哪个 worker”一直覆盖到“用哪个 request-plane transport 发 payload 并等待 ack”。

### `egress/unified_client.rs`

`RequestPlaneClient` 是 request plane 客户端统一接口。

核心方法：

- `send_request(address, payload, headers)`
- `transport_name()`
- `is_healthy()`
- `stats()`
- `close()`

注意 `send_request` 返回的是 ack bytes。  
注释里已经写明：streaming response 不通过这个返回值回来，而是走 TCP response plane。

所以这个 trait 的语义是：

```text
把请求送到远端 request plane，并等一个 transport-level acknowledgment
```

不是：

```text
完成一次完整业务 RPC
```

`RequestPlaneClient` 的输入参数也有明确分层：

```text
address:
  transport-specific 地址
  HTTP 是完整 URL，TCP 是 host:port[/portname_path]，NATS 是 subject

payload:
  已编码的 workload bytes
  通常是 AddressedPushRouter 编出来的 TwoPartMessage bytes

headers:
  request-plane metadata
  包括 trace headers、x-frontend-send-ts-ns 等
```

`Headers` 只是 `HashMap<String, String>` 的类型别名。  
这样 HTTP 可以直接转成 HTTP headers，NATS 可以转成 NATS header map，TCP 可以序列化进 `TcpRequestMessage.headers`。

`ClientStats` 是可选观测接口，不是所有 transport 都能填满：

```text
requests_sent / responses_received / errors:
  请求、ack、错误计数

bytes_sent / bytes_received:
  request payload 和 ack payload 字节数

active_connections / idle_connections:
  主要服务连接池型 transport

avg_latency_us:
  当前很多实现没有真实维护
```

`ClientStats::is_available()` 只检查 `requests_sent > 0` 或 `active_connections > 0`。  
因此 stats 是否“可用”不等于所有字段都准确；它只是表示这个 client 至少暴露了一些运行时信号。

`close()` 默认是空实现。  
NATS client 依赖底层 client 生命周期，HTTP client 基本无显式 close，TCP request client 当前也没有覆盖 close。  
所以这个方法是 trait 预留能力，不是当前所有实现都拥有完整 graceful close。

### `egress/http_router.rs`

`HttpRequestClient` 用 `reqwest` 发送 HTTP request-plane 请求。

它负责：

- 读取 HTTP/2 和 request timeout 配置
- 构造 HTTP client
- POST 到目标地址
- 注入 headers
- 检查 HTTP status
- 返回 response body 作为 ack

`Http2Config` 聚合了 HTTP request-plane 的配置：

```text
max_frame_size:
  PGD_HTTP2_MAX_FRAME_SIZE，默认 1MB

max_concurrent_streams:
  PGD_HTTP2_MAX_CONCURRENT_STREAMS，默认 1000

pool_max_idle_per_host:
  PGD_HTTP2_POOL_MAX_IDLE_PER_HOST，默认 100

pool_idle_timeout:
  PGD_HTTP2_POOL_IDLE_TIMEOUT_SECS，默认 90s

keep_alive_interval:
  PGD_HTTP2_KEEP_ALIVE_INTERVAL_SECS，默认 30s

keep_alive_timeout:
  PGD_HTTP2_KEEP_ALIVE_TIMEOUT_SECS，默认 10s

adaptive_window:
  PGD_HTTP2_ADAPTIVE_WINDOW，默认 true

request_timeout:
  PGD_HTTP_REQUEST_TIMEOUT，默认 5s
```

不过 `with_config` 当前只把其中一部分真正传给 `reqwest::Client::builder()`：

```text
pool_max_idle_per_host
pool_idle_timeout
request_timeout
```

源码注释也说明，高级 HTTP/2 配置不一定在当前 reqwest 版本里稳定可用。  
所以读这段配置时要区分“配置结构已表达”和“builder 当前实际生效”的范围。

发送时，`send_request` 做的是：

```text
POST address
Content-Type: application/octet-stream
body = payload
for headers:
  req.header(key, value)
```

如果 `req.send()` 失败，会包装成 `PagodaError`，错误类型是 `CannotConnect`。  
如果 HTTP status 不是 2xx，则直接 `bail!`，错误信息包含 status 和 response body。  
这和 TCP/NATS 都包装成 `CannotConnect` 的风格不完全一致。

`transport_name()` 返回：

```text
http2
```

不是 `"http"`。  
这会出现在日志和 `AddressedPushRouter` 的 trace 字段里。

健康检查当前比较轻量：client 创建成功后基本视为 healthy。
`Default` 会调用 `new().expect(...)`，因此如果 reqwest client 构造失败，默认构造会 panic，而不是返回 `Result`。

### `egress/tcp_client.rs`

`TcpRequestClient` 是 TCP request plane 客户端。  
它和 `tcp/client.rs` 名字相近，但角色完全不同。

```text
egress/tcp_client.rs:
  request plane client
  负责发送请求 payload，等待 ack

tcp/client.rs:
  response plane call-home client
  负责从 worker 连回 frontend，持续写响应流
```

`TcpRequestClient` 的核心设计是连接池：

- 按 `SocketAddr` 分池
- 过滤不健康连接
- 写路径在调用线程预编码 `TcpRequestMessage`
- 每条连接有 writer task 和 reader task
- request/ack 通过 FIFO 顺序配对
- bounded channel 提供背压

TCP request client 的配置是 `TcpRequestConfig`：

```text
request_timeout:
  PGD_TCP_REQUEST_TIMEOUT，默认 5s

pool_size:
  PGD_TCP_POOL_SIZE，默认每个地址 100 条连接

connect_timeout:
  PGD_TCP_CONNECT_TIMEOUT，默认 5s

channel_buffer:
  PGD_TCP_CHANNEL_BUFFER，默认每条连接 writer channel 50
```

另一个相关环境变量是：

```text
PGD_TCP_MAX_MESSAGE_SIZE:
  TcpResponseCodec 读 ack 时使用
  默认 32MB
```

`TcpRequest` 是写入连接前的内部工作单元：

```text
encoded_data:
  已经用 TcpRequestMessage::encode() 编好的 Bytes

response_tx:
  caller 等待 ack 的 oneshot sender
```

设计上它把编码放在调用方线程完成，而不是放到单一 writer task 里。  
这样多个并发请求可以并行编码，writer task 只负责顺序写 socket。

每个 `TcpConnection` 拆成两个 task：

```text
writer_task:
  从 bounded mpsc 接收 TcpRequest
  write_all(encoded_data)
  写成功后把 response_tx 转交给 reader task

reader_task:
  用 FramedRead<TcpResponseCodec> 读取 ack
  按 FIFO 顺序把 ack 发送给下一个 response_tx
```

这里没有给每个 request 额外写 request id。  
request 和 ack 的对应关系依赖同一条 TCP 连接上的 FIFO 顺序：writer 每成功写一条 request，reader 就按同样顺序等待一个 `TcpResponseMessage`。

socket 层会做低延迟配置：

```text
TCP_NODELAY:
  禁用 Nagle，减少小 ack/request 延迟

recv/send buffer:
  设置为 2MB

tcp-low-latency feature:
  可选启用 TCP_QUICKACK 和 SO_BUSY_POLL
```

地址解析支持：

```text
tcp://host:port
host:port
host:port/portname_path
```

如果地址里带 portname path，会写入：

```text
x-portname-path
```

`SharedTcpServer` 再用这个 path 查 handler。

更准确地说，`send_request(address, payload, headers)` 会先解析地址。  
如果地址包含 `/portname_path`，就把这个 portname 写入：

```text
headers["x-portname-path"]
```

随后 `TcpConnection::send_request` 要求这个 header 必须存在，因为 TCP request envelope 的第一段就是 `portname_path`。  
缺少它会直接返回错误。

成功路径：

```text
1. stats.requests_sent += 1
2. stats.bytes_sent += payload.len()
3. 从连接池取健康连接，没有则新建
4. timeout(conn.send_request(...), request_timeout)
5. 成功读到 ack 后 stats.responses_received += 1
6. stats.bytes_received += ack.len()
7. TCP_BYTES_RECEIVED_TOTAL += ack.len()
8. 健康连接放回 pool
```

失败路径：

```text
conn.send_request 返回 Err:
  stats.errors += 1
  TCP_ERRORS_TOTAL += 1
  包装为 PagodaError(CannotConnect)
  不把连接放回 pool

timeout:
  stats.errors += 1
  TCP_ERRORS_TOTAL += 1
  包装为 PagodaError(CannotConnect, "... timed out")
  不把连接放回 pool
```

`TcpConnectionPool` 以 `SocketAddr` 为 key。  
取连接时会丢弃不健康连接；还连接时如果 pool 已满，就直接丢弃连接，让 task 随连接 drop 清理。

`TcpRequestClient::stats()` 会返回请求、ack、错误和字节计数，但当前 `active_connections`、`idle_connections`、`avg_latency_us` 都是 0。  
`is_healthy()` 当前恒为 true，只表示 client 对象可用；真正的连接健康在 `TcpConnection.healthy` 上维护。

### `egress/nats_client.rs`

`NatsRequestClient` 把 `async_nats::Client` 适配成 `RequestPlaneClient`。

它负责：

- 把 generic headers 转成 NATS headers
- 调 `request_with_headers`
- 返回 NATS response payload 作为 ack
- NATS 错误转成 `CannotConnect` 类错误

发送流程是：

```text
1. 把 HashMap<String, String> headers 转成 async_nats::HeaderMap
2. client.request_with_headers(address, headers, payload).await
3. 成功时返回 response.payload
```

失败时会：

```text
NATS_ERRORS_TOTAL{error_type="request_failed"} += 1
返回 PagodaError(CannotConnect, "NATS request to ... failed")
```

它的 `is_healthy()` 当前恒为 true，因为底层 NATS client 没有直接暴露所需状态。
`stats()` 也只能返回一个很粗的信号：如果 healthy，则 `active_connections = 1`，其他计数为 0。  
`close()` 是空实现，连接生命周期交给 `async_nats::Client` 自己管理。

源码出处：

- `lib/runtime/src/pipeline/network/egress.rs`
- `lib/runtime/src/pipeline/network/egress/unified_client.rs`
- `lib/runtime/src/pipeline/network/egress/http_router.rs`
- `lib/runtime/src/pipeline/network/egress/tcp_client.rs`
- `lib/runtime/src/pipeline/network/egress/nats_client.rs`

---

## 8.7 `egress/push_router.rs`：worker 选择和故障反馈

`PushRouter<T, U>` 是面向调用方的远程路由 engine。  
它实现：

```rust
AsyncEngine<SingleIn<T>, ManyOut<U>, Error>
```

所以在 pipeline 图里，它可以像一个普通 engine 一样被调用。

`PushRouter` 的字段体现了它的两层职责：

```text
client:
  从 discovery 获取 portname instances、transport 地址和可用性状态

router_mode:
  决定本次请求如何选 worker

round_robin_counter:
  RoundRobin 模式的轻量计数器

addressed:
  已知地址后的下游执行器 AddressedPushRouter

busy_threshold:
  可选的 busy 检测阈值

fault_detection_enabled:
  是否在连接/断流/engine shutdown 类错误时 report_instance_down

occupancy_state:
  P2C / LeastLoaded 用的共享 inflight 计数
```

它内部真正发送请求的是：

```text
AddressedPushRouter
```

`PushRouter` 自己主要负责：

- 从 discovery client 获取可用 worker instances
- 根据 `RouterMode` 选择 instance
- 为选中的 instance 找到 transport address
- 包装成 `AddressedRequest<T>`
- 调 `AddressedPushRouter.generate(...)`
- 遇到连接/断流/engine shutdown 类错误时 `report_instance_down`

构造路径有三种：

```text
from_client(client, mode):
  默认 fault detection 开启
  不启用 busy threshold

from_client_with_threshold(client, mode, busy_threshold, worker_monitor):
  可启用 busy 检测
  如果传入 WorkerLoadMonitor，会先 start_monitoring()

from_client_no_fault_detection(client, mode):
  不 report_instance_down
  direct() 使用原始 instance_ids，而不是过滤后的 instance_ids_avail
```

三种构造都会调用内部 `addressed_router(portname)`。  
这个函数从 runtime 的 `NetworkManager` 创建 request-plane client，并从 runtime 拿 TCP response-plane server：

```text
req_client = network_manager.create_client()
resp_transport = portname.drt().tcp_server().await
AddressedPushRouter::new(req_client, resp_transport)
```

所以 `PushRouter` 自身不直接知道当前 request-plane 是 HTTP/TCP/NATS。  
它只拿到一个 `Arc<dyn RequestPlaneClient>`，真正发送在 `AddressedPushRouter` 里完成。

`RouterMode` 包括：

- `RoundRobin`
- `Random`
- `PowerOfTwoChoices`
- `LeastLoaded`
- `Direct`
- `KV`

各模式语义是：

```text
RoundRobin:
  用 round_robin_counter 在 instance_ids_avail() 上取模

Random:
  在 instance_ids_avail() 上随机取一个

PowerOfTwoChoices:
  随机取两个候选，比较 occupancy_state.load，选较低者

LeastLoaded:
  在所有可用 instance 中精确选择当前 load 最小者

Direct:
  需要外部显式传 instance_id，普通 generate() 不允许直接使用

KV:
  给 KV routing 外部逻辑使用，不应该调用 PushRouter.generate()
```

其中 `PowerOfTwoChoices` 和 `LeastLoaded` 依赖 occupancy state。  
代码里用 `OccupancyPermit` / `OccupancyTrackedStream` 确保一次请求对应的 occupancy 能在错误返回或响应流 drop 时归还。

这点很重要：  
远程请求不是 `generate()` 返回时就结束。  
对于 `ManyOut<U>`，请求生命周期一直延续到响应流被消费完或被 drop。

所以 occupancy 不能只在 `generate()` 的 future 完成时减少，而要绑定到返回的 stream 生命周期。

这里有两个 guard：

```text
OccupancyPermit:
  在选中 worker 并 increment 后创建
  如果 generate_with_fault_detection 早期返回错误，Drop 会 decrement

OccupancyTrackedStream:
  generate 成功后包住 ManyOut<U>
  stream drop 时 decrement
```

`PowerOfTwoChoices` 的 `p2c_select_from` 逻辑是：

```text
候选数 = 1:
  直接返回唯一 worker

候选数 > 1:
  随机取两个不同 index
  比较 occupancy load
  返回 load 较小者，tie 时选第一个
```

`LeastLoaded` 则通过 `select_exact_min_and_increment` 做精确最小选择并同时 increment。  
因此它比 P2C 更准确，但也更依赖共享 occupancy state 的一致性。

`select_next_worker()` 和 `peek_next_worker()` 是给外部逻辑使用的轻量选择 API：

```text
RoundRobin:
  select 会递增 counter，peek 不递增

Random:
  select / peek 都是一次随机选择
  peek 不保证和下一次 select 相同

PowerOfTwoChoices / LeastLoaded / Direct:
  返回 None，因为这些模式需要请求生命周期或显式 instance id

KV:
  直接 panic，表示调用方用错了模式
```

busy threshold 是另一层前置拒绝逻辑。  
如果开启 fault detection 且配置了 `busy_threshold`，`generate_with_fault_detection` 会先看 `client.instance_ids_free()`。  
如果 free 列表为空，但 raw instances 不为空，就返回：

```text
PipelineError::ServiceOverloaded("All workers are busy, please retry later")
```

这和“没有任何 instance”不同。  
前者表示 worker 存在但当前都忙，后者表示 discovery 根本没有可路由实例。

真正发请求前，`generate_with_fault_detection` 会根据 discovery instance 的 `TransportType` 取地址：

```text
TransportType::Http(url):
  address = url
  nvtx label = transport.http.request

TransportType::Tcp(addr):
  address = addr
  nvtx label = transport.tcp.request

TransportType::Nats(subject):
  address = subject
  nvtx label = transport.nats.request
```

然后把业务请求包装成：

```text
AddressedRequest<T> {
  request,
  address,
}
```

并交给 `AddressedPushRouter.generate(...)`。

路由阶段会记录：

```text
STAGE_DURATION_SECONDS["route"]
```

这段耗时只覆盖选择 instance、查 transport 地址、包装 addressed request 的前置阶段，不包括 request plane 发送和等待 response plane。

故障反馈由 `is_inhibited` 决定。  
它匹配错误链中的这些类型：

```text
CannotConnect
Disconnected
ConnectionTimeout
Backend(EngineShutdown)
```

有两条触发路径：

```text
AddressedPushRouter.generate(...) 直接返回 Err:
  如果 fault detection 开启且错误 inhibited
  report_instance_down(instance_id)

generate 成功返回 stream，但流中的 item 带 error:
  如果 U::err() 返回错误且错误 inhibited
  report_instance_down(instance_id)
```

第二条路径很重要。  
远程流式请求可能在 `generate()` 已经成功返回后才断开，因此故障反馈不能只看 future 的返回值，还要包装返回的 stream，在 item 级别检查错误。

`KV` 和 `Direct` 也体现了一个边界：

- `KV` 不是为了直接调用 `PushRouter.generate()`
- `Direct` 需要显式 instance id，不能走普通 generate

因此这两个模式在普通 `AsyncEngine::generate` 里会直接报错。

源码出处：

- `lib/runtime/src/pipeline/network/egress/push_router.rs`

---

## 8.8 `egress/addressed_router.rs`：已知地址后的远程调用主桥

`AddressedPushRouter` 是 `PushRouter` 选好 worker 之后的下一层。  
到了这一层，系统已经知道“请求要发到哪里”，剩下的问题不是再选路，而是把一次本地 `generate()` 变成完整的远程往返。

它同时站在三个边界上：

```text
pipeline 边界:
  对上实现 AsyncEngine<SingleIn<AddressedRequest<T>>, ManyOut<U>>
  继续保持 generate() 编程模型

request plane 边界:
  通过 Arc<dyn RequestPlaneClient> 把请求发到远端

response plane 边界:
  通过 TcpStreamServer 先注册回流通道，再等待远端 CallHome
```

所以它虽然叫 router，但这里的核心不是“选择哪个 worker”。  
真正的 worker 选择在 `PushRouter` 完成；`AddressedPushRouter` 的设计重点是协调两条平面：

```text
先准备 response plane:
  TcpStreamServer.register(StreamOptions)
  得到 ConnectionInfo + StreamProvider

再发送 request plane:
  RequestControlMessage 带上 ConnectionInfo
  TwoPartCodec 编码控制头和业务请求
  RequestPlaneClient.send_request(...)

最后恢复 pipeline response:
  等 StreamProvider 完成
  读取 StreamReceiver.rx
  解包 NetworkStreamWrapper<U>
  返回 ManyOut<U>
```

这种顺序不能反过来。  
如果先发 request，远端 `Ingress` 收到后不知道响应应该写回哪里；即使知道地址，也可能前端还没注册好 subject，CallHome 配对会失败。

`AddressedPushRouter` 还把请求级上下文从本地图模型带到网络协议里：

```text
SingleIn<AddressedRequest<T>>
  -> request.transfer(())
  -> 保留原 Context
  -> RequestControlMessage.id = context.id()
  -> ResponseStream::new(..., engine_ctx)
```

这保证了远程响应流回到前端后，仍然带着原来的 request context。  
从调用方看，远端调用仍然像本地 `ManyOut<U>`，取消、stop、指标和阶段耗时也还能挂在同一个上下文上。

本文件里的几个小实体都服务这个主链路，而不是独立的路由策略。

`AddressedRequest<T>` 是 `PushRouter` 和 `AddressedPushRouter` 之间的交接对象：

```text
request:
  原始业务请求 T

address:
  PushRouter 已经选出的目标 worker 地址
```

`AddressedRequest::new(request, address)` 只负责封装这两个值。  
`into_parts()` 是 `pub(crate)`，说明拆包只给当前 crate 的网络层使用；对上层调用方来说，它仍然只是一个带地址的 pipeline request。

`AddressedPushRouter::new(req_client, resp_transport)` 则把两条平面的运行时依赖固定下来：

```text
req_client:
  Arc<dyn RequestPlaneClient>
  隐藏 HTTP/TCP/NATS 等 request plane 具体实现

resp_transport:
  Arc<TcpStreamServer>
  当前 response plane 仍固定使用 TCP streaming
```

返回 `Arc<Self>` 是为了匹配 router 在 pipeline 图和 async task 中被共享的使用方式；构造函数本身不做网络连接，只收口依赖对象。

`RequestType` 和 `ResponseType` 是控制头里的协议枚举。  
它们使用 `snake_case` 序列化，避免 Rust enum 命名泄漏到 wire format。  
虽然枚举里预留了 `ManyIn` / `SingleOut`，但当前 `AddressedPushRouter.generate` 只构造：

```text
request_type = SingleIn
response_type = ManyOut
```

这和 trait 实现的类型完全一致：

```rust
AsyncEngine<SingleIn<AddressedRequest<T>>, ManyOut<U>, Error>
```

因此这里不是通用的所有远程调用形态，而是明确服务 “single request -> streaming response” 的远程生成路径。

`RequestControlMessage` 是 `TwoPartMessage` header 里的控制面数据：

```text
id:
  request context id，用于把远程响应和前端上下文继续关联

request_type / response_type:
  本次远程调用的输入输出形态

connection_info:
  后端应该如何 CallHome 到前端 response plane

frontend_send_ts_ns:
  协议字段已存在，但当前这里写 None
```

它和业务请求体分开编码，是为了让后端 `push_handler` 先理解“如何接回响应流”，再把 data 部分恢复成具体业务类型 `T`。

这里还有两个容易混淆的点。

第一，`RequestPlaneClient::send_request(...)` 的返回值不是业务响应。  
它只是 request plane ack；真正业务响应来自 `StreamReceiver`。

第二，当前时间戳有两条路径：

```text
headers["x-frontend-send-ts-ns"]:
  AddressedPushRouter 发送前写入
  SharedTcpServer worker 侧可用它计算 request-plane transit

RequestControlMessage.frontend_send_ts_ns:
  字段存在
  但 AddressedPushRouter 当前构造时写的是 None
```

所以读指标逻辑时不能把这两者当成同一条数据路径。  
这是当前实现演进中的痕迹：协议对象里预留了字段，但实际观测主要走 transport header。

`generate()` 内部可以按阶段理解。

第一阶段是 inflight 和上下文准备：

```text
queue_start = Instant::now()
REQUEST_PLANE_INFLIGHT.inc()
InflightGuard::new()
request.transfer(())
AddressedRequest::into_parts()
```

`InflightGuard` 是为了防止 gauge 泄漏。  
`generate()` 在创建最终 `ResponseStream` 之前有多处 `?` 可能提前返回：序列化失败、codec 失败、request plane 发送失败、等待 response stream 失败等。  
如果这些错误发生在 `REQUEST_PLANE_INFLIGHT.inc()` 之后、返回 stream 之前，`InflightGuard::drop` 会负责 `dec()`。

成功路径则在最终返回前调用 `inflight_guard.disarm()`，把责任交给 `InflightDecStream`。  
这是因为 `ManyOut<U>` 的生命周期不是 `generate()` future 完成时结束，而是响应流被消费完或被 drop 时结束：

```text
generate() 早已返回:
  远程 stream 仍可能继续产生 token/result

InflightDecStream::drop:
  响应流生命周期结束时再 REQUEST_PLANE_INFLIGHT.dec()
```

`InflightDecStream<S>` 的 `Stream` 实现只把 `poll_next` 转发给 inner stream。  
它不改变业务数据，只把指标生命周期绑定到 stream drop。

第二阶段是 response plane 注册。  
代码构造 `StreamOptions` 时关闭 request stream、打开 response stream：

```text
enable_request_stream(false)
enable_response_stream(true)
```

这对应当前唯一成熟路径 `SingleIn -> ManyOut`。  
`TcpStreamServer.register(options)` 返回 `PendingConnections` 后，代码要求拆出来的形态必须是：

```text
(None, Some(recv_stream))
```

否则直接 `panic!`。  
这不是普通输入错误，而是 response plane 注册结果违反了本路径的不变量：single-input 请求不应该需要 request stream，但必须有 response stream。

第三阶段是构造 request payload。  
`RequestControlMessage` 被序列化成 ctrl bytes，业务请求 `T` 被序列化成 data bytes，然后用：

```text
TwoPartMessage::from_parts(ctrl, data)
TwoPartCodec::encode_message(...)
```

压成 request plane 要发送的 payload。  
这里的 NVTX range `codec.encode` 用来把序列化/编码成本从后面的 transport 发送成本里拆出来。

第四阶段是 request plane 发送。  
发送前调用 `inject_trace_headers_into_map` 注入已有 tracing metadata，然后在真正 transport write 之前写入 `x-frontend-send-ts-ns`。  
这个时间点刻意放在 TwoPart 编码之后，目的是让 worker 侧用它估算 request-plane transit 时，不把前端序列化/编码耗时算进去。

`req_client.send_request(address, buffer, headers).await` 只等待 request plane ack。  
ack 成功说明远端 request plane 接收或排队成功，不说明业务响应已经开始。

第五阶段是等待 response plane 接通并恢复 `ManyOut<U>`。  
`response_stream_provider.await` 失败会被分成两类：

```text
provider 被丢弃:
  PipelineError::DetachedStreamReceiver

CallHome/连接失败:
  PipelineError::ConnectionFailed
```

拿到 `response_stream.rx` 后，代码用 `ReceiverStream` 包成 stream，再用 `StreamNotifyClose` 区分“收到 None”和“正常结束标记已经出现”的语义。

响应流中的每个 bytes 都应该是：

```text
NetworkStreamWrapper<U>
```

解析成功后有三种情况：

```text
data = Some(value):
  输出一个 U

data = None 且 complete_final = true:
  正常结束，不再输出 item

data = None 且 complete_final = false:
  协议异常，转成 U::from_err(...)
```

如果已经看到 `complete_final` 后又收到数据，也会转成错误 item。  
如果 JSON 反序列化失败，当前策略不是直接让 stream panic，而是记录 warn，并把错误包装成 `U::from_err(...)` 交给下游。

底层 channel 关闭时也分三类：

```text
已经 complete_final:
  正常 EOF

engine_ctx.is_stopped():
  用户或上层 stop_generating()，按取消后的正常结束处理

否则:
  ErrorType::Disconnected
  表示 stream 在 generation 完成前意外断开
```

这也是 `MaybeError` 约束存在的原因之一：远程流里的错误需要以 `U` 的错误形态继续沿着 `ManyOut<U>` 输出。

几个指标的时间窗口也不一样：

```text
REQUEST_PLANE_QUEUE_SECONDS:
  从 generate 入口到 TwoPart 编码完成

REQUEST_PLANE_SEND_SECONDS:
  从准备发送到 request plane ack 返回

REQUEST_PLANE_ROUNDTRIP_TTFT_SECONDS:
  从 tx_start 到第一条业务响应 bytes 到达

STAGE_DURATION_SECONDS["transport_roundtrip"]:
  从 queue_start 到第一条业务响应 bytes 到达
```

因此不能只看一个“网络耗时”指标。  
这里同时拆出了前端排队/编码、request plane 发送与 ack、以及到首个业务响应的端到端等待。

`pagoda
_nvtx_range!` 的三个范围也对应这个拆分：

```text
codec.encode:
  TwoPart 编码成本

transport.tcp.send:
  request plane send_request 成本

transport.tcp.wait_backend:
  等待后端 CallHome / response stream provider 完成
```

这里的 `transport.tcp.*` 名称是历史遗留命名。  
即使 request plane 使用 HTTP 或 NATS，`AddressedPushRouter` 里这两个 NVTX range 名字仍然带 `tcp`；它们实际标记的是“request plane send”和“等待后端 response plane 接通”两个阶段，不一定表示当前 request-plane transport 真的是 TCP。

源码出处：

- `lib/runtime/src/pipeline/network/egress/addressed_router.rs`

---

## 8.9 `tcp/`：response plane 的 CallHome 机制

`network/tcp` 这一组文件容易和 `egress/tcp_client.rs` 混淆。  
它们都叫 TCP，但属于两条不同平面：

```text
network/egress/tcp_client.rs:
  TCP request plane
  前端 -> 后端
  发请求 payload，等 ack

network/tcp/server.rs + network/tcp/client.rs:
  TCP response plane
  后端 -> 前端
  后端连回前端，持续写 ManyOut 响应流
```

### `tcp.rs`

`tcp.rs` 定义 response plane 的连接描述：

```text
TcpStreamConnectionInfo:
  address
  subject
  context
  stream_type
```

它可以转成通用的 `ConnectionInfo`：

```text
ConnectionInfo {
  transport: "tcp_server",
  info: JSON(TcpStreamConnectionInfo)
}
```

这个设计把“回流地址是什么 transport、具体怎么连接”塞进一个类型擦除的协议字段里。  
`RequestControlMessage` 不需要知道 TCP 细节，只需要携带 `ConnectionInfo`；真正解释它的是后端的 `tcp::client::TcpClient::create_response_stream(...)`。

`TcpStreamConnectionInfo` 的字段含义是：

```text
address:
  前端 TcpStreamServer 的监听地址，形如 host:port

subject:
  本次 response stream 的一次性配对 key

context:
  前端 request context id
  后端 create_response_stream 会校验它和本地 context.id() 一致

stream_type:
  Request 或 Response
  当前成熟主链路使用 Response
```

`From<TcpStreamConnectionInfo> for ConnectionInfo` 会把结构体序列化成 JSON，并写入：

```text
transport = "tcp_server"
info = JSON(TcpStreamConnectionInfo)
```

`TryFrom<ConnectionInfo> for TcpStreamConnectionInfo` 会反向校验 `transport == "tcp_server"`。  
如果 request control header 里带了别的 transport，后端 TCP response client 会直接拒绝，而不是尝试用错误协议解释。

`CallHomeHandshake` 是 TCP response plane 的第一帧控制消息：

```text
subject:
  告诉 TcpStreamServer 这条新连接要配对哪个 pending stream

stream_type:
  告诉 TcpStreamServer 这是 Request stream 还是 Response stream
```

它不是业务协议的一部分，而是 transport-specific 的连接配对协议。  
编码上它作为 `TwoPartMessage::from_header(...)` 发送，也就是 HeaderOnly 消息。

### `tcp/server.rs`

`TcpStreamServer` 是前端 response plane 的监听者。  
它不是 request plane server，不负责接收业务请求；它负责等远端 worker 连回来。

服务端启动前先通过 `ServerOptions` 决定地址：

```text
port:
  默认 0，表示让 OS 分配空闲端口

interface:
  可选网卡名
  如果指定，则从 list_afinet_netifas() 中找对应 IP
```

如果没有指定 interface，`TcpStreamServer::new_with_resolver` 会先尝试 `local_ip()`，失败且错误是 `LocalIpAddressNotFound` 时再尝试 `local_ipv6()`。  
如果仍然找不到任何可路由 IP，就回退到 `127.0.0.1`；但其他解析错误会直接返回 `PipelineError::Generic`。  
这个策略避免在机器没有外部地址时无法启动，同时也避免把真实配置错误静默降级成 loopback。

`State` 是 server 的配对表：

```text
tx_subjects:
  subject -> RequestedSendConnection
  对应 enable_request_stream

rx_subjects:
  subject -> RequestedRecvConnection
  对应 enable_response_stream

handle:
  tcp_listener task 的 JoinHandle
```

当前远程生成主路径只使用 `rx_subjects`。  
`tx_subjects` 和 `StreamType::Request` 代表未来或不成熟的 request-stream 路径，`process_request_stream()` 目前还是空实现。

注册流程是：

```text
AddressedPushRouter:
  StreamOptions { enable_response_stream: true }
  -> TcpStreamServer.register(options)
  -> 在 rx_subjects 里放入 RequestedRecvConnection
  -> 返回 RegisteredStream<StreamReceiver>
  -> RegisteredStream.connection_info 被写入 RequestControlMessage
```

远端 worker 收到请求后，会拿着这个 `connection_info` 连回 `TcpStreamServer`。  
新 TCP 连接的第一条消息是 `CallHomeHandshake`，里面带 subject 和 stream type。  
`TcpStreamServer` 用 subject 去 `rx_subjects` 找之前注册的等待者，配对成功后再进入真正的数据转发。

`register(StreamOptions)` 实际支持两条注册路径：

```text
enable_request_stream = true:
  生成 sender_subject
  插入 tx_subjects
  返回 send_stream: RegisteredStream
  stream_type = Request

enable_response_stream = true:
  生成 receiver_subject
  插入 rx_subjects
  返回 recv_stream: RegisteredStream
  stream_type = Response
```

`AddressedPushRouter` 当前构造的 options 是：

```text
enable_request_stream(false)
enable_response_stream(true)
```

所以它只期望 `(None, Some(recv_stream))`。  
如果未来启用 request stream，需要补齐 `process_request_stream()` 和上游语义，否则只是注册了一个当前没有实现完整处理路径的 subject。

`start()` 会 spawn `tcp_listener`，并通过 oneshot 把真实端口传回 `new_with_resolver`。  
端口为 0 时，真实端口来自 `listener.local_addr()`，这也是 `TcpStreamConnectionInfo.address` 能写入正确端口的原因。

accept loop 每接收一条连接，会做几个低层处理：

```text
set_nodelay(true):
  禁用 Nagle，降低控制帧/小数据帧延迟

set_linger(0):
  关闭时尽快释放连接

spawn handle_connection:
  每条 CallHome 连接独立处理
```

accept 失败只记录 warn 并继续循环。  
源码里也留了 instrumentation TODO：accepted connections、inflight connections、incoming/outgoing bytes 等指标还没有完整实现。

这里的 `oneshot + mpsc` 分工很典型：

```text
oneshot:
  通知 AddressedPushRouter：远端已经连回来，StreamReceiver 准备好了

mpsc:
  承载后续持续到来的响应 bytes
```

这正好对应 response plane 的两个阶段：

```text
连接配对完成:
  一次性事件

响应流传输:
  多项数据事件
```

`ResponseStreamPrologue` 也在这里发挥作用。  
远端 worker 连回来后，不是立刻把 `StreamReceiver` 交给上游，而是先等一个 prologue：

```text
prologue(None):
  后端 segment.generate() 成功，响应流可以开始读

prologue(Some(error)):
  后端 generate 阶段失败，前端 generate 应该直接失败
```

这让“TCP 已连接成功”和“业务响应流已创建成功”这两个事件分开。  
如果没有 prologue，前端只能知道网络通了，却不知道远端 engine 是否已经成功进入 streaming 阶段。

`process_stream` 是每条 CallHome socket 的入口。  
它先把 socket 拆成 read/write half，并用 `TwoPartCodec` 包成 framed reader/writer；然后读取第一条消息：

```text
message 1:
  HeaderOnly(CallHomeHandshake JSON)
```

如果连接在第一帧前关闭，返回 “Connection closed without a ControlMessage”。  
如果第一帧没有 header，返回 “Expected ControlMessage, got DataMessage”。  
如果 handshake 反序列化失败，返回显式错误。

`process_response_stream` 是当前主路径。  
它做的是：

```text
1. 用 handshake.subject 从 rx_subjects remove RequestedRecvConnection
2. 等第二帧 ResponseStreamPrologue
3. 如果 prologue.error = Some(error)，oneshot 返回 Err(error)
4. 如果 prologue 成功，创建 mpsc channel
5. oneshot 返回 Ok(StreamReceiver { rx })
6. spawn network_send_handler
7. spawn network_receive_handler
8. 等两个 task 结束
```

注意第 1 步是 `remove`，不是 `get`。  
subject 是一次性配对 key，CallHome 成功后就不应该再被第二条连接复用。

`network_receive_handler` 负责从 worker socket 读响应 bytes，转发给 `StreamReceiver.rx`：

```text
收到 data:
  response_tx.send(data)

收到 HeaderOnly(Sentinel):
  正常 shutdown

response_tx.closed():
  前端不再接收，向 worker 发 Kill

context.killed():
  向 worker 发 Kill

context.stopped():
  向 worker 发 Stop，且只发送一次

socket EOF:
  记录 trace，退出
```

这里的 Stop/Kill 是前端 response plane 对后端 writer 的反向控制。  
也就是说，response plane 不是单纯“后端写、前端读”的单向字节管道；它还有一条控制消息通道，把前端取消语义传回 worker。

`network_send_handler` 则反向读取 `control_rx`，把 `ControlMessage::Stop` / `Kill` 写回 worker socket。  
它不允许收到 `Sentinel`，因为 Sentinel 只能由 worker writer 在正常结束时发给 server。  
control channel 关闭后，它会 flush 并 shutdown socket。

`process_control_message` 目前只接受：

```text
ControlMessage::Sentinel:
  返回 ControlAction::Shutdown
```

如果 server receive 侧收到 Stop/Kill，会视为内部协议错误并返回 fatal error。  
这和方向有关：Stop/Kill 应该由 server 发给 client，Sentinel 应该由 client 发给 server。

### `tcp/client.rs`

`TcpClient::create_response_stream(...)` 在后端 worker 侧调用。  
它做的是“CallHome”：

```text
解析 ConnectionInfo
校验 stream_type == Response
校验 context id 匹配
连接前端 TcpStreamServer
发送 CallHomeHandshake
启动 reader task 接收 Stop/Kill 控制消息
启动 writer task 把 StreamSender 收到的 bytes 写到 socket
返回 StreamSender 给 push_handler
```

`TcpClient` 自身只有一个 `worker_id` 字段，目前基本是预留/调试性质。  
真正的 response stream 创建主要依赖传入的 `ConnectionInfo` 和 `AsyncEngineContext`。

`connect(address)` 有一个特殊重试逻辑：  
如果连接错误是 `AddrNotAvailable`，它会每 200ms 线性退避重试；其他错误直接返回。  
连接成功后会 `set_nodelay(true)`。

`create_response_stream` 的前置校验有三层：

```text
ConnectionInfo -> TcpStreamConnectionInfo:
  transport 必须是 tcp_server，info 必须能解析成 JSON

stream_type:
  必须是 Response

context:
  info.context 必须等于当前 context.id()
```

这些校验防止 worker 把响应流连到错误 request 或错误 stream 类型上。

连接建立后，client 会先启动 reader task，再发送 handshake：

```text
message 1:
  HeaderOnly(CallHomeHandshake { subject, stream_type: Response })
```

随后创建 `bytes_tx/bytes_rx`，spawn writer task，并返回：

```text
StreamSender {
  tx: bytes_tx,
  prologue: Some(ResponseStreamPrologue { error: None })
}
```

`push_handler` 拿到这个 `StreamSender` 后，才会调用 `send_prologue(None)` 或 `send_prologue(Some(error))`。  
因此 CallHome handshake 和 response prologue 是两帧不同消息：

```text
message 1:
  CallHomeHandshake
  用于 socket 和 subject 配对

message 2:
  ResponseStreamPrologue
  用于告诉前端业务 stream 是否创建成功
```

这里和 `Context` 的联动很关键。  
前端如果停止读取或发出 stop/kill，response plane 会把控制消息传回后端，后端 `TcpClient` 的 reader task 再调用：

```text
context.stop()
context.kill()
```

也就是说，取消不是简单关闭 socket。  
它会尽量回到第 5 节的请求控制模型里，让后端 pipeline 有机会按同一套 `Controller` 语义停止生成。

`handle_reader` 只接受 header-only 控制消息：

```text
ControlMessage::Stop:
  cancellation counter 最多 inc 一次
  context.stop()

ControlMessage::Kill:
  cancellation counter 最多 inc 一次
  context.kill()

ControlMessage::Sentinel:
  panic
  因为 Sentinel 不应该由 server 发给 client

framed stream EOF:
  认为前端关闭连接
  如果此前没有 Stop/Kill，也计一次 cancellation
```

reader 还监听 `alive_tx.closed()`。  
writer task 结束时会 drop 对应 alive channel，让 reader 不必一直阻塞等待控制消息。

`handle_writer` 从 `bytes_rx` 读取 `TwoPartMessage` 并写到 socket：

```text
bytes_rx 收到消息:
  framed_writer.send(msg)

context.killed() / context.stopped():
  不发送 Sentinel，直接结束

bytes_rx 关闭:
  正常结束，可以发送 Sentinel
```

writer task 正常结束时会发送 `ControlMessage::Sentinel`。  
这个 sentinel 是 response plane 内部的流结束控制消息，和 `NetworkStreamWrapper.complete_final` 不是同一层：

```text
NetworkStreamWrapper.complete_final:
  业务响应流层的正常结束标记
  AddressedPushRouter 用它判断是否完整

ControlMessage::Sentinel:
  TCP response plane 的 socket 关闭协调信号
  TcpStreamServer 用它结束转发任务
```

client 还有一个容易忽略的收尾流程。  
`create_response_stream` 内部 spawn 的 monitor task 会 `join!(reader_task, writer_task)`；如果两者都正常结束，它会把 read/write halves `unsplit` 回一个 `TcpStream`，然后最多等 10 秒读取到 server FIN。  
这用于让 server 侧先关闭 socket，避免 client 过早结束造成连接收尾噪音。

当前实现仍有一些尖锐边界：

```text
handle_reader 收到非法 control message:
  TODO(#171)，当前 panic

handle_reader 解码失败:
  TODO(#171)，当前 panic

handle_writer 发送失败:
  不发送 Sentinel，直接结束

StreamSender::send_prologue:
  只能发送一次 prologue，重复调用会触发内部不变量错误
```

这些都说明 response plane 的控制协议目前更偏“内部可信路径”，对异常 peer 或协议漂移的容错还没有完全产品化。

### `tcp/test_utils.rs`

这个文件提供测试用 TCP pair。  
它不承担生产逻辑，但它说明这组 response plane 代码有大量细节依赖真实 socket 行为，单纯用 channel mock 很难覆盖。

测试覆盖并不只在 `test_utils.rs`。  
`tcp.rs` 里有 client/server 端到端 CallHome 测试；`tcp/client.rs` 有大量针对 reader/writer 的单元测试；`tcp/server.rs` 还覆盖了 IP 解析回退、默认 server 创建和 response stream 注册。  
这些测试对应的不是业务模型，而是 TCP response plane 的控制消息、socket 收尾和注册配对不变量。

源码出处：

- `lib/runtime/src/pipeline/network/tcp.rs`
- `lib/runtime/src/pipeline/network/tcp/server.rs`
- `lib/runtime/src/pipeline/network/tcp/client.rs`
- `lib/runtime/src/pipeline/network/tcp/test_utils.rs`

---

## 8.10 `manager.rs`：request plane 的资源总控

前面几节讲的是单次请求。  
`NetworkManager` 解决的是另一个层面的问题：长期运行的 runtime 里，网络资源由谁创建、谁复用、谁持有。

它是 request plane 的单一配置入口：

```text
读取环境变量
根据 RequestPlaneMode 选择 HTTP / TCP / NATS
创建 RequestPlaneServer
创建 RequestPlaneClient
维护同进程共享 server
记录实际绑定端口
```

这个集中化不是为了“把代码放到一个文件”。  
它主要解决同进程多 worker 的正确性问题。

如果每个 worker 都自己创建 HTTP/TCP server，会出现这种情况：

```text
worker A:
  bind 到 port A
  注册 handler A

worker B:
  bind 到 port B
  注册 handler B

discovery:
  只能发布一个实际端口
```

结果就是调用方可能打到一个 server，但目标 portname handler 注册在另一个 server 上。  
`GLOBAL_TCP_SERVER` / `GLOBAL_HTTP_SERVER` 的作用就是避免这种分裂：同一进程内所有 worker 共享同一个 request plane server。

`ACTUAL_TCP_RPC_PORT` / `ACTUAL_HTTP_RPC_PORT` 也和 discovery 联动。  
当配置端口为 `0` 时，OS 会分配真实端口；这个端口必须被记录下来，后续发布 transport address 时才能告诉其它进程“真正应该连哪里”。

`GLOBAL_*_SERVER_TOKEN` 的设计也很有针对性。  
全局 server 的 accept loop 不能被某个 runtime/servicegroup 的 drop 误杀，否则 `OnceCell` 还会继续返回已经死亡的 server。  
所以全局 server 使用进程级 token，而不是绑定到某个局部 runtime 生命周期。

层次关系可以这样看：

```text
DistributedRuntime:
  持有 NetworkManager
  对外提供 network_manager()

Portname 启动:
  从 NetworkManager 拿 RequestPlaneServer
  register_portname(handler = Ingress)

PushRouter 创建:
  从 NetworkManager 创建 RequestPlaneClient
  从 DRT 获取 response-plane TcpStreamServer
  组合成 AddressedPushRouter
```

这说明 `NetworkManager` 不是单次请求链路中的一环，而是单次请求能稳定发生的前置资源层。  
它把 transport mode、端口、共享 server 和 client 构造收口，避免这些运行时资源散落到 servicegroup、router、portname 各处。

源码出处：

- `lib/runtime/src/pipeline/network/manager.rs`

---

## 8.11 与 `servicegroup` / discovery / pipeline 图的整体连动

`network` 子模块真正的价值，要放进整套 runtime 里看。

### 后端 worker 启动侧

后端 worker 本地先有一个可执行 pipeline：

```text
SegmentSource
  -> ServiceBackend / PipelineOperator / Backend preprocessing
  -> SegmentSource
```

然后用 `Ingress` 把它暴露给 request plane：

```text
Ingress:
  持有 SegmentSource
  实现 PushWorkHandler

RequestPlaneServer:
  register_portname(portname_name, Arc<dyn PushWorkHandler>)
```

这样外部请求进来时，不会绕过 pipeline 图直接调用 engine。  
它仍然通过 `SegmentSource.generate(...)` 进入第六节的图执行模型。

### 前端调用侧

前端不应该关心某个 worker 是 HTTP、TCP 还是 NATS。  
它通过 servicegroup/discovery 拿到可用 instances，再由 `PushRouter` 选择一个 instance。

`PushRouter` 选择完 instance 后，会取出该 instance 发布的 transport address：

```text
TransportType::Http(...)
TransportType::Tcp(...)
TransportType::Nats(...)
```

这个 address 再被包进 `AddressedRequest`，交给 `AddressedPushRouter`。  
因此 discovery 决定“有哪些 worker 和它们的地址”，`PushRouter` 决定“这次选哪个 worker”，`AddressedPushRouter` 决定“已知地址后如何完成一次远程 streaming generate”。

### 和 pipeline 图的关系

对 pipeline 图来说，远程调用最终仍被压缩成一个 `AsyncEngine`：

```text
PushRouter<T, U>:
  AsyncEngine<SingleIn<T>, ManyOut<U>, Error>

AddressedPushRouter:
  AsyncEngine<SingleIn<AddressedRequest<T>>, ManyOut<U>, Error>

Egress:
  AsyncEngine<Req, Resp, Error>
```

这就是整套设计的核心一致性：  
无论执行发生在本地还是远端，上层都尽量继续使用 `generate()`、`Context`、`ResponseStream`、`ManyOut` 这些 pipeline 概念。

`network` 做的不是把一套 RPC 框架硬塞进 pipeline，而是把远程调用翻译回 pipeline 自己已经有的抽象。

---

## 8.12 metrics、health 和取消控制为什么分散在多个层

`network` 里的指标和健康状态不是集中在一个对象里，因为不同层观察的是不同事件。

request plane 观察的是：

```text
请求是否进入远端 server
排队耗时
send_request ack 耗时
inflight request-plane 请求数
transport bytes/errors
```

work handler 观察的是：

```text
payload 是否成功解码
segment.generate() 是否成功
响应 bytes 写回多少
业务处理持续多久
取消次数
```

routing 观察的是：

```text
哪个 worker 被选中
worker 是否忙
是否需要 report_instance_down
occupancy 何时增加和归还
```

response plane 观察的是：

```text
CallHome 是否配对成功
prologue 是否成功
响应流是否完整结束
stop/kill 是否传回后端 context
```

这些指标不能简单合并。  
例如 request plane ack 成功，只说明请求进入远端处理队列；后端 engine 可能随后失败，response plane 也可能中途断开。  
如果只打一类“RPC success/failure”指标，会丢掉这几个阶段之间非常重要的故障边界。

health 也是类似的。  
portname 注册成功后，server 会把 portname health 置为 ready；注销时置为 not ready，并等待 inflight 请求结束。  
`Ingress` 的 `portname_health_check_notifier` 则是另一层信号：响应流正常结束时通知健康检查逻辑延后 canary。  
前者是 portname 生命周期状态，后者是请求完成对健康检查节奏的反馈。

取消控制同样跨层：

```text
前端调用方停止读取 / stop_generating
  -> response plane 发现 channel/socket 状态
  -> ControlMessage::Stop / Kill
  -> 后端 TcpClient reader task
  -> Context.stop() / Context.kill()
  -> 本地 pipeline 按 Controller 语义停止
```

这条链路说明网络层并不是只负责“断连接”。  
它尽量把网络侧的关闭事件翻译回 pipeline 的请求控制模型。

---

## 8.13 当前边界、TODO 和测试覆盖

这一组代码已经形成了完整的远程 streaming 调用骨架，但仍有一些明显的演进边界。

### 已知边界

- response plane 仍然固定 TCP CallHome，尚未抽象成类似 `RequestPlaneClient` / `RequestPlaneServer` 的统一 trait。
- `NetworkStreamWrapper.complete_final` 是临时结束标记，源码里标注未来希望用 SSE 类机制替代。
- `StreamOptions.enable_request_stream` 还没有完整实现，当前成熟路径主要是 `SingleIn -> ManyOut`。
- `RequestControlMessage` 在 `network.rs` 和 `egress/addressed_router.rs` 中存在重复定义，长期看应该避免协议结构漂移。
- `tcp/client.rs` 和 `tcp/server.rs` 中仍有若干 TODO(#171) 的 fatal `panic!` 分支。
- HTTP/NATS 的健康检查目前偏轻量，有些地方接近“对象存在即 healthy”。
- `frontend_send_ts_ns` 既有控制头字段，又有 transport header 路径，当前实际使用上并不完全统一。

### 测试覆盖

测试比较集中在这些区域：

- `codec/two_part.rs`：二段消息编解码、checksum、partial frame 等。
- `codec.rs`：TCP request/response framing。
- `codec/zero_copy_decoder.rs`：零拷贝 request message 读取。
- `tcp.rs` / `tcp/client.rs` / `tcp/server.rs`：response plane CallHome 和控制消息局部行为。
- `ingress/shared_tcp_portname.rs`：inflight、unregister 等 TCP request-plane 生命周期。
- `egress/push_router.rs`：routing、occupancy、P2C、least-loaded 等策略。
- `egress/unified_client.rs`：client stats 的基础行为。

相对薄弱的是：

- `AddressedPushRouter` 的完整端到端行为。
- `Ingress::handle_payload` 与真实 response plane 的组合测试。
- `NetworkManager` 在多 worker / 多 runtime 生命周期下的资源复用行为。

这些测试缺口也反映了模块设计的复杂度：  
最关键的行为往往跨越 discovery、request plane、response plane、pipeline context 和 stream 生命周期，单文件单测很难完整表达。

---

## 8.14 第八节总结：`network` 层真正守住的几个不变量

把 `network` 子模块全部看完后，可以发现它真正维护的不是某个 transport 的细节，而是几组跨模块不变量。

### 第一，远程调用必须仍然长得像本地 pipeline 调用

从上层看，请求仍然是：

```text
AsyncEngine::generate(SingleIn<T>) -> ManyOut<U>
```

这就是为什么有：

```text
Egress:
  把远程调用包装成 AsyncEngine

PushRouter:
  把远程 worker discovery + routing 包装成 AsyncEngine

AddressedPushRouter:
  把已知地址后的远程往返包装成 AsyncEngine

Ingress:
  把网络 payload 落回 SegmentSource.generate()
```

如果没有这个不变量，network 层就会把 pipeline 图撕开，让上层到处感知“这里是远程调用”。  
当前设计尽量避免这一点：远程化发生在 `network` 层内部，上层继续使用第六节的图模型。

### 第二，请求身份和响应回流必须显式化

本地 `Frontend` 可以通过内存里的 `sinks` map、`Context` 和类型系统维护请求/响应关系。  
跨网络后，这些隐含关系必须变成协议字段：

```text
RequestControlMessage.id:
  恢复 Context 和请求身份

RequestControlMessage.request_type / response_type:
  恢复交互形态

RequestControlMessage.connection_info:
  告诉远端 response plane 怎么连回前端
```

这就是 `RequestControlMessage` 为什么是远程调用的最小控制协议。  
它不是“附加 metadata”，而是在跨进程环境里重建本地图执行语义的必要信息。

### 第三，request plane ack 和业务响应必须分层

request plane 的成功只说明：

```text
请求已经送到远端 request-plane server，并被接收或排队
```

业务响应是否成功，需要看 response plane：

```text
CallHome 是否成功
prologue 是否为 Ok
NetworkStreamWrapper 是否完整收到 complete_final
```

这也是为什么指标、错误和测试不能只围绕一个“RPC 成败”来看。  
这条调用链天然分阶段，每个阶段失败时表达的系统问题不同。

### 第四，transport 可替换性目前只完整覆盖 request plane

当前已经统一抽象的是：

```text
RequestPlaneServer
RequestPlaneClient
```

它们覆盖 HTTP/TCP/NATS 的请求面。

但 response plane 仍然是：

```text
TcpStreamServer
TcpClient::create_response_stream
```

这说明 `network` 层处在一个中间演进状态：请求面已经完成 transport 抽象，响应面还没有。  
理解源码时要记住这个不对称，否则很容易误以为所有 transport 都已经在两条方向上完全统一。

### 第五，网络层要把关闭和取消翻译回 pipeline 控制语义

网络连接断开、前端停止读取、后端发送失败，这些都不是纯 I/O 事件。  
它们会影响一次请求的生命周期。

所以 response plane 会尽量把这些事件翻译成：

```text
Context.stop()
Context.kill()
context.stop_generating()
```

这样后端业务逻辑看到的仍然是第 5 节的 `Controller` 语义，而不是底层 socket 错误。  
这也是 `network` 和 `Context` 设计连动最深的地方之一。

---

## 8.15 按一次请求贯穿所有 `network` 文件

如果把第八节所有模块放回一条真实请求链路，可以看到每个文件出现的位置并不是随机的。

### 前端准备阶段

前端首先不是编码请求，而是建立路由上下文：

```text
servicegroup::Client / discovery
  -> PushRouter
  -> RouterMode 选择 instance
  -> 得到 TransportType address
```

这里 `egress/push_router.rs` 的职责是把 distributed runtime 看到的 worker 集合，转换成一次具体请求要使用的目标 instance。  
它关心的是“选谁”和“失败后如何反馈给 discovery”，不是字节协议。

选出 instance 后，才进入：

```text
AddressedPushRouter
```

也就是 `egress/addressed_router.rs`。  
这一层已经不再关心负载均衡，它关心的是如何把“已知地址的一次调用”完整变成远程 streaming generate。

### 响应面先注册

`AddressedPushRouter` 首先调用 response plane：

```text
tcp/server.rs:
  TcpStreamServer.register(StreamOptions)
```

这个动作会创建一个 subject，并把等待者放入 `rx_subjects`。  
返回的 `RegisteredStream` 同时包含两类信息：

```text
connection_info:
  给远端看的回流地址

stream_provider:
  给本地 AddressedPushRouter 等待 CallHome 完成
```

这一步是整个远程调用成立的前提。  
因为后端拿到请求后，必须知道响应流应该写回哪里；前端也必须提前准备好等待这个回连。

### 请求面发送

随后 `AddressedPushRouter` 构造：

```text
RequestControlMessage
  id
  request_type
  response_type
  connection_info
```

再用 `TwoPartCodec` 把它和业务请求体合成 payload：

```text
codec/two_part.rs:
  header = RequestControlMessage JSON
  data   = business request JSON
```

接下来通过 `RequestPlaneClient` 发送：

```text
egress/unified_client.rs:
  RequestPlaneClient::send_request(address, payload, headers)
```

如果当前 request plane 是 TCP，还会再包一层：

```text
egress/tcp_client.rs:
  TcpRequestMessage {
    portname_path,
    headers,
    payload = TwoPartMessage bytes,
  }
```

也就是说，在 TCP request plane 上，真正发出的 bytes 是两层协议：

```text
TcpRequestMessage:
  用于 TCP 共享 server 路由到 portname

TwoPartMessage:
  用于 pipeline 远程调用协议
```

这正是 `codec.rs` 和 `codec/two_part.rs` 要分开的原因。

### 后端 request plane 接收

后端侧由 `NetworkManager` 创建并复用 request-plane server：

```text
manager.rs
  -> SharedTcpServer / SharedHttpServer / NatsMultiplexedServer
```

不同 transport 的入口不同：

```text
TCP:
  shared_tcp_portname.rs
  ZeroCopyTcpDecoder 读取 TcpRequestMessage
  portname_path 查 handler
  入 worker queue 后 ack

HTTP:
  http_portname.rs
  axum route 查 handler
  spawn handle_payload
  返回 202

NATS:
  nats_server.rs + push_portname.rs
  portname.next()
  respond 空 ack
  spawn handle_payload
```

但三者最终都收敛到同一个接口：

```text
PushWorkHandler::handle_payload(Bytes)
```

这就是 `ingress/unified_server.rs` 和 `PushWorkHandler` 的分工：  
server trait 统一 portname 注册和 transport 生命周期，handler trait 统一 payload 进入 pipeline 的方式。

### 后端恢复本地图

`ingress/push_handler.rs` 接到 payload 后，才开始理解 pipeline 协议：

```text
TwoPartCodec 解码
RequestControlMessage 解析
业务请求反序列化
Context::with_id(...)
TcpClient::create_response_stream(connection_info)
segment.generate(request)
```

这一步是 network 和第六节本地图执行层真正接上的地方。  
后端不是直接调用某个裸函数，而是重新进入：

```text
SegmentSource.generate()
```

所以远程 worker 内部依然遵守本地 pipeline 图的执行模型。

### 后端 CallHome 写响应

当 `push_handler` 拿到本地 `ManyOut<U>` 后，响应不是通过 request plane 返回。  
它走之前控制头里的 `connection_info`：

```text
tcp/client.rs:
  TcpClient::create_response_stream(...)
  连接前端 TcpStreamServer
  发送 CallHomeHandshake
  返回 StreamSender
```

随后：

```text
stream item
  -> NetworkStreamWrapper { data: Some(item), complete_final: false }
  -> StreamSender.send(...)

stream end
  -> NetworkStreamWrapper { data: None, complete_final: true }
  -> StreamSender.send(...)
```

这里 `NetworkStreamWrapper` 负责业务响应流是否完整结束。  
而 `ControlMessage::Sentinel` 负责 TCP response plane socket 是否可以收尾。  
两者处于不同层，不应该混在一起看。

### 前端恢复响应流

前端 `TcpStreamServer` 收到 CallHome 后：

```text
CallHomeHandshake.subject
  -> rx_subjects 找到 RequestedRecvConnection
  -> 等 ResponseStreamPrologue
  -> oneshot 交付 StreamReceiver
```

`AddressedPushRouter` 等到 `stream_provider` 完成后，把 `StreamReceiver.rx` 转成 `ResponseStream<U>`：

```text
读取 bytes
  -> serde_json::from_slice<NetworkStreamWrapper<U>>
  -> Some(data) 交给调用方
  -> complete_final=true 时正常结束
  -> socket 提前关闭但没有 complete_final 时注入 Disconnected error
```

到这里，一次远程调用才真正回到调用方看到的形态：

```text
ManyOut<U>
```

### 这条链路说明什么

这条完整路径能解释为什么 `network` 子模块会被拆成这么多文件：

```text
PushRouter:
  选择 worker，和 discovery 联动

AddressedPushRouter:
  协调 request plane 和 response plane

RequestPlaneClient / RequestPlaneServer:
  抽象 HTTP/TCP/NATS 的请求面

codec:
  分别服务 TCP routing 和 pipeline workload

PushWorkHandler:
  把 transport payload 落回 pipeline

TcpStreamServer / TcpClient:
  承担 ManyOut 响应流的 CallHome 回传

NetworkManager:
  把长期运行的 request-plane 资源收口
```

所以，理解 `network` 时最重要的不是记住哪个文件有哪个 struct，而是抓住这条主线：

```text
discovery 选 worker
  -> request plane 送 payload
  -> ingress 恢复 Context 和 SegmentSource
  -> response plane CallHome
  -> 前端恢复 ManyOut
```

每个模块都在这条链路上守住一个边界。  
这也是为什么这套代码看起来像 transport，但实际承担的是“把 pipeline 的本地执行语义跨进程复原”的职责。

---

## 九、这些设计分别解决了什么问题

把整个 `pipeline` 文档从头串起来，可以把核心抽象和它们回答的问题总结成下面这组对应关系。

### 请求语义层回答的问题

- `Context`
  - 解决“请求在不同阶段不断变形时，身份、取消和元数据如何不丢”
- `StreamContext`
  - 解决“响应进入流式阶段后，同一请求的上下文如何继续共享”
- `Controller`
  - 解决“请求级 stop / kill / 子链取消如何统一表达”
- `Registry`
  - 解决“哪些附加信息属于一次请求，但又不该污染业务 payload”
- `PipelineError`
  - 解决“图装配、配对、流生命周期、传输协议等故障如何保留结构化语义”

### 本地图执行层回答的问题

- `Source` / `Sink` / `Edge`
  - 解决“图中的数据推进和连接关系如何被显式建模”
- `Frontend`
  - 解决“图执行如何继续对外表现为普通 `AsyncEngine::generate`”
- `ServiceFrontend` / `SegmentSource`
  - 解决“同一入口机制在不同架构边界上如何拥有清晰名字”
- `SinkEdge`
  - 解决“响应路径如何像请求路径一样沿明确连接面回流”
- `ServiceBackend`
  - 解决“图尾如何落到真实本地业务引擎”
- `SegmentSink`
  - 解决“图装配和真实执行端绑定如何解耦”
- `PipelineNode`
  - 解决“轻量单向变换节点如何被低成本表达”
- `Operator` / `PipelineOperator`
  - 解决“同时理解请求和响应两条路径的双向中间层如何建模”

### 分布式传输层回答的问题

- `ConnectionInfo` / `ResponseService` / `RegisteredStream`
  - 解决“远端如何知道把响应流发回哪里，本地又如何等连接完成”
- `ResponseStreamPrologue`
  - 解决“远程 streaming `generate()` 什么时候才算真正成功”
- `ControlMessage`
  - 解决“取消、终止、结束信号如何与业务数据分离”
- `StreamOptions` / `PendingConnections`
  - 解决“不同 RPC 形态下，流资源需求组合不同”
- `RequestControlMessage`
  - 解决“远端如何知道这次调用的身份、交互形状和回流目标”
- `Egress`
  - 解决“让远程调用继续看起来像本地 engine”
- `Ingress`
  - 解决“让网络字节流重新落回本地 pipeline”
- `NetworkStreamWrapper`
  - 解决“当前协议下如何区分正常结束与异常断流”
- `AddressedPushRouter` / `PushWorkHandler`
  - 解决“前端推出去、后端接回来这条远程主链路如何闭环”
- `NetworkManager`
  - 解决“整套 request plane 资源由谁选型、创建、共享和持有”
- `RequestPlaneServer` / `RequestPlaneClient`
  - 解决“HTTP/TCP/NATS 如何在请求面暴露同一种注册和发送接口”
- `TcpRequestMessage` / `TcpResponseMessage`
  - 解决“共享 TCP request-plane server 如何做 portname multiplexing 和 ack”
- `TwoPartCodec`
  - 解决“控制头和业务体如何在网络 payload 中保持分离”
- `ZeroCopyTcpDecoder`
  - 解决“TCP request-plane 热路径如何避免重复复制大 payload”
- `PushRouter`
  - 解决“discovery 里的多个 worker 如何被选中、避开、下线和负载追踪”
- `TcpStreamServer` / `TcpClient`
  - 解决“ManyOut 响应流如何通过 CallHome 从后端连回前端”

---

## 十、为什么用了这些字段类型

## 10.1 为什么很多地方用 `OnceLock`

因为很多对象天然是：

- 初始化时绑定一次
- 运行期只读

典型例子：

- `Frontend.edge`
- `SinkEdge.edge`
- `Ingress.segment`
- `Ingress.metrics`
- `Ingress.portname_health_check_notifier`
- `SegmentSink.engine`
- `NetworkManager` 全局实际端口记录
- 全局 HTTP/TCP request-plane server 的一次性初始化

相比 `Mutex<Option<T>>`：

- 语义更准确
- 运行期开销更低
- 还能天然表达“重复设置是错误”

更重要的是，它跟这套系统的很多对象生命周期非常契合：

- 图在构建阶段连接
- 运行期只做只读使用

所以 `OnceLock` 不是“懒得写锁”，而是在把“先装配、后运行”的拓扑语义直接反映到字段类型里。

## 10.2 为什么很多地方用 `Arc`

因为网络层、pipeline、后台任务之间天然有并发共享需求。

例如：

- `Controller` 需要被多个节点共享
- `Ingress`、router、metrics 需要被多个 task 共享
- response stream 生命周期可能跨多个 task
- request-plane server 要在多个 portname/worker 间共享
- response-plane `TcpStreamServer` 要同时被多个 router 调用注册流

这也再次说明 `pipeline` 不是单线程、单栈帧式的局部处理模型。  
它从一开始就假设：

- 同一个请求会跨节点共享
- 同一个响应流会跨 task 存活
- 同一个控制状态需要被多方观察

## 10.3 为什么用 `oneshot` 和 `mpsc`

### `oneshot`

适合：

- 一次性交付结果

这里主要用在：

- `StreamProvider`
- `Frontend.generate()` 和响应回传配对

### `mpsc`

适合：

- 持续传输多个数据项

这里主要用在：

- `StreamSender`
- `StreamReceiver`

这个组合非常自然：

- 先用 `oneshot` 交付“流端点已经准备好”
- 再用 `mpsc` 真正传输流数据

这背后实际上是把两类完全不同的事件拆开了：

- “连接/配对完成”是一次性事件
- “流式数据传输”是持续事件

如果两者混在同一种通道模型里，接口会变得非常别扭。

## 10.4 为什么用 trait object 隔离 request plane

`NetworkManager` 对外返回的是：

```text
Arc<dyn RequestPlaneServer>
Arc<dyn RequestPlaneClient>
```

这不是为了追求抽象形式，而是为了让 runtime 其它部分不分裂出三套路径：

```text
HTTP portname 注册路径
TCP portname 注册路径
NATS portname 注册路径
```

如果 servicegroup/portname/router 直接依赖具体 transport 类型，那么每新增一种 request plane，就会在多个上层模块里增加分支。  
现在分支被限制在 `manager.rs` 和具体 transport 实现里，上层只处理“注册 handler”和“发送 payload”这两个稳定动作。

## 10.5 为什么 TCP request plane 和 TCP response plane 没有合成一个对象

虽然两者都基于 TCP，但它们解决的是不同问题：

```text
TCP request plane:
  前端主动连后端
  发送一包请求 payload
  等待 ack
  需要 portname routing、headers、连接池和背压

TCP response plane:
  后端主动连回前端
  持续写响应流
  需要 CallHomeHandshake、prologue、stop/kill 控制和 stream pairing
```

如果强行合并，会让一个对象同时承担两种相反方向、两种生命周期、两套协议状态。  
当前拆开后，名字上会有些相似，但设计边界更清楚：`egress/tcp_client.rs` 是 request plane，`tcp/client.rs` / `tcp/server.rs` 是 response plane。

---

## 十一、当前实现里还在演化中的痕迹

这个模块不是一个完全收口、注释与实现 100% 同步的成品。  
从源码可以看出它还处在持续收敛过程中，尤其是 request plane 已经统一抽象、response plane 仍然偏专用这条边界。

明显痕迹包括：

- 多处注释语法不顺，说明有草稿性质
- `RegisteredStream` 的注释提到想做 RAII 自动清理，但当前没有真正 `Drop`
- `StreamSender` 附近的注释在讨论它是不是更该叫 `ResponseStreamSender`
- `StreamOptions.enable_request_stream` 明确写了当前未完全实现
- `NetworkStreamWrapper` 旁边写着 TODO，说明是临时兼容层
- `ResponseService` 这个命名在 TCP server 侧也有“可能改名”的痕迹
- `network.rs` 和 `addressed_router.rs` 里存在控制消息结构的重复定义
- `tcp/client.rs` 和 `tcp/server.rs` 里仍有 TODO(#171) 的 fatal `panic!` 分支
- `RequestControlMessage.frontend_send_ts_ns` 和 transport header 时间戳路径并存
- HTTP/NATS health check 目前偏轻量，尚未形成和 TCP 一样细的健康语义
- `AddressedPushRouter` 明确写着 response plane 需要进一步 generic data plane 抽象

所以更准确地说：

- **核心协议骨架已经成立**
- **request plane 的 transport 抽象已经比较明确**
- **response plane、结束检测、错误收口和协议结构去重仍在演化**

这也意味着阅读这套设计时，最好把它理解成：

- 核心交互模型已经比较稳定
- 但某些局部实现仍带着过渡性命名、工程补洞和性能路径迭代痕迹

也正因此，这份文档更应该关注“为什么有这些抽象”和“它们在系统里承担什么角色”，而不是过度把当前每一个细节实现都当成最终定型方案。

---

## 十二、如果只用一句话记住每个核心对象

- `Context`：一次请求在 pipeline 里的正式形态
- `Registry`：跟着请求走、但不属于业务 payload 的附加仓库
- `Controller`：请求级控制中心
- `Source`：把数据推进给下游的连接面
- `Sink`：承接并处理上游数据的连接面
- `Edge`：只负责连接、不负责业务的边
- `Frontend`：把图执行折叠成 `generate()` 的入口桥
- `ServiceFrontend`：本地服务语义下的图入口
- `SegmentSource`：分段/远程落地语义下的图入口
- `SinkEdge`：响应路径上的极简回送出口
- `ServiceBackend`：图尾与真实本地 engine 的边界
- `SegmentSink`：支持延迟绑定执行端的图尾
- `PipelineNode`：只做单向局部变换的轻节点
- `Operator`：同时理解请求和响应两条路径的双向逻辑
- `PipelineOperator`：把双向逻辑变成可连接图节点的包装器
- `ConnectionInfo`：跨 transport 的回流地址描述
- `StreamOptions`：一次流资源注册需求的声明
- `RegisteredStream`：把远端回流信息和本地等待句柄绑在一起的注册结果
- `ResponseStreamPrologue`：远程响应流真正建立成功的第一条握手消息
- `StreamSender / StreamReceiver`：带协议语义的网络流端点
- `Egress`：把远程调用伪装成本地 engine 的出口适配器
- `Ingress`：把网络请求恢复成本地图入口的入口适配器
- `RequestControlMessage`：远程调用最小控制协议头
- `PushWorkHandler`：server 层和 pipeline 网络入口之间的统一收口
- `NetworkStreamWrapper`：当前协议里用于显式表达流结束的过渡包装
- `RequestPlaneServer`：HTTP/TCP/NATS request-plane 服务端的统一注册接口
- `RequestPlaneClient`：HTTP/TCP/NATS request-plane 客户端的统一发送接口
- `TcpRequestMessage`：TCP request-plane 的 portname routing 包装
- `TcpResponseMessage`：TCP request-plane 的 ack 包装
- `TwoPartCodec`：远程 workload 的控制头/业务体二段编码
- `ZeroCopyTcpDecoder`：Shared TCP server 热路径的零拷贝 request decoder
- `PushRouter`：基于 discovery 和负载策略选择远端 worker 的路由 engine
- `AddressedPushRouter`：前端远程主链路的协调桥
- `TcpStreamServer`：前端 response plane 的 CallHome 接收端
- `TcpClient`：后端 response plane 的 CallHome 发送端
- `NetworkManager`：network mode、transport 资源和共享 server 的总协调器

---

## 十三、推荐的源码对照阅读顺序

如果你想拿着这份设计文档去对源码，推荐顺序是：

1. `pipeline.rs`
2. `context.rs`
3. `registry.rs`
4. `error.rs`
5. `nodes.rs`
6. `nodes/sources/base.rs`
7. `nodes/sources/common.rs`
8. `nodes/sinks/base.rs`
9. `nodes/sinks/pipeline.rs`
10. `nodes/sinks/segment.rs`
11. `network.rs`
12. `network/codec/two_part.rs`
13. `network/codec.rs`
14. `network/codec/zero_copy_decoder.rs`
15. `network/ingress/unified_server.rs`
16. `network/egress/unified_client.rs`
17. `network/manager.rs`
18. `network/ingress/shared_tcp_portname.rs`
19. `network/ingress/http_portname.rs`
20. `network/ingress/nats_server.rs`
21. `network/ingress/push_portname.rs`
22. `network/ingress/push_handler.rs`
23. `network/egress/push_router.rs`
24. `network/egress/addressed_router.rs`
25. `network/egress/tcp_client.rs`
26. `network/egress/http_router.rs`
27. `network/egress/nats_client.rs`
28. `network/tcp.rs`
29. `network/tcp/server.rs`
30. `network/tcp/client.rs`

之所以推荐这个顺序，是因为它遵循了同一条理解路径：

1. 先理解 pipeline 在类型上认定“请求”和“响应”是什么；
2. 再理解请求级语义如何沿图移动；
3. 再理解本地图如何把这些对象连成一条执行链；
4. 进入 `network.rs`，理解远程化需要哪些公共协议对象；
5. 看 `codec`，理解 bytes 里到底包了什么；
6. 看 `manager` 和 unified traits，理解 transport 资源如何被抽象和创建；
7. 看 ingress server，理解 payload 如何进入 `PushWorkHandler`；
8. 看 egress router，理解 worker 如何被选择、请求如何发出；
9. 最后看 `tcp/` response plane，理解 ManyOut 流如何连回前端。

如果一开始就直接钻 `network.rs` 或 transport 细节，很容易只看到：

- 编解码
- 连接
- bytes 收发

却看不出这些对象为什么必须存在。  
按这里的顺序读，更容易一直把源码和“它在整套设计里回答什么问题”对应起来：  
先看到本地图为什么需要远程化，再看到 request plane 和 response plane 为什么必须拆开，最后再看具体 transport 如何服务这两个平面。

---

## 十四、从排障视角理解 `network` 的分层

如果线上一条远程请求出问题，不能只问“RPC 失败了吗”。  
这套设计把一次调用拆成多个阶段，每个阶段失败代表的系统含义不同。

### 14.1 worker 选择阶段

对应模块：

- `egress/push_router.rs`
- `servicegroup::Client`
- discovery / instance metadata

这一阶段回答的是：

```text
这次请求应该发给哪个 worker？
这个 worker 当前是否可用？
这个 worker 是否已经因为之前的错误被 report_instance_down？
```

如果这里失败，通常还没有进入网络 I/O。  
典型原因是：

- discovery 没有可用 instance
- busy threshold 认为所有 worker 都忙
- Direct routing 指定的 instance 不存在
- KV / Direct 模式被错误地当成普通 `generate()` 使用

这类问题不应该从 TCP 或 HTTP codec 开始查。  
它首先属于“服务发现和路由策略”层。

### 14.2 request plane 发送阶段

对应模块：

- `egress/addressed_router.rs`
- `egress/unified_client.rs`
- `egress/tcp_client.rs`
- `egress/http_router.rs`
- `egress/nats_client.rs`
- `ingress/shared_tcp_portname.rs`
- `ingress/http_portname.rs`
- `ingress/nats_server.rs`

这一阶段回答的是：

```text
请求 payload 是否送到了目标 worker 的 request-plane server？
server 是否找到 portname handler？
payload 是否成功进入处理队列？
ack 是否返回给前端？
```

注意这里的成功不是业务成功。  
request plane ack 的含义只是：

```text
远端 request-plane server 已经接收或排队这次 payload
```

所以如果 `RequestPlaneClient::send_request` 成功，但后面没有业务响应，不应该说“RPC 已经成功”。  
这只能说明第一段 transport 已经完成，后续还要看 response plane 是否建立、后端 pipeline 是否执行成功。

TCP request plane 的排查重点通常是：

```text
address 是否包含 portname path
x-portname-path 是否正确
SharedTcpServer.handlers 是否注册了对应 key
worker queue 是否满
ZeroCopyTcpDecoder 是否拒绝了超大消息
```

HTTP request plane 的排查重点通常是：

```text
PGD_HTTP_RPC_ROOT_PATH 是否匹配
路由 path 是否是期望 portname
HTTP status 是否是 202
headers 是否带上 trace 信息
```

NATS request plane 的排查重点通常是：

```text
service group 是否存在
portname_name-instance_id 是否匹配
request_with_headers 是否返回 ack
PushPortname 是否仍在消费循环中
```

### 14.3 后端 payload 恢复阶段

对应模块：

- `ingress/push_handler.rs`
- `network.rs`
- `codec/two_part.rs`

这一阶段回答的是：

```text
payload 是否能被 TwoPartCodec 解开？
header 是否是 RequestControlMessage？
body 是否能反序列化成业务请求 T？
Context 是否用原 request id 恢复？
connection_info 是否能解释成 response plane 所需信息？
```

如果这里失败，request plane 可能已经 ack 成功，但业务请求还没有进入本地图。  
因此这类错误看起来像“远端收到了请求但没有输出”，根因往往在协议恢复：

- `TwoPartMessage` 不是 `HeaderAndData`
- 控制头 JSON 不匹配
- 业务请求 JSON 不匹配
- `ConnectionInfo.transport` 不是当前支持的 response transport
- request id / context 信息不一致

这也是 `RequestControlMessage` 设计很关键的原因。  
它承载的是远端恢复本地 pipeline 调用所需的最小上下文，不是可有可无的附加字段。

### 14.4 后端本地图执行阶段

对应模块：

- `Ingress<Req, Resp>`
- `SegmentSource`
- `ServiceBackend`
- `PipelineOperator`
- 业务 engine

这一阶段回答的是：

```text
网络 payload 是否成功落回 SegmentSource.generate()？
本地 pipeline 是否正常执行？
业务 engine 是否成功返回 ManyOut？
```

如果 `segment.generate(request)` 失败，后端会尝试通过 response plane 发：

```text
ResponseStreamPrologue { error: Some(...) }
```

这个设计的意义是让前端区分两类成功：

```text
TCP CallHome 已建立:
  网络层连接成功

prologue(None):
  后端业务响应流也成功建立
```

没有 prologue，前端只能知道 socket 连上了，却不知道远端 pipeline 是否真的生成了响应流。

### 14.5 response plane 建立阶段

对应模块：

- `tcp/server.rs`
- `tcp/client.rs`
- `tcp.rs`
- `egress/addressed_router.rs`

这一阶段回答的是：

```text
后端是否按照 connection_info 连回前端？
CallHomeHandshake.subject 是否能匹配前端注册的 rx_subject？
StreamProvider 是否被 oneshot 唤醒？
ResponseStreamPrologue 是否成功？
```

这里最重要的不变量是：

```text
AddressedPushRouter 先 register response plane
  -> 得到 subject
  -> subject 写进 RequestControlMessage.connection_info
  -> 后端 TcpClient CallHome 时带回同一个 subject
  -> TcpStreamServer 用 subject 找到等待者
```

如果 subject 丢失、重复、过早清理或连接到错误地址，request plane 可能已经成功，后端也可能开始执行，但前端仍然拿不到 `StreamReceiver`。

这类问题属于 response plane pairing，不属于 request plane 发送失败。

### 14.6 响应流传输阶段

对应模块：

- `ingress/push_handler.rs`
- `network.rs`
- `egress/addressed_router.rs`
- `tcp/client.rs`
- `tcp/server.rs`

这一阶段回答的是：

```text
每个响应 item 是否被包装成 NetworkStreamWrapper？
前端是否能反序列化 U？
是否收到 complete_final？
连接关闭时是否应当视为正常结束？
```

这里有两个结束信号要区分：

```text
NetworkStreamWrapper.complete_final:
  业务响应流结束
  AddressedPushRouter 用它判断 ManyOut 是否完整

ControlMessage::Sentinel:
  response plane socket 转发结束
  TcpStreamServer 用它收尾连接任务
```

如果前端读到 socket 结束，但没有见到 `complete_final = true`，它会把这次响应流视为异常断流，并向 `ManyOut<U>` 注入 disconnected 类错误。  
这个行为保护的是上层调用方：不能把一个中途断掉的流误判为正常完成。

### 14.7 取消和关闭阶段

对应模块：

- `Context`
- `Controller`
- `tcp/client.rs`
- `tcp/server.rs`
- `ingress/push_handler.rs`
- `egress/addressed_router.rs`

这一阶段回答的是：

```text
前端不再需要响应时，后端 pipeline 是否能及时停止？
socket 关闭是正常取消、异常断开，还是 kill？
metrics 是否记录取消和错误？
```

网络层在这里做的不是简单关闭连接，而是尽量把 I/O 事件翻译回 pipeline 控制语义：

```text
前端 stop / channel closed
  -> response plane 发 ControlMessage::Stop 或 Kill
  -> 后端 TcpClient reader task 调 context.stop() / context.kill()
  -> 本地 pipeline 通过 Controller 感知取消
```

这就是第 5 节和第 8 节之间非常重要的设计连动：  
远程请求虽然跨进程，但取消语义仍然努力回到同一个 `Context` / `Controller` 模型里。

---

## 十五、后续演进时应该守住什么

如果后续继续重构 `network`，最重要的不是简单减少文件数量，而是守住已经形成的设计边界。

### 15.1 应该稳定的边界

第一，`PushWorkHandler` 这条边界应该保留。

```text
transport server:
  负责收 bytes、ack、排队、portname 路由

PushWorkHandler:
  负责理解 pipeline payload、恢复 Context、调用 SegmentSource
```

如果让 HTTP/TCP/NATS server 各自理解 `RequestControlMessage` 和 `SegmentSource`，就会破坏现在最清楚的一层隔离。

第二，`RequestPlaneClient` / `RequestPlaneServer` 这组 request plane trait 应该保留。

它们让 `NetworkManager` 可以成为 request plane 的单一配置入口，也让 `PushRouter` / `AddressedPushRouter` 不需要为每种 transport 分支。

第三，`AddressedPushRouter` 的“两平面协调者”角色应该保留。

它是少数真正同时知道以下三件事的对象：

```text
目标地址
response plane 注册结果
request payload 编码和发送
```

这些逻辑如果散开，远程调用的时序不变量会变得很难维护。

第四，`Ingress` 必须继续落到 `SegmentSource.generate()`。

远端 worker 即使只是包一个 engine，也应该继续通过本地图模型执行。  
这保证远程执行和本地执行共享同一套 `Context`、`Source/Sink/Edge` 和响应回流语义。

### 15.2 最值得收敛的地方

第一，response plane 可以考虑抽象成和 request plane 类似的 trait。

当前代码里：

```text
request plane:
  RequestPlaneClient / RequestPlaneServer
  HTTP / TCP / NATS 可替换

response plane:
  TcpStreamServer / TcpClient
  固定 TCP CallHome
```

这个不对称是当前实现最明显的边界。  
如果未来希望 response plane 也支持 SSE、HTTP streaming 或其它 transport，应该先抽出“注册回流流、生成 ConnectionInfo、等待 StreamReceiver、创建 StreamSender”这组能力，而不是直接把更多分支塞进 `push_handler.rs`。

第二，`RequestControlMessage` 应该去重。

它现在在顶层 `network.rs` 和 `egress/addressed_router.rs` 中有重复定义。  
这个结构是前后端协议契约，长期重复会带来一个风险：

```text
一端增加字段或调整 serde 行为
另一端没有同步
编译仍然通过
运行时协议却开始漂移
```

这类协议结构应该尽量单一定义、双端共享。

第三，结束检测应该收敛。

当前有：

```text
NetworkStreamWrapper.complete_final
ControlMessage::Sentinel
socket close
ResponseStreamPrologue
```

它们分别服务不同层，但读代码时负担较高。  
未来如果使用 SSE 或更明确的 streaming frame，可以减少 `NetworkStreamWrapper` 这种临时业务结束标记，让 response plane 自身更自然地表达流结束。

第四，fatal `panic!` 分支应该变成协议错误。

`tcp/client.rs` 和 `tcp/server.rs` 里还有 TODO(#171) 的 fatal 分支。  
这些分支表达的是“按协议不应该发生”的情况，但在分布式系统里，坏消息、旧版本 peer、半包、错误控制帧都可能出现。  
更理想的方向是把它们转成结构化错误和可观测指标，而不是让 task panic。

第五，时间戳路径应该统一。

现在同时存在：

```text
RequestControlMessage.frontend_send_ts_ns
headers["x-frontend-send-ts-ns"]
```

前者是 pipeline 控制头字段，后者是 transport header。  
两者都能表达“前端发送时间”，但实际写入和读取路径不完全一致。  
后续应明确到底哪一层负责 network transit 指标：是 pipeline workload 协议，还是 request plane transport header。

### 15.3 不应该轻易合并的地方

不要因为名字相近就合并：

```text
egress/tcp_client.rs
tcp/client.rs
```

前者是 request plane 客户端，后者是 response plane CallHome 客户端。  
它们都用 TCP，但方向、生命周期、协议状态完全不同。

也不要把 `PushRouter` 和 `AddressedPushRouter` 合成一个大对象。

```text
PushRouter:
  discovery、负载选择、故障反馈

AddressedPushRouter:
  已知地址后的 request/response plane 协调
```

这两个职责变化原因不同。  
路由策略会因为负载均衡和 discovery 演进而变化；`AddressedPushRouter` 更关心远程调用协议和响应流配对。  
保持拆分可以让两边独立演进。

同样，不建议让 `NetworkManager` 参与单次请求发送逻辑。  
它应该继续负责资源创建和复用，而不是变成请求执行器。  
否则生命周期管理、transport mode 选择和 per-request 协议状态会耦合在一起。

### 15.4 一个好的重构方向

如果要把当前设计继续收敛，可以按这个顺序推进：

1. 先统一 `RequestControlMessage` 定义，保证协议契约单一来源。
2. 再抽象 response plane，把 `TcpStreamServer.register` 和 `TcpClient::create_response_stream` 背后的能力命名出来。
3. 然后替换 `NetworkStreamWrapper` 的结束检测，让流结束成为 response plane frame 语义。
4. 再把 TCP response plane 里的 fatal `panic!` 收敛成结构化错误。
5. 最后统一 timestamp 和 metrics 的归属层。

这个顺序的原因是：  
先稳协议契约，再抽象 transport 边界，再改善错误和观测。  
如果反过来先做 transport 泛化，很容易在还没统一协议对象时复制更多重复结构。
