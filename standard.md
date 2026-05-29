<!--
SPDX-FileCopyrightText: Copyright (c) 2026-2028 Pagoda Contributors
SPDX-License-Identifier: Apache-2.0
-->

# 上游参考提交记录

本文档用于记录 Pagoda 项目参考的上游 Dynamo 提交信息，便于后续追踪来源、对比实现和进行兼容性迁移。

## 上游仓库

- **仓库地址**：<https://github.com/ai-dynamo/dynamo.git>
- **用途说明**：Pagoda 项目在设计和实现过程中参考该上游仓库的指定提交。

## 参考提交

- **Commit**：`e007116d579b69afcbb7263e134a2c4adfacc857`
- **引用**：`HEAD -> release/1.2.0`、`tag: 1.2.0-post.1`、`origin/release/1.2.0`
- **作者**：Julien Mancuso `<161955438+julienmancuso@users.noreply.github.com>`
- **时间**：Thu May 21 19:22:09 2026 -0600
- **标题**：`fix(operator): Avoid imagePullSecrets drift during operator startup (… (#9841)`
- **备注**：Signed-off-by: Julien Mancuso `<jmancuso@nvidia.com>`

## 获取方式

```bash
git clone https://github.com/ai-dynamo/dynamo.git
cd dynamo
git checkout e007116d579b69afcbb7263e134a2c4adfacc857
```