# Pagoda 项目约束

## 文件头规范

所有新建的源码文件和配置文件都必须在文件顶部添加 Pagoda SPDX 文件头；如果文件格式不支持注释，则不添加。

### Rust、C、C++、JavaScript、TypeScript

```text
// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
```

### TOML、YAML、Shell、Python、Markdown

```text
# SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
```

### 注意事项

- 文件头必须作为文件中的第一段非空内容。
- 已经存在 SPDX 文件头的文件，不要重复添加。
- 引入第三方或上游代码时，必须保留原有版权和许可证声明。
- 自动生成文件不要添加该文件头，例如 `Cargo.lock`。