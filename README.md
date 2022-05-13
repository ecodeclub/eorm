# EORM

[![codecov](https://codecov.io/gh/gotomicro/eorm/branch/main/graph/badge.svg?token=vc0BDor3Lk)](https://codecov.io/gh/gotomicro/eorm)

简单的 ORM 框架。

> 注意：这是一个全中文的仓库。这意味着注释、文档和错误信息，都会是中文的。介意的用户可以选择 GORM，这也是一个杰出的 ORM 仓库

## SQL 2003 标准
理论上来说，我们计划支持 [SQL 2003 standard](https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#query%20specification). 不过据我们所知，并不是所有的数据库都支持全部的 SQL 2003 标准，所以用户还是需要进一步检查目标数据库的语法。

## 设计

[设计思路](./docs/design.md)

## 加入我们

我们欢迎任何人给我们提合并请求，但是我们希望合并请求能够做到：
- 一个合并请求一个 Commit ID
- 自己要先确保合并请求能够通过 CI
- 我们使用 uber 的[代码风格](https://github.com/uber-go/guide/blob/master/style.md)

### 设置开发环境

如果你是 Windows 用户，那么我们建议你使用 WSL，因为这个仓库会使用到一个 Unix 命令来帮助构建、测试等。

#### 安装 golangci-lint
参考 [Install golangci-lint](https://golangci-lint.run/usage/install/)
#### 设置 pre-push github hook
将`.github/pre-push` 复制到本仓库的 `.git` 目录下