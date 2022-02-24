# EQL

[![codecov](https://codecov.io/gh/gotomicro/eql/branch/main/graph/badge.svg?token=vc0BDor3Lk)](https://codecov.io/gh/gotomicro/eql)

An easy-use SQL builder.

## SQL 2003 Standard
In theory, we try to support the [SQL 2003 standard](https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#query%20specification). But as we know, some databases do not follow the standard. These databases have its own feature, and we will support these grammers in the future. 

## Design

We are not English native speaker, so we use Chinese to write the design documents. We plan to translate them to English. Or you can use some software to translate it. 

Here is a good one: https://www.deepl.com/en/translator

[设计思路](./docs/design.md)

[B站视频](https://space.bilibili.com/324486985)

## Contribution

You must follow these rules:
- One commit one PR
- You must add change log to `.CHANGELOG.md` file
- You must add license header to every new files

[style guide](https://github.com/uber-go/guide/blob/master/style.md)

### Setup Develop Environment

#### install golangci-lint
Please refer [Install golangci-lint](https://golangci-lint.run/usage/install/)
#### setup pre-push github hook
Please move the `.github/pre-push` to your `.git` directory