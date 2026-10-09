# 🤝 贡献指南

感谢你关注本课程！无论是**修正一个错别字**还是**新增一个实战章节**，所有贡献都欢迎。

## 快速贡献的四种方式

| 方式 | 适合场景 | 怎么做 |
|:---|:---|:---|
| 提 Issue | 勘误、建议、提问 | 按模板填写章节号 + 问题描述 |
| 提 PR（文档） | 修改文档内容/图表 | 直接编辑 `docs/` 下对应 markdown |
| 提 PR（代码） | 新增/修复示例代码 | 遵守下方代码规范 |
| 分享练习答案 | 每章"动手练习" | 提交到 `docs/exercises/` 目录 |

## 文档贡献规范

1. 每章文档统一结构：**学习目标 → 核心概念 → 代码实战 → 运行验证 → 常见坑 → 练习 → 小结**；
2. 图表一律使用 [Mermaid](https://mermaid.js.org/)，保证可维护与可版本化；
3. 中文与英文/数字之间保留空格（如 `Flink 1.20`），使用全角标点；
4. 提示使用 `?> **提示**`，警告使用 `!> **注意**`（Docsify 语法）。

## 代码贡献规范

1. 代码位于 `code/` 单一 Maven 工程，按包分层：`com.flinkcourse.chXX`；
2. 每个示例类必须满足：
   - 带类级 Javadoc：场景说明 + 对应章节 + 运行方式；
   - **可独立运行**（不依赖本章其他 main 类）；
   - 优先使用内存/Socket 数据源，Kafka 版本单独成类；
3. 依赖版本统一在父 `pom.xml` 管理，禁止在子模块写死版本号；
4. 提交信息格式：`[chXX] 摘要`，例：`[ch05] 新增会话窗口 UV 统计示例`。

## 本地验证清单

```bash
# 1. 文档预览
cd docs && python3 -m http.server 4000

# 2. 代码编译（Java 11+）
cd code && mvn clean compile

# 3. 如改动涉及 Kafka/MySQL 示例，先启动环境
docker compose up -d
```

## 提交 PR 流程

```bash
git checkout -b feat/ch05-session-window
# ... 修改并自测 ...
git commit -m "[ch05] 新增会话窗口示例"
git push origin feat/ch05-session-window
# GitHub 上发起 PR，标题与提交信息保持一致
```

> 维护者会在 3 个工作日内 Review。首次贡献者请先阅读《第0章》了解课程定位，避免内容方向偏离。
