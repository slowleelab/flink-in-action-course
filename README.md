<div align="center">

# Flink 实战：从 0 到 1

**一套面向零基础同学、以真实业务场景驱动的 Apache Flink 实战课程**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Flink](https://img.shields.io/badge/Flink-1.20.1-E6526F.svg)](https://flink.apache.org)
[![Java](https://img.shields.io/badge/Java-11+-orange.svg)](https://openjdk.org)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

[课程文档](docs/index.html) · [快速开始](#-快速开始) · [课程大纲](#%EF%B8%8F-课程大纲) · [加入贡献](CONTRIBUTING.md)

</div>

---

## 为什么是这套课程？

市面上的 Flink 教程，要么是纯 API 罗列的"字典"，要么是默认你会大数据全家桶的"高阶教材"。本课程遵循三条原则：

1. **零基础友好** —— 不假设你懂 Spark / Kafka 内部原理，从"什么是有界流/无界流"讲起，每章都有可独立运行的代码；
2. **场景驱动** —— 每一个知识点都挂在一个真实业务上：电商 GMV 大屏、用户行为会话、转账反欺诈、IoT 温度告警、实时数仓分层……学完即可映射到简历项目；
3. **面向生产** —— 不止"跑得通"，还讲 Checkpoint 调优、背压排查、状态 TTL、端到端 Exactly-Once 这些生产环境的真问题。

## 🗺️ 课程全景图

```mermaid
flowchart LR
    A[📦 基础篇<br/>第0~2章<br/>环境与流批一体] --> B[⚙️ 核心篇<br/>第3~7章<br/>API/时间/窗口/状态/容错]
    B --> C[🔌 SQL与生态篇<br/>第8~10章<br/>SQL/CDC/连接器]
    C --> D[🏗️ 实战篇<br/>第11~13章<br/>数仓/风控/IoT]
    D --> E[🚀 进阶篇<br/>第14~15章<br/>调优运维/源码/面试]
    style A fill:#e3f2fd,stroke:#1976d2,color:#0d47a1
    style B fill:#e8f5e9,stroke:#388e3c,color:#1b5e20
    style C fill:#fff8e1,stroke:#f9a825,color:#f57f17
    style D fill:#fce4ec,stroke:#c2185b,color:#880e4f
    style E fill:#ede7f6,stroke:#5e35b1,color:#311b92
```

## 🗂️ 课程大纲

| 篇章 | 章节 | 核心内容 | 业务场景 | 难度 |
|:---|:---|:---|:---|:---:|
| 基础篇 | [第0章 课程导读](docs/ch00.md) | 流计算思维、学习路线、前置知识自查 | — | ⭐ |
| 基础篇 | [第1章 环境搭建](docs/ch01.md) | Docker 一键起 Flink/Kafka、IDE 工程导入 | — | ⭐ |
| 基础篇 | [第2章 流批一体初体验](docs/ch02.md) | WordCount、有界流与无界流 | 词频统计 | ⭐ |
| 核心篇 | [第3章 DataStream API](docs/ch03.md) | Source/Transformation/Sink、并行度、算子链 | 订单清洗 | ⭐⭐ |
| 核心篇 | [第4章 时间与水印](docs/ch04.md) | 事件时间、Watermark、乱序处理 | 延迟订单 | ⭐⭐⭐ |
| 核心篇 | [第5章 窗口](docs/ch05.md) | 滚动/滑动/会话窗口、聚合函数 | 分钟级 GMV、UV 统计 | ⭐⭐⭐ |
| 核心篇 | [第6章 状态管理](docs/ch06.md) | Keyed/Operator State、TTL、RocksDB | 用户购买轨迹 | ⭐⭐⭐ |
| 核心篇 | [第7章 容错机制](docs/ch07.md) | Checkpoint、Savepoint、Exactly-Once | 电商交易对账 | ⭐⭐⭐⭐ |
| SQL生态 | [第8章 Flink SQL](docs/ch08.md) | Table API、SQL 函数、Top-N | 实时大屏 | ⭐⭐⭐ |
| SQL生态 | [第9章 Flink CDC](docs/ch09.md) | CDC 原理、MySQL 实时同步 | 库表实时入仓 | ⭐⭐⭐ |
| SQL生态 | [第10章 连接器](docs/ch10.md) | Kafka/JDBC/Redis/ES 读写 | 维表 Join、落库 | ⭐⭐ |
| 实战篇 | [第11章 电商实时数仓](docs/ch11.md) | ODS→DWD→DWS→ADS 分层、GMV 大屏 | 双11 实时大屏 | ⭐⭐⭐⭐ |
| 实战篇 | [第12章 实时风控引擎](docs/ch12.md) | 状态机 + CEP、定时器、侧输出 | 转账反欺诈 | ⭐⭐⭐⭐ |
| 实战篇 | [第13章 IoT 设备监控](docs/ch13.md) | 侧输出流、迟到大屏、告警去重 | 工厂温度监控 | ⭐⭐⭐ |
| 进阶篇 | [第14章 生产调优](docs/ch14.md) | 背压、内存模型、并行度、RocksDB 调优 | 生产 Checklist | ⭐⭐⭐⭐ |
| 进阶篇 | [第15章 源码与面试](docs/ch15.md) | 源码剖析路线、30 道高频面试题 | 求职冲刺 | ⭐⭐⭐⭐ |

> 📖 **完整文档**：进入 [docs/](docs/) 目录，或本地预览（见下方快速开始）。

## 🚀 快速开始

### 0. 克隆项目

```bash
git clone https://github.com/slowleelab/flink-in-action-course.git
cd flink-in-action-course
```

### 1. 启动文档站

```bash
# 方式一：任意静态服务器
cd docs && python3 -m http.server 4000
# 浏览器打开 http://localhost:4000

# 方式二：VS Code 用户可安装 Live Server 插件直接打开 docs/index.html
```

### 2. 一键拉起全套环境（Flink + Kafka + MySQL + Redis）

```bash
docker compose up -d
```

> 🇨🇳 国内网络拉取镜像超时？见[第1章「镜像加速」小节](docs/ch01.md)——用加速站前缀拉取后重打标签即可。

| 服务 | 地址 | 用途 |
|:---|:---|:---|
| Flink Web UI | http://localhost:8081 | 任务提交与监控 |
| Kafka | localhost:9092 | 数据源/数据总线（复用宿主机已有实例） |
| MySQL | localhost:3306 | CDC 源、结果表 |
| Redis | localhost:6379 | 实时维表、去重 |

### 3. 跑通第一个作业

```bash
# 先起一个 socket 模拟数据源
nc -lk 9999

# 另开终端，运行第 2 章示例
cd code
mvn compile exec:exec -Dexec.mainClass="com.flinkcourse.ch02.StreamWordCount"
```

在 `nc` 窗口输入 `hello flink hello world`，即可看到实时词频统计输出。

## 📁 项目结构

```
flink/
├── docs/                     # 课程文档（Docsify 站点）
│   ├── index.html            # 站点入口
│   ├── home.md               # 课程主页
│   └── ch00.md ~ ch15.md     # 16 章内容
├── code/                     # 全部示例代码（Maven 工程）
│   └── src/main/java/com/flinkcourse/
│       ├── common/           # 公共模型与工具
│       └── ch02/ ~ ch13/     # 各章节可运行示例
├── docker/                   # Docker 初始化脚本
├── docker-compose.yml        # 一键环境
├── CONTRIBUTING.md           # 贡献指南
└── LICENSE                   # Apache 2.0
```

## 🔧 技术栈与版本

| 组件 | 版本 | 说明 |
|:---|:---|:---|
| Apache Flink | 1.20.1 | 当前 1.x 稳定线（2.x 变化见第15章附录） |
| flink-connector-kafka | 3.3.0-1.20 | Kafka Source/Sink |
| flink-connector-jdbc | 3.3.0-1.20 | JDBC Sink |
| flink-connector-mysql-cdc | 3.3.0 | MySQL CDC |
| Java | 11+ | 同时兼容 8/17 |
| Kafka | 3.7 (KRaft) | 无需 ZooKeeper |

## 🤝 参与贡献

课程长期接受贡献：勘误、新章节、新场景、练习题解析都非常欢迎，详见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 📄 许可证

[Apache License 2.0](LICENSE) —— 代码可自由用于学习与商业项目，转载文档请注明出处。
