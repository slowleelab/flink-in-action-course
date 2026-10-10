# 课程示例代码

单一 Maven 工程，按章节分包，**每个示例类可独立运行**。

## 版本基线

| 组件 | 版本 |
|:---|:---|
| Java | 11+（兼容 8/17） |
| Apache Flink | 1.20.1 |
| flink-connector-kafka | 3.3.0-1.20 |
| flink-connector-jdbc | 3.3.0-1.20 |
| flink-connector-mysql-cdc | 3.6.0-1.20 |
| MySQL | 8.0+（含 9.x） |
| Kafka | 3.7（KRaft 模式） |

> 提交集群时：把 pom 中 flink-* 依赖改 `provided` 并配 maven-shade-plugin（详见文档第14章 Checklist）。

## 连接配置（环境变量外置）

所有外部组件地址统一走 `common/util` 下的工具类 + 环境变量覆盖，**不改代码即可适配任意环境**（生产配置外置实践）：

| 环境变量 | 默认值 | 说明 |
|:---|:---|:---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | KafkaUtil：bootstrap 地址 |
| `MYSQL_HOST` | `localhost` | MysqlUtil：主机（CDC hostname） |
| `MYSQL_PORT` | `3306` | MysqlUtil：端口 |
| `MYSQL_USER` | `root` | MysqlUtil：用户名 |
| `MYSQL_PASSWORD` | `flink123` | MysqlUtil：密码 |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | RedisUtil：地址 |

**复用本机已有公共组件**时，运行前 export 即可，例如：

```bash
export KAFKA_BOOTSTRAP=localhost:9094   # 复用其他项目监听在 9094 的 Kafka
export MYSQL_USER=root MYSQL_PASSWORD=你的密码   # 复用本机 MySQL
```

## 示例运行速查表

| 章节 | 类 | 依赖 | 运行方式 |
|:---|:---|:---|:---|
| ch02 | `StreamWordCount` | nc | `nc -lk 9999` 后运行 |
| ch02 | `BoundedWordCount` | 无 | 直接运行 |
| ch03 | `OrderCleanJob` | 无 / Kafka | 直接运行（`kafka` 参数走 Kafka） |
| ch03 | `PartitioningDemo` | 无 | 直接运行，浏览器开 localhost:8082 |
| ch04 | `WatermarkSocketDemo` | nc | `nc -lk 9999` 后运行 |
| ch05 | `TumblingGmvDemo` | 无 | 直接运行 |
| ch05 | `SlidingUvDemo` | 无 | 直接运行 |
| ch05 | `SessionWindowDemo` | 无 | 直接运行 |
| ch06 | `UserBehaviorTrackJob` | 无 | 直接运行 |
| ch06 | `StateTtlDemo` | nc | `nc -lk 9999` 后运行 |
| ch06 | `BufferingSinkDemo` | 无 | 直接运行 |
| ch07 | `CheckpointDemo` | nc | `nc -lk 9999`，输入含 `boom` 触发故障实验 |
| ch08 | `SqlDashboardJob` | Kafka+MySQL | 见类注释 |
| ch09 | `CdcSqlJob` | MySQL | 运行后改 MySQL 数据观察 |
| ch09 | `CdcDataStreamJob` | MySQL | 同上 |
| ch10 | `JdbcSinkDemo` | MySQL | 运行后查 `dws_gmv` 表 |
| ch10 | `RedisSinkDemo` | Redis | 运行后 `ZREVRANGE city:rank:today` |
| ch10 | `LookupJoinJob` | Kafka+MySQL | 见类注释 |
| ch11 | `MockUserActionProducer` | Kafka | 常驻运行（数据源） |
| ch11 | `DwdCleanJob` | Kafka | 与 Producer、DwsGmvJob 联动 |
| ch11 | `DwsGmvJob` | Kafka+MySQL | 与上游联动，查 `dws_gmv` 表 |
| ch12 | `FraudDetectStateJob` | nc | `nc -lk 9999` 后运行 |
| ch12 | `FraudDetectCepJob` | nc | 同上 |
| ch13 | `IotAlertJob` | nc | `nc -lk 9999` 后运行（迟到三级防线实验） |

## 常用命令

```bash
# 编译
mvn compile

# 运行任意示例（以 ch02 为例）
mvn exec:exec -Dexec.mainClass="com.flinkcourse.ch02.StreamWordCount"
```

## 目录结构

```
src/main/java/com/flinkcourse/
├── common/
│   ├── model/    # Order / UserAction / UserClick / TransferEvent / DeviceMetric
│   └── util/     # JsonUtil / KafkaUtil / MysqlUtil / RedisUtil（连接配置外置）
├── ch02/ ~ ch13/ # 各章节示例（详见文档对应章节）
```
