package com.flinkcourse.ch08;

import com.flinkcourse.common.util.MysqlUtil;

import com.flinkcourse.common.util.KafkaUtil;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 第8章：SQL 版实时 GMV 大屏（ODS Kafka → 窗口聚合 → MySQL ADS 表）
 *
 * 前置条件（docker compose 环境）：
 *   1. topic ods_orders 已存在且有数据：
 *      可运行 ch11.MockUserActionProducer（需把它 topic 改为 ods_orders），
 *      或手动灌数据：
 *      docker exec flink-kafka kafka-console-producer.sh --topic ods_orders \
 *        --bootstrap-server localhost:9092
 *      输入示例：{"userId":1001,"amount":299.0,"city":"深圳","status":2,"ts":1730000000000}
 *   2. MySQL 表 dws_gmv 由 docker/mysql/init/01_init.sql 自动创建
 *
 * 运行：IDEA 运行 main，1 分钟窗口触发后查 MySQL：
 *   docker exec -it flink-mysql mysql -uroot -pflink123 flink_course \
 *     -e "SELECT * FROM dws_gmv ORDER BY update_time DESC LIMIT 10"
 */
public class SqlDashboardJob {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.inStreamingMode());

        // ① Source 表：Kafka 订单日志（ODS 层）
        tEnv.executeSql(
            "CREATE TABLE ods_orders (\n" +
            "  user_id    BIGINT,\n" +
            "  amount     DECIMAL(10, 2),\n" +
            "  city       STRING,\n" +
            "  status     INT,\n" +
            "  ts         BIGINT,\n" +
            "  event_time AS CAST(ts AS TIMESTAMP(3)),\n" +     // 毫秒时间戳 → 时间类型
            "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'ods_orders',\n" +
            "  'properties.bootstrap.servers' = '" + KafkaUtil.BOOTSTRAP + "',\n" +
            "  'properties.group.id' = 'sql-dashboard',\n" +
            "  'scan.startup.mode' = 'earliest-offset',\n" +
            "  'format' = 'json',\n" +
            "  'json.ignore-parse-errors' = 'true'\n" +
            ")");

        // ② Sink 表：MySQL 主键表（聚合结果是更新流 → 必须能 upsert）
        tEnv.executeSql(
            "CREATE TABLE dws_gmv (\n" +
            "  stat_time  STRING,\n" +
            "  city       STRING,\n" +
            "  order_cnt  BIGINT,\n" +
            "  gmv_amount DECIMAL(16, 2),\n" +
            "  PRIMARY KEY (stat_time, city) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'jdbc',\n" +
            "  'url' = '" + MysqlUtil.URL_PREFIX + "flink_course',\n" +
            "  'table-name' = 'dws_gmv',\n" +
            "  'username' = '" + MysqlUtil.USER + "',\n" +
            "  'password' = '" + MysqlUtil.PASSWORD + "'\n" +
            ")");

        // ③ 核心逻辑：一行 SQL = 第5章滚动窗口 GMV 的全部代码
        //    窗口 TVF（TUMBLE）+ 分组聚合 + upsert 落库
        tEnv.executeSql(
            "INSERT INTO dws_gmv\n" +
            "SELECT\n" +
            "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm') AS stat_time,\n" +
            "  city,\n" +
            "  COUNT(*)     AS order_cnt,\n" +
            "  SUM(amount)  AS gmv_amount\n" +
            "FROM TABLE(\n" +
            "  TUMBLE(TABLE ods_orders, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))\n" +
            "WHERE status = 2\n" +
            "GROUP BY window_start, window_end, city");
    }
}
