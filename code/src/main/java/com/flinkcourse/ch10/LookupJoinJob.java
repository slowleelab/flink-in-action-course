package com.flinkcourse.ch10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 第10章：Lookup Join —— 流表实时关联 MySQL 维表
 *
 * 前置条件：docker compose 环境
 *   1. topic ods_orders 有数据（见第8章 SqlDashboardJob 的说明，或运行 ch11 生产者）
 *   2. dim_city 维表已由 init SQL 初始化 6 个城市
 *
 * 知识点：
 *   - FOR SYSTEM_TIME AS OF <时间字段> ：Lookup Join 的固定语法
 *   - lookup.cache.max-rows + ttl：缓存是性能生命线（不缓存=每条数据查一次 MySQL）
 *
 * 实验加分项：UPDATE dim_city 改省份名，等 TTL(10min) 过期后观察输出变化
 */
public class LookupJoinJob {

    public static void main(String[] args) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                StreamExecutionEnvironment.getExecutionEnvironment(),
                EnvironmentSettings.inStreamingMode());

        // ① 主流：Kafka 订单流
        tEnv.executeSql(
            "CREATE TABLE orders (\n" +
            "  user_id   BIGINT,\n" +
            "  amount    DECIMAL(10, 2),\n" +
            "  city      STRING,\n" +
            "  status    INT,\n" +
            "  ts        BIGINT,\n" +
            "  proc_time AS PROCTIME()\n" +           // lookup join 需要处理时间属性
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'ods_orders',\n" +
            "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'properties.group.id' = 'lookup-join-demo',\n" +
            "  'scan.startup.mode' = 'earliest-offset',\n" +
            "  'format' = 'json',\n" +
            "  'json.ignore-parse-errors' = 'true'\n" +
            ")");

        // ② 维表：MySQL dim_city（带 lookup 缓存）
        tEnv.executeSql(
            "CREATE TABLE dim_city (\n" +
            "  city_id   BIGINT,\n" +
            "  city_name STRING,\n" +
            "  province  STRING,\n" +
            "  PRIMARY KEY (city_id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'jdbc',\n" +
            "  'url' = 'jdbc:mysql://localhost:3306/flink_course',\n" +
            "  'table-name' = 'dim_city',\n" +
            "  'username' = 'root',\n" +
            "  'password' = 'flink123',\n" +
            "  'lookup.cache.max-rows' = '500',\n" +     // 缓存 500 行
            "  'lookup.cache.ttl' = '10min'\n" +         // 10 分钟过期（维表变更容忍度）
            ")");

        // ③ Lookup Join：每条订单现查维表补全省份（LEFT JOIN 防维表缺失丢数据）
        tEnv.executeSql(
            "SELECT o.user_id, o.city, c.province, o.amount\n" +
            "FROM orders AS o\n" +
            "LEFT JOIN dim_city FOR SYSTEM_TIME AS OF o.proc_time AS c\n" +
            "  ON o.city = c.city_name")
            .print();
    }
}
