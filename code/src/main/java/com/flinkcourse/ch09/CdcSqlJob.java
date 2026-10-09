package com.flinkcourse.ch09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 第9章：SQL 版 MySQL CDC（推荐用法 —— 3 分钟体验"数据库流化"）
 *
 * 前置条件：docker compose 环境（MySQL 已初始化 orders 表，含 7 条预置数据）
 *
 * 实验步骤：
 *   1. IDEA 运行 main —— 控制台先输出 7 条 +I（全量快照阶段）
 *   2. 另开终端改库：
 *      docker exec -it flink-mysql mysql -uroot -pflink123 flink_course
 *      UPDATE orders SET amount = 399.00 WHERE order_id = 1;
 *   3. 毫秒级观察控制台出现：-U(旧值) / +U(新值)  —— binlog 增量阶段
 *   4. 再试 INSERT / DELETE，观察 +I / -D
 */
public class CdcSqlJob {

    public static void main(String[] args) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                StreamExecutionEnvironment.getExecutionEnvironment(),
                EnvironmentSettings.inStreamingMode());

        // ① CDC Source：MySQL orders 表（全量+增量一体化）
        tEnv.executeSql(
            "CREATE TABLE orders_cdc (\n" +
            "  order_id    BIGINT,\n" +
            "  user_id     BIGINT,\n" +
            "  amount      DECIMAL(10, 2),\n" +
            "  city        STRING,\n" +
            "  status      TINYINT,\n" +
            "  create_time TIMESTAMP(3),\n" +
            "  PRIMARY KEY (order_id) NOT ENFORCED\n" +      // 主键 = upsert 依据（CDC 必需）
            ") WITH (\n" +
            "  'connector' = 'mysql-cdc',\n" +
            "  'hostname' = 'localhost',\n" +
            "  'port' = '3306',\n" +
            "  'username' = 'root',\n" +
            "  'password' = 'flink123',\n" +
            "  'database-name' = 'flink_course',\n" +
            "  'table-name' = 'orders',\n" +
            "  'server-time-zone' = 'Asia/Shanghai'\n" +    // 容器默认 UTC，必配！
            ")");

        // ② print sink：把变更流打到控制台，观察 +I / -U / +U / -D
        tEnv.executeSql(
            "CREATE TABLE print_sink (\n" +
            "  order_id BIGINT, user_id BIGINT, amount DECIMAL(10,2),\n" +
            "  city STRING, status TINYINT, create_time TIMESTAMP(3),\n" +
            "  PRIMARY KEY (order_id) NOT ENFORCED\n" +
            ") WITH ('connector' = 'print')");

        tEnv.executeSql("INSERT INTO print_sink SELECT * FROM orders_cdc");
    }
}
