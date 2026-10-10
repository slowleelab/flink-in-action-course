package com.flinkcourse.ch10;

import com.flinkcourse.common.util.MysqlUtil;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 第10章：JDBC Sink 攒批 upsert 落 MySQL
 *
 * 前置条件：docker compose 环境（MySQL 表 dws_gmv 已初始化）
 *
 * 知识点：
 *   - JdbcSink.builder()（Sink2 API）: withQueryStatement + withExecutionOptions + buildAtLeastOnce
 *   - INSERT ... ON DUPLICATE KEY UPDATE = 幂等 upsert（重跑不重不漏）
 *   - 批量三件套：batchSize / batchIntervalMs / maxRetries
 *
 * 运行：IDEA 运行 main，然后查库：
 *   docker exec -it flink-mysql mysql -uroot -pflink123 flink_course \
 *     -e "SELECT * FROM dws_gmv"
 *   重复运行本类，验证不产生重复行（幂等）。
 */
public class JdbcSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Sink2 API（1.20 推荐）：builder 三步走 —— 语句绑定 → 攒批配置 → 连接配置
        JdbcSink<CityGmv> mysqlSink = JdbcSink
                .<CityGmv>builder()
                // upsert：主键冲突时走 UPDATE（幂等的保障）
                .withQueryStatement(
                        "INSERT INTO dws_gmv (stat_time, city, order_cnt, gmv_amount) VALUES (?, ?, ?, ?) "
                                + "ON DUPLICATE KEY UPDATE "
                                + "order_cnt = VALUES(order_cnt), gmv_amount = VALUES(gmv_amount)",
                        (ps, stat) -> {
                            // PreparedStatement 参数绑定
                            ps.setString(1, stat.statTime);
                            ps.setString(2, stat.city);
                            ps.setLong(3, stat.orderCnt);
                            ps.setDouble(4, stat.gmvAmount);
                        })
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withBatchSize(100)          // 攒 100 条一提交
                        .withBatchIntervalMs(1000)   // 或每秒提交一次（先到先触发）
                        .withMaxRetries(3)           // 失败重试 3 次
                        .build())
                .buildAtLeastOnce(
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MysqlUtil.URL_PREFIX + "flink_course"
                                        + "?useSSL=false&serverTimezone=Asia/Shanghai")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername(MysqlUtil.USER)
                                .withPassword(MysqlUtil.PASSWORD)
                                .build());

        env.fromElements(
                new CityGmv("2024-11-11 00:01", "深圳", 3, 1598.0),
                new CityGmv("2024-11-11 00:01", "杭州", 2, 458.9),
                new CityGmv("2024-11-11 00:01", "北京", 5, 1024.5)
        ).sinkTo(mysqlSink)
         .name("mysql-upsert-sink");

        env.execute("JDBC Sink Demo");
    }

    /** 城市维度统计结果 */
    public static class CityGmv {
        public String statTime;
        public String city;
        public long orderCnt;
        public double gmvAmount;

        public CityGmv() {}

        public CityGmv(String statTime, String city, long orderCnt, double gmvAmount) {
            this.statTime = statTime;
            this.city = city;
            this.orderCnt = orderCnt;
            this.gmvAmount = gmvAmount;
        }
    }
}
