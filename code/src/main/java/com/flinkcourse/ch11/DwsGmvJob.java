package com.flinkcourse.ch11;

import com.flinkcourse.common.util.KafkaUtil;

import com.flinkcourse.common.util.JsonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 第11章 作业③：DWD → DWS 聚合层 → ADS 落库
 *
 * 职责：dwd_order 按城市 10 秒滚动窗口聚合 GMV/订单数，upsert 写入 MySQL dws_gmv（大屏直读）
 *
 * 知识点（第5章的"积木"拼成项目）：
 *   - 滚动事件时间窗口 + aggregate(增量, ProcessWindowFunction 包装) 黄金组合
 *   - JdbcSink 攒批 upsert
 *
 * 运行后查结果：
 *   docker exec -it flink-mysql mysql -uroot -pflink123 flink_course \
 *     -e "SELECT * FROM dws_gmv ORDER BY update_time DESC LIMIT 10"
 */
public class DwsGmvJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);
        env.setParallelism(2);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaUtil.BOOTSTRAP)
                .setTopics("dwd_order")
                .setGroupId("dws-gmv")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<CityGmv> aggregated = env
                .fromSource(source,
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((s, recordTs) -> JsonUtil.getLong(s, "ts"))
                                .withIdleness(Duration.ofSeconds(60)),
                        "dwd-source")
                .keyBy(s -> JsonUtil.getStr(s, "city"))          // 城市维度分 key
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CityGmvAgg(), new CityGmvWindow());

        // ADS 层落库：Sink2 API（攒批 + upsert 幂等）
        JdbcSink<CityGmv> mysqlSink = JdbcSink
                .<CityGmv>builder()
                .withQueryStatement(
                        "INSERT INTO dws_gmv (stat_time, city, order_cnt, gmv_amount) VALUES (?, ?, ?, ?) "
                                + "ON DUPLICATE KEY UPDATE "
                                + "order_cnt = VALUES(order_cnt), gmv_amount = VALUES(gmv_amount)",
                        (ps, stat) -> {
                            ps.setString(1, stat.statTime);
                            ps.setString(2, stat.city);
                            ps.setLong(3, stat.orderCnt);
                            ps.setDouble(4, stat.gmvAmount);
                        })
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withBatchSize(100).withBatchIntervalMs(1000).withMaxRetries(3)
                        .build())
                .buildAtLeastOnce(
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/flink_course"
                                        + "?useSSL=false&serverTimezone=Asia/Shanghai")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root").withPassword("flink123")
                                .build());

        aggregated.sinkTo(mysqlSink)
                  .name("ads-mysql-sink");

        env.execute("DWS GMV Job");
    }

    /** 增量聚合：累加器 = [订单数, 金额]，与窗口数据量无关 */
    public static class CityGmvAgg implements AggregateFunction<String, double[], double[]> {
        @Override
        public double[] createAccumulator() { return new double[2]; }

        @Override
        public double[] add(String json, double[] acc) {
            acc[0] += 1;
            acc[1] += JsonUtil.getDouble(json, "amount");
            return acc;
        }

        @Override
        public double[] getResult(double[] acc) { return acc; }

        @Override
        public double[] merge(double[] a, double[] b) {
            a[0] += b[0];
            a[1] += b[1];
            return a;
        }
    }

    /** 包装函数：补窗口标识与城市 key，产出落库结构 */
    public static class CityGmvWindow
            extends ProcessWindowFunction<double[], CityGmv, String, TimeWindow> {
        @Override
        public void process(String city, Context ctx, Iterable<double[]> accs,
                            Collector<CityGmv> out) {
            double[] acc = accs.iterator().next();
            out.collect(new CityGmv(
                    String.valueOf(ctx.window().getEnd() / 1000),   // 以窗口结束秒为统计标识
                    city,
                    (long) acc[0],
                    acc[1]));
        }
    }

    /** ADS 层结构：与 MySQL dws_gmv 表字段对齐 */
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
