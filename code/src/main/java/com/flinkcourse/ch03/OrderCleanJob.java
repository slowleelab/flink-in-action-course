package com.flinkcourse.ch03;

import com.flinkcourse.common.model.Order;
import com.flinkcourse.common.util.JsonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 第3章：订单清洗管道（map / filter / keyBy / reduce 综合）
 *
 * 两种运行模式：
 *   1. IDEA 直接运行 —— 内置测试数据，无需外部组件
 *   2. args[0] == "kafka" —— 从 topic ods_orders 读取（需 docker compose 环境，
 *      并先运行 ch11.MockUserActionProducer 或手动灌数据）
 */
public class OrderCleanJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> raw = (args.length > 0 && "kafka".equals(args[0]))
                ? env.fromSource(buildKafkaSource(), WatermarkStrategy.noWatermarks(), "orders")
                : demoSource(env);

        raw.map(JsonUtil::toOrder)                            // ① 字符串 → POJO
           .filter(o -> o != null && o.getAmount() > 0)       // ② 去脏数据（金额非法）
           .filter(o -> o.getStatus() == Order.STATUS_PAID)   // ③ 只留已支付订单
           .map(o -> {                                        // ④ 手机号脱敏
               if (o.getPhone() != null && o.getPhone().length() == 11) {
                   o.setPhone(o.getPhone().replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2"));
               }
               return o;
           })
           .keyBy(Order::getUserId)                           // ⑤ 按用户逻辑分组
           .reduce((acc, o) -> {                              // ⑥ 滚动累计消费额
               acc.setAmount(acc.getAmount() + o.getAmount());
               return acc;
           })
           .print();

        env.execute("Order Clean Job");
    }

    /** Kafka Source：生产 90% 场景的标配（详见第10章） */
    private static KafkaSource<String> buildKafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("ods_orders")
                .setGroupId("order-clean")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /** 零依赖测试数据：与第3章文档中的示例一致 */
    private static DataStream<String> demoSource(StreamExecutionEnvironment env) {
        return env.fromElements(
                "{\"userId\":1001,\"amount\":299.0,\"city\":\"深圳\",\"status\":2,\"phone\":\"13800000001\",\"ts\":1730000001000}",
                "{\"userId\":1001,\"amount\":1299.0,\"city\":\"深圳\",\"status\":2,\"phone\":\"13800000001\",\"ts\":1730000002000}",
                "{\"userId\":1002,\"amount\":-5.0,\"city\":\"杭州\",\"status\":1,\"phone\":\"13800000002\",\"ts\":1730000003000}",
                "{\"userId\":1002,\"amount\":399.0,\"city\":\"杭州\",\"status\":2,\"phone\":\"13800000002\",\"ts\":1730000004000}",
                "{\"userId\":1003,\"amount\":89.0,\"city\":\"北京\",\"status\":1,\"phone\":\"13800000003\",\"ts\":1730000005000}",
                "{\"userId\":1002,\"amount\":59.9,\"city\":\"杭州\",\"status\":2,\"phone\":\"13800000002\",\"ts\":1730000006000}"
        );
    }
}
