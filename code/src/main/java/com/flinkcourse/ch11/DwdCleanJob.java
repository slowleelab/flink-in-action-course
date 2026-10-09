package com.flinkcourse.ch11;

import com.flinkcourse.common.util.JsonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 第11章 作业②：ODS → DWD 清洗层
 *
 * 职责：从 ods_user_action 读取原始埋点，
 *   只保留有效订单（action=order 且 status=2 且 amount>0），写入 dwd_order topic
 *
 * 知识点：Kafka Source/Sink 组合、filter 清洗规则、Watermark 挂载在 Source
 *
 * 运行顺序（三个终端）：
 *   1. MockUserActionProducer（数据源）
 *   2. DwdCleanJob（本作业）
 *   3. DwsGmvJob（下游聚合）
 * 验证：docker exec flink-kafka kafka-console-consumer.sh --topic dwd_order \
 *         --bootstrap-server localhost:9092 --from-beginning
 */
public class DwdCleanJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(2);

        // ---- Source：ODS 原始层 ----
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("ods_user_action")
                .setGroupId("dwd-clean")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // ---- Sink：DWD 明细层 ----
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dwd_order")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // AT_LEAST_ONCE + 下游 upsert 幂等 = 工程上的"最终精确一次"（见第7章）
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        env.fromSource(source,
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((s, recordTs) -> JsonUtil.getLong(s, "ts"))
                                .withIdleness(Duration.ofSeconds(60)),   // 空闲分区不卡水印（第4章）
                        "ods-source")
           .filter(s -> {
               // ---- DWD 清洗规则（所有下游共用的统一口径） ----
               if (!"order".equals(JsonUtil.getStr(s, "action"))) return false;  // 只留订单
               if (JsonUtil.getInt(s, "status") != 2)               return false; // 只留已支付
               return JsonUtil.getDouble(s, "amount") > 0;                        // 金额合法
           })
           .sinkTo(sink)
           .name("dwd-kafka-sink");

        env.execute("DWD Clean Job");
    }
}
