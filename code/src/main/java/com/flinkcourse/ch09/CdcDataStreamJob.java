package com.flinkcourse.ch09;

import com.flinkcourse.common.util.MysqlUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * 第9章：DataStream 版 MySQL CDC（需要精细控制时：位点、自定义反序列化等）
 *
 * 运行方式：docker compose 环境下 IDEA 直接运行；
 *          启动后去 MySQL 改 orders 表，观察增量输出
 *
 * 输出格式：Debezium 的 SourceRecord.value()（Struct 的 toString）
 *          包含 before/after/op 等 binlog 变更元信息
 */
public class CdcDataStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);   // CDC 的全量分片进度存在状态里，CP 保证断点续传

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(MysqlUtil.HOST_BASE)
                .port(Integer.parseInt(MysqlUtil.PORT))
                .databaseList("flink_course")            // 库级监听
                .tableList("flink_course.orders")        // 精确到表
                .username(MysqlUtil.USER)
                .password(MysqlUtil.PASSWORD)
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new SimpleStringDeserializer())   // 自定义反序列化（见下）
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC")
           .print();

        env.execute("CDC DataStream Job");
    }

    /**
     * 自定义反序列化器：把 Debezium 的 SourceRecord 转成 String
     * 生产中常见做法是解析 Struct 拼 JSON；这里直接 toString 保持教学简洁
     */
    public static class SimpleStringDeserializer implements DebeziumDeserializationSchema<String> {
        @Override
        public void deserialize(SourceRecord record, Collector<String> out) {
            out.collect(record.value().toString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
