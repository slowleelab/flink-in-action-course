package com.flinkcourse.ch02;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 第2章：有界数据词频统计（批 —— 模拟"昨天日志文件"热词）
 *
 * 与 StreamWordCount 唯一的区别是 Source：fromElements 是有限数据，
 * 读完全部数据后作业自动结束 —— 这就是"流批一体"：同一个引擎、同一套 API。
 *
 * 运行方式：IDEA 直接运行 main，无需任何外部组件。
 */
public class BoundedWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 有界数据：模拟一份历史日志
        DataStream<String> lines = env.fromElements(
                "flink spark flink",
                "kafka flink redis",
                "flink kafka hive flink"
        );

        lines.flatMap(new StreamWordCount.Tokenizer())   // 复用第2章的拆词逻辑
             .keyBy(value -> value.f0)
             .sum(1)
             .print();

        env.execute("Bounded WordCount");
    }
}
