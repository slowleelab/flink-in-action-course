package com.flinkcourse.ch02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 第2章：实时词频统计（无界流 —— 模拟实时弹幕热词）
 *
 * 运行方式：
 *   1. 终端1：nc -lk 9999
 *   2. 终端2：mvn exec:exec -Dexec.mainClass="com.flinkcourse.ch02.StreamWordCount"
 *   3. 在终端1 输入文本并回车，观察终端2 的实时词频
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Socket 数据源（无界流）
        DataStream<String> lines = env.socketTextStream("localhost", 9999);

        // 3. 拆词 → 分组 → 计数
        DataStream<Tuple2<String, Integer>> counts = lines
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        // 4. 输出
        counts.print();

        // 5. 触发执行（没有这行，前面的一切都只是"图纸"）
        env.execute("Stream WordCount");
    }

    /** 把一行文本拆成 (单词, 1)；用匿名内部类而非 Lambda，规避提交集群时的序列化问题 */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String word : value.toLowerCase().split("\\W+")) {
                if (!word.isEmpty()) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }
    }
}
