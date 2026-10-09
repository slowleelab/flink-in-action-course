package com.flinkcourse.ch04;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 第4章：Watermark 驱动窗口触发的交互式实验
 *
 * 运行方式：
 *   1. 终端1：nc -lk 9999
 *   2. 终端2：mvn exec:exec -Dexec.mainClass="com.flinkcourse.ch04.WatermarkSocketDemo"
 *
 * 输入格式：单词,事件时间秒     例：apple,1
 * 实验脚本（先预测输出再验证！）：
 *   输入 apple,1    → 无输出   (W = 1-2 = -1)
 *   输入 banana,4   → 无输出   (W = 4-2 = 2)
 *   输入 cat,8      → 窗口[0,5) 触发！ (W = 8-2 = 6 ≥ 5)
 *   输入 dog,3      → 被丢弃（迟到：3 < W=6）
 *   输入 egg,13     → 窗口[5,10) 触发！ (W = 13-2 = 11 ≥ 10)
 */
public class WatermarkSocketDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);   // 单并行度，排除多流干扰，专注观察水印推进

        SingleOutputStreamOperator<String> result = env
                .socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] parts = line.split(",");
                    return new Event(parts[0].trim(), Long.parseLong(parts[1].trim()) * 1000L);
                })
                .returns(Types.POJO(Event.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        // 乱序容忍度 2 秒：W = maxTs - 2s
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((e, recordTs) -> e.getEventTime()))
                .keyBy(e -> "all")
                // 5 秒滚动事件时间窗口（窗口语法详见第5章，本章只关注"何时触发"）
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new WindowWithMetaFunction());

        result.print();
        env.execute("Watermark Demo");
    }

    /** 输出窗口内容 + 元信息，帮助理解触发时机 */
    public static class WindowWithMetaFunction
            extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Event> elems, Collector<String> out) {
            StringBuilder sb = new StringBuilder();
            sb.append("窗口[").append(ctx.window().getStart() / 1000).append(",")
              .append(ctx.window().getEnd() / 1000).append(") 内容: ");
            elems.forEach(e -> sb.append(e.getWord()).append(" "));
            sb.append("| 触发时 watermark=").append(ctx.currentWatermark() / 1000).append("s");
            out.collect(sb.toString());
        }
    }

    /** 简单事件 POJO（word + 毫秒事件时间） */
    public static class Event {
        private String word;
        private long eventTime;

        public Event() {}

        public Event(String word, long eventTime) {
            this.word = word;
            this.eventTime = eventTime;
        }

        public String getWord() { return word; }
        public void setWord(String word) { this.word = word; }
        public long getEventTime() { return eventTime; }
        public void setEventTime(long eventTime) { this.eventTime = eventTime; }
    }
}
