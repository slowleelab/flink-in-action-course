package com.flinkcourse.ch07;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * 第7章：Checkpoint 故障恢复实验（带状态计数 + 注入故障）
 *
 * 实验步骤（见证"从快照续算"）：
 *   1. 终端1：nc -lk 9999
 *   2. 终端2：mvn exec:exec -Dexec.mainClass="com.flinkcourse.ch07.CheckpointDemo"
 *   3. 在终端1 持续输入单词（注意：必须输入含 boom 的行触发故障），保持作业运行超过 10 秒
 *      （让至少 1~2 次 Checkpoint 完成，日志会打印 "Triggering checkpoint"）
 *   4. 输入 boom 十次后作业抛异常 → 自动重启 →
 *      再输入其他单词，观察累计值从"快照值"继续，而不是从 1 重新计数！
 *
 * 知识点：
 *   - Checkpoint 配置三件套：interval / mode / storage
 *   - 重启策略：fixedDelay（3 次，间隔 10s）
 *   - 状态随快照恢复：这正是第2章 (hello,2) 的"账本"不丢的原因
 */
public class CheckpointDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ---- Checkpoint 生产模板配置 ----
        env.enableCheckpointing(5_000, CheckpointingMode.EXACTLY_ONCE);   // 5 秒一次（实验值；生产 60s 起）
        CheckpointConfig cfg = env.getCheckpointConfig();
        cfg.setCheckpointTimeout(60_000);
        cfg.setMinPauseBetweenCheckpoints(2_000);
        cfg.setCheckpointStorage("file:///tmp/flink-course-ckpt");

        // ---- 显式重启策略（生产建议 failureRate；这里 fixedDelay 便于实验） ----
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, Time.of(10, TimeUnit.SECONDS)));

        env.socketTextStream("localhost", 9999)
           .keyBy(line -> line.trim())
           .process(new CountWithState())
           .print();

        env.execute("Checkpoint Demo");
    }

    /** 带状态的计数器：boom 满 10 次抛异常（模拟机器宕机） */
    public static class CountWithState
            extends KeyedProcessFunction<String, String, String> {

        private transient ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(String word, Context ctx, Collector<String> out)
                throws Exception {
            Long c = countState.value();
            c = (c == null ? 0 : c) + 1;
            countState.update(c);

            // ---- 故障注入：boom 输入 10 次后"宕机" ----
            if ("boom".equals(ctx.getCurrentKey()) && c == 10) {
                throw new RuntimeException("💥 模拟机器宕机！作业将从最近 Checkpoint 恢复");
            }

            out.collect("key=" + ctx.getCurrentKey() + " 累计=" + c
                    + "（恢复后从快照值续算）");
        }
    }
}
