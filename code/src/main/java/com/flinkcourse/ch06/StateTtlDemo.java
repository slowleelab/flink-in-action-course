package com.flinkcourse.ch06;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 第6章实战二：状态 TTL —— 让状态自动过期
 *
 * 场景："每设备 5 秒内只提醒一次"，超过 5 秒状态自动失效
 *
 * 交互实验（亲手验证 TTL）：
 *   1. 终端1：nc -lk 9999
 *   2. 终端2：mvn exec:exec -Dexec.mainClass="com.flinkcourse.ch06.StateTtlDemo"
 *   3. 快速连续输入两次 deviceA —— 第二次被去重（"5秒内已提醒"）
 *   4. 等待 5 秒以上再输入 deviceA —— 重新提醒（状态已被 TTL 过期）
 *
 * 注意：TTL 基于"处理时间"（默认），过期是惰性清理（读时判断），
 *      NeverReturnExpired 保证读不到已过期数据。
 */
public class StateTtlDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
           .keyBy(line -> line.trim())
           .process(new DedupWithTtl())
           .print();

        env.execute("State TTL Demo");
    }

    /** TTL 去重：状态存在 = 5 秒内已处理过 */
    public static class DedupWithTtl
            extends KeyedProcessFunction<String, String, String> {

        private transient ValueState<Boolean> seenState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> desc =
                    new ValueStateDescriptor<>("seen", Boolean.class);
            desc.enableTimeToLive(StateTtlConfig
                    .newBuilder(Time.seconds(5))                                       // 存活 5 秒
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)         // 写时续期
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 过期即不可见
                    .build());
            seenState = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(String deviceId, Context ctx, Collector<String> out)
                throws Exception {
            if (seenState.value() == null) {
                seenState.update(true);                       // 首次出现 → 标记
                out.collect("🔔 设备 " + deviceId + " 首次上报，触发提醒");
            } else {
                out.collect("⏭ 设备 " + deviceId + " 5秒内已提醒过，跳过（TTL 去重生效）");
            }
        }
    }
}
