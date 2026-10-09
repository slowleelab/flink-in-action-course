package com.flinkcourse.ch12;

import com.flinkcourse.common.model.TransferEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 第12章实现一：转账反欺诈状态机（ValueState + 事件时间定时器 + 侧输出流）
 *
 * 规则：小额(<1000)转账试探后，10 分钟内出现大额(>=10000)转账 → 判定疑似盗号
 *
 * 运行方式：
 *   1. 终端1：nc -lk 9999
 *   2. 终端2：mvn exec:exec -Dexec.mainClass="com.flinkcourse.ch12.FraudDetectStateJob"
 *   3. 输入格式：账户ID,金额,事件秒
 *
 * 实验脚本（先预测输出再验证）：
 *   输入 1001,50,1       → 小额试探，进入试探态（设 10 分钟闹钟）
 *   输入 1002,300,2      → 另一账户独立判断
 *   输入 1001,50000,10   → 10 秒后大额 → 🚨 告警！（走侧输出）
 *   输入 1002,50000,400  → 试探已超时（定时器清了状态）→ 正常透传
 */
public class FraudDetectStateJob {

    /** 侧输出标签：告警通道（与主流分离） */
    static final OutputTag<String> ALERT_TAG = new OutputTag<String>("alerts"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> alerts = env
                .socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] p = line.split(",");
                    return new TransferEvent(Long.parseLong(p[0].trim()),
                            Double.parseDouble(p[1].trim()),
                            Long.parseLong(p[2].trim()) * 1000L);
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        // 演示要求按 ts 递增输入；生产用 forBoundedOutOfOrderness
                        .<TransferEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((e, recordTs) -> e.getTs()))
                .keyBy(TransferEvent::getAccountId)
                .process(new FraudStateMachine())
                .getSideOutput(ALERT_TAG);      // 只取侧输出的告警流

        alerts.print("🚨");
        env.execute("Fraud Detect (State Machine)");
    }

    /**
     * 状态机核心：
     *   IDLE --小额--> SMALL(记录试探时间+设10分钟闹钟)
     *   SMALL --时限内大额--> 告警并复位
     *   SMALL --闹钟到--> onTimer 清状态回 IDLE
     */
    public static class FraudStateMachine
            extends KeyedProcessFunction<Long, TransferEvent, TransferEvent> {

        /** 非空 = 已发生小额试探，值为试探时间戳 */
        private transient ValueState<Long> smallTsState;

        @Override
        public void open(Configuration parameters) {
            smallTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("smallTs", Long.class));
        }

        @Override
        public void processElement(TransferEvent e, Context ctx,
                                   Collector<TransferEvent> out) throws Exception {
            Long smallTs = smallTsState.value();

            if (e.getAmount() < 1000) {
                // ---- 小额：进入试探态，设 10 分钟事件时间闹钟 ----
                smallTsState.update(e.getTs());
                ctx.timerService().registerEventTimeTimer(e.getTs() + 10 * 60 * 1000L);

            } else if (e.getAmount() >= 10000) {
                if (smallTs != null && e.getTs() - smallTs <= 10 * 60 * 1000L) {
                    // ---- 时限内的大额转账 = 欺诈！走侧输出告警 ----
                    ctx.output(ALERT_TAG, String.format(
                            "🚨 账户%d 疑似盗号：t=%d 小额试探后，t=%d 转走 ¥%.0f",
                            ctx.getCurrentKey(), smallTs / 1000, e.getTs() / 1000, e.getAmount()));
                    smallTsState.clear();       // 告警后复位，防止重复告警
                } else {
                    out.collect(e);             // 大额但无试探 → 正常交易透传
                }
            } else {
                out.collect(e);                 // 中等金额 → 透传
            }
        }

        /** 10 分钟到：试探窗口过期 → 状态机回到 IDLE */
        @Override
        public void onTimer(long timerTs, OnTimerContext ctx, Collector<TransferEvent> out) {
            smallTsState.clear();
        }
    }
}
