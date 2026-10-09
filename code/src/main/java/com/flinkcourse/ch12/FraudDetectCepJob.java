package com.flinkcourse.ch12;

import com.flinkcourse.common.model.TransferEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 第12章实现二：CEP 版反欺诈（与 FraudDetectStateJob 等价，声明式写法）
 *
 * 模式：小额(<1000) → 紧邻下一笔 → 大额(>=10000)，整体在 10 分钟内完成
 *
 * 运行方式：与状态机版相同（nc -lk 9999 后运行本类，输入 账户ID,金额,事件秒）
 *
 * 对比要点（面试）：CEP 声明式、开发快；状态机完全可控、大流量/复杂规则更稳
 */
public class FraudDetectCepJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TransferEvent> transfers = env
                .socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] p = line.split(",");
                    return new TransferEvent(Long.parseLong(p[0].trim()),
                            Double.parseDouble(p[1].trim()),
                            Long.parseLong(p[2].trim()) * 1000L);
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TransferEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((e, recordTs) -> e.getTs()));

        // ① 定义模式：小额 → 大额（10 分钟内）
        Pattern<TransferEvent, ?> pattern = Pattern
                .<TransferEvent>begin("small", AfterMatchSkipStrategy.skipToNext())
                .where(new SimpleCondition<TransferEvent>() {
                    @Override
                    public boolean filter(TransferEvent e) {
                        return e.getAmount() < 1000;          // 小额试探
                    }
                })
                .next("large")
                .where(new SimpleCondition<TransferEvent>() {
                    @Override
                    public boolean filter(TransferEvent e) {
                        return e.getAmount() >= 10000;        // 大额转账
                    }
                })
                .within(Time.minutes(10));                    // 模式整体时限

        // ② 模式作用在 keyed 流（CEP 前必须 keyBy，模式按 key 隔离匹配）
        PatternStream<TransferEvent> patternStream =
                CEP.pattern(transfers.keyBy(TransferEvent::getAccountId), pattern);

        // ③ 匹配到的事件序列交给处理函数
        patternStream.process(new PatternProcessFunction<TransferEvent, String>() {
            @Override
            public void processMatch(Map<String, List<TransferEvent>> match,
                                     Context ctx, Collector<String> out) {
                TransferEvent small = match.get("small").get(0);
                TransferEvent large = match.get("large").get(0);
                out.collect(String.format("🚨 账户%d：t=%d 小额 ¥%.0f 试探后，t=%d 转走 ¥%.0f",
                        large.getAccountId(),
                        small.getTs() / 1000, small.getAmount(),
                        large.getTs() / 1000, large.getAmount()));
            }
        }).print("🚨");

        env.execute("Fraud Detect (CEP)");
    }
}
