package com.flinkcourse.ch05;

import com.flinkcourse.common.model.UserClick;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 第5章实战三：会话窗口切分"用户逛店会话"
 *
 * 规则：同一用户相邻两条行为间隔 < 5 秒 → 同一会话；静默超 5 秒 → 会话关闭
 *
 * 知识点：
 *   - 会话窗口边界由数据动态确定（无固定大小）
 *   - ProcessWindowFunction 全量遍历（会话分析需要完整行为序列）
 *
 * 运行方式：IDEA 直接运行（内置测试数据）
 */
public class SessionWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                // 用户 1001：t=1,3 点击（间隔 2s < 5s，同会话）；t=20 下单（静默 12s，新会话）
                new UserClick(1001L, "点击", 1),
                new UserClick(1001L, "点击", 3),
                new UserClick(1001L, "下单", 20),
                // 用户 1002：t=5,6,8 连续行为（同会话）
                new UserClick(1002L, "点击", 5),
                new UserClick(1002L, "加购", 6),
                new UserClick(1002L, "点击", 8)
        )
        .assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserClick>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((u, recordTs) -> u.getEventTime()))
        .keyBy(UserClick::getUserId)                          // 会话按用户分 key
        .window(EventTimeSessionWindows.withGap(Time.seconds(5)))   // 静默 5 秒切会话
        .process(new SessionFunction())
        .print();

        env.execute("Session Window Demo");
    }

    /** 全量窗口函数：会话分析需要完整序列，逐条拼接行为轨迹 */
    public static class SessionFunction
            extends ProcessWindowFunction<UserClick, String, Long, TimeWindow> {
        @Override
        public void process(Long userId, Context ctx, Iterable<UserClick> clicks,
                            Collector<String> out) {
            List<String> actions = new ArrayList<>();
            clicks.forEach(c -> actions.add(c.getAction()));
            out.collect(String.format("用户%d 会话[%d~%d): %s",
                    userId,
                    ctx.window().getStart() / 1000,
                    ctx.window().getEnd() / 1000,
                    String.join("→", actions)));
        }
    }
}
