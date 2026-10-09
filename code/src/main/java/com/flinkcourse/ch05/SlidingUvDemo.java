package com.flinkcourse.ch05;

import com.flinkcourse.common.model.UserClick;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * 第5章实战二：滑动窗口 UV 统计（"近 5 分钟每 1 分钟"的 demo 压缩版：size=10s, slide=5s）
 *
 * 知识点：
 *   - 滑动窗口：同一份数据会落在多个窗口（size=10s、slide=5s → 每条数据属于 2 个窗口）
 *   - UV 去重：聚合器里放 HashSet（教学版；生产用 RoaringBitmap/HLL，见第11章）
 *
 * 运行方式：IDEA 直接运行（内置测试数据）
 */
public class SlidingUvDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .fromElements(
                        new UserClick(1001L, "view", 1),
                        new UserClick(1002L, "view", 2),
                        new UserClick(1003L, "view", 3),
                        new UserClick(1001L, "view", 7),
                        new UserClick(1002L, "view", 8),
                        new UserClick(1004L, "view", 12),
                        new UserClick(1001L, "view", 13),
                        new UserClick(1005L, "view", 16)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserClick>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((u, recordTs) -> u.getEventTime()))
                .keyBy(u -> "all")
                // 滑动窗口：窗口长 10 秒，每 5 秒滑动一格
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UvAgg(), new UvWindow());

        result.print();
        env.execute("Sliding UV");
    }

    /**
     * UV 聚合器：HashSet 去重
     * 注意：HashSet 随 UV 增长线性膨胀 —— 大促场景换 RoaringBitmap（第11章生产化讨论）
     */
    public static class UvAgg implements AggregateFunction<UserClick, Set<Long>, Long> {
        @Override
        public Set<Long> createAccumulator() { return new HashSet<>(); }

        @Override
        public Set<Long> add(UserClick u, Set<Long> acc) {
            acc.add(u.getUserId());
            return acc;
        }

        @Override
        public Long getResult(Set<Long> acc) { return (long) acc.size(); }

        @Override
        public Set<Long> merge(Set<Long> a, Set<Long> b) {
            a.addAll(b);
            return a;
        }
    }

    /** 包装函数：补窗口起止时间 */
    public static class UvWindow
            extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Long> uvs, Collector<String> out) {
            out.collect(String.format("窗口[%d ~ %d) UV=%d",
                    ctx.window().getStart() / 1000, ctx.window().getEnd() / 1000,
                    uvs.iterator().next()));
        }
    }
}
