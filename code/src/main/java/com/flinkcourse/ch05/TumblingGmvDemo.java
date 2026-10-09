package com.flinkcourse.ch05;

import com.flinkcourse.common.model.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 第5章实战一：滚动窗口统计分钟级 GMV（demo 用 10 秒窗口）
 *
 * 知识点：
 *   - AggregateFunction 增量聚合（累加器只有两个数，与窗口数据量无关）
 *   - ProcessWindowFunction 包装补窗口元信息（起止时间）
 *   - 生产黄金组合：aggregate(增量, 包装)
 *
 * 运行方式：IDEA 直接运行（内置测试数据），无需外部组件
 */
public class TumblingGmvDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .fromElements(
                        new Order(1001L, 299.0, "深圳", Order.STATUS_PAID, 1730000000000L),  // ts=0s
                        new Order(1002L, 59.9,  "杭州", Order.STATUS_PAID, 1730000001000L),  // ts=1s
                        new Order(1001L, 1299.0,"深圳", Order.STATUS_PAID, 1730000008000L),  // ts=8s
                        new Order(1003L, 89.0,  "北京", Order.STATUS_PENDING, 1730000009000L),// ts=9s 未支付→过滤
                        new Order(1002L, 399.0, "杭州", Order.STATUS_PAID, 1730000012000L),  // ts=12s
                        new Order(1004L, 25.5,  "成都", Order.STATUS_PAID, 1730000019000L)   // ts=19s
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((o, recordTs) -> o.getEventTime()))
                .filter(o -> o.getStatus() == Order.STATUS_PAID)     // GMV 只算已支付
                .keyBy(o -> "all")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new GmvAgg(), new GmvWindow());

        result.print();
        env.execute("Tumbling GMV");
    }

    /** 增量聚合器：累加器 = (订单数, 金额)，每来一条 O(1) 更新 */
    public static class GmvAgg implements AggregateFunction<Order, GmvAcc, GmvResult> {
        @Override
        public GmvAcc createAccumulator() { return new GmvAcc(); }

        @Override
        public GmvAcc add(Order o, GmvAcc acc) {
            acc.count++;
            acc.amount += o.getAmount();
            return acc;
        }

        @Override
        public GmvResult getResult(GmvAcc acc) {
            return new GmvResult(acc.count, acc.amount);
        }

        @Override
        public GmvAcc merge(GmvAcc a, GmvAcc b) {   // 仅会话窗口合并场景使用
            a.count += b.count;
            a.amount += b.amount;
            return a;
        }
    }

    /** 包装函数：给增量结果补上窗口起止时间 */
    public static class GmvWindow
            extends ProcessWindowFunction<GmvResult, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<GmvResult> elems,
                            Collector<String> out) {
            GmvResult r = elems.iterator().next();   // 增量聚合后这里只有一个结果
            out.collect(String.format("窗口[%d ~ %d) GMV=%.2f 订单数=%d",
                    ctx.window().getStart(), ctx.window().getEnd(), r.amount, r.count));
        }
    }

    /** 值对象：累加器 */
    public static class GmvAcc {
        public long count;
        public double amount;
    }

    /** 值对象：聚合结果 */
    public static class GmvResult {
        public long count;
        public double amount;

        public GmvResult(long count, double amount) {
            this.count = count;
            this.amount = amount;
        }
    }
}
