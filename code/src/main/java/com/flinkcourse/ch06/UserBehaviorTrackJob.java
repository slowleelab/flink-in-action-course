package com.flinkcourse.ch06;

import com.flinkcourse.common.model.UserAction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 第6章实战一：用户购买轨迹追踪（ListState + ValueState）
 *
 * 规则：出现"加购"时输出该用户完整浏览轨迹 → 识别高意向用户
 *
 * 知识点：
 *   - KeyedProcessFunction：状态管理的"完全体"
 *   - open() 里声明状态（一次），processElement() 里读写（每条）
 *   - 状态按 key 自动隔离：用户 1001/1002 互不干扰
 *
 * 运行方式：IDEA 直接运行（内置测试数据）
 */
public class UserBehaviorTrackJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                new UserAction(1001L, "view", "sku_A", 1L),
                new UserAction(1001L, "view", "sku_B", 2L),
                new UserAction(1002L, "view", "sku_C", 3L),
                new UserAction(1001L, "cart", "sku_A", 4L),   // ← 1001 加购，输出轨迹
                new UserAction(1002L, "view", "sku_C", 5L),
                new UserAction(1002L, "cart", "sku_C", 6L)    // ← 1002 加购，输出轨迹
        )
        .keyBy(UserAction::getUserId)
        .process(new TrackFunction())
        .print();

        env.execute("User Behavior Track");
    }

    /** 状态机核心：两个状态 —— 轨迹列表 + 首次活跃时间 */
    public static class TrackFunction
            extends KeyedProcessFunction<Long, UserAction, String> {

        /** transient：状态对象由框架管理序列化，不随算子对象走 Java 序列化 */
        private transient ListState<String> traceState;
        private transient ValueState<Long> firstTsState;

        @Override
        public void open(Configuration parameters) {
            traceState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("trace", String.class));
            firstTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("firstTs", Long.class));
        }

        @Override
        public void processElement(UserAction action, Context ctx, Collector<String> out)
                throws Exception {
            // 轨迹追加（ListState.add）
            traceState.add(action.getAction() + ":" + action.getSku());

            // 记录首次活跃时间（ValueState 读→判空→写）
            if (firstTsState.value() == null) {
                firstTsState.update(action.getTs());
            }

            if ("cart".equals(action.getAction())) {
                List<String> trace = new ArrayList<>();
                traceState.get().forEach(trace::add);
                out.collect(String.format("🔥 高意向用户 %d（首次活跃 t=%d）轨迹: %s",
                        ctx.getCurrentKey(), firstTsState.value(), String.join(" → ", trace)));
            }
        }
    }
}
