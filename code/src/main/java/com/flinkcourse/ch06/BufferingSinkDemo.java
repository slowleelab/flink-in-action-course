package com.flinkcourse.ch06;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 第6章实战三：Operator State —— 批量缓冲 Sink（Checklist 式状态）
 *
 * 场景：写库前攒批缓冲，每攒 3 条 flush 一次；故障恢复时缓冲区不丢
 * （生产请直接用 JdbcSink 内置攒批，见第10章；本例为理解 Operator State 的教学实现）
 *
 * 知识点：
 *   - CheckpointedFunction：snapshotState 存 / initializeState 恢复
 *   - Operator ListState：每个并行子任务一份（与 Keyed State 的本质区别）
 *   - RichSinkFunction 已被标记 @Deprecated（新项目用 Sink2），教学保留经典写法
 *
 * 运行方式：IDEA 直接运行（内置测试数据）
 */
public class BufferingSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("order-1", "order-2", "order-3", "order-4",
                         "order-5", "order-6", "order-7")
           .addSink(new BufferingSink(3))    // 每攒 3 条 flush
           .name("buffering-sink");

        env.execute("Operator State Demo");
    }

    /** 缓冲 Sink：本地 List 缓冲 + ListState 快照 */
    @SuppressWarnings("deprecation")
    public static class BufferingSink extends RichSinkFunction<String>
            implements CheckpointedFunction {

        private final int batchSize;
        private final transient List<String> buffer = new ArrayList<>();  // 本地缓冲（不序列化）
        private transient ListState<String> checkpointedState;            // 快照状态

        public BufferingSink(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            buffer.add(value);
            if (buffer.size() >= batchSize) {
                flush();
            }
        }

        private void flush() {
            System.out.println("💾 flush " + buffer.size() + " 条: " + buffer);
            buffer.clear();
        }

        /** Checkpoint 时：把本地缓冲写入快照状态 */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            checkpointedState.addAll(buffer);
        }

        /** 启动/恢复时：从快照还原缓冲区 */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            checkpointedState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("buffered-lines", String.class));
            if (context.isRestored()) {          // 故障恢复 → 倒回快照内容
                for (String line : checkpointedState.get()) {
                    buffer.add(line);
                }
                System.out.println("♻️ 从 Checkpoint 恢复缓冲区: " + buffer);
            }
        }
    }
}
