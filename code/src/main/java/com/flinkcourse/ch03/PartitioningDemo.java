package com.flinkcourse.ch03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 第3章：分区策略与并行度实验（配合 Web UI 观察）
 *
 * 实验步骤：
 *   1. IDEA 运行本类（本地 Web UI 自动起在 8082，避开 docker 的 8081）
 *   2. 浏览器打开 http://localhost:8082
 *   3. 对照观察四个阶段的分区策略：
 *      Stage A（forward）    ：source → map，并行度相同，1对1
 *      Stage B（rebalance）  ：显式调用 .rebalance()，全局轮询分发
 *      Stage B2（rescale）   ：显式调用 .rescale()，就近分组轮询（对比 rebalance）
 *      Stage C（hash）       ：keyBy 后，相同 key 进同一子任务
 *
 * 观察要点：Web UI 中算子之间的连线标注（FORWARD / REBALANCE / RESCALE / HASH）
 * 验证 hash：输出行前缀是 subtask 编号，多跑几次，同一单词的编号永远不变
 */
public class PartitioningDemo {

    public static void main(String[] args) throws Exception {
        // 显式指定本地 Web UI 端口，避免与 docker-compose 的 JobManager(8081) 冲突
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStream<String> source = env.fromElements(
                "apple", "banana", "cherry", "apple", "banana", "apple",
                "date", "elderberry", "fig", "grape", "apple", "banana");

        // Stage A：并行度相同 → forward（1对1 顺序发送）
        DataStream<String> stageA = source.map(s -> s + "-A");

        // Stage B：rebalance → 全局轮询，数据均匀铺到所有子任务（治上游单点/倾斜）
        DataStream<String> stageB = stageA.rebalance().map(s -> s + "-B");

        // Stage B2：rescale → 就近分组轮询，通道更少、优先走本机（对比 rebalance 的连线）
        DataStream<String> stageB2 = stageB.rescale().map(s -> s + "-R");

        // Stage C：keyBy → 按 key 哈希路由（相同单词永远进同一子任务，才能正确计数）
        DataStream<Tuple2<String, Integer>> stageC = stageB2
                .map(s -> Tuple2.of(s.replace("-R", "").replace("-B", "").replace("-A", ""), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);

        stageC.print();

        env.execute("Partitioning Demo");
    }
}
