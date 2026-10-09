package com.flinkcourse.ch10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisPooled;

/**
 * 第10章：自定义 Redis Sink（Jedis 实现，含连接管理教学）
 *
 * 前置条件：docker compose 环境（Redis 6379）
 *
 * 知识点（连接管理铁律）：
 *   - open() 建连接（每个并行子任务一次）
 *   - invoke() 复用连接写数据
 *   - close() 释放连接
 *   - 严禁在 invoke() 里每条数据建连接（TCP+AUTH 握手 × QPS = 性能灾难）
 *
 * 运行：IDEA 运行 main，然后验证：
 *   docker exec flink-redis redis-cli ZREVRANGE city:rank:today 0 -1 WITHSCORES
 */
public class RedisSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                Tuple2.of("深圳", 1598.0),
                Tuple2.of("杭州", 458.9),
                Tuple2.of("北京", 1024.5),
                Tuple2.of("深圳", 399.0)      // 深圳再来一单 → ZINCRBY 累加
        ).addSink(new JedisRichSink());

        env.execute("Redis Sink Demo");
    }

    /** Redis 有序集合：天然的城市 GMV 排行榜 */
    @SuppressWarnings("deprecation")
    public static class JedisRichSink extends RichSinkFunction<Tuple2<String, Double>> {

        /** transient：连接不可序列化，随算子生命周期在 open() 中创建 */
        private transient JedisPooled jedis;

        @Override
        public void open(Configuration parameters) {
            // 每个并行子任务启动时建一次连接（生产：连接池 + 环境变量注入地址）
            jedis = new JedisPooled("localhost", 6379);
        }

        @Override
        public void invoke(Tuple2<String, Double> value, Context context) {
            // ZINCRBY：有序集合按 score 累加 —— 排行榜的标准姿势
            jedis.zincrby("city:rank:today", value.f1, value.f0);
        }

        @Override
        public void close() {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
