package com.flinkcourse.common.util;

/**
 * Kafka 连接配置（全课程统一入口）
 *
 * 默认 localhost:9092；若本机 Kafka 对外端口不同（例如复用其他项目的 Kafka，
 * 其 HOST 监听在 9094），运行作业前设置环境变量即可，无需改代码：
 *
 *   export KAFKA_BOOTSTRAP=localhost:9094
 *
 * 这也是生产环境的常见做法：连接地址外置为配置，代码与环境解耦。
 */
public final class KafkaUtil {

    /** Kafka bootstrap servers，可通过环境变量 KAFKA_BOOTSTRAP 覆盖 */
    public static final String BOOTSTRAP =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

    private KafkaUtil() {
    }
}
