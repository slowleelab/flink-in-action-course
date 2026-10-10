package com.flinkcourse.ch11;

import com.flinkcourse.common.util.KafkaUtil;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * 第11章：模拟埋点生产者（普通 Java 程序，非 Flink 作业）
 *
 * 每约 200ms 产生 1 条用户行为：
 *   - 60% view / 24% cart / 16% order（order 中 95% 金额合法，5% 脏数据）
 *   - 3% 数据 ts 故意回拨 3~8 秒（制造乱序，检验 Watermark）
 * 写入 topic：ods_user_action
 *
 * 运行方式：IDEA 直接运行 main（需 docker compose 的 Kafka），Ctrl+C 停止
 */
public class MockUserActionProducer {

    static final String TOPIC = "ods_user_action";

    public static void main(String[] args) throws Exception {
        String[] cities = {"深圳", "杭州", "北京", "上海", "成都", "武汉"};
        Random rnd = new Random();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtil.BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            long sent = 0;
            System.out.println("🚀 开始生产用户行为数据到 topic " + TOPIC + "（Ctrl+C 停止）");
            while (true) {
                long uid = 1000 + rnd.nextInt(200);
                String city = cities[rnd.nextInt(cities.length)];

                int dice = rnd.nextInt(100);
                String action = dice < 60 ? "view" : (dice < 84 ? "cart" : "order");

                // order 才有金额；5% 概率脏数据（负数金额，检验 DWD 清洗）
                double amount = 0;
                if ("order".equals(action)) {
                    amount = rnd.nextInt(100) < 5 ? -rnd.nextInt(100) : rnd.nextInt(2000) + 1;
                }

                // 3% 乱序：ts 回拨 3~8 秒（检验 Watermark）
                long ts = System.currentTimeMillis();
                if (rnd.nextInt(100) < 3) {
                    ts -= 3000 + rnd.nextInt(5000);
                }

                String json = String.format(
                        "{\"userId\":%d,\"action\":\"%s\",\"amount\":%.2f,\"city\":\"%s\",\"status\":%d,\"ts\":%d}",
                        uid, action, amount, city, "order".equals(action) ? 2 : 0, ts);

                // key = userId：同一用户进同一分区，保证用户内有序
                producer.send(new ProducerRecord<>(TOPIC, String.valueOf(uid), json),
                        (meta, e) -> { if (e != null) e.printStackTrace(); });

                sent++;
                if (sent % 50 == 0) {
                    System.out.println("已发送 " + sent + " 条（含脏数据与乱序样本）");
                }
                Thread.sleep(200);
            }
        }
    }
}
