package com.flinkcourse.ch13;

import com.flinkcourse.common.model.DeviceMetric;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 第13章：IoT 设备温度监控告警
 *
 * 规则：
 *   - 每设备 5 秒滚动窗口，均值 > 80°C → 高温告警（主流输出）
 *   - 迟到数据三级防线：Watermark(3s) → allowedLateness(3s) → 侧输出流兜底
 *
 * 运行方式（Socket 交互式，真实复现迟到行为）：
 *   1. 终端1：nc -lk 9999
 *   2. 终端2：mvn exec:exec -Dexec.mainClass="com.flinkcourse.ch13.IotAlertJob"
 *   3. 输入格式：设备ID,温度,事件秒   例：device-1,45.0,0
 *
 * 为什么用 Socket 而不是内置数据（重要教学点，见文档第13章）：
 *   fromElements/fromCollection 是【有界源】，Flink 对有界流禁用周期性 Watermark、
 *   结束时统一发 MAX_WATERMARK——迟到判定永远不会生效。
 *   Socket 是无界源，Watermark 周期（200ms）真实发射，才能观察到"迟到→侧输出"。
 *
 * 实验脚本（照顺序输入，每条间隔 2 秒以上）：
 *   device-1,45.0,0      ┐
 *   device-1,46.0,1      ├ device-1 窗口[0,5) 均值<80 → 正常
 *   device-1,44.0,2      ┘
 *   device-2,88.0,0      ┐
 *   device-2,91.0,1      ├ device-2 窗口[0,5) 均值>80 → 🚨高温告警
 *   device-2,86.0,2      ┘
 *   device-2,90.0,10     ← W 推进到 10-3=7 ≥ 5，两个窗口[0,5)同时触发
 *   device-2,92.0,2      ← W=7 已越过 end+lateness=8？未越过→allowedLateness 窗口重算！
 *                          （若再先输入一条 ts=15 的数据，W=12>8 → 这条走侧输出"⏰迟到"）
 */
public class IotAlertJob {

    /** 侧输出：彻底迟到的数据（复核队列，绝不静默丢弃） */
    static final OutputTag<DeviceMetric> LATE_TAG = new OutputTag<DeviceMetric>("late"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> main = env
                .socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] p = line.split(",");
                    return new DeviceMetric(p[0].trim(),
                            Double.parseDouble(p[1].trim()),
                            Long.parseLong(p[2].trim()));
                })
                .returns(Types.POJO(DeviceMetric.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DeviceMetric>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((m, recordTs) -> m.getEventTime()))
                .keyBy(DeviceMetric::getDeviceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))      // 第二道防线：窗口关闭后 3 秒内迟到 → 窗口重算
                .sideOutputLateData(LATE_TAG)          // 第三道防线：彻底迟到 → 侧输出兜底
                .process(new AvgAlertFunction());

        // ---- 主流：窗口指标 & 告警 ----
        main.print("📊");

        // ---- 侧输出：迟到数据复核 ----
        DataStream<DeviceMetric> late = main.getSideOutput(LATE_TAG);
        late.print("⏰迟到");

        env.execute("IoT Alert Job");
    }

    /** 窗口函数：均值/最高/最低一起算（教学用全量遍历；大数据量改增量聚合） */
    public static class AvgAlertFunction
            extends ProcessWindowFunction<DeviceMetric, String, String, TimeWindow> {
        @Override
        public void process(String deviceId, Context ctx, Iterable<DeviceMetric> metrics,
                            Collector<String> out) {
            double sum = 0, max = Double.MIN_VALUE, min = Double.MAX_VALUE;
            int n = 0;
            for (DeviceMetric m : metrics) {
                sum += m.getTemperature();
                max = Math.max(max, m.getTemperature());
                min = Math.min(min, m.getTemperature());
                n++;
            }
            double avg = sum / n;
            String base = String.format("设备%s 窗口[%d~%d) 均值=%.1f 最高=%.1f 最低=%.1f",
                    deviceId, ctx.window().getStart() / 1000, ctx.window().getEnd() / 1000,
                    avg, max, min);
            out.collect(avg > 80 ? "🚨高温告警 " + base : "正常 " + base);
        }
    }
}
