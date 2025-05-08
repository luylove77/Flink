package com.luy.watermark;

import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.stream.StreamSupport;

/**
 * 乱序的Watermark
 */

public class WatermarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop100", 7777)
                .map(new WaterSensorMapFunction());


        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 指定Watermark生成, 乱序的, 有等待时间, 等待3s时间
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                        (element, recordTimestamp) -> {
                            System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                            return element.getTimestamp() * 1000L;  //返回的时间戳毫秒
                        });//指定怎么提取事件时间

        // 设置Watermark
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        KeyedStream<WaterSensor, String> sensorKS = sensorDSwithWatermark.keyBy(WaterSensor::getId);

        // 要让Watermark起效果，要启动事件时间语义的窗口
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        sensorWS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        // Get window start and end time
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        // Calculate the number of elements in the window
                        long count = StreamSupport.stream(elements.spliterator(), false).count();
                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据====");
                    }
                }
        ).print();

        env.execute();
    }
}