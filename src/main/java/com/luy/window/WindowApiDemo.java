package com.luy.window;

import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowApiDemo {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop100", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //TODO 1 指定窗口的分配器 : 时间or计数 滚动or滑动or会话
        // 1.1 没有keyBy 的窗口， 所有数据进入同一个子任务，并行度只能为1


        // 1.2 有keyBy的窗口，有分区，根据key分为多个流
        // 每个key 开一个窗
        // 时间窗, 窗口为10s, 滚动和滑动
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));// 基于时间, 滚动窗口, 10s一个
        // 增量聚合, 来一条数据 , 计算一条数据, 但是不会马上输出, 窗口触发的时候才会输出
//        sensorWS
//                .reduce()


        // 全窗口函数, 数据来了不计算, 存起来, 一直到窗口触发的时候再计算输出
//        sensorWS.process()



//        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2))) // // 基于时间, 滑动窗口, 10s窗口的长度, 滑动步长2s（两个窗口的间隔）

    }
}
