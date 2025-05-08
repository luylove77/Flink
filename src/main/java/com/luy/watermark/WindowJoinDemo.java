package com.luy.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 1),
                        Tuple2.of("c", 1)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (value, ts) -> value.f1 * 1000L
                                )
                );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env
                .fromElements(
                        Tuple3.of("a", 1, 1),
                        Tuple3.of("a", 11, 1),
                        Tuple3.of("b", 2, 1),
                        Tuple3.of("b", 12, 1),
                        Tuple3.of("c", 14, 1),
                        Tuple3.of("d", 15, 1)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (value, ts) -> value.f1 * 1000L
                                )
                );

        // window join
        DataStream<String> joinResult = ds1.join(ds2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) // 窗口大小为5秒
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    // first ds1 second ds2, 关联上的数据调用join方法
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                        return first + " <----------> " + second;
                    }
                });

        joinResult.print();

        env.execute();
    }
}
