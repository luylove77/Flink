package com.luy.aggreagte;

import com.luy.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // 按照id分组, 重分组, 相同key的数据在同一个分区,比如并行度2, s2在同一个分区，s1和s3在同一个分区
        // 移除悬挂的 Javadoc 注释，使用方法引用替换匿名内部类
        KeyedStream<WaterSensor, String> keyBy = sensorDS.keyBy(WaterSensor::getId);

        /**
         * 简单聚合算子keyBy之后才能调用
         * sum(javabean的字段)，Java bean 是POJO类型就可以这么调用
         * Tuple类型的话可以sum(位置，从0开始)
         */

        SingleOutputStreamOperator<WaterSensor> result = keyBy.sum("vc");

        // max只会取比较字段的最大值，非比较字段保留第一次值
        // maxBy会取比较字段的最大值，同时非比较字段会取最大那条对应的字段值
        //SingleOutputStreamOperator<WaterSensor> result = keyBy.max("vc");
        //SingleOutputStreamOperator<WaterSensor> result = keyBy.maxBy("vc");

        result.print();

        env.execute();

    }
}
