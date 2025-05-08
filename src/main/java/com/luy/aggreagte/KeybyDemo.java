package com.luy.aggreagte;

import com.luy.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeybyDemo {
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
        /**
         * 1> WaterSensor{id='s2', timestamp=2, vc=2}
         * 2> WaterSensor{id='s1', timestamp=1, vc=1}
         * 2> WaterSensor{id='s1', timestamp=11, vc=11}
         * 2> WaterSensor{id='s3', timestamp=3, vc=3}
         */
        KeyedStream<WaterSensor, String> keyBy = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        keyBy.print();


        env.execute();
    }
}
