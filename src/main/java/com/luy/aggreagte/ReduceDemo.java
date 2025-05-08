package com.luy.aggreagte;

import com.luy.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s1", 21L, 21),
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

        // 因为相同的key已经聚合了
        // 每个组的第一条不会进入reduce 方法会直接输出，但是会存起来(状态存储)
        SingleOutputStreamOperator<WaterSensor> result = keyBy.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("value1 "+value1);
                System.out.println("value2 "+value2);
                return new WaterSensor(value1.id, value2.timestamp, value1.vc + value2.vc);
            }
        });
        result.print();

        env.execute();
    }
}
