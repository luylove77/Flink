package com.luy.transform;

import com.luy.function.MyMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromElements(1,2,3,4,5,6)
                .map(new MyMapFunction())
//                .map(value -> value * 2)
                .print();

        env.execute();
    }


}
