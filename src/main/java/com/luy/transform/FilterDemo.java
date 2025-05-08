package com.luy.transform;

// Removed unused import
// import com.luy.function.MyMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromElements(1,2,3,4,5,6)
                .filter(value -> value % 2 == 0)
                .print();

        env.execute();
    }
}
