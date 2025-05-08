package com.luy.transform;

// Removed unused import
// import com.luy.function.MyMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// flatMap 可以将一个元素，拆分成多个元素
// 可以一进一出，也可以一进多出
// map是一进一出，因为是return，所以只能返回一个元素
// flatMap返回的是采集器Collector，所以可以返回多个元素
public class FlatmapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromElements(1,2,3,4,5,6)
                .flatMap(new FlatMapFunction<Integer, Object>() {
                    @Override
                    public void flatMap(Integer value, Collector<Object> out) throws Exception {
                        if (value % 2 == 0) {
                            out.collect("偶数"+value);
                        }else {
                            out.collect("奇数"+value);
                        }
                    }
                })
                .print();

        env.execute();
    }
}
