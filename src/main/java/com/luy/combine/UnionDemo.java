package com.luy.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33);
        DataStreamSource<String> source3 = env.fromElements("aaa", "bbb", "ccc");

        // 合并数据流， 合并的两个流的类型必须一致
//        source1.union(source2).print();

        // 连接数据流， 连接的两个流的类型可以不一致
        ConnectedStreams<Integer, String> connect = source1.connect(source3);

        // 流的转换ConnectedStreams  ---> SingleOutputStreamOperator
        // ConnectedStreams调用了 Map方法又会变成DataStream

        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        map.print();

        env.execute();


    }
}
