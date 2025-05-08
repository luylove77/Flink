package com.luy.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取数据 socket
        DataStreamSource<String> sockedDS = env.socketTextStream("hadoop100", 7777);
        // 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = sockedDS.flatMap(
                        (String value, Collector<Tuple2<String, Integer>> collector) -> {
                            // 按照空格切分单词
                            String[] words = value.split(" ");
                            for (String word : words) {
                                // 转换为(word,1)
                                Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                                // 使用Collector 向下游发送数据
                                collector.collect(wordAndOne);
                            }
                        }
                ).
                returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(
                        (Tuple2<String, Integer> value) -> {
                            // 按照word分组
                            return value.f0;
                        }
                ).sum(
                        1
                );
        // 打印
        sum.print();
        // 执行
        env.execute();

    }
}
