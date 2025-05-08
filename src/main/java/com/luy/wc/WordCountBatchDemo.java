package com.luy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2 读取数据，从文件中读取
        // DataSet API 读取文件，返回的是一个DataSet, 有界流
        DataSource<String> lineDs = env.readTextFile("input/word.txt");

        // 3 切分，转换(word,1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 3.1 按照空格切分单词,
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    // 使用Collector 向下游发送数据
                    collector.collect(wordTuple2);
                }
            }
        });


        // 4 按照word分组,对第一个元素
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupby = wordAndOne.groupBy(0);

        // 5 各分组内聚合,1代表第二个元素
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupby.sum(1);

        // 6 输出
        sum.print();
    }
}
