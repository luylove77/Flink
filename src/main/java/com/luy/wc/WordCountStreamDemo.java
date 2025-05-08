package com.luy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 匿名实现接口(匿名类)
 * 接口A，里面有个方法a()
 * 正常写法，需要创建一个类B，实现接口A，重写方法a()
 * B b = new B();
 *
 * 匿名实现接口，写法
 * new A(){
 *
 *     @Override
 *     public void a() {
 *
 *     }

 }
 */


// 流处理是来一条输出一条，区别于批处理，批处理是来一批数据，输出一批数据
//输出结果
//4> (flink,1)
// 1> (java,1)
// 2> (hello,1)
// 2> (hello,2)
// 2> (hello,3)
// 3> (word,1)
// 编号是并行度编号，和电脑线程有关


public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        // DataStream API 读取文件，返回的是一个DataStream
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从文件读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");
        // 处理数据,切分，转换(word,1),按照word分组，聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 按照空格切分单词
                String[] words = s.split(" ");
                for (String word : words) {
                    // 转换为(word,1)
                    Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                    // 使用Collector 向下游发送数据
                    collector.collect(wordAndOne);
                }
            }
        });

        // 按照word分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1);

        //输出数据
        sumDS.print();

        //执行
        env.execute();

    }
}
