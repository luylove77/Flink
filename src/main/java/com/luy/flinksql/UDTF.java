package com.luy.flinksql;

import com.luy.flinksql.func.SplitFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

// 一对多, 分割
public class UDTF {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> strDS = env.fromElements(
                "hello world",
                "hello flink",
                "hello java"
        );

        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(strDS, $("words"));

        tableEnv.createTemporaryView("str",table);

        // 注册函数
        tableEnv.createTemporaryFunction("SplitFunction", SplitFunction.class);

        // 调用函数
//        tableEnv.executeSql("select SplitFunction(words) as (word, length) from str").print();
    }
}
