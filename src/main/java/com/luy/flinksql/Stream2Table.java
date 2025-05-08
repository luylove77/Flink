package com.luy.flinksql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Stream2Table {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(1);
        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 流
        DataStreamSource<Integer> numDS = env
                .fromElements(1, 2, 3,4);

        // 流转表 api的方式
        tableEnv.createTemporaryView("t_num",numDS);
        tableEnv.executeSql("select * from t_num").print();

        // 提交作业
        // 如果是sql不用提交


    }
}
