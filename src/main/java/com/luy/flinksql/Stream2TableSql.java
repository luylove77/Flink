package com.luy.flinksql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Stream2TableSql {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(1);
        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 可以传入DDL和DML
        tableEnv.executeSql("CREATE TABLE source (\n" +
                " id INT,\n" +
                " ts BIGINT,\n" +
                " vc INT\n" +
                " ) WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '1',\n" +
                "    'fields.id.kind' = 'random',\n" +
                "    'fields.id.min' = '1',\n" +
                "    'fields.id.max' = '10',\n" +
                "    'fields.ts.kind' = 'sequence',\n" +
                "    'fields.ts.start' = '1',\n" +
                "    'fields.ts.end' = '1000000',\n" +
                "    'fields.vc.kind' = 'random',\n" +
                "    'fields.vc.min' = '1',\n" +
                "    'fields.vc.max' = '100'\n" +
                " )");

        tableEnv.executeSql("select * from source").print();

    }
}
