package com.luy.flinksql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 持续查询
 */
public class Sink {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(1);
        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 创建source表
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

        Table sourceTable = tableEnv.sqlQuery("select * from source where id = 1");
        tableEnv.createTemporaryView("source",sourceTable);

        // 只能查询，不能直接打印
//        Table table = tableEnv.sqlQuery("select * from source where id = 1");

        tableEnv.executeSql("CREATE TABLE sink(\n" +
                "id INT,\n" +
                "ts BIGINT,\n" +
                "vc Int\n" +
                ")\n" +
                "WITH (\n" +
                "'connector' = 'print'\n" +
                ")");
//        sourceTable.executeInsert("sink");
        tableEnv.executeSql("insert into sink select * from source");

        // 将查询结果(动态表)转换为流
//        DataStream<Row> dataStream = tableEnv.toDataStream(sourceTable);
//        dataStream.print();
//        env.execute();

    }
}
