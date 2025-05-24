package com.luy.flinkkafkaapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlFromDBZ {
    public static void main(String[] args) {
        // 1. 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE products_dbz(\n" +
                "id int,\n" +
                "name string,\n" +
                "description string,\n" +
                "age int,\n" +
                "op string,\n" +
                "primary key(id) NOT ENFORCED\n" +
                ")\n" +
                "WITH (\n" +
                "\t'connector' = 'upsert-kafka',\n" +
                "\t'properties.bootstrap.servers' = 'hadoop100:9092,hadoop101:9092,hadoop102:9092',\n" +
                "\t'topic' = 'products-res',\n" +
                "\t'key.format' = 'json',\n" +
                "\t'value.format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("desc products_dbz").print();
//        tableEnv.executeSql("select * from products_dbz where op in ('c','u')").print();
        tableEnv.executeSql("select * from products_dbz").print();

    }
}
