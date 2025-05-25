package com.luy.flinkkafkaapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlFromDBZ {
    public static void main(String[] args) {
        // 1. 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // {"age":30,"description":"girl","id":39,"name":"m","op":"u"}
        // {"age":32,"description":"boy","id":40,"name":"n","op":"u"}
        tableEnv.executeSql("CREATE TABLE products_dbz(\n" +
                "id int,\n" +
                "name string,\n" +
                "description string,\n" +
                "age int,\n" +
                "op string,\n" +
                "primary key(id) NOT ENFORCED\n" +
                ")\n" +
                "WITH (\n" +
                "'connector' = 'upsert-kafka',\n" +
                "'properties.bootstrap.servers' = 'hadoop100:9092,hadoop101:9092,hadoop102:9092',\n" +
                "'topic' = 'products-res4',\n" +
                "'key.format' = 'raw',\n" +
                "'value.format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("desc products_dbz").print();
        tableEnv.executeSql("select * from products_dbz where op in ('c','u')").print();
//        tableEnv.executeSql("select * from products_dbz").print();

    }
}
