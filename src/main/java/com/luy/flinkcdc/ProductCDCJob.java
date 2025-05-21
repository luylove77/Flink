package com.luy.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProductCDCJob {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60000);

        // 并行度设置
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 读取MySQL中的数据
        // 'scan.startup.mode' = 'earliest-offset' 表示从最早的偏移量开始读取数据, 默认是初始化，
        // 从上一次未读取处读取
        String cdcSourceDDL = "CREATE TABLE products (" +
                "id INTEGER NOT NULL," +
                "name VARCHAR(255)," +
                "description VARCHAR(512)," +
                "PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = '192.168.10.100'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = 'root'," +
                "'database-name' = 'test'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'table-name' = 'products')";

        tableEnv.executeSql(cdcSourceDDL);

        // 定义Kafka Sink表的DDL
        String kafkaSinkDDL = "CREATE TABLE kafkaSink (" +
                "id INTEGER NOT NULL," +
                "name VARCHAR(255)," +
                "description VARCHAR(512)," +
                "PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'upsert-kafka'," +
                "'topic' = 'product-cdc'," +
                "'key.format' = 'json'," +
                "'value.format' = 'json'," +
                "'properties.bootstrap.servers' = '192.168.10.100:9092')" ;

        tableEnv.executeSql(kafkaSinkDDL);

        // 从源表选择所有数据，准备插入到Sink表
        Table productTable = tableEnv.sqlQuery("select * from products");

        // 插入数据到Kafka Sink表
        productTable.executeInsert("kafkaSink");

    }
}
