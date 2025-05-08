package com.luy.sink;

import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;



/**
 * 将数据写入到MySql
 * 需要引入依赖 flink-connector-jdbc 和 mysql-connector-java
 * 从socket 写入数据， 比如: s1,1,1 写入到mysql 的ws表
 */
public class SinkMySql {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // 必须开启checkpoint, 否则精准一次无法写入Kafka
//        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop100", 7777)
                .map(new WaterSensorMapFunction());


        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into ws values(?,?,?)",
                (JdbcStatementBuilder<WaterSensor>) (preparedStatement, waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTimestamp());
                    preparedStatement.setInt(3, waterSensor.getVc());
                }
                ,
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)  // 最多重试3次
                        .withBatchSize(100) // 一批次100条数据
                        .withBatchIntervalMs(3000) // 3s 执行一次
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop100:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8")
                        .withUsername("root")
                        .withPassword("root")
                        .withConnectionCheckTimeoutSeconds(60) // 检查连接超时时间
                        .build()
        );

        sensorDS.addSink(jdbcSink);

        env.execute();
    }
}
