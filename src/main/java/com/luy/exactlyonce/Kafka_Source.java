package com.luy.exactlyonce;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * 该案例演示了从kafka中读取数据
 */


public class Kafka_Source {
    public static void main(String[] args) throws Exception {
        // 基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从kafka主题中读取数据, 充当消费者的角色, 从first主题中消费数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop100:9092")
                .setTopics("first")
                .setGroupId("test") // 消费者组
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 设置反序列化类型为String
                .setStartingOffsets(OffsetsInitializer.latest()) // 从最新的地方开始消费
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed") // 读已提交，默认是读未提交，那么两阶段提交的预提交就没有意义，消费者隔离级别
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 打印输出
        kafkaStrDS.print();

        env.execute();


    }
}
