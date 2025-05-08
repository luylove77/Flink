package com.luy.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//需要引入flink-connector-kafka
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafka读取数据: 新Source架构, builder构造者模式都是set
        // 发送者kafka , 消费者flink
        // OffsetsInitializer.earliest() 一定从最早消费
        // OffsetsInitializer.latest() 一定从最新消费
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop100:9092,hadoop101:9092,hadoop102:9092")
                .setGroupId("luy")
                .setTopics("topic_1")
                .setValueOnlyDeserializer(new SimpleStringSchema()) //指定反序列化器，反序列化Value
                .setStartingOffsets(OffsetsInitializer.latest()) // Flink消费Kafka的策略
                .build();

        // 从kf中读，新source架构
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .print();

        env.execute();
    }
}
