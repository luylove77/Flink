package com.luy.sink;

import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class SinkKafka {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 必须开启checkpoint, 否则精准一次无法写入Kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        // 读取数据 socket
        DataStreamSource<String> sockedDS = env.socketTextStream("hadoop100", 7777);


        // 写到Kafka 所以Flink是生产者
        // 注意, 如果要使用精准一次，写入kafka, 需要满足以下条件
        // 1. 必须开启checkpoint
        // 2. 必须设置事务前缀
        // 3. 必须设置事务的超时时间 < max的15分钟
        // 需要引入依赖 flink-connector-kafka

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop100:9092,hadoop101:9092,hadoop102:9092")
                .setRecordSerializer( // 序列化器
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("ws")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)  // 保证数据不重复
                .setTransactionalIdPrefix("luylove77-") // 设置事务的前缀
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*60*1000+"") // 设置事务的超时时间
                .build();

        sockedDS.sinkTo(kafkaSink);

        env.execute();
    }
}
