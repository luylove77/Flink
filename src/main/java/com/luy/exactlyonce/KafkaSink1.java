package com.luy.exactlyonce;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * TODO : 需要启动kafka服务， 启动8888端口 ， 相当于生产者
 *
 *
 *
 * 该案例展示了数据写入到kafka数据一致性的事务
 * 在sink端要写保证写入到kafka exactly once， 两阶段提交
 * 开启检查点 env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE)
 * 设置一致性级别为EXACTLY_ONCE开启kafka事务  .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 * 设置事务id的前缀  .setTransactionalIdPrefix("flink_kafka_sink_")
 * 在消费端设置消费数据的隔离级别为读已提交
 */

public class KafkaSink1 {
    public static void main(String[] args) throws Exception {
        // 基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //开启检查点, EXACTLY_ONCE  设置barrier对齐精准一次
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点失效时间为1分钟，如果一分钟没做完算失败
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop100", 8888);

        // 将流数据写入kafka, kafka sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop100:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("first")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)   // 这里设置EXACTLY_ONCE， 底层kafka开启事务
                .setTransactionalIdPrefix("flink_kafka_sink_") // 设置事务id前缀
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15*60*1000 + "") // 配置kafka的事务超时时间, 要比检查点超时时间大, 默认15分钟

                .build();

        socketDS.sinkTo(kafkaSink);

        // 提交作业
        env.execute();
    }
}
