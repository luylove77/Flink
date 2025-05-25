package com.luy.flinkkafkaapi;

import com.alibaba.fastjson.JSON;
import com.luy.bean.ProductsDbz;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.ProducerConfig;


/**
 * debezium 采集mysql的数据写入kafka主题， flink读取kafka主题的数据,
 * 然后再写代码去解析topic中的数据，解析为json格式，因为debezium采集的是crud这种
 * 格式的json数据，所以我们需要解析为json格式，然后再进行后续的处理
 * mao
 */

public class DebeziumToJson {
    public static void main(String[] args) throws Exception {
        // 基本环境准备
        // 1. 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 设置并行度, kafka主题的分区数是4个, 所以设置并行度为4
        env.setParallelism(4);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        env.setStateBackend(new HashMapStateBackend());

        // 设置反序列化

        // 从kafka中读取数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop100:9092,hadoop101:9092,hadoop102:9092")
                .setTopics("bigdata.test.products")
                .setGroupId("test")
                .setValueOnlyDeserializer(new SimpleStringSchema()) //指定反序列化器，反序列化Value
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        // 转换为datastream
        // {"before":null,"after":{"id":20,"name":"baba","description":"boy","age":50},"source":{"version":"1.7.1.Final","connector":"mysql","name":"bigdata","ts_ms":1748059791000,"snapshot":"false","db":"test","sequence":null,"table":"products","server_id":1,"gtid":null,"file":"mysql-bin.000089","pos":630,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1748059791400,"transaction":null}
        DataStreamSource<String> kfDbzDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "products_dbzium");

//        kfDbzDS.print();
        // 过滤kafka中的null消息
        // 加了value == "null"，debezium监控mysql delete null的消息过滤了, 下游ods也不会重复消费
        SingleOutputStreamOperator<String> filteredDS = kfDbzDS.filter(
                new FilterFunction<String>() {

                    @Override
                    public boolean filter(String value) throws Exception {
                        if (value == null || value.trim().isEmpty() || value == "null") {
                            return false;
                        }
                        return true;
                    }
                }
        );


        SingleOutputStreamOperator<ProductsDbz> mapDbz
                = filteredDS.map(new MapFunction<String, ProductsDbz>() {
                    @Override
                    public ProductsDbz map(String value) throws Exception {
                        // 将jsonstr->jsonobj , 为了操作方便
                        JSONObject jsonObj = JSON.parseObject(value);
                        String op = jsonObj.getString("op");
                        // 创建对象，一个对象代表一条数据
                        ProductsDbz productsDbz = null;
                        // 如果删除了，就拿before的数据, 会自动把对应的属性赋值
                        if ("d".equals(op)) {
                            productsDbz = jsonObj.getObject("before", ProductsDbz.class);
                        } else {
                            productsDbz = jsonObj.getObject("after", ProductsDbz.class);
                        }

                        if (productsDbz != null) {
                            productsDbz.setOp(op);
                        }

                        return productsDbz;
                    }
                }
        );

        SingleOutputStreamOperator<String> process = mapDbz.process(new ProcessFunction<ProductsDbz, String>() {
            @Override
            public void processElement(ProductsDbz value, ProcessFunction<ProductsDbz, String>.Context ctx, Collector<String> out) throws Exception {
                if (value != null) {
                // 将对象转换为jsonstr
                String jsonStr = JSON.toJSONString(value);
                out.collect(jsonStr);
                }
            }
        });

        // insert
        // mysql> insert into products (name,description,age) values ('m','girl',20);
        // {"age":20,"description":"girl","id":39,"name":"m","op":"c"}

        /**
         * insert
         * mysql> insert into products (name,description,age) values ('m','girl',20);
         * {"before":null,"after":{"id":39,"name":"m","description":"girl","age":20},"source":{"version":"1.7.1.Final","connector":"mysql","name":"bigdata","ts_ms":1748085617000,"snapshot":"false","db":"test","sequence":null,"table":"products","server_id":1,"gtid":null,"file":"mysql-bin.000089","pos":6265,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1748085620726,"transaction":null}
         * {"age":20,"description":"girl","id":39,"name":"m","op":"c"}
         *
         * update
         * mysql> update products set age = 30 where name = 'm'
         * {"before":{"id":39,"name":"m","description":"girl","age":20},"after":{"id":39,"name":"m","description":"girl","age":30},"source":{"version":"1.7.1.Final","connector":"mysql","name":"bigdata","ts_ms":1748086069000,"snapshot":"false","db":"test","sequence":null,"table":"products","server_id":1,"gtid":null,"file":"mysql-bin.000089","pos":6826,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1748086076527,"transaction":null}
         * {"age":30,"description":"girl","id":39,"name":"m","op":"u"}
         *
         * delete
         * mysql> delete from products where name = 'm';
         * {"before":{"id":39,"name":"m","description":"girl","age":30},"after":null,"source":{"version":"1.7.1.Final","connector":"mysql","name":"bigdata","ts_ms":1748086168000,"snapshot":"false","db":"test","sequence":null,"table":"products","server_id":1,"gtid":null,"file":"mysql-bin.000089","pos":7122,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1748086168604,"transaction":null}
         * null
         * {"age":30,"description":"girl","id":39,"name":"m","op":"d"}
         */


        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop100:9092,hadoop101:9092,hadoop102:9092")
                .setRecordSerializer( // 序列化器
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("products-res4")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        process.sinkTo(kafkaSink);

        env.execute();


    }
}
