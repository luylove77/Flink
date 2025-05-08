package com.luy.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据生成器
 * 第一个：需要实现，重写map方法，输入类型固定是LONG
 * 第二个：Long类型，自动生成的数字序列从1开始自增的最大值，达到停止
 * 第三个：限速策略，比如每秒几条数据
 * 第四个：指定数据类型，这里是String
 * 需要引入flink-connector-datagen依赖
 * Long.MAX_VALUE 是达到Long的最大值才会停止，相当于没有停止条件，无界流
 * 如果有n个并行度，那么会把数据均分成n份，每个并行度都有一份数据，每个并行度都有一个自增的数字
 */
public class DataGeneratorDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Replace the deprecated DataGeneratorSource with the new one
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                // Replace anonymous class with lambda
                value -> "Number:" + value,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(2),
                Types.STRING
        );

        // Ensure the type of WatermarkStrategy matches the source type
        env
                .fromSource(dataGeneratorSource, WatermarkStrategy.<String>noWatermarks(), "datagenerator")
                .print();

        env.execute();
    }
}
