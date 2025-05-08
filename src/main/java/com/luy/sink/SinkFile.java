package com.luy.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

public class SinkFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Replace the deprecated DataGeneratorSource with the new one
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                // Replace anonymous class with lambda
                value -> "Number:" + value,
                100,
                RateLimiterStrategy.perSecond(2),
                Types.STRING
        );

        // Ensure the type of WatermarkStrategy matches the source type
        DataStreamSource<String> dataStreamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.<String>noWatermarks(), "datagenerator");

        // TODO 输出到文件系统
        FileSink<String> fileSink = FileSink
                .<String>forRowFormat(new Path("f:/tmp"), new SimpleStringEncoder<>("UTF-8"))
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("luylove77")
                                .withPartSuffix(".log")
                                .build()
                )
                .build();


        dataStreamSource.sinkTo(fileSink);

        env.execute();
    }
}
