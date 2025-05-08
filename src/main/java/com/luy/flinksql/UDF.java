package com.luy.flinksql;

import com.luy.bean.WaterSensor;
import com.luy.flinksql.func.HashFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UDF {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s3", 4L, 4)
        );

        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 转换为动态表
        tableEnv.createTemporaryView("sensor",sensorDS);

        // 注册函数
        tableEnv.createTemporaryFunction("HashFunction", HashFunction.class);

        // 调用函数
        tableEnv.executeSql("select *,HashFunction(id) as hash_id from sensor").print();
    }
}

