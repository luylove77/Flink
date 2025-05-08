package com.luy.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL_Env {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(1);
        // 指定表执行环境
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 提交作业
        // 如果是sql不用提交


    }
}
