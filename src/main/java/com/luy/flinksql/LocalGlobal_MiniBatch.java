package com.luy.flinksql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LocalGlobal_MiniBatch {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 获取tableEnv的配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();

        // 从命令行获取参数 --local-global true
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        boolean isLocalGlobal = parameterTool.getBoolean("local-global", false);

        if (isLocalGlobal) {
            // 开启MiniBatch
            configuration.setString("table.exec.mini-batch.enabled", "true");

            // 时间和条数只要满足其中一个条件，就会触发批量输出
            // 批量输出的间隔时间
            configuration.setString("table.exec.mini-batch.allow-latency", "5s");
            // 防止OOM，设置每个批次最多缓存数据条数为1000条
            configuration.setString("table.exec.mini-batch.size", "1000");
            // 开启Local Global
            configuration.setString("table.optimizer.agg-phase-strategy", "TWO-PHASE");
        }
    }
}
