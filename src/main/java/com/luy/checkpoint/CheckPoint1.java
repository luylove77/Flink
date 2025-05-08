package com.luy.checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 在代码中配置， 但是实际生产环境一般是在flink.conf中进行配置的
 * 将flink打包到服务器上
 */

public class CheckPoint1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 每隔5s向source发送barrier
        // 默认参数，检查点是barrier对齐的，对齐有精准一次和至少一次，还有一个AT_LEAST_ONCE,
        // 如果看重时效性AT_LEAST_ONCE，准确性EXACTLY_ONCE， barrier不一定对齐好，因为，因为会影响时效
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置状态后端，默认hashMap
        // 状态存在TM内存， checkpoint 默认存在Jobmanager的堆内存中
        env.setStateBackend(new HashMapStateBackend());

        // 检查点的存储, 默认存在Jobmanager的堆内存中
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // namenode的hdfs,检查点存在Hdfs
        checkpointConfig.setCheckpointStorage("hdfs://hadoop100:8020/ck");
        // 超时时间(checkpointTimeout), 设置为1分钟，checkpoint超过1分钟就报错
        checkpointConfig.setCheckpointTimeout(60000L);

        // 最小间隔时间设置为2s，两个检查点之间最小间隔2s，这里是指上一个检查点的结束时间到下一个检查点的开始时间最小间隔2s
        // 比如说：0s 5s 10s这个是做checkpoint的时间，但是有可能状态比较大，0s做备份的时候到7s才做好，那么5s的那个就不做
        // checkpoint了，但是又不能做完马上继续再做，所以至少要再过2s再做
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);

        // 最大并发检查点数量，默认是一个，同时只能有一个检查点进行备份
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 开启外部持久化存储，当作业取消后，检查点是否要保留，比如说我现在Cancel Job的时候，是否要保留状态，下次启动继续开始恢复
        // 取消后，检查点删除
//        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 取消后，检查点保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 检查点连续失败次数，当达到多少次，作业退出, 默认为0
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        // Flink提供的另外一种容错手段，用env设置
        // 每3s重启一次，总共重启3次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

//        // 非对齐检查点(enableUnalignedCheckpoints), 默认是关闭的
//        checkpointConfig.enableUnalignedCheckpoints();
//
//        // 对齐检查点超时时间
//        // 对齐检查点如果10s都没对齐，那么会打开非对齐检查点
//        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(10));

// 设置并行度
        env.setParallelism(2);

        //禁用算子链
        env.disableOperatorChaining();


        env.socketTextStream("hadoop100", 8888).uid("socket_uid")
                .flatMap(
                        (String lineStr, Collector<Tuple2<String, Long>> out) -> {
                            String[] wordArr = lineStr.split(" ");
                            for (String word : wordArr) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        }).uid("flat_map_uid")
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(0)
                .sum(1).uid("sum_uid")
                .print().uid("print_uid");

        env.execute();
    }
}
