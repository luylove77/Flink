package com.luy.partition;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 读取数据 socket
        DataStreamSource<String> sockedDS = env.socketTextStream("hadoop100", 7777);
        // 随机分区, random.nextInt(下游算子并行度)
        sockedDS.shuffle().print();



        //sockedDS.rebalance() 轮询分区，这次1分区 下次2分区;
        //好处在于source 如果分区不均匀的话，可以通过rebalance将数据比较均匀 比如kafka3个分区，source也3个分区，避免读取数据源倾斜
        // nextChannelToSendTo = (nextChannelToSendTo + 1) % 下游算子并行度
        // 执行
        env.execute();

    }
}
