package com.luy.state;


import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 算子状态
 * 需求：在map算子中每个并行度上计算数据的个数
 * 用普通变量万一机器宕机就恢复不过来，所以要用到算子状态
 */

public class KeyedState_OpeState_Var {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 当并行度设置为1的时候，所有的组都共享一个状态
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop100", 7777)
                .map(new WaterSensorMapFunction());


        // 对分组后的数据进行处理
        SingleOutputStreamOperator<String> mapDS = sensorDS.map(
                new RichMapFunction<WaterSensor, String>() {
                    Integer count = 0;

                    @Override
                    public String map(WaterSensor ws) throws Exception {
                        return "并行子任务:" + getRuntimeContext().getIndexOfThisSubtask() + "处理了" + ++count + "条数据";
                    }
                }
        );

        mapDS.printToErr();

        env.execute();


    }
}
