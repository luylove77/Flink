package com.luy.state;

import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 广播状态
 *
 */

public class BroadcastState1 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 当并行度设置为2的时候，所有的组都共享一个状态
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop100", 8888)
                .map(new WaterSensorMapFunction());

        //从指定的网络端口读取阈值信息并进行类型转换，String -> Integer
        SingleOutputStreamOperator<Integer> thresholdDS = env.socketTextStream("hadoop100", 8889)
                .map(Integer::valueOf);

        // 对阈值进行广播
        MapStateDescriptor<String, Integer> mapStateDescriptor
                = new MapStateDescriptor<String, Integer>("mapStateDescriptor",String.class,Integer.class);

        BroadcastStream<Integer> broadcastDS = thresholdDS.broadcast(mapStateDescriptor);

        //非广播流和广播流 connect
        BroadcastConnectedStream<WaterSensor, Integer> connectDS = wsDS.connect(broadcastDS);

        // 对关联后的数据进行处理， process
        SingleOutputStreamOperator<String> processDS = connectDS.process(
                new BroadcastProcessFunction<WaterSensor, Integer, String>() {
                    // 主流数据
                    @Override
                    public void processElement(WaterSensor ws, BroadcastProcessFunction<WaterSensor, Integer, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // 获取广播状态 阈值
                        ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        Integer threshold = broadcastState.get("threshold") == null?0:broadcastState.get("threshold");



                        Integer vc = ws.getVc();
                        if(vc > threshold){
                            out.collect( "当前水位" + vc + "超过阈值" + threshold);
                        }

                    }

                    // 广播流数据
                    @Override
                    public void processBroadcastElement(Integer threshold, BroadcastProcessFunction<WaterSensor, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取广播状态
                        BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        broadcastState.put("threshold",threshold);
                    }
                }
        );

        processDS.print();

        env.execute();

    }
}
