package com.luy.state;


import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * 键控状态，值状态
 * 统计水位值出现次数
 * 使用状态, 键控状态-Map状态
 * 注意点：不需要连网也能跑，但是trae要网
 *
 */

public class KeyedState_MapState {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 当并行度设置为1的时候，所有的组都共享一个状态
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop100", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = sensorDS.keyBy(WaterSensor::getId);

        // 对分组后的数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    MapState<Integer,Integer> vcCountMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<Integer, Integer> stateDescriptor = new MapStateDescriptor<>("vcCountMapState", Integer.class, Integer.class);
                        vcCountMapState = getRuntimeContext().getMapState(stateDescriptor);
                    }


                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.getVc();

                        // 判断状态中是否存在当前的水位
                        if (vcCountMapState.contains(vc)){
                            vcCountMapState.put(vc,vcCountMapState.get(vc)+1);
                        }else{
                            vcCountMapState.put(vc,1);
                        }

                        StringBuilder outStr = new StringBuilder();
                        outStr.append("=============================\n");
                        outStr.append("传感器id为" + ws.getId()+ "\n");
                        for (Map.Entry<Integer,Integer> vcCount : vcCountMapState.entries()) {
                            outStr.append(vcCount.toString() + "\n");
                        }
                        outStr.append("=============================\n");
                        out.collect(outStr.toString());
                    }
                }
        );

        processDS.printToErr();

        env.execute();


    }
}
