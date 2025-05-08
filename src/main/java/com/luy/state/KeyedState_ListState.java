package com.luy.state;


import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 键控状态，列表状态
 * 需求：对于传感器，输出最高的3个水位值
 *
 */

public class KeyedState_ListState {
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
                    // 声明键控状态
                    ListState<Integer> vcListState ;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // "vcListState" 状态名称
                        ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("vcListState",Integer.class);
                        vcListState = getRuntimeContext().getListState(listStateDescriptor);

                    }


                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取当前水位值
                        Integer curVc = ws.getVc();
                        // 把当前水位值添加到状态中
                        vcListState.add(curVc);
                        // 对状态中的数据进行排序
                        List <Integer> vcList = new ArrayList<>();
                        for (Integer vc : vcListState.get()) {
                            vcList.add(vc);
                        }
                        vcList.sort((v1,v2) -> v2-v1);
                        // 判断集合中元素的个数 是否大于3 ， 如果大于3，就删除最后一个， 相当于只存3个最大的
                        // 进来一个会重新排序，然后删除掉排在最后的一个
                        if(vcList.size() > 3){
                            vcList.remove(3);
                        }
                        out.collect("传感器id为" + ws.getId() + ",最大的3个水位值=" + vcList.toString());

                        // 更新状态
                        vcListState.update(vcList);
                    }
                }
        );

        processDS.printToErr();

        env.execute();


    }
}
