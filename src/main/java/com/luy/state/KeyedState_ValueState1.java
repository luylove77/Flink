package com.luy.state;


import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 键控状态，值状态
 * 连续水位值相差10输出报警
 * 不使用状态用map
 */

public class KeyedState_ValueState1 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 当并行度设置为1的时候，所有的组都共享一个状态
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop100", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = sensorDS.keyBy(WaterSensor::getId);

        // 对分组后的数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 如果用普通的变量记录上次的水位值，其作用范围为算子子任务，一个子任务的多个组共享这个变量，有问题的
//                            Integer lastVc = 0;
                            Map<String,Integer> lastVcMap = new HashMap<>();

                            @Override
                            public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                // 当前水位值
                                Integer curVc = ws.getVc();
                                String id = ctx.getCurrentKey();
                                Integer lastVc = lastVcMap.get(id);
                                lastVc = lastVc == null ? 0 : lastVc;
                                if (Math.abs(curVc - lastVc) > 10) {
                                    out.collect("传感器id"+id+ws.getId() +"当前水位值"+curVc+ "和上一次水位值"+ lastVc +"相差超过10");
                                }
                                lastVcMap.put(id,curVc);
                            }

                        }
                );

        processDS.printToErr();

        env.execute();


    }
}
