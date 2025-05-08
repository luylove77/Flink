package com.luy.state;


import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 键控状态，聚合状态

 * 需求 计算每种传感器的平均水位
 *
 */

public class KeyedState_AggregatingState {
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
                    AggregatingState<Integer,Double> vcAvgState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double> aggregatingStateDescriptor
                                = new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                "vcAvgState",
                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0,0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer vc, Tuple2<Integer, Integer> accumulator) {
                                        accumulator.f0 += vc;
                                        accumulator.f1 += 1;
                                        return accumulator;
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        Integer vcSum = accumulator.f0;
                                        Integer countSum = accumulator.f1;
                                        return (double) (vcSum/ countSum);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                        return null;
                                    }
                                },
                                Types.TUPLE(Types.INT,Types.INT)
                        );

                        vcAvgState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);

                    }


                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.getVc();
                        // 当前水位值放到状态中
                        vcAvgState.add(vc);
                        // 从状态中获取平均水位
                        Double vcAvg = vcAvgState.get();
                        out.collect("传感器" + ctx.getCurrentKey() + "平均水位是" + vcAvg);
                    }
                }
        );

        processDS.printToErr();

        env.execute();


    }
}
