package com.luy.state;


import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 键控状态，值状态
 * 该案例演示了状态的保留时间的设置
 * 水位值差超过10报警
 * 调用方法 enableTimeToLive
 *
 */

public class KeyedState_TTL {
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
                    // 声明值状态, 就是值状态, 作用范围是keyBy后的每个组，可以不用map了
                    ValueState<Integer> lastVcState;

                    // 在使用之前进行赋值，富函数的open方法中进行初始化
                    // 不能在声明的时候进行初始化，因为算子声明周期还没开始，是获取不到运行时上下文的，所以要在富函数中初始化
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("lastVcState", Integer.class);
                        // 设置状态的保留时间, 10s
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.seconds(10)).build()
                        );
                        lastVcState = getRuntimeContext().getState(valueStateDescriptor);
                    }


                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 当前水位值
                        Integer curVc = ws.getVc();
                        String id = ctx.getCurrentKey();
                        // 从状态中取出上次的水位值
                        Integer lastVc = lastVcState.value() == null ? 0 : lastVcState.value();

                        if (Math.abs(curVc - lastVc) > 10) {
                            out.collect("传感器id" + id + "当前水位值" + curVc + "和上一次水位值" + lastVc + "相差超过10");
                        }
                        // 当前水位更新
                        lastVcState.update(curVc);
                    }
                }
        );

        processDS.printToErr();

        env.execute();


    }
}
