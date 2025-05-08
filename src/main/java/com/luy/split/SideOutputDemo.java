package com.luy.split;

import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.api.common.typeinfo.Types.POJO;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 读取数据 socket
        DataStreamSource<String> sockedDS = env.socketTextStream("hadoop100", 7777);
        // 转换数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = sockedDS.map(new WaterSensorMapFunction());


        /**
         *  TODO 使用测输出流 实现分流
         *  需求：watersensor的数据， s1和s2的数据分开
         *  process 方法 可以使用 侧输出流
         *
         */

        OutputTag<WaterSensor> s1Tag = new OutputTag<WaterSensor>("s1", POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<WaterSensor>("s2", POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(
                new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    // 主流
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        String id = value.getId();
                        if ("s1".equals(id)) {
                            // 如果是s1, 放到侧输出流s1中
                            /**
                             * 创建OutputTag对象
                             * 第一个参数：标签名
                             * 第二个参数：放入侧输出流中的数据的类型， Typeinformation
                             *
                             */


                            /**
                             * 上下文调用方法 输出到侧输出流
                             * 第一个参数： 侧输出流的标签
                             * 第二个参数： 放入侧输出流中的数据
                             */
                            ctx.output(s1Tag, value);

                        } else if ("s2".equals(id)) {
                            // 如果是s2, 放到侧输出流s2中

                            ctx.output(s2Tag, value);

                        } else {
                            // 非s1 s2 放到主流
                            out.collect(value);
                        }
                    }

                }
        );

        //打印主流
        process.print("主流-非s1 s2");

        //打印侧输出流s1 , 从主流中根据标签获取侧输出流
        process.getSideOutput(s1Tag).print("s1");

        //打印侧输出流s1 , 从主流中根据标签获取侧输出流
        process.getSideOutput(s2Tag).print("s2");


        // 执行
        env.execute();

    }
}
