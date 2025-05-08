package com.luy.state;


import com.luy.bean.WaterSensor;
import com.luy.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 算子状态
 * 需求：在map算子中每个并行度上计算数据的个数
 * 用普通变量万一机器宕机就恢复不过来，所以要用到算子状态
 * 需要实现CheckpointedFunction接口
 * 要打开检查点才能备份状态
 */

 class KeyedState_OpeState_ListState {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 当并行度设置为1的时候，所有的组都共享一个状态
        env.setParallelism(2);
        env.enableCheckpointing(10000); // 10s 备份一次状态

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop100", 7777)
                .map(new WaterSensorMapFunction());


        // 对分组后的数据进行处理
        SingleOutputStreamOperator<String> mapDS = sensorDS.map(
               new MyMap()
        );

        mapDS.printToErr();

        env.execute();


    }
}

class MyMap extends RichMapFunction<WaterSensor, String> implements CheckpointedFunction {
    Integer count = 0;
    ListState<Integer> countState;

    @Override
    public String map(WaterSensor ws) throws Exception {
        return "并行子任务:" + getRuntimeContext().getIndexOfThisSubtask() + "处理了" + ++count + "条数据";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("~~~~snapshotState~~~~");
        countState.clear();
        countState.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("~~~~initializeState~~~~");
        ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("countState", Integer.class);
        countState = context.getOperatorStateStore().getListState(listStateDescriptor);

        if(context.isRestored()){
            Integer restorCount = countState.get().iterator().next();
            count = restorCount;
        }
    }

}
