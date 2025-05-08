package com.luy.function;

import org.apache.flink.api.common.functions.MapFunction;

// 输入String，输出String
public class MyMapFunction implements MapFunction<Integer, Integer> {
    @Override
    //输入参数 i
    public Integer map(Integer i) throws Exception {
        return i * 2;
    }
}
