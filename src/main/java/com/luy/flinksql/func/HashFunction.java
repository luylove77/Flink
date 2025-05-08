package com.luy.flinksql.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

// 自定义函数的实现类
public class HashFunction extends ScalarFunction {
    // 接受任意类型的输入，返回INT类型输出

    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return o.hashCode();
    }


}
