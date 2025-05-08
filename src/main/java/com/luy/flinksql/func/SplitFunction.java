package com.luy.flinksql.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class SplitFunction extends TableFunction<Row> {
    // 返回是void, 用collect收集
    public void eval(String str) {
        for (String s : str.split(" ")) {
            collect(Row.of(s, s.length()));
        }
    }
}
