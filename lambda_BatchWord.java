package org.example;

// 原因: Lambda表达式代码在被编译成字节码文件以后，会出现泛型被擦除的情况。也就是Java程序无法知道数据的具体类型是什么
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class lambda_BatchWord {
    public static void main(String[] args) throws Exception {
        // 1 创建运行环境顶级对象
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.1设置运行模式
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2数据输入
        DataStreamSource<String> source = environment.readTextFile("E:\\Java_Projects\\Java_for_Flink\\src\\main\\resources\\user.txt");

        // 3数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source.flatMap((String value, Collector<String> out) -> {
                    String[] s = value.split(" ");
                    for (String s1 : s) {
                        out.collect(s1);
                    }
                })
                .returns(Types.STRING)  // 通过returns限定返回值的数据类型，避免被编译器擦除泛型导致报错
                .map(value -> new Tuple2<>(value, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                // 由于keyBy严格意义上不算是一个算子，所以不需要指定泛型
                .reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1)); // reduce 算子后没有其他算子，所以不会有泛型擦除问题。

        // 数据输出
        result.print();

        // 启动任务
        environment.execute();

    }
}
