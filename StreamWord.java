package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWord {
    public static void main(String[] args) throws Exception {
        //-1 创建顶级对象
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //-1.1 设置运行模式
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //-2 数据输入（可以有多种方式，这里演示固定端口号获取数据）
        DataStreamSource<String> source = environment.socketTextStream("node1", 4567);

        //-3 数据处理
        SingleOutputStreamOperator<String> flatMap_data = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(s1);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map_data = flatMap_data.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyBy_data = map_data.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce_data = keyBy_data.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });

        //-4 数据输出
        reduce_data.print();

        //-5 执行
        environment.execute();
    }

}
