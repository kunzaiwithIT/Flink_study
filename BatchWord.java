package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class BatchWord {
    public static void main(String[] args) throws Exception {
        //-1创建顶级对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置运行时模式，这里是批处理
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //-2数据输入
        DataStreamSource<String> source = env.readTextFile("E:\\Java_Projects\\Java_for_Flink\\src\\main\\resources\\user.txt");

        //-3数据处理
        SingleOutputStreamOperator<String> flatMapData = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(s1);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapData = flatMapData.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2 map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> KeyByData = mapData.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceData = KeyByData.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0,value1.f1+value2.f1);
            }
        });

        // 数据输出
//        flatMapData.print();
//        source.print();
//        mapData.print();
//        KeyByData.print();
        reduceData.print();

        // 启动流式任务或批处理任务
        env.execute();
    }
}
