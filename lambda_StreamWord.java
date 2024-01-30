package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
//import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class lambda_StreamWord {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        DataStreamSource<String> source = environment.socketTextStream("node1", 4567);
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = source.flatMap((String value, Collector<String> out) -> {
                    String[] s = value.split(" ");
                    for (String s1 : s) {
                        out.collect(s1);
                    }
                })
                .returns(Types.STRING)
                .map(value -> new Tuple2<>(value, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1));

        reduce.print();

        environment.execute();

    }
}
