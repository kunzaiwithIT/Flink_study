package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.ExecutionException;

public class Stream_Table_API {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment Table_env = StreamTableEnvironment.create(environment);
        // 数据输入
        Table_env.createTemporaryTable("source", TableDescriptor
                .forConnector("datagen")
                .schema(Schema.newBuilder().column("word", DataTypes.STRING()).build())
                .option("rows-per-second", "3").option("fields.word.kind", "random")
                .option("fields.word.length", "1")
                .build());

        // 数据输出
        /*
        * 为什么不能用int，一定要用bigint
        * 答：可以，但是flink在计算时产生的整数默认是BIGINT，需要手动cast转换。
        */
        Table_env.createTemporaryTable("sink", TableDescriptor.forConnector("print")
                .schema(Schema.newBuilder().column("word", DataTypes.STRING()).column("cnt", DataTypes.INT()).build())
                .build());

        // 数据处理
        Table_env.from("source")
                .groupBy(Expressions.$("word"))
                .select(Expressions.$("word")
                        ,Expressions.lit("1").count().cast(DataTypes.INT()))
                .executeInsert("sink")
                .await();
        environment.execute();
    }
}
