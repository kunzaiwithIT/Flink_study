package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Stream_SQL_API {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment);
        // 数据输入
        // 创建表
        streamTableEnvironment.executeSql("create table source (" +
                "word varchar" +
                ") " +
                "with ('connector'='datagen'," +
                "'rows-per-second'='10'," +
                "'fields.word.kind'='random'," +
                "'fields.word.length'='1'" +
                ")"
        );

        // 数据输出
        streamTableEnvironment.executeSql("create table sink(" +
            "word varchar, " +
            "cnt bigint" +
            ") " +
            "with ('connector'='print')"
        );
        // 数据处理
        streamTableEnvironment.executeSql("insert into sink select word,count(1) as cnt from source group by word").await();

        // 启动流式任务
        environment.execute();
    }
}
