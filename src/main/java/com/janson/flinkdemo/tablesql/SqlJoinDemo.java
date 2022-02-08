package com.janson.flinkdemo.tablesql;

import com.janson.flinkdemo.datastream.MyStreamingSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description: flink table sql join demo
 * @Author: shanjian
 * @Date: 2022/2/8 2:34 下午
 */
public class SqlJoinDemo {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        SingleOutputStreamOperator<MyStreamingSource.Item> source = bsEnv.addSource(new MyStreamingSource())
                .map(new MapFunction<MyStreamingSource.Item, MyStreamingSource.Item>() {

                    @Override
                    public MyStreamingSource.Item map(MyStreamingSource.Item value) throws Exception {
                        return value;
                    }
                });

        DataStream<MyStreamingSource.Item> evenSelect = source.split(new OutputSelector<MyStreamingSource.Item>() {
            @Override
            public Iterable<String> select(MyStreamingSource.Item value) {
                List<String> outPut = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    outPut.add("even");
                } else {
                    outPut.add("odd");
                }
                return outPut;
            }
        }).select("even");

        DataStream<MyStreamingSource.Item> oddSelect = source.split(new OutputSelector<MyStreamingSource.Item>() {
            @Override
            public Iterable<String> select(MyStreamingSource.Item value) {
                List<String> outPut = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    outPut.add("even");
                } else {
                    outPut.add("odd");
                }
                return outPut;
            }
        }).select("odd");


        bsTableEnv.createTemporaryView("evenTable", evenSelect, "name,id");
        bsTableEnv.createTemporaryView("oddTable", oddSelect, "name,id");
        Table queryTable = bsTableEnv.sqlQuery("select a.id,a.name,b.id,b.name from evenTable as a join oddTable as b on a.name = b.name");
        queryTable.printSchema();
        bsTableEnv.toRetractStream(queryTable, TypeInformation.of(new TypeHint<Tuple4<Integer, String, Integer, String>>() {
        })).printToErr();

        bsEnv.execute("streaming sql job");
    }
}
