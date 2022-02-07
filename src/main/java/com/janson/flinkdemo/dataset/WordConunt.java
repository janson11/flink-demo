package com.janson.flinkdemo.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Description: 单词计数
 * @Author: shanjian
 * @Date: 2022/2/7 3:54 下午
 */
public class WordConunt {

    public static void main(String[] args) throws Exception {
        // 创建Flink运行的上下文环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 创建DataSet
        DataSet<String> text = env.fromElements(
                "Flink Spark Storm",
                "Flink Flink Flink",
                "Spark Spark Spark",
                "Storm,Storm,Storm");


        // 通过Flink内置的转换函数进行计算
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        counts.printToErr();
    }


    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}


