package com.janson.flinkdemo.tablesql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Author: shanjian
 * @Date: 2022/2/7 6:01 下午
 */
public class WordCountSQL {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);
        String words = "hello flink hello xiaoxue";
        String[] split = words.split("\\W+");
        List<WC> list = new ArrayList<>();
        for (String word : split) {
            WC wc = new WC(word,1);
            list.add(wc);
        }

        DataSet<WC> input = fbEnv.fromCollection(list);
        // DataSet 转sql，指定字段名
        Table table = fbTableEnv.fromDataSet(input,"word,frequency");
        table.printSchema();

        // 注册一个表
        fbTableEnv.createTemporaryView("WordCount",table);
        Table table1 = fbTableEnv.sqlQuery("select word as word,sum(frequency) as frequency from WordCount GROUP BY word");
        DataSet<WC> wcDataSet = fbTableEnv.toDataSet(table1, WC.class);
        wcDataSet.printToErr();
    }

    public static class WC {
        public String word;
        public long frequency;

        public WC() {
        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return word + ", " + frequency;
        }
    }
}
