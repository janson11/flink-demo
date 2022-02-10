package com.janson.flinkdemo.datastream;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;

/**
 * @Description:
 * @Author: shanjian
 * @Date: 2022/2/10 3:05 下午
 */
public class CounterTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 9000, "\n");
        dataStream.map(new RichMapFunction<String, String>() {

            // 定义累加器
            private IntCounter intCounter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("counter", this.intCounter);
            }


            @Override
            public String map(String value) throws Exception {
                // 累加
                this.intCounter.add(1);
                return value;
            }
        });

        dataStream.printToErr();
        JobExecutionResult result = env.execute("counter");
        Object accResult = result.getAccumulatorResult("counter");
        System.out.println("累加器计算结果：" + accResult);

    }
}
