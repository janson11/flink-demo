package com.janson.flinkdemo.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Description:
 * @Author: shanjian
 * @Date: 2022/2/7 4:35 下午
 */
public class StreamingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");
        DataStream<WorldWithCount> windowsCounts = text.flatMap(new FlatMapFunction<String, WorldWithCount>() {
            @Override
            public void flatMap(String value, Collector<WorldWithCount> out) throws Exception {
                for (String word : value.split("\\s")) {
                    out.collect(new WorldWithCount(word, 1L));
                }
            }
        })
                .keyBy("word")
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<WorldWithCount>() {
                    @Override
                    public WorldWithCount reduce(WorldWithCount value1, WorldWithCount value2) throws Exception {
                        return new WorldWithCount(value1.word, value1.count + value2.count);
                    }
                });

        windowsCounts.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }

    public static class WorldWithCount {
        public String word;
        public long count;

        public WorldWithCount() {

        }

        public WorldWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
