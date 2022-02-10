package com.janson.flinkdemo.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;


/**
 * @Description: redis sink job
 * @Author: shanjian
 * @Date: 2022/2/10 4:56 下午
 */
public class RedisSinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, String>> stream = env.fromElements("Flink", "Spark", "Storm")
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        return new Tuple2<>(value, value + "_sink2");
                    }
                });
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
        stream.addSink(new RedisSink<>(conf, new RedisSink01()));
        env.execute("redis sink");
    }
}
