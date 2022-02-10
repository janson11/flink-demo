package com.janson.flinkdemo.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @Description: kafka consumer
 * @Author: shanjian
 * @Date: 2022/2/10 11:30 上午
 */
public class DynamicKafkaConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);
        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "group_test");
        // 动态获取partition分区的功能
        properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "10");
        Pattern compile = Pattern.compile("^test([A-Za-z0-9]*)$");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                compile,
                new SimpleStringSchema(),
                properties);
        consumer.setStartFromEarliest();
        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("start consumer ...");
    }

}
