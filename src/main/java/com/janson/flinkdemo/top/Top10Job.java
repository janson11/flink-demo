package com.janson.flinkdemo.top;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * @Description: Top 10 job
 * @Author: shanjian
 * @Date: 2022/2/11 3:08 下午
 */
public class Top10Job {
    public static void main(String[] args) {
        // 0 设置执行环境并且使用事件时间
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        // 1 kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("testOrder", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);

        // 2 时间提取和水印设置
        DataStream<OrderDetail> orderStream = stream.map(message -> JSON.parseObject(message, OrderDetail.class));
        orderStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<OrderDetail>() {
            private Long currentTimeStamp = 0L;
            // 设置允许乱序时间
            private Long maxOutOfOrderNess = 5000L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimeStamp - maxOutOfOrderNess);
            }

            @Override
            public long extractTimestamp(OrderDetail element, long previousElementTimestamp) {
                return element.getTimeStamp();
            }
        });

        // 3 下单金额TopN
        DataStream<OrderDetail> reduce = orderStream.keyBy(value -> value.getUserId())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20)))
                .reduce(new ReduceFunction<OrderDetail>() {
                    @Override
                    public OrderDetail reduce(OrderDetail value1, OrderDetail value2) throws Exception {
                        return new OrderDetail(value1.getUserId(), value1.getItemId(), value1.getCiteName(), value1.getPrice() + value2.getPrice(), value1.getTimeStamp());
                    }
                });


        // 每20s计算一次
        SingleOutputStreamOperator<Tuple2<Double, OrderDetail>> process = reduce.windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
                .process(new ProcessAllWindowFunction<OrderDetail, Tuple2<Double, OrderDetail>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<OrderDetail> elements, Collector<Tuple2<Double, OrderDetail>> out) throws Exception {
                        TreeMap<Double, OrderDetail> treeMap = new TreeMap<Double, OrderDetail>(new Comparator<Double>() {
                            @Override
                            public int compare(Double o1, Double o2) {
                                return (o1 < o2) ? -1 : 1;
                            }
                        });

                        Iterator<OrderDetail> iterator = elements.iterator();
                        if (iterator.hasNext()) {
                            treeMap.put(iterator.next().getPrice(), iterator.next());
                            if (treeMap.size() > 10) {
                                treeMap.pollLastEntry();
                            }
                        }

                        for (Map.Entry<Double, OrderDetail> entry : treeMap.entrySet()) {
                            out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                        }
                    }
                });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();
        process.addSink(new RedisSink<>(config, new RedisMapper<Tuple2<Double, OrderDetail>>() {
            private final String TOPN_PREFIX = "TOPN:";

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, TOPN_PREFIX);
            }

            @Override
            public String getKeyFromData(Tuple2<Double, OrderDetail> data) {
                return String.valueOf(data.f0);
            }

            @Override
            public String getValueFromData(Tuple2<Double, OrderDetail> data) {
                return String.valueOf(data.f1.toString());
            }
        }));

    }
}
