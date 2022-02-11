package com.janson.flinkdemo.top;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Description: kafka生产者
 * @Author: shanjian
 * @Date: 2022/2/10 10:26 上午
 */
public class KafkaOderProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);
        DataStreamSource<OrderDetail> text = env.addSource(new MyOderParalleSource()).setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        FlinkKafkaProducer<OrderDetail> producer = new FlinkKafkaProducer<OrderDetail>(
                "127.0.0.1:9092",
                "testOrder",
                new TypeInformationSerializationSchema<>(PojoTypeInfo.of(OrderDetail.class), new ExecutionConfig())
//                new TypeInformationSerializationSchema<OrderDetail>(PojoTypeInfo.of(OrderDetail.class), new ExecutionConfig())
        );
        producer.setWriteTimestampToKafka(true);
        text.addSink(producer);
        env.execute("write data to kafka");
    }

}
