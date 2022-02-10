package com.janson.flinkdemo.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Description: 自定义序列化
 * @Author: shanjian
 * @Date: 2022/2/10 1:43 下午
 */
@Slf4j
@PublicEvolving
public class CustomDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {

    public CustomDeserializationSchema() {
    }



    // 是否表示流的最后一条元素，设置为false，表示数据会源源不断地到来
    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> nextElement) {
        return false;
    }

    // 这里返回一个ConsumerRecord<String, String>类型的数据，除了原数据，还是包括topic，offset，partition等信息
    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

        log.warn("topic:{} partition:{} offset:{} key:{} value:{}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
        return new ConsumerRecord<String, String>(
                record.topic(),
                record.partition(),
                record.offset(),
                new String(record.key()),
                new String(record.value())
        );
    }

    //指定数据的输入类型
    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
        });
    }
}
