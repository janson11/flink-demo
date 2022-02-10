package com.janson.flinkdemo.datastream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Description:
 * @Author: shanjian
 * @Date: 2022/2/10 4:53 下午
 */
public class RedisSink01 implements RedisMapper<Tuple2<String, String>> {

    // 设置redis的数据类型
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    // 设置key
    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }
    // 设置value
    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }
}
