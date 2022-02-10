package com.janson.flinkdemo.datastream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * @Description:
 * @Author: shanjian
 * @Date: 2022/2/10 3:52 下午
 */
public class SelfRedisSink extends RichSinkFunction<Tuple2<String,String>> {

    private transient Jedis jedis;
    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("localhost",6379);
    }

    @Override
    public void close() throws Exception {
       jedis.close();
    }

    @Override
    public void invoke(Tuple2<String,String> value, Context context) throws Exception {
        if (!jedis.isConnected()) {
            jedis.connect();
        }
        jedis.set(value.f0,value.f1);
    }
}
