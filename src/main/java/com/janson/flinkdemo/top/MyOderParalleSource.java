package com.janson.flinkdemo.top;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Description: 使用flink提供的自定义source方法，产生测试数据
 * @Author: shanjian
 * @Date: 2022/2/10 10:29 上午
 */
public class MyOderParalleSource implements SourceFunction<OrderDetail> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<OrderDetail> ctx) throws Exception {
        while (isRunning) {
            OrderDetail orderDetail = new OrderDetail();
            orderDetail.setUserId(new Random().nextLong());
            orderDetail.setItemId(new Random().nextLong());
            orderDetail.setCiteName("深圳" + new Random().nextInt(100));
            orderDetail.setPrice(new Random().nextDouble());
            orderDetail.setTimeStamp(System.currentTimeMillis());
            ctx.collect(orderDetail);
            Thread.sleep(2000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
