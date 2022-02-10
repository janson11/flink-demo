package com.janson.flinkdemo.datastream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Description: 使用flink提供的自定义source方法，产生测试数据
 * @Author: shanjian
 * @Date: 2022/2/10 10:29 上午
 */
public class MyNoParalleSource implements SourceFunction<String> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        List<String> books = new ArrayList<>();
        while (isRunning) {
            books.add("Pyhton从入门到放弃");//10
            books.add("Java从入门到放弃");//8
            books.add("Php从入门到放弃");//5
            books.add("C++从入门到放弃");//3FlinkKafkaProducer011
            books.add("Scala从入门到放弃");
            int i = new Random().nextInt(5);
            String s = books.get(i);
            System.out.println(s + " book size:" + books.size());
            ctx.collect(s);
            Thread.sleep(2000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
