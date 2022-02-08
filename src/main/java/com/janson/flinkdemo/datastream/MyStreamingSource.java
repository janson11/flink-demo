package com.janson.flinkdemo.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Description: 自定义实时数据源
 * @Author: shanjian
 * @Date: 2022/2/8 11:05 上午
 */
public class MyStreamingSource implements SourceFunction<MyStreamingSource.Item> {


    private boolean isRunning = true;


    @Override
    public void run(SourceContext<Item> sourceContext) throws Exception {
        while (isRunning) {
            Item item = generateItem();
            sourceContext.collect(item);
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private Item generateItem() {
        int i = new Random().nextInt(100);
        Item item = new Item();
        item.setName("name" + i);
        item.setId(i);
        return item;
    }


    class Item {
        private String name;
        private Integer id;

        public Item() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }
    }

}


class StreamingDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MyStreamingSource.Item> text = env.addSource(new MyStreamingSource()).setParallelism(1);
//        SingleOutputStreamOperator<Object> item = text.map(new MapFunction<MyStreamingSource.Item, Object>() {
//            @Override
//            public Object map(MyStreamingSource.Item item) throws Exception {
//                return item.getName();
//            }
//        });
//        SingleOutputStreamOperator<MyStreamingSource.Item> item = text.map((MapFunction<MyStreamingSource.Item, MyStreamingSource.Item>) value -> value);
//        SingleOutputStreamOperator<Object> item = text.map(item1 -> item1.getName());
//        SingleOutputStreamOperator<String> item = text.map(new MyMapFunction());
        SingleOutputStreamOperator<MyStreamingSource.Item> item = text.filter((FilterFunction<MyStreamingSource.Item>) value -> value.getId() % 2 == 0);
        item.print().setParallelism(1);
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }

    static class MyMapFunction extends RichMapFunction<MyStreamingSource.Item,String> {

        @Override
        public String map(MyStreamingSource.Item value) throws Exception {
            return value.getName();
        }
    }
}