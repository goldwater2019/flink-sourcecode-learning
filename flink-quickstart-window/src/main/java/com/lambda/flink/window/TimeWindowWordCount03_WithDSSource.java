package com.lambda.flink.window;

import com.lambda.flink.common.process.SumProcessFunction;
import com.lambda.flink.common.source.OrderedProcessWindowTestSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * @Author: zhangxinsen
 * @Date: 2022/3/2 12:33 AM
 * @Desc:
 * @Version: v1.0
 */

public class TimeWindowWordCount03_WithDSSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.addSource(new OrderedProcessWindowTestSource());
        stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] wordList = s.trim().toLowerCase(Locale.ROOT).split("\\s+");
                for (String word : wordList) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(tuple -> tuple.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new SumProcessFunction())
                .print();

        env.execute("window word count and time");
    }
}
