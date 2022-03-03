package com.lambda.flink.window;

import com.lambda.flink.common.process.SumProcessByProcessTimeFunction;
import com.lambda.flink.common.source.UnOrderedProcessWindowWithEventTimeTestSource;
import com.lambda.flink.common.timestamp.TimeStampExtractor;
import com.lambda.flink.common.watermark.PeriodicWatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Locale;

public class TimeWindowWordCount05_ByEventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.addSource(new UnOrderedProcessWindowWithEventTimeTestSource());
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] wordList = s.toLowerCase(Locale.ROOT).trim().split(",");
                        String word = wordList[0];
                        long eventTime = Long.valueOf(wordList[1]);
                        collector.collect(Tuple2.of(word, eventTime));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                        .withTimestampAssigner((context) -> new TimeStampExtractor()))
                .keyBy(tuple -> tuple.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new SumProcessByProcessTimeFunction())
                .print()
                .setParallelism(1);
        env.execute("time window word count 06: by event time");
    }
}
