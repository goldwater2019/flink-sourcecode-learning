package com.lambda.flink.window;

import com.lambda.flink.common.source.CorpusRichSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * @Author: zhangxinsen
 * @Date: 2022/3/1 11:28 PM
 * @Desc:
 * @Version: v1.0
 */

public class TimeWindowWordCount_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.addSource(new CorpusRichSource());

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] wordList = s.toLowerCase(Locale.ROOT).trim().split("\\s+");
                        for (String word : wordList) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                }).keyBy(tuple -> tuple.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1);
        resultDS.print().setParallelism(1);
        executionEnvironment.execute("WordCount With Sliding Processing Time Window");
    }
}
