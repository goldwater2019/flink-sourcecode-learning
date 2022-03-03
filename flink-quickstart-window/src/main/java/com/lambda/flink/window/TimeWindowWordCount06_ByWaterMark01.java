package com.lambda.flink.window;

import com.lambda.flink.common.process.SumProcessByProcessTimeFunction;
import com.lambda.flink.common.source.UnOrderedProcessWindowWithEventTimeTestSource;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class TimeWindowWordCount06_ByWaterMark01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.addSource(new UnOrderedProcessWindowWithEventTimeTestSource());
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] split = s.trim().split(",");
                if (split.length != 2) {

                } else {
                    String word = split[0];
                    Long eventTime = Long.valueOf(split[1]);
                    collector.collect(Tuple2.of(word, eventTime));
                }
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.forGenerator(
                        (context) -> new PeriodicWatermarkGenerator())
                        .withTimestampAssigner((ctx) -> new TimeStampExtractor()
                )
        ).keyBy(tuple -> tuple.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new SumProcessByProcessTimeFunction())
                .print()
                .setParallelism(1);
        env.execute("time window word count 06 watermark");
    }

    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>>, Serializable {

        private FastDateFormat fastDateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void onEvent(Tuple2<String, Long> stringLongTuple2, long l, WatermarkOutput watermarkOutput) {
            System.out.println("事件触发时间: " + fastDateFormat.format(System.currentTimeMillis()));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 指定可以延迟5秒
            watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis() - 5000L));
        }
    }

    private static class TimeStampExtractor implements TimestampAssigner<Tuple2<String, Long>> {

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long l) {
            return element.f1;
        }
    }
}
