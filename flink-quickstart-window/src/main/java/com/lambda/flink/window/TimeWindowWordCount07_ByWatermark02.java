package com.lambda.flink.window;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;


/**
 * 3s一个窗口，把相同Key的数据合并在一起
 * ---
 * flink,1461756862000
 * flink,1461756866000
 * flink,1461756872000
 * flink,1461756873000
 * flink,1461756874000
 * flink,1461756876000
 * flink,1461756877000
 * ...
 * ----
 * window + watermark 观察窗口是如何被触发的
 * 可以解决乱序问题
 */
public class TimeWindowWordCount07_ByWatermark02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 6789);

        dataStreamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] fields = s.split(",");
                        return Tuple2.of(fields[0], Long.valueOf(fields[1]));
                    }
                })
                // 获得数据里面的eventTime
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forGenerator(context -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner(context -> new TimeStampExtractor())
                )
                .keyBy(tuple -> tuple.f0)
                // 指定了时间类型
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                // .timeWindow(Time.seconds(3))
                .process(new SumProcessWindowFunction())
                .print()
                .setParallelism(1);

        // 执行
        env.execute("WindowWordCountByWaterMark02");
    }

    /**
     * 自定义Watermark生成器
     */
    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>>, Serializable {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        // 当前窗口里面的最大的时间
        private long currentMaxEventTime = 0;
        // 最大允许的乱序时间是10s
        private final long maxOutOfOrderness = 10000;


        /**
         * 该方法每条数据输入都会调用
         *
         * @param event
         * @param eventTimeStamp
         * @param watermarkOutput
         */
        @Override
        public void onEvent(Tuple2<String, Long> event, long eventTimeStamp, WatermarkOutput watermarkOutput) {
            // 更新记录窗口中最大的eventTime
            Long currentElementEventTime = event.f1;
            currentMaxEventTime = Math.max(currentElementEventTime, currentMaxEventTime);

            System.out.println("event = " + event
                    // Event Time
                    + " | " + dateFormat.format(event.f1)
                    // MaxEventTime
                    + " | " + dateFormat.format(currentMaxEventTime)
                    // Current watermark
                    + " | " + dateFormat.format(currentElementEventTime - maxOutOfOrderness)
                    + " | event time | max event time | current watermark"
            );
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            watermarkOutput.emitWatermark(new Watermark(currentMaxEventTime - maxOutOfOrderness));
        }
    }

    /**
     * 指定eventTime字段
     */
    private static class TimeStampExtractor implements TimestampAssigner<Tuple2<String, Long>> {

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            return element.f1;
        }
    }

    /**
     * IN, OUT, KEY, W
     * IN: 输入的数据类型
     * OUT: 输出的数据类型
     * KEY: Key的数据类型（在Flink里面， String用Tuple表示）
     * W: Window的数据类型
     */
    private static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        /**
         * @param key
         * @param context
         * @param elements
         * @param out
         */
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements,
                            Collector<String> out) {
            System.out.println("处理时间: " + dateFormat.format(context.currentProcessingTime()));

            System.out.println("Window start time: " + dateFormat.format(context.window().getStart()));
            ArrayList<String> list = new ArrayList<>();
            for (Tuple2<String, Long> element : elements) {
                list.add(element.toString() + "|" + dateFormat.format(element.f1));
            }
            // 打印出来当前窗口里面所有的数据
            out.collect(list.toString());
            System.out.println("window end time: " + dateFormat.format(context.window().getEnd()));
        }
    }
}
