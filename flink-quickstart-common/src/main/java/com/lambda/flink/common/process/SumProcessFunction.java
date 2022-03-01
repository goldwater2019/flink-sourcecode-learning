package com.lambda.flink.common.process;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: zhangxinsen
 * @Date: 2022/3/1 11:43 PM
 * @Desc:
 * @Version: v1.0
 */

public class SumProcessFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>,
        String, TimeWindow> {
    FastDateFormat dataformat = FastDateFormat.getInstance("HH:mm:ss");
    /**
     * @param key
     * @param context
     * @param elements
     * @param out
     * @throws Exception
     */
    @Override
    public void process(String key, ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        System.out.println("=========== 触发了一次窗口============");
        System.out.println("系统时间: " + dataformat.format(System.currentTimeMillis()));
        System.out.println("process 时间: " + dataformat.format(context.currentProcessingTime()));
        System.out.println("窗口开始时间: " + dataformat.format(context.window().getStart()));
        System.out.println("窗口结束时间: " + dataformat.format(context.window().getEnd()));
        // 实现求和的功能
        int count = 0;
        for (Tuple2<String, Integer> element : elements) {
            count++;
        }
        out.collect(Tuple2.of(key, count));
    }
}
