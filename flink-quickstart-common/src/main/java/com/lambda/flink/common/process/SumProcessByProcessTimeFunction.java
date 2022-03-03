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

public class SumProcessByProcessTimeFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Integer>,
        String, TimeWindow> {
    FastDateFormat dataformat = FastDateFormat.getInstance("HH:mm:ss");
    /**
     * @param key
     * @param context
     * @param allELements
     * @param out
     * @throws Exception
     */
    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Long>> allELements, Collector<Tuple2<String, Integer>> out) {
        int count = 0;
        for (Tuple2<String, Long> element : allELements) {
            count++;
        }
        out.collect(Tuple2.of(key, count));
    }
}
