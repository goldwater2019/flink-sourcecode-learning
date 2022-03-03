package com.lambda.flink.common.source;


import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class UnOrderedProcessWindowWithEventTimeTestSource implements SourceFunction<String> {
    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");


    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        String currTime = String.valueOf(System.currentTimeMillis());
        while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
            currTime = String.valueOf(System.currentTimeMillis());
        }

        System.out.println("当前时间: " + dateFormat.format(System.currentTimeMillis()));
        // 13s输出一条数据
        // 日志里面带有eventTime
        TimeUnit.SECONDS.sleep(13);
        String event = "flink," + System.currentTimeMillis();
        String event1 = event;
        sourceContext.collect(event);

        // 16s输出一条数据
        TimeUnit.SECONDS.sleep(3);
        sourceContext.collect("flink," + System.currentTimeMillis());

        // 本该19s输出的数据， 延迟到16s数据
        TimeUnit.SECONDS.sleep(3);
        sourceContext.collect(event1);
    }

    @Override
    public void cancel() {

    }
}
