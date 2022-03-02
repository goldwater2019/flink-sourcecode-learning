package com.lambda.flink.common.source;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @Author: zhangxinsen
 * @Date: 2022/3/2 9:48 AM
 * @Desc:
 * @Version: v1.0
 */

public class UnorderedProcessWindowTestSource implements SourceFunction<String> {
    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        String currTime = String.valueOf(System.currentTimeMillis());

        while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
            currTime = String.valueOf(System.currentTimeMillis());
            continue;
        }

        System.out.println("当前时间: " + dateFormat.format(System.currentTimeMillis()));

        // 13s 输出一条数据
        TimeUnit.SECONDS.sleep(13);
        String log = "flink";
        ctx.collect(log);

        // 16s 输出一条数据
        TimeUnit.SECONDS.sleep(3);;
        ctx.collect(log);

        // 本该13s输出的另外一条数据延迟到19s输出
        TimeUnit.SECONDS.sleep(3);
        ctx.collect(log);

        TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
    }

    @Override
    public void cancel() {

    }
}
