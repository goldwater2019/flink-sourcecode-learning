package com.lambda.flink.common.source;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @Author: zhangxinsen
 * @Date: 2022/3/2 12:25 AM
 * @Desc:
 * @Version: v1.0
 */

public class OrderedProcessWindowTestSource implements SourceFunction<String> {
    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        String currTime = String.valueOf(System.currentTimeMillis());
        // 为了保证是10s的倍数
        while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
            currTime = String.valueOf(System.currentTimeMillis());
            continue;
        }

        System.out.println("当前时间: " + dateFormat.format(System.currentTimeMillis()));

        TimeUnit.SECONDS.sleep(13);
        ctx.collect("flink");
        ctx.collect("flink");

        // 16s输出一条数据
        TimeUnit.SECONDS.sleep(3);
        ctx.collect("flink");

        TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
    }

    @Override
    public void cancel() {

    }
}
