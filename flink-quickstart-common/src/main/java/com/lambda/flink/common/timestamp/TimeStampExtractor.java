package com.lambda.flink.common.timestamp;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author: zhangxinsen
 * @Date: 2022/3/2 2:53 PM
 * @Desc:
 * @Version: v1.0
 */

/**
 * 指定 eventTime字段
 */
public class TimeStampExtractor implements TimestampAssigner<Tuple2<String, Long>> {
    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
        // 这个地方的单位是ms
        return element.f1;
    }
}
