package com.lambda.flink.common.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

/**
 * @Author: zhangxinsen
 * @Date: 2022/3/2 2:55 PM
 * @Desc: 指定时间字段
 * @Version: v1.0
 */

public class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>>, Serializable {
    @Override
    public void onEvent(Tuple2<String, Long> event, long eventTimeStamp, WatermarkOutput output) {
        System.out.println(eventTimeStamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(System.currentTimeMillis()));
    }
}
