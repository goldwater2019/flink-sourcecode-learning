package com.lambda.flink.kafka;

import com.lambda.flink.common.source.CorpusRichSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: zhangxinsen
 * @Date: 2022/3/1 11:08 PM
 * @Desc: source端, 将数据输入kafka. quick start
 * @Version: v1.0
 */

public class SourceDemo {
    public static void main(String[] args) throws Exception {
        // 获得env
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获得source
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.addSource(new CorpusRichSource());
        // no-ops
        stringDataStreamSource.print();
        executionEnvironment.execute("corpus source kafka sink quickstart");
    }
}
