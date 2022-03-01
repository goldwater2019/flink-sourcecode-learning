package com.lambda.flink.common.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Locale;
import java.util.Random;

/**
 * @Author: zhangxinsen
 * @Date: 2022/3/1 11:24 PM
 * @Desc:
 * @Version: v1.0
 */

public class CorpusRichSource extends RichSourceFunction<String> {
    public static final String FILENAME = "/Users/zhangxinsen/workspace/flink-sourcecode-learning/flink-kafka-demo/src/main/resources/corpus.txt";
    public static final Random RANDOM = new Random();

    /**
     * 循环读取文件中的数据, 并且写入ctx中
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
        while (true) {
            int skipLength = Math.abs(RANDOM.nextInt()) % 10000;
            InputStream inputStream = new FileInputStream(FILENAME);
            byte[] b = new byte[10000];
            byte[] skipBytes = new byte[skipLength];
            inputStream.read(skipBytes);
            StringBuffer sb = new StringBuffer("");
            int len = inputStream.read(b);
            if (len > 0) {
                sb.append(new String(b, 0, len));
                String content = sb.toString();
                ctx.collect(content);
                Thread.sleep(500L);
            }
            inputStream.close();
        }
    }

    @Override
    public void cancel() {

    }
}
