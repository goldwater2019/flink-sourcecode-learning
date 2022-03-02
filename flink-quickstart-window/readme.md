## Flink Time 和 Watermark深入分析

重点分析Flink如何处理乱序数据/延迟数据, 主要内容包括:

```text
1. Flink window常见需求被禁
2. Process Time window(有序)
3. Process Time window(无序)
4. 使用EventTime处理无序
5. 使用WaterMark机制解决无序
6. Flink Watermark机制定义
7. 深入理解Flink Watermark
8. Flink处理太过延迟数据
9. Flink多并行度Watermark
```



### Flink Window常见需求背景

需求描述:

* 每隔5s, 计算最近10s单次出现的次数 -- 滑动窗口
* 每隔5s, 计算最近5秒单词出现的次数 -- 滚动窗口

####  关于TimeCharacteristic

```text
ProcessingTime
IngestionTime
EventTime
```

#### `SlidingProcessingTimeWindows` 可以拆分为: `Sliding` + `ProcessingTime` + `TimeWindows`, 是`WindowAssigner` 的子类

```text
SlidingProcessingTimeWindows
SlidingEventTimeWindows
TumblingEventTimeWindows
TumblingProcessingTimeWindow
```



#### TimeWindow代码实现

```java
public class TimeWindowWordCount_TimeWindow {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 老版本API启动time
    // env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    // 获取数据源
    DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);
    // 数据的处理
    SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = dataStream.flatMap(
    	new FlatMapFunction<String, Tuple2<String, Integer>>() {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
          String [] fields = line.split(",");
          for (String word: fields) {
            out.collect(Tuple2.of(word, 1));
          }
        }
      }
    ).keyBy(tuple -> tuple.f0)
    // .timeWindow(Time.seconds(10), Time.seconds(5))
    .window(SlidingPrcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .sum(1);
    
    result.print()
      	.setParallelism(1);  // 数据的输出
    // 程序的启动
    env.execute("Window word count");
  }
}
```



#### ProcessWindowFunction

如果想要知道`flink`的`window`的执行的一些细节信息

```java
// 定义窗口
.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));
// 定义窗口的数据计算逻辑
.process(new SumProcessFunction());
```



通过自定义window的ProcessFunction来观察窗口的一些信息.

```java
public class TimeWindowWordCount_ProcessWindowFunction {
  public static void main(Stirng[] args) throws Exception {
    // 获得执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 获得dataStream
    DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);
    //
    SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = dataStream.flapMap(
    	)
     .keyBy(tuple -> tuple.f0)
     // .timeWindow(Time.seconds(10), Time.seconds(5))
     // 每个5s去计算10s内的数据的结果, 用的是ProcessingTime
     .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      // process foreach
      .process(new SumProcessFunction());
    resultDS.print()
      .setParallelism(1);
    
    env.execute();
  }
}
```



而对于`SumProceeFunction`

```java
package com.lambda.flink.window.process;

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
```





注意Flink划分窗口的方式: 根据每个5秒统计最近10秒的数据. Flink互粉的窗口是:

```text
[00:00:00, 00:00:05)
[00:00:05, 00:00:10)
[00:00:10, 00:00:15)
...
```



### Flink Time种类

Flink的数据流处理定义了三种Time, 分别是:

* `EventTime`: 时间产生的时间, 它通常由事件中的时间戳描述
* `IngestionTime`: 事件进入FLink的时间(一般不同)
* `ProcessingTime`: 事件被处理时当前系统的时间



举例解释:

```text
2021-11-11 10:10:01, 134 INFO executor.Executor: Finished task in state 0.0
```

这条数据进入Flink的时间是`2021-11-11 20:00:00, 102`, 到达`window` 处理的时间为`2021-11-11 20:00:01, 100`

则对应的三个Time分别是:

* `Event time`: 2021-11-11 10:10:01, 134
* `ingestion time` : 2021-11-11 20:00:00, 102
* `Processing time`: 2021-11-11 20:00:01, 100

在企业生产环境中, 一般使用`EventTime` 来进行计算, 会更加符合业务需求, 比如下述需求:

* 统计每分钟内接口调用失败的错误日志个数
* 统计每分钟每种类型商品的成交单数



下面, 分情况讨论解决方案:

* 假设数据有序, 基于Process Time Window做处理有问题么? 没问题
* 假设数据无序, 基于Process Time Window做处理有问题么? 有问题
* 解决方案: 基于从去执行结果, 会纠正部分结果, 不会把所有计算都搞正确
* 解决方案: 基于Flink提供的watermark实现这个需求
* 最终结论: Flink 基于 `Window` + `EventTime` + `Watermark` 联合起来完成乱序数据的处理

```java
//如何基于eventTime和waterMark去实现乱序数据的处理
.assignTimestampsAndWatermarks(
	// 指定watermark的规则
  WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
  // 指定 eventTime的定义
  .wienTimestampAssigner((ctx) -> new TimestampExetractor())  // 指定时间字段
)
```

### Process Time Window(有序)

需求: 每个5秒计算最近10秒的单词出现的次数(类似于需求: 接口调用出错的次数)

会产生的窗口有:

```text
20:50:00 - 20:50:10
20:50:05 - 20:50:15
20:50:10 - 20:50:20
20:50:15 - 20:50:25
...
```

自定义source, 模拟: 第13秒的时候连续发送两个时间, 第16秒的时候再发送一个事件



### Process Time Window(无序)

自定义Source, 模拟:

```text
正常情况下, 第13秒的时候连续发送2个事件
但是有一个事件确实在第13秒发送出去了, 另外一个事件因为某些原因, 在19秒的时候才发送出去
第16秒的时候再发送一个事件
```



总结一个结论: Flink 基于ProcessingTime + Window来处理乱序数据的话, 计算结果是不准确的.



### 使用EventTime处理无序

