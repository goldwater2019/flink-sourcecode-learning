package com.lambda.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/11 10:34 PM
 * @Desc: 每3个相同的Key就计算一下平均值
 * @Version: v1.0
 * <p>
 * 两个重点<br/>
 * 1. 一个Key一个ValueState
 * <br/>
 * 2. 每个泛型都不固定, 根据需求走
 */

public class FlinkState02ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(3);

        // 每3个相同的Key, 输出他们的平均值
        DataStreamSource<Tuple2<Long, Long>> sourceDS = executionEnvironment.fromElements(Tuple2.of(1L, 3L),
                Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L),
                Tuple2.of(1L, 5L),
                Tuple2.of(2L, 3L),
                Tuple2.of(2L, 5L)
        );

        /**
         * 使用三种State来进行计算
         */

        SingleOutputStreamOperator<Tuple2<Long, Double>> resultDS = sourceDS.keyBy(0)
                /**
                 * 状态变成
                 * flatmap参数, 就必然是一个FlatMapFunction的实现类的实例
                 * 具体使用的其实是RichFlatMapFunction
                 * 在flatMap内部, 一定调用匿名函数的实例方法
                 */
                .flatMap(new CountAverageWithListState());
        /**
         * map和flatmap最大的区别
         * 1. map中的函数, 接受一个输入, 必须输出一个值
         * 2. flatmap(f)中的参数函数f, 接受一个值, 可以输出0,1,N个值
         * 如果仅仅只是简单的映射操作, 用map即可
         * 复杂逻辑使用flatmap
         */

        resultDS.print();
        executionEnvironment.execute("Value State");
    }


    /**
     * RichFlatMapFunction<Input, Output>
     * <p>
     * Source自定义: implements SourceFunction || implements RichSourceFunction
     * <br/>
     * Sink自定义: implements SinkFunction || implements RichSinkFunction
     * <br/>
     */
    // RichFlatMapFUnction<Tuple2<Long, Long>, Tuple2<Long, Double>>
    // 第一个反省参数: 输入数据的类型
    // 第二个泛型参数: 输出数据的类型
    private static class CountAverageWithListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

        private ValueState<Tuple2<Long, Long>> countAndSumState;
        private ListState<Long> listState;

        /**
         * 初始化方法
         * <br/>
         * 1. 先声明一个XXXStateDescriptor 用来描述状态的相关信息
         * <br/>
         * 2. 通过运行时上下文初始化state
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long, Long>> countAndSumDesc = new ValueStateDescriptor<Tuple2<Long, Long>>("countAndSum",
                    Types.TUPLE(Types.LONG, Types.LONG));
            ListStateDescriptor<Integer> avg_listState = new ListStateDescriptor<>(
                    "avg_listState",
                    Types.INT
            );

            countAndSumState = getRuntimeContext().getState(countAndSumDesc);
        }

        /**
         * 状态处理和逻辑处理
         *
         * @param records   输入 Tuple2<Long, Long> records
         * @param collector 方法没有返回值, 但是可以决定是否输出结果, 最终是否要输出结果通过collector来输出
         * @throws Exception 主要两点
         *                   1. 接收到这个Key的前两条数据的时候, 需要做状态更新, 不做计算
         *                   2. 当接收到第三条数据的时候, 从状态中获取出来之前存储的两条数据, 计算平均值, 通过collector输出结果
         *                   <br>
         *                   一个Key一个ValueState
         */
        @Override
        public void flatMap(Tuple2<Long, Long> records, Collector<Tuple2<Long, Double>> collector) throws Exception {
            // 如果不存在, 则初始化一个
            Tuple2<Long, Long> currentState = countAndSumState.value();
            if (currentState == null) {
                currentState = Tuple2.of(0L, 0L);
            }

            // 状态处理
            currentState.f0 += 1;
            currentState.f1 += records.f1;

            // 状态更新
            countAndSumState.update(currentState);

            // 结果输出
            if (currentState.f0 == 3L) {
                double avg = (double) currentState.f1 / currentState.f0;
                collector.collect(Tuple2.of(records.f0, avg));
                countAndSumState.clear();
            }
        }
    }
}
