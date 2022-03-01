package com.lambda.flink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/11 11:41 PM
 * @Desc:
 * @Version: v1.0
 */

public class FlinkState01OperateState {


    // 自定义一个状态管理器
    // 1. Task有自己的状态
    // 2. Key有自己的状态
    // 最怕的就是Task突然死掉, 状态丢失
    // 既然要保证状态数据的安全, 所以加入state持久化机制
    // 到底什么是checkpint
    // 其实就是把每个Task自己身上的state给持久化起来

    /**
     * CheckpointedFunction
     * 1. 持久化的方法 把State存起来 snapshotState
     * 2. 恢复的方法  从State的存储地再恢复过来 initializeState
     */
    class UDPrint implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

        }
    }
}
