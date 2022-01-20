package com.lambda.flink.partitioner;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.PartitionOperator;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/10 11:34 PM
 * @Desc:
 * @Version: v1.0
 */

public class FlinkBatchPartitionerRebalance {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        // 构建数据源
        DataSource<Integer> dataset = executionEnvironment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 11, 12, 13, 15, 14);
        // 重新平很, 解决数据倾斜, 使用Round-Robin的方式
        PartitionOperator<Integer> resultDS = dataset.rebalance().setParallelism(3);
        // 数据
        resultDS.print();

        executionEnvironment.execute("FlinkBatchPartitionerRebalance");
    }
}
