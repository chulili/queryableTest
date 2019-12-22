package com.cll.flink.stateJob;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CountWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L), Tuple2.of(1L, 1L))
//                .keyBy(0)
//                .flatMap(new CountWindowAverage())
//                .print();

        env.addSource(new RandomTuple2Source())
                .keyBy(0) //key by first value of tuple
                .flatMap(new CountWindowAverage())
                .print();
        JobExecutionResult result = env.execute("testQueryableState");

        System.out.println("submit job result:" + result);
    }
}
