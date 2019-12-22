package com.cll.flink.stateJob;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

import static java.lang.Thread.sleep;

public class RandomTuple2Source implements SourceFunction<Tuple2<Integer, Integer>> {

    private volatile boolean running = true;


    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> sourceContext) throws Exception {

        Random random = new Random();
        while (running) {
            Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(1,random.nextInt(100));
            sourceContext.collect(tuple);
            System.out.println("source:" + tuple.toString());
            sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
