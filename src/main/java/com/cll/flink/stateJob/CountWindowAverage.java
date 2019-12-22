package com.cll.flink.stateJob;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    private transient ValueState<Tuple2<Integer, Integer>> sum; // a tuple containing the count and the sum

    @Override
    public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {
        Tuple2<Integer, Integer> currentSum = sum.value();
        if(currentSum == null){
            currentSum = Tuple2.of(1,input.f1);
        }else{
            currentSum.f0 += 1;
            currentSum.f1 += input.f1;
        }

        sum.update(currentSum);

        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<Integer, Integer>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {})); // type information
        descriptor.setQueryable("query-name");
        sum = getRuntimeContext().getState(descriptor);
    }
}
