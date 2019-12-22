package com.cll.flink.queryableState;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;


import java.util.concurrent.CompletableFuture;

import static java.lang.Thread.sleep;

public class QueryableMain {
    public static void main(String[] args) throws Exception {
        JobID jobId = JobID.fromHexString("110aadcc28135db5bf607a42b05e20c0");
        Integer key = 1;
        QueryableStateClient client = new QueryableStateClient("localhost", 6123);

        // the state descriptor of the state to be fetched.
        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                new ValueStateDescriptor<>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {}));

        CompletableFuture<ValueState<Tuple2<Integer, Integer>>> resultFuture =
                client.getKvState(jobId, "query-name", key, BasicTypeInfo.INT_TYPE_INFO, descriptor);

        System.out.println("get kv state return future, waiting......");
        System.out.println("query resultFuture:" + resultFuture.toString());
        System.out.println("query resultFuture 4:" + JSON.toJSONString(resultFuture));

        // now handle the returned value

        sleep(10000);
        System.out.println("query resultFuture 2:" + resultFuture.toString());
        System.out.println("query resultFuture 3:" + JSON.toJSONString(resultFuture));

        resultFuture.thenAccept(response -> {
            try {
                System.out.println("test one" );
                Tuple2<Integer, Integer> res = response.value();
                System.out.println("query res:" + res.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

       /* ValueState<Tuple2<Integer, Integer>> res = resultFuture.join();
        System.out.println("query result:" + res.value());
        client.shutdownAndWait();*/
    }
}
