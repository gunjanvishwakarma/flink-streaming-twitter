package com.gunjan;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

public class QueryableStateClientTest
{
    public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException
    {
        System.out.println("hostname");
        System.out.println("port");
        System.out.println("StateDescriptor name");
        System.out.println("Query");
        System.out.println("Keyed Stream Key");
        System.out.println("Job Id");
        
        
        
        QueryableStateClient client = new QueryableStateClient(args[0],Integer.parseInt(args[1]));
    
        // the state descriptor of the state to be fetched.
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<Boolean>(
                        args[2],
                        TypeInformation.of(new TypeHint<Boolean>() {}));
    
        JobID jobID = JobID.fromHexString(args[5]);
        CompletableFuture<ValueState<Boolean>> resultFuture =
                client.getKvState(jobID, args[3], args[4], BasicTypeInfo.STRING_TYPE_INFO, descriptor);
    
        // now handle the returned value
        ValueState<Boolean> booleanValueState = resultFuture.get();
    
        System.out.println(booleanValueState);
    }
}
