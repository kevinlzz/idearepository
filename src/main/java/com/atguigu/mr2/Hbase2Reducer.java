package com.atguigu.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author liuzuze
 * @create 2019-05-30 22:22
 */
public class Hbase2Reducer extends TableReducer<NullWritable,Put,NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for(Put put:values){
            context.write(NullWritable.get(), put);
        }
    }
}
