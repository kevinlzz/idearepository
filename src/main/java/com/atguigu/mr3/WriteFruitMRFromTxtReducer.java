package com.atguigu.mr3;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author liuzuze
 * @create 2019-06-01 22:26
 */
public class WriteFruitMRFromTxtReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put put:values) {
            context.write(NullWritable.get(), put);
        }
    }
}
