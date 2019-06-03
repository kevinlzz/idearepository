package com.atguigu.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author liuzuze
 * @create 2019-05-29 19:26
 */
public class FruitReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //读出来的每一行数据写入到fruit_mr表中
        for(Put put:values){
            context.write(NullWritable.get(), put);
        }
    }
}
