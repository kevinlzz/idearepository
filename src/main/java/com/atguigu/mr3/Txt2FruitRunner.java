package com.atguigu.mr3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author liuzuze
 * @create 2019-06-01 22:31
 */
public class Txt2FruitRunner extends Configuration implements Tool{
    private Configuration configuration=null;
    public int run(String[] strings) throws Exception {
        //得到Configuration
        Configuration conf = this.getConf();

//创建Job任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Txt2FruitRunner.class);
        Path inPath = new Path("hdfs://hadoop102:9000/input_fruit/fruit.tsv");
        FileInputFormat.addInputPath(job, inPath);

//设置Mapper
        job.setMapperClass(HbaseMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

//设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitMRFromTxtReducer.class, job);

//设置Reduce数量，最少1个
        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw new IOException("Job running with error");
        }

        return isSuccess ? 0 : 1;

    }

    public void setConf(Configuration configuration) {
        this.configuration=configuration;
    }

    public Configuration getConf() {
        return configuration;
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new Txt2FruitRunner(), args);
        System.exit(status);
    }

}
