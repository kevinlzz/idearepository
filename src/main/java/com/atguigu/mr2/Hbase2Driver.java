package com.atguigu.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author liuzuze
 * @create 2019-05-30 22:28
 */
public class Hbase2Driver extends Configuration implements Tool {
    private Configuration conf=null;
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(Hbase2Driver.class);
        job.setMapperClass(Hbase2Mapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);
        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit2", Hbase2Reducer.class, job);
        FileInputFormat.setInputPaths(job,args[0]);
        boolean result = job.waitForCompletion(true);
        return result?0:1;
    }

    public void setConf(Configuration configuration) {
        this.conf=configuration;
    }

    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int i = ToolRunner.run(conf, new Hbase2Driver(), args);
        System.exit(i);
    }
}
