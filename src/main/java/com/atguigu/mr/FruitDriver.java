package com.atguigu.mr;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author liuzuze
 * @create 2019-05-29 19:41
 */
public class FruitDriver extends Configuration implements Tool {
    private Configuration conf=null;
    public int run(String[] strings) throws Exception {
        //得到configuration对象
        conf = this.getConf();
        //创建job任务
        //也可以对job对应的名称
        Job job = Job.getInstance(conf);
        //Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        //设置jar的class
//        job.setJarByClass(FruitDriver.class);
        job.setJarByClass(FruitDriver.class);
        //配置Scan，给scan配置相关属性
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);
        //设置Mapper，注意导入的是mapreduce包下的，不是mapred包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob("fruit", scan, FruitMapper.class, ImmutableBytesWritable.class, Put.class, job);
        //设置reduce
        TableMapReduceUtil.initTableReducerJob("fruit_mr", FruitReducer.class, job);
        boolean result = job.waitForCompletion(true);
        if(!result){
            throw new IOException("Job running with error");
        }
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
        int status = ToolRunner.run(conf, new FruitDriver(), args);
        System.exit(status);
    }
}
