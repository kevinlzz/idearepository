package com.atguigu.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author liuzuze
 * @create 2019-05-29 19:10
 */
public class FruitMapper extends TableMapper<ImmutableBytesWritable,Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        /*实现其中的map方法通过value来获取rawCells()
        对cells进行for循环，创建put对象，其中包含key
       */
//        Cell[] cells = value.rawCells();
        Put put = new Put(key.get());
        /*for(Cell cell:cells){
            if("name".equals(CellUtil.cloneQualifier(cell))){
                put.add(cell);
            }
        }*/
        for(Cell cell: value.rawCells()){
            //添加/克隆列族:info
            if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                //添加/克隆列：name
                if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    //将该列cell加入到put对象中
                    put.add(cell);
                    //添加/克隆列:color
                }else if("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    //向该列cell加入到put对象中
                    put.add(cell);
                }
            }
        }
        context.write(key, put);
    }
}
