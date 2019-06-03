package com.atguigu.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;

import javax.management.relation.Relation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liuzuze
 * @create 2019-06-02 11:11
 */
public class WeiboUtil {
    private static Configuration configuration=HBaseConfiguration.create();
    //创建命名空间
    static {
        configuration.set("hbase.zookeeper.quorum", "192.168.10.102");
    }
    public static void createNamespace(String ns) throws IOException {
        //创建链接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //创建NS描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        //创建操作
        admin.createNamespace(namespaceDescriptor);
        //关闭资源
        admin.close();
        connection.close();
    }
    //创建表
    public static void createTable(String tableName,int versions,String...cfs) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf:cfs){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
    }

    /**
     * 更新微博内容表数据
     * 更新收件箱数据
     *      --获取当前操作人的粉丝
     *      --去往收件箱数据
     * @param uid
     * @param content
     * @throws IOException
     */
    //发布微博
    public static void createData(String uid,String content) throws IOException {
        //获取相关连接以及内容表，关系表
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table conTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        long timeMillis = System.currentTimeMillis();
        String rowKey=uid+"_"+timeMillis;
        //生成put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
        //往内容表添加数据
        conTable.put(put);
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relaTable.get(get);
        Cell[] cells = result.rawCells();
        if(cells.length<=0){
            return;
        }
        List<Put> puts = new ArrayList<Put>();
        for(Cell cell:cells){
            byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
            Put inboxPut = new Put(cloneQualifier);
            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            puts.add(inboxPut);
        }
        inboxTable.put(puts);
        //关闭资源
        inboxTable.close();
        relaTable.close();
        conTable.close();
        connection.close();
    }

    /**
     * 在用户关系表
     *      --添加操作人的attends
     *      --添加被操作人的fans
     * 在收件箱中
     *      --在微博内容表中添加被关注者的3条数据
     *      --在收件箱表中添加操作人的关注者信息
     * @param uids
     */
    //关注用户
    public static void addAttend(String uid,String...uids) throws IOException {
        //获取相关连接以及内容表，关系表
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table conTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        Put relaPut = new Put(Bytes.toBytes(uid));
        ArrayList<Put> puts = new ArrayList<Put>();
        for(String s:uids){
            relaPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(s), Bytes.toBytes(s));
            Put fansPut=new Put(Bytes.toBytes(s));
            fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fansPut);
        }
        puts.add(relaPut);
        relaTable.put(relaPut);
        Put inboxPut=new Put(Bytes.toBytes(uid));
        //获取内容表中被关注者的rowkey
        for(String s:uids){
            Scan scan = new Scan(Bytes.toBytes(s+" "));
            ResultScanner results = conTable.getScanner(scan);
            for(Result result:results){
                String rowKey = Bytes.toString(result.getRow());
                String[] split = rowKey.split("_");
                byte[] row = result.getRow();
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s),Long.parseLong(split[1]),row);
            }
        }
        inboxTable.put(inboxPut);
        inboxTable.close();
        relaTable.close();
        conTable.close();
    }
    //取关用户
    public static void delAttend(String uid,String...uids) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        ArrayList<Delete> deletes = new ArrayList<Delete>();
        Delete relaDel = new Delete(Bytes.toBytes(uid));
        for(String s:uids){
           //创建被取关者删除对象
            Delete fansDel = new Delete(Bytes.toBytes(s));
            fansDel.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(s));
            relaDel.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(s));
            deletes.add(fansDel);
        }
        deletes.add(relaDel);
        relaTable.delete(deletes);
        //删除收件箱表相关内容
        Delete inboxDel=new Delete(Bytes.toBytes(uid));
        for(String s:uids){
            inboxDel.addColumns(Bytes.toBytes("info"), Bytes.toBytes(s));
        }
        //执行收件箱表删除操作
        inboxTable.delete(inboxDel);
        //关闭资源
        inboxTable.close();
        relaTable.close();
        connection.close();
    }
    //获取微博内容（初始化页面）
    public static void getData(String uid) throws IOException {
        //获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(Constant.CONTENT));
        //扫描(过滤器）
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        //遍历打印
        scan.setFilter(rowFilter);
        ResultScanner results = table.getScanner(scan);
        for(Result result:results){
            Cell[] cells = result.rawCells();
            for(Cell cell:cells){
                System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))+",Content:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //关闭资源
        table.close();
    }
    //获取微博内容（查看某个人所有微博内容）
    public static void getInit(String uid) throws IOException {
        //获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表对象（2个）
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        //获取收件箱表数据
        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions();
        Result result = inboxTable.get(get);
        List<Get> gets = new ArrayList<Get>();
        //遍历返回内容，并形成get
        for(Cell cell:result.rawCells()){
            Get contGet = new Get(CellUtil.cloneValue(cell));
            gets.add(contGet);
        }
        //根据收件箱表获取值去往内容表获取微博内容
        Result[] results = contTable.get(gets);
        for (Result result1 : results) {
            Cell[] cells = result1.rawCells();
            //遍历循环
            for (Cell cell : cells) {
                System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))+",Content:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //关闭资源
        inboxTable.close();
        contTable.close();
        connection.close();
    }
}
