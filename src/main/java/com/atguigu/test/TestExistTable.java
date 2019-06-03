package com.atguigu.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liuzuze
 * @create 2019-05-28 12:37
 */
public class TestExistTable {
    /**
     * the old api
     * @param tableName
     * @return
     * @throws IOException
     */
    /*public static boolean isExistTable(String tableName) throws IOException {
        //创建Hbase配置文件
        HBaseConfiguration conf = new HBaseConfiguration();
        //给文件设置参数
        conf.set("hbase.zookeeper.quorum","192.168.10.102");
        //创建Hbaseadmin对象
        HBaseAdmin admin = new HBaseAdmin(conf);
        //使用admin对象进行判断是否存在相关表
        boolean exists = admin.tableExists(tableName);
        //关闭资源
        admin.close();
        //输出返回值
        return exists;
    }*/
    static Connection conn=null;
    static HBaseAdmin admin=null;
    static Configuration conf =null;
    static{
        //创建Hbase配置文件
        conf = HBaseConfiguration.create();
        //给文件设置参数
        conf.set("hbase.zookeeper.quorum","192.168.10.102");
        //创建Hbaseadmin对象
//        HBaseAdmin admin = new HBaseAdmin(conf);
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin)conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static boolean isExistTableNewAPI(String tableName) {
        //使用admin对象进行判断是否存在相关表
        boolean exists = false;
        try {
            exists = admin.tableExists(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //关闭资源
//        closeResource(conn,admin);
//        admin.close();
        //输出返回值
        return exists;
    }

    private static void closeResource(Connection conn, HBaseAdmin admin) {
        if(conn!=null){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    //创建表
    public static void createTable(String tableName,String... cfs){
        //在创建表之前进行判断是否存在表
        if(!isExistTableNewAPI(tableName)){
            //创建表描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            //创建表操作
            for(String cf:cfs){
                //利用表描述器来创建列族
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            try {
                admin.createTable(hTableDescriptor);
                System.out.println("创建"+tableName+"成功！");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        closeResource(conn,admin);
    }
    public static void deleteTable(String tableName){
        if(!isExistTableNewAPI(tableName)){
            return ;
        }
        try {
            //使得表不能用 disable
            admin.disableTable(tableName);
            //执行删除操作
            admin.deleteTable(tableName);
            //输出表已删除
            System.out.println("表已删除！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void deleteTable(String tableName, String... rows) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();

    }
    //添加列族、列名，值
    //执行添加操作
    //增、改 putData
    public static void addTable(String tableName,String rowKey, String columnFamily, String
            column, String value
    ) throws IOException {
        //创建HTable对象(也可以使用connection.getTable方法获取table对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        //创建put对象new Put()其中参数使用Bytes.toBytes
//        Put put = new Put(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily), Bytes.toBytes(value));
        Put put = new Put(Bytes.toBytes(rowKey));
        //向put对象添加数据
//        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),Bytes.toBytes(value));
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column) ,Bytes.toBytes(value) );
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes("sex") ,Bytes.toBytes("male") );
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes("age") ,Bytes.toBytes("18") );
        table.put(put);
        table.close();
        System.out.println("数据插入成功！");
    }

    public static void delete(String tableName,String rowKey,String cf,String cn) throws IOException{
        //获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        //创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //执行删除操作
        table.delete(delete);
        table.close();
    }

    /**
     * 查，全表扫描
     * @param
     * @throws IOException
     */
    public static void scanTable(String tableName) throws IOException {
        //利用连接获取table对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        //创建scan扫描器对象
        Scan scan = new Scan();
        //调用getScanner方法，然后获取结果集对象
        ResultScanner scanner = table.getScanner(scan);
        //遍历循环结果集，结果集中包含cell，对cell进行查看，有RK,CF,CN,VALUE
        for (Result result:scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell:cells){
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }
    //获取指定列族：列的数据
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {
        //获取table对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        //创建一个Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //获取数据的操作
        Result result = table.get(get);
        //获取cells的操作
        Cell[] cells = result.rawCells();
        //遍历循环
        for (Cell cell: cells) {
            System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))+
                ",CF:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                ",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                 ",Value:"+Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
        table.close();
    }
    public static void main(String[] args) throws IOException {
//        boolean isExist = isExistTable("student");
       /* boolean isExist = isExistTable("staff");
        System.out.println(isExist);*/
       /* boolean isExist = isExistTableNewAPI("staff");
        System.out.println(isExist);*/
//        createTable("staff","info");
//        deleteTable("staff");
//        addTable("student", "1003", "info", "name", "diudiu");
//        delete("student", "1003", "info", "name");
//        scanTable("student");
        getData("student","1002","info","name");
//        closeResource(conn, admin);
    }
}


