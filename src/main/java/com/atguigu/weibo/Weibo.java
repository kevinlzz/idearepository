package com.atguigu.weibo;

import java.io.IOException;

/**
 * @author liuzuze
 * @create 2019-06-02 11:11
 */
public class Weibo {
    public static void init() throws IOException {
        //创建相关命名空间&表
        WeiboUtil.createNamespace(Constant.NAMESPACE);
        WeiboUtil.createTable(Constant.CONTENT,1,"info");
        //创建用户关系表
        WeiboUtil.createTable(Constant.RELATIONS, 1,"attends");
        //创建收件箱（多版本）
        WeiboUtil.createTable(Constant.INBOX, 1,"info");
    }

    public static void main(String[] args) throws IOException {
        //测试
//        init ();
        //1001，1002发布微博
//        WeiboUtil.createData("1001", "今天天气好晴朗！");
//        WeiboUtil.createData("1002", "今天大雨滂沱！");
        //1001关注1002和1003
//          WeiboUtil.addAttend("1001","1002","1003");
        //获取1001初始化页面信息
//       WeiboUtil.getInit("1001");
        //1003发布微博
//        WeiboUtil.createData("1003", "今天天气真不好！");
//        System.out.println();
        //获取1001初始化页面信息
//        WeiboUtil.getInit("1001");
        WeiboUtil.delAttend("1001","1002");
//        WeiboUtil.getInit("1001");
    }
}
