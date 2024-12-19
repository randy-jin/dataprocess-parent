package com.tl.test;

import com.ls.pf.base.utils.tools.LinkedBlockingQueue;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by huangchunhuai on 2021/11/23.
 */
public class T3 {


    //上行报文匹配下行报文时传入的任务信息（给后续业务使用）
    private String taskId; 		 //任务标识（外部接口传入）
    private String taskType;     //任务类别（外部接口传入）
    //报文标识
    private String frameId;//唯一标识一条报文

    //业务相关(预留给特殊需求使用)
    private String terminalId;   //终端业务标识
    private String areaCode;     //行政区号码
    private String terminalAddr; //终端地址

    private String meterId;      //电表标识
    private String commAddr;     //通讯地址
    private int pn;           //预抄预留（预抄报文中没有表地址只有pn）

    private String dataItemId;   //数据项标识
    private String tableName;   //数据库表名


    /**
     * 入库数据
     * 1.入库数据对象（拉平）和 数据库表字段对应
     * 2.目前Object为List,后去可兼容其它形式入库
     * key 可为pn或commAddr 用于区分电表
     * value 为 数据list，注意将 dataItemId,tableName放在前两位用去区分不同的数据项
     * list结构例子：{"5000038","e_mp_day_read","1",165433300000,100.00,20.00,30.00,10.00,40.00}
     */
    private Map<Object,List<Object>> dataMap;

    public static void main(String[] args) throws InterruptedException {


        Jedis j=new Jedis("",1);
        j.auth("1qaz2wsx4rfv");
        Set<String> keySet=j.keys("FEE:EXEC:TMNL:*");
        int a=0;
        int b=0;
        int c=0;
        for(String key:keySet){
            String str=j.get(key);
            if(str==null)continue;
            String []s =str.split(":");
            if(s[0].equals("ADJ")){
                b++;
            }
            if(s[0].equals("CAL")){
                c++;
            }
            if(s[0].equals("FEE")){
                a++;
            }
        }

        System.out.println("FEE:"+a+"   ADJ:"+b+"   CAL:"+c);

    }





}
