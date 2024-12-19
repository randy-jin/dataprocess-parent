package com.tl.hades.persist;

import com.ls.athena.dataprocess.sg3761.beans.DataItemObject;
import com.ls.athena.dataprocess.sg3761.beans.TerminalDataObject;
import com.ls.athena.message.redis.utils.BytesCoverUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class PutToRedis {
    public static void main(String[] args) throws Exception {
//        Jedis jedis = new Jedis("120.55.242.68", 18801);
        Jedis jedis = new Jedis("139.9.136.234", 16388);
        jedis.auth("1qaz2wsx4rfv");

        List<DataItemObject> fnList=new ArrayList<>();
        DataItemObject dio=new DataItemObject(161,16,true);
        List dataList=new ArrayList();
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_MONTH,-1);
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
        dataList.add(sdf.parse(sdf.format(calendar.getTime())));
        dataList.add(new Date());
        dataList.add(new Integer(4));
        dataList.add(new Double(42.14));
        List<List> asList=new ArrayList<>();
        List list=new ArrayList();
        list.add(7.32);
        list.add(7.31);
        list.add(5.32);
        list.add(8.32);
        asList.add(list);
        dataList.add(asList);

        dio.setList(dataList);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        fnList.add(dio);
        TerminalDataObject tds=new TerminalDataObject("1101",20021,0,13,0,0,new Date(),false);
        tds.setList(fnList);
        tds.setWriteHBase(false);
        tds.setFinish(true);
        tds.setSeq(-1);
        tds.setProtocolId(1);
        Pipeline pipeline=jedis.pipelined();
        for (int j = 0; j <10000 ; j++) {
            for (int i = 0; i <1000 ; i++) {
//            pipeline.lpush("SWENO".getBytes(), BytesCoverUtil.converToDataBySelf(tds));
//            pipeline.lpush("FRONT".getBytes(), BytesCoverUtil.converToDataBySelf(tds));
                System.out.println(i);
                pipeline.lpush("Q_DATA_FREEZE_S".getBytes(), BytesCoverUtil.converToDataBySelf(tds));
            }
            pipeline.sync();
        }
        System.out.println("ok");
//        jedis.lpush("FRONT".getBytes(), BytesCoverUtil.converToDataBySelf(tds));
//        TerminalDataObject td= (TerminalDataObject) BytesCoverUtil.coverToValueBySelf(jedis.lpop("Q_DATA_FREEZE_FRONT".getBytes()));
//        System.out.println(td.getAreaCode());
//        byte[] value = jedis.lpop("Q_DATA_FREEZE_FRONT");
//        byte[] bytes = value.getBytes();
        try {
//            byte[] object = BytesCoverUtil.coverToValueBySelf();
//            System.out.println(jedis.lpush("Q_DATA_FREEZE_FRONT",String.valueOf(object)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println();
    }
}
