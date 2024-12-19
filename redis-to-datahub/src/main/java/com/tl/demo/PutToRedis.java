package com.tl.demo;

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
/**
 * @author Dongwei-Chen
 * @Date 2020/7/29 17:02
 * @Description
 */
public class PutToRedis {

    public static void main(String[] args) throws Exception {
        Jedis jedis = new Jedis("120.55.242.68", 20024);
        jedis.auth("1qaz2wsx4rfv");

        List<DataItemObject> fnList = new ArrayList<>();
        DataItemObject dio = new DataItemObject(161, 107, true);
        List dataList = new ArrayList();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        dataList.add(sdf.parse(sdf.format(calendar.getTime())));
        dataList.add(new Date());
        dataList.add(new Integer(4));
        dataList.add(new Double(42.14));
        List<List> asList = new ArrayList<>();
        List list = new ArrayList();
        list.add(7.32);
        list.add(7.31);
        list.add(5.32);
        list.add(8.32);
        asList.add(list);
        dataList.add(asList);

        dio.setList(dataList);
        fnList.add(dio);
        TerminalDataObject tds = new TerminalDataObject("2000", 459, 0, 13, 0, 0, new Date(), false);
        tds.setList(fnList);
        tds.setWriteHBase(false);
        tds.setFinish(true);
        tds.setSeq(-1);
        tds.setProtocolId(1);
        for (int j = 0; j < 1; j++) {
            Pipeline pipeline = jedis.pipelined();
            for (int i = 0; i < 1; i++) {
                pipeline.lpush("Q_DATA_FREEZE_FRONT".getBytes(), BytesCoverUtil.converToDataBySelf(tds));
            }
            pipeline.sync();
            System.out.println(j);
        }

        System.out.println();
    }
}
