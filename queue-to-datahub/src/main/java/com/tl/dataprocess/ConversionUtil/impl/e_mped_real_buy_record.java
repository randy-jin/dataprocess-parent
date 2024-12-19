package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 购电状态
 * @author  wjj
 * @date 2021/12/22 15:57
 */
public class e_mped_real_buy_record implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMpedId();
        String dataDate = dataList.get(1).toString();
        //用于缓存删除
        String clearDate="00000000000000";
        if (dataSrc.equals("0")||dataSrc.equals("10")){
            clearDate=dataDate;
        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //ID:BIGINT,DATA_DATE:STRING,COL_TIME:TIMESTAMP,ORG_NO:STRING,REMAIN_ENEGY:DOUBLE,REMAIN_MONEY:DOUBLE,
        // ALARM_ENEGY:DOUBLE,FAIL_ENEGY:DOUBLE,SUM_ENEGY:DOUBLE,SUM_MONEY:DOUBLE,BUY_NUM:BIGINT,
        // OVERDR_LIMIT:DOUBLE,OVERDR_ENEGY:DOUBLE,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(dataList.get(1));
        Object callObj=dataList.get(2);//抄表时间处理
        Date callDate=new Date();
        if(callObj!=null){
            callDate=new Date(Long.parseLong(callObj.toString()));
        }
        finallyList.add(callDate);
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(4), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(5), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(6), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(7), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(8), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(9), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(10), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(11), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(12), Double.class));
        finallyList.add(new Date());
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }
}
