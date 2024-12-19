package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 电表时钟
 * @author  wjj
 * @date 2021/12/22 15:55
 */
public class e_meter_time implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getTerminalId();
        //用于缓存删除
        String clearDate="00000000000000";
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //DATA_DATE:STRING,MPED_ID:BIGINT,SET_TIME:STRING,COLL_TIME:TIMESTAMP,TIME_TYPE:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(0), Date.class));
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(2), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(3), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(4), Double.class));
        finallyList.add(new Date());
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }
}
