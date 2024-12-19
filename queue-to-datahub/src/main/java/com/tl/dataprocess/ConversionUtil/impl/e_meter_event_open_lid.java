package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 电能表开表盖事件
 * @author wjj
 * @date 2021/12/6 10:09
 * @return
 */
public class e_meter_event_open_lid implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMeterId();
        //用于缓存删除
        String clearDate="00000000000000";
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,BEFORE_PAP_R:DOUBLE,
        // BEFORE_RAP_R:DOUBLE,BEFORE_QUAD_1_R:DOUBLE,BEFORE_QUAD_2_R:DOUBLE,BEFORE_QUAD_3_R:DOUBLE,
        // BEFORE_QUAD_4_R:DOUBLE,AFTER_PAP_R:DOUBLE,AFTER_RAP_R:DOUBLE,AFTER_QUAD_1_R:DOUBLE,AFTER_QUAD_2_R:DOUBLE,
        // AFTER_QUAD_3_R:DOUBLE,AFTER_QUAD_4_R:DOUBLE,SORT_NO:BIGINT,EXTEN_INFO:STRING,SHARD_NO:STRING,EVENT_DATE:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(2), Date.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(4), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(5), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(6), Double.class));//BEFORE_PAP_R
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(7), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(8), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(9), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(10), Double.class));//BEFORE_QUAD_3_R
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(11), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(12), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(13), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(14), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(15), Double.class));//AFTER_QUAD_2_R
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(16), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(17), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(18), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(19), String.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(1), String.class));
        return finallyList;
    }
}
