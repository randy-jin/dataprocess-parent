package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 电能表跳闸事件记录
 * @author wjj
 * @date 2021/12/8 9:36
 * @return
 */
public class e_meter_event_trip implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.get(1).toString();
        //用于缓存删除,事件类型用默认值
        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,
        // OP_CODE:BIGINT,PAP_R:DOUBLE,RAP_R:DOUBLE,QUAD_1_R:DOUBLE,QUAD_2_R:DOUBLE,
        // QUAD_3_R:DOUBLE,QUAD_4_R:DOUBLE,SHARD_NO:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(2), Date.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(4), Date.class));//EVENT_ST
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(5), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(6), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(7), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(8), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(9), Double.class));//QUAD_2_R
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(10), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(11), Double.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }
}
