package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 电能表编程事件记录
 * @author wjj
 * @date 2021/12/9 18:09
 * @return.
 */
public class e_meter_event_program implements IConversionUtil {
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
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,OP_CODE:BIGINT,DATA_TAG_1:BIGINT,
        // DATA_TAG_2:BIGINT,DATA_TAG_3:BIGINT,DATA_TAG_4:BIGINT,DATA_TAG_5:BIGINT,DATA_TAG_6:BIGINT,DATA_TAG_7:BIGINT,
        // DATA_TAG_8:BIGINT,DATA_TAG_9:BIGINT,DATA_TAG_10:BIGINT,SHARD_NO:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(2), Date.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(4), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(5), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(6), BigDecimal.class));//DATA_TAG_1
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(7), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(8), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(9), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(10), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(11), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(12), BigDecimal.class));//DATA_TAG_7
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(13), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(14), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(15), BigDecimal.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }
}
