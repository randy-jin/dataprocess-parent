package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 电能表事件清零
 * @author wjj
 * @date 2021/12/7 10:32
 * @return
 */
public class e_meter_event_clear_event implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMeterId();
        String dataDate = dataList.remove(1).toString();
        //用于缓存删除
        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,REPORT_OR_COLL:BIGINT,COLL_TIME:TIMESTAMP,
        // EVENT_ST:TIMESTAMP,OP_CODE:BIGINT,EVENT_FLAG:STRING,SHARD_NO:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMeterId()));
        finallyList.add(new Date());
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(3), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(4), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(5), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(6), BigDecimal.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(7), String.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }
}
