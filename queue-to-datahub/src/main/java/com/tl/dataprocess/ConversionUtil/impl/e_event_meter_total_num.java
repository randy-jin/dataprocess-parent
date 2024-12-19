package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 开盖总次数
 * @author wjj
 * @date 2021/12/6 16:23
 * @return
 */
public class e_event_meter_total_num implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMpedId();
//        String dataDate = dataList.get(1).toString();
//        //用于缓存删除
        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //MPED_ID:BIGINT,DATAITEM_ID:BIGINT,TERMINAL_ID:BIGINT,TOTAL_NUM:BIGINT,SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(1), BigDecimal.class));
        finallyList.add(new BigDecimal(terminalArchivesObject.getTerminalId()));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(3), BigDecimal.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        finallyList.add(new Date());
        return finallyList;
    }
}
