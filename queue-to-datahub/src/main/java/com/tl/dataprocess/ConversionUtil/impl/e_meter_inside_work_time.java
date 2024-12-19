package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 内部电池工作时间
 * @author wjj
 * @date 2021/11/29 14:41
 * @return
 */
public class e_meter_inside_work_time implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getTerminalId();
        String dataDate = dataList.get(1).toString();
        //用于缓存删除
        String clearDate="00000000000000";
        if (dataSrc.equals("0")||dataSrc.equals("10")){
            clearDate=dataDate;
        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
//        MPED_ID:BIGINT,DATA_DATE:STRING,ORG_NO:STRING,WORK_TIME:BIGINT,
// SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(dataList.get(1));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(3), BigDecimal.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        finallyList.add(new Date());
        return finallyList;
    }
}
