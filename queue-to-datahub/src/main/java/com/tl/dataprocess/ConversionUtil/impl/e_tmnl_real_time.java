package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 终端实时时钟信息
 * @author wjj
 * @date 2021/11/30 10:10
 * @return
 */
public class e_tmnl_real_time implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getTerminalId();
        //用于缓存删除
        String clearDate="00000000000000";
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);

        //TERMINAL_ID:BIGINT,AREA_CODE:BIGINT,TERMINAL_ADDR:STRING,T_TIME:TIMESTAMP,M_TIME:TIMESTAMP,OVER_TIME:DOUBLE,
        // CON_TIME_NUM:BIGINT,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getTerminalId()));
        finallyList.add(new BigDecimal(terminalArchivesObject.getAreaCode()));
        finallyList.add(terminalArchivesObject.getTerminalAddr());
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(3), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(4), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(5), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(6), BigDecimal.class));
        finallyList.add(new Date());
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }
}
