package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.util.List;

/**
 * 内部时钟电池电压
 * @author wjj
 * @date 2021/11/29 14:41
 * @return
 */
public class e_meter_inside_battery_voltage implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        //MPED_ID:BIGINT,DATA_DATE:STRING,ORG_NO:STRING,STATUS:STRING,SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
        return ProcessUtil.template2(dataList,terminalArchivesObject,dataSrc);
    }
}
