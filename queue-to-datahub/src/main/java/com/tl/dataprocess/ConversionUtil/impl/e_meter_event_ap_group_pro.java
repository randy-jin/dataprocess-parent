package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.util.List;

/**
 * 电能表有功组合方式编程事件记录
 * @author wjj
 * @date 2021/12/7 11:07
 * @return
 */
public class e_meter_event_ap_group_pro implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
        return ProcessUtil.template5(dataList, terminalArchivesObject, dataSrc);
    }
}
