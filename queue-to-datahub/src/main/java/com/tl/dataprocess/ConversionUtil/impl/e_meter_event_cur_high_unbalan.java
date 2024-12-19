package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.util.List;

/**
 * 电能表电流严重不平衡事件记录
 * @author wjj
 * @date 2021/12/8 9:36
 * @return
 */
public class e_meter_event_cur_high_unbalan implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,SHARD_NO:STRING
        return ProcessUtil.template7(dataList, terminalArchivesObject, dataSrc);
    }
}
