package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.util.List;

/**
 * 状态字3
 * @author wjj
 * @date 2021/11/29 9:23
 * @return
 */
public class e_meter_run_status_num3 implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        //MPED_ID:BIGINT,DATA_DATE:STRING,ORG_NO:STRING,STATUS:STRING,SHARD_NO:STRING,INSERT_TIME:TIMESTAMP
        return ProcessUtil.template1(dataList,terminalArchivesObject,dataSrc);
    }
}
