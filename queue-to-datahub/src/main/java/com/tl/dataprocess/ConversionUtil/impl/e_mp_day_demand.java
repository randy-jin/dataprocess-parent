package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.util.List;

/**
 * (上1结算日)正向有功总最大需量及发生时间
 * @author wjj
 * @date 2021/11/29 17:20
 * @return
 */
public class e_mp_day_demand implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        // ID:BIGINT,DATA_DATE:STRING,PHASE_FLAG:STRING,ORG_NO:STRING,I_VALUE:DOUBLE,POINT_TIME:TIMESTAMP,STATUS:STRING,
        // DATA_SRC:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        return ProcessUtil.template4(dataList,terminalArchivesObject,dataSrc);
    }
}
