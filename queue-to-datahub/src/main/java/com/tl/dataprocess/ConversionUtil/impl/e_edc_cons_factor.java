package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.util.List;

/**
 * 总功率因数
 * @author wjj
 * @date 2021/11/29 17:20
 * @return
 */
public class e_edc_cons_factor implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        // ID:BIGINT,DATA_DATE:STRING,PHASE_FLAG:STRING,ORG_NO:STRING,I_VALUE:DOUBLE,POINT_TIME:TIMESTAMP,STATUS:STRING,
        // DATA_SRC:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        return ProcessUtil.template3(dataList,terminalArchivesObject,dataSrc);
    }
}
