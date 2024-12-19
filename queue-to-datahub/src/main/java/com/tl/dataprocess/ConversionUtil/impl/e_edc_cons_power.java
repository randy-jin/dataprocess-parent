package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.util.List;

/**
 * 瞬时有功功率
 * @author wjj
 * @date 2021/11/29 9:24
 * @return
 */
public class e_edc_cons_power implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        // ID:BIGINT,DATA_DATE:STRING,DATA_TYPE:STRING,ORG_NO:STRING,P_VALUE:DOUBLE,POINT_TIME:TIMESTAMP,STATUS:STRING,
        // DATA_SRC:STRING,INSERT_TIME:TIMESTAMP,SHARD_NO:STRING
        return ProcessUtil.template3(dataList,terminalArchivesObject,dataSrc);
    }
}
