package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.util.List;

/**
 * 电能表失流事件
 * @author wjj
 * @date 2021/12/7 14:31
 * @return
 */
public class e_meter_event_lose_curr implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,PHASE_FLAG:BIGINT,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
        // ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,
        // ST_GRP_1_R_A:DOUBLE,ST_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,ST_P_A:DOUBLE,ST_Q_A:DOUBLE,
        // ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,
        // ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,
        // ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,ST_P_C:DOUBLE,ST_Q_C:DOUBLE,
        // ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,
        // EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,
        // EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,
        // EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
        return ProcessUtil.template6(dataList, terminalArchivesObject, dataSrc);
    }
}
