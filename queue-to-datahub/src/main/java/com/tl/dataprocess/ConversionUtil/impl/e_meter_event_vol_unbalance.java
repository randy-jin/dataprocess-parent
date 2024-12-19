package com.tl.dataprocess.ConversionUtil.impl;

import com.tl.archives.TerminalArchivesObject;
import com.tl.dataprocess.ConversionUtil.IConversionUtil;
import com.tl.dataprocess.ConversionUtil.ProcessUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 电能表电压不平衡事件记录
 * @author wjj
 * @date 2021/12/8 16:41
 * @return
 */
public class e_meter_event_vol_unbalance implements IConversionUtil {
    @Override
    public List<Object> dataProcess(List<Object> dataList, TerminalArchivesObject terminalArchivesObject, String dataSrc){
        List<Object> finallyList=new ArrayList<>();
        String shardIndex=terminalArchivesObject.getMeterId();
//        String dataDate = dataList.get(1).toString();
        //用于缓存删除,事件类型用默认值
        String clearDate="00000000000000";
//        if (dataSrc.equals("0")||dataSrc.equals("10")){
//            clearDate=dataDate;
//        }
        //占用前2个索引
        finallyList.add(shardIndex);
        finallyList.add(clearDate);
        //METER_ID:BIGINT,INPUT_TIME:TIMESTAMP,ORG_NO:STRING,EVENT_ST:TIMESTAMP,EVENT_ET:TIMESTAMP,
        // MAX_UNBALANCE_RATE:DOUBLE,ST_PAP_R:DOUBLE,ST_RAP_R:DOUBLE,ST_GRP_1_R:DOUBLE,ST_GRP_2_R:DOUBLE,
        // ST_PAP_R_A:DOUBLE,ST_RAP_R_A:DOUBLE,ST_GRP_1_R_A:DOUBLE,STz_GRP_2_R_A:DOUBLE,ST_U_A:DOUBLE,ST_I_A:DOUBLE,
        // ST_P_A:DOUBLE,ST_Q_A:DOUBLE,ST_F_A:DOUBLE,ST_PAP_R_B:DOUBLE,ST_RAP_R_B:DOUBLE,ST_GRP_1_R_B:DOUBLE,
        // ST_GRP_2_R_B:DOUBLE,ST_U_B:DOUBLE,ST_I_B:DOUBLE,ST_P_B:DOUBLE,ST_Q_B:DOUBLE,ST_F_B:DOUBLE,
        // ST_PAP_R_C:DOUBLE,ST_RAP_R_C:DOUBLE,ST_GRP_1_R_C:DOUBLE,ST_GRP_2_R_C:DOUBLE,ST_U_C:DOUBLE,ST_I_C:DOUBLE,
        // ST_P_C:DOUBLE,ST_Q_C:DOUBLE,ST_F_C:DOUBLE,EN_PAP_R:DOUBLE,EN_RAP_R:DOUBLE,EN_GRP_1_R:DOUBLE,
        // EN_GRP_2_R:DOUBLE,EN_PAP_R_A:DOUBLE,EN_RAP_R_A:DOUBLE,EN_GRP_1_R_A:DOUBLE,EN_GRP_2_R_A:DOUBLE,
        // EN_PAP_R_B:DOUBLE,EN_RAP_R_B:DOUBLE,EN_GRP_1_R_B:DOUBLE,EN_GRP_2_R_B:DOUBLE,EN_PAP_R_C:DOUBLE,
        // EN_RAP_R_C:DOUBLE,EN_GRP_1_R_C:DOUBLE,EN_GRP_2_R_C:DOUBLE,SHARD_NO:STRING
        finallyList.add(new BigDecimal(terminalArchivesObject.getMpedId()));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(2), Date.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber());
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(4), Date.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(5), Date.class));//EVENT_ET
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(6), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(7), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(8), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(9), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(10), Double.class));//ST_GRP_2_R
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(11), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(12), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(13), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(14), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(15), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(16), Double.class));//ST_I_A
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(17), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(18), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(19), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(20), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(21), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(22), Double.class));//ST_GRP_1_R_B
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(23), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(24), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(25), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(26), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(27), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(28), Double.class));//ST_F_B
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(29), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(30), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(31), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(32), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(33), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(34), Double.class));//ST_I_C
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(35), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(36), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(37), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(38), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(39), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(40), Double.class));//EN_GRP_1_R
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(41), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(42), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(43), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(44), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(45), Double.class));//EN_GRP_2_R_A
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(46), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(47), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(48), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(49), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(50), Double.class));//EN_PAP_R_C
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(51), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(52), Double.class));
        finallyList.add(ProcessUtil.getDataByObj(dataList.get(53), Double.class));
        finallyList.add(terminalArchivesObject.getPowerUnitNumber().substring(0, 5));
        return finallyList;
    }
}
