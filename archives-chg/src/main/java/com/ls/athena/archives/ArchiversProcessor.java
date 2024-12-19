package com.ls.athena.archives;

import com.alibaba.fastjson.JSON;
import com.alicloud.openservices.tablestore.SyncClient;
import com.ls.athena.utils.ArchivesObject;
import com.ls.athena.utils.SaveToRedis;
import com.ls.pf.common.dataCath.bz.service.IOperateBzData;
import org.future.annotate.AutoBond;
import org.future.handler.RunnableTask;
import org.future.pojo.FlowTask;
import org.springframework.jdbc.core.JdbcTemplate;


/**
 * @author Dongwei-Chen
 * @Date 2021/8/25 15:04
 * @Description
 */
public class ArchiversProcessor extends RunnableTask {

    private static final String UPDATE_R_TMNL_PROFILE = " UPDATE R_TMNL_PROFILE SET D_STATUS=? WHERE CHG_OBJ_ID = ? AND D_STATUS='0' AND SHARD_NO = ? limit 1 ";

    @AutoBond
    private IOperateBzData operateBzData;

    @AutoBond
    private SyncClient syncClient;

    /**
     * 档案写入OTS开关
     */
    @AutoBond
    private int WRITE_OTS_ON_OFF;

    //数据核查档案
    private final String DATACHECK_HEADER = "$R_";

    @AutoBond
    private JdbcTemplate jdbcTemplate;


    @Override
    protected FlowTask execute(FlowTask flowTask) throws Exception {
        String jsonString = flowTask.getData();
        ArchivesObject archives = JSON.parseObject(jsonString, ArchivesObject.class);
        String flag = "1";
        SaveToRedis.save(jdbcTemplate, operateBzData,
                syncClient, archives, UPDATE_R_TMNL_PROFILE,
                flag, WRITE_OTS_ON_OFF);
        return null;
    }

    @Override
    protected void tryException(Exception e, FlowTask flowTasks) {
        e.printStackTrace();
    }
}
