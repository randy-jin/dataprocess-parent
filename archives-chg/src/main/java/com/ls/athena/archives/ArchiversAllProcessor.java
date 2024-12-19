package com.ls.athena.archives;

import com.alibaba.fastjson.JSONObject;
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
 * @Description 全量加载
 */
public class ArchiversAllProcessor extends RunnableTask {

    private static final String UPDATE_R_TMNL_PROFILE_ALL = " UPDATE R_TMNL_PROFILE_ALL SET D_STATUS='2' WHERE CHG_OBJ_ID = ? AND SHARD_NO = ? ";

    @AutoBond
    private JdbcTemplate jdbcTemplate;

    @AutoBond
    private IOperateBzData operateBzData;

    @AutoBond
    private SyncClient syncClient;

    // 档案写入OTS开关
    @AutoBond
    private int WRITE_OTS_ON_OFF;

    //数据核查档案
    private final String DATACHECK_HEADER = "$R_";

    @Override
    protected FlowTask execute(FlowTask flowTask) throws Exception {
        String data = flowTask.getData();

        ArchivesObject archives = JSONObject.parseObject(data, ArchivesObject.class);

        SaveToRedis.save(jdbcTemplate, operateBzData,
                syncClient, archives, UPDATE_R_TMNL_PROFILE_ALL,
                null, WRITE_OTS_ON_OFF);
        return null;
    }

    @Override
    protected void tryException(Exception e, FlowTask flowTasks) {
        e.printStackTrace();
    }
}
