package com.tl.subjoin.archives;

import com.alibaba.fastjson.JSON;
import com.ls.pf.common.dataCath.bz.service.IOperateBzData;
import com.tl.subjoin.utils.ArchivesObject;
import com.tl.subjoin.utils.SaveToRedis;
import com.tl.taskcase.AutoInsert;
import com.tl.taskcase.thread.RunnableTask;
import org.springframework.jdbc.core.JdbcTemplate;


public class ArchiversProcessor extends RunnableTask {


    @AutoInsert
    private IOperateBzData operateBzData;



    @AutoInsert
    private JdbcTemplate jdbcTemplate;


    @Override
    protected String doCanFinish(Object jsonString) throws Exception {
        ArchivesObject archives = JSON.parseObject((String) jsonString, ArchivesObject.class);
        SaveToRedis.save(jdbcTemplate, operateBzData, archives);
        return null;
    }

    @Override
    protected void cacheException(Object o, Exception e) {
        e.printStackTrace();
    }
}
