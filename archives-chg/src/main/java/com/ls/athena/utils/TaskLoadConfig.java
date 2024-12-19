package com.ls.athena.utils;

import org.future.datasource.impl.multi.manager.Manager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

/**
 * @author Dongwei-Chen
 * @Date 2021/8/26 17:19
 * @Description 动态提交任务
 */
@Component
public class TaskLoadConfig implements ApplicationRunner {

    @Value("${task.queueName}")
    private String queueName;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        String sql = "SELECT distinct ORG_NO FROM O_ORG WHERE ORG_TYPE='03'";
        //查数据库  获取ShardNo并根据ShardNo启动Task
        SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(sql);
        while (sqlRowSet.next()) {
            String shardNo = sqlRowSet.getString(1);
            if (shardNo == null || "".equals(shardNo)) {
                continue;
            }
            //将任务提交到 线程容器
            Manager.watchQueue(queueName + "_" + shardNo);
        }
    }

}
