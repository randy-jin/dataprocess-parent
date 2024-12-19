package com.tl.subjoin.utils;

import com.tl.taskcase.SystemInitHandler;
import com.tl.taskcase.pojo.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;


@Component
public class TaskLoadConfig implements ApplicationRunner {

    @Value("${task.size}")
    private int coreSize;

    @Value("${task.queueName}")
    private String queueName;

    @Value("${task.runnable}")
    private String runnable;

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
            Task task = new Task();
            task.setThreads(coreSize);
            task.setRunnable(runnable);
            task.setRedisOn(true);
            task.setQueue(queueName + "_" + shardNo);
            SystemInitHandler.addTask(task);
        }
    }

}
