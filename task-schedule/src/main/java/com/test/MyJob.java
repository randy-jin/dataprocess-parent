package com.test;


import java.sql.Timestamp;
import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.StatefulJob;

/**
 * Created by jinzhiqiang on 2020/5/26.
 */
public class MyJob implements Job {
    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        System.out.println(new Timestamp(System.currentTimeMillis()) + ",job executed [" + Thread.currentThread().getName() + "]");
        try {
            Thread.sleep(2 * 60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(new Timestamp(System.currentTimeMillis()) + ",job executed [" + Thread.currentThread().getName() + "]");
    }
}
