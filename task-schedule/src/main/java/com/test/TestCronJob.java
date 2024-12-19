package com.test;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Created by jinzhiqiang on 2020/5/26.
 */
public class TestCronJob {
    public static void main(String[] args) {
//        SchedulerFactory sf = new StdSchedulerFactory();
//        try {
//            // below including instantiate QuartzScheduler,
//            // where quartz QuartzSchedulerThread is instantiated.
//            Scheduler sched = sf.getScheduler();
//            sched.start();
//            JobDetail jd = new JobDetail("myjob", sched.DEFAULT_GROUP, MyJob.class);
//            System.out.println("stateful:" + jd.isStateful());
//            CronTrigger ct = new CronTrigger("JobName", "DEFAULT", "*/10 * * * * ? *");
//            sched.scheduleJob(jd, ct);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        System.out.println(1);
    }
}
