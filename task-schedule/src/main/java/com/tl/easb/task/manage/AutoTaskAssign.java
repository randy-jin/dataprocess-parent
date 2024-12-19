package com.tl.easb.task.manage;

import com.tl.easb.task.JobConstants;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.task.manage.view.AutoTaskJob;
import com.tl.easb.task.param.ParamConstants;
import com.tl.easb.task.quartz.calendar.CronBuilder;
import com.tl.easb.utils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @author lizhenming
 * @version 1.0
 * @描述 自动任务分配
 * @history: 修改时间         修改人                   描述
 * 2014-2-21    Administrator
 */
public class AutoTaskAssign {
    private static Logger logger = LoggerFactory.getLogger(AutoTaskAssign.class);

    private AutoTaskConfig taskConfig = null;

    private boolean isRunImmediately = false;

    /**
     * 当前所执行的任务
     */
    private List<AutoTaskJob> currentTaskJob = new ArrayList<AutoTaskJob>();

    /**
     * 如果存在立即执行的任务，则生成立即执行任务
     */
    private AutoTaskJob immeTaskJob = null;

    /**
     * @param _taskConfig
     * @param _isRunImmediately 是否立即执行
     */
    public AutoTaskAssign(AutoTaskConfig _taskConfig, boolean _isRunImmediately) {
        taskConfig = _taskConfig;
        isRunImmediately = _isRunImmediately;
    }

    /**
     * 任务解析
     */
    public void parse() {
        if (currentTaskJob.size() != 0)
            return;
        List<Trigger> triggers = buildTaskTrigger();
        for (Trigger trigger : triggers) {
            Object obj = trigger.getJobDataMap().get(JobConstants.QUARTZ_TASK_RUN_TIME);
            String runTime = null;
            if (taskConfig.getRunCycle() == JobConstants.TASK_RUN_CYCLE_DAY) {
                if (null != obj) {
                    runTime = String.valueOf(obj);
                }
            }
            JobDetail jobDetail = buildTaskJobDetail(runTime);
            currentTaskJob.add(new AutoTaskJob(trigger, jobDetail, this.taskConfig, JobConstants.TASK_TYPE_COMMON));
        }
        if (isRunImmediately) {
            parseImmediateJob();
        }
    }

    private void parseImmediateJob() {
        if (immeTaskJob != null)
            return;
        JobDetail tmpJobDetail = buildTaskImmidiatelyJobDetail();
        Trigger tmpTrigger = buildTaskImmidiatelyTrigger();
        immeTaskJob = new AutoTaskJob(tmpTrigger, tmpJobDetail, this.taskConfig, JobConstants.TASK_TYPE_IMMEDIATELY);
    }

    /**
     * 安排任务
     *
     * @throws SchedulerException
     */
    public void assignTask() throws SchedulerException {
        this.parse();
        for (AutoTaskJob autoTaskJob : this.currentTaskJob) {
            autoTaskJob.removeJob();
        }

        for (AutoTaskJob autoTaskJob : this.currentTaskJob) {
            autoTaskJob.scheduleJob();
        }
    }

    /**
     * 安排立即执行任务
     *
     * @throws SchedulerException
     */
    public void assignRunImidiatlyTask() throws SchedulerException {
        // this.parse();
        this.parseImmediateJob();
        this.immeTaskJob.scheduleJob();
    }

    /**
     * 安排补采任务
     *
     * @throws SchedulerException
     */
    public void assignRedoTask(int hasRedoCount) throws SchedulerException {
        logger.info("开始-补采任务autoTaskId:[" + this.taskConfig.getAutoTaskId() + "],补采次数：【" + hasRedoCount + "】安排.....");
        SimpleTrigger t = this.createSimpleTrigger();
        putJobData(t, JobConstants.TASK_REDO_COUNT, hasRedoCount + "");
        t.setName(this.getRedoTriggerName(null));
        t.setGroup(JobConstants.QUARTZ_TRIGGER_GROUP_RECOLLECT);
        t.setRepeatCount(0);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, this.taskConfig.getRetryInterval() * 60);
        // cal.add(Calendar.SECOND,1) ;
        t.setStartTime(cal.getTime());

        JobDetail job = new JobDetail(this.getRedoJobName(null), JobConstants.QUARTZ_JOB_GROUP_RECOLLECT, this.getJobClass("TASK.SCHEDULE.JOB.RODO.CLASS"));

        AutoTaskJob reDoJobTask = new AutoTaskJob(t, job, this.taskConfig, JobConstants.TASK_TYPE_REDO);
        reDoJobTask.scheduleJob();
        logger.info("完成-补采任务autoTaskId:[" + this.taskConfig.getAutoTaskId() + "],补采次数：【" + hasRedoCount + "】安排");
    }

    /**
     * 更新任务
     *
     * @throws SchedulerException
     */
    public void updateTask() throws SchedulerException {
        this.parse();
        try {
            this.removeTask();
            this.assignTask();
        } catch (SchedulerException e) {
            logger.error("更新job异常@AutoTaskAsigner.updateTask,autoTaskId:" + this.taskConfig.getAutoTaskId(), e);
            throw e;
        }
    }

    /**
     * 删除任务
     *
     * @throws SchedulerException
     */
    public void removeTask() throws SchedulerException {
        try {
            QuartzManager.removeJobByTaskId(this.taskConfig.getAutoTaskId(), JobConstants.TASK_TYPE_ALL);
        } catch (SchedulerException e) {
            logger.error("移除job异常@AutoTaskAsigner.removeTask,autoTaskId:" + this.taskConfig.getAutoTaskId(), e);
            throw e;
        }
    }

    /**
     * 创建JobDetail
     *
     * @return
     */
    private JobDetail buildTaskJobDetail(String runTime) {
        JobDetail job = new JobDetail(getJobName(runTime), JobConstants.QUARTZ_JOB_GROUP_COLLECT, this.getJobClass("TASK.SCHEDULE.JOB.COMMON.CLASS"));
        return job;
    }

    /**
     * 如果立即执行，创建临时立即执行的任务
     *
     * @return
     */
    private JobDetail buildTaskImmidiatelyJobDetail() {
        JobDetail job = new JobDetail(getJobName(null), JobConstants.QUARTZ_JOB_GROUP_COLLECT_IM_TMP, this.getJobClass("TASK.SCHEDULE.JOB.IMMEDIATELY.CLASS"));
        return job;
    }

    /**
     * 任务安排
     * <p>
     * 是否立即执行
     */
    private Trigger buildTaskImmidiatelyTrigger() {
        SimpleTrigger t = createSimpleTrigger();
        t.setGroup(JobConstants.QUARTZ_TRIGGER_GROUP_COLLECT_IM_TMP);
        t.setName(getTriggerName(null) + "_IMD");
        t.setRepeatCount(0);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, ParamConstants.TASK_SCHEDULE_NEWJOB_START_INTERVEL);
        t.setStartTime(cal.getTime());
        return t;
    }

    /**
     * 创建任务相关的Trigger
     *
     * @return
     */
    public List<CronTrigger> buildTaskCronTrigger() {
        List<CronTrigger> cronTriggers = buildTaskCronTriger();
        return cronTriggers;
    }

    /**
     * 创建任务相关的Trigger
     *
     * @return
     */
    public List<Trigger> buildTaskTrigger() {
        //		int runCycle = this.taskConfig.getRunCycle();
        List<Trigger> triggers = new ArrayList<Trigger>();
        /**
         * add by zhangwei@2013年11月12日
         * ，由于按分钟、小时等可以选择周几执行，在解析时不再使用SimpleTrigger，统一使用cronTrigger
         **/
        List<CronTrigger> cronTriggers = buildTaskCronTriger();
        for (Trigger trigger : cronTriggers) {
            triggers.add(trigger);
        }
        return triggers;

        /***
         * 注释by zhangwei@2013年11月12日 switch (runCycle) { case 1: // 年 case 2: //
         * 月 case 3: // 周 case 4: // 日 case 5: // 时 trigger =
         * buildTaskCronTriger() ; break; case 6: // 30分 trigger =
         * buildTaskSimpleTriger(30) ; break; case 7: // 15分 trigger =
         * buildTaskSimpleTriger(15) ; break; case 8: // 5分 trigger =
         * buildTaskSimpleTriger(5) ; break; case 9: // 1分 trigger =
         * buildTaskSimpleTriger(1) ; break; }
         ***/
    }

    /**
     * 如果是年、月、日、周的任务，创建CronTriger
     *
     * @return
     */
    private List<CronTrigger> buildTaskCronTriger() {
        List<CronTrigger> cronTriggers = new ArrayList<CronTrigger>();
        List<CronExpression> cronExpression = CronBuilder.getCronExpresion(taskConfig);
        for (CronExpression expression : cronExpression) {
            CronTrigger t = createCronTrigger(expression);
            t.setCronExpression(expression);
            t.setGroup(JobConstants.QUARTZ_TRIGGER_GROUP_COLLECT);
            String time = null;
            if (taskConfig.getRunCycle() == JobConstants.TASK_RUN_CYCLE_DAY) {
                time = getTime(expression);
            }
            t.setName(getTriggerName(time));
            cronTriggers.add(t);
        }

        return cronTriggers;
    }

    /**
     * 如果是时及更小粒度的任务，创建SimpleTriger xx秒之后执行
     *
     * @param repeatMin 每*分钟
     * @return
     */
    @SuppressWarnings("unused")
    private SimpleTrigger buildTaskSimpleTriger(int repeatMin) {
        SimpleTrigger t = createSimpleTrigger();
        t.setGroup(JobConstants.QUARTZ_TRIGGER_GROUP_COLLECT);
        t.setName(getTriggerName(null));
        t.setRepeatCount(SimpleTrigger.REPEAT_INDEFINITELY);
        Calendar cal = Calendar.getInstance();
        // mod by zhangwei 修改小粒度任务开始执行时间为间隔时间；
        cal.add(Calendar.SECOND, repeatMin * 60);
        t.setStartTime(cal.getTime());
        t.setRepeatInterval(repeatMin * 1000 * 60);
        return t;
    }

    private String getTriggerName(String runTime) {
        return getName("TRIG_", runTime);
    }

    private String getRedoTriggerName(String runTime) {
        return getName("TRIG_R_", runTime);
    }

    private String getJobName(String runTime) {
        return getName("JOB_", runTime);
    }

    private String getRedoJobName(String runTime) {
        return getName("TRIG_R_", runTime);
    }

    public String getDetectionTriggerName(String runTime) {
        return getName("TRIG_DET_", runTime);
    }

    public String getDetectionJobName(String runTime) {
        return getName("JOB_DET_", runTime);
    }

    private String getName(String prtype, String runTime) {
        int runCycle = this.taskConfig.getRunCycle();
        String type = "";
        switch (runCycle) {
            case 1: // 年
                type = "YR";
                break;
            case 2: // 月
                type = "MON";
                break;
            case 3: // 周
                type = "WK";
                break;
            case 4: // 日
                type = "DAY";
                break;
            case 5: // 时
                type = "HOUR";
                break;
            case 6: // 30分
                type = "MIN";
                break;
            case 7: // 15分
                type = "MIN";
                break;
            case 8: // 5分
                type = "MIN";
                break;
            case 9: // 1分
                type = "MIN";
                break;
        }
        type = type + "_";
        String name = null;
        if (null != runTime) {
            name = prtype + type + taskConfig.getAutoTaskId() + "_" + runTime;
        } else {
            name = prtype + type + taskConfig.getAutoTaskId();
        }
        return name;

    }

    /**
     * 创建CronTrigger
     *
     * @return
     */
    private CronTrigger createCronTrigger(CronExpression expression) {
        String time = getTime(expression);
        CronTrigger t = new CronTrigger();
        setTriggerMetaData(t, time);
        return t;
    }

    private String getTime(CronExpression expression) {
        String express = expression.getCronExpression();
        //0 30 1 ? * * *
        //0 0/15 * ? * * *
        String[] strs = express.replace(" ", ",").split(",");
        int hour = 0;
        int min = 0;
        try {
            hour = Integer.parseInt(strs[2]);
            min = Integer.parseInt(strs[1]);
        } catch (Exception e) {
            return null;
        }
        return hour + ":" + min;
    }

    /**
     * 创建SimpleTrigger
     *
     * @return
     */
    private SimpleTrigger createSimpleTrigger() {
        SimpleTrigger t = new SimpleTrigger();
        setTriggerMetaData(t, null);
        return t;
    }

    /**
     * 根据配置文件，获取类实例
     *
     * @param type
     * @return
     */
    private Class<?> getJobClass(String type) {
        String className = PropertyUtils.getProperValue(type);
        if (StringUtils.isBlank(className)) {
            return null;
        } else {
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException e) {
                logger.error("类：[" + className + "]无法找到");
                return null;
            }
        }
    }

    /**
     * 向任务中设置公共属性
     *
     * @param t
     */
    private void setTriggerMetaData(Trigger t, String time) {
        putJobData(t, JobConstants.QUARTZ_TASK_KEY, taskConfig.getAutoTaskId());
        if (null != time) {
            putJobData(t, JobConstants.QUARTZ_TASK_RUN_TIME, time);
        }
    }

    private void putJobData(Trigger t, String key, Object value) {
        t.getJobDataMap().put(key, value);
    }

    public AutoTaskConfig getTaskConfig() {
        return taskConfig;
    }

    public void setTaskConfig(AutoTaskConfig taskConfig) {
        this.taskConfig = taskConfig;
    }

    public boolean isRunImmediately() {
        return isRunImmediately;
    }

    public void setRunImmediately(boolean isRunImmediately) {
        this.isRunImmediately = isRunImmediately;
    }

    public List<AutoTaskJob> getCurrentTaskJob() {
        return currentTaskJob;
    }

    public void setCurrentTaskJob(List<AutoTaskJob> currentTaskJob) {
        this.currentTaskJob = currentTaskJob;
    }

    public AutoTaskJob getImmeTaskJob() {
        return immeTaskJob;
    }

    public void setImmeTaskJob(AutoTaskJob immeTaskJob) {
        this.immeTaskJob = immeTaskJob;
    }

}