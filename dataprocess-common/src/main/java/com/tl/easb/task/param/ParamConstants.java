package com.tl.easb.task.param;


import com.tl.easb.utils.PropertyUtils;

public class ParamConstants {

    /**
     * 新任务安排后，开始启动时间间隔
     */
    public static final int TASK_SCHEDULE_NEWJOB_START_INTERVEL = PropertyUtils.getIntValue("TASK.SCHEDULE.NEW.JOB.START.INTERVEL", 10);
    /**
     * 监控子任务定时扫描最小间隔时间（毫秒）
     */
    public static final int TASK_DETECTION_INTERVAL = PropertyUtils.getIntValue("TASK.DETECTION.INTERVAL", 30000);

    /**
     * 监控子任务定时扫描每条记录单位时间（毫秒）
     */
    public static final int TASK_DETECTION_UNITTIME = PropertyUtils.getIntValue("TASK.DETECTION.UNITTIME", 10);

    /**
     * 监控子任务定时扫描每条记录应采总数阈值
     */
    public static final int TASK_FINISH_TOTAL = PropertyUtils.getIntValue("TASK.FINISH.TOTAL", 1000);

    /**
     * 监控子任务定时扫描每条记录单位时间（毫秒）
     */
    public static final int TASK_FINISH_TIMEOUT = PropertyUtils.getIntValue("TASK.FINISH.TIMEOUT", 60000);
    /**
     * 监控子任务定时扫描每条记录单位时间（毫秒） 透召任务
     */
    public static final int TASK_TC_FINISH_TIMEOUT = PropertyUtils.getIntValue("TASK.TC.FINISH.TIMEOUT", 60000);
    /**
     * 向历史表中写人采集失败的测点信息，每次写入的子任务数
     */
    public static final int TASK_WRITE_R_AUTOTASK_HISTORY_STEP_LENGTH = PropertyUtils.getIntValue("TASK.WRITE.R_AUTOTASK_HISTORY.STEP.LENGTH", 10000);

    /**
     * 子任务守护线程数
     */
    public static final int TASK_SUBTASK_THREAD_SIZE = PropertyUtils.getIntValue("TASK.SUBTASK.THREAD.SIZE",1);


    /**
     * 采集失败的信息批量入库量
     */
    public static final int TASK_FAIL_PERSISTENCE_STEP = PropertyUtils.getIntValue("TASK.FAIL.PERSISTENCE.STEP", 5000);


    /**
     * 删除redis缓存中的测点信息，每次删除量
     */
    public static final int REDIS_DELETE_MP_STEP_LENGTH = PropertyUtils.getIntValue("REDIS.DELETE.MP.STEP.LENGTH", 10000);

    /**
     * 子任务每次监控休眠时长（毫秒）
     */
    public static final int TASK_SUBTASK_SLEEP = PropertyUtils.getIntValue("TASK.SUBTASK.SLEEP", 10000);

    /**
     * 子任务每次监控休眠时长（毫秒）
     */
    public static final int TASK_SUBTASK_NOTASK_SLEEP = PropertyUtils.getIntValue("TASK.SUBTASK.NOTASK.SLEEP", 5000);

    /**
     * 档案同步线程休眠时长（毫秒）
     */
    public static final int RECORDS_SYNC_SLEEP = PropertyUtils.getIntValue("RECORDS.SYNC.SLEEP", 30000);

    /**
     * 获取redis缓存中的子任务信息，每次获取量
     */
    public static final int REDIS_GAIN_SUBTASK_STEP_LENGTH = PropertyUtils.getIntValue("REDIS.GAIN.SUBTASK.STEP.LENGTH", 1000);

    /**
     * 删除redis缓存中的二级任务与测点信息，每次删除量
     */
    public static final int REDIS_DELETE_SUBTASK_STEP_LENGTH = PropertyUtils.getIntValue("REDIS.DELETE.SUBTASK.STEP.LENGTH", 10000);

    /**
     * 存储过程执行指定datasource名称
     */
    public static String PROCEDURE_DATASOURCE_NAME = PropertyUtils.getProperValue("PROCEDURE.DATASOURCE.NAME", "procedure");
    /**
     * 存储过程执行超时时间
     */
    public static int PROCEDURE_RUN_TIMEOUT = PropertyUtils.getIntValue("PROCEDURE.RUN.TIMEOUT", 360);

    /**
     * 更新超时时间忽略数
     */
    public static final int TASK_FINISH_TIMEOUT_IGNORE = PropertyUtils.getIntValue("TASK.FINISH.TIMEOUT.IGNORE", 50);

    /**
     * 前置透传队列数量
     */
    public static int QZ_TC_NUM = PropertyUtils.getIntValue("QZ.TC.NUM", 6);
    /**
     * 前置预抄队列数量
     */
    public static int QZ_YC_NUM = PropertyUtils.getIntValue("QZ.YC.NUM", 1);
    /**
     * 预抄下行时,单个终端连续两包下发时间间隔不足2秒时,放入阻塞队列的长度限制
     */
    public static int TMNL_BATCH_QUEUE_COUNT = PropertyUtils.getIntValue("TMNL.BATCH.QUEUE.COUNT", 10000);

    /**
     * 预抄下行时,针对单个终端连续两包的下发时间间隔(毫秒)
     */
    public static int TMNL_BATCH_SLEEP = PropertyUtils.getIntValue("TMNL.BATCH.SLEEP", 2000);

    /**
     * 预抄下行时,针对单个终端连续两包的下发队列，下发线程数
     */
    public static int TMNL_BATCH_THREAD_COUNT = PropertyUtils.getIntValue("TMNL.BATCH.THREAD.COUNT", 10);

}
