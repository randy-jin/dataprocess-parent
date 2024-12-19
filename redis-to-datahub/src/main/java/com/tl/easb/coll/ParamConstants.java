package com.tl.easb.coll;


import com.tl.dataprocess.utils.PropertyUtils;

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
     * 监控子任务定时扫描每条记录单位时间（毫秒）
     */
    public static final int TASK_FINISH_TIMEOUT = PropertyUtils.getIntValue("TASK.FINISH.TIMEOUT", 60000);

    /**
     * 向历史表中写人采集失败的测点信息，每次写入的子任务数
     */
    public static final int TASK_WRITE_R_AUTOTASK_HISTORY_STEP_LENGTH = PropertyUtils.getIntValue("TASK.WRITE.R_AUTOTASK_HISTORY.STEP.LENGTH", 10000);

    /**
     * 子任务守护线程数
     */
    public static final int TASK_SUBTASK_THREAD_SIZE = PropertyUtils.getIntValue("TASK.SUBTASK.THREAD.SIZE", 1);

    /**
     * 报文监控读取监控消息线程数
     */
    public static final int MSG_MONITOR_THREAD_SIZE = PropertyUtils.getIntValue("MSG.MONITOR.THREAD.SIZE", 1);

    /**
     * 失败信息写入历史表--线程数
     */
    public static final int TASK_WRITE_TO_HISTORY_THREAD_POOL_SIZE = PropertyUtils.getIntValue("TASK.WRITE.TO.HISTORY.THREAD.POOL.SIZE", 1);

    /**
     * 采集失败的信息批量入库量
     */
    public static final int TASK_FAIL_PERSISTENCE_STEP = PropertyUtils.getIntValue("TASK.FAIL.PERSISTENCE.STEP", 5000);

    /**
     * 事件推送--每次监控休眠时长（毫秒）
     */
    public static final int EVENT_PUSH_SLEEP = PropertyUtils.getIntValue("EVENT.PUSH.SLEEP", 5000);

    /**
     * 信息推送--每次监控休眠时长（毫秒）
     */
    public static final int INFO_PUSH_SLEEP = PropertyUtils.getIntValue("INFO.PUSH.SLEEP", 1000 * 60 * 5);

    /**
     * 事件推送--线程池大小
     */
    public static final int EVENT_PUSH_THREAD_SIZE = PropertyUtils.getIntValue("EVENT.PUSH.THREAD.SIZE", 1);

    /**
     * 信息推送--线程池大小
     */
    public static final int INFO_PUSH_THREAD_SIZE = PropertyUtils.getIntValue("INFO.PUSH.THREAD.SIZE", 1);

    /**
     * 信息推送--线程池大小
     */
    public static final int COST_CTRL_THREAD_SIZE = PropertyUtils.getIntValue("COST.CTRL.THREAD.SIZE", 500);

    /**
     * 费控检测工单异常--每次监控休眠时长（毫秒）
     */
    public static final int COST_CTRL_CHECK_ABNORMAL_ORDERS_SLEEP = PropertyUtils.getIntValue("COST.CTRL.CHECK.ABNORMAL.ORDERS.SLEEP", 5000);

    /**
     * 费控处理待办工单--每次监控休眠时长（毫秒）
     */
    public static final int COST_CTRL_DISPOSE_TO_DO_ORDERS_SLEEP = PropertyUtils.getIntValue("COST.CTRL.DISPOSE.TO_DO.ORDERS.SLEEP", 5000);

    /**
     * 费控  待返回营销工单一次性取得的数目
     */
    public static final int NUM_TO_DO_ORDERS = PropertyUtils.getIntValue("CTRL.NUM.TO_DO.ORDERS", 500);

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
     * 存储过程执行指定datasource名称
     */
    public static int PROCEDURE_RUN_TIMEOUT = PropertyUtils.getIntValue("PROCEDURE.RUN.TIMEOUT", 360);

    /**
     * 更新超时时间忽略数
     */
    public static final int TASK_FINISH_TIMEOUT_IGNORE = PropertyUtils.getIntValue("TASK.FINISH.TIMEOUT.IGNORE", 50);

}
