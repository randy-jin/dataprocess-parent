package com.tl.easb.task;

public class JobConstants {

	/**
	 * 用于定时任务跟任务做关联时，使用的主键信息，可以通过本主键，从Quartz JobDetail中获取当前Job对应的AutoTaskId
	 */
	public final static String QUARTZ_TASK_KEY = "TASK_KEY";
	/**
	 * 任务状态
	 */
	public final static String QUARTZ_TASK_KEY_TYPE = "TASK_KEY_TYPE";
	
	/**
	 * 任务执行时间
	 */
	public final static String QUARTZ_TASK_RUN_TIME = "TASK_RUN_TIME";
	

	/**
	 * 正常任务的TRIGGER所在分组
	 */
	public final static String QUARTZ_TRIGGER_GROUP_COLLECT = "GROUP_TRIGGER_COLLECT";
	
	/**
	 * 立即执行临时任务的TRIGGER所在分组
	 */
	public final static String QUARTZ_TRIGGER_GROUP_COLLECT_IM_TMP = "GROUP_TRIGGER_COLLECT_IM_TMP";

	/**
	 * 补采任务的TRIGGER所在分组
	 */
	public final static String QUARTZ_TRIGGER_GROUP_RECOLLECT = "GROUP_TRIGGER_RECOLLECT";
	

	/**
	 * 正常任务的JOB所在分组
	 */
	public final static String QUARTZ_JOB_GROUP_COLLECT = "GROUP_JOB_COLLECT";
	
	/**
	 * 立即执行临时任务的JOB所在分组
	 */
	public final static String QUARTZ_JOB_GROUP_COLLECT_IM_TMP = "GROUP_JOB_COLLECT_IM_TMP";

	/**
	 * 补采任务的JOB所在分组
	 */
	public final static String QUARTZ_JOB_GROUP_RECOLLECT = "GROUP_JOB_RECOLLECT";
	
	/**
	 * 任务类型
	 */
	public final static String TASK_TYPE = "TASK_TYPE";

	/**
	 * 任务状态 正常
	 */
	public final static char TASK_TYPE_COMMON = '1';

	/**
	 * 任务状态 立即执行（临时）
	 */
	public final static char TASK_TYPE_IMMEDIATELY = '2';

	/**
	 * 任务状态 补采
	 */
	public final static char TASK_TYPE_REDO = '3';

	/**
	 * 任务状态 全部
	 */
	public final static char TASK_TYPE_ALL = '0';

	/**
	 * 任务状态 数据招测
	 */
	public final static char TASK_TYPE_SJZC = '4';
	/**
	 * 任务状态 参数设置
	 */
	public final static char TASK_TYPE_CSSZ = '5';
	/**
	 * 任务状态 招测事件
	 */
	public static final char TASK_TYPE_ZCSJ = '6';
	/**
	 * 任务状态 终端主动上报
	 */
	public static final char TASK_TYPE_ZDSB = '7';
	/**
	 * 任务状态 招测采集点
	 */
	public static final char TASK_TYPE_ZCCP = '8';
	/**
	 * 任务状态 重点用户管理
	 */
	public static final char TASK_TYPE_ZDYH = '9';
	/**
	 * 默认任务执行所在组名称
	 */
	public final static String TASK_DEFAULT_GROUP = "TASK_DEFAULT_GROUP";
	/**
	 * 默认任务执行名称
	 */
	public final static String TASK_DEFAULT_NAME = "TASK_DEFAULT_NAME";

	/**
	 * 检测任务执行所在组名称
	 */
	public final static String TASK_DETECTION_GROUP = "TASK_DETECTION_GROUP";
	/**
	 * 检测任务执行名称
	 */
	public final static String TASK_DETECTION_NAME = "TASK_DETECTION_NAME";

	/**
	 * 补采次数
	 */
	public final static String TASK_REDO_COUNT = "TASK_REDO_COUNT";

	/**
	 * 补采任务总量
	 */
	public final static String TASK_REDO_SUM = "TASK_REDO_SUM";

	/**
	 * 超时开始时间
	 */
	public final static String TASK_TIMEOUT_STARTTIME = "TASK_TIMEOUT_STARTTIME";

	/**
	 * 当前采集或补采任务未处理数据量
	 */
	public final static String TASK_LAST_SUM = "TASK_LAST_SUM";

	/**
	 * 任务开始执行时间
	 */
	public final static String TASK_START_EXEC_TIME = "TASK_START_EXEC_TIME";

	/**
	 * 要采集的数据日期
	 */
	public final static String TASK_COL_DATA_DATE = "TASK_COL_DATA_DATE";
	
	/**
	 * 通用存储过程自动任务安排
	 */
	public final static String TASK_PROC_DB_GROUP = "TASK_PROC_DB_GROUP";
	
	/**
	 * 通用任务立即执行安排
	 */
	public final static String TASK_PROC_DB_IM_GROUP = "TASK_PROC_DB_IM_GROUP";
	
	
	/**
	 * 执行周期
	 */
	public final static int TASK_RUN_CYCLE_YEAR = 1;
	public final static int TASK_RUN_CYCLE_MON = 2;
	public final static int TASK_RUN_CYCLE_WEEK = 3;
	public final static int TASK_RUN_CYCLE_DAY = 4;
	public final static int TASK_RUN_CYCLE_HOUR = 5;
	public final static int TASK_RUN_CYCLE_MIN_30 = 6;
	public final static int TASK_RUN_CYCLE_MIN_15 = 7;
	public final static int TASK_RUN_CYCLE_MIN_5 = 8;
	public final static int TASK_RUN_CYCLE_MIN_1 = 9;
}
