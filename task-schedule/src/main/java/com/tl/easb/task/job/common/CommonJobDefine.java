package com.tl.easb.task.job.common;

/**
 * 通用任务常量定义
 * @author JinZhiQiang
 * @date 2014年4月15日
 */
public class CommonJobDefine {
	
	/**
	 * 任务执行方式：定时执行
	 */
	public static final int RUN_MODE_TIMER = 0;
	
	/**
	 * 任务执行方式：自动任务完成后执行
	 */
	public static final int RUN_MODE_AFTER_AUTOTASK = 1;
	
	/**
	 * 任务执行方式：其他存储过程完成后执行
	 */
	public static final int RUN_MODE_AFTER_PROCEDURE = 2;
	
}
