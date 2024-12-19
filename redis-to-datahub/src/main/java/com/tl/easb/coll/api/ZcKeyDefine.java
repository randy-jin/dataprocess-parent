package com.tl.easb.coll.api;
/**
 * 召测计算:REDIS 数据KEY定义
 * @author JerryHuang
 *
 * 2014-2-25 下午8:01:38
 */
public class ZcKeyDefine {
	/**
	 * 测点信息数据集（测点--二级任务对应关系集)唯一标识.
	 */
	public final static String KEY_CP2SUBTASK="CP2SUBTASK";
	/**
	 * 采集任务测点数据集KEY前缀
	 * 
	 */
	public final static String KPREF_SUBTASK2CP="STCP:";
	/**
	 * 一级任务-二级任务关系数据集KEY前缀
	 */
	public final static String KPREF_TASK2SUBTASK="TASK2SUBTASK:";
	
	/**
	 * 可删除的一级任务-二级任务关系数据集KEY前缀
	 */
	public final static String KPREF_RV_TASK2SUBTASK="RV_TASK2SUBTASK:";
	 
	 
	/**
	 * 任务队列偏移量key前缀
	 */
	public final static String KPREF_TASKOFFSET="TKOS:";
	
	 
	/**
	 * 一级任务更新时间
	 */
	public final static String KPREF_TASKUPDTIME="TKUPDTIME:";
	/**
	 * 任务状态数据的数据集KEY
	 */
	public final static String KEY_TASK2STATUS="TASK2STATUS";
	/**
	 * 任务计算器的数据集的KEY
	 */
	public final static String KEY_TASK2COUNTER="TASK2COUNTER";
	
	/**
	 * 统计剩余测量点数执行步长：一次性执行统计多少个终端的剩余测量点数
	 */
	public final static String KEY_LEFTMPCOUNTER_STEP = "TASK_LEFT_MP_COUNTER_STEP:";
}
