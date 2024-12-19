package com.tl.easb.task.manage.view;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

import com.tl.easb.task.JobConstants;
import com.tl.easb.task.manage.QuartzManager;

/**
 * @描述 
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-21    Administrator
 */
public class AutoTaskJob {
	private Trigger trigger;
	private JobDetail jobDetail;
	private boolean hasschedule = false;
	private AutoTaskConfig taskConfig = null;
	private JobDataMap jobDataMap = null;
	private char jobType;

	public AutoTaskJob(Trigger _trigger, JobDetail _jobDetail, AutoTaskConfig _taskConfig, char _jobType) {
		trigger = _trigger;
		jobDetail = _jobDetail;
		taskConfig = _taskConfig;
		jobType = _jobType;
		// 设置相关数据，任务状态
		trigger.getJobDataMap().put(JobConstants.QUARTZ_TASK_KEY_TYPE, _jobType + "");
		// 设置相关数据，采集数据日期
		trigger.getJobDataMap().put(JobConstants.TASK_COL_DATA_DATE, taskConfig.getDataDate());
	}

	public Trigger getTrigger() {
		return trigger;
	}

	public JobDetail getJobDetail() {
		return jobDetail;
	}

	public AutoTaskConfig getTaskConfig() {
		return taskConfig;
	}

	public void setTaskConfig(AutoTaskConfig taskConfig) {
		this.taskConfig = taskConfig;
	}

	/**
	 * 安排新任务（手工指定Schedule)
	 * 
	 * @param s
	 * @throws SchedulerException
	 */
	public void scheduleJob(Scheduler s) throws SchedulerException {
		if (!hasschedule) {
			QuartzManager.scheduleJob(jobDetail, trigger);
			hasschedule = true;
		}
	}

	/**
	 * 安排新任务（自动从当前Context中获取Schedule)
	 * 
	 * @param s
	 * @throws SchedulerException
	 */
	public void scheduleJob() throws SchedulerException {
		QuartzManager.scheduleJob(this);
	}

	/**
	 * 删除旧任务
	 * @throws SchedulerException
	 */
	public void removeJob() throws SchedulerException {
		QuartzManager.removeJob(this);
	}

	public JobDataMap getJobDataMap() {
		return jobDataMap;
	}

	public void setJobDataMap(JobDataMap jobDataMap) {
		this.jobDataMap = jobDataMap;
	}

	/**
	 * 获取任务类型<br>
	 * 1.正常任务 2.立即执行的临时任务 3.补采任务
	 * 
	 * @return
	 */
	public char getJobType() {
		return jobType;
	}

	/**
	 * 设置任务类型<br>
	 * TASK_TYPE_COMMON.正常任务<br>
	 * TASK_TYPE_IMMEDIATELY.立即执行的临时任务<br>
	 * TASK_TYPE_REDO.补采任务
	 * 
	 * @return
	 */
	public void setJobType(char jobType) {
		this.jobType = jobType;
	}

	/**
	 * 是否常用任务
	 * 
	 * @return
	 */
	public boolean isCommonJob() {
		return JobConstants.TASK_TYPE_COMMON == (this.jobType);
	}

	/**
	 * 是否补采任务
	 * 
	 * @return
	 */
	public boolean isRedoJob() {
		return JobConstants.TASK_TYPE_REDO == (this.jobType);
	}

	/**
	 * 是否立即执行任务
	 * 
	 * @return
	 */
	public boolean isImmediatelyJob() {
		return JobConstants.TASK_TYPE_IMMEDIATELY == (this.jobType);
	}

}
