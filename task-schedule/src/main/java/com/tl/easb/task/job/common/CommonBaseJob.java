package com.tl.easb.task.job.common;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * quartz任务执行基类
 * @author JinZhiQiang
 * @date 2014年4月25日
 */
public abstract class CommonBaseJob implements Job {

	private String cronExpression;
	protected String params;
	private String triggerName;
	protected String spId;
	protected String pdId;
	private String className;
	private String methodName;
	protected String dataDate;
	
	public String getDataDate() {
		return dataDate;
	}

	public void setDataDate(String dataDate) {
		this.dataDate = dataDate;
	}

	public String getPdId() {
		return pdId;
	}

	public void setPdId(String pdId) {
		this.pdId = pdId;
	}

	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getSpId() {
		return spId;
	}

	public void setSpId(String spId) {
		this.spId = spId;
	}

	public void execute(JobExecutionContext ctx) throws JobExecutionException {
		this.setParams(ctx.getTrigger().getJobDataMap().getString("params"));
		this.setSpId(ctx.getTrigger().getJobDataMap().getString("SP_ID"));
		this.setPdId(ctx.getTrigger().getJobDataMap().getString("PD_ID"));
		this.setTriggerName(ctx.getTrigger().getJobDataMap().getString("triggerName"));
		this.setDataDate(ctx.getTrigger().getJobDataMap().getString("DATA_DATE"));
		this.onExecute();
	}

	/***
	 * 执行本次任务
	 */
	protected abstract void onExecute();

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public String getParams() {
		return params;
	}

	public void setParams(String params) {
		this.params = params;
	}

	public String getTriggerName() {
		return triggerName;
	}

	public void setTriggerName(String triggerName) {
		this.triggerName = triggerName;
	}

}
