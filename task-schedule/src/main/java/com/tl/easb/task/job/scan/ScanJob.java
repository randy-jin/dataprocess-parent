package com.tl.easb.task.job.scan;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.tl.easb.task.manage.AutoTaskManage;

/**
 * @描述 扫描任务
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-21    Administrator
 */
public class ScanJob implements Job {
	
	public void execute(JobExecutionContext jobexecutioncontext)
			throws JobExecutionException {
		
		AutoTaskManage autoTaskManage = AutoTaskManage.getInstance();
		
		// 扫描是否存在新任务
		autoTaskManage.scanAndAssignTask() ;
	}

}
