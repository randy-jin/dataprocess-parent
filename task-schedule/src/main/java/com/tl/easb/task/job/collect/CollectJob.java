package com.tl.easb.task.job.collect;

import com.tl.easb.task.JobConstants;
import com.tl.easb.task.job.BaseJob;

/**
 * @描述 采集任务 
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-21    Administrator
 */
public class CollectJob extends BaseJob {

	@Override
	public char getType() {
		return JobConstants.TASK_TYPE_COMMON;
	}
}
