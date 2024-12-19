package com.tl.easb.task.job.imm;

import com.tl.easb.task.JobConstants;
import com.tl.easb.task.job.BaseJob;

/**
 * @描述 立即采集任务 
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-22    Administrator
 */
public class ImmCollectJob extends BaseJob {

	@Override
	public char getType() {
		return JobConstants.TASK_TYPE_IMMEDIATELY;
	}

}
