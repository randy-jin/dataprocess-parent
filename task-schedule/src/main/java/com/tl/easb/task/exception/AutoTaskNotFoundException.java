package com.tl.easb.task.exception;

/**
 * @描述 
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-21    Administrator
 */
public class AutoTaskNotFoundException extends Exception {
	private static final long serialVersionUID = -8164503344007260645L;

	public AutoTaskNotFoundException() {
		// TODO Auto-generated constructor stub
		super("任务表R_AUTOTASK_CONFIG中未找到！");
	}

	public AutoTaskNotFoundException(String taskId) {
		// TODO Auto-generated constructor stub
		super("任务编号为[" + taskId + "] 在表R_AUTOTASK_CONFIG中未找到！");
	}

}
