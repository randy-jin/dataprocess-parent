package com.tl.easb.task.quartz.calendar;
/**
 * 
 * @author Administrator
 * 解析任务时，如果是分钟的任务，则报错
 */
public class CronParseException extends RuntimeException {
	
	private static final long serialVersionUID = 8956855352756358058L;
	public CronParseException() {
		// TODO Auto-generated constructor stub
		super("本任务是分钟任务，不能生成Cron表达式！");
	}
	public CronParseException(String message) {
		// TODO Auto-generated constructor stub
		super(message);
	}
}
