package com.tl.easb.task.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.tl.easb.task.handle.subtask.SubTaskExec;
import com.tl.easb.task.param.ParamConstants;

/**
 * @描述 子任务监听
 * @author jinzhiqiang
 * @version 1.0
 * @history: 修改时间 修改人 描述 2014-2-27 Administrator
 */
public class SubJobThreadPool {
	private static int subTaskThreadPoolSize = ParamConstants.TASK_SUBTASK_THREAD_SIZE;

	//接收后台定时任务线程池
	private static  ExecutorService subTaskThreadPool= null;

	static {
		//读取配置文件 线程数目
		subTaskThreadPool = Executors.newFixedThreadPool(subTaskThreadPoolSize,new TaskThreadPool("SubJobThreadPool"));
	}
	
	// 对外接口启动线程
	public static void startThread() {
		for (int i = 0; i < subTaskThreadPoolSize; i++) {
			subTaskThreadPool.execute(
					new Runnable() {
						public void run() {
							SubTaskExec.execute();
						}
					});
		}
		subTaskThreadPool.shutdown();
	}
}
