package com.tl.easb.system.listener;

import javax.servlet.ServletContextEvent;


import com.tl.easb.task.thread.SubJobThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 启动采集子任务
 * @author JinZhiQiang
 * @date 2014年4月25日
 */
public class SubJobListener extends AbstractSystemListener {
	private static Logger log = LoggerFactory.getLogger(SubJobListener.class);
	
	@Override
	public void onInit(ServletContextEvent event) {
		// 子任务守护线程
		SubJobThreadPool.startThread();
		log.info("启动子任务守护线程完毕！");
	}

	@Override
	public void onDestory(ServletContextEvent event) {
	}

}
