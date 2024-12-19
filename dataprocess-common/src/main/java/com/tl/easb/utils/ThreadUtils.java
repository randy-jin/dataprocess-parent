package com.tl.easb.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 线程工具类
 * @author jinzhiqiang
 *
 */
public class ThreadUtils {
	private static Logger log = LoggerFactory.getLogger(ThreadUtils.class);
	
	/**
	 * 线程钩子
	 * @param checkThread
	 */
	public static void addShutdownHook(final Thread checkThread) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.info("Receiving shutdown signal,hook starting,checking thread ["+checkThread.getName()+"]'s status");
				// 不断检测一次执行状态，如果线程一直没有执行完毕，超时后，放弃等待 \
				for (int i = 0; i < 60; i++) {
					if (checkThread.getState() == State.TERMINATED) {
						log.info("Thread is finished,hook is exiting...");
						return;
					}
					ThreadUtils.sleep(1000);
				}
				log.info("Checking thread status timeout,waiting thread ["+checkThread.getName()+"] has abandoned,this thread will be force-completed");
			}
		});
	}
	
	/**
	 * 休眠（毫秒）
	 * @param millis
	 */
	private static void sleep(long millis) {
		try {
			TimeUnit.MILLISECONDS.sleep(millis);
		} catch (InterruptedException e) {
			log.error("Sleeping excetpion:", e);
		}
	}
}
