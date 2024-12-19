package com.tl.dataprocess.utils;

import java.util.concurrent.ThreadFactory;

/**
 * 公共线程工厂类
 * 
 * @author jinzhiqiang
 *
 */
public class TaskThreadPool implements ThreadFactory {
	private int counter = 0;
	private String prefix = "";

	public TaskThreadPool(String prefix) {
		this.prefix = prefix;
	}

	public Thread newThread(Runnable r) {
		return new Thread(r, prefix + "-" + counter++);
	}
}
