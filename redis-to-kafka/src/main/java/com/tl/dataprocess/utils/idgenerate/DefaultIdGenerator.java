package com.tl.dataprocess.utils.idgenerate;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultIdGenerator implements IdGenerator, Runnable {

	private String time;

	private AtomicInteger value;

	private static final DateFormat FORMATTER = new SimpleDateFormat("yyMMddHHmmss");

	private IdGeneratorConfig config;

	private Thread thread;

	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	private static DefaultIdGenerator instance = new DefaultIdGenerator();

	private DefaultIdGenerator() {
		config = new DefaultIdGeneratorConfig();
		time = DefaultIdGenerator.FORMATTER.format(new Date());
		value = new AtomicInteger(config.getInitial());

		thread = new Thread(this);
		thread.setDaemon(true);
		thread.start();
	}

	public static DefaultIdGenerator newInstance() {
		return instance;
	}

//	public DefaultIdGenerator() {
//		config = new DefaultIdGeneratorConfig();
//		time = DefaultIdGenerator.FORMATTER.format(new Date());
//		value = new AtomicInteger(config.getInitial());
//
//		thread = new Thread(this);
//		thread.setDaemon(true);
//		thread.start();
//	}

	public DefaultIdGenerator(IdGeneratorConfig config) {
		this.config = config;
		time = DefaultIdGenerator.FORMATTER.format(new Date());
		value = new AtomicInteger(config.getInitial());

		thread = new Thread(this);
		thread.setDaemon(true);
		thread.start();
	}

	@Override
	public String next() {
		lock.readLock().lock();
		StringBuffer sb = new StringBuffer(config.getPrefix()).append(config.getSplitString()).append(time)
				.append(config.getSplitString()).append(value.getAndIncrement());
//		sb.append(Thread.currentThread().getId());
		lock.readLock().unlock();
		return sb.toString();
	}

	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(1000 * config.getRollingInterval());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			String now = DefaultIdGenerator.FORMATTER.format(new Date());
			if (!now.equals(time)) {
				lock.writeLock().lock();
				time = now;
				value.set(config.getInitial());
				lock.writeLock().unlock();
			}
		}
	}

}
