package com.tl.easb.coll.facade;

import java.util.Map;

public interface IDataFetchCallback<T>{
	/**
	 * 初始化参数
	 * @param args
	 */
	public void init(Map<String, Object> args);
	/**
	 * 判断循环测试是否结束
	 * @return
	 */
	public boolean isEnd();
	/**
	 * 从指定开始位置获取数据
	 * @param fromIdx
	 * @return
	 */
	
	public T fetchData(long fromIdx);
	/**
	 * 每次获取数据的最大记录数
	 * @return
	 */
	public long getFetchSize();
	/**
	 * 获取预定的总共循环次数（即fetchData方法将被执行的次数)。
	 * <p>在一些场景下可能无法或不想预先获取指定的循环次数，这时可以直接返回0.<p>
	 * 该参数不影响处理逻辑，其主要用在线程池执行环境下，用来判断所有线程是否执行完成。
	 * @return 预定的总共循环数.0代表不计算循环次数,这时将无法判断所有线程是否执行完成
	 */
	public int getCountdownLatchNum();
	/**
	 * 预先计算的被获取的记录总数。该参数不影响处理逻辑，主要用来与实际被处理的记录数进行比较。
	 * @return
	 */
	public long getCountedNum();
	
}
