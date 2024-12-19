package com.tl.easb.coll.base.concurrent;

/**
 * 数据处理接口
 * @author JerryHuang
 *
 * 2014-3-7 下午2:43:08
 */
public interface ITaskHandler {
	/**
	 * 数据处理逻辑
	 * @return 被处理的数据记录数
	 */
	public long handle();
}
