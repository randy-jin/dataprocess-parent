package com.tl.easb.coll.base.concurrent;
/**
 * 数据处理接口
 * @author JerryHuang
 *
 * 2014-3-7 下午2:43:08
 */
public interface IDataHandler<T> {
	/**
	 * 数据处理逻辑
	 * @param data 需要处理的数据对象
	 * @return 被成功处理的数据
	 */
	public long handleData(T data);
}
