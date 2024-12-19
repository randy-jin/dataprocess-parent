package com.tl.dataprocess.utils.idgenerate;

/**
 * 
 * ID生成器接口, 用于生成全局唯一的ID流水号
 * 
 * @author jinzhiqiang
 */
public interface IdGenerator {
	/**
	 * 生成下一个不重复的流水号
	 * 
	 * @return
	 */
	String next();
}
