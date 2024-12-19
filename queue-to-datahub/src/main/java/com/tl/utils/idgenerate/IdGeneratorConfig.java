package com.tl.utils.idgenerate;

/**
 * ID生成器的配置接口
 * @author jinzhiqiang
 */
public interface IdGeneratorConfig {
	/**
	 * 获取分隔符
	 * @return
	 */
	String getSplitString();
	
	/**
	 * 获取初始值
	 * @return
	 */
	int getInitial(); 
	
	/**
	 * 获取ID前缀
	 * @return
	 */
	String getPrefix();
	
	/**
	 * 获取滚动间隔, 单位: 秒
	 * @return
	 */
	int getRollingInterval();
	
}
