package com.tl.easb.system.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @描述 
 * @author lizhenming
 * @version 1.0
 * @history:
 *   修改时间         修改人                   描述
 *   2014-2-18    Administrator
 */
public class SystemContextListener implements ServletContextListener {
	private static Logger log = LoggerFactory.getLogger(SystemContextListener.class);
	// 要加载的类参数
	public static final String INIT_CLASS_PARAM = "systemListenerInit";
	// 分隔符
	public static final String SPLIT_STRING_CLZ = ",";
	// 参数分割符号
	public static final String SPLIT_STRING_PARAM = "#";

	public void contextDestroyed(ServletContextEvent event) {
		// 从配置文件获取要初始化的类
		String ctxParam = event.getServletContext().getInitParameter(INIT_CLASS_PARAM);
		if (null != ctxParam && ctxParam.length() > 0) {
			String[] names = ctxParam.split(SPLIT_STRING_CLZ);
			if (null != names) {
				for (int i = 0; i < names.length; i++) {
					String className = names[i].trim();
					if (!StringUtils.isEmpty(className))
						continue;
					try {
						AbstractSystemListener sysInit = (AbstractSystemListener) Class.forName(className).newInstance();
						sysInit.destory(event);
					} catch (Exception e) {
						log.error("【监听】系统监听销毁【" + className + "】时处错", e);
					}
				}
			}
		}
	}

	public void contextInitialized(ServletContextEvent event) {
		// 从配置文件获取要初始化的类
		String ctxParam = event.getServletContext().getInitParameter(INIT_CLASS_PARAM);
		log.info("要启动的类字符串：" + ctxParam);
		if (null != ctxParam && ctxParam.length() > 0) {
			String[] names = ctxParam.split(SPLIT_STRING_CLZ);
			if (null != names) {
				for (int i = 0; i < names.length; i++) {
					String className = names[i].trim();
					String params = null ;
					if (!StringUtils.isNotEmpty(className))
						continue;
					if (className.indexOf(SPLIT_STRING_PARAM) > 0) {
						String[] clzPrms = className.split(SPLIT_STRING_PARAM);
						className = clzPrms[0].trim();
						params =  clzPrms[1].trim();
					}
					try {
						AbstractSystemListener sysInit = (AbstractSystemListener) Class.forName(className).newInstance();
						sysInit.setParameters(params);
						sysInit.init(event);
					} catch (Exception e) {
						log.error("初始化类【"+className+"】时发生异常：" + e);
					}
				}
			}
		}
		log.info("-------启动类初始化完毕-------");
	}

}
