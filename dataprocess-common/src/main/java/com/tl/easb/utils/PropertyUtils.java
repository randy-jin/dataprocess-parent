package com.tl.easb.utils;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;

/**
 * hch 把property文件放入apollo
 */
public class PropertyUtils {
	private static Logger logger =  LoggerFactory.getLogger(PropertyUtils.class);


	private static Map<String, Config> configMaps = new HashMap<String, Config>();
	public static void initMap(String nameSpace) {
		Config config= ConfigService.getConfig(nameSpace);
		configMaps.put(nameSpace,config);
	}

	public void setConfigMaps(Map<String, String> map) {
		Iterator<?> it = map.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next().toString();
			String value = map.get(key);
			if (!configMaps.containsKey(key)) {
				initMap(key);
			}
		}
	}


	public static String getValue(String name, String key) {
		if (!configMaps.containsKey(name)) {
			initMap(key);
		}
		return configMaps.get(name).getProperty(key,null);
	}



	/**
	 * 获取prperty值，如果不存在，返回""空字符串
	 *
	 * @param key
	 * @return
	 */
	public static String getProperValue(String key) {
		return getProperValue(key, null);
	}

	/**
	 * 获取prperty值，如果不存在，返回defalultValue
	 *
	 * @param key
	 * @return
	 */
	public static String getProperValue(String key, String defalultValue) {
		for(Map.Entry<String, Config> pro : configMaps.entrySet()) {
			String value = pro.getValue().getProperty(key,null);
			if(null != value && !value.equals(""))
				return value;
		}
		return null;
	}

	/**
	 * 获取整数类型数据
	 *
	 * @param key
	 * @return
	 */
	public static int getIntValue(String key) {
		return getIntValue(key, false, -1);
	}

	/**
	 * 获取整数类型数据
	 *
	 * @param key
	 * @param defaultVal
	 * @return
	 */
	public static int getIntValue(String key, int defaultVal) {
		return getIntValue(key, true, defaultVal);
	}

	private static int getIntValue(String key, boolean needDefault, int defaultval) {
		String value = getProperValue(key);
		int retVal = -1;
		try {
			retVal = Integer.parseInt(value);
		} catch (Exception e) {
			if (needDefault) {
				retVal = defaultval;
			} else {
				throw new RuntimeException(e);
			}
		}
		return retVal;
	}
}
