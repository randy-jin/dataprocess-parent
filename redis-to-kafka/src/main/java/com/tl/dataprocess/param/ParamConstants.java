package com.tl.dataprocess.param;

import com.tl.dataprocess.utils.PropertyUtils;
import org.apache.log4j.Logger;

import java.util.*;

public class ParamConstants {
	private static Logger logger = Logger.getLogger(ParamConstants.class);
	public static final Map<Object, Class<?>> classMap = new HashMap<Object, Class<?>>();

	public static void init() {
		Properties properties = PropertyUtils.getProperties("/params.properties");
		if (null != properties) {
			Set<Map.Entry<Object, Object>> set = properties.entrySet();
			Iterator<Map.Entry<Object, Object>> iterator = set.iterator();
			while (iterator.hasNext()) {
				Map.Entry<Object, Object> map_entry = iterator.next();
				try {
					Class<?> clazz = Class.forName((String) map_entry.getValue());
					classMap.put(Integer.parseInt((String) map_entry.getKey()), clazz);
				} catch (ClassNotFoundException e) {
					logger.error("由规约ID:" + map_entry.getKey() + "获取对应的Class Name:" + map_entry.getValue() + "发生异常:",
							e);
					continue;
				}
			}
		}
	}

}
