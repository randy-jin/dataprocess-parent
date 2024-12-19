package com.tl.dataprocess.param;

import com.tl.dataprocess.utils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ParamConstants {
    private static Logger logger = LoggerFactory.getLogger(ParamConstants.class);
    public static final Map<Object, Class<?>> classMap = new HashMap<Object, Class<?>>();
    private static String orgStartWith;//地域区分

    public String getOrgStartWith() {
        return orgStartWith;
    }

    public void setOrgStartWith(String orgStartWith) {
        this.orgStartWith = orgStartWith;
    }

    public static String startWith = null;

    public static void init() {
        startWith = orgStartWith;
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
                    logger.error("由规约ID:" + map_entry.getKey() + "获取对应的Class Name:" + map_entry.getValue() + "发生异常:",e);
                    continue;
                }
            }
        }
    }

}
