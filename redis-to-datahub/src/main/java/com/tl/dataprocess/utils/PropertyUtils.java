package com.tl.dataprocess.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author lizhenming
 * @version 1.0
 * @描述
 * @history: 修改时间 修改人 描述 2014-2-21 Administrator
 */
public class PropertyUtils {
    private static Logger logger = LoggerFactory.getLogger(PropertyUtils.class);

    private static Map<String, Properties> propertiesMaps = new HashMap<>();

    public void setPropertiesMaps(Map<String, String> map) {
        Iterator<?> it = map.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next().toString();
            String value = map.get(key);
            if (!propertiesMaps.containsKey(key)) {
                propertiesMaps.put(key, getProperties(value));
            }
        }
    }

    /**
     * <p>
     * 获取properties的Value
     * </p>
     *
     * @param propertiesName的文件名称 （不包括后缀）
     * @param properties中的属性名
     * @return
     * @author 曾凡
     * @time 2013-7-17 下午02:48:47
     */
    public static String getValue(String name, String key) {
        if (!propertiesMaps.containsKey(name)) {
            propertiesMaps.put(name, getProperties(name));
        }
        return propertiesMaps.get(name).getProperty(key);
    }

    /**
     * <p>
     * </p>
     *
     * @param properties的文件名称 （不包括后缀）
     * @return Properties类
     * @author 曾凡
     * @time 2013-7-17 下午02:48:51
     */
    public static Properties getProperties(String propertiesName) {
        InputStream in = null;
        Properties properties = null;
        try {
            in = PropertyUtils.class.getResourceAsStream(propertiesName);
            if (in != null) {
                properties = new Properties();
                properties.load(in);
            }
        } catch (FileNotFoundException e) {

        } catch (IOException e) {
            logger.error(propertiesName + ".properties", e);
        } finally {
            if (in != null)
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error(propertiesName + ".properties", e);
                }
        }
        return properties;
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
     * 获取value值以，号分割的值，并转化成数组
     *
     * @param key
     * @return
     */
    public static int[] getTopicProperValues(String key) {
        if (null == key) {
            return null;
        }
        key = key.trim();
        String properValue = getProperValue(key);
        if (null == properValue) {
            properValue = "50,7";
        }
        String[] arrVal = properValue.split(",");
        int[] intArr = new int[arrVal.length];
        for (int i = 0; i < arrVal.length; i++) {
            intArr[i] = Integer.valueOf(arrVal[i]);
        }
        return intArr;
    }

    /**
     * 获取prperty值，如果不存在，返回defalultValue
     *
     * @param key
     * @return
     */
    public static String getProperValue(String key, String defalultValue) {
        // return ResourcesFactory.getString(key, defalultValue);
        for (Map.Entry<String, Properties> pro : propertiesMaps.entrySet()) {
            String value = pro.getValue().getProperty(key);
            if (null != value && !value.equals(""))
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
