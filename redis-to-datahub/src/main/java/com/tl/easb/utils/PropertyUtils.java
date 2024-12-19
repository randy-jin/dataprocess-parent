package com.tl.easb.utils;

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
 * @author Dongwei-Chen
 * @Date 2020/5/28 19:28
 * @Description 
 */
public class PropertyUtils {

    private static Logger logger = LoggerFactory.getLogger(PropertyUtils.class);

    private static Map<String, Properties> propertiesMaps = new HashMap<String, Properties>();

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


    public static String getValue(String name, String key) {
        if (!propertiesMaps.containsKey(name)) {
            propertiesMaps.put(name, getProperties(name));
        }
        return propertiesMaps.get(name).getProperty(key);
    }


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
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error(propertiesName + ".properties", e);
                }
            }
        }
        return properties;
    }


    public static String getProperValue(String key) {
        return getProperValue(key, null);
    }


    public static String getProperValue(String key, String defalultValue) {
        for (Map.Entry<String, Properties> pro : propertiesMaps.entrySet()) {
            String value = pro.getValue().getProperty(key);
            if (null != value && !value.equals("")) {
                return value;
            }
        }
        return null;
    }

    public static int getIntValue(String key) {
        return getIntValue(key, false, -1);
    }


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
