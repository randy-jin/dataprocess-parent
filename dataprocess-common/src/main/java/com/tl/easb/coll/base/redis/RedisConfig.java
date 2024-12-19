package com.tl.easb.coll.base.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class RedisConfig {
    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);
    private static final String RESOURCE = "redis.properties";

    private static Properties properties = null;

    private RedisConfig() {

    }

    static {
        loadProps();
    }

    /**
     * Returns system property.
     *
     * @param name the name of the property to return.
     * @return the property value specified by name.
     */
    public static String getProperty(String name) {
        String property = properties.getProperty(name);
        if (property == null) {
            return null;
        } else {
            return property.trim();
        }

    }

    public static int getInt(String name) {
        return Integer.parseInt(getProperty(name));
    }

    public static long getLong(String name) {
        return Long.parseLong(getProperty(name));
    }

    public static boolean getBoolean(String name) {
        return Boolean.parseBoolean(getProperty(name));
    }

    /**
     * Loads system properties from the disk.
     */
    private static void loadProps() {
        properties = new Properties();
        InputStream in = null;

        try {
            in = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(RESOURCE);
            properties.load(in);

        } catch (IOException ioe) {
            log.error("fail to load properties file" + RESOURCE);
        } finally {
            try {
                if (in != null)
                    in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}