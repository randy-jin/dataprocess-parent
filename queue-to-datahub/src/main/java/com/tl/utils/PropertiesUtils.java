package com.tl.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 读取配置YAML配置文件工具类
 *
 * @author jinzhiqiang
 * @date 2020.12.15
 */
public class PropertiesUtils {
    private static Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);


    public static LinkedBlockingQueue<Object[]> clearQueue=new LinkedBlockingQueue<>();

    private static String application = "application.yml";
    private static String application_dev = "application-dev.yml";
    private static String application_prod = "application-prod.yml";
    private static Properties properties = null;
    private final static String activeKey = "spring.profiles.active";

    private static void loadYaml() {
        Resource resource = new ClassPathResource(application);
        try {
            YamlPropertiesFactoryBean yamlPropertiesFactoryBean = new YamlPropertiesFactoryBean();
            yamlPropertiesFactoryBean.setResources(resource);
            properties = yamlPropertiesFactoryBean.getObject();
            String activeValue = properties.getProperty(activeKey);
            if ("dev".equals(activeValue)) {
                Resource resource_dev = new ClassPathResource(application_dev);
                yamlPropertiesFactoryBean.setResources(resource, resource_dev);
            } else if ("prod".equals(activeValue)) {
                Resource resource_prod = new ClassPathResource(application_prod);
                yamlPropertiesFactoryBean.setResources(resource, resource_prod);
            }
            properties = yamlPropertiesFactoryBean.getObject();
        } catch (Exception e) {
            logger.error("读取YAML文件异常:", e);
        }
    }

    public static String getCommonYaml(String key) {
        if (null == properties) {
            loadYaml();
        }
        String value = properties.getProperty(key);
        if (null == value || value.equals("")) {
            logger.error("无法从当前工程的YAML配置文件中找到key为[" + key + "]的配置");
            return null;
        }
        return value;
    }

    public static void main(String[] args) {
        logger.info(getCommonYaml("spring.profiles.active"));
    }

}
