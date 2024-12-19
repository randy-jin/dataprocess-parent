package com.tl.easb.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * @author lizhenming
 * @version 1.0
 * @描述 获取spring bean工具类
 * @history: 修改时间         修改人                   描述
 * 2014-2-18    Administrator
 */
public class SpringUtils implements ApplicationContextAware {

    private static ApplicationContext applicationContext;

    @SuppressWarnings("static-access")
    public void setApplicationContext(ApplicationContext arg0)
            throws BeansException {

        this.applicationContext = arg0;
    }

    /**
     * 从当前IOC获取bean bean的id
     *
     * @return
     */
    public static Object getBean(String id) {
        Object obj = applicationContext.getBean(id);
        return obj;
    }
}
