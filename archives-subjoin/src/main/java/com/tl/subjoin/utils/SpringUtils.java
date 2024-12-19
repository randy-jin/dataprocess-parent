package com.tl.subjoin.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;


@Component
@Order(value = 1)
public class SpringUtils implements ApplicationContextAware {

    private static ApplicationContext applicationContext;

    /**
     * 服务器启动，Spring容器初始化时，当加载了当前类为bean组件后，
     * 将会调用下面方法注入ApplicationContext实例
     */
    @Override
    public void setApplicationContext(ApplicationContext arg0) throws BeansException {
        SpringUtils.applicationContext = arg0;
    }

    /**
     * 外部调用这个getBean方法就可以手动获取到bean
     * 用bean组件的name来获取bean
     *
     * @param beanName
     * @return
     */
    public static <T> T getBean(String beanName) {
        return (T) applicationContext.getBean(beanName);
    }
}
