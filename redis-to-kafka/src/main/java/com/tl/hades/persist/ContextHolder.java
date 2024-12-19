package com.tl.hades.persist;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class ContextHolder implements ApplicationContextAware {

    public static ApplicationContext context;

    private boolean inited = Boolean.FALSE;


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
        inited = Boolean.TRUE;
    }

    public static ApplicationContext getContext(){
        if (context == null){
            throw new RuntimeException("spring 未初始化该bean");
        }
        return context;
    }

    private boolean checkInit(){
        if (context == null || !inited){
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }
    
    public static <T> T getBean(Class<T> tClass){
        return context.getBean(tClass);
    }
}