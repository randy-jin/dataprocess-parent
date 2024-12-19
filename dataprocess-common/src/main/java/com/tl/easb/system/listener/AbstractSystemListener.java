package com.tl.easb.system.listener;

import javax.servlet.ServletContextEvent;

/**
 * @author lizhenming
 * @version 1.0
 * @描述
 * @history: 修改时间            修改人                   	描述
 */
public abstract class AbstractSystemListener {

    private String parameters;
    private String configString;

    /**
     * 初始化
     *
     * @param event
     */
    public abstract void onInit(ServletContextEvent event);

    /**
     * 销毁
     *
     * @param event
     */
    public abstract void onDestory(ServletContextEvent event);

    public void init(ServletContextEvent event) {
        this.onInit(event);
    }

    public void destory(ServletContextEvent event) {
        this.onDestory(event);
    }

    public String getParameters() {
        return parameters;
    }

    public void setParameters(String parameters) {
        this.parameters = parameters;
    }

    public String getConfigString() {
        return configString;
    }

    public void setConfigString(String configString) {
        this.configString = configString;
    }

}
