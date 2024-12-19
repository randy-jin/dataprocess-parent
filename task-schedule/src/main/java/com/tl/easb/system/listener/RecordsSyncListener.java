package com.tl.easb.system.listener;

import javax.servlet.ServletContextEvent;


import com.tl.easb.recordsync.exec.RecordsSyncExec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 启动加载dataitem
 * @author JinZhiQiang
 * @date 2014年4月25日
 */
public class RecordsSyncListener extends AbstractSystemListener {
    private static Logger log = LoggerFactory.getLogger(RecordsSyncListener.class);

    @Override
    public void onInit(ServletContextEvent event) {
        // 档案同步守护线程
        RecordsSyncExec.startThread();
        log.info("启动数据项加载监控线程完毕！");
    }

    @Override
    public void onDestory(ServletContextEvent event) {

    }

}
