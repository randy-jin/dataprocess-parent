package com.tl.eas.archives.service;

import org.future.annotate.AutoBond;
import org.future.handler.RunnableTask;
import org.future.pojo.FlowTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author Dongwei-Chen
 * @Date 2022/8/23 15:37
 * @Description
 */
public class SendTagsHandler extends RunnableTask {

    @AutoBond
    private RedisTemplate<String, String> redisTemplate;

    private final String QUEUE_NAME = "Q_CHG_ARCHIVES";

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    protected FlowTask execute(FlowTask flowTask) throws Exception {
        String data = flowTask.getData();
        if (data == null) {
            return null;
        }
        long ret = redisTemplate.boundListOps(QUEUE_NAME).leftPush(data);
        logger.info("返回队列长度:{}", ret);
        return null;
    }

    @Override
    protected void tryException(Exception e, FlowTask flowTask) {
        logger.error("写redis抛出异常", e);
    }
}
