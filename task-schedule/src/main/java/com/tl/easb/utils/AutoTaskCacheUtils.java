package com.tl.easb.utils;

import com.alibaba.fastjson.JSON;
import com.tl.easb.task.manage.view.AutoTaskConfig;
import com.tl.easb.utils.template.RedisOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;


/**
 * Created by huangchunhuai on 2021/9/24.
 */
public class AutoTaskCacheUtils {
    private static Logger log = LoggerFactory.getLogger(AutoTaskCacheUtils.class);

    private static final String AUTOTASK_MANAGER="TASKMANAGER";

    /**
     * 初始化任务信息到redis中
     * @param taskId
     * @param ac
     */
    public static void initTask(final String taskId,final AutoTaskConfig ac) {
        RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
            public void doWithJedis(Jedis jedis) {
                try {
//                    jedis.set(AUTOTASK_MANAGER+":"+taskId,JSON.toJSONString(ac));
                    jedis.hset(AUTOTASK_MANAGER,taskId,JSON.toJSONString(ac));
                } catch (Exception e) {
                    log.error("过程数据清理发生异常：", e);
                }
            }
        });
    }

    /**
     * 删除任务缓存
     * @param taskId
     */
    public static void delTask(final String taskId) {
        RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid<Void>() {
            public void doWithJedis(Jedis jedis) {
                try {
                    jedis.hdel(AUTOTASK_MANAGER,taskId);
                } catch (Exception e) {
                    log.error("过程数据清理发生异常：", e);
                }
            }
        });
    }

    /**
     * 查找任务缓存
     * @param taskId
     */
    public static AutoTaskConfig findTask(final String taskId) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<AutoTaskConfig>() {
            public AutoTaskConfig doWithJedis(Jedis jedis) {
                String taskJson=jedis.hget(AUTOTASK_MANAGER,taskId);
                if(taskJson==null){
                    return null;
                }
                return  JSON.parseObject(taskJson,AutoTaskConfig.class);
            }
        });
    }
}
