package com.tl.easb.coll.api;

import com.tl.easb.coll.base.redis.LuaScriptManager;
import com.tl.easb.utils.template.RedisOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;

public class ZcTaskQueueManager {

    private static Logger logger = LoggerFactory.getLogger(ZcTaskQueueManager.class);

    /**
     * 初始化一级任务-二级任务关系数据集
     *
     * @param task     一级任务
     * @param subtasks 二级任务。LIST类型。
     * @return
     */
    public static long initRemovableTask2Subtask(final String task, final List<String> subtasks) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Integer>() {
            public Integer doWithJedis(Jedis jedis) {
                Pipeline pipeline = jedis.pipelined();
                String taskA = ZcKeyDefine.KPREF_RV_TASK2SUBTASK + task;
                for (String it : subtasks) {
                    pipeline.rpush(taskA, it);
                }
                pipeline.sync();
                if (logger.isInfoEnabled()) {
                    logger.info("[" + subtasks.size() + "] elements are populated into redis");
                }
                return subtasks.size();
            }
        });
    }

    /**
     * 获取指定长度的队列元素，并且删除它
     *
     * @param task
     * @param stepSize
     * @return 被获取的队列元素集
     */
    public static List<String> getAndRemoveSubtask(final String task, final int stepSize) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<List<String>>() {
            public List<String> doWithJedis(Jedis jedis) {
                String taskA = ZcKeyDefine.KPREF_RV_TASK2SUBTASK + task;
                List<String> keys = new ArrayList<String>(2);
                keys.add(taskA);
                keys.add(String.valueOf(stepSize));

                List<String> subtasks = (List<String>) LuaScriptManager.evalsha(jedis, "lua/TaskQueueFetch.mylua", keys);
                return subtasks;
            }
        });
    }
}
