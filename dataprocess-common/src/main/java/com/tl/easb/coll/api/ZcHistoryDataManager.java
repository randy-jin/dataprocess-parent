package com.tl.easb.coll.api;

import com.tl.easb.utils.template.RedisOperation;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.*;

/**
 * 召测历史数据管理
 *
 * @author JerryHuang
 * <p>
 * 2014-2-25 下午8:00:36
 */
public class ZcHistoryDataManager {
    /**
     * 根据指定开始-结束区间，获取指定任务下属的子任务列表
     *
     * @param task
     * @param fromIdx
     * @param toIdx
     * @return
     */
    public static List<String> getSubtasks(String task, final long fromIdx, final long toIdx) {
        final String task2subtaskKey = ZcKeyDefine.KPREF_TASK2SUBTASK + task;
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<List<String>>() {
            public List<String> doWithJedis(Jedis jedis) {
                return jedis.lrange(task2subtaskKey, fromIdx, toIdx);
            }
        });
    }

    /**
     * 获取指定二级任务下对应的所有测点集合
     *
     * @param jedis
     * @param subtasks
     * @return Map类型:key为二级任务编码(String类型),value为对应的测点数据（Set类型）
     */
    public static Map<String, Set<String>> getSubtaskCps(Jedis jedis, List<String> subtasks) {

        Map<String, Set<String>> result = new HashMap<String, Set<String>>();
        Pipeline pipeline = jedis.pipelined();
        List<Response<Set<String>>> responses = new ArrayList<Response<Set<String>>>();
        Response<Set<String>> response = null;
        for (String subtask : subtasks) {
            response = pipeline.smembers(ZcKeyDefine.KPREF_SUBTASK2CP + subtask);
            responses.add(response);
        }
        pipeline.sync();
        Set<String> cps = null;
        String subtask = null;
        for (int i = 0, m = subtasks.size(); i < m; i++) {
            subtask = subtasks.get(i);
            cps = responses.get(i).get();
            result.put(subtask, cps);
        }
        return result;
    }

}
