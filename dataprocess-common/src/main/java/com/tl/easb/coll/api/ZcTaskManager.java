package com.tl.easb.coll.api;

import com.tl.easb.coll.base.redis.LuaScriptManager;
import com.tl.easb.task.handle.subtask.SubTaskDefine;
import com.tl.easb.utils.template.RedisOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.*;

/**
 * 召测任务分发处理
 *
 * @author JerryHuang
 * <p>
 * 2014-2-24 下午4:47:33
 */
public class ZcTaskManager {
    private static Logger logger = LoggerFactory.getLogger(ZcTaskManager.class);

    static final int retryCount = 5;// 最多重复执行5次

    /**
     * 领取子任务.获取指定步长的子列表，如果返回为空，则当前任务执行完毕。
     *
     * @param task
     * @param stepLength
     * @return 子任务列表
     */
    @SuppressWarnings("unchecked")
    public static List<String> drawSubtasks(final String task, final long stepLength) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<List<String>>() {
            public List<String> doWithJedis(Jedis jedis) {
                String task2subtaskKey = ZcKeyDefine.KPREF_TASK2SUBTASK + task;
                String taskOffsetKey = ZcKeyDefine.KPREF_TASKOFFSET + task;

                List<String> keys = new ArrayList<String>(3);
                keys.add(taskOffsetKey);
                keys.add(String.valueOf(stepLength));
                keys.add(task2subtaskKey);

                String scriptFile = "lua/DrawTask.mylua";
                Object response = LuaScriptManager.evalsha(jedis, scriptFile, keys, null);
                List<String> result = (List<String>) response;
                return result;
            }
        });
    }

    /**
     * 获取给定二级任务的所有测点集合
     *
     * @param subtask
     * @return 测点集合
     */
    public static Set<String> getCps(final String subtask) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Set<String>>() {
            public Set<String> doWithJedis(Jedis jedis) {
                String subtaskCpKey = ZcKeyDefine.KPREF_SUBTASK2CP + subtask;
                return jedis.smembers(subtaskCpKey);
            }
        });
    }

    /**
     * 获取给定二级任务的所有测点集合长度
     *
     * @param subtasks 二级任务编号列表
     * @return 所有二级任务下测点数之和
     */
    public static long getCpSize(final List<String> subtasks) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Long>() {
            public Long doWithJedis(Jedis jedis) {
                long cpSize = 0;
                for (String subtask : subtasks) {
                    cpSize += jedis.scard(ZcKeyDefine.KPREF_SUBTASK2CP + subtask);
                }
                return cpSize;
            }
        });
    }

    /**
     * 获取给定二级任务的所有测点集合。
     * <p>
     * 为了不影响关键性的实时写入性能，请注意控制每次提交的subtask的数量。
     *
     * @param subtasks 二级任务编号列表
     * @return 测点集合
     */
    public static Set<String> getCps(final List<String> subtasks) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Set<String>>() {
            public Set<String> doWithJedis(Jedis jedis) {
//                Pipeline pp = jedis.pipelined();
//                List<Response<Set<String>>> allResult = new ArrayList<Response<Set<String>>>();
//                Response<Set<String>> singleResult = null;
//                for (String subtask : subtasks) {
//                    singleResult = pp.smembers(ZcKeyDefine.KPREF_SUBTASK2CP + subtask);
//                    allResult.add(singleResult);
//                }
//                pp.sync();
//
//                Set<String> result = new HashSet<String>();
//                for (Response<Set<String>> it : allResult) {
//                    result.addAll(it.get());
//                }
//                return result;
                Pipeline pp = jedis.pipelined();
                Map<String,Response<Set<String>>> allResult = new HashMap<>();

                Response<Set<String>> singleResult = null;
                for (String subtask : subtasks) {
                    String areaCode = subtask.split("_") [SubTaskDefine.SUBTASK_PARAMS_AREA];
                    String terminalAddr = subtask.split("_") [SubTaskDefine.SUBTASK_PARAMS_TERMINAL_ADDR];
                    singleResult = pp.smembers(ZcKeyDefine.KPREF_SUBTASK2CP + subtask);
                    StringBuffer sb = new StringBuffer();
                    sb.append("_").append(areaCode).append("_").append(terminalAddr);
                    allResult.put(sb.toString(),singleResult);
                }
                pp.sync();
                Set<String> result = new HashSet<String>();
                Set<Map.Entry<String, Response<Set<String>>>> entries = allResult.entrySet();
                for (Map.Entry<String, Response<Set<String>>> entry : entries) {
                    String areaTerminalAdd = entry.getKey();
                    for (String cp : entry.getValue().get()) {
                        cp=cp+areaTerminalAdd;
                        result.add(cp);
                    }
                }
                return result;
            }
        });
    }

    /**
     * 任务完成判定.
     * <p>
     * 任务完成判定：判断“任务队列偏移量”是否走到队尾，如果不是，则说明任务未下发完成，判定任务未结束；否则比较当前时间与“一级任务更新时间”
     * 之间的时间差是否超过预设阀值，如果超过，则判定任务执行完成，否则判定任务未完成。
     * 如果判定任务完成，则重置“一级任务更新时间”与“任务队列偏移量”为初始值。
     *
     * @param task      一级任务编号
     * @param timeLatch 时间阀值【毫秒数】，如果当前时间距离上次更新时间的间隔超过指定阀值，则判定任务执行完成。
     * @return
     */
    public static boolean checkTaskComplete(final String task, final long timeLatch) {
        return RedisOperation.runWithJedis(new RedisOperation.ActionWithRet<Boolean>() {
            public Boolean doWithJedis(Jedis jedis) {
                long currentMsTime = System.currentTimeMillis();
                String taskOffsetKey = ZcKeyDefine.KPREF_TASKOFFSET + task;
                String taskInfoKey = ZcKeyDefine.KPREF_TASK2SUBTASK + task;
                String taskUpdTimeKey = ZcKeyDefine.KPREF_TASKUPDTIME + task;

                Pipeline pipeline = jedis.pipelined();
                Response<Long> rMaxLen = pipeline.llen(taskInfoKey);
                Response<String> rOffset = pipeline.get(taskOffsetKey);
                Response<String> rUpdTime = pipeline.get(taskUpdTimeKey);
                pipeline.sync();
                long offset = Long.parseLong(rOffset.get());
                long maxLen = rMaxLen.get();
                if (offset < (maxLen - 1)) {
                    return false;
                }
                long lastTime = 0;
                if (rUpdTime.get() != null) {
                    lastTime = Long.parseLong(rUpdTime.get());
                }

                logger.info("taskUpdTimeKey[" + task + "]:" + rUpdTime.get());
                return (currentMsTime - lastTime) > timeLatch;
            }
        });
    }
}
