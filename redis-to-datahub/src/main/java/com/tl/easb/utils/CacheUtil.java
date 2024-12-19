package com.tl.easb.utils;

import com.tl.easb.coll.ParamConstants;
import com.tl.easb.coll.SubTaskDefine;
import com.tl.easb.coll.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 缓存存取工具类
 *
 * @author JinZhiQiang
 * @date 2014年3月10日
 */
public class CacheUtil {

    private static Logger log = LoggerFactory.getLogger(CacheUtil.class);

    private static final String KEY_HEADER = "T$";
    private static final String SEPARATOR = "#";

//	private static RedisTemplate redisTemplate = RedisTemplateInstance.redisTemplate;
//	private static JedisSentinelPool jedisSentinelPool = RedisTemplateInstance.jedisSentinelPool;

    private CacheUtil() {
    }

    /**
     * 清除指定缓存中的所有信息
     */
    @Deprecated
    public static void clearAll() {
        ZcDataManager.clearAll();
    }

    /**
     * 初始化一级任务-二级任务关系数据集
     *
     * @param autoTaskId
     * @param subtasks
     * @return
     */
    public static long initTask2Subtask(String autoTaskId, Set<String> subtasks, String collectDate) {
        long ret;
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autoTaskId, collectDate);
        ret = ZcDataManager.initTask2Subtask(wrapAutoTaskId, subtasks);
        return ret;
    }

    /**
     * 1、删除子任务与测点信息数据集 2、删除任务与子任务数据集 被方法clearCpData代替
     *
     * @param autotaskId
     */
    @Deprecated
    public static void cleanTaskCache(String autotaskId, String dataDate) {
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, dataDate);
        ZcDataManager.clearSubtaskCp(wrapAutoTaskId, ParamConstants.REDIS_DELETE_SUBTASK_STEP_LENGTH);
        ZcDataManager.clearTask2subtask(wrapAutoTaskId);
    }

    /**
     * 检索缓存任务队列是否有未处理信息
     *
     * @param autotaskId
     * @return
     */
    public static boolean isTaskAllDistributed(String autotaskId, String colDataDate) {
        boolean finished;
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, colDataDate);
        finished = ZcMonitorManager.isTaskAllDistributed(wrapAutoTaskId);
        return finished;
    }

    /**
     * 判断任务是否执行完成
     *
     * @param autotaskId
     * @return
     */
    public static boolean checkTaskComplete(String autotaskId, String colDataDate) {
        int timeLatch;
        boolean finished;
        timeLatch = ParamConstants.TASK_FINISH_TIMEOUT;
        String wrapAutoTaskId = CacheUtil.wrapAutoTaskId(autotaskId, colDataDate);
        // 判断缓存是否读取完成
        finished = ZcTaskManager.checkTaskComplete(wrapAutoTaskId, timeLatch);
        return finished;
    }

    /**
     * 过程数据清理 入参格式：数组内容为[行政区码,终端地址,测量点序号,采集日期（yyyymmddhhmmss）,AFN,FN] 供前入库服务调用
     * 根据测点信息，找到子任务信息，再根据子任务信息删除其对应的【采集信息数据集】
     *
     * @param cps
     */
    public static void process(final List<Object[]> cps) {
        RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid() {
            @Override
            public void doWithJedis(Jedis jedis) {
                Set<String> newCps = new HashSet<>(cps.size());
                try {
                    translate(cps, newCps);
                    if (newCps.size() > 0) {
                        List<String> newCpsList = new ArrayList<String>(newCps);
                        ZcProcedureManager.process(newCpsList, ParamConstants.REDIS_DELETE_MP_STEP_LENGTH,
                                System.currentTimeMillis());
                        newCps.clear();
                        newCpsList.clear();
                    }
                } catch (Exception e) {
                    log.error("过程数据清理发生异常：", e);
                }
            }
        });
    }

    /**
     * 采集点信息转换 入参格式：数组内容为[行政区码,终端地址,测量点序号,采集日期（yyyymmddhhmmss）,AFN,FN]
     * 出参格式：行政区码_终端地址_测量点序号_采集日期（yyyymmddhhmmss）_数据项标示
     *
     * @param cps
     * @return
     */
    private static void translate(List<Object[]> cps, Set<String> newCps) {
        for (int i = 0; i < cps.size(); i++) {
            Object[] cp = cps.get(i);
            String _tempCp = StringUtil.arrToStr(SubTaskDefine.SEPARATOR, cp[0], cp[1], cp[2], cp[3], cp[4]);
            newCps.add(_tempCp);
        }
        cps.clear();
    }

    /**
     * 功能：传入任务ID，从缓存中获取任务状态，并将其解析成数组，数组内容如下 [0]:开始时间 [1]:采集日期 [2]:优先级 [3]:补采次数
     * [4]:采集信息生成方式 [5]:执行标识 [6]:应采总数
     *
     * @param autotaskId
     * @return
     */
    public static String[] getTaskStatus(String autotaskId) {
        String taskStatus = ZcDataManager.getTaskStatus(autotaskId);
        String[] strs;
        if (null == taskStatus) {
            return null;
        }
        strs = StringUtil.explodeString(taskStatus, "_");
        return strs;
    }

    /**
     * 功能：清除任务状态缓存
     *
     * @param autotaskId
     * @return
     */
    @Deprecated
    public static long removeTaskStatus(String autotaskId) {
        return ZcDataManager.removeTaskStatus(autotaskId);
    }


    /**
     * 包装任务编号，作为redis键值使用
     *
     * @param autotaskId  任务编号
     * @param dataDateStr 采集数据日期字符串，格式如：20140407000000
     * @return
     */
    public static String wrapAutoTaskId(String autotaskId, String dataDateStr) {
        if (null == autotaskId) {
            return null;
        }
        if (null == dataDateStr) {
            String[] taskStatus = getTaskStatus(autotaskId);
            if (null != taskStatus) {
                dataDateStr = taskStatus[1];
            } else {
                log.error("Autotask [" + autotaskId + "]'s dataDateStr is null");
            }
        }
        return autotaskId + "_" + dataDateStr;
    }


    static final int retryCount = 5;// 最多重复获取5次

    /**
     * 获取jedis对象
     *
     * @return
     */
    public static Jedis getJedis() {
        Jedis jedis = null;
        JedisSentinelPool jedisSentinelPool = null;
        int curCount = 0;
        while (curCount < retryCount) {
            try {
                jedisSentinelPool = SpringUtils.getBean("jedisSentinelPool");
                jedis = jedisSentinelPool.getResource();
                return jedis;
            } catch (Exception e) {
                log.info("JedisPool activeNum:" + jedisSentinelPool.getNumActive() + ",idleNum:"
                        + jedisSentinelPool.getNumIdle() + ",waiterNum:" + jedisSentinelPool.getNumWaiters()
                        + ",MeanBorrowWaitTime:" + jedisSentinelPool.getMeanBorrowWaitTimeMillis()
                        + ",MaxBorrowWaitTime:" + jedisSentinelPool.getMaxBorrowWaitTimeMillis() + ",MaxBorrowWaitTime:"
                        + jedisSentinelPool.getMaxBorrowWaitTimeMillis());
                log.error("Get Jedis Object fail,Current Run Count:[" + curCount + "]", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    log.error(e1 + "");
                }
            }
            curCount++;
        }
        return jedis;
    }

    /**
     * 回收jedis到pool中
     *
     * @param jedis
     */
    public static void returnJedis(Jedis jedis) {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                log.error("Close Jedis Exception:", e);
            }

        }
    }

    /**
     * 清除redis实例范围内所有数据。谨慎使用！
     * <p>
     * redis客户端实例
     */
    public static void clearDB() {
        RedisOperation.runWithJedis(new RedisOperation.ActionWithVoid() {
            @Override
            public void doWithJedis(Jedis jedis) {
                jedis.flushDB();
            }
        });
    }

}
