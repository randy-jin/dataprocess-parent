package com.tl.easb.coll.facade;

import com.tl.easb.coll.api.CpInitResultModel;
import com.tl.easb.coll.api.ZcDataManager;
import com.tl.easb.coll.base.concurrent.ConcurrentDataExecutor;
import com.tl.easb.coll.base.concurrent.IDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

/**
 * @author JerryHuang
 * <p>
 * 2014-3-7 下午2:29:50
 */
public class ZcDataInitiator {
    private Logger logger = LoggerFactory.getLogger(ZcDataInitiator.class);

    public static CpInitResultModel initCpAndSubtask(String task,
                                                     IDataFetchCallback<Map<String, Set<String>>> callback) {
        Map<String, Object> args = new HashMap<String, Object>(1);
        args.put("task", task);
        long sum = 0;
        callback.init(args);
        long fromIdx = 0;
        CpInitResultModel cpInitResult = new CpInitResultModel();
        List<String> existedCps = new ArrayList<String>();
        while (!callback.isEnd()) {
            Map<String, Set<String>> data = callback.fetchData(fromIdx);
            fromIdx += callback.getFetchSize();
            List<String> currentExistedCps = ZcDataManager.initCpAndSubtaskDs(data);
            existedCps.addAll(currentExistedCps);
            sum += calculateSum(data);
        }
        cpInitResult.setExistedCps(existedCps);
        cpInitResult.setHandledSum(sum);
        return cpInitResult;
    }

    private static long calculateSum(Map<String, Set<String>> data) {
        long sum = 0;
        for (Collection<String> it : data.values()) {
            sum += it.size();
        }
        return sum;
    }

    public static CpInitResultModel initCpAndSubtaskConcurrently(final JedisPool jedisPool, String task,
                                                                 final IDataFetchCallback<Map<String, Set<String>>> callback, int threadNum, long sleepInterval) {
        Map<String, Object> callbackArgs = new HashMap<String, Object>(1);
        callbackArgs.put("task", task);
        callback.init(callbackArgs);

        ConcurrentDataExecutor<Map<String, Set<String>>> executor = new ConcurrentDataExecutor<Map<String, Set<String>>>();
        CpInitResultModel result = new CpInitResultModel();
        final List<String> allExistedCps = new ArrayList<String>();
        long handledSum = executor.execute(threadNum, sleepInterval, callback,
                new IDataHandler<Map<String, Set<String>>>() {
                    public long handleData(Map<String, Set<String>> data) {
                        Jedis jedis = null;
                        long currentHandledSum = 0;
                        try {
                            jedis = jedisPool.getResource();
                            List<String> existedCps = ZcDataManager.initCpAndSubtaskDs(data);
                            allExistedCps.addAll(existedCps);
                            currentHandledSum = calculateSum(data);
                            // System.out.println("recordnum:"+recordNum);
                        } finally {
//                            jedisPool.returnResource(jedis);
                            jedis.close();
                        }
                        return currentHandledSum;
                    }

                });
        result.setExistedCps(allExistedCps);
        result.setHandledSum(handledSum);
        return result;
    }

    public static long initCp2subtask(Jedis jedis, String task, IDataFetchCallback<Map<String, String>> callback) {
        Map<String, Object> args = new HashMap<String, Object>(1);
        args.put("task", task);
        long sum = 0;
        callback.init(args);
        long fromIdx = 0;
        while (!callback.isEnd()) {
            Map<String, String> data = callback.fetchData(fromIdx);
            fromIdx += callback.getFetchSize();
            sum += ZcDataManager.initCp2subtask(data);
        }
        return sum;
    }

    public static long initCp2subtaskConcurrently(final JedisPool jedisPool, String task,
                                                  final IDataFetchCallback<Map<String, String>> callback, int threadNum, long sleepInterval) {
        Map<String, Object> callbackArgs = new HashMap<String, Object>(1);
        callbackArgs.put("task", task);
        callback.init(callbackArgs);

        ConcurrentDataExecutor<Map<String, String>> executor = new ConcurrentDataExecutor<Map<String, String>>();

        return executor.execute(threadNum, sleepInterval, callback, new IDataHandler<Map<String, String>>() {
            public long handleData(Map<String, String> data) {
                Jedis jedis = null;
                long recordNum = 0;
                try {
                    jedis = jedisPool.getResource();
                    recordNum = ZcDataManager.initCp2subtask(data);
                    // System.out.println("recordnum:"+recordNum);
                } finally {
//                    jedisPool.returnResource(jedis);
                    jedis.close();
                }
                return recordNum;
            }

        });
    }
}
