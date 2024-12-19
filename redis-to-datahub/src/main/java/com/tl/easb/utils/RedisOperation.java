package com.tl.easb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * Redis操作模版
 *
 * @author jinzhiqiang
 */
public class RedisOperation {
    private static Logger logger = LoggerFactory.getLogger(RedisOperation.class);

    static final int retryCount = 5;// 最多重复执行5次

    public interface ActionWithVoid<T> {
        void doWithJedis(Jedis jedis);
    }

    public interface ActionWithRet<T> {
        T doWithJedis(Jedis jedis);
    }

    public static <T> T runWithJedis(ActionWithRet<T> action) {
        int curCount = 0;
        while (curCount < retryCount) {
            Jedis jedis = CacheUtil.getJedis();
            if (jedis != null) {
                try {
                    return action.doWithJedis(jedis);
                } catch (Exception e) {
                    logger.error("redis operation(with return value) error,run count [" + curCount + "]:", e);
                } finally {
                    if (null != jedis) {
                        CacheUtil.returnJedis(jedis);
                    }
                }
            }
            curCount++;
        }
        return null;
    }

    public static void runWithJedis(ActionWithVoid<Void> action) {
        int curCount = 0;
        while (curCount < retryCount) {
            Jedis jedis = CacheUtil.getJedis();
            if (jedis != null) {
                try {
                    action.doWithJedis(jedis);
                    return;
                } catch (Exception e) {
                    logger.error("redis operation(with void) error,run count [" + curCount + "]:", e);
                } finally {
                    if (null != jedis) {
                        CacheUtil.returnJedis(jedis);
                    }
                }
            }
            curCount++;
        }
        return;
    }
}