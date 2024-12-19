package com.tl.utils.helper;

import com.sun.istack.internal.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Redis工具类
 *
 * @author jinzhiqiang
 */
public class RedisHelper {
    private static Logger logger = LoggerFactory.getLogger(RedisHelper.class);

    @Autowired
    public StringRedisTemplate stringRedisTemplate;

    @Autowired
    public RedisTemplate redisTemplate;

    /**
     * 对所有的key进行操作
     *
     * @param pattern  表达式
     * @param consumer 对迭代的key进行操作
     */
    public void scan(String pattern, Consumer<byte[]> consumer) {
        this.stringRedisTemplate.execute((RedisConnection connection) -> {
            try (Cursor<byte[]> cursor = connection.scan(ScanOptions.scanOptions().count(Long.MAX_VALUE).match(pattern).build())) {
                cursor.forEachRemaining(consumer);
                return null;
            } catch (IOException e) {
                logger.error("Redis Scan Excetpion: ", e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 获取所有符合条件的key
     *
     * @param pattern 表达式
     * @return
     */
    public List<String> keys(String pattern) {
        List<String> keys = new ArrayList<>();
        this.scan(pattern, item -> {
            // 符合条件的key
            String key = new String(item, StandardCharsets.UTF_8);
            keys.add(key);
        });
        return keys;
    }

    /**
     * 队列左边推入元素
     *
     * @param key
     * @param obj
     */
    public Long leftPush(String key, Object obj) {
        return redisTemplate.boundListOps(key).leftPush(obj);
    }

    /**
     * 队列左边推入列表元素
     *
     * @param key
     * @param obj
     */
    public Long leftPushAll(String key, Object obj) {
        return redisTemplate.boundListOps(key).leftPushAll(obj);
    }

    /**
     * 往ZSET结构中添加元素
     *
     * @param key
     * @param obj
     * @param score
     * @return
     */
    public Boolean zAdd(String key, Object obj, double score) {
        return redisTemplate.boundZSetOps(key).add(obj, score);
    }

    public void leftPushJdk(String key, Object obj) {
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new JdkSerializationRedisSerializer());
        redisTemplate.boundListOps(key).leftPush(obj);
    }

    /**
     * 队列右侧取出元素
     *
     * @param key
     * @return
     */
    public Object rightPop(String key) {
        return redisTemplate.boundListOps(key).rightPop();
    }

    public String hget(String key, String field) {
        return (String) redisTemplate.boundHashOps(key).get(field);
    }
    public List<String> hmget(String key, List<String> fieldList) {
        return  redisTemplate.opsForHash().multiGet(key,fieldList);
    }

    /**
     * 获取所有
     * @param key
     * @return
     */
    public Set<String> hgetall(String key) {
        return  redisTemplate.boundHashOps(key).keys();
    }

    public void hdel(String key,String filed) {
        redisTemplate.boundHashOps(key).delete(filed);
    }
    public boolean del(String key) {
        return redisTemplate.delete(key);
    }

    public List executeScript(RedisScript<List> redisScript, List<Object> keyList, Object... args) {
        List retList = (List) redisTemplate.execute(redisScript, keyList, args);
        return retList;
    }

    public void info() {
        System.out.println(redisTemplate.getConnectionFactory().getConnection().info());
    }

    public long llen(String key) {
        return redisTemplate.boundListOps(key).size();
    }

    /**
     *
     * @param key
     * @param value
     * @return
     */
    public boolean delByUnique(String key, @NotNull String value) {
        if (value.equals(stringRedisTemplate.opsForValue().get(key))) {
            stringRedisTemplate.delete(key);
        }
        return false;
    }



}
