package com.tl.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
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


    public void hset(String name, String key,String value) {
        redisTemplate.boundHashOps(name).put(key,value);
    }
    /**
     * 获取所有
     * @param key
     * @return
     */
    public Set<String> hkeys(String key) {
        return  redisTemplate.boundHashOps(key).keys();
    }


    public Map<String,String> hgetall(String key) {
        redisTemplate.setHashValueSerializer(new StringRedisSerializer());
        return  redisTemplate.boundHashOps(key).entries();
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



    public List<String> scanByHash(String hashName,String pattern){
        List<String> hasList=new ArrayList<>();
        BoundHashOperations<String,String,Object> boundHashOperations= stringRedisTemplate.boundHashOps(hashName);
        Cursor<Map.Entry<String,Object>> cursor=boundHashOperations.scan(ScanOptions.scanOptions().match(pattern).build());
        while (cursor.hasNext()){
            Map.Entry<String,Object> entry=cursor.next();
            hasList.add(entry.getKey());
        }
        return hasList;
    }


    public Map<Object,Double> scanByZset(String sortName,String pattern){
        Map<Object,Double> scanMap=new HashMap<>();
        BoundZSetOperations boundZSetOperations= stringRedisTemplate.boundZSetOps(sortName);
        Cursor<ZSetOperations.TypedTuple> cursor=boundZSetOperations.scan(ScanOptions.NONE);
        while (cursor.hasNext()){
            ZSetOperations.TypedTuple typedTuple=cursor.next();
            if(typedTuple.getValue().toString().contains(pattern)){
                scanMap.put(typedTuple.getValue(),typedTuple.getScore());
            }

        }
        return scanMap;
    }

}
