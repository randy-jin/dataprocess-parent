package com.tl.utils.helper;

import com.tl.redis.StandaloneRedisConfig;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisConnectionUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Redis默认实例工具类
 *
 * @author jinzhiqiang
 * @date 2019-08-08
 */
@Component
public class StandaloneRedisHelper extends RedisHelper {

    /**
     * 随机获取RedisTemplate连接
     * @author wjj
     * @date 2021/11/23 16:33
     * @return
     */
    public static RedisTemplate getRandomTemplate() {
        return StandaloneRedisConfig.getRandomTemplate();
    }

    /**
     * 获取指定的redisTemplate连接
     * @author wjj
     * @date 2021/11/23 16:32
     * @return
     */
    private static RedisTemplate getTemplateByIndex(Integer num) {
        RedisTemplate redisTemplate = new RedisTemplate();
        redisTemplate.setConnectionFactory(StandaloneRedisConfig.getJedisConnectionFactory(num));
        redisTemplate.setDefaultSerializer(new StringRedisSerializer());
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    /**
     * 通过管道批量获取队列数据
     * @author wjj
     * @date 2021/11/23 16:32
     * @return
     */
    public static List<String> lpopPipline(int count, String key) {
        RedisTemplate template  = getRandomTemplate();
        List<String> stringList;
        stringList = template.executePipelined( new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                for (int i = 0; i < count; i++) {
                    connection.lPop(((StringRedisSerializer) template.getKeySerializer()).serialize(key));;
                }
                return null;
            }
        });
        stringList.removeAll(Collections.singleton(null));
        return stringList;
    }

}
