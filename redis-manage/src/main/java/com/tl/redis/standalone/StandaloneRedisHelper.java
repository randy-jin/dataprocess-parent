package com.tl.redis.standalone;

import com.tl.redis.RedisHelper;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;

/**
 * Redis默认实例工具类
 *
 * @author jinzhiqiang
 * @date 2019-08-08
 */
@Component
public class StandaloneRedisHelper extends RedisHelper {

    @Resource(name = "standaloneRedisTemplate")
    public RedisTemplate redisTemplate;
}
