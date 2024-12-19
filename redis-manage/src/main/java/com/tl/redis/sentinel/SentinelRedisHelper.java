package com.tl.redis.sentinel;

import com.tl.redis.RedisHelper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * Redis默认实例工具类
 *
 * @author jinzhiqiang
 * @date 2019-08-08
 */
@Component
public class SentinelRedisHelper extends RedisHelper {


    @Resource(name = "sentinelRedisTemplate")
    public RedisTemplate redisTemplate;


}
